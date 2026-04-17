package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/DevLikhith5/kasoku/internal/merkle"
	"github.com/DevLikhith5/kasoku/internal/ring"
	"github.com/DevLikhith5/kasoku/internal/rpc"
	"github.com/DevLikhith5/kasoku/internal/server"
	storage "github.com/DevLikhith5/kasoku/internal/store"
	lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
)

type NodeConfig struct {
	NodeID         string        // unique identifier, e.g. 'localhost:8080'
	HTTPAddr       string        // address to listen on, e.g. ':8080'
	DataDir        string        // where to store data, e.g. './data/node1'
	Seeds          []string      // addresses of existing cluster nodes to join
	N              int           // replication factor (default 3)
	W              int           // write quorum (default 2)
	R              int           // read quorum (default 2)
	GossipInterval time.Duration // how often to gossip (default 1s)
}

func DefaultNodeConfig() NodeConfig {
	return NodeConfig{
		N:              3,
		W:              2,
		R:              2,
		GossipInterval: time.Second,
	}
}

type Node struct {
	cfg            NodeConfig
	engine         *lsmengine.LSMEngine
	ring           *ring.Ring
	members        *MemberList
	hints          *HintStore
	phiDetectors   *PhiDetectorMap
	cluster        *Cluster
	httpSrv        *server.Server
	httpServer     *http.Server // Bug 6/7 fix: stored so Stop() can shut it down
	versionCounter versionCounter
	timeoutTracker *AdaptiveTimeout
	logger         *slog.Logger
	done           chan struct{} // shutdown signal
	stopOnce       sync.Once     // prevents double-close of done
	wg             sync.WaitGroup
	mu             sync.RWMutex
	vectorClocks   map[string]storage.VectorClock // per-key vector clocks
	vcMu           sync.RWMutex
}

func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.N <= 0 {
		cfg.N = 3
	}
	if cfg.W <= 0 {
		cfg.W = 2
	}
	if cfg.R <= 0 {
		cfg.R = 2
	}
	if cfg.GossipInterval <= 0 {
		cfg.GossipInterval = time.Second
	}

	// Open LSM storage engine
	engine, err := lsmengine.NewLSMEngineWithConfig(cfg.DataDir, lsmengine.LSMConfig{
		NodeID: cfg.NodeID,
	})
	if err != nil {
		return nil, fmt.Errorf("open engine: %w", err)
	}

	r := ring.New(ring.DefaultVNodes)
	r.AddNode(cfg.NodeID) // add yourself first

	n := &Node{
		cfg:            cfg,
		engine:         engine,
		ring:           r,
		members:        NewMemberList(cfg.NodeID),
		hints:          NewHintStore(),
		phiDetectors:   NewPhiDetectorMap(),
		timeoutTracker: NewAdaptiveTimeout(),
		logger:         slog.Default(),
		done:           make(chan struct{}),
	}

	// Build the cluster layer (replication logic)
	n.cluster = New(ClusterConfig{
		NodeID:            cfg.NodeID,
		NodeAddr:          cfg.HTTPAddr,
		Ring:              r,
		Store:             engine,
		ReplicationFactor: cfg.N,
		QuorumSize:        cfg.W,
		RPCTimeout:        5 * time.Second,
		Logger:            n.logger,
		Members:           n.members,
	})

	// Build the HTTP server and wire it to this node
	n.httpSrv = server.New(n)

	return n, nil
}

func (n *Node) Start() error {
	// Join existing cluster nodes
	for _, seed := range n.cfg.Seeds {
		if err := n.joinSeed(seed); err != nil {
			n.logger.Warn("could not join seed", "addr", seed, "error", err)
		}
	}

	addr := n.cfg.HTTPAddr
	if addr == "" {
		addr = ":8080"
	}

	// Bug 7 fix: build the http.Server from n.httpSrv (the stored server), not
	// a separate anonymous struct, so Stop() can shut down the actual running server.
	n.httpServer = &http.Server{
		Addr:    addr,
		Handler: n.httpSrv.Routes(),
	}

	// Bug 6 fix: track the HTTP server goroutine in n.wg so Stop() correctly waits
	n.wg.Add(3 + 1) // 3 background goroutines + 1 HTTP server goroutine
	go n.gossipLoop()
	go n.antiEntropyLoop()
	go n.hintDeliveryLoop()

	go func() {
		defer n.wg.Done()
		n.logger.Info("starting HTTP server", "addr", addr)
		if err := n.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			n.logger.Error("HTTP server error", "error", err)
		}
	}()

	// Bug fix: Start() must not block the caller — run the serve+shutdown
	// in a goroutine so callers can select on a done/error channel.
	go func() {
		// Wait for shutdown signal
		<-n.done

		// Graceful HTTP shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		n.httpServer.Shutdown(ctx) //nolint:errcheck
	}()

	return nil
}

func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.done) }) // safe to call multiple times
	n.wg.Wait()
	n.engine.Close()
}

// --- Node methods exposed to HTTP handlers ---

func (n *Node) Scan(ctx context.Context, prefix string) ([]string, error) {
	entries, err := n.engine.Scan(prefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = e.Key
	}
	return keys, nil
}

func (n *Node) GetRing() *ring.Ring {
	return n.ring
}

func (n *Node) GetMembers() []string {
	return n.members.Members()
}

func (n *Node) GetNodeID() string {
	return n.cfg.NodeID
}

func (n *Node) GetStatus() map[string]any {
	return map[string]any{
		"node_id":       n.cfg.NodeID,
		"http_addr":     n.cfg.HTTPAddr,
		"ring_nodes":    n.ring.NodeCount(),
		"total_members": n.members.MemberCount(),
		"alive_members": n.members.AliveCount(),
		"replication":   n.cfg.N,
		"write_quorum":  n.cfg.W,
		"read_quorum":   n.cfg.R,
		"pending_hints": n.hints.PendingCount(),
	}
}

func (n *Node) GetRingNodes() []string {
	return n.ring.GetAllNodes()
}

func (n *Node) getOrCreateVectorClock(key string) storage.VectorClock {
	n.vcMu.Lock()
	defer n.vcMu.Unlock()

	if n.vectorClocks == nil {
		n.vectorClocks = make(map[string]storage.VectorClock)
	}

	if vc, ok := n.vectorClocks[key]; ok {
		return vc
	}

	return storage.NewVectorClock()
}

func (n *Node) updateVectorClock(key string, vc storage.VectorClock) {
	n.vcMu.Lock()
	defer n.vcMu.Unlock()

	if n.vectorClocks == nil {
		n.vectorClocks = make(map[string]storage.VectorClock)
	}
	n.vectorClocks[key] = vc
}

func (n *Node) getVectorClock(key string) (storage.VectorClock, bool) {
	n.vcMu.RLock()
	defer n.vcMu.RUnlock()

	if n.vectorClocks == nil {
		return nil, false
	}
	vc, ok := n.vectorClocks[key]
	return vc, ok
}

func (n *Node) HandleReplicate(ctx context.Context, key string, value []byte) error {
	return n.engine.Put(key, value)
}

func (n *Node) HandleReplicateGet(ctx context.Context, key string) ([]byte, bool, error) {
	entry, err := n.engine.Get(key)
	if err != nil {
		return nil, false, err
	}
	return entry.Value, true, nil
}

func (n *Node) HandleReplicateGetEntry(ctx context.Context, key string) (storage.Entry, error) {
	return n.engine.Get(key)
}

func (n *Node) HandleReplicateDelete(ctx context.Context, key string) (bool, error) {
	err := n.engine.Delete(key)
	return err == nil, err
}

func (n *Node) HandleGossip(remoteMembers []string) []string {
	merged := n.members.Merge(remoteMembers)

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, nodeID := range merged {
		if nodeID == n.cfg.NodeID || nodeID == n.cfg.HTTPAddr {
			continue
		}
		if !n.ring.HasNode(nodeID) {
			n.ring.AddNode(nodeID)
			n.cluster.AddPeer(nodeID, nodeID)
		}
	}

	return merged
}

func (n *Node) HandleHint(key string, value []byte, targetNode string) error {
	return n.hints.Store(key, value, targetNode)
}

// HandleMerkle returns a serialized Merkle tree of all local keys
// for anti-entropy comparison with peer nodes.
func (n *Node) HandleMerkle() ([]byte, error) {
	tree, err := n.buildLocalMerkle()
	if err != nil {
		return nil, err
	}
	return merkle.Serialize(tree)
}

func (n *Node) buildLocalMerkle() (*merkle.Node, error) {
	keys, err := n.engine.Keys()
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	tree := merkle.Build(keys, func(k string) []byte {
		e, err := n.engine.Get(k)
		if err != nil {
			return nil
		}
		return e.Value
	})
	return tree, nil
}

func (n *Node) fetchRemoteMerkle(peerID string) (*merkle.Node, error) {
	addr, ok := n.cluster.nodeAddrMap[peerID]
	if !ok {
		addr = peerID
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/internal/merkle", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create merkle request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	getClient := func() *http.Client {
		return &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 50,
				IdleConnTimeout:     30 * time.Second,
			},
			Timeout: 10 * time.Second,
		}
	}
	resp, err := getClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("merkle request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("merkle request failed: status %d", resp.StatusCode)
	}

	var buf []byte
	buf, err = readAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read merkle response: %w", err)
	}

	return merkle.Deserialize(buf)
}

// --- Background goroutines ---

func (n *Node) joinSeed(seedAddr string) error {
	n.logger.Info("joining seed", "addr", seedAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := rpc.NewClient(seedAddr)
	if err := client.HealthCheck(ctx); err != nil {
		n.logger.Warn("seed unreachable, adding optimistically", "addr", seedAddr, "error", err)
	}

	n.members.AddMember(seedAddr)
	n.ring.AddNode(seedAddr)
	n.cluster.AddPeer(seedAddr, seedAddr)

	n.logger.Info("joined seed", "addr", seedAddr)
	return nil
}

// gossipLoop periodically exchanges membership info with peers.
// Bug 16 fix: reuses the cluster's client pool instead of creating a new
// rpc.Client on every tick, preventing an unbounded resource leak.
func (n *Node) gossipLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(n.cfg.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.members.Tick()
			// Pick a random member to gossip with
			peer := n.members.RandomMember()
			if peer == "" || peer == n.cfg.NodeID {
				continue
			}
			// Reuse client from cluster pool to avoid creating a new connection every tick
			client, ok := n.cluster.getClient(peer)
			if !ok {
				client = rpc.NewClient(peer) // fallback if not in pool
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			ourMembers := n.members.Members()
			theirMembers, err := client.Gossip(ctx, ourMembers)
			cancel()
			if err != nil {
				n.logger.Debug("gossip failed", "peer", peer, "error", err)
				continue
			}
			n.members.Merge(theirMembers)
			n.phiDetectors.Heartbeat(peer)
			n.logger.Debug("gossip completed", "peer", peer, "remote_members", len(theirMembers))
		case <-n.done:
			return
		}
	}
}

// antiEntropyLoop periodically syncs data with peers using Merkle tree comparison.
// This catches divergence that hinted handoff missed (e.g. hints expired after 24h).
func (n *Node) antiEntropyLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.runAntiEntropy()
		case <-n.done:
			return
		}
	}
}

func (n *Node) runAntiEntropy() {
	peers := n.members.Members()
	for _, peerID := range peers {
		if peerID == n.cfg.NodeID {
			continue
		}
		if !n.members.IsAlive(peerID) {
			continue
		}
		if err := n.syncWithPeer(peerID); err != nil {
			n.logger.Warn("anti-entropy sync failed",
				"peer", peerID, "error", err)
		}
	}
}

// syncWithPeer builds a local Merkle tree, fetches the remote one, diffs them,
// and synchronizes any divergent keys by comparing versions.
func (n *Node) syncWithPeer(peerID string) error {
	// Build local Merkle tree
	localTree, err := n.buildLocalMerkle()
	if err != nil {
		return fmt.Errorf("build local merkle: %w", err)
	}

	// Get remote Merkle tree
	remoteTree, err := n.fetchRemoteMerkle(peerID)
	if err != nil {
		return fmt.Errorf("fetch remote merkle: %w", err)
	}

	// Find differing keys — O(K log N) instead of O(N)
	diffKeys := merkle.Diff(localTree, remoteTree)
	if len(diffKeys) == 0 {
		n.logger.Debug("anti-entropy: in sync", "peer", peerID)
		return nil
	}

	n.logger.Info("anti-entropy: syncing",
		"peer", peerID, "diff_count", len(diffKeys))

	// For each differing key, compare versions and sync the winner
	for _, key := range diffKeys {
		localEntry, localErr := n.engine.Get(key)
		remoteEntry, remoteErr := n.remoteGet(context.Background(), peerID, key)

		switch {
		case localErr != nil && remoteErr == nil:
			// Remote has it, we don't — pull it
			if err := n.engine.Put(key, remoteEntry.Value); err != nil {
				n.logger.Warn("anti-entropy pull failed", "key", key, "error", err)
			}
		case localErr == nil && remoteErr != nil:
			// We have it, remote doesn't — push it
			if !errors.Is(remoteErr, storage.ErrKeyNotFound) {
				break // remote error, not a missing key
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := n.remoteReplicate(ctx, peerID, key, localEntry.Value, false, localEntry.Version)
			cancel() // always cancel, even on success
			if err != nil {
				n.logger.Warn("anti-entropy push failed", "key", key, "error", err)
			}
		case localErr == nil && remoteErr == nil:
			// Both have it — highest version wins
			if localEntry.Version < remoteEntry.Version {
				if err := n.engine.Put(key, remoteEntry.Value); err != nil {
					n.logger.Warn("anti-entropy pull failed", "key", key, "error", err)
				}
			} else if localEntry.Version > remoteEntry.Version {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				err := n.remoteReplicate(ctx, peerID, key, localEntry.Value, false, localEntry.Version)
				cancel() // always cancel, even on success
				if err != nil {
					n.logger.Warn("anti-entropy push failed", "key", key, "error", err)
				}
			}
		}
	}
	return nil
}

// hintDeliveryLoop periodically retries delivering hinted handoffs.
// Bug 17 fix: reuses the cluster's client pool instead of creating a new
// rpc.Client on every retry, preventing an unbounded resource leak.
func (n *Node) hintDeliveryLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.hints.RetryFailed(func(targetNode string, key string, value []byte) error {
				// Reuse client from cluster pool to avoid creating a new connection every retry
				client, ok := n.cluster.getClient(targetNode)
				if !ok {
					client = rpc.NewClient(targetNode) // fallback if not in pool
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return client.ReplicatedPut(ctx, key, value)
			})
		case <-n.done:
			return
		}
	}
}

// readAll is a small helper wrapping io.ReadAll so fetchRemoteMerkle
// doesn't need an inline alias for the io package.
func readAll(r io.Reader) ([]byte, error) {
	return io.ReadAll(r)
}
