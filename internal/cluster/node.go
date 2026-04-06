package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/DevLikhith5/kasoku/internal/ring"
	"github.com/DevLikhith5/kasoku/internal/rpc"
	"github.com/DevLikhith5/kasoku/internal/server"
	storage "github.com/DevLikhith5/kasoku/internal/store"
	lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
)

// NodeConfig holds all configuration for a Kasoku node
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

// DefaultNodeConfig returns a NodeConfig with sensible defaults
func DefaultNodeConfig() NodeConfig {
	return NodeConfig{
		N:              3,
		W:              2,
		R:              2,
		GossipInterval: time.Second,
	}
}

// Node is a single Kasoku cluster member
type Node struct {
	cfg            NodeConfig
	engine         *lsmengine.LSMEngine
	ring           *ring.Ring
	members        *MemberList
	hints          *HintStore
	cluster        *Cluster
	httpSrv        *server.Server
	versionCounter versionCounter
	logger         *slog.Logger
	done           chan struct{} // shutdown signal
	wg             sync.WaitGroup
}

// NewNode creates and initializes a new Kasoku node
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
	engine, err := lsmengine.NewLSMEngine(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("open engine: %w", err)
	}

	r := ring.New(ring.DefaultVNodes)
	r.AddNode(cfg.NodeID) // add yourself first

	n := &Node{
		cfg:     cfg,
		engine:  engine,
		ring:    r,
		members: NewMemberList(cfg.NodeID),
		hints:   NewHintStore(),
		logger:  slog.Default(),
		done:    make(chan struct{}),
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
	})

	// Build the HTTP server and wire it to this node
	n.httpSrv = server.New(n)

	return n, nil
}

// Start launches the node — HTTP server + background goroutines
func (n *Node) Start() error {
	// Join existing cluster nodes
	for _, seed := range n.cfg.Seeds {
		if err := n.joinSeed(seed); err != nil {
			n.logger.Warn("could not join seed", "addr", seed, "error", err)
		}
	}

	// Start background goroutines
	n.wg.Add(3)
	go n.gossipLoop()
	go n.antiEntropyLoop()
	go n.hintDeliveryLoop()

	// Start HTTP server (blocks until context is cancelled)
	addr := n.cfg.HTTPAddr
	if addr == "" {
		addr = ":8080"
	}

	n.logger.Info("starting HTTP server", "addr", addr)
	srv := &http.Server{
		Addr:    addr,
		Handler: n.httpSrv.Routes(),
	}

	// Run server in a goroutine so Start returns
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			n.logger.Error("HTTP server error", "error", err)
		}
	}()

	// Wait for shutdown signal
	<-n.done
	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)

	return nil
}

// Stop signals all goroutines to stop and waits for them
func (n *Node) Stop() {
	close(n.done) // signal all goroutines to stop
	n.wg.Wait()   // wait for them to finish
	n.engine.Close()
}

// --- Node methods exposed to HTTP handlers ---

// Scan returns all keys with the given prefix
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

// GetRing returns the consistent hashing ring
func (n *Node) GetRing() *ring.Ring {
	return n.ring
}

// GetMembers returns the current gossip member list
func (n *Node) GetMembers() []string {
	return n.members.Members()
}

// GetNodeID returns this node's ID
func (n *Node) GetNodeID() string {
	return n.cfg.NodeID
}

// GetStatus returns a map of node status information
func (n *Node) GetStatus() map[string]any {
	return map[string]any{
		"node_id":       n.cfg.NodeID,
		"http_addr":     n.cfg.HTTPAddr,
		"ring_nodes":    n.ring.NodeCount(),
		"members":       n.members.MemberCount(),
		"replication":   n.cfg.N,
		"write_quorum":  n.cfg.W,
		"read_quorum":   n.cfg.R,
		"pending_hints": n.hints.PendingCount(),
	}
}

// HandleReplicate is called when another node asks us to replicate data
func (n *Node) HandleReplicate(ctx context.Context, key string, value []byte) error {
	return n.engine.Put(key, value)
}

// HandleReplicateGet is called when another node reads from us
func (n *Node) HandleReplicateGet(ctx context.Context, key string) ([]byte, bool, error) {
	entry, err := n.engine.Get(key)
	if err != nil {
		return nil, false, err
	}
	return entry.Value, true, nil
}

// HandleReplicateGetEntry returns the full Entry for remote get (includes version/tombstone)
func (n *Node) HandleReplicateGetEntry(ctx context.Context, key string) (storage.Entry, error) {
	return n.engine.Get(key)
}

// HandleReplicateDelete is called when another node asks us to delete data
func (n *Node) HandleReplicateDelete(ctx context.Context, key string) (bool, error) {
	err := n.engine.Delete(key)
	return err == nil, err
}

// HandleGossip processes an incoming gossip message and returns our view
func (n *Node) HandleGossip(remoteMembers []string) []string {
	return n.members.Merge(remoteMembers)
}

// HandleHint stores a hinted handoff entry
func (n *Node) HandleHint(key string, value []byte, targetNode string) error {
	return n.hints.Store(key, value, targetNode)
}

// HandleMerkle returns a merkle tree summary for anti-entropy
func (n *Node) HandleMerkle() ([]byte, error) {
	// Placeholder — full merkle tree implementation comes later
	return []byte("merkle-summary-placeholder"), nil
}

// --- Background goroutines ---

// joinSeed contacts a seed node to learn about the cluster
func (n *Node) joinSeed(seedAddr string) error {
	n.logger.Info("joining seed", "addr", seedAddr)

	// Use the seed address as both node ID and address
	n.members.AddMember(seedAddr)
	n.ring.AddNode(seedAddr)
	n.cluster.AddPeer(seedAddr, seedAddr)

	n.logger.Info("joined seed successfully", "addr", seedAddr)
	return nil
}

// gossipLoop periodically exchanges membership info with peers
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
			client := rpc.NewClient(peer)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			ourMembers := n.members.Members()
			theirMembers, err := client.Gossip(ctx, ourMembers)
			cancel()
			if err != nil {
				n.logger.Debug("gossip failed", "peer", peer, "error", err)
				continue
			}
			n.members.Merge(theirMembers)
			n.logger.Debug("gossip completed", "peer", peer, "remote_members", len(theirMembers))
		case <-n.done:
			return
		}
	}
}

// antiEntropyLoop periodically syncs data with peers
func (n *Node) antiEntropyLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			peers := n.members.Members()
			for _, peer := range peers {
				if peer == n.cfg.NodeID {
					continue
				}
				// Sync with peer — placeholder for full merkle-based sync
				n.logger.Debug("anti-entropy sync", "peer", peer)
			}
		case <-n.done:
			return
		}
	}
}

// hintDeliveryLoop periodically retries delivering hinted handoffs
func (n *Node) hintDeliveryLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.hints.RetryFailed(func(targetNode string, key string, value []byte) error {
				// Try to deliver the hint to the target node
				client := rpc.NewClient(targetNode)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return client.ReplicatedPut(ctx, key, value)
			})
		case <-n.done:
			return
		}
	}
}
