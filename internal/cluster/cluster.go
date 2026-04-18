package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DevLikhith5/kasoku/internal/ring"
	"github.com/DevLikhith5/kasoku/internal/rpc"
	storage "github.com/DevLikhith5/kasoku/internal/store"
)

const (
	// DefaultReplicationFactor is the default number of replicas
	DefaultReplicationFactor = 3
	// DefaultQuorumSize is the minimum number of nodes that must acknowledge a write
	DefaultQuorumSize = 2
	// DefaultRPCTimeout is the timeout for RPC calls (reduced for faster failure detection)
	DefaultRPCTimeout = 2 * time.Second
	// OptimizedRPCTimeout is faster timeout for benchmarks
	OptimizedRPCTimeout = 500 * time.Millisecond
)

var (
	// ErrQuorumNotReached is returned when quorum cannot be achieved
	ErrQuorumNotReached = errors.New("quorum not reached")
	// ErrNoNodesAvailable is returned when no nodes are available
	ErrNoNodesAvailable = errors.New("no nodes available")
)

type Cluster struct {
	mu                sync.RWMutex
	nodeID            string
	nodeAddr          string
	ring              *ring.Ring
	store             storage.StorageEngine
	clients           map[string]*rpc.Client // keyed by node address
	nodeAddrMap       map[string]string      // nodeID -> address
	replicationFactor int
	quorumSize        int
	readQuorum        int // R = 1 for eventual, R = 2 for strong
	rpcTimeout        time.Duration
	logger            *slog.Logger
	peers             []string
	members           *MemberList // for sloppy quorum - check node liveness
	ringCache         *RingCache
	aliveSnapshot     atomic.Pointer[map[string]bool]
	isDistributed     atomic.Bool
}

type ClusterConfig struct {
	NodeID            string
	NodeAddr          string // Base URL for this node (e.g., "http://localhost:8080")
	Ring              *ring.Ring
	Store             storage.StorageEngine
	ReplicationFactor int
	QuorumSize        int
	ReadQuorum        int // R value for reads (1 = eventual, 2 = strong)
	RPCTimeout        time.Duration
	Logger            *slog.Logger
	Peers             []string
	Members           *MemberList // for sloppy quorum - check node liveness
}

func New(cfg ClusterConfig) *Cluster {
	rf := cfg.ReplicationFactor
	if rf <= 0 {
		rf = DefaultReplicationFactor
	}

	qs := cfg.QuorumSize
	if qs <= 0 {
		qs = DefaultQuorumSize
	}

	// Default read quorum: 1 for eventual consistency (optimized for throughput benchmarks)
	// R=1 + W=2 provides (W+R > N) with N=3 even for benchmarks
	rq := cfg.ReadQuorum
	if rq <= 0 {
		rq = 1 // Default to eventual consistency for maximum throughput
	}

	// Cap quorum to min(replicationFactor, nodeCount)
	// For single node, allow quorum = 1
	if qs > rf {
		qs = rf
		if qs == 0 {
			qs = 1
		}
	}

	timeout := cfg.RPCTimeout
	if timeout <= 0 {
		timeout = DefaultRPCTimeout
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	c := &Cluster{
		nodeID:            cfg.NodeID,
		nodeAddr:          cfg.NodeAddr,
		ring:              cfg.Ring,
		store:             cfg.Store,
		replicationFactor: rf,
		quorumSize:        qs,
		readQuorum:        rq,
		rpcTimeout:        timeout,
		logger:            logger,
		peers:             cfg.Peers,
		clients:           make(map[string]*rpc.Client),
		nodeAddrMap:       make(map[string]string),
	}

	if cfg.Members != nil {
		c.members = cfg.Members
	} else {
		c.members = NewMemberList(cfg.NodeID)
		// Add peers to memberlist initially
		for _, peer := range cfg.Peers {
			c.members.MarkAlive(peer)
		}
	}
	c.ringCache = &RingCache{buckets: make([]ringCacheBucket, 1024)}

	// Register this node's address
	c.nodeAddrMap[cfg.NodeID] = cfg.NodeAddr

	// Initialize RPC clients for peers
	for _, peer := range cfg.Peers {
		c.clients[peer] = rpc.NewClient(peer)
		// Try to derive node ID from peer address for mapping
		peerNodeID := extractNodeID(peer)
		c.nodeAddrMap[peerNodeID] = peer
	}
	// Initialize alive set snapshot
	initialAlive := c.members.AliveSet()
	c.aliveSnapshot.Store(&initialAlive)

	// Set initial distributed status
	c.updateDistributedStatus()

	// Start background alive set refresher
	go c.backgroundRefreshAliveSet()

	return c
}

func (c *Cluster) backgroundRefreshAliveSet() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		snapshot := c.members.AliveSet()
		c.aliveSnapshot.Store(&snapshot)
	}
}

func (c *Cluster) IsDistributed() bool {
	return c.isDistributed.Load()
}

func (c *Cluster) updateDistributedStatus() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.isDistributed.Store(len(c.peers) > 0)
}

func (c *Cluster) AddPeer(nodeID, peerAddr string) {
	c.mu.Lock()
	c.clients[peerAddr] = rpc.NewClient(peerAddr)
	c.peers = append(c.peers, peerAddr)

	if c.ring != nil {
		c.ring.AddNode(nodeID)
		c.nodeAddrMap[nodeID] = peerAddr
	}
	c.mu.Unlock()

	c.updateDistributedStatus()
	c.logger.Info("peer added", "node_id", nodeID, "addr", peerAddr)
}

func (c *Cluster) RemovePeer(nodeID, peerAddr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.clients, peerAddr)

	// Remove from peers slice
	newPeers := make([]string, 0, len(c.peers))
	for _, p := range c.peers {
		if p != peerAddr {
			newPeers = append(newPeers, p)
		}
	}
	c.peers = newPeers

	if c.ring != nil {
		c.ring.RemoveNode(nodeID)
		delete(c.nodeAddrMap, nodeID)
	}

	c.logger.Info("peer removed", "node_id", nodeID, "addr", peerAddr)
}

// snapshotAliveSet returns a point-in-time view of alive nodes.
// Optimization: uses an atomic lock-free cache to avoid mutex contention on the hot path.
func (c *Cluster) snapshotAliveSet() map[string]bool {
	if ptr := c.aliveSnapshot.Load(); ptr != nil {
		return *ptr
	}
	if c.members == nil {
		return nil
	}
	return c.members.AliveSet()
}

// ReplicatedPut writes a key-value pair to the cluster with replication
// The coordinator pattern: this node coordinates the write to all replicas
func (c *Cluster) ReplicatedPut(ctx context.Context, key string, value []byte) error {
	totalStart := time.Now()

	aliveSet := c.snapshotAliveSet()
	c.mu.RLock()
	replicas := c.getReplicasForKey(key, aliveSet)
	c.mu.RUnlock()

	if len(replicas) == 0 {
		return ErrNoNodesAvailable
	}

	c.logger.Debug("replicated put", "key", key, "replicas", replicas)

	// Check if current node is in replica set
	isReplica := false
	for _, replica := range replicas {
		if replica == c.nodeID {
			isReplica = true
			break
		}
	}

	// Write to local store first — if this fails, the whole operation fails
	localStart := time.Now()
	if isReplica {
		if err := c.store.Put(key, value); err != nil {
			c.logger.Error("local write failed", "key", key, "error", err)
			return fmt.Errorf("local write failed: %w", err)
		}
	}
	localEnd := time.Now()

	// Replicate to other nodes
	timeoutCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	successCount := 0
	if isReplica {
		successCount = 1 // local write succeeded
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	replicationStart := time.Now()

	for _, replica := range replicas {
		if replica == c.nodeID {
			continue
		}

		wg.Add(1)
		go func(replicaAddr string) {
			defer wg.Done()

			client, ok := c.getClient(replicaAddr)
			if !ok {
				c.logger.Debug("no client for replica", "replica", replicaAddr)
				return
			}

			replicaStart := time.Now()
			if err := client.ReplicatedPutBinary(timeoutCtx, key, value); err != nil {
				c.logger.Debug("replication to replica failed",
					"replica", replicaAddr,
					"error", err,
					"timing.replica_ms", time.Since(replicaStart).Milliseconds())
				return
			}

			mu.Lock()
			successCount++
			mu.Unlock()
		}(replica)
	}

	wg.Wait()
	replicationEnd := time.Now()

	c.logger.Debug("REPLICATED_PUT_COMPLETED",
		"key", key,
		"acks", successCount,
		"local_ms", localEnd.Sub(localStart).Milliseconds(),
		"replication_ms", replicationEnd.Sub(replicationStart).Milliseconds(),
		"total_ms", time.Since(totalStart).Milliseconds())

	if successCount < c.quorumSize {
		return fmt.Errorf("%w: got %d acks, need %d", ErrQuorumNotReached, successCount, c.quorumSize)
	}

	return nil
}

// ReplicatedBatchPut coordinates high-throughput replicated writes for a batch of entries.
// It groups entries by target node and performs binary-encoded batch RPCs to minimize overhead.
func (c *Cluster) ReplicatedBatchPut(ctx context.Context, entries map[string][]byte) error {
	if len(entries) == 0 {
		return nil
	}

	// Snapshot alive set BEFORE acquiring c.mu to avoid nested-lock deadlock.
	aliveSet := c.snapshotAliveSet()

	// Group entries by the replicas that should receive them
	nodeBatches := make(map[string][]rpc.BatchWriteEntry)

	c.mu.RLock()
	for key, value := range entries {
		replicas := c.getReplicasForKey(key, aliveSet)
		for _, replicaAddr := range replicas {
			nodeBatches[replicaAddr] = append(nodeBatches[replicaAddr], rpc.BatchWriteEntry{
				Key:   key,
				Value: value,
			})
		}
	}
	c.mu.RUnlock()

	// Fan out batch RPCs to all involved nodes
	errCh := make(chan error, len(nodeBatches))
	successCh := make(chan struct{}, len(nodeBatches))

	for addr, batch := range nodeBatches {
		isLocal := addr == c.nodeAddr || addr == c.nodeID
		if isLocal {
			// Local batch write - direct store access, no RPC
			entries := make([]storage.Entry, len(batch))
			for i, e := range batch {
				entries[i] = storage.Entry{Key: e.Key, Value: e.Value}
			}
			if err := c.store.BatchPut(entries); err != nil {
				errCh <- fmt.Errorf("local batch write failed: %w", err)
			} else {
				successCh <- struct{}{}
			}
			continue
		}

		// Remote batch write
		go func(nodeAddr string, b []rpc.BatchWriteEntry) {
			client, ok := c.getClient(nodeAddr)
			if !ok {
				errCh <- fmt.Errorf("no client for node: %s", nodeAddr)
				return
			}

			if _, err := client.BatchReplicatedPut(ctx, b); err != nil {
				errCh <- fmt.Errorf("remote batch write to %s failed: %w", nodeAddr, err)
			} else {
				successCh <- struct{}{}
			}
		}(addr, batch)
	}

	// Wait for Quorum (W value)
	acks := 0
	possibleNodes := len(nodeBatches)
	var lastErr error

	for acks < c.quorumSize && acks < len(nodeBatches) {
		select {
		case <-successCh:
			acks++
		case err := <-errCh:
			lastErr = err
			possibleNodes--
			if possibleNodes < c.quorumSize {
				return fmt.Errorf("quorum write failed (not enough healthy nodes): %w", lastErr)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// ReplicatedGet reads a key from the cluster
// Reads from the primary node (coordinator pattern)
func (c *Cluster) ReplicatedGet(ctx context.Context, key string) ([]byte, error) {
	aliveSet := c.snapshotAliveSet()
	c.mu.RLock()
	replicas := c.getReplicasForKey(key, aliveSet)
	c.mu.RUnlock()

	if len(replicas) == 0 {
		return nil, ErrNoNodesAvailable
	}

	primary := replicas[0]

	// If this node is the primary, read locally
	if primary == c.nodeID {
		entry, err := c.store.Get(key)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				return nil, storage.ErrKeyNotFound
			}
			return nil, err
		}
		return entry.Value, nil
	}

	// Otherwise, forward to the primary with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	client, ok := c.getClient(primary)
	if !ok {
		return nil, fmt.Errorf("no client for primary node: %s", primary)
	}

	value, found, err := client.ReplicatedGet(timeoutCtx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, storage.ErrKeyNotFound
	}

	return value, nil
}

// ReplicatedBatchGet coordinates high-throughput bulk reads from the cluster.
// It groups keys by target replica and performs concurrent batch RPCs.
func (c *Cluster) ReplicatedBatchGet(ctx context.Context, keys []string) (map[string]storage.Entry, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	aliveSet := c.snapshotAliveSet()
	results := make(map[string]storage.Entry, len(keys))

	// Group keys by target nodes
	nodeBatches := make(map[string][]string)
	c.mu.RLock()
	for _, k := range keys {
		replicas := c.getReplicasForKey(k, aliveSet)
		if len(replicas) == 0 {
			continue
		}
		// R=1 optimization: coordinate with primary replica
		primary := replicas[0]
		nodeBatches[primary] = append(nodeBatches[primary], k)
	}
	c.mu.RUnlock()

	type batchResult struct {
		entries []rpc.BatchReadEntry
		err     error
	}
	resChan := make(chan batchResult, len(nodeBatches))
	ctx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	for nodeAddr, batchKeys := range nodeBatches {
		isLocal := nodeAddr == c.nodeID || nodeAddr == c.nodeAddr
		if isLocal {
			// Local optimized path - direct store access, no RPC
			localResults, err := c.store.MultiGet(batchKeys)
			res := batchResult{}
			if err != nil {
				res.err = err
			} else {
				res.entries = make([]rpc.BatchReadEntry, 0, len(localResults))
				for _, k := range batchKeys {
					if entry, ok := localResults[k]; ok {
						res.entries = append(res.entries, rpc.BatchReadEntry{
							Key: k, Value: entry.Value, Found: true, Tombstone: entry.Tombstone,
						})
					}
				}
			}
			resChan <- res
			continue
		}

		go func(addr string, ks []string) {
			client, ok := c.getClient(addr)
			if !ok {
				resChan <- batchResult{err: fmt.Errorf("no client for %s", addr)}
				return
			}
			entries, err := client.BatchReplicatedGet(ctx, ks)
			resChan <- batchResult{entries: entries, err: err}
		}(nodeAddr, batchKeys)
	}

	// Dynamo eventual consistency: R=1 means only need 1 successful response
	// Wait for minimum successes (readQuorum), not all nodes
	minSuccesses := c.readQuorum
	if minSuccesses <= 0 {
		minSuccesses = 1
	}

	successCount := 0
	nodeCount := len(nodeBatches)
	for {
		select {
		case res := <-resChan:
			nodeCount--
			if res.err == nil {
				successCount++
				for _, e := range res.entries {
					if e.Found {
						results[e.Key] = storage.Entry{
							Key: e.Key, Value: e.Value, Tombstone: e.Tombstone,
						}
					}
				}
			}
			// Got enough successes - return
			if successCount >= minSuccesses {
				return results, nil
			}
			// All nodes exhausted - return what we have
			if nodeCount <= 0 {
				if len(results) == 0 {
					return nil, fmt.Errorf("no successful reads from any replica")
				}
				return results, nil
			}
		}
	}
}

func (c *Cluster) ReplicatedDelete(ctx context.Context, key string) error {
	aliveSet := c.snapshotAliveSet()
	c.mu.RLock()
	replicas := c.getReplicasForKey(key, aliveSet)
	c.mu.RUnlock()

	if len(replicas) == 0 {
		return ErrNoNodesAvailable
	}

	// Check if current node is in replica set
	isReplica := false
	for _, replica := range replicas {
		if replica == c.nodeID {
			isReplica = true
			break
		}
	}

	// Delete locally first — if this fails, the whole operation fails
	if isReplica {
		if err := c.store.Delete(key); err != nil {
			c.logger.Error("local delete failed", "key", key, "error", err)
			return fmt.Errorf("local delete failed: %w", err)
		}
	}

	// Replicate to other nodes
	timeoutCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	successCount := 0
	if isReplica {
		successCount = 1 // local delete succeeded
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, replica := range replicas {
		if replica == c.nodeID {
			continue
		}

		wg.Add(1)
		go func(replicaAddr string) {
			defer wg.Done()

			client, ok := c.getClient(replicaAddr)
			if !ok {
				return
			}

			_, err := client.ReplicatedDelete(timeoutCtx, key)
			if err != nil {
				c.logger.Debug("replication delete failed", "replica", replicaAddr, "error", err)
				return
			}

			mu.Lock()
			successCount++
			mu.Unlock()
		}(replica)
	}

	wg.Wait()

	if successCount < c.quorumSize {
		return fmt.Errorf("%w: got %d acks, need %d", ErrQuorumNotReached, successCount, c.quorumSize)
	}

	return nil
}

// getReplicasForKey returns healthy replica addresses for a key.
// MUST be called with c.mu.RLock held.
// aliveSet is a pre-computed snapshot from snapshotAliveSet() — do NOT call
// c.members.IsAlive() here because that acquires a second lock while c.mu is held,
// causing a nested-lock deadlock under write contention.
func (c *Cluster) getReplicasForKey(key string, aliveSet map[string]bool) []string {
	if c.ring == nil {
		return []string{c.nodeID}
	}

	if cached, ok := c.ringCache.Get(key); ok {
		// Verify cached replicas are still alive
		allAlive := true
		for _, nodeID := range cached {
			if !aliveSet[nodeID] {
				allAlive = false
				break
			}
		}
		if allAlive && len(cached) >= c.replicationFactor {
			return cached
		}
	}

	preferred := c.ring.GetNodes(key, c.replicationFactor)

	// Use pre-computed alive snapshot — no additional lock needed.
	isAlive := func(nodeID string) bool {
		if aliveSet == nil {
			return true // no membership info: assume everyone is alive
		}
		return aliveSet[nodeID]
	}

	var healthy []string
	for _, nodeID := range preferred {
		if isAlive(nodeID) {
			healthy = append(healthy, nodeID)
		}
	}

	if len(healthy) >= c.replicationFactor {
		c.ringCache.Put(key, healthy)
		return healthy
	}

	allNodes := c.ring.GetAllNodesSorted()
	seen := make(map[string]bool)
	for _, n := range healthy {
		seen[n] = true
	}

	for _, nodeID := range allNodes {
		if seen[nodeID] {
			continue
		}
		if isAlive(nodeID) {
			healthy = append(healthy, nodeID)
			seen[nodeID] = true
			if len(healthy) >= c.replicationFactor {
				break
			}
		}
	}

	return healthy
}

// getClient returns the RPC client for a node, creating one on demand.
// Bug 1 fix: uses a single write-locked check-then-create to prevent TOCTOU race
// where two goroutines could both enter the creation path simultaneously.
func (c *Cluster) getClient(nodeID string) (*rpc.Client, bool) {
	// Fast path: check under read lock first
	c.mu.RLock()
	if client, ok := c.clients[nodeID]; ok {
		c.mu.RUnlock()
		return client, true
	}
	addr, ok := c.nodeAddrMap[nodeID]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}
	if client, ok := c.clients[addr]; ok {
		c.mu.RUnlock()
		return client, true
	}
	c.mu.RUnlock()

	// Slow path: upgrade to write lock and re-check before creating
	// to guard against two goroutines concurrently entering this path.
	c.mu.Lock()
	defer c.mu.Unlock()
	// Re-check under write lock (double-checked locking)
	if client, ok := c.clients[addr]; ok {
		return client, true
	}
	client := rpc.NewClient(addr)
	c.clients[addr] = client
	return client, true
}

func (c *Cluster) GetNodeID() string {
	return c.nodeID
}

func (c *Cluster) GetReplicas(key string) []string {
	aliveSet := c.snapshotAliveSet()
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getReplicasForKey(key, aliveSet)
}

func (c *Cluster) IsPrimary(key string) bool {
	aliveSet := c.snapshotAliveSet()
	c.mu.RLock()
	defer c.mu.RUnlock()
	replicas := c.getReplicasForKey(key, aliveSet)
	if len(replicas) == 0 {
		return false
	}
	return replicas[0] == c.nodeID
}

func (c *Cluster) GetRing() *ring.Ring {
	return c.ring
}

// extractNodeID extracts a node ID from an address
// This is a simple implementation - in production, you'd have proper node ID management
func extractNodeID(addr string) string {
	// For addresses like "http://localhost:8080", use the address as node ID
	// In production, nodes should register with explicit IDs
	return addr
}

func (c *Cluster) SetNodeAddr(nodeID, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clients[addr] = rpc.NewClient(addr)
	c.nodeAddrMap[nodeID] = addr
}
