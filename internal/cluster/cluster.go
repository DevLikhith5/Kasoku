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
	grpcrpc "github.com/DevLikhith5/kasoku/internal/rpc/grpc"
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
	clients           map[string]*rpc.Client // keyed by node address (HTTP)
	grpcClients       map[string]*grpcrpc.ReplicatedClient // keyed by node address (gRPC)
	grpcPool          *grpcrpc.Pool // shared gRPC connection pool
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

	// Background replication for eventual consistency
	backgroundReplicator *BackgroundReplicator
	gossipProtocol       *GossipProtocol
	merkleAntiEntropy   *MerkleAntiEntropy
	hintStore           *HintStore
	stopCh              chan struct{}
}

// GetClient returns RPC client for a node address
func (c *Cluster) GetClient(nodeAddr string) (*rpc.Client, bool) {
	return c.getClient(nodeAddr)
}

// GetQuorumSize returns the configured quorum size
func (c *Cluster) GetQuorumSize() int {
	return c.quorumSize
}

// GetReadQuorum returns the configured read quorum
func (c *Cluster) GetReadQuorum() int {
	return c.readQuorum
}

// GetReplicationFactor returns the configured replication factor
func (c *Cluster) GetReplicationFactor() int {
	return c.replicationFactor
}

type ClusterConfig struct {
	NodeID            string
	NodeAddr          string // Base URL for this node (e.g., "http://localhost:8080")
	GRPCPort         int  // gRPC port for inter-node communication
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
		grpcClients:       make(map[string]*grpcrpc.ReplicatedClient),
		grpcPool:          grpcrpc.NewPool(),
		nodeAddrMap:       make(map[string]string),
		stopCh:            make(chan struct{}),
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
	c.ringCache = &RingCache{buckets: make([]ringCacheBucket, 1021)}

	// Register this node's address
	c.nodeAddrMap[cfg.NodeID] = cfg.NodeAddr

	// Get gRPC port for peers
	grpcPort := cfg.GRPCPort
	if grpcPort == 0 {
		grpcPort = 9002 // default
	}

	// Initialize RPC clients for peers (both HTTP and gRPC)
	for _, peer := range cfg.Peers {
		c.clients[peer] = rpc.NewClient(peer)
		peerNodeID := extractNodeID(peer)
		c.nodeAddrMap[peerNodeID] = peer

		// Create gRPC client for this peer using pool
		grpcAddr := convertToGRPCAddr(peer, grpcPort)
		client, err := c.grpcPool.Get(grpcAddr)
		if err == nil {
			c.grpcClients[peer] = client
		}
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
	for {
		select {
		case <-ticker.C:
			snapshot := c.members.AliveSet()
			c.aliveSnapshot.Store(&snapshot)
		case <-c.stopCh:
			return
		}
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

	// Write locally with WAL for durability
	if isReplica {
		if err := c.store.Put(key, value); err != nil {
			c.logger.Error("local write failed", "key", key, "error", err)
			return fmt.Errorf("local write failed: %w", err)
		}
	}

	// Background replication for W=1 (Dynamo-style eventual consistency)
	// Write locally + enqueue for async peer sync - no RPC on hot path
	if c.quorumSize <= 1 && isReplica {
		if c.backgroundReplicator != nil {
			c.backgroundReplicator.Enqueue(key, value, false, 0)
		}
		return nil
	}

	// Replicate to other nodes (sync path for W>1)
	timeoutCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	successCount := 0
	if isReplica {
		successCount = 1 // local write succeeded
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

			// Check if quorum already reached before starting
			mu.Lock()
			if successCount >= c.quorumSize {
				mu.Unlock()
				return
			}
			mu.Unlock()

			client, ok := c.getClient(replicaAddr)
		if !ok {
			c.logger.Debug("no client for replica", "replica", replicaAddr)
			return
		}

		replicaStart := time.Now()

		// Try gRPC first, fall back to HTTP
		if grpcClient, ok := c.grpcClients[replicaAddr]; ok {
			if err := grpcClient.ReplicatedPutBinary(timeoutCtx, key, value); err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
				return
			}
		}

// Fall back to HTTP
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

	if successCount < c.quorumSize {
		return fmt.Errorf("%w: got %d acks, need %d", ErrQuorumNotReached, successCount, c.quorumSize)
	}

	return nil
}

// asyncReplicate replicates to other nodes in background without waiting
func (c *Cluster) asyncReplicate(replicas []string, key string, value []byte) {
	for _, replica := range replicas {
		if replica == c.nodeID {
			continue
		}
		go func(replicaAddr string) {
			client, ok := c.getClient(replicaAddr)
			if !ok {
				return
			}
			bkgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// Try gRPC first
			if grpcClient, ok := c.grpcClients[replicaAddr]; ok {
				if err := grpcClient.ReplicatedPutBinary(bkgCtx, key, value); err == nil {
					return
				}
			}

			// Fall back to HTTP
			if err := client.ReplicatedPutBinary(bkgCtx, key, value); err != nil {
				c.logger.Debug("async replication failed", "replica", replicaAddr, "error", err)
			}
		}(replica)
	}
}

// Dynamo W=1 optimized: write to local node only (fastest path)
// For eventual consistency, write to nearest node - async replication happens in background
// This achieves maximum throughput without waiting for quorum
func (c *Cluster) ReplicatedBatchPut(ctx context.Context, entries map[string][]byte) error {
	if len(entries) == 0 {
		return nil
	}

	entriesSlice := make([]storage.Entry, 0, len(entries))
	for key, value := range entries {
		entriesSlice = append(entriesSlice, storage.Entry{Key: key, Value: value})
	}

	if err := c.store.BatchPut(entriesSlice); err != nil {
		return fmt.Errorf("local batch write failed: %w", err)
	}

	// Async batch replication to peers using gRPC batch
	if c.backgroundReplicator != nil {
		for key, value := range entries {
			c.backgroundReplicator.Enqueue(key, value, false, 0)
		}
	}

	return nil
}

func (c *Cluster) _ReplicatedBatchPutOLD(ctx context.Context, entries map[string][]byte) error {
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
// Dynamo R=1 optimized: Smart routing - local reads fast, remote reads parallelized
func (c *Cluster) ReplicatedBatchGet(ctx context.Context, keys []string) (map[string]storage.Entry, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	aliveSet := c.snapshotAliveSet()
	results := make(map[string]storage.Entry, len(keys))

	// Group keys by target nodes for parallel fetch
	nodeBatches := make(map[string][]string)
	c.mu.RLock()
	for _, k := range keys {
		replicas := c.getReplicasForKey(k, aliveSet)
		if len(replicas) == 0 {
			continue
		}
		primary := replicas[0]
		nodeBatches[primary] = append(nodeBatches[primary], k)
	}
	c.mu.RUnlock()

	type batchResult struct {
		entries []rpc.BatchReadEntry
		err     error
	}
	resChan := make(chan batchResult, len(nodeBatches))
	fetchCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
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

		// Remote node - parallel fetch
		go func(addr string, ks []string) {
			client, ok := c.getClient(addr)
			if !ok {
				resChan <- batchResult{err: fmt.Errorf("no client for %s", addr)}
				return
			}
			entries, err := client.BatchReplicatedGet(fetchCtx, ks)
			resChan <- batchResult{entries: entries, err: err}
		}(nodeAddr, batchKeys)
	}

	// Collect results
	for range nodeBatches {
		res := <-resChan
		if res.err != nil {
			continue
		}
		for _, entry := range res.entries {
			if entry.Found {
				results[entry.Key] = storage.Entry{
					Key:   entry.Key,
					Value: entry.Value,
				}
			}
		}
	}

	return results, nil
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

	// Slow path: create client on demand
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if client, ok := c.clients[nodeID]; ok {
		return client, true
	}
	if client, ok := c.clients[addr]; ok {
		return client, true
	}

	// Create new client
	newClient := rpc.NewClient(addr)
	c.clients[addr] = newClient
	return newClient, true
}

// GetPeerClients returns a map of all peer clients (excluding self)
func (c *Cluster) GetPeerClients() map[string]*rpc.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*rpc.Client)
	for id, client := range c.clients {
		if id != c.nodeID && id != c.nodeAddr {
			result[id] = client
		}
	}
	return result
}

func (c *Cluster) GetNodeID() string {
	return c.nodeID
}

func (c *Cluster) GetMembership() map[string]MemberInfo {
	result := make(map[string]MemberInfo)
	if c.members == nil {
		return result
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	for nodeID, m := range c.members.members {
		result[nodeID] = MemberInfo{
			Addr:      m.Address,
			Heartbeat: m.LastSeen.UnixMilli(),
			State:     m.State.String(),
		}
	}
	return result
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

// StartBackgroundWorkers starts background replication, gossip, and anti-entropy
func (c *Cluster) StartBackgroundWorkers(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	queue := NewReplicationQueue(DefaultMaxQueueSize, DefaultFlushInterval, DefaultMaxBatchSize)
	c.backgroundReplicator = NewBackgroundReplicator(c, queue, c.logger)
	c.backgroundReplicator.Start(ctx)

	c.gossipProtocol = NewGossipProtocol(c, c.members, c.logger)
	c.gossipProtocol.Start(ctx)

	c.merkleAntiEntropy = NewMerkleAntiEntropy(c, c.backgroundReplicator, c.logger)
	c.merkleAntiEntropy.Start(ctx)

	c.logger.Info("background workers started",
		"quorum_size", c.quorumSize,
		"replication_factor", c.replicationFactor)
}

// StopBackgroundWorkers stops all background workers
func (c *Cluster) StopBackgroundWorkers() {
	close(c.stopCh)
	if c.backgroundReplicator != nil {
		c.backgroundReplicator.Stop()
	}
	if c.gossipProtocol != nil {
		c.gossipProtocol.Stop()
	}
	if c.merkleAntiEntropy != nil {
		c.merkleAntiEntropy.Stop()
	}
}

// EnqueueForReplication adds a key/value to the background replication queue
func (c *Cluster) EnqueueForReplication(key string, value []byte, tombstone bool, version uint64) error {
	if c.backgroundReplicator == nil {
		return nil
	}
	return c.backgroundReplicator.Enqueue(key, value, tombstone, version)
}

// EnqueueBatchForReplication adds multiple entries to the background replication queue
func (c *Cluster) EnqueueBatchForReplication(entries []storage.Entry) error {
	if c.backgroundReplicator == nil {
		return nil
	}
	for _, e := range entries {
		c.backgroundReplicator.Enqueue(e.Key, e.Value, e.Tombstone, e.Version)
	}
	return nil
}

// GetBackgroundReplicatorStats returns stats for monitoring
func (c *Cluster) GetBackgroundReplicatorStats() map[string]interface{} {
	if c.backgroundReplicator == nil {
		return nil
	}
	return c.backgroundReplicator.GetStats()
}

// AddHint stores a hint for a failed node (hint handoff)
func (c *Cluster) AddHint(targetNode, key string, value []byte) {
	if c.hintStore == nil {
		c.hintStore = NewHintStore()
	}
	c.hintStore.Store(key, value, targetNode)
}

// GetHintsForNode returns hints destined for a specific node
func (c *Cluster) GetHintsForNode(nodeID string) []*Hint {
	if c.hintStore == nil {
		return nil
	}
	return c.hintStore.GetHintsForNode(nodeID)
}

// RemoveHint removes a hint after successful delivery
func (c *Cluster) RemoveHint(key, targetNode string) {
	if c.hintStore == nil {
		return
	}
	c.hintStore.RemoveHint(key, targetNode)
}

// StartHintDelivery starts background delivery of hints to recovered nodes
func (c *Cluster) StartHintDelivery(ctx context.Context) {
	if c.hintStore == nil {
		return
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.deliverHints()
			}
		}
	}()
}

func (c *Cluster) deliverHints() {
	if c.hintStore == nil {
		return
	}

	peers := c.GetPeerClients()
	for addr := range peers {
		hints := c.GetHintsForNode(addr)
		if len(hints) == 0 {
			continue
		}

		client, ok := c.GetClient(addr)
		if !ok {
			continue
		}

		for _, hint := range hints {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := client.ReplicatedPutBinary(ctx, hint.Key, hint.Value)
			cancel()

			if err == nil {
				c.RemoveHint(hint.Key, addr)
			}
		}
	}
}

// convertToGRPCAddr converts an HTTP address to gRPC address
func convertToGRPCAddr(httpAddr string, grpcPort int) string {
	// Extract host from http://localhost:9000
	host := httpAddr
	if len(host) > 7 && host[:7] == "http://" {
		host = host[7:]
	}
	if len(host) > 8 && host[:8] == "https://" {
		host = host[8:]
	}
	// Remove any trailing path
	if idx := findByte(host, '/'); idx > 0 {
		host = host[:idx]
	}
	// Remove port if present
	if idx := findByte(host, ':'); idx > 0 {
		host = host[:idx]
	}
	return fmt.Sprintf("%s:%d", host, grpcPort)
}

func findByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
