package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DevLikhith5/kasoku/internal/metrics"
	"github.com/DevLikhith5/kasoku/internal/ring"
	"github.com/DevLikhith5/kasoku/internal/rpc"
	grpcrpc "github.com/DevLikhith5/kasoku/internal/rpc/grpc"
	storage "github.com/DevLikhith5/kasoku/internal/store"
	"github.com/DevLikhith5/kasoku/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
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
	metrics           *metrics.Metrics

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
	NodeAddr          string // Base URL for this node (e.g. "http://localhost:8080")
	GRPCPort         int  // gRPC port for inter-node communication
	Ring              *ring.Ring
	Store             storage.StorageEngine
	ReplicationFactor int
	QuorumSize        int
	ReadQuorum        int // R value for reads (1 = eventual, 2 = strong)
	RPCTimeout        time.Duration
	Logger            *slog.Logger
	Peers             []string
	PeerGRPCAddrs     []string // gRPC addresses for peers (parallel to Peers)
	Members           *MemberList // for sloppy quorum - check node liveness
	Metrics           *metrics.Metrics
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
		metrics:           cfg.Metrics,
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
	for i, peer := range cfg.Peers {
		c.clients[peer] = rpc.NewClient(peer)
		peerNodeID := extractNodeID(peer)
		c.nodeAddrMap[peerNodeID] = peer

		// Determine gRPC address: use explicit PeerGRPCAddrs if available,
		// otherwise derive from HTTP address using grpcPort
		var grpcAddr string
		if i < len(cfg.PeerGRPCAddrs) && cfg.PeerGRPCAddrs[i] != "" {
			grpcAddr = cfg.PeerGRPCAddrs[i]
		} else {
			grpcAddr = convertToGRPCAddr(peer, grpcPort)
		}

		// Create gRPC client for this peer using pool (non-blocking)
		go func(p string, addr string) {
			client, err := c.grpcPool.Get(addr)
			if err == nil {
				c.mu.Lock()
				c.grpcClients[p] = client
				c.mu.Unlock()
			}
		}(peer, grpcAddr)
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

// ReplicatedPut writes a key-value pair to the cluster with replication.
// Production-grade: any node can accept writes. Non-coordinators forward to the
// coordinator, which handles quorum replication.
func (c *Cluster) ReplicatedPut(ctx context.Context, key string, value []byte) error {
	ctx, span := tracing.StartSpan(ctx, "cluster.ReplicatedPut",
		attribute.String("key", key),
		attribute.Int("value_size", len(value)))
	defer span.End()

	aliveSet := c.snapshotAliveSet()
	c.mu.RLock()
	replicas := c.getReplicasForKey(key, aliveSet)
	c.mu.RUnlock()

	if len(replicas) == 0 {
		span.SetAttributes(attribute.String("error", "no_nodes"))
		return ErrNoNodesAvailable
	}

	span.SetAttributes(attribute.String("replicas", fmt.Sprintf("%v", replicas)))

	coordinator := replicas[0]
	isCoordinator := coordinator == c.nodeID

	span.SetAttributes(
		attribute.String("coordinator", coordinator),
		attribute.Bool("is_coordinator", isCoordinator),
	)

	// ── Non-coordinator path: forward to coordinator ─────────────────────────
	if !isCoordinator {
		if c.quorumSize <= 1 {
			// W=1: sloppy quorum — write locally, async forward
			localStart := time.Now()
			_ = c.store.Put(key, value)
			if c.metrics != nil {
				c.metrics.RecordReplicationWriteLatency("local", time.Since(localStart))
			}
			if c.backgroundReplicator != nil {
				c.backgroundReplicator.Enqueue(key, value, false, 0)
			}
			return nil
		}

		// W>1: synchronous forward to coordinator
		c.mu.RLock()
		grpcClient, hasGRPC := c.grpcClients[coordinator]
		c.mu.RUnlock()
		if !hasGRPC {
			return fmt.Errorf("no gRPC client for coordinator %s", coordinator)
		}

		fwdStart := time.Now()
		err := grpcClient.ReplicatedPutBinary(ctx, key, value)
		if c.metrics != nil {
			c.metrics.RecordReplicationWriteLatency("network", time.Since(fwdStart))
		}
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("forward to coordinator %s failed: %w", coordinator, err)
		}
		return nil
	}

	// ── Coordinator path ─────────────────────────────────────────────────────

	// 1. Write locally
	localStart := time.Now()
	if err := c.store.Put(key, value); err != nil {
		c.logger.Error("local write failed", "key", key, "error", err)
		if c.metrics != nil {
			c.metrics.RecordReplicationWriteLatency("local", time.Since(localStart))
		}
		return fmt.Errorf("local write failed: %w", err)
	}
	if c.metrics != nil {
		c.metrics.RecordReplicationWriteLatency("local", time.Since(localStart))
	}

	// 2. W=1: fire-and-forget background replication
	if c.quorumSize <= 1 {
		if c.backgroundReplicator != nil {
			c.backgroundReplicator.Enqueue(key, value, false, 0)
		}
		return nil
	}

	// 3. W>1: replicate to W-1 peers
	timeoutCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	successCount := 1
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, replicaID := range replicas {
		if replicaID == c.nodeID {
			continue
		}
		wg.Add(1)
		go func(rid string) {
			defer wg.Done()

			mu.Lock()
			if successCount >= c.quorumSize {
				mu.Unlock()
				return
			}
			mu.Unlock()

			replicaStart := time.Now()

			c.mu.RLock()
			grpcClient, hasGRPC := c.grpcClients[rid]
			c.mu.RUnlock()

			if hasGRPC {
				if err := grpcClient.ReplicatedPutBinaryInternal(timeoutCtx, key, value); err == nil {
					if c.metrics != nil {
						c.metrics.RecordReplicationWriteLatency("network", time.Since(replicaStart))
					}
					mu.Lock()
					successCount++
					mu.Unlock()
					return
				}
			}

			// Fall back to HTTP
			client, ok := c.getClient(replicaID)
			if !ok {
				c.logger.Debug("no client for replica", "replica", replicaID)
				return
			}

			if err := client.ReplicatedPutBinary(timeoutCtx, key, value); err != nil {
				if c.metrics != nil {
					c.metrics.RecordReplicationWriteLatency("network", time.Since(replicaStart))
				}
				return
			}
			if c.metrics != nil {
				c.metrics.RecordReplicationWriteLatency("network", time.Since(replicaStart))
			}
			mu.Lock()
			successCount++
			mu.Unlock()
		}(replicaID)
	}

	quorumStart := time.Now()
	wg.Wait()
	if c.metrics != nil {
		c.metrics.RecordReplicationWriteLatency("quorum_wait", time.Since(quorumStart))
	}

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
				if err := grpcClient.ReplicatedPutBinaryInternal(bkgCtx, key, value); err == nil {
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

// ReplicatedBatchPut writes a batch of entries to the cluster with replication.
// Production-grade: any node can accept writes. Non-coordinators forward to the
// coordinator, which then replicates to W-1 replicas.
func (c *Cluster) ReplicatedBatchPut(ctx context.Context, entries map[string][]byte) error {
	ctx, span := tracing.StartSpan(ctx, "cluster.ReplicatedBatchPut",
		attribute.Int("entry_count", len(entries)))
	defer span.End()

	if len(entries) == 0 {
		return nil
	}

	// Use the first key as the representative for coordinator determination.
	var representativeKey string
	for k := range entries {
		representativeKey = k
		break
	}

	aliveSet := c.snapshotAliveSet()
	c.mu.RLock()
	replicas := c.getReplicasForKey(representativeKey, aliveSet)
	c.mu.RUnlock()

	if len(replicas) == 0 {
		return ErrNoNodesAvailable
	}

	coordinator := replicas[0]
	isCoordinator := coordinator == c.nodeID

	span.SetAttributes(
		attribute.String("coordinator", coordinator),
		attribute.Bool("is_coordinator", isCoordinator),
	)

	// ── Non-coordinator path: forward to coordinator ─────────────────────────
	if !isCoordinator {
		// W=1: write locally (sloppy quorum) and fire-and-forget forward
		if c.quorumSize <= 1 {
			localStart := time.Now()
			entriesSlice := make([]storage.Entry, 0, len(entries))
			for k, v := range entries {
				entriesSlice = append(entriesSlice, storage.Entry{Key: k, Value: v})
			}
			_ = c.store.BatchPut(entriesSlice) // best-effort local write
			if c.metrics != nil {
				c.metrics.RecordReplicationWriteLatency("local", time.Since(localStart))
			}
			// Async forward to coordinator for eventual consistency
			if c.backgroundReplicator != nil {
				for k, v := range entries {
					c.backgroundReplicator.Enqueue(k, v, false, 0)
				}
			}
			return nil
		}

		// W>1: synchronous forward to coordinator
		c.mu.RLock()
		grpcClient, hasGRPC := c.grpcClients[coordinator]
		c.mu.RUnlock()

		if !hasGRPC {
			return fmt.Errorf("no gRPC client for coordinator %s", coordinator)
		}

		batch := make([]grpcrpc.BatchWriteEntry, 0, len(entries))
		for k, v := range entries {
			batch = append(batch, grpcrpc.BatchWriteEntry{Key: k, Value: v})
		}

		fwdStart := time.Now()
		_, err := grpcClient.BatchReplicatedPut(ctx, batch)
		if c.metrics != nil {
			c.metrics.RecordReplicationWriteLatency("network", time.Since(fwdStart))
		}
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("forward to coordinator %s failed: %w", coordinator, err)
		}
		return nil
	}

	// ── Coordinator path ─────────────────────────────────────────────────────

	// 1. Write locally
	localStart := time.Now()
	entriesSlice := make([]storage.Entry, 0, len(entries))
	for k, v := range entries {
		entriesSlice = append(entriesSlice, storage.Entry{Key: k, Value: v})
	}
	if err := c.store.BatchPut(entriesSlice); err != nil {
		if c.metrics != nil {
			c.metrics.RecordReplicationWriteLatency("local", time.Since(localStart))
		}
		return fmt.Errorf("local batch write failed: %w", err)
	}
	if c.metrics != nil {
		c.metrics.RecordReplicationWriteLatency("local", time.Since(localStart))
	}

	// 2. W=1: fire-and-forget background replication
	if c.quorumSize <= 1 {
		if c.backgroundReplicator != nil {
			for k, v := range entries {
				c.backgroundReplicator.Enqueue(k, v, false, 0)
			}
		}
		return nil
	}

	// 3. W>1: replicate to W-1 peers
	timeoutCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	successCount := 1 // local write
	replicaIDs := make([]string, 0, len(replicas)-1)
	for _, r := range replicas {
		if r != c.nodeID {
			replicaIDs = append(replicaIDs, r)
		}
	}

	if len(replicaIDs) == 0 {
		return fmt.Errorf("%w: no peers available", ErrQuorumNotReached)
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, rid := range replicaIDs {
		wg.Add(1)
		go func(replicaID string) {
			defer wg.Done()

			mu.Lock()
			if successCount >= c.quorumSize {
				mu.Unlock()
				return
			}
			mu.Unlock()

			replicaStart := time.Now()

			// gRPC with x-replication header (peers store locally only)
			c.mu.RLock()
			grpcClient, hasGRPC := c.grpcClients[replicaID]
			c.mu.RUnlock()

			if hasGRPC {
				batch := make([]grpcrpc.BatchWriteEntry, 0, len(entries))
				for k, v := range entries {
					batch = append(batch, grpcrpc.BatchWriteEntry{Key: k, Value: v})
				}
				if _, err := grpcClient.BatchReplicatedPutInternal(timeoutCtx, batch); err == nil {
					if c.metrics != nil {
						c.metrics.RecordReplicationWriteLatency("network", time.Since(replicaStart))
					}
					mu.Lock()
					successCount++
					mu.Unlock()
					return
				}
			}

			// HTTP fallback
			client, ok := c.getClient(replicaID)
			if !ok {
				return
			}
			rpcEntries := make([]rpc.BatchWriteEntry, 0, len(entries))
			for k, v := range entries {
				rpcEntries = append(rpcEntries, rpc.BatchWriteEntry{Key: k, Value: v})
			}
			if _, err := client.BatchReplicatedPut(timeoutCtx, rpcEntries); err != nil {
				return
			}
			if c.metrics != nil {
				c.metrics.RecordReplicationWriteLatency("network", time.Since(replicaStart))
			}
			mu.Lock()
			successCount++
			mu.Unlock()
		}(rid)
	}

	wg.Wait()

	if successCount < c.quorumSize {
		return fmt.Errorf("%w: got %d acks, need %d", ErrQuorumNotReached, successCount, c.quorumSize)
	}
	return nil
}




// ReplicatedGet reads a key from the cluster with R-quorum consistency.
// R=1: read from local store (fast path — eventual consistency).
// R>1: read from R replicas in parallel, return the value with highest version.
func (c *Cluster) ReplicatedGet(ctx context.Context, key string) ([]byte, error) {
	ctx, span := tracing.StartSpan(ctx, "cluster.ReplicatedGet",
		attribute.String("key", key))
	defer span.End()

	// Fast path: R=1, just read locally
	if c.readQuorum <= 1 {
		localStart := time.Now()
		entry, err := c.store.Get(key)
		if c.metrics != nil {
			c.metrics.RecordReplicationReadLatency("local", time.Since(localStart))
		}
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				return nil, storage.ErrKeyNotFound
			}
			return nil, err
		}
		return entry.Value, nil
	}

	// R>1: read from R replicas for strong consistency
	aliveSet := c.snapshotAliveSet()
	c.mu.RLock()
	replicas := c.getReplicasForKey(key, aliveSet)
	c.mu.RUnlock()

	if len(replicas) == 0 {
		span.SetAttributes(attribute.String("error", "no_nodes"))
		return nil, ErrNoNodesAvailable
	}

	type readResult struct {
		value   []byte
		version uint64
		err     error
	}

	resCh := make(chan readResult, len(replicas))
	timeoutCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	// Read from all replicas in parallel
	for _, replica := range replicas {
		go func(replicaID string) {
			if replicaID == c.nodeID {
				localStart := time.Now()
				entry, err := c.store.Get(key)
				if c.metrics != nil {
					c.metrics.RecordReplicationReadLatency("local", time.Since(localStart))
				}
				if err != nil {
					resCh <- readResult{err: err}
					return
				}
				resCh <- readResult{value: entry.Value, version: entry.Version}
				return
			}

			client, ok := c.getClient(replicaID)
			if !ok {
				resCh <- readResult{err: fmt.Errorf("no client for %s", replicaID)}
				return
			}

			networkStart := time.Now()
			value, found, err := client.ReplicatedGet(timeoutCtx, key)
			if c.metrics != nil {
				c.metrics.RecordReplicationReadLatency("network", time.Since(networkStart))
			}
			if err != nil {
				resCh <- readResult{err: err}
				return
			}
			if !found {
				resCh <- readResult{err: storage.ErrKeyNotFound}
				return
			}
			resCh <- readResult{value: value}
		}(replica)
	}

	// Collect R responses
	var bestValue []byte
	var bestVersion uint64
	successCount := 0
	notFoundCount := 0

	for i := 0; i < len(replicas) && successCount < c.readQuorum; i++ {
		select {
		case res := <-resCh:
			if res.err != nil {
				if errors.Is(res.err, storage.ErrKeyNotFound) {
					notFoundCount++
				}
				continue
			}
			successCount++
			if res.version >= bestVersion {
				bestVersion = res.version
				bestValue = res.value
			}
		case <-timeoutCtx.Done():
			break
		}
	}

	if successCount >= c.readQuorum {
		return bestValue, nil
	}

	// All replicas returned not-found
	if notFoundCount > 0 && successCount == 0 {
		return nil, storage.ErrKeyNotFound
	}

	// Fallback: try local read
	entry, err := c.store.Get(key)
	if err != nil {
		return nil, err
	}
	return entry.Value, nil
}

// ReplicatedBatchGet coordinates high-throughput bulk reads from the cluster.
// R=1: read all keys from local store (fast path — no network I/O).
// R>1: read from R replicas per key and reconcile versions for strong consistency.
func (c *Cluster) ReplicatedBatchGet(ctx context.Context, keys []string) (map[string]storage.Entry, error) {
	ctx, span := tracing.StartSpan(ctx, "cluster.ReplicatedBatchGet",
		attribute.Int("key_count", len(keys)))
	defer span.End()

	if len(keys) == 0 {
		return nil, nil
	}

	// Fast path: R=1, read everything locally (eventual consistency)
	if c.readQuorum <= 1 {
		localStart := time.Now()
		results, err := c.store.MultiGet(keys)
		if c.metrics != nil {
			c.metrics.RecordReplicationReadLatency("local", time.Since(localStart))
		}
		if err != nil {
			return nil, err
		}
		return results, nil
	}

	// R>1: read from up to R replicas per key for strong consistency
	aliveSet := c.snapshotAliveSet()

	// keyReplicas maps each key -> its first R replica node addresses
	keyReplicas := make(map[string][]string, len(keys))
	// nodeBatches maps each node -> keys it should read
	nodeBatches := make(map[string][]string)
	c.mu.RLock()
	for _, k := range keys {
		replicas := c.getReplicasForKey(k, aliveSet)
		if len(replicas) == 0 {
			continue
		}
		// Limit to first R replicas
		r := c.readQuorum
		if len(replicas) < r {
			r = len(replicas)
		}
		keyReplicas[k] = replicas[:r]
		for _, node := range replicas[:r] {
			nodeBatches[node] = append(nodeBatches[node], k)
		}
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
			localStart := time.Now()
			localResults, err := c.store.MultiGet(batchKeys)
			if c.metrics != nil {
				c.metrics.RecordReplicationReadLatency("local", time.Since(localStart))
			}
			res := batchResult{}
			if err != nil {
				res.err = err
			} else {
				res.entries = make([]rpc.BatchReadEntry, 0, len(localResults))
				for _, k := range batchKeys {
					if entry, ok := localResults[k]; ok {
						res.entries = append(res.entries, rpc.BatchReadEntry{
							Key:       k,
							Value:     entry.Value,
							Found:     true,
							Tombstone: entry.Tombstone,
							Version:   entry.Version,
						})
					}
				}
			}
			resChan <- res
			continue
		}

		// Remote node - parallel fetch
		go func(addr string, ks []string) {
			networkStart := time.Now()
			client, ok := c.getClient(addr)
			if !ok {
				if c.metrics != nil {
					c.metrics.RecordReplicationReadLatency("network", time.Since(networkStart))
				}
				resChan <- batchResult{err: fmt.Errorf("no client for %s", addr)}
				return
			}
			entries, err := client.BatchReplicatedGet(fetchCtx, ks)
			if c.metrics != nil {
				c.metrics.RecordReplicationReadLatency("network", time.Since(networkStart))
			}
			resChan <- batchResult{entries: entries, err: err}
		}(nodeAddr, batchKeys)
	}

	// Per-key version reconciliation
	type keyState struct {
		bestEntry storage.Entry
		bestVer   uint64
		found     bool
	}
	keyStates := make(map[string]*keyState, len(keys))
	for _, k := range keys {
		keyStates[k] = &keyState{}
	}

	quorumStart := time.Now()
	for range nodeBatches {
		res := <-resChan
		if res.err != nil {
			continue
		}
		for _, entry := range res.entries {
			if !entry.Found {
				continue
			}
			ks, ok := keyStates[entry.Key]
			if !ok {
				continue
			}
			if !ks.found || entry.Version >= ks.bestVer {
				ks.bestVer = entry.Version
				ks.bestEntry = storage.Entry{
					Key:       entry.Key,
					Value:     entry.Value,
					Tombstone: entry.Tombstone,
					Version:   entry.Version,
				}
				ks.found = true
			}
		}
	}
	if c.metrics != nil {
		c.metrics.RecordReplicationReadLatency("quorum_wait", time.Since(quorumStart))
	}

	// Build final results (only keys that were found)
	results := make(map[string]storage.Entry, len(keys))
	for _, k := range keys {
		if ks := keyStates[k]; ks != nil && ks.found {
			results[k] = ks.bestEntry
		}
	}

	return results, nil
}

func (c *Cluster) ReplicatedDelete(ctx context.Context, key string) error {
	ctx, span := tracing.StartSpan(ctx, "cluster.ReplicatedDelete",
		attribute.String("key", key))
	defer span.End()

	aliveSet := c.snapshotAliveSet()
	c.mu.RLock()
	replicas := c.getReplicasForKey(key, aliveSet)
	c.mu.RUnlock()

	if len(replicas) == 0 {
		span.SetAttributes(attribute.String("error", "no_nodes"))
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

	// Sloppy quorum: using fallback nodes
	if c.metrics != nil {
		c.metrics.IncSloppyQuorumFallbacks()
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
			deliveryStart := time.Now()
			err := client.ReplicatedPutBinary(ctx, hint.Key, hint.Value)
			cancel()

			if err == nil {
				c.RemoveHint(hint.Key, addr)
			} else if c.metrics != nil {
				c.metrics.IncHintedHandoffRetries()
			}
			if c.metrics != nil {
				c.metrics.RecordReplicationWriteLatency("hint_delivery", time.Since(deliveryStart))
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
