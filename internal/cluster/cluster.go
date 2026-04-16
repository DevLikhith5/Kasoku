package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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
	// DefaultRPCTimeout is the timeout for RPC calls
	DefaultRPCTimeout = 5 * time.Second
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

	// Default read quorum: 1 for eventual consistency (faster reads), 2 for strong
	rq := cfg.ReadQuorum
	if rq <= 0 {
		// If R=1 and W=2, still provides some consistency (W+R > N with N=3, 2+1=3)
		// But for strong consistency, use R=2
		rq = 2 // Default to strong consistency
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

	// Register this node's address
	c.nodeAddrMap[cfg.NodeID] = cfg.NodeAddr

	// Initialize RPC clients for peers
	for _, peer := range cfg.Peers {
		c.clients[peer] = rpc.NewClient(peer)
		// Try to derive node ID from peer address for mapping
		peerNodeID := extractNodeID(peer)
		c.nodeAddrMap[peerNodeID] = peer
	}

	return c
}

func (c *Cluster) AddPeer(nodeID, peerAddr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clients[peerAddr] = rpc.NewClient(peerAddr)
	c.peers = append(c.peers, peerAddr)

	if c.ring != nil {
		c.ring.AddNode(nodeID)
		// Register the mapping so ring lookups can resolve to this address
		c.nodeAddrMap[nodeID] = peerAddr
	}

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

// ReplicatedPut writes a key-value pair to the cluster with replication
// The coordinator pattern: this node coordinates the write to all replicas
func (c *Cluster) ReplicatedPut(ctx context.Context, key string, value []byte) error {
	c.mu.RLock()
	replicas := c.getReplicasForKey(key)
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
	if isReplica {
		if err := c.store.Put(key, value); err != nil {
			c.logger.Error("local write failed", "key", key, "error", err)
			return fmt.Errorf("local write failed: %w", err)
		}
	}

	// Replicate to other nodes
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

			client, ok := c.getClient(replicaAddr)
			if !ok {
				c.logger.Debug("no client for replica", "replica", replicaAddr)
				return
			}

			if err := client.ReplicatedPut(timeoutCtx, key, value); err != nil {
				c.logger.Debug("replication to replica failed", "replica", replicaAddr, "error", err)
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

	c.logger.Debug("replicated put completed", "key", key, "acks", successCount)
	return nil
}

// ReplicatedBatchPut coordinates high-throughput replicated writes for a batch of entries.
// It groups entries by target node and performs binary-encoded batch RPCs to minimize overhead.
func (c *Cluster) ReplicatedBatchPut(ctx context.Context, entries map[string][]byte) error {
	if len(entries) == 0 {
		return nil
	}

	// Group entries by the replicas that should receive them
	nodeBatches := make(map[string][]rpc.BatchWriteEntry)

	c.mu.RLock()
	for key, value := range entries {
		replicas := c.getReplicasForKey(key)
		for _, replicaAddr := range replicas {
			nodeBatches[replicaAddr] = append(nodeBatches[replicaAddr], rpc.BatchWriteEntry{
				Key:   key,
				Value: value,
			})
		}
	}
	c.mu.RUnlock()

	// Fan out batch RPCs to all involved nodes
	var wg sync.WaitGroup
	errCh := make(chan error, len(nodeBatches))

	for addr, batch := range nodeBatches {
		if addr == c.nodeAddr {
			// Local batch write
			wg.Add(1)
			go func(b []rpc.BatchWriteEntry) {
				defer wg.Done()
				for _, e := range b {
					if err := c.store.Put(e.Key, e.Value); err != nil {
						errCh <- fmt.Errorf("local batch write failed: %w", err)
						return
					}
				}
			}(batch)
			continue
		}

		// Remote batch write
		wg.Add(1)
		go func(nodeAddr string, b []rpc.BatchWriteEntry) {
			defer wg.Done()
			client, ok := c.getClient(nodeAddr)
			if !ok {
				errCh <- fmt.Errorf("no client for node: %s", nodeAddr)
				return
			}

			if _, err := client.BatchReplicatedPut(ctx, b); err != nil {
				errCh <- fmt.Errorf("remote batch write to %s failed: %w", nodeAddr, err)
			}
		}(addr, batch)
	}

	wg.Wait()
	close(errCh)

	// Return first error encountered
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// ReplicatedGet reads a key from the cluster
// Reads from the primary node (coordinator pattern)
func (c *Cluster) ReplicatedGet(ctx context.Context, key string) ([]byte, error) {
	c.mu.RLock()
	replicas := c.getReplicasForKey(key)
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

func (c *Cluster) ReplicatedDelete(ctx context.Context, key string) error {
	c.mu.RLock()
	replicas := c.getReplicasForKey(key)
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

func (c *Cluster) getReplicasForKey(key string) []string {
	if c.ring == nil {
		return []string{c.nodeID}
	}
	return c.ring.GetNodes(key, c.replicationFactor)
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getReplicasForKey(key)
}

func (c *Cluster) IsPrimary(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	replicas := c.getReplicasForKey(key)
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
