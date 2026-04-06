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

// Cluster manages the distributed cluster state and replication
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
	rpcTimeout        time.Duration
	logger            *slog.Logger
	peers             []string
}

// ClusterConfig holds cluster configuration
type ClusterConfig struct {
	NodeID            string
	NodeAddr          string // Base URL for this node (e.g., "http://localhost:8080")
	Ring              *ring.Ring
	Store             storage.StorageEngine
	ReplicationFactor int
	QuorumSize        int
	RPCTimeout        time.Duration
	Logger            *slog.Logger
	Peers             []string
}

// New creates a new Cluster instance
func New(cfg ClusterConfig) *Cluster {
	rf := cfg.ReplicationFactor
	if rf <= 0 {
		rf = DefaultReplicationFactor
	}

	qs := cfg.QuorumSize
	if qs <= 0 {
		qs = DefaultQuorumSize
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

// AddPeer adds a new peer to the cluster with an explicit node ID
func (c *Cluster) AddPeer(nodeID, peerAddr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clients[peerAddr] = rpc.NewClient(peerAddr)
	c.peers = append(c.peers, peerAddr)
	c.nodeAddrMap[nodeID] = peerAddr

	if c.ring != nil {
		c.ring.AddNode(nodeID)
	}

	c.logger.Info("peer added", "node_id", nodeID, "addr", peerAddr)
}

// RemovePeer removes a peer from the cluster
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
	}
	delete(c.nodeAddrMap, nodeID)

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

// ReplicatedDelete deletes a key from the cluster with replication
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

// getReplicasForKey returns the replica nodes for a given key
func (c *Cluster) getReplicasForKey(key string) []string {
	if c.ring == nil {
		return []string{c.nodeID}
	}
	return c.ring.GetNodes(key, c.replicationFactor)
}

// getClient returns the RPC client for a node
func (c *Cluster) getClient(nodeID string) (*rpc.Client, bool) {
	// First try direct lookup by node ID (in case nodeID == address)
	c.mu.RLock()
	if client, ok := c.clients[nodeID]; ok {
		c.mu.RUnlock()
		return client, true
	}

	// Resolve nodeID -> address -> client
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

	// Client doesn't exist yet — create one on demand
	client := rpc.NewClient(addr)
	c.mu.Lock()
	c.clients[addr] = client
	c.mu.Unlock()
	return client, true
}

// GetNodeID returns this node's ID
func (c *Cluster) GetNodeID() string {
	return c.nodeID
}

// GetReplicas returns the current replica set for a key
func (c *Cluster) GetReplicas(key string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getReplicasForKey(key)
}

// IsPrimary checks if this node is the primary for a key
func (c *Cluster) IsPrimary(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	replicas := c.getReplicasForKey(key)
	if len(replicas) == 0 {
		return false
	}
	return replicas[0] == c.nodeID
}

// GetRing returns the consistent hashing ring
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

// SetNodeAddr sets the address for a node (for client mapping)
func (c *Cluster) SetNodeAddr(nodeID, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clients[addr] = rpc.NewClient(addr)
	c.nodeAddrMap[nodeID] = addr
}
