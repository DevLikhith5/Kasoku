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
	clients           map[string]*rpc.Client
	replicationFactor int
	quorumSize        int
	rpcTimeout        time.Duration
	logger            *slog.Logger
	peers             []string
}

// Config holds cluster configuration
type Config struct {
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
func New(cfg Config) *Cluster {
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
	}

	// Initialize RPC clients for peers
	for _, peer := range cfg.Peers {
		c.clients[peer] = rpc.NewClient(peer)
	}

	return c
}

// AddPeer adds a new peer to the cluster
func (c *Cluster) AddPeer(peerAddr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clients[peerAddr] = rpc.NewClient(peerAddr)
	c.peers = append(c.peers, peerAddr)

	if c.ring != nil {
		// Extract node ID from peer address (assumes format like "http://localhost:8080")
		nodeID := extractNodeID(peerAddr)
		c.ring.AddNode(nodeID)
	}

	c.logger.Info("peer added", "peer", peerAddr)
}

// RemovePeer removes a peer from the cluster
func (c *Cluster) RemovePeer(peerAddr string) {
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
		nodeID := extractNodeID(peerAddr)
		c.ring.RemoveNode(nodeID)
	}

	c.logger.Info("peer removed", "peer", peerAddr)
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

	// Determine if this node is the primary (coordinator)
	isPrimary := replicas[0] == c.nodeID

	c.logger.Debug("replicated put", "key", key, "replicas", replicas, "is_primary", isPrimary)

	// Write to local store if this node is a replica
	successCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Check if current node is in replica set
	isReplica := false
	for _, replica := range replicas {
		if replica == c.nodeID {
			isReplica = true
			break
		}
	}

	if isReplica {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.store.Put(key, value); err != nil {
				c.logger.Error("local write failed", "key", key, "error", err)
				return
			}
			mu.Lock()
			successCount++
			mu.Unlock()
		}()
	}

	// Replicate to other nodes
	timeoutCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

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
				return nil, nil
			}
			return nil, err
		}
		return entry.Value, nil
	}

	// Otherwise, forward to the primary
	client, ok := c.getClient(primary)
	if !ok {
		return nil, fmt.Errorf("no client for primary node: %s", primary)
	}

	value, found, err := client.ReplicatedGet(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
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

	successCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Check if current node is in replica set
	isReplica := false
	for _, replica := range replicas {
		if replica == c.nodeID {
			isReplica = true
			break
		}
	}

	if isReplica {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.store.Delete(key)
			if err != nil {
				c.logger.Error("local delete failed", "key", key, "error", err)
				return
			}
			mu.Lock()
			successCount++
			mu.Unlock()
		}()
	}

	// Replicate to other nodes
	timeoutCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

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
	// Try to find client by node ID (peer address)
	if client, ok := c.clients[nodeID]; ok {
		return client, true
	}

	// If not found, the nodeID might be just an identifier
	// In a real implementation, you'd have a nodeID -> address mapping
	return nil, false
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
}
