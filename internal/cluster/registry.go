package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type NodeState int

const (
	NodeStateUnknown NodeState = iota
	NodeStateHealthy
	NodeStateUnhealthy
	NodeStateLeaving
	NodeStateDown
)

func (s NodeState) String() string {
	switch s {
	case NodeStateHealthy:
		return "healthy"
	case NodeStateUnhealthy:
		return "unhealthy"
	case NodeStateLeaving:
		return "leaving"
	case NodeStateDown:
		return "down"
	default:
		return "unknown"
	}
}

type NodeInfo struct {
	NodeID   string
	Address  string
	State    NodeState
	LastSeen time.Time
	AddedAt  time.Time
	Metadata map[string]string
}

type NodeRegistry struct {
	mu              sync.RWMutex
	stopOnce        sync.Once // Bug 5 fix: prevents double-close panic on Stop()
	nodes           map[string]*NodeInfo
	nodeIDByAddr    map[string]string
	healthCheckFunc func(ctx context.Context, addr string) error
	healthInterval  time.Duration
	healthTimeout   time.Duration
	stopCh          chan struct{}
	logger          *slog.Logger
}

func NewNodeRegistry(healthCheckFunc func(ctx context.Context, addr string) error, interval time.Duration, logger *slog.Logger) *NodeRegistry {
	if logger == nil {
		logger = slog.Default()
	}

	return &NodeRegistry{
		nodes:           make(map[string]*NodeInfo),
		nodeIDByAddr:    make(map[string]string),
		healthCheckFunc: healthCheckFunc,
		healthInterval:  interval,
		healthTimeout:   5 * time.Second,
		stopCh:          make(chan struct{}),
		logger:          logger,
	}
}

func (r *NodeRegistry) Register(nodeID, address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; exists {
		r.logger.Debug("node already registered", "node_id", nodeID)
		return
	}

	now := time.Now()
	r.nodes[nodeID] = &NodeInfo{
		NodeID:   nodeID,
		Address:  address,
		State:    NodeStateUnknown,
		LastSeen: now,
		AddedAt:  now,
		Metadata: make(map[string]string),
	}
	r.nodeIDByAddr[address] = nodeID

	r.logger.Info("node registered", "node_id", nodeID, "address", address)
}

func (r *NodeRegistry) Unregister(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	node, exists := r.nodes[nodeID]
	if !exists {
		return
	}

	delete(r.nodeIDByAddr, node.Address)
	delete(r.nodes, nodeID)

	r.logger.Info("node unregistered", "node_id", nodeID)
}

func (r *NodeRegistry) GetNode(nodeID string) (*NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	node, exists := r.nodes[nodeID]
	if !exists {
		return nil, false
	}

	// Return a copy
	nodeCopy := *node
	return &nodeCopy, true
}

func (r *NodeRegistry) GetNodeByAddr(address string) (*NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodeID, exists := r.nodeIDByAddr[address]
	if !exists {
		return nil, false
	}

	node, exists := r.nodes[nodeID]
	if !exists {
		return nil, false
	}

	nodeCopy := *node
	return &nodeCopy, true
}

func (r *NodeRegistry) SetNodeState(nodeID string, state NodeState) {
	r.mu.Lock()
	defer r.mu.Unlock()

	node, exists := r.nodes[nodeID]
	if !exists {
		return
	}

	oldState := node.State
	node.State = state
	node.LastSeen = time.Now()

	if oldState != state {
		r.logger.Info("node state changed", "node_id", nodeID, "old_state", oldState, "new_state", state)
	}
}

func (r *NodeRegistry) GetHealthyNodes() []*NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var healthy []*NodeInfo
	for _, node := range r.nodes {
		if node.State == NodeStateHealthy {
			nodeCopy := *node
			healthy = append(healthy, &nodeCopy)
		}
	}

	return healthy
}

func (r *NodeRegistry) GetAllNodes() []*NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(r.nodes))
	for _, node := range r.nodes {
		nodeCopy := *node
		nodes = append(nodes, &nodeCopy)
	}

	return nodes
}

func (r *NodeRegistry) GetNodeAddress(nodeID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	node, exists := r.nodes[nodeID]
	if !exists {
		return "", false
	}

	return node.Address, true
}

func (r *NodeRegistry) StartHealthChecks() {
	if r.healthCheckFunc == nil {
		r.logger.Warn("no health check function provided, health checks disabled")
		return
	}

	go func() {
		ticker := time.NewTicker(r.healthInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.runHealthChecks()
			case <-r.stopCh:
				r.logger.Info("health checks stopped")
				return
			}
		}
	}()

	r.logger.Info("health checks started", "interval", r.healthInterval)
}

// Stop stops the health check loop.
// Bug 5 fix: uses sync.Once to prevent double-close panic.
func (r *NodeRegistry) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
}

func (r *NodeRegistry) runHealthChecks() {
	r.mu.RLock()
	nodes := make([]*NodeInfo, 0, len(r.nodes))
	for _, node := range r.nodes {
		nodeCopy := *node // take a value copy to avoid holding reference to map entry
		nodes = append(nodes, &nodeCopy)
	}
	r.mu.RUnlock()

	for _, node := range nodes {
		if node.State == NodeStateLeaving || node.State == NodeStateDown {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), r.healthTimeout)
		err := r.healthCheckFunc(ctx, node.Address)
		cancel()

		r.mu.Lock()
		// Bug 11 fix: re-check that node wasn't unregistered between the snapshot
		// and this write. Access via SetNodeState (which is nil-safe) instead of
		// indexing directly, which would panic if the node was deleted.
		if _, exists := r.nodes[node.NodeID]; exists {
			if err != nil {
				r.nodes[node.NodeID].State = NodeStateUnhealthy
				r.nodes[node.NodeID].LastSeen = time.Now()
				r.logger.Warn("node unhealthy", "node_id", node.NodeID, "address", node.Address, "error", err)
			} else {
				r.nodes[node.NodeID].State = NodeStateHealthy
				r.nodes[node.NodeID].LastSeen = time.Now()
			}
		}
		r.mu.Unlock()
	}
}

func (r *NodeRegistry) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

func (r *NodeRegistry) HealthyNodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, node := range r.nodes {
		if node.State == NodeStateHealthy {
			count++
		}
	}

	return count
}

func (r *NodeRegistry) String() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return fmt.Sprintf("NodeRegistry{total=%d, healthy=%d}", len(r.nodes), r.healthyCountLocked())
}

func (r *NodeRegistry) healthyCountLocked() int {
	count := 0
	for _, node := range r.nodes {
		if node.State == NodeStateHealthy {
			count++
		}
	}
	return count
}
