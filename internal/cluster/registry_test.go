package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestNodeRegistry_Register(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	registry.Register("node-1", "http://localhost:8080")
	registry.Register("node-2", "http://localhost:8081")

	if registry.NodeCount() != 2 {
		t.Errorf("expected 2 nodes, got %d", registry.NodeCount())
	}

	// Duplicate registration should be ignored
	registry.Register("node-1", "http://localhost:8080")
	if registry.NodeCount() != 2 {
		t.Errorf("expected still 2 nodes after duplicate, got %d", registry.NodeCount())
	}
}

func TestNodeRegistry_Unregister(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	registry.Register("node-1", "http://localhost:8080")
	registry.Register("node-2", "http://localhost:8081")

	registry.Unregister("node-1")
	if registry.NodeCount() != 1 {
		t.Errorf("expected 1 node, got %d", registry.NodeCount())
	}

	// Unregister non-existent node
	registry.Unregister("node-nonexistent")
	if registry.NodeCount() != 1 {
		t.Errorf("expected still 1 node, got %d", registry.NodeCount())
	}
}

func TestNodeRegistry_GetNode(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	registry.Register("node-1", "http://localhost:8080")

	node, exists := registry.GetNode("node-1")
	if !exists {
		t.Fatal("expected node to exist")
	}
	if node.NodeID != "node-1" {
		t.Errorf("expected node-1, got %s", node.NodeID)
	}
	if node.Address != "http://localhost:8080" {
		t.Errorf("expected http://localhost:8080, got %s", node.Address)
	}

	// Non-existent node
	_, exists = registry.GetNode("node-nonexistent")
	if exists {
		t.Error("expected non-existent node to not exist")
	}
}

func TestNodeRegistry_GetNodeByAddr(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	registry.Register("node-1", "http://localhost:8080")

	node, exists := registry.GetNodeByAddr("http://localhost:8080")
	if !exists {
		t.Fatal("expected node to exist")
	}
	if node.NodeID != "node-1" {
		t.Errorf("expected node-1, got %s", node.NodeID)
	}

	// Non-existent address
	_, exists = registry.GetNodeByAddr("http://localhost:9999")
	if exists {
		t.Error("expected non-existent address to not exist")
	}
}

func TestNodeRegistry_SetNodeState(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	registry.Register("node-1", "http://localhost:8080")

	registry.SetNodeState("node-1", NodeStateHealthy)
	node, _ := registry.GetNode("node-1")
	if node.State != NodeStateHealthy {
		t.Errorf("expected healthy, got %s", node.State)
	}

	registry.SetNodeState("node-1", NodeStateUnhealthy)
	node, _ = registry.GetNode("node-1")
	if node.State != NodeStateUnhealthy {
		t.Errorf("expected unhealthy, got %s", node.State)
	}

	// Non-existent node
	registry.SetNodeState("node-nonexistent", NodeStateHealthy)
}

func TestNodeRegistry_GetHealthyNodes(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	registry.Register("node-1", "http://localhost:8080")
	registry.Register("node-2", "http://localhost:8081")
	registry.Register("node-3", "http://localhost:8082")

	registry.SetNodeState("node-1", NodeStateHealthy)
	registry.SetNodeState("node-2", NodeStateUnhealthy)
	registry.SetNodeState("node-3", NodeStateHealthy)

	healthy := registry.GetHealthyNodes()
	if len(healthy) != 2 {
		t.Errorf("expected 2 healthy nodes, got %d", len(healthy))
	}
}

func TestNodeRegistry_GetAllNodes(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	registry.Register("node-1", "http://localhost:8080")
	registry.Register("node-2", "http://localhost:8081")

	nodes := registry.GetAllNodes()
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestNodeRegistry_GetNodeAddress(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	registry.Register("node-1", "http://localhost:8080")

	addr, exists := registry.GetNodeAddress("node-1")
	if !exists {
		t.Fatal("expected address to exist")
	}
	if addr != "http://localhost:8080" {
		t.Errorf("expected http://localhost:8080, got %s", addr)
	}

	// Non-existent node
	_, exists = registry.GetNodeAddress("node-nonexistent")
	if exists {
		t.Error("expected non-existent node to not have address")
	}
}

func TestNodeRegistry_HealthChecks(t *testing.T) {
	logger := slog.Default()
	
	// Mock health check function
	healthyNodes := map[string]bool{
		"http://localhost:8080": true,
		"http://localhost:8081": true,
		"http://localhost:8082": false,
	}

	healthCheckFunc := func(ctx context.Context, addr string) error {
		if healthy, ok := healthyNodes[addr]; ok && healthy {
			return nil
		}
		return fmt.Errorf("node %s is unhealthy", addr)
	}

	registry := NewNodeRegistry(healthCheckFunc, 100*time.Millisecond, logger)

	registry.Register("node-1", "http://localhost:8080")
	registry.Register("node-2", "http://localhost:8081")
	registry.Register("node-3", "http://localhost:8082")

	// Run health checks manually
	registry.runHealthChecks()

	// Check states
	node1, _ := registry.GetNode("node-1")
	node2, _ := registry.GetNode("node-2")
	node3, _ := registry.GetNode("node-3")

	if node1.State != NodeStateHealthy {
		t.Errorf("expected node-1 healthy, got %s", node1.State)
	}
	if node2.State != NodeStateHealthy {
		t.Errorf("expected node-2 healthy, got %s", node2.State)
	}
	if node3.State != NodeStateUnhealthy {
		t.Errorf("expected node-3 unhealthy, got %s", node3.State)
	}
}

func TestNodeRegistry_HealthyNodeCount(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	registry.Register("node-1", "http://localhost:8080")
	registry.Register("node-2", "http://localhost:8081")

	registry.SetNodeState("node-1", NodeStateHealthy)
	registry.SetNodeState("node-2", NodeStateUnhealthy)

	if registry.HealthyNodeCount() != 1 {
		t.Errorf("expected 1 healthy node, got %d", registry.HealthyNodeCount())
	}
}

func TestNodeRegistry_ConcurrentAccess(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	var wg sync.WaitGroup

	// Concurrent registrations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			registry.Register(fmt.Sprintf("node-%d", id), fmt.Sprintf("http://localhost:%d", 8080+id))
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			registry.GetNode(fmt.Sprintf("node-%d", id))
			registry.GetNodeByAddr(fmt.Sprintf("http://localhost:%d", 8080+id))
			registry.GetHealthyNodes()
			registry.GetAllNodes()
		}(i)
	}

	// Concurrent state changes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			registry.SetNodeState(fmt.Sprintf("node-%d", id), NodeStateHealthy)
		}(i)
	}

	wg.Wait()

	if registry.NodeCount() != 10 {
		t.Errorf("expected 10 nodes, got %d", registry.NodeCount())
	}
}

func TestNodeRegistry_String(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	registry.Register("node-1", "http://localhost:8080")
	registry.SetNodeState("node-1", NodeStateHealthy)

	str := registry.String()
	if str == "" {
		t.Error("expected non-empty string")
	}
}

func TestNodeRegistry_Stop(t *testing.T) {
	logger := slog.Default()
	registry := NewNodeRegistry(nil, time.Second, logger)

	registry.Register("node-1", "http://localhost:8080")
	registry.Stop()
}

func TestNodeRegistry_HealthChecksSkipsLeavingNodes(t *testing.T) {
	logger := slog.Default()
	
	checkCount := 0
	healthCheckFunc := func(ctx context.Context, addr string) error {
		checkCount++
		return nil
	}

	registry := NewNodeRegistry(healthCheckFunc, 100*time.Millisecond, logger)

	registry.Register("node-1", "http://localhost:8080")
	registry.SetNodeState("node-1", NodeStateLeaving)

	registry.runHealthChecks()

	// Should not have checked leaving node
	if checkCount != 0 {
		t.Errorf("expected 0 health checks for leaving node, got %d", checkCount)
	}
}

func TestNodeRegistry_HealthChecksSkipsDownNodes(t *testing.T) {
	logger := slog.Default()
	
	checkCount := 0
	healthCheckFunc := func(ctx context.Context, addr string) error {
		checkCount++
		return nil
	}

	registry := NewNodeRegistry(healthCheckFunc, 100*time.Millisecond, logger)

	registry.Register("node-1", "http://localhost:8080")
	registry.SetNodeState("node-1", NodeStateDown)

	registry.runHealthChecks()

	// Should not have checked down node
	if checkCount != 0 {
		t.Errorf("expected 0 health checks for down node, got %d", checkCount)
	}
}

func TestNodeState_String(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{NodeStateUnknown, "unknown"},
		{NodeStateHealthy, "healthy"},
		{NodeStateUnhealthy, "unhealthy"},
		{NodeStateLeaving, "leaving"},
		{NodeStateDown, "down"},
		{NodeState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("NodeState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}
