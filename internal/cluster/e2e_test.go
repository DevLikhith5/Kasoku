package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/DevLikhith5/kasoku/internal/ring"
	storage "github.com/DevLikhith5/kasoku/internal/store"
)

// TestCluster_E2E_SingleNode tests basic operations with a single node
func TestCluster_E2E_SingleNode(t *testing.T) {
	logger := slog.Default()
	store := NewMockStore()
	r := ring.New(150)
	r.AddNode("node-1")

	cfg := ClusterConfig{
		NodeID:            "node-1",
		NodeAddr:          "http://localhost:8080",
		Ring:              r,
		Store:             store,
		ReplicationFactor: 1,
		QuorumSize:        1,
		RPCTimeout:        5 * time.Second,
		Logger:            logger,
	}

	c := New(cfg)
	ctx := context.Background()

	// Test put
	err := c.ReplicatedPut(ctx, "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Test get
	value, err := c.ReplicatedGet(ctx, "key1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("expected value1, got %s", string(value))
	}

	// Test delete
	err = c.ReplicatedDelete(ctx, "key1")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Verify deletion — should return ErrKeyNotFound
	value, err = c.ReplicatedGet(ctx, "key1")
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound after delete, got: %v", err)
	}
	if value != nil {
		t.Errorf("expected nil value after delete, got %s", string(value))
	}
}

// TestCluster_E2E_ConsistentHashing verifies consistent hashing behavior
func TestCluster_E2E_ConsistentHashing(t *testing.T) {
	logger := slog.Default()
	store := NewMockStore()
	r := ring.New(150)

	// Add 5 nodes
	for i := 1; i <= 5; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	cfg := ClusterConfig{
		NodeID:            "node-1",
		NodeAddr:          "http://localhost:8080",
		Ring:              r,
		Store:             store,
		ReplicationFactor: 3,
		QuorumSize:        2,
		Logger:            logger,
	}

	c := New(cfg)

	// Verify replica sets
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		replicas := c.GetReplicas(key)

		if len(replicas) != 3 {
			t.Errorf("key %s: expected 3 replicas, got %d", key, len(replicas))
		}

		// Verify no duplicates
		seen := make(map[string]bool)
		for _, replica := range replicas {
			if seen[replica] {
				t.Errorf("key %s: duplicate replica %s", key, replica)
			}
			seen[replica] = true
		}
	}
}

// TestCluster_E2E_NodeFailure tests behavior when nodes fail
func TestCluster_E2E_NodeFailure(t *testing.T) {
	logger := slog.Default()
	store := NewMockStore()
	r := ring.New(150)

	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	cfg := ClusterConfig{
		NodeID:            "node-1",
		NodeAddr:          "http://localhost:8080",
		Ring:              r,
		Store:             store,
		ReplicationFactor: 3,
		QuorumSize:        2,
		Logger:            logger,
	}

	c := New(cfg)
	ctx := context.Background()

	// Write some data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		c.ReplicatedPut(ctx, key, value)
	}

	// Simulate node failure by removing from ring
	r.RemoveNode("node-2")

	// Writes should still work with remaining nodes
	err := c.ReplicatedPut(ctx, "new-key", []byte("new-value"))
	if err != nil {
		t.Logf("write after node removal: %v", err)
	}
}

// TestCluster_E2E_ConcurrentOperations tests concurrent operations
func TestCluster_E2E_ConcurrentOperations(t *testing.T) {
	logger := slog.Default()
	store := NewMockStore()
	r := ring.New(150)
	r.AddNode("node-1")

	cfg := ClusterConfig{
		NodeID:            "node-1",
		NodeAddr:          "http://localhost:8080",
		Ring:              r,
		Store:             store,
		ReplicationFactor: 1,
		QuorumSize:        1,
		Logger:            logger,
	}

	c := New(cfg)
	ctx := context.Background()

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Concurrent writes
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				if err := c.ReplicatedPut(ctx, key, value); err != nil {
					errors <- fmt.Errorf("write %s failed: %w", key, err)
				}
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				c.ReplicatedGet(ctx, key)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Log any errors
	for err := range errors {
		t.Log(err)
	}
}

// TestCluster_E2E_RingDistribution tests key distribution across the ring
func TestCluster_E2E_RingDistribution(t *testing.T) {
	r := ring.New(150)

	// Add 10 nodes
	for i := 1; i <= 10; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Distribute 10,000 keys
	keyCounts := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.GetNode(key)
		keyCounts[node]++
	}

	// Check distribution balance
	expected := 10000 / 10
	tolerance := expected / 5 // 20% tolerance

	for nodeID, count := range keyCounts {
		diff := count - expected
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			t.Errorf("node %s has %d keys, expected %d ± %d", nodeID, count, expected, tolerance)
		}
	}
}

// TestCluster_E2E_ReplicationFactor tests different replication factors
func TestCluster_E2E_ReplicationFactor(t *testing.T) {
	logger := slog.Default()

	for rf := 1; rf <= 5; rf++ {
		t.Run(fmt.Sprintf("rf=%d", rf), func(t *testing.T) {
			store := NewMockStore()
			r := ring.New(150)

			// Add enough nodes
			for i := 1; i <= 5; i++ {
				r.AddNode(fmt.Sprintf("node-%d", i))
			}

			cfg := ClusterConfig{
				NodeID:            "node-1",
				NodeAddr:          "http://localhost:8080",
				Ring:              r,
				Store:             store,
				ReplicationFactor: rf,
				QuorumSize:        rf/2 + 1,
				Logger:            logger,
			}

			c := New(cfg)
			ctx := context.Background()

			// Test write
			err := c.ReplicatedPut(ctx, "test-key", []byte("test-value"))
			if err != nil {
				t.Logf("rf=%d write result: %v", rf, err)
			}

			// Verify replica count
			replicas := c.GetReplicas("test-key")
			if len(replicas) != rf {
				t.Errorf("rf=%d: expected %d replicas, got %d", rf, rf, len(replicas))
			}
		})
	}
}

// TestCluster_E2E_NodeRegistry tests node registry integration
func TestCluster_E2E_NodeRegistry(t *testing.T) {
	logger := slog.Default()

	healthCheckFunc := func(ctx context.Context, addr string) error {
		return nil // All nodes healthy
	}

	registry := NewNodeRegistry(healthCheckFunc, time.Second, logger)

	// Register nodes
	for i := 1; i <= 5; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		addr := fmt.Sprintf("http://localhost:%d", 8080+i)
		registry.Register(nodeID, addr)
		registry.SetNodeState(nodeID, NodeStateHealthy)
	}

	if registry.NodeCount() != 5 {
		t.Errorf("expected 5 nodes, got %d", registry.NodeCount())
	}
	if registry.HealthyNodeCount() != 5 {
		t.Errorf("expected 5 healthy nodes, got %d", registry.HealthyNodeCount())
	}

	// Simulate node failure
	registry.SetNodeState("node-3", NodeStateUnhealthy)
	if registry.HealthyNodeCount() != 4 {
		t.Errorf("expected 4 healthy nodes after failure, got %d", registry.HealthyNodeCount())
	}
}

// TestCluster_E2E_FailureDetector tests failure detector integration
func TestCluster_E2E_FailureDetector(t *testing.T) {
	logger := slog.Default()
	fd := NewFailureDetector(8.0, time.Minute, 3, logger)

	// Record heartbeats
	for i := 0; i < 10; i++ {
		fd.RecordHeartbeat("node-1")
		time.Sleep(50 * time.Millisecond)
	}

	if !fd.IsAvailable("node-1") {
		t.Error("expected node-1 to be available")
	}

	// Wait for timeout
	time.Sleep(200 * time.Millisecond)

	// Node should still be available (within threshold)
	if !fd.IsAvailable("node-1") {
		t.Error("expected node-1 to still be available")
	}
}

// TestCluster_E2E_QuorumChecker tests quorum checker integration
func TestCluster_E2E_QuorumChecker(t *testing.T) {
	qc := NewQuorumChecker(3, 2)

	// Test various scenarios
	if !qc.CheckWriteQuorum(2) {
		t.Error("expected quorum with 2/3")
	}
	if !qc.CheckWriteQuorum(3) {
		t.Error("expected quorum with 3/3")
	}
	if qc.CheckWriteQuorum(1) {
		t.Error("expected no quorum with 1/3")
	}

	// Check if quorum is possible
	if !qc.IsQuorumPossible(2) {
		t.Error("expected quorum possible with 2 nodes")
	}
	if qc.IsQuorumPossible(1) {
		t.Error("expected quorum not possible with 1 node")
	}
}

// TestCluster_E2E_KeyMovementOnNodeAdd tests consistent hashing key movement
func TestCluster_E2E_KeyMovementOnNodeAdd(t *testing.T) {
	r := ring.New(150)

	// Add 4 nodes
	for i := 1; i <= 4; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Track key assignments
	keyNodeMap := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.GetNode(key)
		keyNodeMap[key] = node
	}

	// Add 5th node
	r.AddNode("node-5")

	// Count moved keys
	movedKeys := 0
	for key, oldNode := range keyNodeMap {
		newNode, _ := r.GetNode(key)
		if newNode != oldNode {
			movedKeys++
		}
	}

	// With consistent hashing, ~20% of keys should move
	expectedMoved := 1000 / 5
	tolerance := 200

	diff := movedKeys - expectedMoved
	if diff < 0 {
		diff = -diff
	}
	if diff > tolerance {
		t.Errorf("expected ~%d keys to move, got %d (diff=%d)", expectedMoved, movedKeys, diff)
	}

	t.Logf("key movement: %d/%d keys moved (expected ~%d)", movedKeys, 1000, expectedMoved)
}

// TestCluster_E2E_KeyMovementOnNodeRemove tests key movement when node is removed
func TestCluster_E2E_KeyMovementOnNodeRemove(t *testing.T) {
	r := ring.New(150)

	// Add 5 nodes
	for i := 1; i <= 5; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Track key assignments
	keyNodeMap := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.GetNode(key)
		keyNodeMap[key] = node
	}

	// Remove node-3
	r.RemoveNode("node-3")

	// Count moved keys
	movedKeys := 0
	for key, oldNode := range keyNodeMap {
		if oldNode == "node-3" {
			// Keys from removed node must move
			newNode, _ := r.GetNode(key)
			if newNode == oldNode {
				t.Error("key should have moved from removed node")
			}
			movedKeys++
		} else {
			// Keys from other nodes should stay
			newNode, _ := r.GetNode(key)
			if newNode != oldNode {
				movedKeys++
			}
		}
	}

	// Only keys from removed node should move (~20%)
	expectedMoved := 1000 / 5
	tolerance := 200

	diff := movedKeys - expectedMoved
	if diff < 0 {
		diff = -diff
	}
	if diff > tolerance {
		t.Errorf("expected ~%d keys to move, got %d (diff=%d)", expectedMoved, movedKeys, diff)
	}

	t.Logf("key movement: %d/%d keys moved (expected ~%d)", movedKeys, 1000, expectedMoved)
}

// TestCluster_E2E_ReadRepair tests read repair functionality
func TestCluster_E2E_ReadRepair(t *testing.T) {
	logger := slog.Default()
	rr := NewReadRepair(logger)

	// Simulate inconsistent replicas
	values := map[string][]byte{
		"node-1": []byte("value-v2"),
		"node-2": []byte("value-v1"), // Stale
		"node-3": []byte("value-v2"),
	}

	repairs := make(map[string][]byte)
	writeFunc := func(ctx context.Context, nodeID string, key string, value []byte) error {
		repairs[nodeID] = value
		return nil
	}

	count := rr.CheckAndRepair(context.Background(), "test-key", values, writeFunc)

	// node-2 should be repaired
	if count != 1 {
		t.Errorf("expected 1 repair, got %d", count)
	}

	if string(repairs["node-2"]) != "value-v2" {
		t.Errorf("expected node-2 to be repaired to value-v2, got %s", string(repairs["node-2"]))
	}
}
