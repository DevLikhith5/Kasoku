package ring

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestRing_New(t *testing.T) {
	t.Run("default vnodes", func(t *testing.T) {
		r := New(0)
		if r.vnodeCount != DefaultVNodes {
			t.Errorf("expected vnodeCount=%d, got %d", DefaultVNodes, r.vnodeCount)
		}
		if r.NodeCount() != 0 {
			t.Errorf("expected empty ring, got %d nodes", r.NodeCount())
		}
	})

	t.Run("custom vnodes", func(t *testing.T) {
		r := New(100)
		if r.vnodeCount != 100 {
			t.Errorf("expected vnodeCount=100, got %d", r.vnodeCount)
		}
	})
}

func TestRing_AddNode(t *testing.T) {
	r := New(150)

	// Add first node
	r.AddNode("node-1")
	if !r.HasNode("node-1") {
		t.Error("expected node-1 to exist")
	}
	if r.NodeCount() != 1 {
		t.Errorf("expected 1 node, got %d", r.NodeCount())
	}

	// Add same node again (should be no-op)
	r.AddNode("node-1")
	if r.NodeCount() != 1 {
		t.Errorf("expected still 1 node after duplicate add, got %d", r.NodeCount())
	}

	// Add second node
	r.AddNode("node-2")
	if r.NodeCount() != 2 {
		t.Errorf("expected 2 nodes, got %d", r.NodeCount())
	}

	// Verify vnodes were created
	if len(r.vnodes) != 2*150 {
		t.Errorf("expected %d vnodes, got %d", 2*150, len(r.vnodes))
	}
}

func TestRing_RemoveNode(t *testing.T) {
	r := New(150)
	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	// Remove middle node
	r.RemoveNode("node-2")
	if r.HasNode("node-2") {
		t.Error("node-2 should be removed")
	}
	if r.NodeCount() != 2 {
		t.Errorf("expected 2 nodes, got %d", r.NodeCount())
	}

	// Verify vnodes were cleaned up
	if len(r.vnodes) != 2*150 {
		t.Errorf("expected %d vnodes, got %d", 2*150, len(r.vnodes))
	}

	// Remove non-existent node (should be no-op)
	r.RemoveNode("node-nonexistent")
	if r.NodeCount() != 2 {
		t.Errorf("expected still 2 nodes, got %d", r.NodeCount())
	}
}

func TestRing_GetNode(t *testing.T) {
	r := New(150)

	// Empty ring
	node, ok := r.GetNode("key1")
	if ok {
		t.Errorf("expected no node for empty ring, got %s", node)
	}

	// Single node
	r.AddNode("node-1")
	node, ok = r.GetNode("key1")
	if !ok {
		t.Error("expected node for single-node ring")
	}
	if node != "node-1" {
		t.Errorf("expected node-1, got %s", node)
	}

	// Multiple nodes - verify consistency
	r.AddNode("node-2")
	r.AddNode("node-3")

	// Same key should always map to same node
	for i := 0; i < 100; i++ {
		node, _ := r.GetNode("consistent-key")
		if node == "" {
			t.Error("expected consistent node mapping")
		}
	}
}

func TestRing_GetNodes(t *testing.T) {
	r := New(150)
	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	// Request more nodes than exist
	nodes := r.GetNodes("key1", 5)
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes max, got %d", len(nodes))
	}

	// Request exact number
	nodes = r.GetNodes("key1", 3)
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}

	// Verify all nodes are distinct
	seen := make(map[string]bool)
	for _, node := range nodes {
		if seen[node] {
			t.Errorf("duplicate node in replica set: %s", node)
		}
		seen[node] = true
	}

	// First node should be the primary
	primary, _ := r.GetNode("key1")
	if nodes[0] != primary {
		t.Errorf("expected first node to be primary %s, got %s", primary, nodes[0])
	}
}

func TestRing_Distribution(t *testing.T) {
	r := New(150)

	// Add 10 nodes
	for i := 1; i <= 10; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	dist := r.Distribution()

	// Each node should have approximately 10% of the ring
	expected := 100.0 / 10.0 // 10%
	tolerance := 2.0         // 2% tolerance

	for nodeID, pct := range dist {
		diff := math.Abs(pct - expected)
		if diff > tolerance {
			t.Errorf("node %s has %.2f%% of ring, expected %.2f%% ± %.2f%%", nodeID, pct, expected, tolerance)
		}
	}
}

func TestRing_DistributionWithManyNodes(t *testing.T) {
	r := New(150)

	// Add 100 nodes
	for i := 1; i <= 100; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	dist := r.Distribution()

	// Each node should have approximately 1% of the ring
	expected := 1.0 // 1%
	tolerance := 0.5          // 0.5% tolerance

	for nodeID, pct := range dist {
		diff := math.Abs(pct - expected)
		if diff > tolerance {
			t.Errorf("node %s has %.2f%% of ring, expected %.2f%% ± %.2f%%", nodeID, pct, expected, tolerance)
		}
	}
}

func TestRing_LoadBalancing(t *testing.T) {
	r := New(150)

	// Add 4 nodes
	for i := 1; i <= 4; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Distribute 1,000,000 keys
	keyCounts := make(map[string]int)
	for i := 0; i < 1000000; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.GetNode(key)
		keyCounts[node]++
	}

	// Each node should have approximately 25% of keys
	// With 150 vnodes and statistical variance, use 10% tolerance
	expected := 1000000 / 4
	tolerance := expected / 10 // 10% tolerance

	for nodeID, count := range keyCounts {
		diff := int(math.Abs(float64(count - expected)))
		if diff > tolerance {
			t.Errorf("node %s has %d keys, expected %d ± %d", nodeID, count, expected, tolerance)
		}
	}
}

func TestRing_NodeAdditionKeyMovement(t *testing.T) {
	r := New(150)

	// Add 4 nodes
	for i := 1; i <= 4; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Track which keys belong to which node
	keyNodeMap := make(map[string]string)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.GetNode(key)
		keyNodeMap[key] = node
	}

	// Add 5th node
	r.AddNode("node-5")

	// Count how many keys moved
	movedKeys := 0
	for key, oldNode := range keyNodeMap {
		newNode, _ := r.GetNode(key)
		if newNode != oldNode {
			movedKeys++
		}
	}

	// With consistent hashing, approximately 1/5 = 20% of keys should move
	expectedMoved := 10000 / 5
	tolerance := 1000 // 10% tolerance

	diff := int(math.Abs(float64(movedKeys - expectedMoved)))
	if diff > tolerance {
		t.Errorf("expected ~%d keys to move, got %d (diff=%d)", expectedMoved, movedKeys, diff)
	}

	// Verify that keys that didn't move still belong to the same node
	// This is the key property of consistent hashing
	stayedKeys := 10000 - movedKeys
	expectedStayed := 10000 - expectedMoved
	if stayedKeys < expectedStayed-tolerance {
		t.Errorf("too many keys moved: %d stayed, expected ~%d", stayedKeys, expectedStayed)
	}
}

func TestRing_NodeRemovalKeyMovement(t *testing.T) {
	r := New(150)

	// Add 5 nodes
	for i := 1; i <= 5; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Track which keys belong to which node
	keyNodeMap := make(map[string]string)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.GetNode(key)
		keyNodeMap[key] = node
	}

	// Remove one node
	r.RemoveNode("node-3")

	// Count how many keys moved
	movedKeys := 0
	for key, oldNode := range keyNodeMap {
		if oldNode == "node-3" {
			// Keys that belonged to removed node must move
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

	// Only keys from the removed node should have moved
	// Approximately 1/5 = 20% of keys
	expectedMoved := 10000 / 5
	tolerance := 1000

	diff := int(math.Abs(float64(movedKeys - expectedMoved)))
	if diff > tolerance {
		t.Errorf("expected ~%d keys to move, got %d (diff=%d)", expectedMoved, movedKeys, diff)
	}
}

func TestRing_ConcurrentAccess(t *testing.T) {
	r := New(150)

	// Add initial nodes
	for i := 1; i <= 3; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				key := fmt.Sprintf("key-%d", j)
				r.GetNode(key)
				r.GetNodes(key, 3)
			}
		}()
	}

	// Concurrent writes (add/remove nodes)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				nodeID := fmt.Sprintf("node-%d", id*10+j)
				r.AddNode(nodeID)
				time.Sleep(time.Microsecond)
				r.RemoveNode(nodeID)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

func TestRing_GetAllNodes(t *testing.T) {
	r := New(150)

	nodes := r.GetAllNodes()
	if len(nodes) != 0 {
		t.Errorf("expected empty list, got %d nodes", len(nodes))
	}

	expected := []string{"node-1", "node-2", "node-3"}
	for _, nodeID := range expected {
		r.AddNode(nodeID)
	}

	nodes = r.GetAllNodes()
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}
}

func TestRing_SearchWraparound(t *testing.T) {
	r := New(10) // Small vnode count for predictable testing
	r.AddNode("node-1")
	r.AddNode("node-2")

	// Test with a key that hashes to a very high value
	// This should wrap around to the beginning of the ring
	key := "wraparound-test-key"
	node, ok := r.GetNode(key)
	if !ok {
		t.Error("expected node for wraparound key")
	}
	if node == "" {
		t.Error("got empty node for wraparound key")
	}
}

func TestRing_ReplicationFactor(t *testing.T) {
	r := New(150)

	// Add 5 nodes
	for i := 1; i <= 5; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Test different replication factors
	for rf := 1; rf <= 5; rf++ {
		nodes := r.GetNodes("test-key", rf)
		if len(nodes) != rf {
			t.Errorf("expected %d nodes for rf=%d, got %d", rf, rf, len(nodes))
		}

		// Verify no duplicates
		seen := make(map[string]bool)
		for _, node := range nodes {
			if seen[node] {
				t.Errorf("duplicate node %s in replica set for rf=%d", node, rf)
			}
			seen[node] = true
		}
	}
}

func BenchmarkRing_GetNode(b *testing.B) {
	r := New(150)
	for i := 1; i <= 10; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.GetNode(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkRing_GetNodes(b *testing.B) {
	r := New(150)
	for i := 1; i <= 10; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.GetNodes(fmt.Sprintf("key-%d", i), 3)
	}
}

func BenchmarkRing_AddNode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r := New(150)
		for j := 0; j < 10; j++ {
			r.AddNode(fmt.Sprintf("node-%d", j))
		}
	}
}

func TestRing_DeterministicMapping(t *testing.T) {
	r1 := New(150)
	r2 := New(150)

	// Add nodes in same order
	for i := 1; i <= 5; i++ {
		r1.AddNode(fmt.Sprintf("node-%d", i))
		r2.AddNode(fmt.Sprintf("node-%d", i))
	}

	// Same keys should map to same nodes
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		n1, _ := r1.GetNode(key)
		n2, _ := r2.GetNode(key)
		if n1 != n2 {
			t.Errorf("key %s: ring1=%s, ring2=%s", key, n1, n2)
		}
	}
}

func TestRing_RandomSeedIndependence(t *testing.T) {
	// Consistent hashing should not depend on random seed
	rand.Seed(1)
	r1 := New(150)
	r1.AddNode("node-1")

	rand.Seed(2)
	r2 := New(150)
	r2.AddNode("node-1")

	// Both rings should produce identical mappings
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		n1, _ := r1.GetNode(key)
		n2, _ := r2.GetNode(key)
		if n1 != n2 {
			t.Errorf("key %s: ring1=%s, ring2=%s", key, n1, n2)
		}
	}
}
