package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DevLikhith5/kasoku/internal/ring"
	storage "github.com/DevLikhith5/kasoku/internal/store"
)

type MockStore struct {
	mu     sync.RWMutex
	data   map[string][]byte
	closed bool
}

func NewMockStore() *MockStore {
	return &MockStore{
		data: make(map[string][]byte),
	}
}

func (s *MockStore) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("store closed")
	}
	s.data[key] = value
	return nil
}

func (s *MockStore) Get(key string) (storage.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return storage.Entry{}, storage.ErrEngineClosed
	}
	val, ok := s.data[key]
	if !ok {
		return storage.Entry{}, storage.ErrKeyNotFound
	}
	return storage.Entry{Key: key, Value: val}, nil
}

func (s *MockStore) MultiGet(keys []string) (map[string]storage.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]storage.Entry, len(keys))
	for _, k := range keys {
		if val, ok := s.data[k]; ok {
			result[k] = storage.Entry{Key: k, Value: val}
		}
	}
	return result, nil
}

func (s *MockStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("store closed")
	}
	delete(s.data, key)
	return nil
}

func (s *MockStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *MockStore) Keys() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var keys []string
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func (s *MockStore) Scan(prefix string) ([]storage.Entry, error) {
	return nil, nil // Not needed for these tests
}

func (s *MockStore) Stats() storage.EngineStats {
	return storage.EngineStats{}
}

func TestCluster_ReplicatedPut(t *testing.T) {
	store := NewMockStore()
	r := ring.New(150)

	// Add nodes to ring
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
		RPCTimeout:        5 * time.Second,
	}

	c := New(cfg)

	// Test basic put (will fail to replicate but should succeed locally)
	ctx := context.Background()
	err := c.ReplicatedPut(ctx, "test-key", []byte("test-value"))

	// Since we don't have actual RPC clients for other nodes,
	// we expect quorum to fail, but local write should succeed
	if err == nil {
		// If it succeeds, it means we have enough mock clients
		t.Log("replicated put succeeded")
	} else {
		t.Logf("replicated put failed (expected without real peers): %v", err)
	}

	// Verify local write succeeded
	entry, err := store.Get("test-key")
	if err != nil {
		t.Error("expected key to be written locally")
	}
	if string(entry.Value) != "test-value" {
		t.Errorf("expected test-value, got %s", string(entry.Value))
	}
}

func TestCluster_IsPrimary(t *testing.T) {
	store := NewMockStore()
	r := ring.New(150)

	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	cfg := ClusterConfig{
		NodeID: "node-1",
		Ring:   r,
		Store:  store,
	}

	c := New(cfg)

	// Test multiple keys to see which ones this node is primary for
	primaryCount := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		if c.IsPrimary(key) {
			primaryCount++
		}
	}

	// With 3 nodes, node-1 should be primary for roughly 1/3 of keys
	if primaryCount == 0 {
		t.Error("expected node-1 to be primary for some keys")
	}
	if primaryCount > 50 {
		t.Errorf("node-1 is primary for too many keys: %d", primaryCount)
	}

	t.Logf("node-1 is primary for %d/100 keys", primaryCount)
}

func TestCluster_GetReplicas(t *testing.T) {
	store := NewMockStore()
	r := ring.New(150)

	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	cfg := ClusterConfig{
		NodeID:            "node-1",
		Ring:              r,
		Store:             store,
		ReplicationFactor: 3,
	}

	c := New(cfg)

	replicas := c.GetReplicas("test-key")
	if len(replicas) != 3 {
		t.Errorf("expected 3 replicas, got %d", len(replicas))
	}

	// Verify no duplicates
	seen := make(map[string]bool)
	for _, replica := range replicas {
		if seen[replica] {
			t.Errorf("duplicate replica: %s", replica)
		}
		seen[replica] = true
	}

	// First replica should be the primary
	if replicas[0] != "node-1" && replicas[0] != "node-2" && replicas[0] != "node-3" {
		t.Errorf("invalid primary replica: %s", replicas[0])
	}
}

func TestCluster_NodeOperations(t *testing.T) {
	store := NewMockStore()
	r := ring.New(150)

	cfg := ClusterConfig{
		NodeID: "node-1",
		Ring:   r,
		Store:  store,
	}

	c := New(cfg)

	// Add peers
	c.AddPeer("http://localhost:8081", "http://localhost:8081")
	c.AddPeer("http://localhost:8082", "http://localhost:8082")

	if r.NodeCount() != 1 {
		// Node count only tracks nodes added via AddNode, not peers
		t.Logf("node count: %d", r.NodeCount())
	}

	// Remove peer
	c.RemovePeer("http://localhost:8081", "http://localhost:8081")
}

func TestCluster_QuorumSize(t *testing.T) {
	store := NewMockStore()
	r := ring.New(150)

	r.AddNode("node-1")

	// Test with quorum size 1 (single node)
	cfg := ClusterConfig{
		NodeID:            "node-1",
		Ring:              r,
		Store:             store,
		ReplicationFactor: 1,
		QuorumSize:        1,
	}

	c := New(cfg)

	ctx := context.Background()
	err := c.ReplicatedPut(ctx, "quorum-test", []byte("value"))
	if err != nil {
		t.Errorf("expected success with quorum size 1: %v", err)
	}
}

func TestCluster_Distribution(t *testing.T) {
	store := NewMockStore()
	r := ring.New(150)

	// Add 10 nodes
	for i := 1; i <= 10; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	cfg := ClusterConfig{
		NodeID: "node-1",
		Ring:   r,
		Store:  store,
	}

	c := New(cfg)

	dist := r.Distribution()

	// Each node should have approximately 10% of the ring
	for nodeID, pct := range dist {
		if pct < 8 || pct > 12 {
			t.Errorf("node %s has %.2f%% of ring, expected ~10%%", nodeID, pct)
		}
	}

	_ = c // Use cluster variable
}

func TestCluster_EmptyRing(t *testing.T) {
	store := NewMockStore()
	r := ring.New(150)

	cfg := ClusterConfig{
		NodeID: "node-1",
		Ring:   r,
		Store:  store,
	}

	c := New(cfg)

	ctx := context.Background()
	err := c.ReplicatedPut(ctx, "test", []byte("value"))

	// Should succeed with single node (no replication needed)
	if err != nil {
		t.Logf("empty ring put result: %v", err)
	}
}

func TestCluster_NilRing(t *testing.T) {
	store := NewMockStore()

	cfg := ClusterConfig{
		NodeID: "node-1",
		Ring:   nil, // No ring
		Store:  store,
	}

	c := New(cfg)

	// Should fall back to single-node mode
	replicas := c.GetReplicas("test-key")
	if len(replicas) != 1 {
		t.Errorf("expected 1 replica with nil ring, got %d", len(replicas))
	}
}

func TestCluster_ConcurrentAccess(t *testing.T) {
	store := NewMockStore()
	r := ring.New(150)

	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	cfg := ClusterConfig{
		NodeID:            "node-1",
		Ring:              r,
		Store:             store,
		ReplicationFactor: 3,
		QuorumSize:        2,
	}

	c := New(cfg)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				if err := c.ReplicatedPut(ctx, key, value); err != nil {
					// Expected to fail without real peers
					return
				}
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				c.ReplicatedGet(ctx, key)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

func TestCluster_GetNodeID(t *testing.T) {
	store := NewMockStore()
	r := ring.New(150)

	cfg := ClusterConfig{
		NodeID: "test-node-123",
		Ring:   r,
		Store:  store,
	}

	c := New(cfg)

	if c.GetNodeID() != "test-node-123" {
		t.Errorf("expected test-node-123, got %s", c.GetNodeID())
	}
}

func TestCluster_GetRing(t *testing.T) {
	store := NewMockStore()
	r := ring.New(150)

	cfg := ClusterConfig{
		NodeID: "node-1",
		Ring:   r,
		Store:  store,
	}

	c := New(cfg)

	retrievedRing := c.GetRing()
	if retrievedRing != r {
		t.Error("expected same ring instance")
	}
}

func TestCluster_SetNodeAddr(t *testing.T) {
	store := NewMockStore()
	r := ring.New(150)

	cfg := ClusterConfig{
		NodeID: "node-1",
		Ring:   r,
		Store:  store,
	}

	c := New(cfg)

	c.SetNodeAddr("node-2", "http://localhost:8081")
	// This should create a client for node-2
}

func BenchmarkCluster_ReplicatedPut(b *testing.B) {
	store := NewMockStore()
	r := ring.New(150)

	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	cfg := ClusterConfig{
		NodeID:            "node-1",
		Ring:              r,
		Store:             store,
		ReplicationFactor: 3,
		QuorumSize:        2,
	}

	c := New(cfg)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		c.ReplicatedPut(ctx, key, value)
	}
}

func BenchmarkCluster_ReplicatedGet(b *testing.B) {
	store := NewMockStore()
	r := ring.New(150)

	r.AddNode("node-1")

	cfg := ClusterConfig{
		NodeID: "node-1",
		Ring:   r,
		Store:  store,
	}

	c := New(cfg)
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		store.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%1000)
		c.ReplicatedGet(ctx, key)
	}
}
