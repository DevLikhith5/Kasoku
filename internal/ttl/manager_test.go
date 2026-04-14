package ttl

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	m := NewManager(Config{})
	if m == nil {
		t.Fatal("expected manager, got nil")
	}
	if m.interval != 1*time.Minute {
		t.Errorf("expected default interval 1m, got %v", m.interval)
	}
}

func TestAddAndGetExpiration(t *testing.T) {
	m := NewManager(Config{})

	key := "test-key"
	ttl := 5 * time.Second
	m.Add(key, ttl)

	expireAt, exists := m.GetExpiration(key)
	if !exists {
		t.Fatal("expected key to exist")
	}

	// Should expire approximately 5 seconds from now
	expected := time.Now().Add(ttl)
	diff := expireAt.Sub(expected)
	if diff < -time.Millisecond || diff > time.Millisecond {
		t.Errorf("expected expiration around %v, got %v", expected, expireAt)
	}

	if m.IsExpired(key) {
		t.Error("expected key to not be expired yet")
	}
}

func TestRemove(t *testing.T) {
	m := NewManager(Config{})

	key := "test-key"
	m.Add(key, 5*time.Second)

	if _, exists := m.GetExpiration(key); !exists {
		t.Fatal("expected key to exist")
	}

	m.Remove(key)

	if _, exists := m.GetExpiration(key); exists {
		t.Error("expected key to be removed")
	}
}

func TestUpdateExistingKey(t *testing.T) {
	m := NewManager(Config{})

	key := "test-key"
	m.Add(key, 10*time.Second)
	firstExpire, _ := m.GetExpiration(key)

	time.Sleep(10 * time.Millisecond)

	// Update with a longer TTL - should replace the old expiration
	m.Add(key, 20*time.Second)
	secondExpire, _ := m.GetExpiration(key)

	// Second expiration should be later than first (updated TTL)
	if !secondExpire.After(firstExpire) {
		t.Errorf("expected updated TTL to be later than original: first=%v, second=%v", firstExpire, secondExpire)
	}

	if m.Size() != 1 {
		t.Errorf("expected size 1, got %d", m.Size())
	}
}

func TestIsExpired(t *testing.T) {
	m := NewManager(Config{})

	key := "test-key"
	m.Add(key, 50*time.Millisecond)

	if m.IsExpired(key) {
		t.Error("should not be expired immediately after adding")
	}

	time.Sleep(100 * time.Millisecond)

	if !m.IsExpired(key) {
		t.Error("should be expired after TTL passes")
	}
}

func TestSize(t *testing.T) {
	m := NewManager(Config{})

	m.Add("key1", 10*time.Second)
	m.Add("key2", 20*time.Second)
	m.Add("key3", 30*time.Second)

	if m.Size() != 3 {
		t.Errorf("expected size 3, got %d", m.Size())
	}

	m.Remove("key2")
	if m.Size() != 2 {
		t.Errorf("expected size 2 after removal, got %d", m.Size())
	}
}

func TestExpirationCallback(t *testing.T) {
	var expiredCount atomic.Int32
	var mu sync.Mutex
	var expiredKeys []string

	m := NewManager(Config{
		CheckInterval: 50 * time.Millisecond,
		OnExpire: func(key string) {
			expiredCount.Add(1)
			mu.Lock()
			expiredKeys = append(expiredKeys, key)
			mu.Unlock()
		},
	})

	m.Add("key1", 100*time.Millisecond)
	m.Add("key2", 100*time.Millisecond)
	m.Add("key3", 100*time.Millisecond)

	m.Start()

	// Wait for expiration + checker interval
	time.Sleep(200 * time.Millisecond)

	m.Stop()

	// Give callback time to finish
	time.Sleep(50 * time.Millisecond)

	if expiredCount.Load() != 3 {
		t.Errorf("expected 3 expired keys, got %d", expiredCount.Load())
	}

	mu.Lock()
	if len(expiredKeys) != 3 {
		t.Errorf("expected 3 keys in callback, got %d", len(expiredKeys))
	}
	mu.Unlock()
}

func TestStartStop(t *testing.T) {
	m := NewManager(Config{
		CheckInterval: 100 * time.Millisecond,
	})

	m.Start()
	m.Add("key1", 200*time.Millisecond)

	time.Sleep(300 * time.Millisecond)

	m.Stop()

	// Should be safe to call Stop multiple times
	m.Stop()
}

func TestAddAfterStop(t *testing.T) {
	m := NewManager(Config{})
	m.Stop()

	// Should not panic
	m.Add("key1", 10*time.Second)

	if m.Size() != 0 {
		t.Error("expected no entries added after stop")
	}
}

func TestRemoveAfterStop(t *testing.T) {
	m := NewManager(Config{})
	m.Stop()

	// Should not panic
	m.Remove("key1")
}

func TestMultipleKeysDifferentTTLs(t *testing.T) {
	var expiredKeys sync.Map

	m := NewManager(Config{
		CheckInterval: 50 * time.Millisecond,
		OnExpire: func(key string) {
			expiredKeys.Store(key, true)
		},
	})

	m.Add("short1", 50*time.Millisecond)
	m.Add("short2", 100*time.Millisecond)
	m.Add("long1", 500*time.Millisecond)

	m.Start()

	// Wait for short keys to expire
	time.Sleep(150 * time.Millisecond)

	if _, ok := expiredKeys.Load("short1"); !ok {
		t.Error("expected short1 to be expired")
	}
	if _, ok := expiredKeys.Load("short2"); !ok {
		t.Error("expected short2 to be expired")
	}
	if _, ok := expiredKeys.Load("long1"); ok {
		t.Error("expected long1 to NOT be expired yet")
	}

	m.Stop()
}

func TestPriorityQueueOrdering(t *testing.T) {
	pq := NewPriorityQueue()

	now := time.Now()
	items := []*Item{
		{Key: "key3", ExpireAt: now.Add(30 * time.Second)},
		{Key: "key1", ExpireAt: now.Add(10 * time.Second)},
		{Key: "key2", ExpireAt: now.Add(20 * time.Second)},
	}

	for _, item := range items {
		heapPush(pq, item)
	}

	// Pop should return in expiration order
	item1 := heapPop(pq).(*Item)
	if item1.Key != "key1" {
		t.Errorf("expected key1 first, got %s", item1.Key)
	}

	item2 := heapPop(pq).(*Item)
	if item2.Key != "key2" {
		t.Errorf("expected key2 second, got %s", item2.Key)
	}

	item3 := heapPop(pq).(*Item)
	if item3.Key != "key3" {
		t.Errorf("expected key3 third, got %s", item3.Key)
	}
}

func TestPriorityQueueRemove(t *testing.T) {
	pq := NewPriorityQueue()

	now := time.Now()
	pq.Push(&Item{Key: "key1", ExpireAt: now.Add(10 * time.Second)})
	pq.Push(&Item{Key: "key2", ExpireAt: now.Add(20 * time.Second)})
	pq.Push(&Item{Key: "key3", ExpireAt: now.Add(30 * time.Second)})

	pq.Remove("key2")

	if pq.Len() != 2 {
		t.Errorf("expected length 2 after removal, got %d", pq.Len())
	}

	if _, exists := pq.items["key2"]; exists {
		t.Error("expected key2 to be removed from items map")
	}
}

func TestNonExistentKey(t *testing.T) {
	m := NewManager(Config{})

	// IsExpired should return true for non-tracked keys
	if !m.IsExpired("non-existent") {
		t.Error("expected non-existent key to be considered expired")
	}

	// GetExpiration should return false for non-tracked keys
	_, exists := m.GetExpiration("non-existent")
	if exists {
		t.Error("expected non-existent key to not exist")
	}
}

func TestConcurrentOperations(t *testing.T) {
	m := NewManager(Config{
		CheckInterval: 100 * time.Millisecond,
	})

	m.Start()
	defer m.Stop()

	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.Add("key", time.Duration(id)*time.Second)
		}(i)
	}

	// Concurrent removes
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.Remove("key")
		}(i)
	}

	wg.Wait()

	// Size should be at most 1 (either the key exists or was removed)
	if m.Size() > 1 {
		t.Errorf("expected size <= 1, got %d", m.Size())
	}
}

func TestTombstoneWithTTL(t *testing.T) {
	m := NewManager(Config{
		CheckInterval: 50 * time.Millisecond,
	})

	m.AddWithTombstone("deleted-key", 100*time.Millisecond)

	expireAt, exists := m.GetExpiration("deleted-key")
	if !exists {
		t.Fatal("expected tombstone to exist")
	}

	if time.Now().After(expireAt) {
		t.Error("expected tombstone to not be expired yet")
	}

	time.Sleep(150 * time.Millisecond)

	if !m.IsExpired("deleted-key") {
		t.Error("expected tombstone to be expired")
	}
}

// Helper functions to use heap operations directly
func heapPush(pq *PriorityQueue, item *Item) {
	heap.Push(pq, item)
}

func heapPop(pq *PriorityQueue) interface{} {
	return heap.Pop(pq)
}
