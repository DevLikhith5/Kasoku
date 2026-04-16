package lsmengine

import (
	"sync"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

// KeyCache caches both positive and negative key lookups in the LSM engine.
// Positive cache: stores the actual entry (avoids disk I/O)
// Negative cache: stores "not found" (avoids repeated bloom filter + binary search)
type KeyCache struct {
	mu      sync.RWMutex
	items   map[string]*keyCacheItem
	maxSize int
}

type keyCacheItem struct {
	entry storage.Entry
	found bool
}

func newKeyCache(maxSize int) *KeyCache {
	return &KeyCache{
		items:   make(map[string]*keyCacheItem, maxSize),
		maxSize: maxSize,
	}
}

// Get returns (entry, found). If found=false, key is known absent.
func (kc *KeyCache) Get(key string) (*keyCacheItem, bool) {
	kc.mu.RLock()
	item, ok := kc.items[key]
	kc.mu.RUnlock()
	return item, ok
}

// Put stores a lookup result. found=true means key exists, false means absent.
// Uses simple eviction: when full, delete oldest 10% to make room.
func (kc *KeyCache) Put(key string, entry storage.Entry, found bool) {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	// If already exists, update
	if _, ok := kc.items[key]; ok {
		kc.items[key] = &keyCacheItem{entry: entry, found: found}
		return
	}

	// Evict 10% of oldest when at capacity
	if len(kc.items) >= kc.maxSize {
		evictCount := kc.maxSize / 10
		if evictCount < 1 {
			evictCount = 1
		}
		for k := range kc.items {
			delete(kc.items, k)
			evictCount--
			if evictCount <= 0 {
				break
			}
		}
	}

	kc.items[key] = &keyCacheItem{entry: entry, found: found}
}

// Invalidate removes a key from the cache (called on write/delete)
func (kc *KeyCache) Invalidate(key string) {
	kc.mu.Lock()
	delete(kc.items, key)
	kc.mu.Unlock()
}

// Clear wipes the entire cache (called after compaction/flush)
func (kc *KeyCache) Clear() {
	kc.mu.Lock()
	clear(kc.items)
	kc.mu.Unlock()
}
