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
// Uses simple full-cache eviction: when full, clear everything at once (O(1) instead of O(n)).
func (kc *KeyCache) Put(key string, entry storage.Entry, found bool) {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	// If already exists, update
	if _, ok := kc.items[key]; ok {
		kc.items[key] = &keyCacheItem{entry: entry, found: found}
		return
	}

	// When full, clear entire cache at once - O(1) vs O(n) iteration
	// Trade-off: lose all cached data, but avoids O(n) stall
	if len(kc.items) >= kc.maxSize {
		clear(kc.items)
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
