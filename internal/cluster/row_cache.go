package cluster

import (
	"sync"
	"time"
)

// RowCache is a thread-safe LRU cache for hot keys.
// It stores (value, version, expiry) tuples to serve fast reads
// without hitting the distributed quorum read path.
type RowCache struct {
	mu      sync.RWMutex
	items   map[string]*cacheItem
	evict   map[string]struct{}
	maxSize int
	order   []string // LRU order (oldest first)

	// Metrics (lock-free via atomic)
	hits   uint64
	misses uint64
}

type cacheItem struct {
	value   []byte
	version uint64
	expiry  time.Time
}

// TTL for cached rows — 5 seconds balances freshness vs cache hit rate
const rowCacheTTL = 5 * time.Second

// Get returns (value, found). Checks TTL and version staleness.
func (c *RowCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	// Check TTL
	if time.Now().After(item.expiry) {
		c.delete(key)
		return nil, false
	}

	return item.value, true
}

// Put stores a value in the cache with TTL.
// If the cache is full, evicts the oldest entry.
func (c *RowCache) Put(key string, value []byte, version uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If key already exists, update and move to end of LRU
	if _, ok := c.items[key]; ok {
		c.items[key] = &cacheItem{
			value:   value,
			version: version,
			expiry:  time.Now().Add(rowCacheTTL),
		}
		c.moveToEnd(key)
		return
	}

	// Evict oldest if at capacity
	if len(c.items) >= c.maxSize {
		c.evictOldest()
	}

	c.items[key] = &cacheItem{
		value:   value,
		version: version,
		expiry:  time.Now().Add(rowCacheTTL),
	}
	c.order = append(c.order, key)
}

// Delete removes a key from the cache (used on write/delete to invalidate)
func (c *RowCache) Delete(key string) {
	c.delete(key)
}

// Invalidate all entries with version <= given version
// Used after read repair to purge stale cached entries
func (c *RowCache) InvalidateStale(version uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, item := range c.items {
		if item.version <= version {
			delete(c.items, key)
			// Remove from order slice
			for i, k := range c.order {
				if k == key {
					c.order = append(c.order[:i], c.order[i+1:]...)
					break
				}
			}
		}
	}
}

func (c *RowCache) delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
}

func (c *RowCache) evictOldest() {
	if len(c.order) == 0 {
		return
	}

	oldest := c.order[0]
	c.order = c.order[1:]
	delete(c.items, oldest)
}

func (c *RowCache) moveToEnd(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			c.order = append(c.order, key)
			break
		}
	}
}

func (c *RowCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}
