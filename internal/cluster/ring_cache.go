package cluster

import (
	"hash/fnv"
	"sync"
)

// RingCache caches ring lookups (key → replica node IDs)
// to avoid repeated consistent-hash computation on hot keys.
type RingCache struct {
	mu      sync.RWMutex
	buckets []ringCacheBucket
	cap     int
}

type ringCacheBucket struct {
	mu   sync.RWMutex
	key  string
	val  []string
}

// func newRingCache(cap int) *RingCache {
// 	n := cap / 8
// 	if n < 16 {
// 		n = 16
// 	}
// 	rc := &RingCache{
// 		buckets: make([]ringCacheBucket, n),
// 		cap:     cap,
// 	}
// 	return rc
// }

func (rc *RingCache) Get(key string) ([]string, bool) {
	b := &rc.buckets[rc.hash(key)%uint32(len(rc.buckets))]
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.key == key {
		return b.val, true
	}
	return nil, false
}

func (rc *RingCache) Put(key string, val []string) {
	b := &rc.buckets[rc.hash(key)%uint32(len(rc.buckets))]
	b.mu.Lock()
	defer b.mu.Unlock()
	b.key = key
	// Copy to avoid slice aliasing
	cp := make([]string, len(val))
	copy(cp, val)
	b.val = cp
}

func (rc *RingCache) hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}
