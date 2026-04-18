package storage

import (
	"errors"
	"time"
)

// VectorClock represents a vector clock for tracking causality across nodes
// Map from nodeID -> counter (monotonically increasing)
type VectorClock map[string]uint64

func NewVectorClock() VectorClock {
	return make(VectorClock)
}

// Increment increments the counter for the given node and returns a new clock
func (vc VectorClock) Increment(nodeID string) VectorClock {
	result := make(VectorClock, len(vc)+1)
	for k, v := range vc {
		result[k] = v
	}
	result[nodeID]++
	return result
}

// Merge takes the element-wise maximum of two clocks
func (vc VectorClock) Merge(other VectorClock) VectorClock {
	result := make(VectorClock, len(vc))
	for k, v := range vc {
		result[k] = v
	}
	for nodeID, ts := range other {
		if ts > result[nodeID] {
			result[nodeID] = ts
		}
	}
	return result
}

type Entry struct {
	Key         string
	Value       []byte
	Version     uint64
	TimeStamp   time.Time
	Tombstone   bool
	VectorClock VectorClock // per-key version vector for conflict detection
}

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrKeyTooLong    = errors.New("key exceeds 1KB limit")
	ErrValueTooLarge = errors.New("value exceeds 1MB limit")
	ErrEngineClosed  = errors.New("engine is closed")
)

const (
	MaxKeyLen   = 1024        // 1KB
	MaxValueLen = 1024 * 1024 // 1MB
)

type EngineStats struct {
	KeyCount    int64
	DiskBytes   int64
	MemBytes    int64
	BloomFPRate float64
}

type StorageEngine interface {
	Get(key string) (Entry, error)
	MultiGet(keys []string) (map[string]Entry, error)
	Put(key string, value []byte) error
	PutWithVectorClock(key string, value []byte, vc VectorClock) error
	// BatchPut writes multiple entries in a single WAL + memtable lock cycle.
	// Much faster than Put in a loop for batch workloads.
	BatchPut(pairs []Entry) error
	Delete(key string) error
	// Keys returns all non-deleted keys
	Keys() ([]string, error)
	// Scan returns all non-deleted entries with the given prefix
	Scan(prefix string) ([]Entry, error)
	Stats() EngineStats
	Close() error
}
