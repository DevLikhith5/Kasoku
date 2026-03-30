package lsmengine

import (
	"sort"
	"strings"
	"sync"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

// Iterator provides a cursor-based range scan over the LSM store
type Iterator struct {
	mu      sync.RWMutex
	entries []storage.Entry
	pos     int
	prefix  string
	closed  bool
	err     error
}

// NewIterator creates a new iterator over the given entries
func NewIterator(entries []storage.Entry, prefix string) *Iterator {
	// Filter by prefix if specified
	var filtered []storage.Entry
	if prefix == "" {
		filtered = make([]storage.Entry, len(entries))
		copy(filtered, entries)
	} else {
		for _, e := range entries {
			if strings.HasPrefix(e.Key, prefix) {
				filtered = append(filtered, e)
			}
		}
	}

	// Sort by key
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Key < filtered[j].Key
	})

	return &Iterator{
		entries: filtered,
		prefix:  prefix,
		pos:     -1, // before first
	}
}

// Valid returns true if the iterator is positioned at a valid entry
func (it *Iterator) Valid() bool {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return !it.closed && it.pos >= 0 && it.pos < len(it.entries)
}

// First moves the cursor to the first entry
func (it *Iterator) First() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || len(it.entries) == 0 {
		it.pos = -1
		return false
	}

	it.pos = 0
	return true
}

// Last moves the cursor to the last entry
func (it *Iterator) Last() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || len(it.entries) == 0 {
		it.pos = -1
		return false
	}

	it.pos = len(it.entries) - 1
	return true
}

// Next advances the cursor to the next entry
func (it *Iterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.pos >= len(it.entries)-1 {
		it.pos = len(it.entries)
		return false
	}

	it.pos++
	return true
}

// Prev moves the cursor to the previous entry
func (it *Iterator) Prev() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.pos <= 0 {
		it.pos = -1
		return false
	}

	it.pos--
	return true
}

// Seek positions the cursor at the first key >= the given key
func (it *Iterator) Seek(key string) bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || len(it.entries) == 0 {
		it.pos = -1
		return false
	}

	// Binary search for first key >= key
	idx := sort.Search(len(it.entries), func(i int) bool {
		return it.entries[i].Key >= key
	})

	if idx >= len(it.entries) {
		it.pos = len(it.entries)
		return false
	}

	it.pos = idx
	return true
}

// SeekToFirst is an alias for First()
func (it *Iterator) SeekToFirst() bool {
	return it.First()
}

// SeekToLast is an alias for Last()
func (it *Iterator) SeekToLast() bool {
	return it.Last()
}

// Key returns the current key
func (it *Iterator) Key() string {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if !it.Valid() {
		return ""
	}
	return it.entries[it.pos].Key
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if !it.Valid() {
		return nil
	}
	return it.entries[it.pos].Value
}

// Entry returns the current entry
func (it *Iterator) Entry() storage.Entry {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if !it.Valid() {
		return storage.Entry{}
	}
	return it.entries[it.pos]
}

// Error returns any error encountered during iteration
func (it *Iterator) Error() error {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return it.err
}

// Close releases resources associated with the iterator
func (it *Iterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.closed = true
	it.entries = nil // help GC
	return nil
}

// Pos returns the current position (0-based index)
func (it *Iterator) Pos() int {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return it.pos
}

// Total returns the total number of entries
func (it *Iterator) Total() int {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return len(it.entries)
}
