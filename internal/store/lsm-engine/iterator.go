package lsmengine

import (
	"sort"
	"strings"
	"sync"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

type Iterator struct {
	mu      sync.RWMutex
	entries []storage.Entry
	pos     int
	prefix  string
	closed  bool
	err     error
}

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

func (it *Iterator) Valid() bool {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return !it.closed && it.pos >= 0 && it.pos < len(it.entries)
}

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

func (it *Iterator) SeekToFirst() bool {
	return it.First()
}

func (it *Iterator) SeekToLast() bool {
	return it.Last()
}

func (it *Iterator) Key() string {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if !it.Valid() {
		return ""
	}
	return it.entries[it.pos].Key
}

func (it *Iterator) Value() []byte {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if !it.Valid() {
		return nil
	}
	return it.entries[it.pos].Value
}

func (it *Iterator) Entry() storage.Entry {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if !it.Valid() {
		return storage.Entry{}
	}
	return it.entries[it.pos]
}

func (it *Iterator) Error() error {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return it.err
}

func (it *Iterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.closed = true
	it.entries = nil // help GC
	return nil
}

func (it *Iterator) Pos() int {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return it.pos
}

func (it *Iterator) Total() int {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return len(it.entries)
}
