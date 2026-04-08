package lsmengine

import (
	"sync"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

type node struct {
	entry   storage.Entry
	forward []*node
}

type SkipList struct {
	head     *node
	level    int
	maxLevel int
	p        uint32 // threshold for randomLevel (replaces float64)
	size     int

	// Fast RNG — xorshift64, protected by caller's lock
	seed uint64

	// Pool for update arrays to reduce allocations
	updatePool sync.Pool
}

func NewSkipList(maxLevel int, p float64) *SkipList {
	head := &node{
		forward: make([]*node, maxLevel),
	}

	// Convert float probability to uint32 threshold
	// p=1.0 → always max level, p=0.5 → 50% chance
	var threshold uint32
	if p >= 1.0 {
		threshold = ^uint32(0) // max uint32, always passes
	} else if p <= 0.0 {
		threshold = 0 // never passes
	} else {
		threshold = uint32(p * float64(1<<32))
	}

	sl := &SkipList{
		head:     head,
		level:    1,
		maxLevel: maxLevel,
		p:        threshold,
		seed:     0x1234567890ABCDEF,
	}

	sl.updatePool.New = func() interface{} {
		s := make([]*node, maxLevel)
		return &s
	}

	return sl
}

// fastRand returns a pseudo-random uint32 using xorshift64
// MUST be called while holding s.mu.Lock
func (s *SkipList) fastRand() uint32 {
	x := s.seed
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	s.seed = x
	return uint32(x)
}

func (s *SkipList) randomLevel() int {
	lvl := 1
	for s.fastRand() < s.p && lvl < s.maxLevel {
		lvl++
	}
	return lvl
}

func (s *SkipList) Get(key string) (storage.Entry, bool) {
	curr := s.head

	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].entry.Key < key {
			curr = curr.forward[i]
		}
	}

	curr = curr.forward[0]

	if curr != nil && curr.entry.Key == key {
		return curr.entry, true
	}

	return storage.Entry{}, false
}

func (s *SkipList) Put(entry storage.Entry) int64 {
	updatePtr := s.updatePool.Get().(*[]*node)
	update := *updatePtr
	defer s.updatePool.Put(updatePtr)

	curr := s.head

	// Find insertion points
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].entry.Key < entry.Key {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	curr = curr.forward[0]

	// Update if exists
	if curr != nil && curr.entry.Key == entry.Key {
		oldSize := int64(len(curr.entry.Value))
		curr.entry = entry
		return oldSize
	}

	// Insert new node
	lvl := s.randomLevel()

	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	newNode := &node{
		entry:   entry,
		forward: make([]*node, lvl),
	}

	for i := range lvl {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	s.size++
	return 0
}

func (s *SkipList) Seek(key string) *node {
	curr := s.head

	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].entry.Key < key {
			curr = curr.forward[i]
		}
	}

	return curr.forward[0]
}

func (s *SkipList) Entries() []storage.Entry {
	result := make([]storage.Entry, 0, s.size)
	curr := s.head.forward[0]

	for curr != nil {
		result = append(result, curr.entry)
		curr = curr.forward[0]
	}

	return result
}

func (s *SkipList) Size() int {
	return s.size
}
