package lsmengine

import (
	"math/rand"
	storage "github.com/DevLikhith5/kasoku/internal/store"
	"time"
)


type node struct {
	entry   storage.Entry
	forward []*node
}



type SkipList struct {
	head     *node
	level    int
	maxLevel int
	p        float64
	size     int
	rng      *rand.Rand
}



func NewSkipList(maxLevel int, p float64) *SkipList {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))


	head := &node{
		forward: make([]*node, maxLevel),
	}

	return &SkipList{
		head:     head,
		level:    1,
		maxLevel: maxLevel,
		p:        p,
		rng:      rng, 
	}
}



func (s *SkipList) randomLevel() int {
	lvl := 1
	for s.rng.Float64() < s.p && lvl < s.maxLevel {
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



func (s *SkipList) Put(entry storage.Entry) {
	update := make([]*node, s.maxLevel)
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
		curr.entry = entry
		return
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