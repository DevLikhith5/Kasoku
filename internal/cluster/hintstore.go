package cluster

import (
	"sync"
	"sync/atomic"
	"time"
)

type Hint struct {
	Key        string
	Value      []byte
	TargetNode string
	CreatedAt  time.Time
	Attempts   atomic.Int32
}

type HintStore struct {
	mu         sync.RWMutex
	hints      []*Hint
	maxHints   int
	dropOldest bool
}

const DefaultMaxHints = 100000

func NewHintStore() *HintStore {
	return NewHintStoreWithMax(DefaultMaxHints)
}

func NewHintStoreWithMax(maxHints int) *HintStore {
	if maxHints <= 0 {
		maxHints = DefaultMaxHints
	}
	return &HintStore{
		hints:      make([]*Hint, 0, maxHints),
		maxHints:   maxHints,
		dropOldest: true,
	}
}

func (hs *HintStore) Store(key string, value []byte, targetNode string) error {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.maxHints > 0 && len(hs.hints) >= hs.maxHints {
		if hs.dropOldest {
			removeCount := hs.maxHints / 10
			if removeCount < 1 {
				removeCount = 1
			}
			if removeCount < len(hs.hints) {
				hs.hints = hs.hints[removeCount:]
			}
		} else {
			return nil
		}
	}

	hs.hints = append(hs.hints, &Hint{
		Key:        key,
		Value:      value,
		TargetNode: targetNode,
		CreatedAt:  time.Now(),
	})
	return nil
}

func (hs *HintStore) GetHintsForNode(nodeID string) []*Hint {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	result := make([]*Hint, 0)
	for _, h := range hs.hints {
		if h.TargetNode == nodeID {
			result = append(result, h)
		}
	}
	return result
}

func (hs *HintStore) PendingCount() int {
	hs.mu.RLock()
	defer hs.mu.RUnlock()
	return len(hs.hints)
}

func (hs *HintStore) RemoveHint(key string, targetNode string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	newHints := make([]*Hint, 0, len(hs.hints))
	for _, h := range hs.hints {
		if h.Key == key && h.TargetNode == targetNode {
			continue
		}
		newHints = append(newHints, h)
	}
	hs.hints = newHints
}

func (hs *HintStore) RetryFailed(deliver func(targetNode string, key string, value []byte) error) {
	hs.mu.Lock()
	hints := make([]*Hint, len(hs.hints))
	copy(hints, hs.hints)
	hs.mu.Unlock()

	delivered := make(map[*Hint]bool)

	for _, h := range hints {
		if h.Attempts.Load() >= 10 {
			continue
		}

		err := deliver(h.TargetNode, h.Key, h.Value)
		if err == nil {
			delivered[h] = true
		} else {
			h.Attempts.Add(1)
		}
	}

	if len(delivered) > 0 {
		hs.mu.Lock()
		newHints := make([]*Hint, 0, len(hs.hints))
		for _, h := range hs.hints {
			if !delivered[h] {
				newHints = append(newHints, h)
			}
		}
		hs.hints = newHints
		hs.mu.Unlock()
	}
}
