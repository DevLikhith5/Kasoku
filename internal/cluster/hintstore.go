package cluster

import (
	"sync"
	"sync/atomic"
	"time"
)

type Hint struct {
	Key        string
	Value      []byte
	TargetNode string // the node that should ultimately receive this write
	CreatedAt  time.Time
	Attempts   atomic.Int32
}

// HintStore stores writes that couldn't be delivered to their target node
// (hinted handoff for temporary unavailability)
type HintStore struct {
	mu    sync.RWMutex
	hints []*Hint
}

func NewHintStore() *HintStore {
	return &HintStore{
		hints: make([]*Hint, 0),
	}
}

func (hs *HintStore) Store(key string, value []byte, targetNode string) error {
	hs.mu.Lock()
	defer hs.mu.Unlock()

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

func (hs *HintStore) RemoveHint(key string, targetNode string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	newHints := make([]*Hint, 0, len(hs.hints))
	for _, h := range hs.hints {
		if h.Key == key && h.TargetNode == targetNode {
			continue // skip this one — it was delivered
		}
		newHints = append(newHints, h)
	}
	hs.hints = newHints
}

// RetryFailed attempts to redeliver all hints using the provided delivery function.
// Bug 9 fix: tracks delivered hints by pointer identity so that duplicate hints
// (same key+target) are only removed one at a time when delivered, not all at once.
func (hs *HintStore) RetryFailed(deliver func(targetNode string, key string, value []byte) error) {
	hs.mu.Lock()
	hints := make([]*Hint, len(hs.hints))
	copy(hints, hs.hints)
	hs.mu.Unlock()

	delivered := make(map[*Hint]bool)

	for _, h := range hints {
		if h.Attempts.Load() >= 10 {
			// Give up after 10 attempts — in production you'd persist these
			continue
		}

		err := deliver(h.TargetNode, h.Key, h.Value)
		if err == nil {
			delivered[h] = true // mark by pointer, not composite key
		} else {
			h.Attempts.Add(1)
		}
	}

	// Remove successfully delivered hints by pointer identity
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

func (hs *HintStore) PendingCount() int {
	hs.mu.RLock()
	defer hs.mu.RUnlock()
	return len(hs.hints)
}
