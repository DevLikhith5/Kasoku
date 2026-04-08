// Package cluster/vclock implements vector clocks for tracking causal
// ordering of events across cluster nodes. Vector clocks detect true
// concurrency (conflicting writes) vs causally ordered writes.
package cluster

// VectorClock is a map from nodeID to a monotonically increasing counter.
// Each entry tracks how many writes a particular node has produced.
//
// Example: {"node-1": 3, "node-2": 1} means node-1 has done 3 writes
// and node-2 has done 1 write as observed by this clock.
type VectorClock map[string]uint64

func NewVectorClock() VectorClock {
	return make(VectorClock)
}

// Increment increments this node's counter, returning a new clock.
// Called when a node performs a write operation.
func (vc VectorClock) Increment(nodeID string) VectorClock {
	next := vc.Clone()
	next[nodeID]++
	return next
}

// Merge takes the element-wise maximum of two clocks, returning a new clock.
// Used after receiving a message: local = Merge(local, received).
// This captures the "I've seen everything you've seen" semantics.
func (vc VectorClock) Merge(other VectorClock) VectorClock {
	result := vc.Clone()
	for nodeID, ts := range other {
		if ts > result[nodeID] {
			result[nodeID] = ts
		}
	}
	return result
}

type Ordering int

const (
	// Before means vc happened before other (vc < other)
	Before Ordering = -1
	// Concurrent means neither happened before the other — CONFLICT
	Concurrent Ordering = 0
	// After means vc happened after other (vc > other)
	After Ordering = 1
)

func (o Ordering) String() string {
	switch o {
	case Before:
		return "BEFORE"
	case After:
		return "AFTER"
	case Concurrent:
		return "CONCURRENT"
	default:
		return "UNKNOWN"
	}
}

// Compare determines the causal relationship between two clocks.
//
// Returns:
//   - Before:     vc happened before other (all vc[i] <= other[i], at least one <)
//   - After:      vc happened after other  (all vc[i] >= other[i], at least one >)
//   - Concurrent: neither happened before the other — genuine CONFLICT
//
// Equal clocks are treated as After (not before).
func (vc VectorClock) Compare(other VectorClock) Ordering {
	vcLessOther := false // vc[i] < other[i] for some i
	otherLessVc := false // other[i] < vc[i] for some i

	// Check all keys from both clocks
	allKeys := unionKeys(vc, other)
	for _, k := range allKeys {
		if vc[k] < other[k] {
			vcLessOther = true
		}
		if vc[k] > other[k] {
			otherLessVc = true
		}
	}

	switch {
	case vcLessOther && !otherLessVc:
		return Before // vc < other
	case !vcLessOther && otherLessVc:
		return After // vc > other
	case !vcLessOther && !otherLessVc:
		return After // equal = not before
	default:
		return Concurrent // CONFLICT — genuine write conflict
	}
}

func (vc VectorClock) Clone() VectorClock {
	c := make(VectorClock, len(vc))
	for k, v := range vc {
		c[k] = v
	}
	return c
}

func (vc VectorClock) IsZero() bool {
	return len(vc) == 0
}

func unionKeys(a, b VectorClock) []string {
	seen := make(map[string]bool)
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	return keys
}
