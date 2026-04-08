package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVectorClock_NewIsEmpty(t *testing.T) {
	vc := NewVectorClock()
	assert.True(t, vc.IsZero())
	assert.Empty(t, vc)
}

func TestVectorClock_Increment(t *testing.T) {
	vc := NewVectorClock()
	vc = vc.Increment("node-1")
	assert.Equal(t, uint64(1), vc["node-1"])

	vc = vc.Increment("node-1")
	assert.Equal(t, uint64(2), vc["node-1"])

	vc = vc.Increment("node-2")
	assert.Equal(t, uint64(2), vc["node-1"])
	assert.Equal(t, uint64(1), vc["node-2"])
}

func TestVectorClock_Increment_DoesNotMutateOriginal(t *testing.T) {
	vc := NewVectorClock()
	vc["node-1"] = 5
	vc2 := vc.Increment("node-1")
	assert.Equal(t, uint64(5), vc["node-1"], "original should not be modified")
	assert.Equal(t, uint64(6), vc2["node-1"])
}

func TestVectorClock_Merge(t *testing.T) {
	a := VectorClock{"node-1": 3, "node-2": 1}
	b := VectorClock{"node-1": 2, "node-2": 4, "node-3": 1}

	merged := a.Merge(b)
	assert.Equal(t, uint64(3), merged["node-1"]) // max(3, 2) = 3
	assert.Equal(t, uint64(4), merged["node-2"]) // max(1, 4) = 4
	assert.Equal(t, uint64(1), merged["node-3"]) // max(0, 1) = 1
}

func TestVectorClock_Merge_DoesNotMutateOriginal(t *testing.T) {
	a := VectorClock{"node-1": 3}
	b := VectorClock{"node-1": 5}
	a.Merge(b)
	assert.Equal(t, uint64(3), a["node-1"], "Merge should not modify receiver")
}

func TestVectorClock_Compare_Before(t *testing.T) {
	// A = {node-1: 3, node-2: 1}
	// B = {node-1: 3, node-2: 2}
	// B > A because B[node-2]=2 > A[node-2]=1 and B[node-1]=A[node-1]
	a := VectorClock{"node-1": 3, "node-2": 1}
	b := VectorClock{"node-1": 3, "node-2": 2}
	assert.Equal(t, Before, a.Compare(b), "A should be before B")
}

func TestVectorClock_Compare_After(t *testing.T) {
	a := VectorClock{"node-1": 3, "node-2": 2}
	b := VectorClock{"node-1": 3, "node-2": 1}
	assert.Equal(t, After, a.Compare(b), "A should be after B")
}

func TestVectorClock_Compare_Concurrent(t *testing.T) {
	// C = {node-1: 4, node-2: 1}
	// D = {node-1: 3, node-2: 2}
	// C[node-1]=4 > D[node-1]=3 but C[node-2]=1 < D[node-2]=2
	// CONCURRENT — neither happened before the other — CONFLICT!
	c := VectorClock{"node-1": 4, "node-2": 1}
	d := VectorClock{"node-1": 3, "node-2": 2}
	assert.Equal(t, Concurrent, c.Compare(d), "C and D should be concurrent")
}

func TestVectorClock_Compare_Equal(t *testing.T) {
	a := VectorClock{"node-1": 3, "node-2": 1}
	b := VectorClock{"node-1": 3, "node-2": 1}
	assert.Equal(t, After, a.Compare(b), "equal clocks should be treated as After (not before)")
}

func TestVectorClock_Compare_EmptyClocks(t *testing.T) {
	a := NewVectorClock()
	b := NewVectorClock()
	assert.Equal(t, After, a.Compare(b))
}

func TestVectorClock_Compare_OneEmpty(t *testing.T) {
	a := VectorClock{"node-1": 1}
	b := NewVectorClock()
	assert.Equal(t, After, a.Compare(b))
	assert.Equal(t, Before, b.Compare(a))
}

func TestVectorClock_Clone(t *testing.T) {
	vc := VectorClock{"node-1": 3, "node-2": 1}
	clone := vc.Clone()
	assert.Equal(t, vc, clone)

	// Mutating clone should not affect original
	clone["node-1"] = 99
	assert.Equal(t, uint64(3), vc["node-1"])
}

func TestVectorClock_CausalChain(t *testing.T) {
	// Simulate a causal chain of writes
	// Alice writes on node-1
	alice := NewVectorClock()
	alice = alice.Increment("node-1") // {node-1: 1}

	// Bob reads Alice's write and then writes on node-2
	bob := alice.Merge(alice)     // sees {node-1: 1}
	bob = bob.Increment("node-2") // {node-1: 1, node-2: 1}

	// Alice's write happened before Bob's write
	assert.Equal(t, Before, alice.Compare(bob))
	assert.Equal(t, After, bob.Compare(alice))
}

func TestVectorClock_ConcurrentWrites(t *testing.T) {
	// Two clients write independently to the same key
	// Neither saw the other's write

	// Client A writes on node-1
	a := NewVectorClock()
	a = a.Increment("node-1") // {node-1: 1}

	// Client B writes on node-2 (independently, doesn't see A)
	b := NewVectorClock()
	b = b.Increment("node-2") // {node-2: 1}

	// These should be concurrent — genuine conflict
	assert.Equal(t, Concurrent, a.Compare(b))
	assert.Equal(t, Concurrent, b.Compare(a))
}

func TestOrdering_String(t *testing.T) {
	assert.Equal(t, "BEFORE", Before.String())
	assert.Equal(t, "AFTER", After.String())
	assert.Equal(t, "CONCURRENT", Concurrent.String())
}
