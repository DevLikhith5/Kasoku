package merkle

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuild_EmptyKeys(t *testing.T) {
	tree := Build(nil, nil)
	assert.NotNil(t, tree)
	assert.Equal(t, sha256.Sum256(nil), tree.Hash)
}

func TestBuild_SingleKey(t *testing.T) {
	keys := []string{"user:42"}
	tree := Build(keys, func(k string) []byte {
		return []byte("Alice")
	})
	assert.NotNil(t, tree)
	assert.True(t, tree.IsLeaf)
	assert.Equal(t, keys, tree.Keys)
}

func TestBuild_FourKeys_SingleLeaf(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	tree := Build(keys, func(k string) []byte {
		return []byte("val-" + k)
	})
	assert.True(t, tree.IsLeaf)
	assert.Equal(t, 4, len(tree.Keys))
}

func TestBuild_FiveKeys_TwoLeaves(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e"}
	tree := Build(keys, func(k string) []byte {
		return []byte("val-" + k)
	})
	assert.False(t, tree.IsLeaf)
	assert.NotNil(t, tree.Left)
	assert.NotNil(t, tree.Right)
}

func TestDiff_IdenticalTrees(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e", "f"}
	sort.Strings(keys)
	getValue := func(k string) []byte { return []byte("val-" + k) }

	tree1 := Build(keys, getValue)
	tree2 := Build(keys, getValue)

	diff := Diff(tree1, tree2)
	assert.Empty(t, diff, "identical trees should produce no diff")
}

func TestDiff_SingleKeyDifferent(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	sort.Strings(keys)

	tree1 := Build(keys, func(k string) []byte { return []byte("val-" + k) })
	tree2 := Build(keys, func(k string) []byte {
		if k == "e" {
			return []byte("CHANGED")
		}
		return []byte("val-" + k)
	})

	diff := Diff(tree1, tree2)
	assert.NotEmpty(t, diff, "should detect difference")
	assert.Contains(t, diff, "e", "diff should contain the changed key")
}

func TestDiff_MissingKey(t *testing.T) {
	keys1 := []string{"a", "b", "c", "d", "e"}
	keys2 := []string{"a", "b", "c", "d"} // missing "e"
	sort.Strings(keys1)
	sort.Strings(keys2)

	getValue := func(k string) []byte { return []byte("val-" + k) }

	tree1 := Build(keys1, getValue)
	tree2 := Build(keys2, getValue)

	diff := Diff(tree1, tree2)
	assert.NotEmpty(t, diff, "should detect missing key")
}

func TestDiff_CompletelyDifferent(t *testing.T) {
	keys1 := []string{"a", "b", "c"}
	keys2 := []string{"x", "y", "z"}

	getValue := func(k string) []byte { return []byte("val-" + k) }

	tree1 := Build(keys1, getValue)
	tree2 := Build(keys2, getValue)

	diff := Diff(tree1, tree2)
	assert.NotEmpty(t, diff)
}

func TestDiff_NilTrees(t *testing.T) {
	diff := Diff(nil, nil)
	assert.Empty(t, diff)
}

func TestDiff_OneNilTree(t *testing.T) {
	keys := []string{"a", "b"}
	tree := Build(keys, func(k string) []byte { return []byte("v") })
	diff := Diff(tree, nil)
	assert.NotEmpty(t, diff)
}

func TestSerializeDeserialize(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e"}
	tree := Build(keys, func(k string) []byte { return []byte("val-" + k) })

	data, err := Serialize(tree)
	require.NoError(t, err)

	tree2, err := Deserialize(data)
	require.NoError(t, err)

	assert.Equal(t, tree.Hash, tree2.Hash)
}

func TestRootHash(t *testing.T) {
	keys := []string{"a", "b", "c"}
	getValue := func(k string) []byte { return []byte("val-" + k) }

	tree1 := Build(keys, getValue)
	tree2 := Build(keys, getValue)

	assert.Equal(t, RootHash(tree1), RootHash(tree2))
}

func TestBuild_LargeDataset(t *testing.T) {
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%04d", i)
	}
	sort.Strings(keys)

	tree := Build(keys, func(k string) []byte {
		return []byte("value-for-" + k)
	})
	assert.NotNil(t, tree)
	assert.False(t, tree.IsLeaf)

	// Changing one key should produce a small diff
	tree2 := Build(keys, func(k string) []byte {
		if k == "key:0500" {
			return []byte("CHANGED")
		}
		return []byte("value-for-" + k)
	})
	diff := Diff(tree, tree2)
	assert.NotEmpty(t, diff)
	assert.Contains(t, diff, "key:0500")
	// The diff should be small relative to the total dataset
	assert.Less(t, len(diff), 50, "diff should be much smaller than total dataset")
}
