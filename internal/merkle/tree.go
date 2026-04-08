// Package merkle implements Merkle hash trees for efficient data synchronization
// between cluster nodes. Two identical datasets produce identical root hashes;
// differing datasets can be reconciled in O(K log N) messages by comparing
// subtree hashes top-down, where K is the number of differing keys and N is total keys.
package merkle

import (
	"crypto/sha256"
	"encoding/json"
	"sort"
)

// Node represents a node in the Merkle tree
type Node struct {
	Hash   [32]byte `json:"hash"`
	Left   *Node    `json:"left,omitempty"`
	Right  *Node    `json:"right,omitempty"`
	IsLeaf bool     `json:"is_leaf"`
	Keys   []string `json:"keys,omitempty"` // only set on leaf nodes
}

// Build constructs a Merkle tree from sorted keys.
// getValue is called to get the value for each key.
// Keys MUST be sorted before calling Build.
func Build(keys []string, getValue func(string) []byte) *Node {
	if len(keys) == 0 {
		return &Node{Hash: sha256.Sum256(nil)}
	}
	if len(keys) <= 4 { // leaf: covers up to 4 keys
		return buildLeaf(keys, getValue)
	}
	mid := len(keys) / 2
	left := Build(keys[:mid], getValue)
	right := Build(keys[mid:], getValue)
	combined := append(left.Hash[:], right.Hash[:]...)
	return &Node{
		Hash:  sha256.Sum256(combined),
		Left:  left,
		Right: right,
	}
}

func buildLeaf(keys []string, getValue func(string) []byte) *Node {
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write(getValue(k))
	}
	var hash [32]byte
	copy(hash[:], h.Sum(nil))
	return &Node{Hash: hash, IsLeaf: true, Keys: keys}
}

// Diff returns keys that differ between two trees.
// This is where the magic happens: O(K log N) instead of O(N),
// because identical subtrees are skipped entirely via hash comparison.
func Diff(local, remote *Node) []string {
	if local == nil && remote == nil {
		return nil
	}
	if local == nil || remote == nil {
		// One tree is nil — collect all keys from the other
		return collectKeys(local, remote)
	}
	if local.Hash == remote.Hash {
		return nil // identical subtree — skip entirely
	}

	if local.IsLeaf || remote.IsLeaf {
		// Found differing leaf — return all keys for comparison
		seen := make(map[string]bool)
		for _, k := range local.Keys {
			seen[k] = true
		}
		for _, k := range remote.Keys {
			seen[k] = true
		}
		result := make([]string, 0, len(seen))
		for k := range seen {
			result = append(result, k)
		}
		sort.Strings(result)
		return result
	}
	// Recurse into both subtrees
	diff := Diff(local.Left, remote.Left)
	diff = append(diff, Diff(local.Right, remote.Right)...)
	return diff
}

// collectKeys gathers all keys from whichever node is non-nil
func collectKeys(a, b *Node) []string {
	n := a
	if n == nil {
		n = b
	}
	if n == nil {
		return nil
	}
	if n.IsLeaf {
		result := make([]string, len(n.Keys))
		copy(result, n.Keys)
		return result
	}
	keys := collectKeys(n.Left, nil)
	keys = append(keys, collectKeys(n.Right, nil)...)
	return keys
}

// RootHash returns the root hash of the tree as a hex string for easy comparison
func RootHash(n *Node) [32]byte {
	if n == nil {
		return sha256.Sum256(nil)
	}
	return n.Hash
}

// Serialize encodes the Merkle tree to JSON for transmission between nodes
func Serialize(n *Node) ([]byte, error) {
	return json.Marshal(n)
}

// Deserialize decodes a Merkle tree from JSON
func Deserialize(data []byte) (*Node, error) {
	var n Node
	if err := json.Unmarshal(data, &n); err != nil {
		return nil, err
	}
	return &n, nil
}
