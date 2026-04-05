package ring

import (
	"fmt"
	"sort"
	"sync"

	"github.com/spaolacci/murmur3"
)

const DefaultVNodes = 150

// Ring represents a consistent hashing ring with virtual nodes
type Ring struct {
	mu         sync.RWMutex
	vnodes     []uint32            // sorted list of virtual node positions
	nodeMap    map[uint32]string   // maps vnode position to physical node ID
	nodes      map[string]bool     // set of active physical nodes
	vnodeCount int                 // number of virtual nodes per physical node
}

// New creates a new consistent hashing ring
func New(vnodeCount int) *Ring {
	if vnodeCount <= 0 {
		vnodeCount = DefaultVNodes
	}
	return &Ring{
		nodeMap:    make(map[uint32]string),
		nodes:      make(map[string]bool),
		vnodeCount: vnodeCount,
	}
}

// hash computes the murmur3 hash of a key
func (r *Ring) hash(key string) uint32 {
	return murmur3.Sum32([]byte(key))
}

// AddNode adds a physical node to the ring with its virtual nodes
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[nodeID] {
		return // already in ring
	}

	r.nodes[nodeID] = true

	// Create virtual nodes for this physical node
	for i := 0; i < r.vnodeCount; i++ {
		vnodeKey := fmt.Sprintf("%s#vnode%d", nodeID, i)
		pos := r.hash(vnodeKey)
		r.vnodes = append(r.vnodes, pos)
		r.nodeMap[pos] = nodeID
	}

	// Keep vnodes sorted for binary search
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i] < r.vnodes[j]
	})
}

// RemoveNode removes a physical node from the ring
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodes[nodeID] {
		return // not in ring
	}

	delete(r.nodes, nodeID)

	// Rebuild vnodes without this node's positions
	var newVnodes []uint32
	newNodeMap := make(map[uint32]string)

	for _, pos := range r.vnodes {
		if r.nodeMap[pos] != nodeID {
			newVnodes = append(newVnodes, pos)
			newNodeMap[pos] = r.nodeMap[pos]
		}
	}

	r.vnodes = newVnodes
	r.nodeMap = newNodeMap
}

// GetNode returns the primary node responsible for a key
func (r *Ring) GetNode(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return "", false
	}

	pos := r.hash(key)
	idx := r.search(pos)

	return r.nodeMap[r.vnodes[idx]], true
}

// GetNodes returns the next n distinct physical nodes for replication
// The first node is the primary, subsequent nodes are replicas
func (r *Ring) GetNodes(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return nil
	}

	if n > len(r.nodes) {
		n = len(r.nodes)
	}

	pos := r.hash(key)
	idx := r.search(pos)

	seen := make(map[string]bool, n)
	result := make([]string, 0, n)

	for len(result) < n {
		nodeID := r.nodeMap[r.vnodes[idx%len(r.vnodes)]]
		if !seen[nodeID] {
			seen[nodeID] = true
			result = append(result, nodeID)
		}
		idx++
	}

	return result
}

// search uses binary search to find the first vnode >= pos
// Returns the index in the vnodes slice (with wraparound)
func (r *Ring) search(pos uint32) int {
	n := len(r.vnodes)
	idx := sort.Search(n, func(i int) bool {
		return r.vnodes[i] >= pos
	})
	return idx % n // wrap around to beginning if not found
}

// Distribution returns the percentage of the ring owned by each node
// This is useful for verifying the ring is balanced
func (r *Ring) Distribution() map[string]float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Count vnodes per physical node
	counts := make(map[string]int)
	for _, nodeID := range r.nodeMap {
		counts[nodeID]++
	}

	dist := make(map[string]float64)
	total := float64(len(r.vnodes))

	for nodeID, count := range counts {
		dist[nodeID] = float64(count) / total * 100
	}

	return dist
}

// NodeCount returns the number of physical nodes in the ring
func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// HasNode checks if a node exists in the ring
func (r *Ring) HasNode(nodeID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodes[nodeID]
}

// GetAllNodes returns all physical nodes in the ring
func (r *Ring) GetAllNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, 0, len(r.nodes))
	for nodeID := range r.nodes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}
