package ring

import (
	"fmt"
	"slices"
	"sort"
	"sync"

	"hash/crc32"
)

const DefaultVNodes = 150

type Ring struct {
	mu         sync.RWMutex
	vnodes     []uint32
	nodeMap    map[uint32]string
	nodes      map[string]bool
	vnodeCount int
}

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

func (r *Ring) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[nodeID] {
		return
	}

	r.nodes[nodeID] = true

	for i := 0; i < r.vnodeCount; i++ {
		vnodeKey := fmt.Sprintf("%s#vnode%d", nodeID, i)
		pos := r.hash(vnodeKey)
		r.vnodes = append(r.vnodes, pos)
		r.nodeMap[pos] = nodeID
	}

	slices.Sort(r.vnodes)
}

func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodes[nodeID] {
		return
	}

	delete(r.nodes, nodeID)

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

func (r *Ring) search(pos uint32) int {
	n := len(r.vnodes)
	idx := sort.Search(n, func(i int) bool {
		return r.vnodes[i] >= pos
	})
	return idx % n
}

func (r *Ring) Distribution() map[string]float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

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

func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

func (r *Ring) HasNode(nodeID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodes[nodeID]
}

func (r *Ring) GetAllNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, 0, len(r.nodes))
	for nodeID := range r.nodes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}
