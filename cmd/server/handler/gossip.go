package handler

import (
	"encoding/json"
	"net/http"

	rpc "github.com/DevLikhith5/kasoku/internal/rpc"
	storage "github.com/DevLikhith5/kasoku/internal/store"
)

func (s *Server) handleGossipState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req rpc.GossipStateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	membership := make(map[string]string)
	if s.cluster != nil {
		for nodeID, memberInfo := range s.cluster.GetMembership() {
			membership[nodeID] = memberInfo.Addr
		}
	}

	resp := rpc.GossipStateResponse{
		NodeID:        s.nodeID,
		NodeAddr:      s.addr,
		Version:       1,
		LastHeartbeat: 0,
		Membership:    membership,
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    resp,
	})
}

func (s *Server) handleMerkleRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	keys, err := s.store.Keys()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to get keys")
		return
	}

	tree := buildMerkleTree(keys)
	rootHash := tree.RootHash()

	resp := rpc.MerkleRootResponse{
		RootHash: rootHash,
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    resp,
	})
}

func (s *Server) handleMerkleDiff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req rpc.KeyDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	allKeys, err := s.store.Keys()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to get keys")
		return
	}

	keySet := make(map[string]bool)
	for _, k := range req.Keys {
		keySet[k] = true
	}

	var diffKeys []string
	for _, k := range allKeys {
		if !keySet[k] {
			diffKeys = append(diffKeys, k)
		}
	}

	resp := rpc.KeyDiffResponse{
		Keys: diffKeys,
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    resp,
	})
}

type merkleNode struct {
	hash  []byte
	left  *merkleNode
	right *merkleNode
}

type MerkleTree struct {
	root *merkleNode
	dept int
}

func buildMerkleTree(keys []string) *MerkleTree {
	if len(keys) == 0 {
		return &MerkleTree{}
	}

	nodes := make([]*merkleNode, len(keys))
	for i, k := range keys {
		nodes[i] = &merkleNode{
			hash: simpleHash([]byte(k)),
		}
	}

	for len(nodes) > 1 {
		var newLevel []*merkleNode
		for i := 0; i < len(nodes); i += 2 {
			if i+1 < len(nodes) {
				combined := append(nodes[i].hash, nodes[i+1].hash...)
				newLevel = append(newLevel, &merkleNode{
					hash:  simpleHash(combined),
					left:  nodes[i],
					right: nodes[i+1],
				})
			} else {
				newLevel = append(newLevel, nodes[i])
			}
		}
		nodes = newLevel
	}

	return &MerkleTree{
		root: nodes[0],
		dept: 10,
	}
}

func (m *MerkleTree) RootHash() []byte {
	if m.root == nil {
		return []byte{}
	}
	return m.root.hash
}

func simpleHash(data []byte) []byte {
	sum := uint64(0)
	for i, b := range data {
		sum += uint64(b) * uint64(i+1)
	}
	h := make([]byte, 8)
	for i := range h {
		h[i] = byte(sum >> (i * 8))
	}
	return h
}

type Entry = storage.Entry
