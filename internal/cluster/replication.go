package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

const ReplicaTimeout = 200 * time.Millisecond

type replicaResult struct {
	nodeID string
	entry  storage.Entry
	err    error
}

// versionCounter is a per-node atomic version generator
type versionCounter struct {
	counter atomic.Uint64
}

func (vc *versionCounter) next() uint64 {
	return vc.counter.Add(1)
}

// ReplicatedPut writes to N replicas and waits for W acks
func (n *Node) ReplicatedPut(ctx context.Context, key string, value []byte) error {
	replicas := n.ring.GetNodes(key, n.cfg.N)
	if len(replicas) == 0 {
		return ErrNoNodesAvailable
	}

	results := make(chan replicaResult, len(replicas))
	version := n.versionCounter.next()

	// Send write to ALL replicas concurrently
	for _, nodeID := range replicas {
		go func(nid string) {
			rCtx, cancel := context.WithTimeout(ctx, ReplicaTimeout)
			defer cancel()
			var err error
			if nid == n.cfg.NodeID {
				// Local write — directly into our LSM engine
				err = n.engine.Put(key, value)
			} else {
				// Remote write — HTTP call to peer
				err = n.remoteReplicate(rCtx, nid, key, value, false, version)
			}
			if err != nil {
				// Store hint for delivery when node recovers
				go n.hints.Store(key, value, nid)
			}
			results <- replicaResult{nodeID: nid, err: err}
		}(nodeID)
	}

	// Count acks — return success when W acks received
	acks, failures := 0, 0
	for i := 0; i < len(replicas); i++ {
		res := <-results
		if res.err == nil {
			acks++
			if acks >= n.cfg.W {
				return nil // quorum reached!
			}
		} else {
			failures++
			n.logger.Warn("replica write failed",
				"node", res.nodeID, "error", res.err)
			// If too many failures, quorum impossible
			if failures > len(replicas)-n.cfg.W {
				return fmt.Errorf("write quorum failed: %d/%d acks", acks, n.cfg.W)
			}
		}
	}
	return fmt.Errorf("write quorum failed: only %d/%d acks", acks, n.cfg.W)
}

// ReplicatedGet reads from R replicas and returns highest version
func (n *Node) ReplicatedGet(ctx context.Context, key string) ([]byte, error) {
	entry, err := n.replicatedGetEntry(ctx, key)
	if err != nil {
		return nil, err
	}
	if entry.Tombstone {
		return nil, storage.ErrKeyNotFound
	}
	return entry.Value, nil
}

// replicatedGetEntry returns the full Entry (including version) for internal use
func (n *Node) replicatedGetEntry(ctx context.Context, key string) (storage.Entry, error) {
	replicas := n.ring.GetNodes(key, n.cfg.N)
	if len(replicas) == 0 {
		return storage.Entry{}, ErrNoNodesAvailable
	}

	results := make(chan replicaResult, len(replicas))

	for _, nodeID := range replicas {
		go func(nid string) {
			rCtx, cancel := context.WithTimeout(ctx, ReplicaTimeout)
			defer cancel()
			var entry storage.Entry
			var err error
			if nid == n.cfg.NodeID {
				entry, err = n.engine.Get(key)
			} else {
				entry, err = n.remoteGet(rCtx, nid, key)
			}
			results <- replicaResult{nodeID: nid, entry: entry, err: err}
		}(nodeID)
	}

	var responses []replicaResult
	for i := 0; i < len(replicas); i++ {
		res := <-results
		if res.err == nil || errors.Is(res.err, storage.ErrKeyNotFound) {
			responses = append(responses, res)
			if len(responses) >= n.cfg.R {
				// Got R responses — find highest version
				latest := latestEntry(responses)
				// Read repair: update stale replicas
				go n.readRepair(ctx, key, latest, responses)
				if latest.entry.Tombstone {
					return storage.Entry{}, storage.ErrKeyNotFound
				}
				return latest.entry, nil
			}
		}
	}
	return storage.Entry{}, fmt.Errorf("read quorum failed")
}

// ReplicatedDelete deletes a key from all replicas with tombstone
func (n *Node) ReplicatedDelete(ctx context.Context, key string) error {
	replicas := n.ring.GetNodes(key, n.cfg.N)
	if len(replicas) == 0 {
		return ErrNoNodesAvailable
	}

	results := make(chan replicaResult, len(replicas))
	version := n.versionCounter.next()

	// Send delete (tombstone) to ALL replicas concurrently
	for _, nodeID := range replicas {
		go func(nid string) {
			rCtx, cancel := context.WithTimeout(ctx, ReplicaTimeout)
			defer cancel()
			var err error
			if nid == n.cfg.NodeID {
				err = n.engine.Delete(key)
				if errors.Is(err, storage.ErrKeyNotFound) {
					err = nil // deleting non-existent key is fine
				}
			} else {
				// Remote delete via tombstone
				err = n.remoteReplicate(rCtx, nid, key, nil, true, version)
			}
			if err != nil {
				go n.hints.Store(key, nil, nid)
			}
			results <- replicaResult{nodeID: nid, err: err}
		}(nodeID)
	}

	// Count acks
	acks, failures := 0, 0
	for i := 0; i < len(replicas); i++ {
		res := <-results
		if res.err == nil {
			acks++
			if acks >= n.cfg.W {
				return nil
			}
		} else {
			failures++
			n.logger.Warn("replica delete failed",
				"node", res.nodeID, "error", res.err)
			if failures > len(replicas)-n.cfg.W {
				return fmt.Errorf("delete quorum failed: %d/%d acks", acks, n.cfg.W)
			}
		}
	}
	return fmt.Errorf("delete quorum failed: only %d/%d acks", acks, n.cfg.W)
}

// latestEntry finds the response with the highest version number
func latestEntry(responses []replicaResult) replicaResult {
	latest := responses[0]
	for _, r := range responses[1:] {
		if r.entry.Version > latest.entry.Version {
			latest = r
		}
	}
	return latest
}

// readRepair proactively updates replicas that have stale data
func (n *Node) readRepair(ctx context.Context, key string,
	latest replicaResult, all []replicaResult) {
	for _, r := range all {
		if r.entry.Version < latest.entry.Version && r.nodeID != n.cfg.NodeID {
			n.logger.Debug("read repair",
				"node", r.nodeID,
				"stale", r.entry.Version,
				"latest", latest.entry.Version)
			rCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			n.remoteReplicate(rCtx, r.nodeID, key, latest.entry.Value, false, latest.entry.Version)
			cancel()
		}
	}
}

// remoteReplicate sends a write to another node via HTTP
func (n *Node) remoteReplicate(ctx context.Context,
	nodeID, key string, value []byte, tombstone bool, version uint64) error {
	addr, ok := n.cluster.nodeAddrMap[nodeID]
	if !ok {
		// Try using nodeID directly as address (fallback for seed nodes)
		addr = nodeID
	}

	body, _ := json.Marshal(map[string]any{
		"key":       key,
		"value":     value,
		"tombstone": tombstone,
		"version":   version,
	})

	url := fmt.Sprintf("%s/internal/replicate", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create replicate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("replicate request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("replicate failed: status %d", resp.StatusCode)
	}
	return nil
}

// remoteGet fetches an entry from another node via HTTP
func (n *Node) remoteGet(ctx context.Context, nodeID, key string) (storage.Entry, error) {
	addr, ok := n.cluster.nodeAddrMap[nodeID]
	if !ok {
		addr = nodeID
	}

	body, _ := json.Marshal(map[string]any{"key": key})
	url := fmt.Sprintf("%s/internal/replicate/get", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return storage.Entry{}, fmt.Errorf("create get request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return storage.Entry{}, fmt.Errorf("remote get failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return storage.Entry{}, storage.ErrKeyNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return storage.Entry{}, fmt.Errorf("remote get failed: status %d", resp.StatusCode)
	}

	var result struct {
		Key       string `json:"key"`
		Value     []byte `json:"value"`
		Version   uint64 `json:"version"`
		Tombstone bool   `json:"tombstone"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return storage.Entry{}, fmt.Errorf("decode response: %w", err)
	}

	return storage.Entry{
		Key:       result.Key,
		Value:     result.Value,
		Version:   result.Version,
		Tombstone: result.Tombstone,
	}, nil
}
