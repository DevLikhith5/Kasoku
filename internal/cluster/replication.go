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

type replicaResult struct {
	nodeID string
	entry  storage.Entry
	err    error
}

type versionCounter struct {
	counter atomic.Uint64
}

func (vc *versionCounter) next() uint64 {
	return vc.counter.Add(1)
}

func (n *Node) ReplicatedPut(ctx context.Context, key string, value []byte) error {
	replicas := n.ring.GetNodes(key, n.cfg.N)
	if len(replicas) == 0 {
		return ErrNoNodesAvailable
	}

	results := make(chan replicaResult, len(replicas))
	version := n.versionCounter.next()

	// Adaptive timeout
	timeout := n.timeoutTracker.TimeoutForReplicas(replicas)

	// fanout concurrent worker pattern
	var quorumReached atomic.Bool
	for _, nodeID := range replicas {
		go func(nid string) {
			start := time.Now()
			rCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			var err error
			if nid == n.cfg.NodeID {
				// Local write — directly into our LSM engine
				err = n.engine.Put(key, value)
			} else {
				// Remote write — HTTP call to peer
				err = n.remoteReplicate(rCtx, nid, key, value, false, version)
			}
			// Record latency for adaptive timeout
			n.timeoutTracker.Record(nid, time.Since(start))

			if err != nil {
				// Bug 13 fix: store hint synchronously to avoid unbounded goroutine growth
				_ = n.hints.Store(key, value, nid)
			}
			results <- replicaResult{nodeID: nid, err: err}
		}(nodeID)
	}

	// Count acks — wait for ALL replicas to respond
	acks, failures := 0, 0
	for range replicas {
		res := <-results
		if res.err == nil {
			acks++
			if acks >= n.cfg.W {
				quorumReached.Store(true)
			}
		} else {
			failures++
			n.logger.Warn("replica write failed",
				"node", res.nodeID, "error", res.err)
		}
	}

	if quorumReached.Load() {
		return nil
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

	// Adaptive timeout based on historical latencies
	timeout := n.timeoutTracker.TimeoutForReplicas(replicas)

	for _, nodeID := range replicas {
		go func(nid string) {
			start := time.Now()
			rCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			var entry storage.Entry
			var err error
			if nid == n.cfg.NodeID {
				entry, err = n.engine.Get(key)
			} else {
				entry, err = n.remoteGet(rCtx, nid, key)
			}
			// Record latency for adaptive timeout
			n.timeoutTracker.Record(nid, time.Since(start))

			results <- replicaResult{nodeID: nid, entry: entry, err: err}
		}(nodeID)
	}

	var responses []replicaResult
	for range replicas {
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

	// Adaptive timeout based on historical latencies
	timeout := n.timeoutTracker.TimeoutForReplicas(replicas)

	// Send delete (tombstone) to ALL replicas concurrently
	var quorumReached atomic.Bool
	for _, nodeID := range replicas {
		go func(nid string) {
			start := time.Now()
			rCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			var err error
			if nid == n.cfg.NodeID {
				// Bug 14 fix: local delete uses the engine's Delete which writes a tombstone
				// in the LSM (the engine.Delete writes a tombstone entry, not a hard delete)
				err = n.engine.Delete(key)
				if errors.Is(err, storage.ErrKeyNotFound) {
					err = nil // deleting non-existent key is fine
				}
			} else {
				// Remote delete via tombstone
				err = n.remoteReplicate(rCtx, nid, key, nil, true, version)
			}
			// Record latency for adaptive timeout
			n.timeoutTracker.Record(nid, time.Since(start))

			if err != nil {
				// Bug 13 fix: store hint synchronously to avoid unbounded goroutine growth
				_ = n.hints.Store(key, nil, nid)
			}
			results <- replicaResult{nodeID: nid, err: err}
		}(nodeID)
	}

	// Count acks — wait for ALL replicas to respond
	acks, failures := 0, 0
	for range replicas {
		res := <-results
		if res.err == nil {
			acks++
			if acks >= n.cfg.W {
				quorumReached.Store(true)
			}
		} else {
			failures++
			n.logger.Warn("replica delete failed",
				"node", res.nodeID, "error", res.err)
		}
	}

	if quorumReached.Load() {
		return nil
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
			// Bug 3 fix: log and store hint on read repair failure instead of silently ignoring
			if err := n.remoteReplicate(rCtx, r.nodeID, key, latest.entry.Value, false, latest.entry.Version); err != nil {
				n.logger.Warn("read repair failed, storing hint",
					"node", r.nodeID, "key", key, "error", err)
				_ = n.hints.Store(key, latest.entry.Value, r.nodeID)
			}
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

	// Bug 4 fix: check json.Marshal error instead of silently discarding it
	body, err := json.Marshal(map[string]any{
		"key":       key,
		"value":     value,
		"tombstone": tombstone,
		"version":   version,
	})
	if err != nil {
		return fmt.Errorf("marshal replicate request: %w", err)
	}

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

	// Bug 4 fix: check json.Marshal error
	body, err := json.Marshal(map[string]any{"key": key})
	if err != nil {
		return storage.Entry{}, fmt.Errorf("marshal get request: %w", err)
	}
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
