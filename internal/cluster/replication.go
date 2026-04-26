package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

var (
	httpClient     *http.Client
	httpClientOnce sync.Once
)

func getHTTPClient() *http.Client {
	httpClientOnce.Do(func() {
		httpClient = &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        500,
				MaxIdleConnsPerHost: 300,
				MaxConnsPerHost:     300,
				IdleConnTimeout:     90 * time.Second,
				DisableKeepAlives:   false,
			},
			Timeout: 10 * time.Second,
		}
	})
	return httpClient
}

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
	// Get preferred (original) nodes from ring - needed to identify fallbacks for hints
	preferred := n.ring.GetNodes(key, n.cfg.N)
	if len(preferred) == 0 {
		return ErrNoNodesAvailable
	}

	// Get actual replicas (includes fallback nodes if some preferred are down)
	aliveSet := n.cluster.snapshotAliveSet()
	replicas := n.cluster.getReplicasForKey(key, aliveSet)
	if len(replicas) == 0 {
		return ErrNoNodesAvailable
	}

	// Generate vector clock for this write
	vc := n.getOrCreateVectorClock(key)
	vc = vc.Increment(n.cfg.NodeID)

	// FAST PATH: W=1 local-first - write locally only for max throughput
	if n.cfg.W == 1 {
		return n.engine.PutWithVectorClock(key, value, vc)
	}

	// SLOW PATH: W>1 - wait for quorum
	results := make(chan replicaResult, len(replicas))
	timeout := n.timeoutTracker.TimeoutForReplicas(replicas)

	// Build map: which preferred nodes are actually in the replica set?
	preferredInSet := make(map[string]bool)
	for _, p := range preferred {
		for _, r := range replicas {
			if p == r {
				preferredInSet[p] = true
				break
			}
		}
	}

	// Determine fallback mapping: fallback node -> dead target node
	fallbackToTarget := make(map[string]string)
	usedPreferred := make(map[string]bool)
	for _, p := range preferred {
		for _, r := range replicas {
			if p == r {
				usedPreferred[p] = true
				break
			}
		}
	}
	for _, r := range replicas {
		if !preferredInSet[r] {
			// This is a fallback - find the dead preferred node
			for _, p := range preferred {
				if !usedPreferred[p] {
					fallbackToTarget[r] = p
					usedPreferred[p] = true
					break
				}
			}
		}
	}

	for _, nodeID := range replicas {
		go func(nid string) {
			start := time.Now()
			rCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			var err error
			if nid == n.cfg.NodeID {
				err = n.engine.PutWithVectorClock(key, value, vc)
			} else {
				targetNode := fallbackToTarget[nid] // empty if primary, set if fallback
				err = n.remoteReplicateWithVC(rCtx, nid, key, value, false, vc, targetNode)
			}
			n.timeoutTracker.Record(nid, time.Since(start))

			if err != nil && preferredInSet[nid] {
				_ = n.hints.Store(key, value, nid)
			}
			results <- replicaResult{nodeID: nid, err: err}
		}(nodeID)
	}

	// Count acks — EARLY EXIT once quorum reached
	acks, failures := 0, 0
	for i := 0; i < len(replicas); i++ {
		res := <-results
		if res.err == nil {
			acks++
		} else {
			failures++
			n.logger.Debug("replica write failed", "node", res.nodeID, "error", res.err)
		}

		if acks >= n.cfg.W {
			return nil
		}
		if failures > len(replicas)-n.cfg.W {
			break
		}
	}

	return fmt.Errorf("write quorum failed: only %d/%d acks", acks, n.cfg.W)
}

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

func (n *Node) replicatedGetEntry(ctx context.Context, key string) (storage.Entry, error) {
	replicas := n.ring.GetNodes(key, n.cfg.N)
	if len(replicas) == 0 {
		return storage.Entry{}, ErrNoNodesAvailable
	}

	// FAST PATH: R=1 local-first - read from local, no RPC needed
	if n.cfg.R == 1 {
		entry, err := n.engine.Get(key)
		if err == nil || errors.Is(err, storage.ErrKeyNotFound) {
			return entry, err
		}
		// If local read fails, try peers in background
	}

	// SLOW PATH: R>1 - wait for quorum
	results := make(chan replicaResult, len(replicas))

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
			n.timeoutTracker.Record(nid, time.Since(start))

			results <- replicaResult{nodeID: nid, entry: entry, err: err}
		}(nodeID)
	}

	var responses []replicaResult
	failures := 0
	for i := 0; i < len(replicas); i++ {
		res := <-results
		if res.err == nil || errors.Is(res.err, storage.ErrKeyNotFound) {
			responses = append(responses, res)
			if len(responses) >= n.cfg.R {
				// Got R responses — find highest version
				latest := latestEntry(responses)
				// Read repair: update stale replicas in background
				go n.readRepair(ctx, key, latest, responses)
				if latest.entry.Tombstone {
					return storage.Entry{}, storage.ErrKeyNotFound
				}
				return latest.entry, nil
			}
		} else {
			failures++
		}

		if failures > len(replicas)-n.cfg.R {
			break
		}
	}
	return storage.Entry{}, fmt.Errorf("read quorum failed")
}

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

	// Count acks — EARLY EXIT once quorum reached
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
			n.logger.Warn("replica delete failed", "node", res.nodeID, "error", res.err)
		}
		
		if failures > len(replicas)-n.cfg.W {
			break
		}
	}

	return fmt.Errorf("delete quorum failed: only %d/%d acks", acks, n.cfg.W)
}

func latestEntry(responses []replicaResult) replicaResult {
	latest := responses[0]
	for _, r := range responses[1:] {
		ord := compareVectorClocks(r.entry.VectorClock, latest.entry.VectorClock)
		if ord == After {
			latest = r
		}
	}
	return latest
}

func compareVectorClocks(a, b storage.VectorClock) Ordering {
	if a == nil {
		return After
	}
	if b == nil {
		return Before
	}

	aLessB := false
	bLessA := false

	allKeys := make(map[string]bool)
	for k := range a {
		allKeys[k] = true
	}
	for k := range b {
		allKeys[k] = true
	}

	for k := range allKeys {
		av := a[k]
		bv := b[k]
		if av < bv {
			aLessB = true
		}
		if av > bv {
			bLessA = true
		}
	}

	if aLessB && !bLessA {
		return Before
	}
	if !aLessB && bLessA {
		return After
	}
	if !aLessB && !bLessA {
		return After
	}
	return Concurrent
}

func (n *Node) readRepair(ctx context.Context, key string,
	latest replicaResult, all []replicaResult) {
	// Rate-limited read repair
	for _, r := range all {
		if r.entry.Version < latest.entry.Version && r.nodeID != n.cfg.NodeID {
			if n.repSemaphore != nil && !n.repSemaphore.TryAcquire() {
				continue
			}
			go func(r replicaResult) {
				if n.repSemaphore != nil {
					defer n.repSemaphore.Release()
				}
				n.logger.Debug("read repair",
					"node", r.nodeID,
					"stale", r.entry.Version,
					"latest", latest.entry.Version)
				rCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
				if err := n.remoteReplicate(rCtx, r.nodeID, key, latest.entry.Value, false, latest.entry.Version); err != nil {
					n.logger.Warn("read repair failed, storing hint",
						"node", r.nodeID, "key", key, "error", err)
					_ = n.hints.Store(key, latest.entry.Value, r.nodeID)
				}
			}(r)
		}
	}
}

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

	resp, err := getHTTPClient().Do(req)
	if err != nil {
		return fmt.Errorf("replicate request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("replicate failed: status %d", resp.StatusCode)
	}
	return nil
}

// remoteReplicateWithVC sends replication request with vector clock
// targetNode is the original intended replica (for hinted handoff on fallback nodes)
func (n *Node) remoteReplicateWithVC(ctx context.Context,
	nodeID, key string, value []byte, tombstone bool, vc storage.VectorClock, targetNodes ...string) error {
	addr, ok := n.cluster.nodeAddrMap[nodeID]
	if !ok {
		addr = nodeID
	}

	vcMap := map[string]uint64(vc)
	bodyMap := map[string]any{
		"key":          key,
		"value":        value,
		"tombstone":    tombstone,
		"vector_clock": vcMap,
	}
	// Include hint: original target node(s) for this replica
	if len(targetNodes) > 0 && targetNodes[0] != "" {
		bodyMap["target_node"] = targetNodes[0]
	}
	body, err := json.Marshal(bodyMap)
	if err != nil {
		return fmt.Errorf("marshal replicate request: %w", err)
	}

	url := fmt.Sprintf("%s/internal/replicate", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create replicate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := getHTTPClient().Do(req)
	if err != nil {
		return fmt.Errorf("replicate request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("replicate failed: status %d", resp.StatusCode)
	}
	return nil
}

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

	resp, err := getHTTPClient().Do(req)
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
