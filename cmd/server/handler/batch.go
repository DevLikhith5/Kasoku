package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	rpc "github.com/DevLikhith5/kasoku/internal/rpc"
	storage "github.com/DevLikhith5/kasoku/internal/store"
)

type batchPutRequestEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type batchPutRequest struct {
	Entries []batchPutRequestEntry `json:"entries"`
}

type batchPutResponse struct {
	Applied int `json:"applied"`
	Errors  int `json:"errors"`
}

type batchGetRequest struct {
	Keys []string `json:"keys"`
}

type batchGetEntry struct {
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
	Found bool   `json:"found"`
}

type batchGetResponse struct {
	Found   int             `json:"found"`
	Missing int             `json:"missing"`
	Entries []batchGetEntry `json:"entries,omitempty"`
}

// handleBatchPut handles PUT /api/v1/batch — a high-throughput bulk write endpoint.
// Accepts a JSON array of {key, value} pairs and applies them all in a single request.
// In cluster mode it uses quorum-based replication for durability.
func (s *Server) handleBatchPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req batchPutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	// Limit batch entries to prevent DoS
	if len(req.Entries) > 10000 {
		s.writeError(w, http.StatusBadRequest, "batch size exceeds limit of 10000")
		return
	}

	if len(req.Entries) == 0 {
		s.writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Data:    batchPutResponse{Applied: 0, Errors: 0},
		})
		return
	}

	applied, errs := 0, 0
	start := s.metrics.RecordPutStart()
	batchStart := time.Now()

	entries := make([]storage.Entry, len(req.Entries))
	for i, entry := range req.Entries {
		entries[i] = storage.Entry{Key: entry.Key, Value: []byte(entry.Value)}
	}

	if s.cluster != nil && s.cluster.IsDistributed() {
		if err := s.replicatedBatchPut(r.Context(), entries); err != nil {
			s.logger.Error("replicated batch put failed", "error", err)
			errs = len(req.Entries)
		} else {
			applied = len(req.Entries)
		}
	} else {
		if err := s.store.BatchPut(entries); err != nil {
			errs = len(req.Entries)
		} else {
			applied = len(req.Entries)
		}
	}

	batchEnd := time.Now()
	s.metrics.RecordPutEnd(start, errs == 0 || applied > 0)
	s.metrics.RecordBatchPut(applied)
	s.metrics.RecordHandlerStage("batch", "store", batchEnd.Sub(batchStart))

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    batchPutResponse{Applied: applied, Errors: errs},
	})
}

// replicatedBatchPut writes entries to the cluster with quorum-based replication
func (s *Server) replicatedBatchPut(ctx context.Context, entries []storage.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	type batchResult struct {
		success int
		err     error
	}
	resultCh := make(chan batchResult, 1)

	go func() {
		nodeBatches := make(map[string][]storage.Entry)
		keyToReplicas := make(map[string][]string)

		for _, entry := range entries {
			replicas := s.ring.GetNodes(entry.Key, ReplicationFactor)
			keyToReplicas[entry.Key] = replicas
			for _, replica := range replicas {
				nodeBatches[replica] = append(nodeBatches[replica], entry)
			}
		}

		successCount := 0
		var mu sync.Mutex
		var wg sync.WaitGroup

		quorumSize := s.cluster.GetQuorumSize()

		for addr, batch := range nodeBatches {
			isLocal := addr == s.nodeID || addr == s.addr
			if isLocal {
				if err := s.store.BatchPut(batch); err != nil {
					mu.Lock()
					successCount--
					mu.Unlock()
				}
				mu.Lock()
				successCount++
				mu.Unlock()
				continue
			}

			// W=1: Fire and forget - async replication, don't wait
			if quorumSize <= 1 {
				go func(nodeAddr string, batch []storage.Entry) {
					client, ok := s.cluster.GetClient(nodeAddr)
					if !ok {
						return
					}
					rpcEntries := make([]rpc.BatchWriteEntry, len(batch))
					for i, e := range batch {
						rpcEntries[i] = rpc.BatchWriteEntry{Key: e.Key, Value: e.Value}
					}
					bkgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					client.BatchReplicatedPut(bkgCtx, rpcEntries)
					// Fire and forget - don't wait for response
				}(addr, batch)
				mu.Lock()
				successCount++ // Count as success immediately
				mu.Unlock()
				continue
			}

			// W>1: Sync replication - wait for quorum
			wg.Add(1)
			go func(nodeAddr string, batch []storage.Entry) {
				defer wg.Done()
				client, ok := s.cluster.GetClient(nodeAddr)
				if !ok {
					return
				}
				rpcEntries := make([]rpc.BatchWriteEntry, len(batch))
				for i, e := range batch {
					rpcEntries[i] = rpc.BatchWriteEntry{Key: e.Key, Value: e.Value}
				}
				if _, err := client.BatchReplicatedPut(ctx, rpcEntries); err != nil {
					return
				}
				mu.Lock()
				successCount++
				mu.Unlock()
			}(addr, batch)
		}

		wg.Wait()

		if successCount < quorumSize {
			resultCh <- batchResult{err: fmt.Errorf("quorum not reached: got %d, need %d", successCount, quorumSize)}
			return
		}
		resultCh <- batchResult{success: successCount}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-resultCh:
		return result.err
	}
}

func (s *Server) handleBatchGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req batchGetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	// Limit batch get keys to prevent DoS
	if len(req.Keys) > 10000 {
		s.writeError(w, http.StatusBadRequest, "batch size exceeds limit of 10000")
		return
	}

	if len(req.Keys) == 0 {
		s.writeJSON(w, http.StatusOK, APIResponse{
			Success: true,
			Data:    batchGetResponse{Found: 0, Missing: 0},
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	var results map[string]storage.Entry
	var err error

	start := s.metrics.RecordGetStart()
	batchStart := time.Now()

	if s.cluster != nil && s.ring != nil && s.cluster.IsDistributed() {
		// Cluster mode: use distributed batch read coordination
		results, err = s.cluster.ReplicatedBatchGet(ctx, req.Keys)
	} else {
		// Single-node mode: local MultiGet
		results, err = s.store.MultiGet(req.Keys)
	}

	batchEnd := time.Now()
	s.metrics.RecordHandlerStage("batch_get", "store", batchEnd.Sub(batchStart))

	if err != nil {
		s.logger.Error("batch get error", "error", err)
		s.writeError(w, http.StatusInternalServerError, "batch read failed: "+err.Error())
		return
	}

	s.metrics.RecordGetEnd(start, err == nil)

	found, missing := 0, 0
	entries := make([]batchGetEntry, 0, len(req.Keys))

	for _, key := range req.Keys {
		if entry, ok := results[key]; ok {
			found++
			entries = append(entries, batchGetEntry{Key: key, Value: entry.Value, Found: true})
		} else {
			missing++
			entries = append(entries, batchGetEntry{Key: key, Found: false})
		}
	}

	s.metrics.RecordBatchGet(found)

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: batchGetResponse{
			Found:   found,
			Missing: missing,
			Entries: entries,
		},
	})
}
