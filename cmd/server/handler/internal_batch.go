package handler

import (
	"net/http"

	"github.com/DevLikhith5/kasoku/internal/rpc"
	storage "github.com/DevLikhith5/kasoku/internal/store"
)

// handleInternalBatchReplicate handles POST /internal/replicate/batch.
// It receives a batch of key-value entries and writes them all to local storage atomically.
// This endpoint exists solely for inter-node replication — it bypasses the
// distributed coordinator logic to prevent infinite proxy loops.
func (s *Server) handleInternalBatchReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req rpc.BatchWriteRequest
	if err := decodeInternal(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "decode error: "+err.Error())
		return
	}

	if len(req.Entries) == 0 {
		s.writeInternal(w, r, http.StatusOK, rpc.BatchWriteResponse{Success: true, Applied: 0})
		return
	}

	// Convert to storage.Entry slice for BatchPut (single WAL + single memtable lock)
	entries := make([]storage.Entry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = storage.Entry{Key: e.Key, Value: e.Value}
	}

	if err := s.store.BatchPut(entries); err != nil {
		s.logger.Error("batch replicate: BatchPut failed", "count", len(entries), "error", err)
		s.writeInternal(w, r, http.StatusInternalServerError, rpc.BatchWriteResponse{
			Success: false,
			Applied: 0,
			Error:   "BatchPut failed: " + err.Error(),
		})
		return
	}

	s.metrics.RecordBatchPut(len(entries))

	s.writeInternal(w, r, http.StatusOK, rpc.BatchWriteResponse{
		Success: true,
		Applied: len(entries),
	})
}

// handleInternalBatchGet handles POST /internal/replicate/batch/get.
// It receives a list of keys and reads them from local storage in a single bulk operation.
func (s *Server) handleInternalBatchGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req rpc.BatchReadRequest
	if err := decodeInternal(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "decode error: "+err.Error())
		return
	}

	if len(req.Keys) == 0 {
		s.writeInternal(w, r, http.StatusOK, rpc.BatchReadResponse{Success: true})
		return
	}

	results, err := s.store.MultiGet(req.Keys)
	if err != nil {
		s.logger.Error("batch read: MultiGet failed", "error", err)
		s.writeInternal(w, r, http.StatusInternalServerError, rpc.BatchReadResponse{
			Success: false,
			Error:   "MultiGet failed: " + err.Error(),
		})
		return
	}

	entries := make([]rpc.BatchReadEntry, 0, len(req.Keys))
	for _, key := range req.Keys {
		if entry, ok := results[key]; ok {
			entries = append(entries, rpc.BatchReadEntry{
				Key:       key,
				Value:     entry.Value,
				Found:     true,
				Tombstone: entry.Tombstone,
			})
		} else {
			entries = append(entries, rpc.BatchReadEntry{
				Key:   key,
				Found: false,
			})
		}
	}

	s.writeInternal(w, r, http.StatusOK, rpc.BatchReadResponse{
		Success: true,
		Entries: entries,
	})
}
