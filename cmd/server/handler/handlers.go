package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// handleHealth returns basic health status
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"status":  "healthy",
			"node_id": s.nodeID,
		},
	})
}

// handleLive checks if server is running
func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"status": "alive"},
	})
}

// handleReady checks if server is ready to serve requests
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	stats := s.store.Stats()
	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"status": "ready",
			"stats":  stats,
		},
	})
}

// handleGet handles GET /api/v1/get/:key
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	key := extractKeyFromPath(r.URL.Path, "/api/v1/get/")
	if key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	start := s.metrics.RecordGetStart()
	entry, err := s.store.Get(key)
	s.metrics.RecordGetEnd(start, err == nil)

	if err != nil {
		if isKeyNotFound(err) {
			s.writeError(w, http.StatusNotFound, "key not found")
		} else {
			s.logger.Error("get error", "key", key, "error", err)
			s.writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: GetResponse{
			Key:       entry.Key,
			Value:     entry.Value,
			Version:   entry.Version,
			Timestamp: entry.TimeStamp.UnixNano(),
		},
	})
}

// handlePut handles PUT /api/v1/put/:key
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	key := extractKeyFromPath(r.URL.Path, "/api/v1/put/")
	if key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	value := []byte(req.Value)
	if len(value) == 0 {
		s.writeError(w, http.StatusBadRequest, "value is required")
		return
	}

	start := s.metrics.RecordPutStart()
	err := s.store.Put(key, value)
	s.metrics.RecordPutEnd(start, err == nil)

	if err != nil {
		s.logger.Error("put error", "key", key, "error", err)
		if isKeyTooLong(err) {
			s.writeError(w, http.StatusBadRequest, "key too long (max 1KB)")
		} else if isValueTooLarge(err) {
			s.writeError(w, http.StatusBadRequest, "value too large (max 1MB)")
		} else {
			s.writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"status": "ok"},
	})
}

// handleDelete handles DELETE /api/v1/delete/:key
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	key := extractKeyFromPath(r.URL.Path, "/api/v1/delete/")
	if key == "" {
		s.writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	start := s.metrics.RecordDeleteStart()
	err := s.store.Delete(key)
	s.metrics.RecordDeleteEnd(start, err == nil)

	if err != nil {
		if isKeyNotFound(err) {
			s.writeError(w, http.StatusNotFound, "key not found")
		} else {
			s.logger.Error("delete error", "key", key, "error", err)
			s.writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"status": "deleted"},
	})
}

// handleScan handles GET /api/v1/scan?prefix=xxx
func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	prefix := r.URL.Query().Get("prefix")
	entries, err := s.store.Scan(prefix)
	if err != nil {
		s.logger.Error("scan error", "prefix", prefix, "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = e.Key
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: ScanResponse{
			Keys: keys,
		},
	})
}

// handleKeys handles GET /api/v1/keys
func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	keys, err := s.store.Keys()
	if err != nil {
		s.logger.Error("keys error", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: ScanResponse{
			Keys: keys,
		},
	})
}

// handleNodeInfo returns information about this node
func (s *Server) handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	stats := s.store.Stats()
	s.writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"node_id": s.nodeID,
			"addr":    s.addr,
			"stats":   stats,
		},
	})
}

// handleMetrics returns prometheus-style metrics
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m := s.metrics.Get()
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "kasoku_get_total %d\n", m.GetTotal)
	fmt.Fprintf(w, "kasoku_put_total %d\n", m.PutTotal)
	fmt.Fprintf(w, "kasoku_delete_total %d\n", m.DeleteTotal)
	fmt.Fprintf(w, "kasoku_get_errors_total %d\n", m.GetErrors)
	fmt.Fprintf(w, "kasoku_put_errors_total %d\n", m.PutErrors)
	fmt.Fprintf(w, "kasoku_delete_errors_total %d\n", m.DeleteErrors)
}
