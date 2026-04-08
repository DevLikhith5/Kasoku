package handler

import (
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
