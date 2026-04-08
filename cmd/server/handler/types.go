package handler

import (
	"encoding/json"
	"net/http"
)

type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type GetResponse struct {
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	Version   uint64 `json:"version"`
	Timestamp int64  `json:"timestamp"`
}

type PutRequest struct {
	Value string `json:"value"`
}

type ScanResponse struct {
	Keys []string `json:"keys"`
}

// Helper functions

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("write response error", "error", err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, APIResponse{
		Success: false,
		Error:   message,
	})
}

func extractKeyFromPath(path, prefix string) string {
	key := ""
	if len(path) > len(prefix) {
		key = path[len(prefix):]
	}
	return key
}
