package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/DevLikhith5/kasoku/internal/rpc"
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

// writeInternal writes a response, choosing between gob and JSON based on the request's Content-Type.
func (s *Server) writeInternal(w http.ResponseWriter, r *http.Request, status int, data interface{}) {
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, rpc.GobContentType) {
		w.Header().Set("Content-Type", rpc.GobContentType)
		w.WriteHeader(status)
		payload, err := rpc.GobEncode(data)
		if err != nil {
			s.logger.Error("gob encode error", "error", err)
			return
		}
		w.Write(payload)
		return
	}

	// Fallback to JSON
	s.writeJSON(w, status, data)
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
