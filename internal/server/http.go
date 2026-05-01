package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/DevLikhith5/kasoku/internal/rpc"
	storage "github.com/DevLikhith5/kasoku/internal/store"
)

type NodeInterface interface {
	ReplicatedPut(ctx context.Context, key string, value []byte) error
	ReplicatedGet(ctx context.Context, key string) ([]byte, error)
	ReplicatedDelete(ctx context.Context, key string) error
	Scan(ctx context.Context, prefix string) ([]string, error)
	GetMembers() []string
	GetNodeID() string
	GetStatus() map[string]any
	GetRingNodes() []string
	HandleReplicate(ctx context.Context, key string, value []byte, targetNode string) error
	HandleReplicateGet(ctx context.Context, key string) ([]byte, bool, error)
	HandleReplicateGetEntry(ctx context.Context, key string) (storage.Entry, error)
	HandleReplicateDelete(ctx context.Context, key string) (bool, error)
	HandleGossip(remoteMembers []string) []string
	HandleHint(key string, value []byte, targetNode string) error
	HandleMerkle() ([]byte, error)
}

type Server struct {
	node   NodeInterface
	logger *slog.Logger
}

func New(node NodeInterface) *Server {
	return &Server{
		node:   node,
		logger: slog.Default(),
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// Client-facing endpoints
	mux.HandleFunc("GET /keys/{key}", s.handleGet)
	mux.HandleFunc("PUT /keys/{key}", s.handlePut)
	mux.HandleFunc("DELETE /keys/{key}", s.handleDelete)
	mux.HandleFunc("GET /keys", s.handleScan)

	// Internal node-to-node endpoints
	mux.HandleFunc("PUT /internal/replicate", s.handleReplicate)
	mux.HandleFunc("GET /internal/replicate", s.handleReplicate)
	mux.HandleFunc("DELETE /internal/replicate", s.handleReplicate)
	mux.HandleFunc("POST /internal/replicate", s.handleReplicate)
	mux.HandleFunc("POST /internal/replicate/get", s.handleReplicateGetRemote)
	mux.HandleFunc("POST /internal/gossip", s.handleGossip)
	mux.HandleFunc("POST /internal/hint", s.handleHint)
	mux.HandleFunc("GET /internal/merkle", s.handleMerkle)

	// Health check (Kubernetes compatible)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /ring", s.handleRing)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /internal/debug/key/{key}", s.handleDebugKey)

	return s.loggingMiddleware(mux)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}

	// Coordinator: run quorum write
	if err := s.node.ReplicatedPut(r.Context(), key, []byte(body.Value)); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value, err := s.node.ReplicatedGet(r.Context(), key)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"key":   key,
		"value": string(value),
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := s.node.ReplicatedDelete(r.Context(), key); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	keys, err := s.node.Scan(r.Context(), prefix)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"keys": keys,
	})
}

// handleReplicate processes an internal replication request from another node
// Dispatches on HTTP method: PUT=write, GET=read, DELETE=delete, POST=tombstone write
func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut, http.MethodPost:
		s.handleReplicatePut(w, r)
	case http.MethodGet:
		s.handleReplicateGet(w, r)
	case http.MethodDelete:
		s.handleReplicateDelete(w, r)
	default:
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func decodeInternal(r *http.Request, v interface{}) error {
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, rpc.GobContentType) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		if len(body) == 0 {
			return nil
		}
		return rpc.GobDecode(body, v)
	}
	// Default: JSON
	return json.NewDecoder(r.Body).Decode(v)
}

func (s *Server) handleReplicatePut(w http.ResponseWriter, r *http.Request) {
	var body rpc.ReplicatedWriteRequest
	if err := decodeInternal(r, &body); err != nil {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}

	err := s.node.HandleReplicate(r.Context(), body.Key, body.Value, body.TargetNode)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleReplicateGet(w http.ResponseWriter, r *http.Request) {
	var body rpc.ReplicatedReadRequest
	if err := decodeInternal(r, &body); err != nil {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}

	entry, err := s.node.HandleReplicateGetEntry(r.Context(), body.Key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"found": false,
		})
		return
	}
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"found":     false,
			"error":     err.Error(),
			"tombstone": false,
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"found":     true,
		"value":     entry.Value,
		"tombstone": entry.Tombstone,
	})
}

func (s *Server) handleReplicateGetRemote(w http.ResponseWriter, r *http.Request) {
	var body rpc.ReplicatedReadRequest
	if err := decodeInternal(r, &body); err != nil {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}

	entry, err := s.node.HandleReplicateGetEntry(r.Context(), body.Key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"key":       entry.Key,
		"value":     entry.Value,
		"version":   entry.Version,
		"tombstone": entry.Tombstone,
	})
}

func (s *Server) handleReplicateDelete(w http.ResponseWriter, r *http.Request) {
	var body rpc.ReplicatedDeleteRequest
	if err := decodeInternal(r, &body); err != nil {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}

	deleted, err := s.node.HandleReplicateDelete(r.Context(), body.Key)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"deleted": false,
			"error":   err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"deleted": deleted,
	})
}

func (s *Server) handleGossip(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Members []string `json:"members"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}

	response := s.node.HandleGossip(body.Members)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"members": response,
	})
}

func (s *Server) handleHint(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key        string `json:"key"`
		Value      []byte `json:"value"`
		TargetNode string `json:"target_node"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}

	if err := s.node.HandleHint(body.Key, body.Value, body.TargetNode); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMerkle(w http.ResponseWriter, r *http.Request) {
	summary, err := s.node.HandleMerkle()
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(summary)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"node_id": s.node.GetNodeID(),
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	status := s.node.GetStatus()
	if status["engine_status"] == "ok" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"ready": true,
		})
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{
			"ready": false,
		})
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.node.GetStatus()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *Server) handleRing(w http.ResponseWriter, r *http.Request) {
	nodes := s.node.GetRingNodes()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"nodes": nodes,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	status := s.node.GetStatus()

	fmt.Fprintf(w, "# HELP kasoku_up Node is up and ready\n")
	fmt.Fprintf(w, "# TYPE kasoku_up gauge\n")
	fmt.Fprintf(w, "kasoku_up{node_id=\"%s\"} 1\n\n", status["node_id"])

	fmt.Fprintf(w, "# HELP kasoku_peers_healthy Number of healthy peers\n")
	fmt.Fprintf(w, "# TYPE kasoku_peers_healthy gauge\n")
	fmt.Fprintf(w, "kasoku_peers_healthy{node_id=\"%s\"} %d\n\n", status["node_id"], status["peers_healthy"])

	fmt.Fprintf(w, "# HELP kasoku_hints_pending Pending hints in store\n")
	fmt.Fprintf(w, "# TYPE kasoku_hints_pending gauge\n")
	fmt.Fprintf(w, "kasoku_hints_pending{node_id=\"%s\"} %d\n\n", status["node_id"], status["hints_pending"])

	fmt.Fprintf(w, "# HELP kasoku_ring_nodes Number of nodes in consistent hash ring\n")
	fmt.Fprintf(w, "# TYPE kasoku_ring_nodes gauge\n")
	fmt.Fprintf(w, "kasoku_ring_nodes{node_id=\"%s\"} %d\n\n", status["node_id"], status["ring_nodes"])

	fmt.Fprintf(w, "# HELP kasoku_write_quorum Write quorum size\n")
	fmt.Fprintf(w, "# TYPE kasoku_write_quorum gauge\n")
	fmt.Fprintf(w, "kasoku_write_quorum{node_id=\"%s\"} %d\n", status["node_id"], status["write_quorum"])
}

func (s *Server) handleDebugKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	entry, err := s.node.HandleReplicateGetEntry(r.Context(), key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"key":       key,
			"found":     false,
			"tombstone": false,
		})
		return
	}
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"key":       entry.Key,
		"value":     string(entry.Value),
		"version":   entry.Version,
		"tombstone": entry.Tombstone,
		"found":     true,
	})
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		s.logger.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}
