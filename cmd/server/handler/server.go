package handler

import (
	"net/http"

	"github.com/DevLikhith5/kasoku/cmd/server/metrics"
	"log/slog"

	storage "github.com/DevLikhith5/kasoku/internal/store"
)

// Server is the HTTP server that exposes the KV store
type Server struct {
	store   storage.StorageEngine
	nodeID  string
	addr    string
	logger  *slog.Logger
	metrics *metrics.Metrics
}

// New creates a new HTTP server
func New(store storage.StorageEngine, nodeID, addr string, logger *slog.Logger, metrics *metrics.Metrics) *Server {
	return &Server{
		store:   store,
		nodeID:  nodeID,
		addr:    addr,
		logger:  logger,
		metrics: metrics,
	}
}

// RegisterRoutes registers all HTTP routes on the given mux
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	// Health endpoints
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/health/live", s.handleLive)
	mux.HandleFunc("/health/ready", s.handleReady)

	// KV endpoints
	mux.HandleFunc("/api/v1/get/", s.handleGet)
	mux.HandleFunc("/api/v1/put/", s.handlePut)
	mux.HandleFunc("/api/v1/delete/", s.handleDelete)
	mux.HandleFunc("/api/v1/scan", s.handleScan)
	mux.HandleFunc("/api/v1/keys", s.handleKeys)

	// Node info endpoint
	mux.HandleFunc("/api/v1/node", s.handleNodeInfo)

	// Metrics endpoint
	mux.HandleFunc("/metrics", s.handleMetrics)
}
