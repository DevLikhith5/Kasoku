package handler

import (
	"net/http"

	"log/slog"

	"github.com/DevLikhith5/kasoku/cmd/server/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/DevLikhith5/kasoku/internal/cluster"
	"github.com/DevLikhith5/kasoku/internal/ring"
	storage "github.com/DevLikhith5/kasoku/internal/store"
)

const ReplicationFactor = 3

type Server struct {
	store   storage.StorageEngine
	nodeID  string
	addr    string
	logger  *slog.Logger
	metrics *metrics.Metrics
	cluster *cluster.Cluster
	ring    *ring.Ring
}

func New(store storage.StorageEngine, nodeID, addr string, logger *slog.Logger, metrics *metrics.Metrics) *Server {
	return &Server{
		store:   store,
		nodeID:  nodeID,
		addr:    addr,
		logger:  logger,
		metrics: metrics,
	}
}

func NewDistributed(store storage.StorageEngine, nodeID, addr string, logger *slog.Logger, metrics *metrics.Metrics, cfg *cluster.ClusterConfig) *Server {
	r := cfg.Ring
	if r == nil {
		r = ring.New(ring.DefaultVNodes)
	}

	// Add this node to the ring
	r.AddNode(nodeID)

	c := cluster.New(*cfg)

	s := &Server{
		store:   store,
		nodeID:  nodeID,
		addr:    addr,
		logger:  logger,
		metrics: metrics,
		cluster: c,
		ring:    r,
	}

	return s
}

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

	// Cluster endpoints
	if s.cluster != nil {
		mux.HandleFunc("/api/v1/cluster/status", s.handleClusterStatus)
		mux.HandleFunc("/api/v1/cluster/add-peer", s.handleAddPeer)
		mux.HandleFunc("/api/v1/cluster/remove-peer", s.handleRemovePeer)

		// Internal replication endpoints (used by rpc.Client)
		mux.HandleFunc("/internal/replicate", s.handleInternalReplicate)
	}

	// Hash ring visualization endpoint
	mux.HandleFunc("/ring", s.handleRing)

	// Metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())
}

func (s *Server) SetCluster(c *cluster.Cluster) {
	s.cluster = c
}

func (s *Server) SetRing(r *ring.Ring) {
	s.ring = r
}
