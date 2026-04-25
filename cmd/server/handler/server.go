package handler

import (
	"context"
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

	// Start background workers for eventual consistency (gossip, background replication, anti-entropy)
	c.StartBackgroundWorkers(context.Background())

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

func (s *Server) Cluster() *cluster.Cluster {
	return s.cluster
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

	// Batch endpoints
	mux.HandleFunc("/api/v1/batch", s.handleBatchPut)
	mux.HandleFunc("/api/v1/batch/get", s.handleBatchGet)

	// Node info endpoint
	mux.HandleFunc("/api/v1/node", s.handleNodeInfo)

	// Cluster endpoints
	if s.cluster != nil {
		mux.HandleFunc("/api/v1/cluster/status", s.handleClusterStatus)
		mux.HandleFunc("/api/v1/cluster/add-peer", s.handleAddPeer)
		mux.HandleFunc("/api/v1/cluster/remove-peer", s.handleRemovePeer)

		// Internal replication endpoints (used by rpc.Client)
		mux.HandleFunc("/internal/replicate", s.handleInternalReplicate)
		mux.HandleFunc("/internal/replicate/batch", s.handleInternalBatchReplicate)
		mux.HandleFunc("/internal/replicate/batch/get", s.handleInternalBatchGet)

		// Gossip protocol endpoints
		mux.HandleFunc("/internal/gossip/state", s.handleGossipState)

		// Merkle tree endpoints for anti-entropy
		mux.HandleFunc("/internal/merkle/root", s.handleMerkleRoot)
		mux.HandleFunc("/internal/merkle/diff", s.handleMerkleDiff)
	}

	// Hash ring visualization endpoint
	mux.HandleFunc("/ring", s.handleRing)

	// Metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	// Flush endpoint
	mux.HandleFunc("/api/v1/flush", s.handleFlush)

	// Simple benchmark endpoint
	mux.HandleFunc("/api/v1/benchmark", s.handleBenchmark)

	// Network benchmark endpoint
	mux.HandleFunc("/api/v1/benchmark/network", s.handleNetworkBenchmark)
}

func (s *Server) SetCluster(c *cluster.Cluster) {
	s.cluster = c
}

func (s *Server) SetRing(r *ring.Ring) {
	s.ring = r
}
