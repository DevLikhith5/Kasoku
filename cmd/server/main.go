package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DevLikhith5/kasoku/cmd/server/handler"
	"github.com/DevLikhith5/kasoku/cmd/server/metrics"
	"github.com/DevLikhith5/kasoku/internal/cluster"
	"github.com/DevLikhith5/kasoku/internal/config"
	"github.com/DevLikhith5/kasoku/internal/ring"
	lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
	"flag"
)

func main() {
	// Auto-tune memory limits
	applyMemoryLimit()

	// Reduce GC frequency for high-throughput workloads
	if os.Getenv("GOGC") == "" {
		os.Setenv("GOGC", "200")
	}

	// Parse command line flags
	cfgPathFlag := flag.String("config", "", "Path to config file")
	nodeIDFlag := flag.String("node-id", "", "Node ID (overrides config)")
	portFlag := flag.Int("port", 0, "HTTP port (overrides config)")
	flag.Parse()

	// Load configuration
	cfgPath := *cfgPathFlag
	if cfgPath == "" {
		cfgPath = os.Getenv("KASOKU_CONFIG")
		if cfgPath == "" {
			cfgPath = "kasoku.yaml"
		}
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Apply flag overrides
	if *nodeIDFlag != "" {
		cfg.Cluster.NodeID = *nodeIDFlag
	}
	if *portFlag != 0 {
		cfg.HTTPPort = *portFlag
		cfg.Cluster.NodeAddr = fmt.Sprintf("http://localhost:%d", *portFlag)
	}

	// Default config
	nodeAddr := cfg.Cluster.NodeAddr
	if nodeAddr == "" {
		nodeAddr = fmt.Sprintf("http://localhost:%d", cfg.HTTPPort)
	}

	// Initialize logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	}))

	// Initialize storage engine (LSM Engine)
	store, err := lsmengine.NewLSMEngineWithConfig(cfg.DataDir, lsmengine.LSMConfig{
		MemTableSize:        cfg.Memory.MemTableSize,
		MaxMemtableBytes:    cfg.Memory.MaxMemtableBytes,
		WALSyncInterval:     cfg.WAL.SyncInterval,
		WALCheckpointBytes:  cfg.WAL.CheckpointBytes,
		WALMaxBufferedBytes: cfg.WAL.MaxBufferedBytes,
		CompactionThreshold: cfg.Compaction.Threshold,
		L0SizeThreshold:     cfg.Compaction.L0SizeThreshold,
		BloomFPRate:         cfg.Memory.BloomFPRate,
		LevelRatio:          cfg.LSM.LevelRatio,
	})
	if err != nil {
		logger.Error("failed to create storage engine", "error", err)
		fmt.Fprintf(os.Stderr, "failed to create storage engine: %v\n", err)
		os.Exit(1)
	}

	// Initialize block cache and compression from config
	lsmengine.InitBlockCache(cfg.Memory.BlockCacheSize)
	logger.Info("block cache initialized", "size_bytes", cfg.Memory.BlockCacheSize)

	// Apply compression settings
	lsmengine.SetCompression(cfg.Memory.CompressionEnabled, cfg.Memory.UseZstd)
	logger.Info("compression configured", "enabled", cfg.Memory.CompressionEnabled, "use_zstd", cfg.Memory.UseZstd)

	// Initialize metrics
	m := metrics.New()

	// Create HTTP server
	var server *handler.Server

	if cfg.Cluster.Enabled {
		// Distributed mode with consistent hashing
		logger.Info("starting in distributed mode",
			"node_id", cfg.Cluster.NodeID,
			"node_addr", nodeAddr,
			"replication_factor", cfg.Cluster.ReplicationFactor,
			"vnodes", cfg.Cluster.VNodes,
		)

		// Create consistent hashing ring
		r := ring.New(cfg.Cluster.VNodes)

		// IMPORTANT: Use nodeAddr as the consistent identity in the ring so that
		// ring lookup results (peer URLs) can be compared directly to s.nodeID
		// in the handler. Previously, nodeID was "node-1" but peers were added
		// as "http://localhost:9001" — causing replicas[0] == s.nodeID to never
		// match, leading to infinite proxy loops and crashes.
		r.AddNode(nodeAddr) // Add self using nodeAddr as identity

		// Create cluster config — use nodeAddr as NodeID for ring consistency
		clusterCfg := cluster.ClusterConfig{
			NodeID:            nodeAddr, // Use URL as ID for ring consistency
			NodeAddr:          nodeAddr,
			Ring:              r,
			Store:             store,
			ReplicationFactor: cfg.Cluster.ReplicationFactor,
			QuorumSize:        cfg.Cluster.QuorumSize,
			ReadQuorum:        cfg.Cluster.ReadQuorum,
			RPCTimeout:        time.Duration(cfg.Cluster.RPCTimeoutMs) * time.Millisecond,
			Logger:            logger,
			Peers:             cfg.Cluster.Peers,
		}

		server = handler.NewDistributed(store, nodeAddr, nodeAddr, logger, m, &clusterCfg)

		// Add peer nodes to the ring using their node_addr URLs as identity
		for _, peer := range cfg.Cluster.Peers {
			r.AddNode(peer)
		}
	} else {
		// Single-node mode
		logger.Info("starting in single-node mode",
			"node_id", cfg.Cluster.NodeID,
			"addr", nodeAddr,
		)

		server = handler.New(store, cfg.Cluster.NodeID, nodeAddr, logger, m)
	}

	// Setup routes
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	// Apply middleware
	httpHandler := handler.WithLogging(logger)(handler.WithRecovery(logger)(mux))

	// Background metrics scraping
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			stats := store.Stats()
			m.SetStorageKeys(stats.KeyCount)
			m.SetStorageBytes(stats.MemBytes, stats.DiskBytes)

			if cfg.Cluster.Enabled {
				// We only have ring nodes exposed in main via cfg.Cluster
				// In a full integration, you'd pull from HintStore/PhiMap here.
				m.SetClusterNodes(len(cfg.Cluster.Peers) + 1)
			}
		}
	}()

	listenPort := cfg.HTTPPort
	addr := fmt.Sprintf(":%d", listenPort)
	logger.Info("starting HTTP server", "addr", addr, "node_id", cfg.Cluster.NodeID)

	httpServer := &http.Server{
		Addr:         addr,
		Handler:      httpHandler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig

		logger.Info("shutting down server")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(ctx); err != nil {
			logger.Error("server shutdown error", "error", err)
		}

		if err := store.Close(); err != nil {
			logger.Error("store close error", "error", err)
		}
	}()

	if err := httpServer.ListenAndServe(); err != nil {
		logger.Error("server error", "error", err)
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
