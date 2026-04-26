package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	kasokuapi "github.com/DevLikhith5/kasoku/api"
	"github.com/DevLikhith5/kasoku/cmd/server/handler"
	"github.com/DevLikhith5/kasoku/cmd/server/metrics"
	"github.com/DevLikhith5/kasoku/internal/cluster"
	"github.com/DevLikhith5/kasoku/internal/config"
	"github.com/DevLikhith5/kasoku/internal/ring"
	lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
	rpcgrpc "github.com/DevLikhith5/kasoku/internal/rpc/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"flag"
)

func main() {
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

	// Initialize block cache from config
	lsmengine.InitBlockCache(cfg.Memory.BlockCacheSize)
	logger.Info("block cache initialized", "size_bytes", cfg.Memory.BlockCacheSize)

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

		// get gRPC port for cluster config
		clusterGRPCPort := cfg.GRPCPort
		if clusterGRPCPort == 0 {
			clusterGRPCPort = cfg.Port + 2
		}

		// Create cluster config — use nodeAddr as NodeID for ring consistency
		clusterCfg := cluster.ClusterConfig{
			NodeID:            nodeAddr, // Use URL as ID for ring consistency
			NodeAddr:          nodeAddr,
			GRPCPort:         clusterGRPCPort,
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

	// Start HTTP/HTTPS server in background (goroutine)
	var httpErr error
	if cfg.TLS.Enabled {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			logger.Error("failed to load TLS cert", "error", err)
			fmt.Fprintf(os.Stderr, "failed to load TLS cert: %v\n", err)
			os.Exit(1)
		}
		httpServer.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		logger.Info("TLS enabled", "cert", cfg.TLS.CertFile)
		go func() { httpErr = httpServer.ListenAndServeTLS(cfg.TLS.CertFile, cfg.TLS.KeyFile) }()
	} else {
		go func() { httpErr = httpServer.ListenAndServe() }()
	}

	grpcPort := cfg.GRPCPort
	if grpcPort == 0 {
		grpcPort = cfg.Port + 2
	}
	grpcAddr := fmt.Sprintf(":%d", grpcPort)
	logger.Info("starting gRPC server", "addr", grpcAddr, "node_id", cfg.Cluster.NodeID)

	grpcOpts := []grpc.ServerOption{}
	if cfg.TLS.Enabled {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			logger.Error("failed to load TLS cert", "error", err)
			os.Exit(1)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewServerTLSFromCert(&cert)))
		logger.Info("gRPC TLS enabled")
	}

	grpcServer := grpc.NewServer(grpcOpts...)
	grpcSrv := rpcgrpc.NewServer(store, nodeAddr, nodeAddr, logger)
	if cfg.Cluster.Enabled && server != nil && server.Cluster() != nil {
		grpcSrv.SetCluster(server.Cluster())
	}
	kasokuapi.RegisterKasokuServiceServer(grpcServer, grpcSrv)

	go func() {
		listener, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			logger.Error("gRPC listener error", "error", err)
			return
		}
		if err := grpcServer.Serve(listener); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

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

		grpcServer.GracefulStop()

		if err := store.Close(); err != nil {
			logger.Error("store close error", "error", err)
		}
	}()

		if err := httpErr; err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		}
		
	// Wait forever (servers run in goroutines)
	select {}
}
