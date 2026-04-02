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
	lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
)

// Config holds server configuration
type Config struct {
	Addr   string `yaml:"addr"`
	NodeID string `yaml:"node_id"`
	WALDir string `yaml:"wal_dir"`
}

func main() {
	// Default config
	cfg := Config{
		Addr:   ":8080",
		NodeID: "node-1",
		WALDir: "./data", // Same as CLI
	}

	// Override with environment variables if set
	if addr := os.Getenv("KASOKU_ADDR"); addr != "" {
		cfg.Addr = addr
	}
	if nodeID := os.Getenv("KASOKU_NODE_ID"); nodeID != "" {
		cfg.NodeID = nodeID
	}
	if walDir := os.Getenv("KASOKU_WAL_DIR"); walDir != "" {
		cfg.WALDir = walDir
	}

	// Initialize logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	}))

	// Initialize storage engine (LSM Engine - same as CLI)
	store, err := lsmengine.NewLSMEngine(cfg.WALDir)
	if err != nil {
		logger.Error("failed to create storage engine", "error", err)
		fmt.Fprintf(os.Stderr, "failed to create storage engine: %v\n", err)
		os.Exit(1)
	}

	// Initialize metrics
	m := metrics.New()

	// Create HTTP server
	server := handler.New(store, cfg.NodeID, cfg.Addr, logger, m)

	// Setup routes
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	// Apply middleware
	httpHandler := handler.WithLogging(logger)(handler.WithRecovery(logger)(mux))

	logger.Info("starting HTTP server", "addr", cfg.Addr, "node_id", cfg.NodeID)

	httpServer := &http.Server{
		Addr:         cfg.Addr,
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
