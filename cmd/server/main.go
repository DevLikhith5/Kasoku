package main

// import (
// 	"flag"
// 	"fmt"
// 	"log"
// 	"os"
// 	"os/signal"
// 	"path/filepath"
// 	"syscall"

// 	"github.com/DevLikhith5/kasoku/internal/config"
// 	lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
// )

// var (
// 	version   = "dev"
// 	buildTime = "unknown"
// 	gitCommit = "unknown"
// )

// func main() {
// 	// Command-line flags
// 	configFile := flag.String("config", "", "Path to configuration file (YAML)")
// 	dataDir := flag.String("dir", "", "Data directory (overrides config file)")
// 	port := flag.Int("port", 0, "Server port (overrides config file)")
// 	showVersion := flag.Bool("version", false, "Show version information")
// 	initConfig := flag.Bool("init-config", false, "Generate default configuration file")
// 	flag.Parse()

// 	// Show version
// 	if *showVersion {
// 		fmt.Printf("Kasoku Server %s\n", version)
// 		fmt.Printf("Build time: %s\n", buildTime)
// 		fmt.Printf("Git commit: %s\n", gitCommit)
// 		os.Exit(0)
// 	}

// 	// Generate default config
// 	if *initConfig {
// 		cfg := config.DefaultConfig()
// 		cfgPath := "kasoku.yaml"
// 		if *configFile != "" {
// 			cfgPath = *configFile
// 		}
// 		if err := cfg.Save(cfgPath); err != nil {
// 			log.Fatalf("Failed to save config: %v", err)
// 		}
// 		fmt.Printf("Generated default configuration: %s\n", cfgPath)
// 		os.Exit(0)
// 	}

// 	// Load configuration
// 	cfg, err := config.Load(*configFile)
// 	if err != nil {
// 		log.Fatalf("Failed to load configuration: %v", err)
// 	}

// 	// Override with CLI flags
// 	if *dataDir != "" {
// 		cfg.DataDir = *dataDir
// 	}
// 	if *port != 0 {
// 		cfg.Port = *port
// 	}

// 	// Ensure data directory exists
// 	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
// 		log.Fatalf("Failed to create data directory: %v", err)
// 	}

// 	// Setup logging
// 	if err := setupLogging(cfg); err != nil {
// 		log.Fatalf("Failed to setup logging: %v", err)
// 	}

// 	// Print startup banner
// 	printBanner(cfg)

// 	// Initialize LSM engine with WAL config
// 	log.Printf("Initializing LSM engine with data directory: %s", cfg.DataDir)
// 	log.Printf("  - WAL sync: %v", cfg.WAL.Sync)
// 	log.Printf("  - WAL sync interval: %v", cfg.WAL.SyncInterval)

// 	lsmCfg := lsmengine.LSMConfig{
// 		WALSyncInterval: cfg.WAL.SyncInterval,
// 	}

// 	engine, err := lsmengine.NewLSMEngineWithConfig(cfg.DataDir, lsmCfg)
// 	if err != nil {
// 		log.Fatalf("Failed to create LSM engine: %v", err)
// 	}
// 	defer engine.Close()

// 	log.Printf("LSM engine initialized successfully")
// 	log.Printf("  - MemTable size: %d MB", cfg.Memory.MemTableSize/(1024*1024))
// 	log.Printf("  - Bloom filter FP rate: %.2f%%", cfg.Memory.BloomFPRate*100)
// 	log.Printf("  - Compaction threshold: %d SSTables", cfg.Compaction.Threshold)

// 	// TODO: Start HTTP/gRPC server here
// 	// For now, just wait for shutdown signal
// 	log.Printf("Server listening on port %d (HTTP: %d)", cfg.Port, cfg.HTTPPort)
// 	log.Printf("Press Ctrl+C to shutdown")

// 	// Wait for shutdown signal
// 	sigCh := make(chan os.Signal, 1)
// 	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

// 	sig := <-sigCh
// 	log.Printf("Received signal %v, shutting down...", sig)

// 	// Graceful shutdown
// 	log.Printf("Closing LSM engine...")
// 	if err := engine.Close(); err != nil {
// 		log.Printf("Error closing engine: %v", err)
// 	}

// 	// Cleanup WAL files
// 	cleanupWAL(cfg.DataDir)

// 	log.Printf("Server stopped gracefully")
// }

// // setupLogging configures logging based on config
// func setupLogging(cfg *config.Config) error {
// 	if cfg.LogFile != "" {
// 		// Ensure log directory exists
// 		logDir := filepath.Dir(cfg.LogFile)
// 		if err := os.MkdirAll(logDir, 0755); err != nil {
// 			return fmt.Errorf("failed to create log directory: %w", err)
// 		}

// 		f, err := os.OpenFile(cfg.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 		if err != nil {
// 			return fmt.Errorf("failed to open log file: %w", err)
// 		}
// 		log.SetOutput(f)
// 		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
// 	}

// 	// Set log level (implementation would filter log output)
// 	log.Printf("Log level: %s", cfg.LogLevel)
// 	return nil
// }

// // printBanner prints the startup banner
// func printBanner(cfg *config.Config) {
// 	fmt.Println()
// 	fmt.Println("╔══════════════════════════════════════════════════════════╗")
// 	fmt.Println("║                    Kasoku Server                         ║")
// 	fmt.Println("║          High-Performance LSM Key-Value Store            ║")
// 	fmt.Println("╚══════════════════════════════════════════════════════════╝")
// 	fmt.Println()
// 	fmt.Printf("  Version:     %s (%s)\n", version, gitCommit)
// 	fmt.Printf("  Data Dir:    %s\n", cfg.DataDir)
// 	fmt.Printf("  Port:        %d\n", cfg.Port)
// 	fmt.Printf("  HTTP Port:   %d\n", cfg.HTTPPort)
// 	fmt.Printf("  Log Level:   %s\n", cfg.LogLevel)
// 	fmt.Println()
// }

// // cleanupWAL removes old WAL files after clean shutdown
// func cleanupWAL(dataDir string) {
// 	// Archive or remove old WAL files
// 	// This is a placeholder - actual implementation would depend on WAL package
// 	walPath := filepath.Join(dataDir, "wal.log")
// 	if _, err := os.Stat(walPath); err == nil {
// 		// WAL exists - could archive it instead of deleting
// 		log.Printf("WAL file preserved: %s", walPath)
// 	}
// }
