package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the Kasoku server
type Config struct {
	// Data directory for LSM storage
	DataDir string `yaml:"data_dir" env:"KASOKU_DATA_DIR" default:"./data"`

	// Server port (for HTTP/gRPC)
	Port int `yaml:"port" env:"KASOKU_PORT" default:"9000"`

	// HTTP port (if different from gRPC)
	HTTPPort int `yaml:"http_port" env:"KASOKU_HTTP_PORT" default:"9001"`

	// Log level (debug, info, warn, error)
	LogLevel string `yaml:"log_level" env:"KASOKU_LOG_LEVEL" default:"info"`

	// Log file path (empty = stdout)
	LogFile string `yaml:"log_file" env:"KASOKU_LOG_FILE" default:""`

	// LSM Engine settings
	LSM LSMConfig `yaml:"lsm"`

	// Compaction settings
	Compaction CompactionConfig `yaml:"compaction"`

	// Memory settings
	Memory MemoryConfig `yaml:"memory"`

	// WAL settings
	WAL WALConfig `yaml:"wal"`

	// Cluster settings (for distributed mode)
	Cluster ClusterConfig `yaml:"cluster"`
}

// LSMConfig holds LSM engine specific settings
type LSMConfig struct {
	// Number of levels in LSM tree
	Levels int `yaml:"levels" env:"KASOKU_LSM_LEVELS" default:"7"`

	// Size ratio between levels
	LevelRatio float64 `yaml:"level_ratio" env:"KASOKU_LSM_LEVEL_RATIO" default:"10.0"`

	// Base size for L0
	L0BaseSize int64 `yaml:"l0_base_size" env:"KASOKU_LSM_L0_BASE_SIZE" default:"67108864"` // 64MB
}

// CompactionConfig holds compaction settings
type CompactionConfig struct {
	// Number of SSTables to trigger compaction
	Threshold int `yaml:"threshold" env:"KASOKU_COMPACTION_THRESHOLD" default:"4"`

	// Maximum concurrent compactions
	MaxConcurrent int `yaml:"max_concurrent" env:"KASOKU_COMPACTION_MAX_CONCURRENT" default:"2"`

	// Size threshold for L0 compaction
	L0SizeThreshold int64 `yaml:"l0_size_threshold" env:"KASOKU_COMPACTION_L0_SIZE_THRESHOLD" default:"134217728"` // 128MB
}

// MemoryConfig holds memory-related settings
type MemoryConfig struct {
	// Memtable size in bytes
	MemTableSize int64 `yaml:"memtable_size" env:"KASOKU_MEMTABLE_SIZE" default:"67108864"` // 64MB

	// Max memory for memtables
	MaxMemtableBytes int64 `yaml:"max_memtable_bytes" env:"KASOKU_MAX_MEMTABLE_BYTES" default:"268435456"` // 256MB

	// Bloom filter false positive rate
	BloomFPRate float64 `yaml:"bloom_fp_rate" env:"KASOKU_BLOOM_FP_RATE" default:"0.01"`

	// Block cache size
	BlockCacheSize int64 `yaml:"block_cache_size" env:"KASOKU_BLOCK_CACHE_SIZE" default:"134217728"` // 128MB
}

// WALConfig holds Write-Ahead Log settings
type WALConfig struct {
	// Sync every write (safer but slower)
	Sync bool `yaml:"sync" env:"KASOKU_WAL_SYNC" default:"true"`

	// Sync interval in milliseconds (if sync=false)
	SyncInterval time.Duration `yaml:"sync_interval" env:"KASOKU_WAL_SYNC_INTERVAL" default:"100ms"`

	// WAL file size before rotation
	MaxFileSize int64 `yaml:"max_file_size" env:"KASOKU_WAL_MAX_FILE_SIZE" default:"67108864"` // 64MB
}

// ClusterConfig holds distributed cluster settings
type ClusterConfig struct {
	// Enable cluster mode
	Enabled bool `yaml:"enabled" env:"KASOKU_CLUSTER_ENABLED" default:"false"`

	// Node ID
	NodeID string `yaml:"node_id" env:"KASOKU_NODE_ID" default:"node-1"`

	// Node address (for inter-node communication)
	NodeAddr string `yaml:"node_addr" env:"KASOKU_NODE_ADDR" default:"http://localhost:9000"`

	// Peer nodes
	Peers []string `yaml:"peers" env:"KASOKU_PEERS" default:""`

	// Gossip port
	GossipPort int `yaml:"gossip_port" env:"KASOKU_GOSSIP_PORT" default:"9002"`

	// Raft port
	RaftPort int `yaml:"raft_port" env:"KASOKU_RAFT_PORT" default:"9003"`

	// Replication factor (number of replicas)
	ReplicationFactor int `yaml:"replication_factor" env:"KASOKU_REPLICATION_FACTOR" default:"3"`

	// Quorum size (minimum acks for write)
	QuorumSize int `yaml:"quorum_size" env:"KASOKU_QUORUM_SIZE" default:"2"`

	// Virtual nodes per physical node (for consistent hashing)
	VNodes int `yaml:"vnodes" env:"KASOKU_VNODES" default:"150"`

	// RPC timeout for inter-node communication
	RPCTimeoutMs int `yaml:"rpc_timeout_ms" env:"KASOKU_RPC_TIMEOUT_MS" default:"5000"`
}

// DefaultConfig returns a Config with default values
func DefaultConfig() *Config {
	return &Config{
		DataDir:  "./data",
		Port:     9000,
		HTTPPort: 9001,
		LogLevel: "info",
		LogFile:  "",
		LSM: LSMConfig{
			Levels:     7,
			LevelRatio: 10.0,
			L0BaseSize: 64 * 1024 * 1024,
		},
		Compaction: CompactionConfig{
			Threshold:       4,
			MaxConcurrent:   2,
			L0SizeThreshold: 128 * 1024 * 1024,
		},
		Memory: MemoryConfig{
			MemTableSize:     64 * 1024 * 1024,
			MaxMemtableBytes: 256 * 1024 * 1024,
			BloomFPRate:      0.01,
			BlockCacheSize:   128 * 1024 * 1024,
		},
		WAL: WALConfig{
			Sync:         true,
			SyncInterval: 100 * time.Millisecond,
			MaxFileSize:  64 * 1024 * 1024,
		},
		Cluster: ClusterConfig{
			Enabled:           false,
			NodeID:            "node-1",
			NodeAddr:          "http://localhost:9000",
			Peers:             []string{},
			GossipPort:        9002,
			RaftPort:          9003,
			ReplicationFactor: 3,
			QuorumSize:        2,
			VNodes:            150,
			RPCTimeoutMs:      5000,
		},
	}
}

// Load loads configuration from a YAML file
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	// Override with environment variables
	if err := applyEnvOverrides(cfg); err != nil {
		return nil, fmt.Errorf("failed to apply environment variables: %w", err)
	}

	// Validate and normalize
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// applyEnvOverrides applies environment variable overrides
func applyEnvOverrides(cfg *Config) error {
	// DataDir
	if v := os.Getenv("KASOKU_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	// Port
	if v := os.Getenv("KASOKU_PORT"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Port)
	}
	// HTTPPort
	if v := os.Getenv("KASOKU_HTTP_PORT"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Port)
	}
	// LogLevel
	if v := os.Getenv("KASOKU_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
	// LogFile
	if v := os.Getenv("KASOKU_LOG_FILE"); v != "" {
		cfg.LogFile = v
	}
	// MemTableSize
	if v := os.Getenv("KASOKU_MEMTABLE_SIZE"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Memory.MemTableSize)
	}

	return nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("data_dir cannot be empty")
	}

	// Resolve to absolute path
	absPath, err := filepath.Abs(c.DataDir)
	if err != nil {
		return fmt.Errorf("invalid data_dir: %w", err)
	}
	c.DataDir = absPath

	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}

	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("http_port must be between 1 and 65535")
	}

	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[c.LogLevel] {
		return fmt.Errorf("invalid log_level: %s (must be debug, info, warn, or error)", c.LogLevel)
	}

	if c.Memory.MemTableSize < 1024*1024 {
		return fmt.Errorf("memtable_size must be at least 1MB")
	}

	if c.Memory.BloomFPRate <= 0 || c.Memory.BloomFPRate >= 1 {
		return fmt.Errorf("bloom_fp_rate must be between 0 and 1")
	}

	return nil
}

// Save saves the configuration to a YAML file
func (c *Config) Save(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// String returns a string representation of the config (for logging)
func (c *Config) String() string {
	return fmt.Sprintf("Config{DataDir: %s, Port: %d, HTTPPort: %d, LogLevel: %s}",
		c.DataDir, c.Port, c.HTTPPort, c.LogLevel)
}
