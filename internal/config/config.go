package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	// Data directory for LSM storage
	DataDir string `yaml:"data_dir" env:"KASOKU_DATA_DIR" default:"./data"`

	// Server port (for HTTP/gRPC)
	Port int `yaml:"port" env:"KASOKU_PORT" default:"9000"`

	// HTTP port (if different from gRPC)
	HTTPPort int `yaml:"http_port" env:"KASOKU_HTTP_PORT" default:"9001"`

	// gRPC port (if different from port, defaults to Port+2)
	GRPCPort int `yaml:"grpc_port" env:"KASOKU_GRPC_PORT" default:"0"`

	// TLS settings
	TLS TLSConfig `yaml:"tls"`

	// Auth settings
	Auth AuthConfig `yaml:"auth"`

	// Log level (debug, info, warn, error)
	LogLevel string `yaml:"log_level" env:"KASOKU_LOG_LEVEL" default:"info"`

	// Log file path (empty = stdout)
	LogFile string `yaml:"log_file" env:"KASOKU_LOG_FILE" default:""`

	// Rate limiting
	RateLimit RateLimitConfig `yaml:"rate_limit"`

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

type TLSConfig struct {
	// Enable TLS
	Enabled bool `yaml:"enabled" env:"KASOKU_TLS_ENABLED" default:"false"`

	// TLS certificate file
	CertFile string `yaml:"cert_file" env:"KASOKU_TLS_CERT_FILE" default:"./certs/server-cert.pem"`

	// TLS key file
	KeyFile string `yaml:"key_file" env:"KASOKU_TLS_KEY_FILE" default:"./certs/server-key.pem"`

	// Client certificate verification (none, request, require)
	ClientAuth string `yaml:"client_auth" env:"KASOKU_TLS_CLIENT_AUTH" default:"none"`
}

type AuthConfig struct {
	// Enable authentication
	Enabled bool `yaml:"enabled" env:"KASOKU_AUTH_ENABLED" default:"false"`

	// API key for authentication
	APIKey string `yaml:"api_key" env:"KASOKU_AUTH_API_KEY"`

	// JWT secret for JWT authentication
	JWTSecret string `yaml:"jwt_secret" env:"KASOKU_AUTH_JWT_SECRET"`
}

type RateLimitConfig struct {
	// Enable rate limiting
	Enabled bool `yaml:"enabled" env:"KASOKU_RATE_LIMIT_ENABLED" default:"false"`

	// Requests per second per client
	RequestsPerSecond int `yaml:"requests_per_second" env:"KASOKU_RATE_LIMIT_RPS" default:"1000"`

	// Burst size
	Burst int `yaml:"burst" env:"KASOKU_RATE_LIMIT_BURST" default:"100"`
}

type LSMConfig struct {
	// Number of levels in LSM tree
	Levels int `yaml:"levels" env:"KASOKU_LSM_LEVELS" default:"7"`

	// Size ratio between levels
	LevelRatio float64 `yaml:"level_ratio" env:"KASOKU_LSM_LEVEL_RATIO" default:"10.0"`

	// Base size for L0
	L0BaseSize int64 `yaml:"l0_base_size" env:"KASOKU_LSM_L0_BASE_SIZE" default:"67108864"` // 64MB
}

type CompactionConfig struct {
	// Number of SSTables to trigger compaction
	Threshold int `yaml:"threshold" env:"KASOKU_COMPACTION_THRESHOLD" default:"4"`

	// Maximum concurrent compactions
	MaxConcurrent int `yaml:"max_concurrent" env:"KASOKU_COMPACTION_MAX_CONCURRENT" default:"2"`

	// Size threshold for L0 compaction
	L0SizeThreshold int64 `yaml:"l0_size_threshold" env:"KASOKU_COMPACTION_L0_SIZE_THRESHOLD" default:"134217728"` // 128MB
}

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

type WALConfig struct {
	// Sync every write (safer but slower)
	Sync bool `yaml:"sync" env:"KASOKU_WAL_SYNC" default:"true"`

	// Sync interval in milliseconds (if sync=false)
	SyncInterval time.Duration `yaml:"sync_interval" env:"KASOKU_WAL_SYNC_INTERVAL" default:"100ms"`

	// WAL file size before rotation
	MaxFileSize int64 `yaml:"max_file_size" env:"KASOKU_WAL_MAX_FILE_SIZE" default:"67108864"` // 64MB

	// Checkpoint bytes - checkpoint sync after this many bytes
	CheckpointBytes int64 `yaml:"checkpoint_bytes" env:"KASOKU_WAL_CHECKPOINT_BYTES" default:"67108864"` // 64MB

	// Max buffered bytes - max buffered before forced flush
	MaxBufferedBytes int64 `yaml:"max_buffered_bytes" env:"KASOKU_WAL_MAX_BUFFERED_BYTES" default:"16777216"` // 16MB
}

type ClusterConfig struct {
	// Enable cluster mode
	Enabled bool `yaml:"enabled" env:"KASOKU_CLUSTER_ENABLED" default:"false"`

	// Node ID
	NodeID string `yaml:"node_id" env:"KASOKU_NODE_ID" default:"node-1"`

	// Node address (for inter-node communication)
	NodeAddr string `yaml:"node_addr" env:"KASOKU_NODE_ADDR" default:"http://localhost:9000"`

	// gRPC address (for inter-node communication)
	GRPCAddr string `yaml:"grpc_addr" env:"KASOKU_GRPC_ADDR" default:"localhost:9002"`

	// Peer nodes
	Peers []string `yaml:"peers" env:"KASOKU_PEERS"`

	// Gossip port
	GossipPort int `yaml:"gossip_port" env:"KASOKU_GOSSIP_PORT" default:"9002"`

	// Raft port
	RaftPort int `yaml:"raft_port" env:"KASOKU_RAFT_PORT" default:"9003"`

	// Replication factor (number of replicas)
	ReplicationFactor int `yaml:"replication_factor" env:"KASOKU_REPLICATION_FACTOR" default:"3"`

	// Quorum size (minimum acks for write)
	QuorumSize int `yaml:"quorum_size" env:"KASOKU_QUORUM_SIZE" default:"2"`

	// Read quorum (minimum replicas to read from)
	// R = 2 (strong consistency, W+R > N)
	// R = 1 (eventual consistency, faster but may read stale)
	ReadQuorum int `yaml:"read_quorum" env:"KASOKU_READ_QUORUM" default:"2"`

	// Virtual nodes per physical node (for consistent hashing)
	VNodes int `yaml:"vnodes" env:"KASOKU_VNODES" default:"150"`

	// RPC timeout for inter-node communication
	RPCTimeoutMs int `yaml:"rpc_timeout_ms" env:"KASOKU_RPC_TIMEOUT_MS" default:"5000"`
}

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
			Sync:             false, // Async by default for throughput
			SyncInterval:     100 * time.Millisecond,
			MaxFileSize:      64 * 1024 * 1024,
			CheckpointBytes:  64 * 1024 * 1024, // 64MB
			MaxBufferedBytes: 16 * 1024 * 1024, // 16MB
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

func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		// Expand environment variables in YAML content
		expanded := os.ExpandEnv(string(data))

		if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
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

func applyEnvOverrides(cfg *Config) error {
	// DataDir
	if v := os.Getenv("KASOKU_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	// Port
	if v := os.Getenv("KASOKU_PORT"); v != "" {
		var port int
		fmt.Sscanf(v, "%d", &port)
		cfg.Port = port
	}
	// HTTPPort
	if v := os.Getenv("KASOKU_HTTP_PORT"); v != "" {
		var port int
		fmt.Sscanf(v, "%d", &port)
		cfg.HTTPPort = port
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
	// Cluster NodeID
	if v := os.Getenv("KASOKU_NODE_ID"); v != "" {
		cfg.Cluster.NodeID = v
	}
	// Cluster NodeAddr
	if v := os.Getenv("KASOKU_NODE_ADDR"); v != "" {
		cfg.Cluster.NodeAddr = v
	}

	return nil
}

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

func (c *Config) Save(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

func (c *Config) String() string {
	return fmt.Sprintf("Config{DataDir: %s, Port: %d, HTTPPort: %d, LogLevel: %s}",
		c.DataDir, c.Port, c.HTTPPort, c.LogLevel)
}
