# Kasoku Configuration Reference

Complete guide to configuring Kasoku storage engine.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Configuration File](#configuration-file)
3. [Environment Variables](#environment-variables)
4. [Configuration Sections](#configuration-sections)
5. [Production Recommendations](#production-recommendations)

---

## Quick Start

### Default Configuration

```bash
# Start server with defaults
./kasoku-server

# Or specify config file
./kasoku-server --config kasoku.yaml
```

### Minimal Config File

```yaml
# kasoku.yaml
data_dir: /var/lib/kasoku
port: 9000
log_level: info
```

---

## Configuration File

### Location

Default locations checked (in order):
1. `./kasoku.yaml` (current directory)
2. `~/.kasoku/kasoku.yaml` (home directory)
3. `/etc/kasoku/kasoku.yaml` (system-wide)

### Format

YAML format with the following structure:

```yaml
# Server settings
data_dir: ./data
port: 9000
http_port: 9001
log_level: info
log_file: /var/log/kasoku/server.log

# LSM Engine settings
lsm:
  levels: 7
  level_ratio: 10.0
  l0_base_size: 67108864  # 64MB

# Compaction settings
compaction:
  threshold: 4
  max_concurrent: 2
  l0_size_threshold: 134217728  # 128MB

# Memory settings
memory:
  memtable_size: 67108864      # 64MB
  max_memtable_bytes: 268435456 # 256MB
  bloom_fp_rate: 0.01
  block_cache_size: 134217728   # 128MB

# WAL settings
wal:
  sync: true
  sync_interval: 100ms
  max_file_size: 67108864  # 64MB

# Cluster settings (optional)
cluster:
  enabled: false
  node_id: node-1
  peers: []
  gossip_port: 9002
  raft_port: 9003
```

---

## Environment Variables

All configuration options can be overridden via environment variables:

```bash
# Server
export KASOKU_DATA_DIR=/var/lib/kasoku
export KASOKU_PORT=9000
export KASOKU_HTTP_PORT=9001
export KASOKU_LOG_LEVEL=info
export KASOKU_LOG_FILE=/var/log/kasoku/server.log

# LSM Engine
export KASOKU_LSM_LEVELS=7
export KASOKU_LSM_LEVEL_RATIO=10.0
export KASOKU_LSM_L0_BASE_SIZE=67108864

# Compaction
export KASOKU_COMPACTION_THRESHOLD=4
export KASOKU_COMPACTION_MAX_CONCURRENT=2
export KASOKU_COMPACTION_L0_SIZE_THRESHOLD=134217728

# Memory
export KASOKU_MEMTABLE_SIZE=67108864
export KASOKU_MAX_MEMTABLE_BYTES=268435456
export KASOKU_BLOOM_FP_RATE=0.01
export KASOKU_BLOCK_CACHE_SIZE=134217728

# WAL
export KASOKU_WAL_SYNC=true
export KASOKU_WAL_SYNC_INTERVAL=100ms
export KASOKU_WAL_MAX_FILE_SIZE=67108864

# Cluster
export KASOKU_CLUSTER_ENABLED=false
export KASOKU_NODE_ID=node-1
export KASOKU_PEERS=""
export KASOKU_GOSSIP_PORT=9002
export KASOKU_RAFT_PORT=9003
```

### Priority Order

1. Environment variables (highest priority)
2. Configuration file
3. Default values (lowest priority)

---

## Configuration Sections

### Server Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `data_dir` | string | `./data` | Directory for storing data files (WAL, SSTables) |
| `port` | int | `9000` | Main server port (gRPC/TCP) |
| `http_port` | int | `9001` | HTTP API port |
| `log_level` | string | `info` | Logging level: `debug`, `info`, `warn`, `error` |
| `log_file` | string | `` | Log file path (empty = stdout) |

**Example**:
```yaml
data_dir: /mnt/ssd/kasoku
port: 9000
http_port: 9001
log_level: debug
log_file: /var/log/kasoku/server.log
```

---

### LSM Engine Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `levels` | int | `7` | Number of levels in LSM tree (L0-L6) |
| `level_ratio` | float | `10.0` | Size multiplier between levels |
| `l0_base_size` | int64 | `64MB` | Target size for L0 SSTables |

**How Levels Work**:
- L0: 64MB SSTables (freshly flushed from MemTable)
- L1: 640MB (10x L0)
- L2: 6.4GB (10x L1)
- L3: 64GB
- ...and so on

**Example**:
```yaml
lsm:
  levels: 7
  level_ratio: 10.0
  l0_base_size: 67108864  # 64MB
```

---

### Compaction Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `threshold` | int | `4` | Number of SSTables to trigger compaction |
| `max_concurrent` | int | `2` | Maximum concurrent compaction goroutines |
| `l0_size_threshold` | int64 | `128MB` | Total L0 size to trigger compaction |

**Compaction Behavior**:
- When L0 has ≥4 SSTables → compact to L1
- When L1 has ≥4 SSTables → compact to L2
- Continues up the levels

**Example**:
```yaml
compaction:
  threshold: 4
  max_concurrent: 2
  l0_size_threshold: 134217728  # 128MB
```

---

### Memory Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `memtable_size` | int64 | `64MB` | Size threshold for MemTable flush |
| `max_memtable_bytes` | int64 | `256MB` | Maximum total memory for all memtables |
| `bloom_fp_rate` | float | `0.01` | Bloom filter false positive rate (0.01 = 1%) |
| `block_cache_size` | int64 | `128MB` | LRU cache size for hot data blocks |

**Memory Usage**:
- Active MemTable: up to 64MB
- Immutable MemTable: up to 64MB (while flushing)
- Block Cache: up to 128MB
- **Total RAM**: ~256MB typical

**Example**:
```yaml
memory:
  memtable_size: 67108864      # 64MB
  max_memtable_bytes: 268435456 # 256MB
  bloom_fp_rate: 0.01          # 1% false positive rate
  block_cache_size: 134217728   # 128MB block cache
```

---

### WAL Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `sync` | bool | `true` | Sync WAL on every write (safer, slower) |
| `sync_interval` | duration | `100ms` | Background sync interval (if sync=false) |
| `max_file_size` | int64 | `64MB` | WAL file size before rotation |

**Durability vs Performance**:

| Setting | Durability | Performance | Use Case |
|---------|------------|-------------|----------|
| `sync: true` | Full (no data loss) | Slower | Production, financial data |
| `sync: false, sync_interval: 100ms` | ~100ms data loss on crash | Faster | Logging, caching |

**Example**:
```yaml
wal:
  sync: true              # Safest
  sync_interval: 100ms    # Used only if sync=false
  max_file_size: 67108864 # 64MB
```

---

### Cluster Settings (Future)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `false` | Enable distributed cluster mode |
| `node_id` | string | `node-1` | Unique node identifier |
| `peers` | list | `[]` | List of peer node addresses |
| `gossip_port` | int | `9002` | Gossip protocol port |
| `raft_port` | int | `9003` | Raft consensus port |

**Example**:
```yaml
cluster:
  enabled: true
  node_id: node-1
  peers:
    - 192.168.1.10:9000
    - 192.168.1.11:9000
  gossip_port: 9002
  raft_port: 9003
```

---

## Production Recommendations

### High Performance (SSD)

```yaml
data_dir: /mnt/nvme/kasoku

memory:
  memtable_size: 134217728    # 128MB
  block_cache_size: 536870912 # 512MB

wal:
  sync: false
  sync_interval: 10ms

compaction:
  max_concurrent: 4
```

### High Durability (Financial Data)

```yaml
wal:
  sync: true  # Never lose data

memory:
  bloom_fp_rate: 0.005  # Lower false positive rate
```

### Low Memory (Embedded/Raspberry Pi)

```yaml
memory:
  memtable_size: 16777216     # 16MB
  max_memtable_bytes: 33554432 # 32MB
  block_cache_size: 33554432   # 32MB

lsm:
  levels: 5  # Fewer levels
```

### Large Dataset (TB Scale)

```yaml
lsm:
  levels: 10
  l0_base_size: 268435456  # 256MB

memory:
  memtable_size: 268435456   # 256MB
  block_cache_size: 1073741824 # 1GB

compaction:
  l0_size_threshold: 1073741824 # 1GB
```

---

## Validation Rules

The configuration validator enforces:

- `data_dir`: Cannot be empty, resolved to absolute path
- `port`: Must be 1-65535
- `http_port`: Must be 1-65535
- `log_level`: Must be `debug`, `info`, `warn`, or `error`
- `memtable_size`: Must be at least 1MB
- `bloom_fp_rate`: Must be between 0 and 1

---

## Example Configurations

### Development

```yaml
# kasoku.dev.yaml
data_dir: ./data
log_level: debug
memory:
  memtable_size: 16777216  # 16MB
```

### Production Web App

```yaml
# kasoku.prod.yaml
data_dir: /var/lib/kasoku
port: 9000
log_level: info
log_file: /var/log/kasoku/server.log

memory:
  memtable_size: 134217728    # 128MB
  block_cache_size: 268435456 # 256MB

wal:
  sync: false
  sync_interval: 50ms
```

### Benchmarking

```yaml
# kasoku.bench.yaml
data_dir: /tmp/kasoku-bench
log_level: error

memory:
  memtable_size: 268435456    # 256MB
  block_cache_size: 1073741824 # 1GB

wal:
  sync: false  # Fastest (unsafe)
```

---

## Troubleshooting

### Out of Memory

```yaml
# Reduce memory usage
memory:
  memtable_size: 33554432     # 32MB
  max_memtable_bytes: 67108864 # 64MB
  block_cache_size: 67108864   # 64MB
```

### Slow Writes

```yaml
# Increase MemTable size (fewer flushes)
memory:
  memtable_size: 134217728  # 128MB

# Or disable WAL sync (less durable)
wal:
  sync: false
  sync_interval: 100ms
```

### Slow Reads

```yaml
# Increase block cache
memory:
  block_cache_size: 536870912  # 512MB

# Reduce bloom filter false positive rate
memory:
  bloom_fp_rate: 0.005
```

### Disk Space Issues

```yaml
# More aggressive compaction
compaction:
  threshold: 3  # Compact sooner
```

---

## API Reference

### Load Configuration

```go
import "github.com/DevLikhith5/kasoku/internal/config"

// Load from file
cfg, err := config.Load("kasoku.yaml")

// Load with defaults
cfg := config.DefaultConfig()

// Validate manually
err := cfg.Validate()

// Save to file
err := cfg.Save("kasoku.yaml")
```

### Configuration Struct

```go
type Config struct {
    DataDir    string
    Port       int
    HTTPPort   int
    LogLevel   string
    LogFile    string
    LSM        LSMConfig
    Compaction CompactionConfig
    Memory     MemoryConfig
    WAL        WALConfig
    Cluster    ClusterConfig
}
```

---

## Changelog

### Version 1.0
- Added block compression (Snappy)
- Added LRU block cache
- Added WAL compaction (checkpoint + truncate)
- Added iterator API
- Improved compaction (sequential level-by-level)
