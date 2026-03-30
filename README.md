# Kasoku

**High-Performance LSM Key-Value Storage Engine**

[![Go Version](https://img.shields.io/badge/Go-1.25+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

> A modern, production-ready key-value storage engine inspired by LevelDB and RocksDB, built in Go.

---

## Features

### Core Features
- LSM Tree Architecture - Optimized for high write throughput
- Write-Ahead Log (WAL) - Crash recovery and durability
- Bloom Filters - Fast negative lookups (skip disk reads)
- Block Compression (Snappy) - 2-5x storage reduction
- LRU Block Cache - Cache hot data blocks for fast reads
- Automatic Compaction - Background merging of SSTables
- Skip List MemTable - O(log n) in-memory inserts
- Iterator API - Efficient range scans with cursor
- Tombstones - Proper delete handling with 24h TTL
- MVCC Versions - Version tracking for each key

### Performance
- **Writes**: ~200,000 ops/sec (with 10ms WAL sync)
- **Reads**: ~5,000,000 ops/sec (cached)
- **Scans**: Efficient prefix-based range queries
- **Storage**: 2-5x reduction with Snappy compression

---

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/DevLikhith5/kasoku.git
cd kasoku

# Build binaries
make build

# Or manually
go build -o kvctl ./cmd/kvctl
go build -o kasoku-server ./cmd/server
```

### Requirements
- Go 1.25 or higher
- Linux/macOS (Windows support experimental)

---

## Quick Start

### Basic Operations

```bash
# Start using kvctl (no server needed!)
./kvctl put user:1 "Alice"
./kvctl get user:1
./kvctl delete user:1

# Scan with prefix
./kvctl scan user:

# List all keys
./kvctl keys

# View statistics
./kvctl stats
```

### Benchmark

```bash
# Run performance benchmark
./kvctl bench --wal-sync 10

# Output:
# ┌───────────┬───────┬──────────┬───────────┐
# │ OPERATION │  OPS  │ DURATION │ OPS / SEC │
# ├───────────┼───────┼──────────┼───────────┤
# │ Write     │ 10000 │ 50ms     │ 200000    │
# │ Read      │ 10000 │ 2ms      │ 5000000   │
# └───────────┴───────┴──────────┴───────────┘
```

### Interactive Shell

```bash
./kvctl shell

kvctl> put user:1 "Alice"
OK
kvctl> get user:1
user:1 = Alice
kvctl> scan user:
user:1 = Alice
kvctl> stats
kvctl> exit
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [USAGE.md](USAGE.md) | Complete usage guide |
| [CONFIGURATION.md](CONFIGURATION.md) | Configuration reference |
| [STUDY-GUIDE.md](STUDY-GUIDE.md) | Learning guide & internals |
| [IMPROVEMENTS.md](IMPROVEMENTS.md) | Performance improvements |

---

## Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────┐
│                    kvctl (CLI)                           │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                  StorageEngine Interface                 │
│   Get() | Put() | Delete() | Scan() | Iter() | Keys()   │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                    LSMEngine Core                        │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │  Write Path                                         │ │
│  │  PUT → WAL → MemTable → SSTable (flush)            │ │
│  └────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────┐ │
│  │  Read Path                                          │ │
│  │  GET → MemTable → Bloom → Cache → SSTable          │ │
│  └────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────┐ │
│  │  Background                                         │ │
│  │  Flush Loop | Compaction Loop | WAL Sync           │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### Storage Format

```
data/
├── wal.log              # Write-Ahead Log (durability)
├── L0_*.sst            # Level 0 SSTables (freshly flushed)
├── L1_*.sst            # Level 1 SSTables (compacted)
└── L2_*.sst            # Level 2 SSTables (further compacted)
```

### SSTable Format

```
┌─────────────────────────────┐
│  Data Blocks (compressed)   │  ← Snappy compression
├─────────────────────────────┤
│  Index (sorted)             │  ← Binary search
├─────────────────────────────┤
│  Bloom Filter               │  ← Skip unnecessary reads
├─────────────────────────────┤
│  Footer (32 bytes)          │  ← Metadata
└─────────────────────────────┘
```

---

## Configuration

### Example Configuration (kasoku.yaml)

```yaml
# Server settings
data_dir: ./data
port: 9000
log_level: info

# Memory settings
memory:
  memtable_size: 67108864      # 64MB
  block_cache_size: 134217728  # 128MB
  bloom_fp_rate: 0.01

# WAL settings
wal:
  sync: false
  sync_interval: 100ms  # Background sync (recommended)

# Compaction
compaction:
  threshold: 4
  max_concurrent: 2
```

### Environment Variables

```bash
export KASOKU_DATA_DIR=/var/lib/kasoku
export KASOKU_WAL_SYNC=false
export KASOKU_WAL_SYNC_INTERVAL=100ms
export KASOKU_MEMTABLE_SIZE=67108864
```

See [CONFIGURATION.md](CONFIGURATION.md) for complete reference.

---

## Commands

| Command | Description | Example |
|---------|-------------|---------|
| `put` | Store key-value | `kvctl put key value` |
| `get` | Retrieve value | `kvctl get key` |
| `delete` | Remove key | `kvctl delete key` |
| `scan` | Prefix scan | `kvctl scan user:` |
| `keys` | List all keys | `kvctl keys` |
| `stats` | Show statistics | `kvctl stats` |
| `bench` | Run benchmark | `kvctl bench --wal-sync 10` |
| `shell` | Interactive mode | `kvctl shell` |
| `flush` | Force flush | `kvctl flush` |
| `compact` | Trigger compaction | `kvctl compact` |
| `import` | Import from JSON | `kvctl import data.json` |
| `export` | Export to JSON | `kvctl export data.json` |

---

## Performance Benchmarks

### Write Performance

| WAL Sync | Ops/Sec | Latency | Use Case |
|----------|---------|---------|----------|
| `0ms` (none) | ~50,000 | <1ms | Testing |
| `10ms` | ~20,000 | <5ms | High-throughput |
| `100ms` | ~5,000 | <20ms | Production |
| `sync=true` | ~250 | <100ms | Critical data |

### Read Performance

| Scenario | Ops/Sec | Latency |
|----------|---------|---------|
| Cached (hot) | ~5,000,000 | <100μs |
| MemTable | ~1,000,000 | <1ms |
| SSTable (disk) | ~10,000 | 1-5ms |

---

## Testing

```bash
# Run all tests
make test

# Run specific tests
go test ./internal/store/lsm-engine/... -v

# Run benchmarks
make bench

# Format code
make fmt

# Lint code
make lint
```

---

## Project Structure

```
kasoku/
├── cmd/
│   ├── kvctl/           # CLI client (modular)
│   │   ├── commands/    # Command implementations
│   │   ├── output/      # Output formatting
│   │   ├── engine/      # Engine management
│   │   └── validate/    # Input validation
│   └── server/          # Server (optional)
├── internal/
│   ├── store/           # Storage engine
│   │   ├── lsm-engine/  # LSM implementation
│   │   ├── wal.go       # Write-Ahead Log
│   │   └── engine.go    # Interface
│   └── config/          # Configuration
├── data/                # Database files (gitignored)
└── docs/                # Documentation
```

---

## Development

### Build Commands

```bash
# Build all binaries
make build

# Run server
make server

# Clean build artifacts
make clean

# Format code
make fmt

# Lint code
make lint
```

### Adding New Commands

1. Create new file in `cmd/kvctl/commands/`
2. Define command with Cobra
3. Register in `init()`

```go
// commands/mycmd.go
var myCmd = &cobra.Command{
    Use:   "mycmd",
    Short: "My command",
    RunE: func(cmd *cobra.Command, args []string) error {
        // Your logic
        return nil
    },
}

func init() {
    RootCmd.AddCommand(myCmd)
}
```

---

## Use Cases

### Good For

- Session Storage - Fast writes, TTL-ready
- Metrics/Time-Series - High write throughput
- Cache Layer - Block cache for hot data
- Event Logging - Sequential writes
- Key-Value Store - Core functionality
- Configuration Store - Fast reads

### Not Ideal For

- Transactions - No ACID guarantees
- Complex Queries - No SQL, indexes
- Full-Text Search - No text indexing
- Graph Data - No relationship tracking

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing`)
3. Commit your changes (`git commit -am 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing`)
5. Create a Pull Request

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- **LevelDB** - Original LSM implementation by Google
- **RocksDB** - Facebook's enhanced version
- **BadgerDB** - Go-based LSM database
- **Cobra** - CLI framework used in kvctl

---

## Contact

- **Author**: DevLikhith5
- **Repository**: [github.com/DevLikhith5/kasoku](https://github.com/DevLikhith5/kasoku)
- **Issues**: [GitHub Issues](https://github.com/DevLikhith5/kasoku/issues)

---

<div align="center">

**Built with Go**

[Back to Top](#kasoku)

</div>
