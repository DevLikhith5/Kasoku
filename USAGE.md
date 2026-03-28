# Kasoku - Key-Value Storage Engine

A high-performance key-value storage engine with LSM-tree and HashMap backends.

## Overview

Kasoku provides two storage engines:

| Engine             | Storage                       | Use Case                             |
|--------------------|-------------------------------|--------------------------------------|
| **LSM Engine**     | Disk-based with memory buffer | Production workloads, large datasets |
| **HashMap Engine** | In-memory with WAL            | Testing, caching, small datasets     |

## Installation

```bash
go get github.com/DevLikhith5/kasoku
```

## Quick Start

### Using the CLI

```bash
# Build the CLI
go build -o kvctl ./cmd/kvctl

# Store a value
./kvctl put user:1 "Alice"

# Retrieve a value (with JSON output)
./kvctl -o json get user:1

# Delete a key
./kvctl delete user:1

# Scan by prefix (with table output)
./kvctl -o table scan user:

# List all keys
./kvctl keys

# View statistics
./kvctl stats

# Export data to JSON
./kvctl export backup.json

# Import data from JSON
./kvctl import backup.json

# Start interactive shell
./kvctl shell

# Run benchmark
./kvctl bench
```

**Global Flags:**

| Flag                 | Description                            | Default  |
|----------------------|----------------------------------------|----------|
| `-d, --dir <path>`   | Data directory                         | `./data` |
| `-o, --output <fmt>` | Output format: `text`, `json`, `table` | `text`   |
| `--verbose`          | Enable verbose output                  | `false`  |
| `-h, --help`         | Print help message                     | -        |
| `-v, --version`      | Print version                          | -        |

### Using the Go API

#### LSM Engine (Recommended for Production)

```go
package main

import (
    "log"
    lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
)

func main() {
    // Open database (creates if not exists)
    engine, err := lsmengine.NewLSMEngine("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer engine.Close()

    // Write a key-value pair
    err = engine.Put("user:1", []byte("Alice"))
    if err != nil {
        log.Fatal(err)
    }

    // Read by key
    entry, err := engine.Get("user:1")
    if err != nil {
        log.Fatal(err)
    }
    println(string(entry.Value)) // Output: Alice

    // Delete a key
    err = engine.Delete("user:1")
    if err != nil {
        log.Fatal(err)
    }
}
```

#### HashMap Engine (In-Memory)

```go
package main

import (
    "log"
    storage "github.com/DevLikhith5/kasoku/internal/store"
)

func main() {
    engine, err := storage.NewHashmapEngine("./wal.log")
    if err != nil {
        log.Fatal(err)
    }
    defer engine.Close()

    err = engine.Put("key1", []byte("value1"))
    entry, _ := engine.Get("key1")
    println(string(entry.Value)) // Output: value1
}
```

## API Reference

### Put

Stores a key-value pair.

```go
err := engine.Put("user:1", []byte("Alice"))
```

**Limits:**

- Key: maximum 1KB
- Value: maximum 1MB

### Get

Retrieves a value by key.

```go
entry, err := engine.Get("user:1")
if err == storage.ErrKeyNotFound {
    // Key does not exist
}
if err != nil {
    // Handle error
}

// Access entry fields
println(entry.Key)       // "user:1"
println(entry.Value)     // []byte("Alice")
println(entry.Version)   // uint64 (auto-incrementing)
println(entry.TimeStamp) // time.Time (when written)
println(entry.Tombstone) // bool (true if deleted)
```

### Delete

Removes a key. Uses tombstones internally (marks as deleted, does not immediately remove from disk).

```go
err := engine.Delete("user:1")
if err == storage.ErrKeyNotFound {
    // Key does not exist
}
```

### Scan

Returns all entries with keys matching a prefix, sorted lexicographically.

```go
entries, err := engine.Scan("user:")
if err != nil {
    log.Fatal(err)
}

for _, entry := range entries {
    println(entry.Key, "=>", string(entry.Value))
}
// Output:
// user:1 => Alice
// user:2 => Bob
// user:3 => Charlie
```

### Keys

Returns all non-deleted keys.

```go
keys, err := engine.Keys()
if err != nil {
    log.Fatal(err)
}

for _, key := range keys {
    println(key)
}
```

### Stats

Returns engine statistics.

```go
stats := engine.Stats()
println("Total Keys:", stats.KeyCount)
println("Memory Usage:", stats.MemBytes, "bytes")
println("Disk Usage:", stats.DiskBytes, "bytes")
println("Bloom Filter False Positive Rate:", stats.BloomFPRate)
```

## CLI Commands

| Command             | Description                         | Example                    |
|---------------------|-------------------------------------|----------------------------|
| `put <key> <value>` | Store a key-value pair              | `kvctl put user:1 "Alice"` |
| `get <key>`         | Retrieve a value by key             | `kvctl get user:1`         |
| `delete <key>`      | Delete a key                        | `kvctl delete user:1`      |
| `scan <prefix>`     | Scan keys by prefix                 | `kvctl scan user:`         |
| `keys [prefix]`     | List all keys (optionally filtered) | `kvctl keys user:`         |
| `stats`             | Show database statistics            | `kvctl stats`              |
| `dump`              | Dump all keys and values            | `kvctl dump`               |
| `compact`           | Trigger manual compaction           | `kvctl compact`            |
| `import <file>`     | Import data from JSON file          | `kvctl import backup.json` |
| `export <file>`     | Export data to JSON file            | `kvctl export backup.json` |
| `shell`             | Start interactive shell             | `kvctl shell`              |
| `bench`             | Run benchmark tests                 | `kvctl bench`              |

**Output Formats:** Use `-o` flag to change output format:

- `-o text` - Plain text (default)
- `-o json` - JSON format
- `-o table` - Formatted table

## Interactive Shell

Start the interactive shell for a REPL-style experience:

```bash
./kvctl shell
```

### Shell Commands

| Command | Description | Example |
|---------|-------------|---------|
| `SET <key> <value>` | Store a key-value pair | `SET user:1 "Alice"` |
| `GET <key>` | Retrieve a value | `GET user:1` |
| `DEL <key> [key...]` | Delete one or more keys | `DEL user:1 user:2` |
| `EXISTS <key> [key...]` | Check if keys exist | `EXISTS user:1 user:2` |
| `KEYS <pattern>` | Find keys matching pattern | `KEYS user:*` |
| `SCAN [cursor] [MATCH pattern]` | Iterate over keys | `SCAN 0 MATCH user:*` |
| `TTL <key>` | Get time to live | `TTL user:1` |
| `INFO` | Show server statistics | `INFO` |
| `FLUSHALL` | Delete all keys | `FLUSHALL` |
| `DUMP <key>` | Show key with metadata | `DUMP user:1` |
| `PING` | Check connectivity | `PING` |
| `CLEAR` | Clear screen | `CLEAR` |
| `EXIT` | Exit shell | `EXIT` |

Commands are case-insensitive (`SET`, `set`, `Set` all work).

### Pattern Matching

Use wildcards in `KEYS` and `SCAN` commands:

- `*` - Matches any sequence of characters
  - `user:*` matches `user:1`, `user:alice`, etc.
  - `*key*` matches `mykey`, `key123`, etc.
- `?` - Matches any single character
  - `user:?` matches `user:1`, `user:a`, etc.
  - `user:??` matches `user:12`, `user:ab`, etc.

### Shell Example

```text
Kasoku Interactive Shell
Commands: SET, GET, DEL, EXISTS, KEYS, SCAN, TTL, INFO, FLUSHALL, DUMP, PING, CLEAR, EXIT
──────────────────────────────────────────────────
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET user:1 "Alice"
OK
127.0.0.1:6379> SET user:2 "Bob"
OK
127.0.0.1:6379> GET user:1
"Alice"
127.0.0.1:6379> KEYS user:*
1) "user:1"
2) "user:2"
127.0.0.1:6379> EXISTS user:1 user:2 nonexistent
(integer) 2
127.0.0.1:6379> DEL user:1
(integer) 1
127.0.0.1:6379> INFO
# Server
redis_version:7.0.0
redis_mode:standalone
# Keyspace
db0:keys=1,expires=0,avg_ttl=0
127.0.0.1:6379> EXIT
Goodbye!
```

## Data Storage

### Disk Layout

Data is stored in the specified directory (default: `./data`):

```text
data/
├── wal.log          # Write-Ahead Log (all operations)
├── L0_*.sst         # Level 0 SSTables (freshly flushed)
├── L1_*.sst         # Level 1 SSTables (compacted)
└── L2_*.sst         # Level 2 SSTables (further compacted)
```

### Write Path

1. Write is appended to WAL (durability)
2. Write is added to in-memory MemTable
3. When MemTable fills, it flushes to disk as an SSTable
4. Background compaction merges SSTables to reduce disk usage

### Read Path

1. Check active MemTable (fastest)
2. Check immutable MemTable (being flushed)
3. Check SSTables from L0 to Ln (uses bloom filter to skip unnecessary reads)

### Compaction

Background process that:

- Merges multiple SSTables into one
- Keeps only the latest version of each key
- Removes expired tombstones (deleted keys older than 24 hours)
- Moves data from L0 → L1 → L2 for better read performance

## Error Handling

```go
import storage "github.com/DevLikhith5/kasoku/internal/store"

// Common errors
storage.ErrKeyNotFound   // Key does not exist
storage.ErrKeyTooLong    // Key exceeds 1KB limit
storage.ErrValueTooLarge // Value exceeds 1MB limit
storage.ErrEngineClosed  // Engine was already closed
```

## Graceful Shutdown

Always close the engine to ensure data is flushed to disk:

```go
engine, err := lsmengine.NewLSMEngine("./data")
if err != nil {
    log.Fatal(err)
}
defer engine.Close()

// Or explicit shutdown
if err := engine.Close(); err != nil {
    log.Printf("Shutdown error: %v", err)
}
```

## Running the Server

```bash
go run cmd/server/main.go -dir ./data -port 9000
```

**Flags:**

- `-dir`: Data directory (default: `./data`)
- `-port`: Server port (default: `9000`)

## Architecture

```text
┌─────────────────────────────────────────┐
│           StorageEngine                 │
│           (interface)                   │
└─────────────────────────────────────────┘
                │
        ┌───────┴───────┐
        │               │
┌───────▼───────┐ ┌─────▼──────┐
│  LSMEngine    │ │ HashMap    │
│               │ │ Engine     │
│ ┌───────────┐ │ │ ┌────────┐ │
│ │ MemTable  │ │ │ │ HashMap│ │
│ │ SSTables  │ │ │ │ + WAL  │ │
│ │ WAL       │ │ │ └────────┘ │
│ │ Bloom     │ │ └────────────┘
│ └───────────┘ │
└───────────────┘
```

## Features

- **Thread-safe**: Concurrent reads and writes supported
- **Durability**: WAL ensures crash recovery
- **Sorted keys**: Lexicographic ordering
- **Prefix scans**: Efficient range queries
- **Tombstones**: Proper delete handling with cleanup
- **Bloom filters**: Fast negative lookups (no disk read if key absent)
- **Version tracking**: MVCC-ready with auto-incrementing versions
- **Graceful shutdown**: Ensures all data is flushed

## Performance Characteristics

| Operation     | LSM Engine                       | HashMap Engine |
|---------------|----------------------------------|----------------|
| Point Read    | O(log N) with bloom filter       | O(1)           |
| Write         | O(1) amortized                   | O(1)           |
| Scan          | O(N + M) where M = matching keys | O(N)           |
| Delete        | O(1) (tombstone)                 | O(1)           |
| Memory Usage  | Buffer only                      | All data       |
| Max Size      | Disk capacity                    | RAM capacity   |

## Performance Characteristics

| Operation     | LSM Engine                       | HashMap Engine |
|---------------|----------------------------------|----------------|
| Point Read    | O(log N) with bloom filter       | O(1)           |
| Write         | O(1) amortized                   | O(1)           |
| Scan          | O(N + M) where M = matching keys | O(N)           |
| Delete        | O(1) (tombstone)                 | O(1)           |
| Memory Usage  | Buffer only                      | All data       |
| Max Size      | Disk capacity                    | RAM capacity   |

## WAL Durability Configuration

The Write-Ahead Log (WAL) can be configured to trade durability for performance:

### Default: Sync on Every Write (Safest)

```go
engine, err := lsmengine.NewLSMEngine("./data")
// Equivalent to:
engine, err := lsmengine.NewLSMEngineWithConfig("./data", lsmengine.LSMConfig{
    WALSyncInterval: 0, // Sync on every write
})
```

**Pros:** Full durability — no data loss on power failure
**Cons:** ~300-400 writes/sec (limited by disk fsync latency)

### Background Sync (Recommended for Production)

```go
engine, err := lsmengine.NewLSMEngineWithConfig("./data", lsmengine.LSMConfig{
    WALSyncInterval: 100 * time.Millisecond, // Sync every 100ms
})
```

**Pros:** ~40,000+ writes/sec (100x faster)
**Cons:** Up to 100ms of data may be lost on hard power crash

This is the same tradeoff as **Redis AOF "everysec"** mode — acceptable for most applications.

### Performance Comparison (M1 SSD, 10K writes)

| Configuration         | Time (10K writes) | Throughput     |
|-----------------------|-------------------|----------------|
| Sync on Write         | ~37.5 seconds     | ~267 ops/sec   |
| Sync Every 100ms      | ~48 milliseconds  | ~208,000 ops/sec |

> **Note:** The 780x speedup comes from batching 100+ writes into a single fsync.

### Benchmark Your Setup

```bash
cd /Users/cvlikhith/kasoku
go test ./internal/store/lsm-engine -bench=Benchmark10KWrites -benchmem -run=^$ -benchtime=1x
```

## Limitations

- Keys limited to 1KB
- Values limited to 1MB
- Tombstones expire after 24 hours (deleted keys may reappear if not compacted within this window)
