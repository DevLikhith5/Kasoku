# Kasoku
<img width="1428" height="900" alt="image" src="https://github.com/user-attachments/assets/179de515-be24-43e5-a218-b4a252cf08f8" />
<img width="1428" height="900" alt="image" src="https://github.com/user-attachments/assets/532f5a90-ddce-45f9-be65-1eea8b0976dc" />
<img width="1440" height="889" alt="image" src="https://github.com/user-attachments/assets/69baa06c-787e-46f3-804c-6a375c76d645" />


**Distributed Key-Value Storage Engine**

Kasoku is a distributed, highly available key-value storage engine written entirely in Go. It is built on a custom Log-Structured Merge-Tree (LSM-Tree) beneath a Dynamo-style distributed cluster layer. It is designed to serve production workloads that require horizontal scalability, strong durability guarantees, and resilience to node failures.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Storage Engine](#storage-engine)
3. [Distributed Cluster Layer](#distributed-cluster-layer)
4. [Performance Benchmarks](#performance-benchmarks)
5. [Project Structure](#project-structure)
6. [Getting Started](#getting-started)
7. [Configuration](#configuration)
8. [License and Usage Restrictions](#license-and-usage-restrictions)

---

## Architecture

Kasoku consists of two major subsystems that operate in tandem.

The **storage layer** is a custom-built LSM-Tree that persists data durably to disk using Write-Ahead Logging, compressed SSTables, Bloom Filters, and a background compaction scheduler.

The **cluster layer** implements a fully masterless, peer-to-peer topology where every node can coordinate reads and writes. Cluster membership, failure detection, and data reconciliation are handled entirely in-process without any external dependencies such as ZooKeeper or etcd.

```
Client Request
     |
     v
HTTP API Handler
     |
     v
Coordinator (Consistent Hash Ring -> replica nodes selected)
     |
     +-------> Local LSM-Tree Write + WAL
     |
     +-------> Remote Replica Write (RPC) x 2 (W=2 quorum)
                     |
           Hinted Handoff if replica down
```

Every node runs the same binary and code path. There is no leader election. Any node accepts both read and write requests.

---

## Storage Engine

The LSM-Tree engine is designed specifically for write-heavy workloads. All writes are sequential, which saturates disk throughput. Reads are optimized through in-memory structures and probabilistic filters.

### Write Path

1. The entry is appended to the Write-Ahead Log (WAL) on disk. With `wal.sync: true`, each write is fsynced; with `wal.sync: false`, a background thread syncs every `wal.sync_interval` (default 100ms).
2. The entry is inserted into the in-memory MemTable (implemented as a probabilistic Skip List ordered by key).
3. When the MemTable exceeds its configured capacity (default 64MB), it is frozen and flushed to a Level 0 SSTable on disk.
4. Background compaction merges Level 0 SSTables into progressively larger sorted levels to eliminate duplicate and deleted keys.

### Read Path

1. The active MemTable is checked first.
2. Immutable MemTables awaiting flush are checked next.
3. SSTables are searched from newest to oldest. Each SSTable consults its Bloom Filter before performing any disk I/O. If the Bloom Filter indicates the key is absent, the SSTable is skipped entirely.
4. The LRU Block Cache serves recently accessed disk blocks from memory to avoid repeated reads.

### Storage Engine Features

| Feature | Detail |
| :--- | :--- |
| Write-Ahead Log | Configurable sync: per-write fsync or background sync (default 100ms interval); atomic WAL truncation via rename |
| MemTable | Probabilistic Skip List (concurrent-safe, lock-free reads under MemTable lock) |
| SSTables | Sorted, immutable, Snappy-compressed segments |
| Bloom Filters | Per-SSTable, configurable false positive rate (default 1%) |
| LRU Block Cache | Configurable size; shared across all SSTable readers |
| Compaction | Leveled compaction, runs as a background goroutine |
| Tombstones | Soft-deletes tracked across levels, purged during compaction |
| MVCC Versioning | Monotonic version counter per key for conflict resolution |
| WAL Checkpointing | Periodic offset checkpoints enable crash recovery and log truncation |

---

## Distributed Cluster Layer

The cluster layer is implemented entirely in-process and requires no external coordination service.

### Consistent Hashing Ring

Data is partitioned across nodes using a CRC32-based consistent hashing ring. Each node occupies 150 virtual node positions (vnodes) on the ring by default. This ensures that adding or removing a node requires relocating approximately 1/N of keys rather than re-partitioning the entire dataset.

### Quorum Replication

Write and read operations follow a quorum model with the following defaults:

- Replication factor: N = 3
- Write quorum: W = 2
- Read quorum: R = 2

The constraint W + R > N (2 + 2 > 3) guarantees that any read set will overlap with the most recent write set by at least one replica, providing read-your-writes consistency under normal operation.

### Gossip Protocol

Cluster membership state is propagated using an epidemic gossip protocol. Each node periodically exchanges its member list with a random subset of peers. This achieves eventual consistency of cluster state across all nodes in O(log N) gossip rounds without any central registry.

### Phi Accrual Failure Detection

Node health is tracked using the Phi Accrual failure detector rather than fixed timeouts. The detector continuously measures the inter-arrival time of heartbeat messages from each peer and computes a suspicion level (phi) based on the statistical distribution of observed intervals. A node is considered unhealthy when phi exceeds a configurable threshold (default 8.0), adapting automatically to variations in network latency without producing false positives under temporary slowdowns.

### Hinted Handoff

When a write is destined for a replica node that is currently unavailable, the coordinating node stores a timestamped hint in its local hint store. A background delivery loop retries delivering all pending hints every 10 seconds. Hints expire after 24 hours. This mechanism preserves write availability during short network partitions without permanently compromising consistency.

### Anti-Entropy with Merkle Trees

A background anti-entropy loop runs every 30 seconds. Each node builds a SHA-256 Merkle Tree over all keys it holds. It exchanges this tree with each peer and computes the symmetric difference in O(K log N) time, where K is the number of differing keys. Only the divergent keys are synchronized, minimizing network bandwidth. This mechanism heals data divergence caused by expired hints, node crashes, or prolonged partitions.

### Vector Clocks

Every write is associated with a vector clock entry identifying the originating node and the logical time of the write. Vector clocks support three ordering comparisons: Before, After, and Concurrent. Concurrent writes (where neither clock dominates) represent a true conflict that can be resolved by application-level policy or Last-Write-Wins using the attached version counter.

### Distributed Cluster Features

| Feature | Detail |
| :--- | :--- |
| Topology | Fully masterless, symmetric peer-to-peer |
| Partitioning | CRC32 consistent hashing with 150 virtual nodes per node |
| Replication | N=3, W=2, R=2 quorum; configurable |
| Membership | Gossip protocol; no external dependency |
| Failure Detection | Phi Accrual detector; adaptive to network jitter |
| Write Durability | Hinted Handoff with 24-hour expiry and background retry |
| Data Reconciliation | SHA-256 Merkle Tree anti-entropy; O(K log N) diff |
| Conflict Tracking | Vector clocks with Before / After / Concurrent comparison |
| Read Repair | Coordinator detects stale replicas on read and patches them |

---

## Performance Benchmarks

Benchmarks executed on Apple M1 (ARM64, 8-core) using the `pressure` load testing tool (Dynamo-style).

### Single Node (Optimized - April 2026)

| Operation | Type | Throughput | Latency p50 | Latency p99 |
| :--- | :--- | ---: | ---: | ---: |
| **Writes** | Single-key | **118,000 ops/sec** | 70µs | 675µs |
| **Reads** | Single-Key | **308,000 ops/sec** | 56µs | 647µs |
| **Reads** | Batch (25 keys) | **435,000 ops/sec** | — | — |

### 3-Node Cluster (RF=3, W=2, R=1)

| Operation | Type | Throughput | Latency p50 | Latency p99 |
| :--- | :--- | ---: | ---: | ---: |
| **Writes** | Single-key (quorum) | **24,000-62,000 ops/sec** | 495µs | 3.88ms |
| **Reads** | Single-Key | **28,000 ops/sec** | 85µs | 79.65ms |
| **Reads** | Batch (peak) | **262,000 ops/sec** | — | — |

### Comparison with Dynamo Paper & DynamoDB

| System | Writes (single-key) | Reads (single-key) | Batch Reads |
|--------|-------------------|-------------------|-------------|
| **Dynamo Paper (2007)** | ~100,000+ ops/sec | ~100,000+ ops/sec | N/A |
| **DynamoDB** | ~50,000+ ops/sec | ~50,000+ ops/sec | ~200,000+ ops/sec |
| **Cassandra** | ~50,000 ops/sec | ~50,000 ops/sec | ~100,000 ops/sec |
| **Kasoku (single node)** | **118,000 ops/sec** | **308,000 ops/sec** | **435,000 ops/sec** |
| **Kasoku (cluster)** | **24,000-62,000 ops/sec** | **28,000 ops/sec** | **262,000 ops/sec** |

### Optimizations Applied

- **WAL**: Async batch sync (100ms interval + 1MB checkpoint)
- **Encoding**: Pure binary with magic byte (no JSON)
- **Block Size**: 64KB (was 4KB)
- **Caches**: 1GB block cache, 1M key cache entries
- **MemTable**: 256MB (was 64MB)
- **Level Ratio**: 10 (was 2) — fewer levels = faster compaction
- **Parallel Compaction**: Concurrent level compactions

### Key Insights

- **Batch operations** are significantly faster than single-key due to amortizing HTTP overhead.
- **Single-node** outperforms cluster for writes due to no quorum overhead.
- **Cluster reads** can exceed single node with R=1 (eventual consistency).
- All benchmarks use background compaction — never blocks read/write operations.
- **Eventual Consistency Mode**: Set `quorum_size: 1` and `read_quorum: 1` in config for ~3x faster reads.

### Environment & Notes

- **Hardware**: Apple Silicon (ARM64, 8-core)
- **Network**: localhost loopback (no external network latency)
- **Workers**: 60 concurrent goroutines in pressure-tool
- **Duration**: 20 second measurement phase per operation

> **Note**: Performance varies based on hardware, system load, and workload characteristics. Batch operations provide best throughput for high-volume scenarios.

---

## Project Structure

```
kasoku/
├── cmd/
│   ├── server/         Entry point for the cluster node HTTP server
│   └── kvctl/          Entry point for the kvctl command-line client
├── internal/
│   ├── cluster/        Gossip, Phi failure detector, quorum, hinted handoff, anti-entropy, vector clocks
│   ├── config/         YAML configuration loading and validation
│   ├── merkle/         SHA-256 Merkle tree implementation
│   ├── metrics/        Prometheus metrics exposition
│   ├── ring/           CRC32 consistent hash ring with virtual nodes
│   ├── rpc/            HTTP-based cross-node RPC client
│   ├── server/         HTTP server middleware and routing
│   └── store/
│       ├── lsm-engine/ WAL, MemTable, SSTable, Bloom Filter, Block Cache, Compactor
│       └── hashmap/    In-memory fallback engine for testing
├── kasoku.yaml         Active cluster configuration
├── kasoku.example.yaml Annotated reference configuration
├── Makefile            Build, test, and lint targets
└── USAGE.md            Detailed API reference and operation examples
```

---

## Getting Started

### Prerequisites

- Go 1.25 or higher

### Build

```bash
# Build the server and CLI binaries
make build

# Or manually
go build -o kasoku-server ./cmd/server/main.go
go build -o kvctl ./cmd/kvctl/main.go
```

### Run a Single Node

```bash
# Start server with config file
KASOKU_CONFIG=kasoku.yaml ./kasoku-server

# Or set individual settings via environment variables
KASOKU_CLUSTER_ENABLED=false ./kasoku-server
```

### Run a Three-Node Local Cluster

```bash
# Terminal 1 - Bootstrap node
KASOKU_NODE_ID=node-1 KASOKU_PORT=9000 KASOKU_CLUSTER_ENABLED=true \
  KASOKU_GOSSIP_PORT=9002 KASOKU_PEERS="http://localhost:9001,http://localhost:9002" \
  ./kasoku-server

# Terminal 2
KASOKU_NODE_ID=node-2 KASOKU_PORT=9001 KASOKU_CLUSTER_ENABLED=true \
  KASOKU_GOSSIP_PORT=9003 KASOKU_PEERS="http://localhost:9000,http://localhost:9002" \
  ./kasoku-server

# Terminal 3
KASOKU_NODE_ID=node-3 KASOKU_PORT=9002 KASOKU_CLUSTER_ENABLED=true \
  KASOKU_GOSSIP_PORT=9004 KASOKU_PEERS="http://localhost:9000,http://localhost:9001" \
  ./kasoku-server
```

### Basic Operations via CLI

```bash
./kvctl put user:1001 "Alice"
./kvctl get user:1001
./kvctl delete user:1001
./kvctl scan user:
./kvctl keys
./kvctl stats
```

### Run Tests

```bash
# All unit and integration tests
go test ./...

# With data race detection enabled
go test -race ./...

# Benchmarks (LSM engine)
go test ./internal/store/lsm-engine/... -bench=. -benchmem -run=^$
```

---

## Configuration

Key configuration fields in `kasoku.yaml`:

```yaml
# Server
data_dir: ./data
port: 9000
http_port: 9001

# LSM Engine
lsm:
  levels: 7
  level_ratio: 10.0
  l0_base_size: 67108864  # 64MB

# Memory
memory:
  memtable_size: 67108864       # 64MB
  max_memtable_bytes: 268435456 # 256MB
  bloom_fp_rate: 0.01
  block_cache_size: 134217728   # 128MB

# WAL
wal:
  sync: false
  sync_interval: 100ms
  max_file_size: 67108864       # 64MB

# Cluster
cluster:
  enabled: false
  node_id: node-1
  node_addr: http://localhost:9000
  peers: []
  replication_factor: 3
  quorum_size: 2
  vnodes: 150
  rpc_timeout_ms: 5000
```

See `kasoku.example.yaml` for the full annotated reference.

---

## License and Usage Restrictions

Copyright (c) 2025. All Rights Reserved.

This software and its associated architecture, source code, documentation, and distributed systems design are the exclusive intellectual property of the author.

**This is not open-source software.**

This repository is made publicly visible strictly for portfolio review and technical evaluation by prospective employers and collaborators. You are permitted only to read and review the source code for evaluation purposes.

The following actions are explicitly prohibited without prior written permission from the author:

- Cloning, copying, forking, or re-hosting this repository or any portion of its contents
- Modifying, adapting, or creating derivative works based on this code or architecture
- Using this code, in whole or in part, in any personal, commercial, or academic project
- Redistributing or publishing this code through any channel or medium
- Submitting any portion of this code as your own academic work

Violations of these restrictions may constitute copyright infringement under applicable law.

To request permission for any use not described above, contact the author directly.
