# Kasoku

**Distributed LSM Key-Value Storage Engine**

*Built in Go | Multi-Node Clustering | Production-Ready Architecture*

Kasoku is a distributed, high-performance key-value storage engine designed for scalability and durability. Built entirely in Go, it implements a Log-Structured Merge-Tree (LSM-Tree) architecture with automatic sharding, seamless replication, and fault tolerance across a decentralized cluster of nodes.

---

## Proprietary License & Restrictions

This software and its associated documentation are the exclusive property of the author. **All Rights Reserved.** 

You are strictly **prohibited** from:
- Cloning, copying, or distributing this repository.
- Modifying or publishing any derivative works.
- Using this codebase for any personal, commercial, or academic purpose without explicit written permission.

---

## Architecture Overview

Kasoku separates the write path from the read path to optimize for write-heavy workloads while maintaining fast read speeds.

### Write Path
The engine writes sequentially to a Write-Ahead Log (WAL) to ensure crash-safe durability. Operations are then inserted into an in-memory Skip List (MemTable). Once the MemTable reaches its capacity, it is flushed to disk as an immutable Sorted String Table (SSTable). Background compaction continuously merges SSTables to optimize disk space.

### Read Path
Reads query the active MemTable first, followed by immutable MemTables. If the key is not found in memory, Kasoku sequentially queries SSTables starting from Level 0 downwards. Bloom Filters and an LRU Block Cache are used to minimize disk I/O and maintain low latency.

---

## Core Features

- **LSM-Tree Architecture**: Optimized for write-heavy workloads and sequential disk access.
- **Write-Ahead Log (WAL)**: Ensures crash recovery and strict durability.
- **Bloom Filters**: O(1) negative lookups to skip unnecessary disk reads.
- **Snappy Compression**: Achieves 2-5x storage space reduction.
- **LRU Block Cache**: Maintains hot data in memory for rapid recurring access.
- **Auto-Compaction**: Background SSTable merging and garbage collection.
- **Skip List MemTable**: Fast, concurrent O(log n) inserts.
- **MVCC Versioning**: Tracks key versions accurately.
- **Tombstone Processing**: Handles graceful deletions in an append-only system.

## Distributed Capabilities

- **Horizontal Scaling**: Add nodes to the cluster seamlessly without downtime.
- **Data Replication**: Maintains data redundancy and fault tolerance across distinct nodes.
- **Consistent Hashing**: Automatically shards data evenly across the cluster.
- **Gossip Protocol**: Facilitates peer discovery and robust failure detection.
- **Decentralized Coordination**: Operates without a single master or point of failure.

---

## Quick Start

### Single Node Usage

```bash
# Clone the repository
git clone https://github.com/DevLikhith5/kasoku.git
cd kasoku

# Build the project
make build

# Start using via CLI directly
./kvctl put session:token "session_data"
./kvctl get session:token
./kvctl scan session:

# Or start the standalone HTTP server
./kasoku-server
curl http://localhost:8080/api/v1/get/session:token
```

### Multi-Node Cluster Setup

```bash
# Start Node 1 (Bootstrap Node)
./kasoku-server --node-id node-1 --port 9001 --peers ""

# Start Node 2 (Joins cluster automatically)
./kasoku-server --node-id node-2 --port 9002 --peers "localhost:9001"

# Start Node 3
./kasoku-server --node-id node-3 --port 9003 --peers "localhost:9001"

# Write data to Node 1
curl -X PUT http://localhost:9001/api/v1/put/user:1 -d '{"value":"Alice"}'

# Read identical replicated data from Node 3
curl http://localhost:9003/api/v1/get/user:1
```

---

## Performance Characteristics

Based on continuous integration benchmarks running with a 10ms WAL Sync frequency.

| Operation | Ops/Sec | Latency | Target Use Case |
|-----------|---------|---------|-----------------|
| **Write** | ~200,000 | <5ms | High-throughput metric ingestion |
| **Read (cached)** | ~5,000,000 | <100us | High-frequency hot data access |
| **Read (disk)** | ~10,000 | 1-5ms | Infrequent cold data access |
| **Scan** | ~50,000 | <10ms | Prefix interval queries |

---

## Interfaces

Kasoku provides multiple interaction paradigms depending on the deployment environment:
- **CLI (kvctl)**: Direct database access for scripts and background administration.
- **HTTP REST API**: Language-agnostic endpoints for web applications.
- **Interactive Shell**: A built-in REPL for ad-hoc queries and debugging.

### Standard Commands

- `put [key] [value]`
- `get [key]`
- `delete [key]`
- `scan [prefix]`
- `keys`
- `stats`
- `bench --count [N]`
- `shell`

---

## License

This software is **proprietary and strictly confidential**. It is NOT open source. See the LICENSE file for full restrictions. You may NOT clone, copy, distribute, or claim this software in any capacity.
