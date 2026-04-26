# Kasoku Architecture

A comprehensive technical guide to Kasoku's distributed key-value store architecture.

## Table of Contents

1. [System Overview](#system-overview)
2. [Storage Engine](#storage-engine)
3. [Cluster Layer](#cluster-layer)
4. [Replication](#replication)
5. [Failure Handling](#failure-handling)
6. [Security](#security)
7. [Performance](#performance)

---

## System Overview

Kasoku is a distributed, masterless key-value store implementing the Amazon Dynamo paper. It combines:

- **LSM-Tree** for high-throughput storage
- **Consistent Hashing** for data partitioning
- **Gossip Protocol** for membership
- **Sloppy Quorum** for availability
- **Hinted Handoff** for durability

### Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Masterless** | Any node can accept reads/writes |
| **Eventually Consistent** | W=1, R=1 quorum |
| **Partition Tolerant** | Sloppy quorum + hinted handoff |
| **High Throughput** | Async replication, batch operations |
| **Crash-Safe** | WAL + MemTable + SSTable |

---

## Storage Engine

### Write Path

```
Client Request
      │
      ▼
┌─────────────────┐
│  Write-Ahead Log │  ← Persisted immediately
└─────────────────┘
      │
      ▼
┌─────────────────┐
│    MemTable      │  ← In-memory skip list
└─────────────────┘
      │
      ▼ (when full)
┌─────────────────┐
│   Immutable Q    │  ← Ready to flush
└─────────────────┘
      │
      ▼ (background)
┌─────────────────┐
│   SSTable       │  ← Sorted, compressed on disk
└─────────────────┘
```

### Components

| Component | File | Purpose |
|-----------|------|---------|
| WAL | `internal/store/lsm-engine/wal.go` | Durable write log |
| MemTable | `internal/store/lsm-engine/memtable.go` | In-memory sorted map |
| SSTable | `internal/store/lsm-engine/sstable.go` | On-disk sorted files |
| Compactor | `internal/store/lsm-engine/compactor.go` | Background compaction |

### Key Features

- **Bloom Filters**: Skip SSTables that definitely don't contain the key
- **Block Cache**: LRU cache for frequently accessed blocks
- **Parallel Compaction**: Background goroutines for merging levels

---

## Cluster Layer

### Consistent Hash Ring

```
        ┌──────────────────────────────────────────┐
        │         CONSISTENT HASH RING             │
        │                                          │
        │    0° ┌─────────────────────────┐      │
        │       │                         │      │
        │       │    Virtual Nodes         │      │
        │       │  (150 per physical)     │      │
        │       │                         │      │
        │       └─────────────────────────┘      │
        │               ▲     ▲     ▲            │
        │               │     │     │            │
        │           ┌───┴───┐ ┌───┴───┐ ┌───┴───┐
        │           │ Node1 │ │ Node2 │ │ Node3 │
        │           │       │ │       │ │       │
        │           └───────┘ └───────┘ └───────┘
        │                                          │
        │   Key "user:123" → hash → position      │
        │   First N vnodes = preference list     │
        └──────────────────────────────────────────┘
```

### Code: `internal/ring/ring.go`

```go
// GetNodes returns the top N nodes responsible for a key
func (r *Ring) GetNodes(key string, n int) []string {
    pos := r.hash(key)           // CRC32 hash
    idx := r.search(pos)         // Binary search
    
    // Walk clockwise to find N distinct nodes
    seen := make(map[string]bool)
    for len(result) < n {
        nodeID := r.nodeMap[r.vnodes[idx]]
        if !seen[nodeID] {
            seen[nodeID] = true
            result = append(result, nodeID)
        }
        idx = (idx + 1) % len(r.vnodes)
    }
    return result
}
```

---

## Replication

### Write Flow (W=1, R=1)

```
┌─────────────────────────────────────────────────────────────────────┐
│                    WRITE PATH                                      │
└─────────────────────────────────────────────────────────────────────┘

Client: PUT key="user:1" value="alice"
        │
        ▼
┌─────────────────────────────────────┐
│  1. Determine Preference List      │
│     hash("user:1") → [A, B, C]    │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│  2. Local Write (always succeeds)   │
│     - Write to WAL                  │
│     - Write to MemTable             │
│     - Return success                │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│  3. Async Replicate                │
│     - Send to B: SUCCESS ✓         │
│     - Send to C: FAIL (down!)      │
└─────────────────────────────────────┘
        │
        ▼ (if C is down)
┌─────────────────────────────────────┐
│  4. Create Hint                    │
│     Store locally: {target: C,      │
│                 key: "user:1",      │
│                 value: "alice"}      │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│  5. Return SUCCESS to client       │
│     (data safely stored)           │
└─────────────────────────────────────┘
```

### Sloppy Quorum

When a preferred node is down, the system uses the next available node:

```
Normal: Key "user:1" → [A, B, C] (A is primary)

If B is DOWN:
  → Use fallback: [A, C, D]
  
This is "sloppy" - we don't insist on the original
preference list, we use whatever nodes are available.
```

**Code: `internal/cluster/cluster.go:755-787`**

```go
func (c *Cluster) getReplicasForKey(key string, aliveSet map[string]bool) []string {
    // 1. Get preferred nodes from hash ring
    preferred := c.ring.GetNodes(key, c.replicationFactor)
    
    // 2. Filter to alive only
    var healthy []string
    for _, nodeID := range preferred {
        if aliveSet[nodeID] {
            healthy = append(healthy, nodeID)
        }
    }
    
    // 3. SLOPPY: Fall back to other nodes if needed
    if len(healthy) < c.replicationFactor {
        for _, nodeID := range c.ring.GetAllNodesSorted() {
            if len(healthy) >= c.replicationFactor {
                break
            }
            if aliveSet[nodeID] && !seen[nodeID] {
                healthy = append(healthy, nodeID)
            }
        }
    }
    return healthy
}
```

---

## Failure Handling

### Gossip Protocol

Every 500ms, nodes exchange membership information:

```
T=0:    Node A starts → memberlist = {A: alive}

T=500ms: A ──"I know [A]"──► B
         B ──"I know [B]"──► A
         Merge → {A, B} both know {A, B}

T=1000ms: A ──"I know [A,B]"──► C
         Merge → {A, B, C}

After O(log N) rounds, all nodes converge
```

**Code: `internal/cluster/memberlist.go`**

```go
func (ml *MemberList) Merge(remoteMembers []string) []string {
    for _, nodeID := range remoteMembers {
        if existing, ok := ml.members[nodeID]; ok {
            // Revive if previously dead
            if existing.State != MemberStateAlive {
                existing.State = MemberStateAlive
                existing.Incarnation++
            }
            existing.LastSeen = time.Now()
        } else {
            // New node
            ml.members[nodeID] = &Member{
                NodeID:  nodeID,
                State:   MemberStateAlive,
                LastSeen: time.Now(),
            }
        }
    }
    return ml.Members()
}
```

### Hinted Handoff

When a preferred node is down, the coordinator uses a fallback node and embeds the hint in the write:

```
Scenario: key="user:1" prefers [A, B, C], but B and C are down, D is fallback

         ┌──────────────────────────────────────────────────────────────┐
         │  COORDINATOR (A)                                         │
         │  1. Write locally: user:1 = "alice" ✓                    │
         │  2. Send to D: Put user:1 = "alice" + hint: target=B     │
         │     (includes target node in request)                     │
         └──────────────────────────────────────────────────────────────┘
                              │
                              ▼
         ┌──────────────────────────────────────────────────────────────┐
         │  FALLBACK NODE (D)                                        │
         │  1. Store: user:1 = "alice"                             │
         │  2. Store hint: {target: B, key: user:1} ← LOCAL        │
         │  3. Return success to A ✓                              │
         └──────────────────────────────────────────────────────────────┘
                              │
         ┌──────────────────────────────────────────────────────────────┐
         │  EVERY 10 SECONDS ON NODE D                            │
         │  Scan hints → Try deliver to target                   │
         │  If B is alive → deliver data, delete hint           │
         └──────────────────────────────────────────────────────────────┘
```

**Why hint on fallback node is critical:**

If hint were only stored on coordinator (A):
- A dies → hint lost → data orphaned on D forever
- B never gets the data

With hint on fallback node (D):
- Even if A dies, D knows to deliver to B
- System heals automatically
Normal replication fails
         │
         ▼
┌─────────────────────┐
│  Create Hint          │
│  {target: NodeC,     │
│   key: "user:1",   │
│   value: "alice",   │
│   timestamp: now}    │
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Store in HintStore  │
│  (local disk)       │
└─────────────────────┘
         │
         ▼ (every 10 seconds)
┌─────────────────────┐
│  Background Scan     │
│  Try deliver hints   │
│  If success → delete │
│  If fail → retry    │
└─────────────────────┘
```

**Code: `internal/cluster/replication_queue.go`**

```go
func (br *BackgroundReplicator) deliverHints() {
    for _, hint := range br.hintStore.GetAll() {
        // Try to deliver to target node
        err := br.replicateToPeer(hint.TargetNode, hint.Key, hint.Value)
        if err == nil {
            br.hintStore.Delete(hint.Key, hint.TargetNode)  // Success!
        } else {
            // Keep hint for retry
        }
    }
}
```

### Merkle Anti-Entropy

Every 30 seconds, nodes compare Merkle trees to find divergent keys:

```
1. Build local Merkle tree from all keys
2. Exchange root hash with peer
3. If different → exchange full trees
4. Find different keys → sync only those
```

**Code: `internal/merkle/tree.go`**

```go
// Build constructs a Merkle tree from sorted keys
func Build(keys []string, getValue func(string) []byte) *Node {
    if len(keys) <= 4 {
        return buildLeaf(keys, getValue)
    }
    mid := len(keys) / 2
    left := Build(keys[:mid], getValue)
    right := Build(keys[mid:], getValue)
    return &Node{
        Hash: sha256.Sum256(append(left.Hash[:], right.Hash[:]...)),
        Left:  left,
        Right: right,
    }
}
```

---

## Security

### TLS Encryption

Both HTTP and gRPC support TLS:

```yaml
# configs/single.yaml
tls:
    enabled: true
    cert_file: ./certs/server-cert.pem
    key_file: ./certs/server-key.pem
```

### API Key Authentication

```yaml
auth:
    enabled: true
    api_key: your-secret-api-key
```

Usage:
```bash
curl -H "X-API-Key: your-secret-api-key" http://localhost:9001/api/v1/get/user:1
```

### Rate Limiting

```yaml
rate_limit:
    enabled: true
    requests_per_second: 1000
    burst: 100
```

---

## Performance

### Benchmark Results (3-Node Cluster)

| Protocol | Writes | Reads | Total |
|----------|--------|-------|-------|
| HTTP | 146K | 117K | 263K |
| **gRPC** | 759K | 1.26M | **1.0M** |

**gRPC is 3.8x faster than HTTP**

### Configuration

```yaml
# Optimal settings for high throughput
memory:
    memtable_size: 268435456      # 256MB
    block_cache_size: 536870912    # 512MB
    bloom_fp_rate: 0.01

compaction:
    max_concurrent: 4

wal:
    sync: false
    sync_interval: 500ms

cluster:
    replication_factor: 3
    quorum_size: 1          # W=1 for speed
    read_quorum: 1          # R=1 for speed
```

---

## File Structure

```
kasoku/
├── cmd/
│   └── server/           # Main server entry
├── internal/
│   ├── cluster/          # Replication, gossip, membership
│   ├── config/           # Configuration loading
│   ├── merkle/           # Merkle tree implementation
│   ├── ring/             # Consistent hashing
│   ├── rpc/              # HTTP + gRPC clients
│   │   └── grpc/         # gRPC server & client pool
│   └── store/
│       └── lsm-engine/   # LSM tree storage
├── api/                   # Protocol buffer definitions
├── configs/              # Example configurations
├── deploy/               # Docker, K8s configs
└── tests/               # Integration tests
```

---

## API Endpoints

### HTTP

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/get/{key}` | Get a value |
| PUT | `/api/v1/put/{key}` | Set a value |
| DELETE | `/api/v1/delete/{key}` | Delete a key |
| POST | `/api/v1/batch` | Batch write |
| POST | `/api/v1/batch/get` | Batch read |
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus metrics |

### gRPC

| Method | Description |
|--------|-------------|
| `Put` | Single key write |
| `Get` | Single key read |
| `BatchPut` | Batch write |
| `MultiGet` | Batch read |
| `Delete` | Delete key |
| `Scan` | Range scan |

---

## Summary

Kasoku implements all major features from the Dynamo paper:

- ✅ Consistent Hashing
- ✅ Sloppy Quorum (W=1, R=1)
- ✅ Gossip Protocol
- ✅ Hinted Handoff
- ✅ Merkle Anti-Entropy
- ✅ Vector Clocks
- ✅ Read Repair
- ✅ Masterless Architecture

This is a production-grade distributed database suitable for high-throughput workloads.
