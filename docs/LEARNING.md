# Kasoku Codebase Learning Guide

## Start Here: Entry Points

### 1. Server Entry Point
**File:** `cmd/server/main.go`
- Start here to understand how everything boots up
- Creates LSM engine, initializes caches, starts HTTP server

---

## Layer 1: Storage (LSM-Tree)

Read in order:

| Order | File | What You'll Learn |
|-------|------|----------------|
| 1 | `internal/store/engine.go` | Interface contract |
| 2 | `internal/store/lsm-engine/memtable.go` | In-memory sorted data |
| 3 | `internal/store/lsm-engine/skiplist.go` | O(log n) ordered skiplist |
| 4 | `internal/store/lsm-engine/sstable.go` | On-disk sorted files |
| 5 | `internal/store/wal.go` | Write-Ahead Log (durability) |
| 6 | `internal/store/lsm-engine/lsm.go` | The main engine |

### Key Patterns (Storage)

```go
// 1. sync.Pool for memory efficiency
var entryPool = sync.Pool{ New: func() interface{} { return &Entry{} } }

// 2. Atomic for lock-free
version atomic.Uint64
closed atomic.Bool

// 3. RWMutex for concurrent reads
mu sync.RWMutex
e.mu.RLock()
defer e.mu.RUnlock()

// 4. Channel signaling
flushCh := make(chan struct{}, 1)
```

---

## Layer 2: Distributed (Dynamo-Style)

Read in order:

| Order | File | What You'll Learn |
|-------|------|----------------|
| 1 | `internal/cluster/cluster.go` | Main cluster coordinator |
| 2 | `internal/cluster/ring.go` | Consistent hashing |
| 3 | `internal/cluster/replication.go` | W=1, R=1 fast path |
| 4 | `internal/cluster/vclock.go` | Vector clocks |
| 5 | `internal/cluster/hintstore.go` | Hinted handoff |
| 6 | `internal/cluster/failure_detector.go` | Phi accrual detection |

### Key Patterns (Cluster)

```go
// 1. Local-first writes (Dynamo optimization)
if n.cfg.W == 1 {
    return n.engine.PutWithVectorClock(key, value, vc)  // Fast path!
}

// 2. Early exit on quorum
if acks >= n.cfg.W {
    return nil  // Don't wait for all
}

// 3. Gossip for membership
go n.gossipLoop(ctx)

// 4. Vector clock increment
vc = vc.Increment(n.cfg.NodeID)
```

---

## Layer 3: Network

| File | What You'll Learn |
|------|----------------|
| `internal/rpc/client.go` | HTTP client, connection pooling |
| `internal/rpc/batch.go` | Batch operations |
| `internal/rpc/encoding.go` | Gob encoding (binary) |

---

## Learning Exercises

### Exercise 1: Trace a Write
```bash
# Start at HTTP handler → storage → WAL → memtable
# Files:
cmd/server/handler/handlers.go      # HTTP handler
internal/store/lsm-engine/lsm.go    # PutEntry()
internal/store/wal.go           # WAL.Append()
internal/store/lsm-engine/memtable.go # MemTable.Put()
```

### Exercise 2: Trace a Read
```bash
# memtable → immutable → SSTables → bloom filter → block cache
# Files:
internal/store/lsm-engine/lsm.go    # Get()
internal/store/lsm-engine/sstable.go   # SSTableReader.Get()
internal/store/lsm-engine/bloom.go  # MightContain()
```

### Exercise 3: Trace Cluster Write
```bash
# HTTP → Cluster.ReplicatedPut → local → async replicate
internal/cluster/cluster.go         # ReplicatedPut()
internal/cluster/replication.go    # ReplicatedPut()
internal/rpc/client.go          # RemoteRPC
```

---

## Key Files Summary

```
Must Read:
├── cmd/server/main.go         # Entry point (START HERE)
├── internal/store/engine.go         # Interface
├── internal/store/lsm-engine/lsm.go # Main engine
├── internal/cluster/cluster.go      # Cluster
├── internal/cluster/ring.go     # Hash ring
└── docs/PAPER.md             # Design (why it works)
```

---

## Go Patterns Reference

### 1. Interface
```go
type StorageEngine interface {
    Get(key string) (Entry, error)
    Put(key string, value []byte) error
    Delete(key string) error
}
```

### 2. Embedding
```go
type Node struct {
    cfg Config
    engine StorageEngine  // interface!
}
```

### 3. Options pattern
```go
func NewEngine(dir string, opts ...Option) *Engine {
    e := &Engine{}
    for _, opt := range opts {
        opt(e)
    }
    return e
}
```

### 4. sync.Pool
```go
var pool = sync.Pool{New: func() interface{} { return new(Entry) }}
func Get() *Entry { return pool.Get().(*Entry) }
func Put(e *Entry) { pool.Put(e) }
```

### 5. Atomic
```go
type Engine struct {
    version atomic.Uint64
    closed atomic.Bool
}
```

### 6. RWMutex
```go
func (e *Engine) Get() {
    e.mu.RLock()
    defer e.mu.RUnlock()
}
```

---

## Code Walkthrough: Trace a Write Request

```
HTTP GET /api/v1/get/key
    │
    ▼
cmd/server/handler/handlers.go::handleGet()
    │
    ▼
internal/store/lsm-engine/lsm.go::Get()
    │
    ├─► 1. Check active MemTable (RLock)
    │       └─► SkipList.Get() - O(log n)
    │
    ├─► 2. Check immutable[]
    │       └─► Each SkipList.Get()
    │
    ├─► 3. Check levels[] (newest → oldest)
    │       ├─► BloomFilter.MightContain()
    │       └─► SSTableReader.Get()
    │               └─► BlockCache → disk
    │
    ▼
Return Entry (or ErrKeyNotFound)
```

## Code Walkthrough: Trace a Write Request

```
HTTP Request PUT /api/v1/put/key
    │
    ▼
cmd/server/handler/handlers.go::handlePut()
    │  - Parse key/value from URL
    │
    ▼
internal/cluster/cluster.go::ReplicatedPut()
    │  - W=1: write local only (fast path!)
    │
    ▼
internal/store/lsm-engine/lsm.go::PutEntry()
    │  - Write to WAL first (durability)
    │  - Write to MemTable (memory)
    │
    ▼
internal/store/lsm-engine/memtable.go::Put()
    │  - Insert into SkipList (O(log n))
    │
    ▼
Done! (async flush to SSTable later)
```

## Quick Study: Start at cmd/server/main.go

```go
// main.go - WHERE TO START!

func main() {
    // 1. Load config from YAML
    cfg, err := config.Load(cfgPath)
    
    // 2. Create LSM engine
    engine, err := lsmengine.NewLSMEngineWithConfig(...)
    
    // 3. Initialize block cache
    lsmengine.InitBlockCache(cfg.Memory.BlockCacheSize)
    
    // 4. Start HTTP server
    http.ListenAndServe(...)
}
```

Then trace through handlers → cluster → replication!

---

## Files You MUST Read

| Priority | File | Why |
|----------|------|-----|
| **START** | `cmd/server/main.go` | Entry point - understand boot |
| **START** | `internal/store/engine.go` | Interface contract |
| 2 | `internal/store/lsm-engine/lsm.go` | Main engine |
| 3 | `internal/cluster/cluster.go` | Cluster coordinator |
| 4 | `internal/cluster/ring.go` | Consistent hashing |
| 5 | `docs/PAPER.md` | Design decisions |
| 6 | `docs/README.md` | Benchmarks |

---

## Time Estimate

| Topic | Time | Lines |
|-------|------|-------|
| Storage basics | 2 hrs | 2000 |
| Cluster | 3 hrs | 1500 |
| Network | 1 hr | 500 |
| Tests | 2 hrs | - |
| **Total** | **8 hrs** | 4000 |