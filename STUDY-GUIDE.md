# Kasoku - Complete Study Guide

> **A High-Performance Key-Value Storage Engine**  
> Built with Go, implementing LSM-Tree architecture with production-grade features

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture at a Glance](#2-architecture-at-a-glance)
3. [Directory Structure](#3-directory-structure)
4. [Core Concepts](#4-core-concepts)
5. [Deep Dive: Storage Engines](#5-deep-dive-storage-engines)
6. [Deep Dive: LSM Engine Internals](#6-deep-dive-lsm-engine-internals)
7. [Data Flow Walkthrough](#7-data-flow-walkthrough)
8. [CLI & Usage](#8-cli--usage)
9. [Key Design Decisions](#9-key-design-decisions)
10. [Study Path & Learning Checklist](#10-study-path--learning-checklist)

---

## 1. Project Overview

### What is Kasoku?

Kasoku is a **persistent key-value storage engine** inspired by databases like LevelDB and RocksDB. It provides two storage backends:

| Engine             | Type                          | Best For                       |
|--------------------|-------------------------------|--------------------------------|
| **LSM Engine**     | Disk-based with memory buffer | Production, large datasets     |
| **HashMap Engine** | In-memory with WAL            | Testing, caching, development  |

### Key Features

- ✅ **Durability** - Write-Ahead Log (WAL) ensures crash recovery
- ✅ **High Performance** - O(1) amortized writes, O(log N) reads
- ✅ **Sorted Keys** - Lexicographic ordering enables range queries
- ✅ **Bloom Filters** - Fast negative lookups (skip disk reads)
- ✅ **Compaction** - Background merging reduces disk usage
- ✅ **Thread-Safe** - Concurrent reads and writes supported
- ✅ **MVCC-Ready** - Version tracking for multi-version concurrency
- ✅ **Tombstones** - Proper delete handling with automatic cleanup

### Technology Stack

- **Language**: Go 1.24+
- **Testing**: testify
- **Storage**: File-based (SSTables + WAL)
- **Data Structures**: Skip Lists, Bloom Filters, B-Trees (via sorted files)

---

## 2. Architecture at a Glance

### High-Level Architecture

```text
┌─────────────────────────────────────────────────────────┐
│                    Application Layer                     │
│                    (CLI kvctl / Server)                  │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                   StorageEngine Interface                │
│  Get() | Put() | Delete() | Scan() | Keys() | Stats()   │
└─────────────────────────────────────────────────────────┘
                          │
            ┌─────────────┴─────────────┐
            │                           │
            ▼                           ▼
┌───────────────────────┐   ┌───────────────────────┐
│     LSMEngine         │   │   HashMapEngine       │
│  (Production Ready)   │   │  (In-Memory + WAL)    │
│                       │   │                       │
│  ┌─────────────────┐  │   │  ┌─────────────────┐  │
│  │  Active MemTable│  │   │  │   Go HashMap    │  │
│  │  Immutable MT   │  │   │  │   + WAL         │  │
│  │  SSTables (L0)  │  │   │  └─────────────────┘  │
│  │  SSTables (L1)  │  │   └───────────────────────┘
│  │  SSTables (L2+) │  │
│  │  WAL            │  │
│  └─────────────────┘  │
└───────────────────────┘
```

### Read Path (LSM Engine)

```text
GET("user:1")
    │
    ├─► Check Active MemTable (fastest - in memory)
    │       └─► Found? Return immediately
    │
    ├─► Check Immutable MemTable (being flushed)
    │       └─► Found? Return immediately
    │
    └─► Check SSTables (L0 → L1 → L2 → ...)
            ├─► Bloom Filter check (skip if definitely not present)
            └─► Binary search index → Read data block from disk
```

### Write Path (LSM Engine)

```text
PUT("user:1", "Alice")
    │
    ├─► Append to WAL (durability - fsync to disk)
    │
    ├─► Insert to Active MemTable (in-memory skip list)
    │
    └─► If MemTable full:
            ├─► Mark as Immutable
            ├─► Create new Active MemTable
            └─► Flush Immutable to disk as SSTable (L0)
                    └─► Trigger compaction if L0 has ≥4 SSTables
```

---

## 3. Directory Structure

```text
kasoku/
│
├── cmd/                          # Command-line applications
│   ├── kvctl/                    # CLI client (main.go)
│   │   └── Commands: put, get, delete, scan, keys, stats, etc.
│   └── server/                   # TCP server (main.go)
│       └── Starts LSM engine on port 9000
│
├── internal/                     # Private packages
│   ├── store/                    # Core storage engine code
│   │   ├── engine.go             # StorageEngine interface + Entry type
│   │   ├── hashmap.go            # HashMapEngine (in-memory)
│   │   ├── hashmap_test.go       # HashMap tests
│   │   ├── wal.go                # Write-Ahead Log implementation
│   │   ├── wal_test.go           # WAL tests
│   │   └── lsm-engine/           # LSM Tree implementation
│   │       ├── lsm.go            # Main LSMEngine (orchestrator)
│   │       ├── memtable.go       # MemTable (wrapper around SkipList)
│   │       ├── skiplist.go       # SkipList data structure
│   │       ├── sstable.go        # SSTable (disk format: data + index + bloom)
│   │       ├── bloom.go          # BloomFilter (probabilistic data structure)
│   │       ├── bloom_test.go     # Bloom filter tests
│   │       └── memtable_test.go  # MemTable tests
│   │
│   ├── cluster/                  # (Empty - future clustering support)
│   ├── rpc/                      # (Empty - future RPC layer)
│   ├── ring/                     # (Empty - future consistent hashing)
│   ├── metrics/                  # (Empty - future monitoring)
│   └── ttl/                      # (Empty - future TTL support)
│
├── client/                       # (Empty - future Go client library)
├── proto/                        # (Empty - future protobuf definitions)
│
├── data/                         # Default data directory
│   ├── wal.log                   # Write-Ahead Log
│   ├── L0_*.sst                  # Level 0 SSTables (freshly flushed)
│   ├── L1_*.sst                  # Level 1 SSTables (compacted)
│   └── L2_*.sst                  # Level 2 SSTables (further compacted)
│
├── go.mod                        # Go module definition
├── go.sum                        # Dependency checksums
├── Makefile                      # Build automation
├── docker-compose.yml            # Docker setup
├── USAGE.md                      # User documentation
└── kvctl                         # Compiled binary (after build)
```

---

## 4. Core Concepts

### 4.1 Key-Value Model

Kasoku stores data as **key-value pairs**:

```go
Key:   "user:1"        (max 1KB)
Value: []byte("Alice") (max 1MB)
```

### 4.2 Entry Structure

Every stored item is wrapped in an `Entry`:

```go
type Entry struct {
    Key       string    // The key
    Value     []byte    // The value
    Version   uint64    // Auto-incrementing version (MVCC support)
    TimeStamp time.Time // When it was written
    Tombstone bool      // True if this is a deletion marker
}
```

### 4.3 Write-Ahead Log (WAL)

**Purpose**: Ensure durability. If the system crashes, data can be recovered from the WAL.

**How it works**:

1. Every write is **first** appended to WAL
2. WAL is **fsync'd** to disk (physical write)
3. Only then is data added to MemTable

```json
{
  "op": "PUT",
  "key": "user:1",
  "value": "Alice",
  "ver": 42,
  "ts": 1234567890
}
```

### 4.4 SSTable (Sorted String Table)

**Purpose**: Immutable, sorted disk files for efficient reads.

**File Format**:

```text
┌─────────────────────────────┐
│  Data Block 1               │  (JSON-encoded Entry)
│  Data Block 2               │
│  Data Block 3               │
│  ...                        │
├─────────────────────────────┤
│  Index (JSON array)         │  [(key, offset, size), ...]
├─────────────────────────────┤
│  Bloom Filter               │  (Bit array)
├─────────────────────────────┤
│  Footer (32 bytes)          │  [indexOffset, indexSize, bloomOffset, bloomSize]
└─────────────────────────────┘
```

**Why SSTables?**

- Sorted → enables binary search (O(log N) reads)
- Immutable → no random writes, easy to share across threads
- Mergable → multiple SSTables can be compacted into one

### 4.5 Bloom Filter

**Purpose**: Quickly tell if a key is **definitely NOT** in an SSTable (avoid disk reads).

**How it works**:

1. When writing: hash each key and set bits in a bit array
2. When reading: hash the key and check if all bits are set
   - If **any bit is 0** → key is definitely NOT present (skip disk read!)
   - If **all bits are 1** → key is probably present (read from disk)

**False Positive Rate**: ~1% (configurable)

### 4.6 MemTable

**Purpose**: In-memory buffer for recent writes.

**Implementation**: Skip List (sorted, probabilistic data structure)

**Lifecycle**:

1. Writes go to **Active MemTable**
2. When full (64MB default) → becomes **Immutable MemTable**
3. New writes go to new Active MemTable
4. Immutable is flushed to disk as SSTable

### 4.7 Compaction

**Purpose**: Merge multiple SSTables to reduce disk usage and improve read performance.

**When triggered**:

- L0 has ≥4 SSTables
- Manual trigger via `kvctl compact`

**Process**:

1. Read all entries from SSTables at a level
2. Merge sort by key
3. Keep only the **latest version** of each key
4. Remove old tombstones (>24 hours old)
5. Write merged result to next level (L0→L1→L2)

---

## 5. Deep Dive: Storage Engines

### 5.1 HashMap Engine (In-Memory)

**File**: `internal/store/hashmap.go`

**Best for**: Testing, development, caching

```go
type HashMapEngine struct {
    mu      sync.RWMutex
    data    map[string]Entry  // In-memory storage
    version atomic.Uint64
    closed  atomic.Bool
    wal     *WAL              // Optional durability
}
```

**Operations**:

| Operation | Complexity | Notes                              |
|-----------|------------|------------------------------------|
| Put       | O(1)       | Hash map insert + WAL append       |
| Get       | O(1)       | Hash map lookup                    |
| Delete    | O(1)       | Mark as tombstone + WAL append     |
| Scan      | O(N)       | Iterate all keys, filter by prefix |
| Keys      | O(N)       | Iterate all keys                   |

**Pros**:

- Extremely fast (all in-memory)
- Simple implementation

**Cons**:

- Limited by RAM
- Full WAL replay on restart (slow for large datasets)

---

### 5.2 LSM Engine (Production)

**File**: `internal/store/lsm-engine/lsm.go`

**Best for**: Production workloads, large datasets

```go
type LSMEngine struct {
    mu        sync.RWMutex
    active    *MemTable       // Current write buffer
    immutable *MemTable       // Being flushed to disk
    wal       *WAL            // Durability
    levels    [][]*SSTableReader  // L0, L1, L2, ...
    version   atomic.Uint64
    flushCh   chan struct{}   // Triggers flush
    compCh    chan struct{}   // Triggers compaction
}
```

**Operations**:

| Operation | Complexity | Notes                                          |
|-----------|------------|------------------------------------------------|
| Put       | O(1)*      | *Amortized (flush/compact in background)       |
| Get       | O(log N)   | Check MemTables + SSTables (with bloom filter) |
| Delete    | O(1)*      | Write tombstone (same as Put)                  |
| Scan      | O(N + M)   | M = matching keys                              |
| Keys      | O(N)       | Scan all entries                               |

**Pros**:

- Handles datasets larger than RAM
- High write throughput (sequential writes)
- Sorted keys enable range queries

**Cons**:

- More complex implementation
- Read amplification (may check multiple SSTables)
- Write amplification (compaction rewrites data)

---

## 6. Deep Dive: LSM Engine Internals

### 6.1 Skip List (MemTable Implementation)

**File**: `internal/store/lsm-engine/skiplist.go`

**What is it?** A probabilistic alternative to balanced trees. Provides O(log N) search/insert/delete.

**Structure**:

```text
┌─────────────────────────────────────────┐
Level 3:  head ──────► node5 ──────► node10
Level 2:  head ──► node3 ──► node5 ──► node10
Level 1:  head ─► node2 ─► node3 ──► node5 ──► node10
└─────────────────────────────────────────┘
```

**Key Insight**: Each node has multiple "forward" pointers. Higher levels skip more nodes.

**Random Level Generation**:

```go
func (s *SkipList) randomLevel() int {
    lvl := 1
    for s.rng.Float64() < s.p && lvl < s.maxLevel {
        lvl++
    }
    return lvl
}
```

- Probability `p = 0.5` → 50% chance to go to next level
- Max level = 16 → supports up to 2^16 nodes efficiently

---

### 6.2 MemTable

**File**: `internal/store/lsm-engine/memtable.go`

**Purpose**: Wrap SkipList with size tracking.

```go
type MemTable struct {
    mu        sync.RWMutex
    list      *SkipList
    sizeBytes int64      // Current memory usage
    maxBytes  int64      // 64MB default
}
```

**Key Methods**:

- `Put(entry)` - Insert/update entry, track size
- `Get(key)` - Lookup in SkipList
- `IsFull()` - Check if sizeBytes >= maxBytes
- `Entries()` - Export all entries (for flushing)

---

### 6.3 SSTable Reader/Writer

**File**: `internal/store/lsm-engine/sstable.go`

**Writer**:

```go
writer, _ := NewSSTableWriter(path, expectedEntries)
writer.WriteEntry(entry)  // Append data, update index, add to bloom
writer.Finalize()         // Write index, bloom filter, footer
```

**Reader**:

```go
reader, _ := OpenSSTable(path)
entry, err := reader.Get(key)   // Bloom check → binary search → disk read
entries, _ := reader.Scan(prefix)  // Range scan
```

**Lazy Loading**: Only index and bloom filter are loaded into memory. Data blocks are read on-demand.

---

### 6.4 Bloom Filter

**File**: `internal/store/lsm-engine/bloom.go`

**Math**:

- `m` = optimal bits = `-n * ln(p) / (ln(2)^2)`
- `k` = optimal hashes = `m/n * ln(2)`

For n=1000, p=0.01:

- m ≈ 9585 bits (~1.2KB)
- k ≈ 7 hash functions

**Double Hashing Trick**:
Instead of k independent hashes, use:

```text
hash_i(key) = h1(key) + i * h2(key)
```

Only need 2 hash functions (h1, h2) to generate k hashes!

---

### 6.5 Flush Loop

**File**: `internal/store/lsm-engine/lsm.go`

```go
func (e *LSMEngine) flushLoop() {
    for range e.flushCh {
        e.flushMemTable()
    }
}

func (e *LSMEngine) flushMemTable() error {
    // 1. Swap active → immutable
    e.immutable = e.active
    e.active = NewMemTable(DefaultMemTableSize)

    // 2. Export entries from immutable
    entries := e.immutable.Entries()

    // 3. Write SSTable (L0)
    sstPath := filepath.Join(e.dir, fmt.Sprintf("L0_%d.sst", time.Now().UnixNano()))
    writer, _ := NewSSTableWriter(sstPath, len(entries))
    for _, entry := range entries {
        writer.WriteEntry(entry)
    }
    writer.Finalize()

    // 4. Reset WAL (data now on disk)
    e.wal.Reset()

    // 5. Register SSTable
    reader, _ := OpenSSTable(sstPath)
    e.levels[0] = append([]*SSTableReader{reader}, e.levels[0]...)

    // 6. Trigger compaction if needed
    if len(e.levels[0]) >= 4 {
        e.compCh <- struct{}{}
    }
}
```

---

### 6.6 Compaction

**File**: `internal/store/lsm-engine/lsm.go`

```go
func (e *LSMEngine) compactLevel(level int) {
    // 1. Copy SSTables to merge
    toMerge := e.levels[level]
    e.levels[level] = nil  // Clear level

    // 2. Merge sort all entries
    merged := mergeSSTables(toMerge)

    // 3. Deduplicate (keep latest version, remove old tombstones)
    deduped := deduplicateEntries(merged, 24*time.Hour)

    // 4. Write to next level
    nextLevel := level + 1
    sstPath := filepath.Join(e.dir, fmt.Sprintf("L%d_%d.sst", nextLevel, time.Now().UnixNano()))
    writer, _ := NewSSTableWriter(sstPath, len(deduped))
    for _, entry := range deduped {
        writer.WriteEntry(entry)
    }
    writer.Finalize()

    // 5. Register new SSTable
    reader, _ := OpenSSTable(sstPath)
    e.levels[nextLevel] = append(e.levels[nextLevel], reader)

    // 6. Delete old files
    for _, sst := range toMerge {
        sst.Close()
        os.Remove(sst.path)
    }
}
```

---

## 7. Data Flow Walkthrough

### Scenario: Store and Retrieve "user:1" → "Alice"

#### Step 1: PUT Operation

```bash
./kvctl put user:1 "Alice"
```

**What happens internally**:

```text
1. Parse command → key="user:1", value="Alice"

2. Create Entry:
   Entry{
       Key: "user:1",
       Value: []byte("Alice"),
       Version: 1,
       TimeStamp: 2026-03-27 10:00:00,
       Tombstone: false
   }

3. Append to WAL:
   - Serialize to JSON: {"op":"PUT","key":"user:1","value":"Alice","ver":1,"ts":1234567890}
   - Write to wal.log
   - fsync() → physical disk

4. Insert to MemTable:
   - SkipList.Put(entry)
   - sizeBytes += len("user:1") + len("Alice")

5. Check if MemTable full:
   - If yes → send to flushCh
```

#### Step 2: GET Operation

```bash
./kvctl get user:1
```

**What happens internally**:

```text
1. Parse command → key="user:1"

2. Check Active MemTable:
   - SkipList.Get("user:1")
   - Found! Return Entry

3. Check Tombstone:
   - entry.Tombstone == false → OK

4. Format output:
   - JSON/Table/Text based on -o flag

5. Print result:
   Key: user:1
   Value: Alice
   Version: 1
   Timestamp: 2026-03-27 10:00:00
```

#### Step 3: DELETE Operation

```bash
./kvctl delete user:1
```

**What happens internally**:

```text
1. Parse command → key="user:1"

2. Create Tombstone Entry:
   Entry{
       Key: "user:1",
       Value: nil,
       Version: 2,
       TimeStamp: 2026-03-27 10:05:00,
       Tombstone: true
   }

3. Append to WAL (same as PUT)

4. Insert to MemTable (same as PUT)

5. On GET after delete:
   - Find tombstone in MemTable
   - Return ErrKeyNotFound
```

#### Step 4: Flush to Disk (Automatic)

```text
When MemTable reaches 64MB:

1. Flush loop wakes up (receives from flushCh)

2. Swap MemTables:
   - immutable = active
   - active = new MemTable

3. Flush immutable to SSTable:
   - Export all entries (sorted by SkipList)
   - Write data blocks to L0_1234567890.sst
   - Write index (JSON array)
   - Write bloom filter
   - Write footer

4. Reset WAL:
   - Truncate wal.log
   - Seek to beginning

5. Trigger compaction if L0 has ≥4 SSTables
```

#### Step 5: Compaction (Background)

```text
When L0 has 4+ SSTables:

1. Compact loop wakes up (receives from compCh)

2. Select SSTables to compact (all of L0)

3. Merge sort:
   - Read all entries from all SSTables
   - Sort by key, then by version (descending)

4. Deduplicate:
   - Keep only latest version of each key
   - Remove tombstones older than 24 hours

5. Write to L1:
   - Create new SSTable at L1_1234567890.sst
   - Write deduplicated entries

6. Delete old L0 files

7. Update level pointers
```

---

## 8. CLI & Usage

### Available Commands

| Command   | Description             | Example                    |
|-----------|-------------------------|----------------------------|
| `put`     | Store key-value         | `kvctl put user:1 "Alice"` |
| `get`     | Retrieve value          | `kvctl get user:1`         |
| `delete`  | Remove key              | `kvctl delete user:1`      |
| `scan`    | Prefix scan             | `kvctl scan user:`         |
| `keys`    | List all keys           | `kvctl keys`               |
| `stats`   | Show statistics         | `kvctl stats`              |
| `dump`    | Dump all data           | `kvctl dump`               |
| `compact` | Manual compaction       | `kvctl compact`            |
| `import`  | Import JSON             | `kvctl import backup.json` |
| `export`  | Export JSON             | `kvctl export backup.json` |
| `shell`   | Interactive mode        | `kvctl shell`              |
| `bench`   | Run benchmarks          | `kvctl bench`              |

### Global Flags

```bash
-d, --dir <path>     # Data directory (default: ./data)
-o, --output <fmt>   # Output format: text, json, table
--verbose            # Enable verbose output
-h, --help           # Show help
-v, --version        # Show version
```

### Examples

```bash
# Basic operations
./kvctl put user:1 "Alice"
./kvctl get user:1
./kvctl delete user:1

# JSON output
./kvctl -o json get user:1

# Table output
./kvctl -o table scan user:

# Custom data directory
./kvctl -d /var/lib/kasoku stats

# Export/Import
./kvctl export backup.json
./kvctl import backup.json

# Interactive shell
./kvctl shell
> put key1 value1
> get key1
> scan key
> exit
```

---

## 9. Key Design Decisions

### 9.1 Why Skip List for MemTable?

**Alternatives considered**:

- Red-Black Tree: Complex balancing logic
- B-Tree: Overkill for in-memory
- Hash Map: Doesn't maintain order

**Why Skip List?**

- ✅ Maintains sorted order (needed for SSTable flushing)
- ✅ O(log N) operations
- ✅ Simpler than balanced trees
- ✅ Probabilistic balancing (no complex rotations)

---

### 9.2 Why JSON for SSTable Format?

**Alternatives considered**:

- Protocol Buffers: Requires schema, code generation
- Binary format: Harder to debug
- Gob: Go-specific, less portable

**Why JSON?**

- ✅ Human-readable (easy debugging)
- ✅ No schema needed
- ✅ Standard library support
- ✅ Portable across languages

**Trade-off**: Slightly larger on disk, slower parsing

---

### 9.3 Why 64MB MemTable Size?

**Trade-offs**:

- larger MemTable → fewer SSTables, less compaction
- But: longer recovery time (more WAL to replay)
- And: more memory usage

**64MB is a reasonable default**:

- Fits in most systems' RAM
- Flushes in ~100ms on SSD
- Balances write throughput vs. memory

---

### 9.4 Why 4 SSTables Trigger Compaction?

**Trade-offs**:

- Fewer SSTables → faster reads (fewer files to check)
- But: more frequent compaction (write amplification)

**4 is a sweet spot**:

- Enough SSTables to batch compaction
- Not so many that reads become slow

---

### 9.5 Why 24-Hour Tombstone TTL?

**Purpose**: Prevent deleted keys from reappearing.

**Scenario without TTL**:

1. Delete key "user:1" (tombstone written)
2. Compaction removes tombstone (to save space)
3. Old SSTable (before delete) still has "user:1"
4. Next compaction merges → "user:1" reappears! 😱

**Solution**: Keep tombstones for 24 hours:

- Ensures all SSTables with the key are compacted
- Then safe to remove tombstone

---

### 9.6 Why Double Hashing for Bloom Filter?

**Naive approach**: k independent hash functions

- Expensive: k hash computations per key

**Double hashing**: `hash_i(key) = h1(key) + i * h2(key)`

- Only 2 hash computations
- Mathematically proven to work as well as k independent hashes

---

## 10. Study Path & Learning Checklist

### Phase 1: Foundations (Week 1)

- [ ] Read `internal/store/engine.go` - Understand the interface
- [ ] Read `internal/store/wal.go` - Understand Write-Ahead Logging
- [ ] Read `internal/store/hashmap.go` - Simple in-memory engine
- [ ] Run tests: `go test ./internal/store/...`
- [ ] **Milestone**: Implement your own simple KV store with WAL

### Phase 2: Data Structures (Week 2)

- [ ] Read `internal/store/lsm-engine/skiplist.go` - Skip List
- [ ] Read `internal/store/lsm-engine/bloom.go` - Bloom Filter
- [ ] Draw diagrams of how they work
- [ ] **Milestone**: Implement Skip List from scratch

### Phase 3: LSM Engine Core (Week 3)

- [ ] Read `internal/store/lsm-engine/memtable.go` - MemTable wrapper
- [ ] Read `internal/store/lsm-engine/sstable.go` - SSTable format
- [ ] Read `internal/store/lsm-engine/lsm.go` - Main orchestrator
- [ ] Trace through PUT/GET/DELETE flows
- [ ] **Milestone**: Build minimal LSM engine (no compaction)

- [ ] Read `internal/store/lsm-engine/lsm_test.go` - Main engine tests (Integration)

### Phase 4: Advanced Topics (Week 4)

- [ ] Understand flush loop and compaction loop
- [ ] Read merge/deduplicate logic
- [ ] Understand tombstone handling
- [ ] **Milestone**: Add compaction to your LSM engine

### Phase 5: CLI & Operations (Week 5)

- [ ] Read `cmd/kvctl/main.go` - CLI implementation
- [ ] Try all commands manually
- [ ] Run benchmarks: `./kvctl bench`
- [ ] **Milestone**: Add a new CLI command (e.g., `ttl`)

### Phase 6: Deep Understanding (Week 6)

- [ ] Answer these questions:
  - [ ] Why does GET check MemTable before SSTables?
  - [ ] How does bloom filter reduce disk reads?
  - [ ] What happens during compaction?
  - [ ] Why do we need immutable MemTable?
  - [ ] How does WAL enable crash recovery?

- [ ] **Milestone**: Write a blog post explaining LSM trees

---

## Quick Reference

### Performance Characteristics

| Operation      | LSM Engine                     | HashMap Engine |
|----------------|--------------------------------|----------------|
| Point Read     | O(log N)                       | O(1)           |
| Write          | O(1)*                          | O(1)           |
| Scan           | O(N + M)                       | O(N)           |
| Delete         | O(1)*                          | O(1)           |
| Memory Usage   | Buffer only                    | All data       |
| Max Size       | Disk capacity                  | RAM capacity   |

*\*Amortized (background flush/compact)*

### Performance Improvements (v1.0+)

| Feature | Benefit | Implementation |
|---------|---------|----------------|
| Block Compression (Snappy) | 2-5x storage reduction | `sstable.go` |
| Block Cache (LRU) | ~100x faster hot reads | `sstable.go` |
| WAL Compaction | Prevents unbounded growth | `wal.go` |
| Iterator API | Memory-efficient scans | `iterator.go` |

### File Extensions

| Extension | Description                   |
|-----------|-------------------------------|
| `.log`    | Write-Ahead Log               |
| `.sst`    | SSTable (Sorted String Table) |

### Error Types

```go
storage.ErrKeyNotFound   // Key does not exist
storage.ErrKeyTooLong    // Key > 1KB
storage.ErrValueTooLarge // Value > 1MB
storage.ErrEngineClosed  // Engine was closed
```

### Limits

- Max Key Size: 1KB
- Max Value Size: 1MB
- MemTable Size: 64MB (configurable)
- Tombstone TTL: 24 hours

---

## Further Reading

1. **LevelDB Paper**: [LevelDB: A Persistent Key-Value Store](https://static.googleusercontent.com/media/research.google.com/en//archive/leveldb.pdf)
2. **RocksDB Docs**: [RocksDB Architecture](https://github.com/facebook/rocksdb/wiki)
3. **Bloom Filters**: [Original Paper by Burton Bloom](http://www.cs.cmu.edu/~dga/15-741/Spring-2008/Papers/bloom70space.pdf)
4. **Skip Lists**: [Original Paper by William Pugh](https://dl.acm.org/doi/10.1145/127719.122718)

---

## Glossary

| Term             | Definition                                                        |
|------------------|-------------------------------------------------------------------|
| **LSM Tree**     | Log-Structured Merge Tree - a data structure optimized for writes |
| **SSTable**      | Sorted String Table - immutable sorted disk file                  |
| **MemTable**     | In-memory buffer for recent writes                                |
| **WAL**          | Write-Ahead Log - durability mechanism                            |
| **Bloom Filter** | Probabilistic data structure for set membership                   |
| **Tombstone**    | Marker indicating a deleted key                                   |
| **Compaction**   | Process of merging SSTables to reduce disk usage                  |
| **Flush**        | Process of writing MemTable to disk as SSTable                    |
| **MVCC**         | Multi-Version Concurrency Control - enables snapshots             |

---

### Happy Learning! 🚀

If you have questions, trace through the code with a debugger or add print statements to see the flow in action.
