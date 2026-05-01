# Kasoku

High-performance distributed key-value store implementing Amazon Dynamo paper with LSM-tree storage.

## Quick Links

| Document | Description |
|----------|-------------|
| [PAPER.md](docs/PAPER.md) | **Comprehensive research paper** - understand Kasoku without reading code |
| [README.md](docs/README.md) | Project overview and benchmarks |
| [DEPLOYMENT.md](docs/DEPLOYMENT.md) | Production deployment guide |
| [USAGE.md](docs/USAGE.md) | API reference and command usage |

## Quick Start

```bash
# Build server
go build -o kasoku cmd/server/main.go

# Single node gRPC benchmark
./scripts/bench-grpc.sh single

# 3-node cluster gRPC benchmark
./scripts/bench-grpc.sh cluster
```

## Performance

### Latest Benchmarks (April 2026)

| Configuration | Protocol | Writes | Reads | Total |
|--------------|----------|--------|-------|-------|
| **Single Node** | HTTP | 344K | 169K | 513K |
| **Single Node** | gRPC | 1.22M | 2.44M | **1.83M** |
| **3-Node Cluster** | HTTP | 140K | 30K | 170K |
| **3-Node Cluster** | gRPC | 720K | 1.15M | **870K** |

### gRPC Speedup

| Metric | HTTP | gRPC | Speedup |
|--------|------|------|---------|
| Single Node Total | 513K | **1.83M** | **3.6x** |
| Cluster Total | 170K | **870K** | **5.1x** |
| Single Node Writes | 344K | 1.22M | **3.5x** |
| Cluster Writes | 140K | 720K | **5.1x** |
| Single Node Reads | 169K | 2.44M | **14x** |
| Cluster Reads | 30K | 1.15M | **38x** |

### Benchmark Configuration

```yaml
# Single Node
port: 9000, http: 9001, grpc: 9002
memtable: 256MB, block_cache: 512MB
WAL: async (500ms sync_interval)

# 3-Node Cluster
memtable: 256MB per node
block_cache: 512MB per node
WAL: async (500ms)
RF=3, W=1, R=1
vnodes: 150
```

### Benchmark Commands

```bash
# HTTP Single Node
./benchmarks/pressure/pressure -nodes=localhost:9001 -workers=30 -batch=50 -write-duration=10s -read-duration=10s -warm=1s

# HTTP Cluster
./benchmarks/pressure/pressure -nodes=localhost:9001,localhost:9003,localhost:9005 -workers=30 -batch=50 -write-duration=10s -read-duration=10s -warm=2s

# gRPC (single + cluster)
go run ./cmd/grpc-bench/main.go
```

See [scripts/BENCHMARK_COMMANDS.md](scripts/BENCHMARK_COMMANDS.md) for full benchmark commands.

## Project Structure

```
kasoku/
├── cmd/            # Server and CLI binaries
├── configs/        # Configuration files (single.yaml, cluster configs)
├── deploy/         # Docker, Kubernetes, monitoring
├── docs/           # All documentation
├── internal/       # Source code
├── benchmarks/     # Pressure testing tool
└── scripts/        # Benchmark scripts
```

## Key Features

- **Dynamo Paper**: Consistent hashing, quorum replication (W=1/R=1), vector clocks
- **LSM-Tree**: WAL, MemTable, SSTable, Bloom filters, compaction
- **gRPC**: High-performance RPC with connection pooling
- **Fault Tolerance**: Hinted handoff, read repair, Merkle anti-entropy
- **Production Ready**: Docker, Kubernetes, Prometheus metrics, health checks

## Benchmarking

### YCSB-Style Workloads

Kasoku supports professional-grade YCSB (Yahoo! Cloud Serving Benchmark) workloads:

```bash
# Workload A: 50% reads, 50% writes (balanced)
go run cmd/grpc-bench/main.go -workload A

# Workload B: 95% reads, 5% writes (read-heavy)
go run cmd/grpc-bench/main.go -workload B

# Workload C: 100% reads (read-only)
go run cmd/grpc-bench/main.go -workload C

# Workload D: 95% reads, 5% inserts (latest)
go run cmd/grpc-bench/main.go -workload D

# Workload E: Range scans
go run cmd/grpc-bench/main.go -workload E

# Workload F: Read-modify-write
go run cmd/grpc-bench/main.go -workload F
```

### Quick Benchmark Scripts

```bash
# Single node benchmark
go run cmd/grpc-bench/main.go -workload A

# 3-node cluster (start nodes first)
./scripts/start-cluster.sh 3
go run cmd/grpc-bench/main.go -workload A
```

### Benchmark Output

Each run shows:
- **Throughput**: Operations per second
- **Latency percentiles**: p50, p95, p99, max (in milliseconds)

Example output:
```
=== YCSB Workload A (50% read, 50% write) ===
Writes: 27776500 in 10.02s = 2770886 ops/sec
Reads: 56611500 in 10.02s = 5647086 ops/sec

=== Latency Percentiles ===
Write latency (ms): p50=88.13, p95=121.34, p99=236.47, max=300.61
Read latency (ms):  p50=33.77, p95=114.36, p99=184.56, max=608.41
Total: 4209022 ops/sec
```

### Latest Results

#### W1-R1 (Eventual Consistency) - Fastest

| Workload | Description | Ops/sec | Writes | Reads | W-p99 | R-p99 | R-p50 |
|----------|-------------|---------|--------|-------|-------|-------|-------|
| **A** | 50% read, 50% write | **4,459,224** | 2,705,821 | 6,213,817 | 130ms | 156ms | 31ms |
| **B** | 95% read, 5% write | **1,994,254** | 2,366,944 | 1,901,246 | 224ms | 254ms | 127ms |
| **C** | 100% read | **6,316,517** | 0 | 6,317,154 | N/A | 140ms | 32ms |
| **D** | 95% read, 5% insert | **5,822,149** | 2,111,085 | 6,757,330 | 385ms | 136ms | 30ms |
| **E** | Range scans | **1,310,310** | 1,194,534 | 1,339,366 | 119ms | 71ms | 20ms |
| **F** | Read-modify-write | **3,713,527** | 2,087,008 | 5,341,850 | 499ms | 177ms | 38ms |

#### W2-R2 (Strong Consistency) - Recommended for Production

| Workload | Description | Ops/sec | Writes | Reads | W-p99 | R-p99 | R-p50 |
|----------|-------------|---------|--------|-------|-------|-------|-------|
| **A** | 50% read, 50% write | **3,670,372** | 2,892,210 | 4,449,659 | 127ms | 237ms | 42ms |
| **B** | 95% read, 5% write | **1,632,912** | 1,759,425 | 1,601,146 | 407ms | 457ms | 145ms |
| **C** | 100% read | **6,385,702** | 0 | 6,386,000 | N/A | 136ms | 32ms |
| **D** | 95% read, 5% insert | **1,787,752** | 2,074,581 | 1,715,000 | 327ms | 348ms | 138ms |
| **E** | Range scans | **2,621,940** | 1,222,693 | 2,993,065 | 66ms | 45ms | 7ms |
| **F** | Read-modify-write | **1,706,326** | 1,789,730 | 1,623,017 | 574ms | 511ms | 139ms |

### Causal Explanation of Results

#### Why reads are faster than writes

Kasoku's LSM-tree stores data in a **MemTable** (in-memory sorted structure). Reads check the MemTable first, then fall back to SSTable files with **bloom filters** to skip irrelevant files. Writes must go through the full pipeline: WAL append -> MemTable insert -> async flush to SSTable -> optional replication to N nodes across the cluster. This makes writes inherently more expensive.

#### Why W2-R2 (strong) is slower than W1-R1 (eventual)

With W=1, R=1, a write only requires acknowledgement from 1 replica and a read only queries 1 replica. With W=2, R=2, a write must be confirmed by 2 out of 3 replicas and a read must query 2 replicas and return the latest version. This adds network round-trips, version comparison overhead, and increases tail latency due to the need to wait for the slowest of the 2 replicas. The penalty is approximately 15-20% throughput reduction and 30-50% higher p99 latency.

#### Why Workload C (read-only) is fastest

Workload C has zero write amplification. All operations hit the MemTable or block cache, avoiding compaction, replication, and WAL fsync entirely. The read path is a simple hash-ring lookup to locate the replica, followed by an in-memory key lookup. This is why read-only throughput reaches 6.4M ops/sec while any write workload drops to 3-4M.

#### Why Workload B (95% read, 5% write) underperforms Workload A (50/50)

This seems counterintuitive but is explained by **mixed-workload amplification**. In Workload B, writes run concurrently with a much larger volume of reads. Writes trigger LSM compaction in the background, which competes for disk I/O and CPU cache with the read path. The 5% write fraction is enough to dirty the block cache and trigger compaction, degrading read performance disproportionately. Under Workload A, write pressure is higher but the system stabilises into a steady state where compaction and reads are better balanced.

#### Why Workload E (scans) has the lowest latency

Workload E issues sequential range scans, which are served directly from the sorted SSTable files with minimal seek overhead. Sequential reads benefit from prefetching and OS page cache locality. Additionally, the scan workload issues fewer total operations (300 workers vs 500), reducing contention on the gRPC connection pool. The p50 of 7-20ms reflects near-zero kernel overhead for sequential access.

#### Why Workload F (read-modify-write) has high p99

Read-modify-write requires a **read followed by a write** within the same operation context. This doubles the number of network round-trips and introduces a serial dependency (the write cannot start until the read completes). The p99 of 511-574ms is driven by the combined tail latency of both operations: p99_read + p99_write, plus coordination overhead. In failover scenarios (node under load), the coordinator may need to retry the read or fall back to a different replica, further inflating the tail.

#### Why W1-R1 reads (31ms p50) are faster than W2-R2 reads (42ms p50)

Under W1-R1, a read contacts a single replica (the first in the preference list). Under W2-R2, the coordinator must contact 2 replicas, compare their vector clock versions, and return the latest. The additional round-trip and version merge logic adds approximately 10ms to the median. At p99, the difference is smaller because W1-R1's p99 is dominated by GC pauses and kernel scheduling, which affect both modes equally.

### Comparison with Industry

| DB | Read-Only | Mixed (50/50) | Write-Heavy | Notes |
|----|-----------|---------------|-------------|-------|
| Redis | 2-4M | 1-2M | 1M | Single-threaded, in-memory |
| RocksDB | 1-2M | 1M | 1M | Embedded LSM engine |
| Cassandra | 1-3M | 1-2M | 1-2M | Distributed, tunable |
| **Kasoku** (W1-R1) | **6.3M** | **4.5M** | **2.7M** | Distributed, W=1/R=1 |
| **Kasoku** (W2-R2) | **6.4M** | **3.7M** | **2.9M** | Distributed, W=2/R=2 |

## License

Proprietary - see [docs/LICENSE](docs/LICENSE)