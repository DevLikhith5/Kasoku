# Kasoku

High-performance distributed key-value store implementing Amazon Dynamo paper with LSM-tree storage.

## Documentation

See `docs/ARCHITECTURE.md` for a complete deep dive into the system design, the LSM-tree storage engine, consistent hashing, and cluster replication mechanics.

## uick Start (1-Click Docker)

The easiest way to run Kasoku is via Docker, just like Postgres or Redis.

### Standalone Node (Local Development)
```bash
docker-compose up -d
```
Your database is now running in the background on port `9000`!

### 3-Node Distributed Cluster
```bash
docker-compose -f docker-compose.cluster.yml up -d
```
This spins up a fully connected 3-node cluster on ports `9001`, `9002`, and `9003`.

---

## Manual Build (Go)

```bash
# 1. Build the server
go build -o kasoku cmd/server/main.go

# 2. Start a single node
KASOKU_CONFIG=./configs/example.yaml ./kasoku

# 3. Run the automated benchmark suite
bash benchmarks/run_ycsb_bench.sh
```

## Performance (YCSB-Standard Benchmarks)

Production benchmarks with realistic cache sizes, YCSB workloads, 60-second runs.

### Benchmark Configuration

```yaml
# Realistic cache (data >> cache to force SSTable I/O)
key_cache_size: 10000        # 10K entries (not 1M)
block_cache_size: 16MB       # 16MB (not 128MB)
memtable_size: 16MB          # Forces frequent flushes
dataset: 2M keys × 100B = ~200MB (>> 16MB cache)
workers: 200, duration: 60s, batch: 100 keys/request
```

### 1. Single Node (Sync WAL — Strict Local Durability)

Every write fsyncs to local disk before returning. Best for standalone deployments.

| Workload | Combined | Writes | Reads | W p50 | R p50 | Errors |
|----------|----------|--------|-------|-------|-------|--------|
| **B (95%R/5%W)** | **99,586 keys/s** | 5,008/s | 94,578/s | 2.7ms | 42.0ms | 12W, 567R |
| **A (50%R/50%W)** | **97,678 keys/s** | 48,736/s | 48,941/s | 51.0ms | 12.6ms | 0W, 73R |

### 2. 3-Node Cluster (Async WAL + W=2 R=2 — Dynamo-Style)

Replication provides durability (2 acks before return). Local WAL is async (1s flush). This is the **recommended production config**.

| Workload | Combined | Writes | Reads | W p50 | R p50 | Errors |
|----------|----------|--------|-------|-------|-------|--------|
| **B (95%R/5%W)** | **287,265 keys/s** | 14,218/s | 273,047/s | 6.3ms | 17.8ms | 0W, 8R |
| **A (50%R/50%W)** | **85,864 keys/s** | 42,727/s | 43,137/s | 14.0ms | 22.0ms | 117W, 32R |

### 3. Config Comparison (Cluster Workload B)

| Config | WAL | Quorum | Combined | Read p50 | Notes |
|--------|-----|--------|----------|----------|-------|
| Async W=1 | 1s | W=1 R=1 | 156,375/s | 24.9ms | Eventual, fire-and-forget |
| Sync W=1 | 0s | W=1 R=1 | 87,367/s | 43.7ms | Local fsync, still eventual repl |
| Sync W=2 | 0s | W=2 R=2 | 261,166/s | 18.7ms | Strict durability + strong consistency |
| **Async W=2** | **1s** | **W=2 R=2** | **287,265/s** | **17.8ms** | **Dynamo-style (recommended)** |

*Async WAL + W=2 is optimal: replication provides durability, removing the need for local fsync on every write.*

### How to Reproduce

```bash
# Full YCSB benchmark suite (single + cluster, ~20 min)
bash benchmarks/run_ycsb_bench.sh

# Or manually:
go build -o kasoku-server ./cmd/server/

# Single Node (sync WAL)
KASOKU_DATA_DIR=./data/bench KASOKU_CONFIG=./configs/bench-realistic.yaml ./kasoku-server &
go run ./cmd/bench/main.go -nodes=localhost:9100 -workers=200 -batch=100 -seed=2000000 -reads=95 -dur=60

# 3-Node Cluster (Dynamo-style: async WAL + W=2 R=2)
KASOKU_DATA_DIR=./data/n1 KASOKU_CONFIG=./configs/bench-cluster-node1.yaml ./kasoku-server &
KASOKU_DATA_DIR=./data/n2 KASOKU_CONFIG=./configs/bench-cluster-node2.yaml ./kasoku-server &
KASOKU_DATA_DIR=./data/n3 KASOKU_CONFIG=./configs/bench-cluster-node3.yaml ./kasoku-server &
go run ./cmd/bench/main.go -nodes=localhost:9002,localhost:9012,localhost:9022 -workers=200 -batch=100 -seed=2000000 -reads=95 -dur=60
```

### Industry Comparison

| System | Workload B | Workload A | Consistency | Notes |
|--------|-----------|-----------|-------------|-------|
| **Kasoku (cluster)** | **287K keys/s** | **86K keys/s** | W=2 R=2 strong | Go, LSM, laptop SSD |
| **Kasoku (single)** | **100K keys/s** | **98K keys/s** | None (local) | Sync WAL, no replication |
| Cassandra | 100-200K/s | 50-80K/s | W=1 eventual | Java, tuned for speed |
| Cassandra (strong) | 30-50K/s | 20-40K/s | W=2 R=2 | Same system, quorum writes |
| etcd (3-node) | 10-15K/s | 10K/s | Raft | Go, log-structured, metadata |
| DynamoDB (cloud) | 50-100K/s | 30-50K/s | Per-partition | AWS managed, not comparable |
| Redis (in-memory) | 500K-1M/s | 500K-1M/s | None | Pure RAM, no disk durability |
| RocksDB | 200-400K/s | 100-200K/s | None | C++, embedded, no network |
| TiKV | 100-200K/s | 50-100K/s | Raft | Rust, distributed transactions |

*Kasoku cluster with W=2 R=2 matches Cassandra's speed while providing stronger consistency, and outperforms Cassandra's strong consistency mode by 5-9×.*

With cold data and larger datasets, writes would equal or exceed reads.

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

## Distributed Tracing

Kasoku includes built-in OpenTelemetry tracing for observability across the distributed cluster.

### Enable Tracing

```bash
# Enable with stdout export (prints to console)
KASOKU_TRACING=true ./kasoku --config config.yaml

# Enable with OTLP export (send to Jaeger/Zipkin)
KASOKU_TRACING=true KASOKU_TRACING_EXPORTER=otlp KASOKU_OTLP_ENDPOINT=localhost:4317 ./kasoku --config config.yaml
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KASOKU_TRACING` | Enable tracing (`true`/`false`) | `false` |
| `KASOKU_TRACING_EXPORTER` | Export format (`stdout`, `otlp`) | `stdout` |
| `KASOKU_OTLP_ENDPOINT` | OTLP collector address | `localhost:4317` |
| `KASOKU_OTLP_INSECURE` | Use insecure gRPC | `false` |

### Traced Operations

- **gRPC Handlers**: Put, Get, BatchPut, MultiGet, Delete
- **Cluster Replication**: ReplicatedPut, ReplicatedBatchPut
- **Storage Engine**: LSM Put/Get/Batch operations
- **Cross-node**: Trace context propagates across cluster nodes

### View Traces

**Console (stdout):**
```bash
KASOKU_TRACING=true go run cmd/server/main.go
# Spans print as JSON to stdout
```

**Jaeger:**
```bash
# Start Jaeger
docker run -d --name jaeger -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one

# Run server with tracing
KASOKU_TRACING=true KASOKU_TRACING_EXPORTER=otlp ./kasoku --config config.yaml

# Open http://localhost:16686
```

**Zipkin:**
```bash
# Start Zipkin
docker run -d --name zipkin -p 9411:9411 openzipkin/zipkin

# Run with OTLP (requires zipkin-collector)
KASOKU_TRACING=true KASOKU_TRACING_EXPORTER=otlp KASOKU_OTLP_ENDPOINT=localhost:9411 ./kasoku
```

### Trace Example Output

```json
{
  "Span": {
    "TraceId": "a1b2c3d4e5f6...",
    "SpanId": "1234567890ab",
    "ParentSpanId": "",
    "Name": "gRPC.BatchPut",
    "StartTime": "2026-05-03T12:00:00.000Z",
    "EndTime": "2026-05-03T12:00:00.015Z",
    "Attributes": {
      "entry_count": "100",
      "success": "true"
    }
  }
}
```

## License

Proprietary - see [docs/LICENSE](docs/LICENSE)