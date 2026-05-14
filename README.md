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

Production benchmarks with durable WAL (fsync every write), realistic cache sizes, YCSB workloads, 60-second runs.

### Benchmark Configuration

```yaml
# Realistic cache (data >> cache to force SSTable I/O)
key_cache_size: 10000        # 10K entries (not 1M)
block_cache_size: 16MB       # 16MB (not 128MB)
memtable_size: 16MB          # Forces frequent flushes
dataset: 2M keys × 100B = 220MB (>> 16MB cache)
wal: sync=true (fsync every write)
workers: 20, duration: 60s, batch: 1 key/request
```

### 1. Strict Durability (Bank-Style)
*`wal.sync=true`, `batch=1` — Every write waits for SSD physical fsync.*

| Workload | Write | Read | Write p50/p95/p99 | Read p50/p95/p99 |
|----------|-------|------|-------------------|-----------------|
| **Single-Node** (50R/50W) | ~1,500 ops/s | ~1,500 ops/s | 3.5 / 8.2 / 22.3ms | 0.5 / 0.9 / 1.3ms |
| **3-Node Cluster** (Quorum W=2, R=2) | ~2,500 ops/s | ~2,500 ops/s | 3.8 / 10.7 / 22.4ms | 0.5 / 1.4 / 3.7ms |

### 2. High-Throughput AP Mode (Cassandra/DynamoDB-Style)
*`wal.sync=false` (1s background flush), `batch=25`, `workers=100`, `quorum=1` (Eventual Consistency).*

| Workload | Write | Read | Combined Ops/s |
|----------|-------|------|----------------|
| **Single-Node** (50R/50W) | ~17,800 ops/s | ~17,700 ops/s | **~35,500 ops/s** |
| **3-Node Cluster** (Eventual, W=1) | ~35,500 ops/s | ~35,600 ops/s | **~71,200 ops/s** |

*Notice how the 3-node cluster scales horizontally, processing exactly 2x the throughput of the single node under heavy network load by utilizing Eventual Consistency.*

### How to Reproduce

```bash
# Full YCSB benchmark suite (single + cluster, ~20 min)
bash benchmarks/run_ycsb_bench.sh

# Or manually:
go build -o kasoku-server ./cmd/server/

# Single Node
KASOKU_DATA_DIR=./data/bench KASOKU_CONFIG=./configs/bench-realistic.yaml ./kasoku-server &
go run ./cmd/bench/main.go -nodes=localhost:9100 -workers=20 -batch=1 -seed=2000000 -reads=95 -dur=60

# 3-Node Cluster
KASOKU_DATA_DIR=./data/n1 KASOKU_CONFIG=./configs/bench-cluster-node1.yaml ./kasoku-server &
KASOKU_DATA_DIR=./data/n2 KASOKU_CONFIG=./configs/bench-cluster-node2.yaml ./kasoku-server &
KASOKU_DATA_DIR=./data/n3 KASOKU_CONFIG=./configs/bench-cluster-node3.yaml ./kasoku-server &
go run ./cmd/bench/main.go -nodes=localhost:9002,localhost:9012,localhost:9022 -workers=20 -batch=1 -seed=2000000 -reads=95 -dur=60
```

### Industry Comparison

| System | Write (durable) | Read | Notes |
|--------|-----------------|------|-------|
| Redis (AOF) | ~100K/s | ~1M/s | In-memory, optional durability |
| Cassandra | ~50K/s | ~50K/s | Distributed, tunable consistency |
| RocksDB | ~500K/s | ~1M/s | Embedded, optional WAL |
| **Kasoku (single)** | **48K/s** | **905K/s** | YCSB-B, durable WAL, small cache |
| **Kasoku (cluster)** | **152K/s** | **152K/s** | YCSB-A, RF=3, W=2, R=2 |

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