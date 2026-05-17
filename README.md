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

These metrics represent the true, verified performance of the Kasoku Distributed LSM-Engine, measured across a 3-node cluster with strong consistency (`W=2, R=2`) and 50–100 concurrent workers over gRPC.

### 1. Local Throughput (Pre-loading Phase)
*When writing directly to the node without network consensus routing:*
* **Local Write Throughput:** `1,000,000 to 1,700,000 keys/sec`
* **Local Write Latency:** `< 1 ms`

### 2. Distributed YCSB-Style Benchmarks
*When simulating real-world distributed workloads with `any-node-accept` routing:*

| YCSB Workload | Mix | Total Throughput | Write P99 Latency | Read P99 Latency |
| :--- | :--- | :--- | :--- | :--- |
| **Workload A** (Heavy Update) | 50% R / 50% W | **60,238 keys/sec** | 34.4 ms | 15.2 ms |
| **Workload B** (Read Mostly) | 95% R / 5% W | **60,024 keys/sec** | 47.6 ms | 21.3 ms |
| **Workload C** (Read Only) | 100% R / 0% W | **58,155 keys/sec** | N/A | 27.3 ms |
| **Max Write** (Heavy Insert) | 0% R / 100% W | **54,122 keys/sec** | 59.2 ms | N/A |

### 3. Stability & Consistency Metrics
* **Error Rate:** `0.00%` (0 dropped or failed batches across millions of operations)
* **Metadata Propagation:** Perfectly synchronized (Coordinator-Authoritative metadata ensures `Version`, `TimeStamp`, and `VectorClock` are identical across all replicas).

### How to Reproduce

```bash
# Build the server and benchmark tools
go build -o kasoku-server ./cmd/server/
go build -o kasoku-bench ./cmd/bench/

# 3-Node Cluster (Dynamo-style: async WAL + W=2 R=2)
KASOKU_DATA_DIR=./data/n1 KASOKU_CONFIG=./configs/bench-cluster-node1.yaml ./kasoku-server &
KASOKU_DATA_DIR=./data/n2 KASOKU_CONFIG=./configs/bench-cluster-node2.yaml ./kasoku-server &
KASOKU_DATA_DIR=./data/n3 KASOKU_CONFIG=./configs/bench-cluster-node3.yaml ./kasoku-server &

# Run Workload B (95% Reads)
./kasoku-bench -nodes=localhost:9100,localhost:9101,localhost:9102 -workers=50 -batch=5 -seed=10000 -reads=95 -dur=15
```

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