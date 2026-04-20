# Kasoku Performance Benchmarks

## Latest Results (April 2026)

### Single Node Performance

| Metric | Ops/sec | Latency p50 | Latency p99 |
|--------|--------|-------------|-------------|
| **Writes** | 255,735 | 46µs | 602µs |
| **Reads (Batch)** | 244,398 | 76µs | 728µs |
| **Total** | 500,133 | - | - |

### 3-Node Cluster (RF=3, W=2, R=1) - Strong Consistency

| Metric | Ops/sec | Latency p50 | Latency p99 |
|--------|--------|-------------|-------------|
| **Writes** | 82,390 | 138µs | 1.37ms |
| **Reads** | 125,415 | 155µs | 1.55ms |
| **Total** | 207,805 | - | - |

## Benchmark Configuration

### Hardware
- Apple M-Series Chip
- 8-core
- SSD Storage

### Software Settings
- MemTable Size: 256MB
- Block Size: 64KB
- Block Cache: 1GB
- Key Cache: 1M entries
- WAL: Async (100ms sync interval)
- Encoding: Binary with magic byte

### Test Parameters
- Workers: 30
- Batch Size: 25 keys/request
- Test Duration: 20 seconds
- Warm-up: 3 seconds

## How to Run

### Prerequisites
```bash
# Build the server and benchmark tool
go build -o kasoku-server ./cmd/server/
go build -o pressure ./tools/benchmarks/pressure/
```

### Single Node Benchmark

```bash
# Start single node
KASOKU_CONFIG=configs/single.yaml ./kasoku-server &

# Run benchmark
./pressure -nodes=localhost:9000 -write-duration=20s -read-duration=20s -workers=30 -batch=25
```

### 3-Node Cluster Benchmark

```bash
# Start node 1
KASOKU_NODE_ID=node-1 KASOKU_DATA_DIR=./data/node1 KASOKU_HTTP_PORT=9000 KASOKU_CONFIG=configs/cluster.yaml ./kasoku-server &

# Start node 2
KASOKU_NODE_ID=node-2 KASOKU_DATA_DIR=./data/node2 KASOKU_HTTP_PORT=9001 KASOKU_CONFIG=configs/cluster.yaml ./kasoku-server &

# Start node 3
KASOKU_NODE_ID=node-3 KASOKU_DATA_DIR=./data/node3 KASOKU_HTTP_PORT=9002 KASOKU_CONFIG=configs/cluster.yaml ./kasoku-server &

# Run benchmark
./pressure -nodes=localhost:9000,localhost:9001,localhost:9002 -write-duration=20s -read-duration=20s -workers=30 -batch=25
```

## Understanding Results

### Why Cluster Writes Are Lower
In distributed mode with RF=3 and W=2:
- Each write is replicated to 3 nodes
- Coordinator waits for 2 acks before responding
- Network latency between nodes affects throughput

### Why Cluster Reads Are Higher Peak
With R=1 (eventual consistency):
- Reads can be served by any node with the data
- Peak throughput can exceed single node when data is distributed
- Average is lower due to coordinator routing overhead

### LSM Engine Fix Applied (April 2026)
Fixed race condition in flush/compact loops:
- Added directFlushCh channel to coordinate flush operations
- Added flushing atomic flag to prevent dual flushing
- Performance improved significantly over original benchmarks

## Performance Tips

### Maximize Single Node Performance
- Use `kasoku-single.yaml` config
- Disable cluster mode
- Increase memory settings for larger caches

### Maximize Cluster Throughput
- Use R=1 for reads (eventual consistency)
- Place nodes in same datacenter for low latency
- Use larger batch sizes (25-100)
- Tune worker count based on CPU cores
