# Kasoku Performance Benchmarks

## Latest Results (April 2026) - gRPC vs HTTP

### Single Node Performance

| Protocol | Writes | Reads | Total | Speedup |
|---------|--------|-------|-------|---------|
| HTTP | 344K | 169K | 513K | baseline |
| gRPC | 1.22M | 2.44M | **1.83M** | **3.6x** |

### 3-Node Cluster Performance

| Protocol | Writes | Reads | Total | Speedup |
|---------|--------|-------|-------|---------|
| HTTP | 140K | 30K | 170K | baseline |
| gRPC | 720K | 1.15M | **870K** | **5.1x** |

## Benchmark Configuration

### Hardware
- Apple M-Series Chip
- 8-core
- SSD Storage

### Software Settings
- MemTable Size: 256MB
- Block Cache: 512MB
- WAL: Async (500ms sync interval)
- Encoding: ProtoBuf

### Test Parameters
- Workers: 30
- Batch Size: 50 keys/request
- Test Duration: 10 seconds
- Warm-up: 1-2 seconds

## How to Run

### Prerequisites
```bash
# Build the server and benchmark tool
go build -o kasoku-server ./cmd/server/
go build -o pressure ./benchmarks/pressure/
```

### HTTP Benchmark (Single Node)
```bash
# Start single node
./kasoku-server -config ./configs/single.yaml &

# Run HTTP benchmark
./pressure -nodes=localhost:9001 -workers=30 -batch=50 -write-duration=10s -read-duration=10s -warm=1s
```

### gRPC Benchmark (Single Node)
```bash
# Start single node
./kasoku-server -config ./configs/single.yaml &

# Run gRPC benchmark
go run ./cmd/grpc-bench/main.go
```

### 3-Node Cluster HTTP
```bash
# Start cluster
./kasoku-server -config ./configs/node1.yaml &
./kasoku-server -config ./configs/node2.yaml &
./kasoku-server -config ./configs/node3.yaml &

# Run HTTP benchmark
./pressure -nodes=localhost:9001,localhost:9003,localhost:9005 -workers=30 -batch=50 -write-duration=10s -read-duration=10s -warm=2s
```

### 3-Node Cluster gRPC
```bash
# Start cluster
./kasoku-server -config ./configs/node1.yaml &
./kasoku-server -config ./configs/node2.yaml &
./kasoku-server -config ./configs/node3.yaml &

# Run gRPC benchmark
go run ./cmd/grpc-bench/main.go
```

## Configuration Files

### single.yaml
```yaml
port: 9000
http_port: 9001
grpc_port: 9002
memtable_size: 268435456
block_cache_size: 536870912
wal:
    sync_interval: 500ms
cluster:
    enabled: false
```

### node1.yaml, node2.yaml, node3.yaml
```yaml
# node1: port=9000, http=9001, grpc=9002
# node2: port=9002, http=9003, grpc=9004
# node3: port=9004, http=9005, grpc=9006

memtable_size: 256MB
block_cache_size: 512MB
cluster:
    enabled: true
    replication_factor: 3
    quorum_size: 1
    read_quorum: 1
    vnodes: 150
```

## Key Findings

1. **gRPC is 3.6x faster** on single node, **5.1x faster** on cluster
2. **Reads benefit most** - 14x faster (single), 38x faster (cluster)
3. **Connection pooling** eliminates setup overhead
4. **Binary encoding** (ProtoBuf) vs JSON reduces serialization

## Understanding Results

### Why gRPC Is Faster
- **Connection pooling**: 4-16 persistent connections reused
- **Binary encoding**: ProtoBuf vs JSON
- **No HTTP overhead**: No request/response framing
- **Streaming support**: Efficient batch operations

### Why Cluster Writes Are Lower
- Each write replicates to 3 nodes (RF=3)
- Background async replication adds overhead
- Network latency between nodes

### Why Cluster Reads Are Much Higher with gRPC
- Connection pooling eliminates setup latency
- gRPC MultiGet is more efficient than HTTP batch
- Less serialization overhead