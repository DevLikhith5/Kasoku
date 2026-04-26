#!/bin/bash
set -e

cleanup() {
    pkill -f "kas" 2>/dev/null || true
    sleep 1
}
trap cleanup EXIT

MODE=${1:-single}

cleanup
cd /Users/cvlikhith/kasoku
go build -o /tmp/kas cmd/server/main.go

if [ "$MODE" = "single" ]; then
    echo "=== SINGLE NODE gRPC BENCHMARK ==="
    /tmp/kas --config configs/grpc-single.yaml > /tmp/kas.log 2>&1 &
    sleep 5
    
    sed -i '' 's/3-NODE CLUSTER/SINGLE NODE/' cmd/grpc-bench/main.go
    sed -i '' 's/localhost:9002,localhost:9003,localhost:9004/localhost:9002/' cmd/grpc-bench/main.go
    go run cmd/grpc-bench/main.go 2>&1
    sed -i '' 's/localhost:9002/localhost:9002,localhost:9003,localhost:9004/' cmd/grpc-bench/main.go
    sed -i '' 's/SINGLE NODE/3-NODE CLUSTER/' cmd/grpc-bench/main.go

elif [ "$MODE" = "cluster" ]; then
    echo "=== 3-NODE CLUSTER gRPC BENCHMARK ==="
    rm -rf data/ && mkdir -p data/
    /tmp/kas --config configs/cluster-node1.yaml > /tmp/n1.log 2>&1 &
    /tmp/kas --config configs/cluster-node2.yaml > /tmp/n2.log 2>&1 &
    /tmp/kas --config configs/cluster-node3.yaml > /tmp/n3.log 2>&1 &
    sleep 20
    
    sed -i '' 's/SINGLE NODE/3-NODE CLUSTER/' cmd/grpc-bench/main.go
    sed -i '' 's/localhost:9002/localhost:9002,localhost:9003,localhost:9004/' cmd/grpc-bench/main.go
    go run cmd/grpc-bench/main.go 2>&1
    sed -i '' 's/localhost:9002,localhost:9003,localhost:9004/localhost:9002/' cmd/grpc-bench/main.go
    sed -i '' 's/3-NODE CLUSTER/SINGLE NODE/' cmd/grpc-bench/main.go
fi

echo ""
echo "=== DONE ==="