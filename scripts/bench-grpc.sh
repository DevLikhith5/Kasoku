#!/bin/bash
set -e

cleanup() {
    pkill -f "go run cmd/server" 2>/dev/null || true
}
trap cleanup EXIT

# Need to pass MODE as arg
MODE=${1:-single}

cleanup

if [ "$MODE" = "single" ]; then
    echo "=== SINGLE NODE gRPC BENCHMARK ==="
    go run cmd/server/main.go --config configs/grpc-single.yaml > /tmp/kasoku-single.log 2>&1 &
    sleep 4
    # Ensure bench file is in single mode
    sed -i '' 's/3-NODE CLUSTER/SINGLE NODE/' cmd/grpc-bench/main.go
    sed -i '' 's/localhost:9002,localhost:9003,localhost:9004/localhost:9002/' cmd/grpc-bench/main.go
    go run cmd/grpc-bench/main.go 2>&1

elif [ "$MODE" = "cluster" ]; then
    echo "=== 3-NODE CLUSTER gRPC BENCHMARK ==="
    go run cmd/server/main.go --config configs/cluster-node1.yaml > /tmp/node1.log 2>&1 &
    go run cmd/server/main.go --config configs/cluster-node2.yaml > /tmp/node2.log 2>&1 &
    go run cmd/server/main.go --config configs/cluster-node3.yaml > /tmp/node3.log 2>&1 &
    sleep 12
    # Switch bench to cluster mode
    sed -i '' 's/SINGLE NODE/3-NODE CLUSTER/' cmd/grpc-bench/main.go
    sed -i '' 's/localhost:9002/localhost:9002,localhost:9003,localhost:9004/' cmd/grpc-bench/main.go
    go run cmd/grpc-bench/main.go 2>&1
    # Restore
    sed -i '' 's/3-NODE CLUSTER/SINGLE NODE/' cmd/grpc-bench/main.go
    sed -i '' 's/localhost:9002,localhost:9003,localhost:9004/localhost:9002/' cmd/grpc-bench/main.go

elif [ "$MODE" = "all" ]; then
    echo "=== SINGLE NODE gRPC BENCHMARK ==="
    go run cmd/server/main.go --config configs/grpc-single.yaml > /tmp/kasoku-single.log 2>&1 &
    sleep 4
    sed -i '' 's/3-NODE CLUSTER/SINGLE NODE/' cmd/grpc-bench/main.go
    sed -i '' 's/localhost:9002,localhost:9003,localhost:9004/localhost:9002/' cmd/grpc-bench/main.go
    go run cmd/grpc-bench/main.go 2>&1

    cleanup
    sleep 2

    echo ""
    echo "=== 3-NODE CLUSTER gRPC BENCHMARK ==="
    go run cmd/server/main.go --config configs/cluster-node1.yaml > /tmp/node1.log 2>&1 &
    go run cmd/server/main.go --config configs/cluster-node2.yaml > /tmp/node2.log 2>&1 &
    go run cmd/server/main.go --config configs/cluster-node3.yaml > /tmp/node3.log 2>&1 &
    sleep 12
    sed -i '' 's/SINGLE NODE/3-NODE CLUSTER/' cmd/grpc-bench/main.go
    sed -i '' 's/localhost:9002/localhost:9002,localhost:9003,localhost:9004/' cmd/grpc-bench/main.go
    go run cmd/grpc-bench/main.go 2>&1
    sed -i '' 's/3-NODE CLUSTER/SINGLE NODE/' cmd/grpc-bench/main.go
    sed -i '' 's/localhost:9002,localhost:9003,localhost:9004/localhost:9002/' cmd/grpc-bench/main.go
fi

echo ""
echo "=== DONE ==="