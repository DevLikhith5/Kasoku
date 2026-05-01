#!/bin/bash

# Kasoku Benchmark Script
# Usage: ./scripts/benchmark.sh [single|cluster] [workload]
# 
# Examples:
#   ./scripts/benchmark.sh single A      # Single node, Workload A
#   ./scripts/benchmark.sh cluster B     # 3-node cluster, Workload B
#   ./scripts/benchmark.sh                # Default: single node, Workload A

set -e

MODE=${1:-single}
WORKLOAD=${2:-A}

echo "============================================"
echo "  Kasoku Benchmark - $MODE mode, Workload $WORKLOAD"
echo "============================================"

# Kill existing servers
pkill -9 -f "kasoku" 2>/dev/null || true
sleep 2

# Build
echo "Building..."
cd /Users/cvlikhith/kasoku
go build -o /tmp/kas cmd/server/main.go

# Clean data
rm -rf /Users/cvlikhith/kasoku/data/node*
mkdir -p /Users/cvlikhith/kasoku/data/node1 /Users/cvlikhith/kasoku/data/node2 /Users/cvlikhith/kasoku/data/node3

if [ "$MODE" = "cluster" ]; then
    echo "Starting 3-node cluster..."
    nohup /tmp/kas --config /Users/cvlikhith/kasoku/configs/cluster-node1.yaml > /tmp/kas1.log 2>&1 &
    nohup /tmp/kas --config /Users/cvlikhith/kasoku/configs/cluster-node2.yaml > /tmp/kas2.log 2>&1 &
    nohup /tmp/kas --config /Users/cvlikhith/kasoku/configs/cluster-node3.yaml > /tmp/kas3.log 2>&1 &
    sleep 6
    
    # Check nodes
    echo "Checking nodes..."
    curl -s http://localhost:9000/health > /dev/null && echo "Node 1: OK" || echo "Node 1: FAILED"
    curl -s http://localhost:9001/health > /dev/null && echo "Node 2: OK" || echo "Node 2: FAILED"  
    curl -s http://localhost:9002/health > /dev/null && echo "Node 3: OK" || echo "Node 3: FAILED"
else
    echo "Starting single node..."
    nohup /tmp/kas --config /Users/cvlikhith/kasoku/configs/cluster-node1.yaml > /tmp/kas1.log 2>&1 &
    sleep 6
fi

echo ""
echo "Running benchmark..."
echo ""

# Run benchmark
go run cmd/grpc-bench/main.go -workload $WORKLOAD

echo ""
echo "============================================"
echo "  Benchmark Complete!"
echo "============================================"