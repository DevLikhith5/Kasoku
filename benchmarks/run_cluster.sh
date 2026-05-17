#!/bin/bash
# Kasoku Distributed Benchmark — 3-Node Cluster
# Usage: ./benchmarks/run_cluster.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║     KASOKU — 3-Node Cluster Benchmark                       ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Build
echo "▸ Building..."
go build -o kasoku-server ./cmd/server
go build -o bench-client ./cmd/bench
echo "  Done."

# Clean old data
rm -rf data/bench-node1 data/bench-node2 data/bench-node3

# Start 3 nodes
echo ""
echo "▸ Starting 3-node cluster..."

./kasoku-server --config configs/bench-cluster-node1.yaml &
NODE1_PID=$!

./kasoku-server --config configs/bench-cluster-node2.yaml &
NODE2_PID=$!

./kasoku-server --config configs/bench-cluster-node3.yaml &
NODE3_PID=$!

cleanup() {
    echo ""
    echo "▸ Shutting down nodes..."
    kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true
    wait $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true
    echo "  Done."
}
trap cleanup EXIT

# Wait for nodes to be ready
echo "  Waiting for nodes to start..."
sleep 3

# Health check
for port in 9001 9011 9021; do
    if curl -s "http://localhost:$port/health" > /dev/null 2>&1; then
        echo "  Node on port $port: OK"
    else
        echo "  Node on port $port: FAILED (waiting 2s more...)"
        sleep 2
        if ! curl -s "http://localhost:$port/health" > /dev/null 2>&1; then
            echo "  ERROR: Node on port $port still not ready. Aborting."
            exit 1
        fi
        echo "  Node on port $port: OK (after retry)"
    fi
done

echo ""
echo "▸ Running benchmark..."
echo ""

./bench-client \
    -nodes "localhost:9100,localhost:9101,localhost:9102" \
    -workers 30 \
    -batch 1 \
    -seed 500000 \
    -reads 80 \
    -dur 30

echo ""
echo "▸ Benchmark complete!"
