#!/usr/bin/env bash
set -e

echo "=================================="
echo "   Kasoku 3-Node Cluster Starter"
echo "=================================="

# Build
echo "[1/4] Building kasoku..."
go build -o kasoku ./cmd/server/main.go

# Clean data
echo "[2/4] Cleaning old data..."
pkill -f "kasoku" 2>/dev/null || true
sleep 1
rm -rf data/node-1 data/node-2 data/node-3
mkdir -p data/node-1 data/node-2 data/node-3

# Cleanup handler
PIDS=()
cleanup() {
    echo ""
    echo "Shutting down all Kasoku nodes..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    echo "Goodbye!"
    exit 0
}
trap cleanup SIGINT SIGTERM

# Start nodes
echo "[3/4] Starting 3-node cluster..."

KASOKU_NODE_ID=node-1 KASOKU_DATA_DIR=./data/node-1 KASOKU_HTTP_PORT=9000 \
    KASOKU_CONFIG=configs/cluster.yaml ./kasoku > /tmp/kasoku-node1.log 2>&1 &
PIDS+=($!)
sleep 0.5

KASOKU_NODE_ID=node-2 KASOKU_DATA_DIR=./data/node-2 KASOKU_HTTP_PORT=9001 \
    KASOKU_CONFIG=configs/cluster.yaml ./kasoku > /tmp/kasoku-node2.log 2>&1 &
PIDS+=($!)
sleep 0.5

KASOKU_NODE_ID=node-3 KASOKU_DATA_DIR=./data/node-3 KASOKU_HTTP_PORT=9002 \
    KASOKU_CONFIG=configs/cluster.yaml ./kasoku > /tmp/kasoku-node3.log 2>&1 &
PIDS+=($!)
sleep 3

# Verify
echo ""
echo "[4/4] Verifying nodes..."
ALL_OK=true
for port in 9000 9001 9002; do
    if curl -sf http://localhost:$port/health > /dev/null 2>&1; then
        echo "      ✓ Node on port $port is healthy"
    else
        echo "      ✗ Node on port $port not responding"
        ALL_OK=false
    fi
done

if $ALL_OK; then
    echo ""
    echo "=================================="
    echo "  ✓ Cluster running!"
    echo "  Node 1: http://localhost:9000"
    echo "  Node 2: http://localhost:9001"
    echo "  Node 3: http://localhost:9002"
    echo ""
    echo "  Logs: /tmp/kasoku-node{1,2,3}.log"
    echo "=================================="
    echo ""
    echo "Press Ctrl+C to stop all nodes."
else
    echo ""
    echo "Some nodes failed. Check /tmp/kasoku-node*.log"
fi

wait
