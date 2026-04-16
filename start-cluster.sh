#!/usr/bin/env bash
set -e

echo "=================================="
echo "   Kasoku 3-Node Cluster Launcher "
echo "=================================="

# Rebuild binary
echo "[1/4] Compiling kasoku binary..."
export PATH=$PATH:/usr/local/go/bin:/opt/homebrew/bin
go build -o kasoku ./cmd/server/main.go
echo "      Build OK"

# Kill old processes
echo "[2/4] Preparing data directories..."
pkill -f "./kasoku" 2>/dev/null || true
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

KASOKU_CONFIG=kasoku-node1.yaml ./kasoku > /tmp/kasoku-node1.log 2>&1 &
PIDS+=($!)
sleep 0.5

KASOKU_CONFIG=kasoku-node2.yaml ./kasoku > /tmp/kasoku-node2.log 2>&1 &
PIDS+=($!)
sleep 0.5

KASOKU_CONFIG=kasoku-node3.yaml ./kasoku > /tmp/kasoku-node3.log 2>&1 &
PIDS+=($!)
sleep 3

echo ""
echo "[4/4] Verifying nodes are up..."
for port in 9000 9001 9002; do
    if curl -sf http://localhost:$port/health > /dev/null 2>&1; then
        echo "      ✓ Node on port $port is healthy"
    else
        echo "      ✗ Node on port $port not responding (check /tmp/kasoku-node*.log)"
    fi
done

echo ""
echo "=================================="
echo "  Cluster is running!"
echo "  Node 1 (node-1): http://localhost:9000"
echo "  Node 2 (node-2): http://localhost:9001"
echo "  Node 3 (node-3): http://localhost:9002"
echo ""
echo "  Logs: /tmp/kasoku-node{1,2,3}.log"
echo ""
echo "  Press Ctrl+C to stop all nodes."
echo "=================================="

# Wait for all background jobs
wait
