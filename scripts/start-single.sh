#!/usr/bin/env bash
set -e

echo "=================================="
echo "   Kasoku Single Node Starter"
echo "=================================="

# Build
echo "[1/3] Building kasoku..."
go build -o kasoku ./cmd/server/main.go

# Clean data
echo "[2/3] Cleaning old data..."
rm -rf data/single
mkdir -p data/single

# Kill old processes
pkill -f "kasoku" 2>/dev/null || true
pkill -f "vite" 2>/dev/null || true
sleep 1

# Start
echo "[3/3] Starting single node..."
KASOKU_CONFIG=configs/single.yaml ./kasoku > /tmp/kasoku-single.log 2>&1 &
sleep 2

# Verify
if curl -sf http://localhost:9000/health > /dev/null 2>&1; then
    echo ""
    echo "=================================="
    echo "  ✓ Single node running at http://localhost:9000"
    echo "  Log: /tmp/kasoku-single.log"
    echo "=================================="
    echo ""
    echo "Press Ctrl+C to stop."
else
    echo "✗ Node failed to start. Check /tmp/kasoku-single.log"
fi

trap 'pkill -f "kasoku"; echo "Stopped."' SIGINT SIGTERM
wait
