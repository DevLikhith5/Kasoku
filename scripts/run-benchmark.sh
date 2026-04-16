#!/usr/bin/env bash
set -e

# Kasoku Benchmark Runner
# Usage: ./run-benchmark.sh [single|cluster]

MODE="${1:-single}"

echo "=================================="
echo "   Kasoku Benchmark Runner"
echo "=================================="

# Build benchmark tool
echo "[1/3] Building pressure tool..."
go build -o benchmarks/pressure ./benchmarks/pressure/pressure.go

# Ensure server is running
echo "[2/3] Checking if server is running..."
if ! curl -sf http://localhost:9000/health > /dev/null 2>&1; then
    echo "Server not running. Starting single node..."
    ./scripts/start-single.sh &
    sleep 3
fi

# Run benchmark
echo "[3/3] Running $MODE benchmark..."
echo ""

NODES="localhost:9000"
if [ "$MODE" = "cluster" ]; then
    NODES="localhost:9000,localhost:9001,localhost:9002"
fi

./benchmarks/pressure/pressure -nodes=$NODES -write-duration=20s -read-duration=20s -workers=30 -batch=25

echo ""
echo "=================================="
echo "  Benchmark complete!"
echo "=================================="
