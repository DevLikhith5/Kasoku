#!/usr/bin/env bash
set -e

# Kasoku Benchmark Script
# Usage: ./scripts/benchmark.sh [single|cluster] [batch-size]

MODE="${1:-single}"
BATCH="${2:-50}"
WORKERS=30
WRITE_DUR=20s
READ_DUR=20s

echo "=========================================="
echo "   Kasoku Benchmark Runner"
echo "=========================================="
echo "Mode: $MODE"
echo "Batch: $BATCH"
echo "Workers: $WORKERS"
echo ""

# Function to start single node
start_single() {
    echo "[1/3] Starting single node..."
    rm -f kasoku
    go build -o kasoku ./cmd/server
    
    # Use existing data if available, don't delete
    if [ ! -d "data/single" ]; then
        mkdir -p data/single
    fi
    
    KASOKU_CONFIG=configs/single.yaml ./kasoku > /tmp/kasoku.log 2>&1 &
    sleep 3
    
    if curl -sf localhost:9000/health > /dev/null 2>&1; then
        echo "   ✓ Node running at http://localhost:9000"
    else
        echo "   ✗ Failed to start"
        exit 1
    fi
}

# Function to start cluster
start_cluster() {
    echo "[1/3] Starting 3-node cluster..."
    rm -f kasoku
    go build -o kasoku ./cmd/server
    
    for port in 9000 9001 9002; do
        if ! curl -sf localhost:$port/health > /dev/null 2>&1; then
            KASOKU_CONFIG=configs/cluster-node$((port-9000+1)).yaml ./kasoku > /tmp/kasoku-$port.log 2>&1 &
        fi
    done
    
    sleep 4
    
    for port in 9000 9001 9002; do
        if curl -sf localhost:$port/health > /dev/null 2>&1; then
            echo "   ✓ Node running at http://localhost:$port"
        else
            echo "   ✗ Node $port failed"
            exit 1
        fi
    done
}

# Stop all nodes
stop_all() {
    pkill -f "kasoku" 2>/dev/null || true
    sleep 1
}

# Main
case "$MODE" in
    single)
        stop_all
        start_single
        NODES="localhost:9000"
        ;;
    cluster)
        stop_all
        start_cluster
        NODES="localhost:9000,localhost:9001,localhost:9002"
        ;;
    stop)
        stop_all
        echo "All nodes stopped"
        exit 0
        ;;
    *)
        echo "Usage: $0 [single|cluster|stop] [batch-size]"
        echo "Examples:"
        echo "  $0 single          # Single node benchmark"
        echo "  $0 cluster        # 3-node cluster benchmark"
        echo "  $0 single 25      # Single node with batch=25"
        echo "  $0 stop           # Stop all nodes"
        exit 1
        ;;
esac

echo ""
echo "[2/3] Running benchmark..."
echo "Config: -nodes=$NODES -workers=$WORKERS -batch=$BATCH"
echo ""

# Run benchmark
go run ./benchmarks/pressure/pressure.go \
    -nodes=$NODES \
    -workers=$WORKERS \
    -batch=$BATCH \
    -write-duration=$WRITE_DUR \
    -read-duration=$READ_DUR \
    -warm=5s

echo ""
echo "[3/3] Complete!"
echo ""
echo "Results saved. To stop servers: $0 stop"