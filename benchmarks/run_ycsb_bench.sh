#!/bin/bash
# ============================================================================
# Kasoku YCSB-Standard Benchmark Runner
# Tests single-node and 3-node cluster with realistic configs
# ============================================================================
set -e

BOLD='\033[1m'
GREEN='\033[32m'
CYAN='\033[36m'
YELLOW='\033[33m'
RED='\033[31m'
RESET='\033[0m'

SEED=2000000          # 2M keys (~200MB, exceeds 16MB cache)
WORKERS=20
DURATION=60           # 60 seconds per workload (YCSB minimum)
BATCH=1               # Per-key operations for honest numbers

banner() {
    echo ""
    echo -e "${BOLD}${YELLOW}╔══════════════════════════════════════════════════════════════╗${RESET}"
    echo -e "${BOLD}${YELLOW}║  $1${RESET}"
    echo -e "${BOLD}${YELLOW}╚══════════════════════════════════════════════════════════════╝${RESET}"
    echo ""
}

cleanup() {
    echo -e "${CYAN}Cleaning up...${RESET}"
    pkill -f kasoku-server 2>/dev/null || true
    sleep 2
    rm -rf /tmp/kasoku-bench-*
}

# Build
banner "BUILDING KASOKU"
go build -o kasoku-server ./cmd/server/
echo -e "${GREEN}✓ Built kasoku-server${RESET}"

# ============================================================================
# PHASE 1: SINGLE NODE
# ============================================================================
banner "PHASE 1: SINGLE NODE BENCHMARK"
echo -e "Config: key_cache=10K, block_cache=16MB, memtable=16MB, WAL=fsync"
echo -e "Dataset: ${SEED} keys × 100B values = ~200MB (>> 16MB cache)"
echo ""

cleanup
mkdir -p /tmp/kasoku-bench-single

echo -e "${CYAN}Starting single-node server...${RESET}"
KASOKU_DATA_DIR=/tmp/kasoku-bench-single KASOKU_CONFIG=./configs/bench-realistic.yaml ./kasoku-server &
SERVER_PID=$!
sleep 3

# YCSB Workload B: 95% read / 5% write (most common production pattern)
echo ""
echo -e "${BOLD}━━━ YCSB Workload B (95% Read / 5% Write) ━━━${RESET}"
go run ./cmd/grpc-bench/main.go \
    -nodes=localhost:9100 \
    -workers=$WORKERS \
    -batch=$BATCH \
    -seed=$SEED \
    -reads=95 \
    -dur=$DURATION 2>&1 | tee /tmp/kasoku-bench-single-B.log

# Kill and restart for fresh state
kill $SERVER_PID 2>/dev/null || true
sleep 2
rm -rf /tmp/kasoku-bench-single
mkdir -p /tmp/kasoku-bench-single

KASOKU_DATA_DIR=/tmp/kasoku-bench-single KASOKU_CONFIG=./configs/bench-realistic.yaml ./kasoku-server &
SERVER_PID=$!
sleep 3

# YCSB Workload A: 50% read / 50% write
echo ""
echo -e "${BOLD}━━━ YCSB Workload A (50% Read / 50% Write) ━━━${RESET}"
go run ./cmd/grpc-bench/main.go \
    -nodes=localhost:9100 \
    -workers=$WORKERS \
    -batch=$BATCH \
    -seed=$SEED \
    -reads=50 \
    -dur=$DURATION 2>&1 | tee /tmp/kasoku-bench-single-A.log

kill $SERVER_PID 2>/dev/null || true
sleep 2

# ============================================================================
# PHASE 2: 3-NODE CLUSTER
# ============================================================================
banner "PHASE 2: 3-NODE CLUSTER BENCHMARK (RF=3, W=2, R=2)"
echo -e "Config: 3 nodes × (key_cache=10K, block_cache=16MB), quorum W=2 R=2"
echo -e "Dataset: ${SEED} keys × 100B values, replicated 3x"
echo ""

cleanup
mkdir -p /tmp/kasoku-bench-node1 /tmp/kasoku-bench-node2 /tmp/kasoku-bench-node3

echo -e "${CYAN}Starting 3-node cluster...${RESET}"
KASOKU_DATA_DIR=/tmp/kasoku-bench-node1 KASOKU_CONFIG=./configs/bench-cluster-node1.yaml ./kasoku-server &
PID1=$!
KASOKU_DATA_DIR=/tmp/kasoku-bench-node2 KASOKU_CONFIG=./configs/bench-cluster-node2.yaml ./kasoku-server &
PID2=$!
KASOKU_DATA_DIR=/tmp/kasoku-bench-node3 KASOKU_CONFIG=./configs/bench-cluster-node3.yaml ./kasoku-server &
PID3=$!
sleep 5
echo -e "${GREEN}✓ All 3 nodes running${RESET}"

# YCSB Workload B: 95/5
echo ""
echo -e "${BOLD}━━━ CLUSTER: YCSB Workload B (95% Read / 5% Write) ━━━${RESET}"
go run ./cmd/grpc-bench/main.go \
    -nodes=localhost:9002,localhost:9012,localhost:9022 \
    -workers=$WORKERS \
    -batch=$BATCH \
    -seed=$SEED \
    -reads=95 \
    -dur=$DURATION 2>&1 | tee /tmp/kasoku-bench-cluster-B.log

# Kill and restart for fresh state
kill $PID1 $PID2 $PID3 2>/dev/null || true
sleep 2
rm -rf /tmp/kasoku-bench-node1 /tmp/kasoku-bench-node2 /tmp/kasoku-bench-node3
mkdir -p /tmp/kasoku-bench-node1 /tmp/kasoku-bench-node2 /tmp/kasoku-bench-node3

KASOKU_DATA_DIR=/tmp/kasoku-bench-node1 KASOKU_CONFIG=./configs/bench-cluster-node1.yaml ./kasoku-server &
PID1=$!
KASOKU_DATA_DIR=/tmp/kasoku-bench-node2 KASOKU_CONFIG=./configs/bench-cluster-node2.yaml ./kasoku-server &
PID2=$!
KASOKU_DATA_DIR=/tmp/kasoku-bench-node3 KASOKU_CONFIG=./configs/bench-cluster-node3.yaml ./kasoku-server &
PID3=$!
sleep 5

# YCSB Workload A: 50/50
echo ""
echo -e "${BOLD}━━━ CLUSTER: YCSB Workload A (50% Read / 50% Write) ━━━${RESET}"
go run ./cmd/grpc-bench/main.go \
    -nodes=localhost:9002,localhost:9012,localhost:9022 \
    -workers=$WORKERS \
    -batch=$BATCH \
    -seed=$SEED \
    -reads=50 \
    -dur=$DURATION 2>&1 | tee /tmp/kasoku-bench-cluster-A.log

# Cleanup
kill $PID1 $PID2 $PID3 2>/dev/null || true

banner "BENCHMARK COMPLETE"
echo -e "Results saved to /tmp/kasoku-bench-*.log"
echo -e "Single-node Workload A: /tmp/kasoku-bench-single-A.log"
echo -e "Single-node Workload B: /tmp/kasoku-bench-single-B.log"
echo -e "Cluster Workload A:     /tmp/kasoku-bench-cluster-A.log"
echo -e "Cluster Workload B:     /tmp/kasoku-bench-cluster-B.log"
