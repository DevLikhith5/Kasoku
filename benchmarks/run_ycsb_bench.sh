#!/bin/bash
# ============================================================================
# Kasoku YCSB-Standard Benchmark Suite
# Runs YCSB workloads (A–F) against a 3-node cluster
#
# Usage:
#   ./benchmarks/run_ycsb_bench.sh              # default (A,B,C,D,F cluster)
#   ./benchmarks/run_ycsb_bench.sh --workload b  # single workload
#   ./benchmarks/run_ycsb_bench.sh --single      # single-node only
#   ./benchmarks/run_ycsb_bench.sh --all         # includes scan workload E
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

# ── Colors ───────────────────────────────────────────────────────────────────
BOLD='\033[1m'
GREEN='\033[32m'
CYAN='\033[36m'
YELLOW='\033[33m'
RED='\033[31m'
DIM='\033[2m'
RESET='\033[0m'

# ── Configuration ────────────────────────────────────────────────────────────
RECORD_COUNT=1000000
FIELD_LENGTH=100
WORKERS=50
BATCH=1
DURATION=30
WORKLOADS="a,b,c,d,f"
RUN_SINGLE=false
RUN_CLUSTER=true
RESULTS_DIR="$ROOT_DIR/benchmarks/results"

# ── Parse Args ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --cluster)   RUN_SINGLE=false; RUN_CLUSTER=true; shift ;;
        --single)    RUN_SINGLE=true; RUN_CLUSTER=false; shift ;;
        --both)      RUN_SINGLE=true; RUN_CLUSTER=true; shift ;;
        --workload)  WORKLOADS="$2"; shift 2 ;;
        --workers)   WORKERS="$2"; shift 2 ;;
        --records)   RECORD_COUNT="$2"; shift 2 ;;
        --batch)     BATCH="$2"; shift 2 ;;
        --dur)       DURATION="$2"; shift 2 ;;
        --all)       WORKLOADS="a,b,c,d,e,f"; shift ;;
        --help)
            echo "Usage: $0 [--cluster|--single|--both] [--workload a,b,c] [--workers N] [--records N] [--batch N] [--dur N] [--all]"
            exit 0 ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

mkdir -p "$RESULTS_DIR"

banner() {
    echo ""
    echo -e "${BOLD}${YELLOW}╔══════════════════════════════════════════════════════════════════╗${RESET}"
    echo -e "${BOLD}${YELLOW}║  $1${RESET}"
    echo -e "${BOLD}${YELLOW}╚══════════════════════════════════════════════════════════════════╝${RESET}"
    echo ""
}

cleanup_all() {
    echo -e "${DIM}Cleaning up processes...${RESET}"
    pkill -f "kasoku-server" 2>/dev/null || true
    sleep 1
}

trap cleanup_all EXIT

# ── Build ────────────────────────────────────────────────────────────────────
banner "KASOKU YCSB BENCHMARK SUITE"

echo -e "${CYAN}Building...${RESET}"
go build -o kasoku-server ./cmd/server/
go build -o ycsb-bench ./cmd/ycsb/
echo -e "${GREEN}✓ Built kasoku-server + ycsb-bench${RESET}"

echo ""
echo -e "  Record Count:  ${BOLD}${RECORD_COUNT}${RESET}"
echo -e "  Field Length:   ${BOLD}${FIELD_LENGTH} bytes${RESET}"
echo -e "  Workers:        ${BOLD}${WORKERS}${RESET}"
echo -e "  Batch Size:     ${BOLD}${BATCH}${RESET}"
echo -e "  Duration:       ${BOLD}${DURATION}s per workload${RESET}"
echo -e "  Workloads:      ${BOLD}${WORKLOADS}${RESET}"

# ── Helper: start & run one workload ─────────────────────────────────────────
run_cluster_workload() {
    local wl="$1"
    
    # Kill any leftover servers and clean data
    pkill -f "kasoku-server" 2>/dev/null || true
    sleep 1
    rm -rf data/bench-node1 data/bench-node2 data/bench-node3

    # Start fresh 3-node cluster
    ./kasoku-server --config configs/bench-cluster-node1.yaml &>/dev/null &
    local PID1=$!
    ./kasoku-server --config configs/bench-cluster-node2.yaml &>/dev/null &
    local PID2=$!
    ./kasoku-server --config configs/bench-cluster-node3.yaml &>/dev/null &
    local PID3=$!
    sleep 2

    # Quick health check
    local ok=true
    for port in 9001 9011 9021; do
        if ! curl -s "http://localhost:$port/health" > /dev/null 2>&1; then
            sleep 2
            if ! curl -s "http://localhost:$port/health" > /dev/null 2>&1; then
                echo -e "  ${RED}✗ Node :$port FAILED${RESET}"
                ok=false
            fi
        fi
    done

    if [ "$ok" = false ]; then
        echo -e "${RED}Cluster not healthy, skipping workload $wl${RESET}"
        kill $PID1 $PID2 $PID3 2>/dev/null || true
        return
    fi

    ./ycsb-bench \
        -nodes "localhost:9100,localhost:9101,localhost:9102" \
        -workers "$WORKERS" \
        -batch "$BATCH" \
        -recordcount "$RECORD_COUNT" \
        -fieldlength "$FIELD_LENGTH" \
        -dur "$DURATION" \
        -workload "$wl"

    # Kill after workload
    kill $PID1 $PID2 $PID3 2>/dev/null || true
    wait $PID1 $PID2 $PID3 2>/dev/null || true
}

run_single_workload() {
    local wl="$1"
    
    pkill -f "kasoku-server" 2>/dev/null || true
    sleep 1
    rm -rf data/ycsb-single
    mkdir -p data/ycsb-single

    KASOKU_DATA_DIR=./data/ycsb-single \
    KASOKU_CONFIG=./configs/bench-realistic.yaml \
    ./kasoku-server &>/dev/null &
    local SPID=$!
    sleep 2

    if ! curl -s "http://localhost:9001/health" > /dev/null 2>&1; then
        sleep 2
    fi

    ./ycsb-bench \
        -nodes "localhost:9100" \
        -workers "$WORKERS" \
        -batch "$BATCH" \
        -recordcount "$RECORD_COUNT" \
        -fieldlength "$FIELD_LENGTH" \
        -dur "$DURATION" \
        -workload "$wl"

    kill $SPID 2>/dev/null || true
    wait $SPID 2>/dev/null || true
}

# ── Run ──────────────────────────────────────────────────────────────────────
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
IFS=',' read -ra WL_ARRAY <<< "$WORKLOADS"

if [ "$RUN_SINGLE" = true ]; then
    banner "SINGLE-NODE YCSB"
    for wl in "${WL_ARRAY[@]}"; do
        wl=$(echo "$wl" | tr -d ' ')
        echo -e "\n${BOLD}━━━ Workload $wl ━━━${RESET}"
        run_single_workload "$wl" 2>&1 | tee -a "$RESULTS_DIR/single_${TIMESTAMP}.txt"
    done
fi

if [ "$RUN_CLUSTER" = true ]; then
    banner "3-NODE CLUSTER YCSB (RF=3, W=1, R=1)"
    for wl in "${WL_ARRAY[@]}"; do
        wl=$(echo "$wl" | tr -d ' ')
        echo -e "\n${BOLD}━━━ Workload $wl ━━━${RESET}"
        run_cluster_workload "$wl" 2>&1 | tee -a "$RESULTS_DIR/cluster_${TIMESTAMP}.txt"
    done
fi

# ── Summary ──────────────────────────────────────────────────────────────────
banner "BENCHMARK COMPLETE"
echo -e "Results saved to: ${BOLD}$RESULTS_DIR/${RESET}"
ls -lh "$RESULTS_DIR"/*_${TIMESTAMP}* 2>/dev/null || echo "  (no files found)"
