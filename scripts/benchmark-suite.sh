#!/bin/bash
# Kasoku Comprehensive Benchmark Suite
# Tests all quorum configurations across all YCSB workloads
#
# Usage: ./scripts/benchmark-suite.sh [output_file]
#
# Example:
#   ./scripts/benchmark-suite.sh          # Prints to stdout
#   ./scripts/benchmark-suite.sh results.md  # Saves to file

set -euo pipefail

KASOKU_DIR="/Users/cvlikhith/kasoku"
BUILD="${KASOKU_DIR}/cmd/grpc-bench/main.go"
OUTPUT_FILE="${1:-}"

QUORUMS=(
  "W1-R1:quorum_size=1:read_quorum=1"
  "W2-R1:quorum_size=2:read_quorum=1"
  "W1-R2:quorum_size=1:read_quorum=2"
  "W2-R2:quorum_size=2:read_quorum=2"
)

WORKLOADS=("A" "B" "C" "D" "E" "F")

# Node config files
NODE1_CONFIG="${KASOKU_DIR}/configs/cluster-node1.yaml"
NODE2_CONFIG="${KASOKU_DIR}/configs/cluster-node2.yaml"
NODE3_CONFIG="${KASOKU_DIR}/configs/cluster-node3.yaml"

cleanup() {
  pkill -9 -f "kasoku" 2>/dev/null || true
  pkill -9 -f "/tmp/kas" 2>/dev/null || true
  sleep 1
}

set_quorum() {
  local qs=$1
  local rq=$2
  for cfg in "$NODE1_CONFIG" "$NODE2_CONFIG" "$NODE3_CONFIG"; do
    sed -i '' "s/quorum_size: [0-9]/quorum_size: $qs/" "$cfg"
    sed -i '' "s/read_quorum: [0-9]/read_quorum: $rq/" "$cfg"
  done
}

start_nodes() {
  rm -rf "${KASOKU_DIR}/data/node1" "${KASOKU_DIR}/data/node2" "${KASOKU_DIR}/data/node3"
  mkdir -p "${KASOKU_DIR}/data/node1" "${KASOKU_DIR}/data/node2" "${KASOKU_DIR}/data/node3"

  /tmp/kas --config "$NODE1_CONFIG" > /tmp/kas1.log 2>&1 &
  /tmp/kas --config "$NODE2_CONFIG" > /tmp/kas2.log 2>&1 &
  /tmp/kas --config "$NODE3_CONFIG" > /tmp/kas3.log 2>&1 &
  sleep 6
}

run_benchmark() {
  local workload=$1
  local result
  result=$(go run "$BUILD" -workload "$workload" 2>&1)
  echo "$result"
}

extract_metrics() {
  local output=$1
  local workload=$2
  local quorum=$3

  local total_ops=$(echo "$output" | grep "Total:" | awk '{print $2}')
  local write_ops=$(echo "$output" | grep "Writes:" | awk '{print $4}')
  local read_ops=$(echo "$output" | grep "Reads:" | awk '{print $4}')
  local write_p50=$(echo "$output" | grep "Write latency" | grep -o 'p50=[^,]*' | cut -d= -f2 || echo "N/A")
  local write_p99=$(echo "$output" | grep "Write latency" | grep -o 'p99=[^,]*' | cut -d= -f2 || echo "N/A")
  local read_p50=$(echo "$output" | grep "Read latency" | grep -o 'p50=[^,]*' | cut -d= -f2 || echo "N/A")
  local read_p99=$(echo "$output" | grep "Read latency" | grep -o 'p99=[^,]*' | cut -d= -f2 || echo "N/A")

  echo "$workload,$quorum,$total_ops,$write_ops,$read_ops,$write_p50,$write_p99,$read_p50,$read_p99"
}

print_separator() {
  printf "+--------+--------+----------+----------+----------+----------+----------+----------+----------+\n"
}

print_header() {
  print_separator
  printf "| %-6s | %-6s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s |\n" \
    "Work" "Quorum" "Total" "Writes" "Reads" "W-p50" "W-p99" "R-p50" "R-p99"
  printf "| %-6s | %-6s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s |\n" \
    "load" "" "ops/sec" "ops/sec" "ops/sec" "ms" "ms" "ms" "ms"
  print_separator
}

print_row() {
  local workload=$1
  local quorum=$2
  local total=$3
  local writes=$4
  local reads=$5
  local wp50=$6
  local wp99=$7
  local rp50=$8
  local rp99=$9
  printf "| %-6s | %-6s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s |\n" \
    "$workload" "$quorum" "$total" "$writes" "$reads" "$wp50" "$wp99" "$rp50" "$rp99"
}

# Build server
echo "Building Kasoku server..."
cd "$KASOKU_DIR"
go build -o /tmp/kas cmd/server/main.go
echo ""

if [ -n "$OUTPUT_FILE" ]; then
  exec > >(tee "$OUTPUT_FILE") 2>&1
fi

echo "======================================================================"
echo "  Kasoku Comprehensive Benchmark Suite"
echo "  3-Node Cluster | 6 YCSB Workloads | 4 Quorum Configurations"
echo "======================================================================"
echo ""

echo "Quorum configurations under test:"
for q in "${QUORUMS[@]}"; do
  label=$(echo "$q" | cut -d: -f1)
  qs=$(echo "$q" | cut -d: -f2 | cut -d= -f2)
  rq=$(echo "$q" | cut -d: -f3 | cut -d= -f2)
  echo "  $label  ->  W=$qs, R=$rq  (W+R > N = $((qs + rq)) > 3 = $([[ $((qs + rq)) -gt 3 ]] && echo "STRONG" || echo "WEAK"))"
done
echo ""

echo "YCSB Workloads:"
echo "  A  - 50% reads, 50% writes (balanced)"
echo "  B  - 95% reads, 5% writes (read-heavy)"
echo "  C  - 100% reads (read-only)"
echo "  D  - 95% reads, 5% inserts (latest data)"
echo "  E  - Range scans"
echo "  F  - Read-modify-write"
echo ""

# Collect all results
results=()

for q in "${QUORUMS[@]}"; do
  label=$(echo "$q" | cut -d: -f1)
  qs=$(echo "$q" | cut -d: -f2 | cut -d= -f2)
  rq=$(echo "$q" | cut -d: -f3 | cut -d= -f2)

  echo "------------------------------------------------------------"
  echo "  Quorum: $label (W=$qs, R=$rq)"
  echo "------------------------------------------------------------"

  set_quorum "$qs" "$rq"
  cleanup
  start_nodes

  for wl in "${WORKLOADS[@]}"; do
    echo "  Running workload $wl..."
    result=$(run_benchmark "$wl")
    metrics=$(extract_metrics "$result" "$wl" "$label")
    results+=("$metrics")
  done

  cleanup
  echo ""
done

echo ""
echo "======================================================================"
echo "  RESULTS"
echo "======================================================================"
echo ""
print_header

for r in "${results[@]}"; do
  IFS=',' read -ra cols <<< "$r"
  print_row "${cols[0]}" "${cols[1]}" "${cols[2]}" "${cols[3]}" "${cols[4]}" "${cols[5]}" "${cols[6]}" "${cols[7]}" "${cols[8]}"
done

print_separator

echo ""
echo "======================================================================"
echo "  INTERPRETATION"
echo "======================================================================"
echo ""
echo "  W+R > N = STRONG consistency (read your writes)"
echo "  W+R <= N = WEAK consistency (eventual)"
echo ""
echo "  For real production deployments, W=2, R=2 is recommended."
echo "  This provides full read-after-write consistency at the cost"
echo "  of approximately 30-50% throughput reduction."
echo ""