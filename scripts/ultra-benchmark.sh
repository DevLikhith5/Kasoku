#!/bin/bash
# Ultra High Production Ready Benchmark Suite

set -euo pipefail

KASOKU_DIR="/Users/cvlikhith/kasoku"
DATA_DIR="$KASOKU_DIR/data"

log() { echo "[$(date '+%H:%M:%S')] $1"; }

cleanup() {
    pkill -9 -f "kas" 2>/dev/null || true
    sleep 2
}

fresh_start() {
    pkill -9 -f "kas" 2>/dev/null || true
    sleep 1
    rm -rf "$DATA_DIR"/node*
    mkdir -p "$DATA_DIR"/node1 "$DATA_DIR"/node2 "$DATA_DIR"/node3
}

write_config() {
    local q=$1 r=$2
    cat > "$KASOKU_DIR/configs/cluster-node1.yaml" <<EOF
# Kasoku Cluster Node 1 Configuration
data_dir: ./data/node1
port: 9000
http_port: 9000
grpc_port: 9100
log_level: error
lsm:
    levels: 5
    level_ratio: 10
    l0_base_size: 67108864
    key_cache_size: 1000000
compaction:
    threshold: 2000
    max_concurrent: 2
    l0_size_threshold: 21474836480
memory:
    memtable_size: 134217728
    max_memtable_bytes: 268435456
    bloom_fp_rate: 0.01
    block_cache_size: 67108864
wal:
    sync: false
    sync_interval: 100ms
    checkpoint_bytes: 67108864
    max_file_size: 67108864
cluster:
    enabled: true
    node_id: node-1
    node_addr: http://localhost:9000
    peers:
        - http://localhost:9000
        - http://localhost:9001
        - http://localhost:9002
    replication_factor: 3
    quorum_size: $q
    read_quorum: $r
    vnodes: 150
    rpc_timeout_ms: 500
EOF

    cat > "$KASOKU_DIR/configs/cluster-node2.yaml" <<EOF
# Kasoku Cluster Node 2 Configuration
data_dir: ./data/node2
port: 9001
http_port: 9001
grpc_port: 9101
log_level: error
lsm:
    levels: 5
    level_ratio: 10
    l0_base_size: 67108864
    key_cache_size: 1000000
compaction:
    threshold: 1000
    max_concurrent: 1
    l0_size_threshold: 10737418240
memory:
    memtable_size: 134217728
    max_memtable_bytes: 268435456
    bloom_fp_rate: 0.01
    block_cache_size: 67108864
wal:
    sync: false
    sync_interval: 100ms
    checkpoint_bytes: 67108864
    max_file_size: 67108864
cluster:
    enabled: true
    node_id: node-2
    node_addr: http://localhost:9001
    peers:
        - http://localhost:9000
        - http://localhost:9001
        - http://localhost:9002
    replication_factor: 3
    quorum_size: $q
    read_quorum: $r
    vnodes: 150
    rpc_timeout_ms: 500
EOF

    cat > "$KASOKU_DIR/configs/cluster-node3.yaml" <<EOF
# Kasoku Cluster Node 3 Configuration
data_dir: ./data/node3
port: 9002
http_port: 9002
grpc_port: 9102
log_level: error
lsm:
    levels: 5
    level_ratio: 10
    l0_base_size: 67108864
    key_cache_size: 1000000
compaction:
    threshold: 1000
    max_concurrent: 1
    l0_size_threshold: 10737418240
memory:
    memtable_size: 134217728
    max_memtable_bytes: 268435456
    bloom_fp_rate: 0.01
    block_cache_size: 67108864
wal:
    sync: false
    sync_interval: 100ms
    checkpoint_bytes: 67108864
    max_file_size: 67108864
cluster:
    enabled: true
    node_id: node-3
    node_addr: http://localhost:9002
    peers:
        - http://localhost:9000
        - http://localhost:9001
        - http://localhost:9002
    replication_factor: 3
    quorum_size: $q
    read_quorum: $r
    vnodes: 150
    rpc_timeout_ms: 500
EOF
}

start_nodes() {
    go build -o /tmp/kas "$KASOKU_DIR/cmd/server" 2>&1
    go build -o /tmp/bench "$KASOKU_DIR/cmd/grpc-bench" 2>&1
    
    /tmp/kas --config "$KASOKU_DIR/configs/cluster-node1.yaml" > /tmp/k1.log 2>&1 &
    /tmp/kas --config "$KASOKU_DIR/configs/cluster-node2.yaml" > /tmp/k2.log 2>&1 &
    /tmp/kas --config "$KASOKU_DIR/configs/cluster-node3.yaml" > /tmp/k3.log 2>&1 &
    sleep 5
    
    curl -s http://localhost:9000/health > /dev/null || { log "Node1 failed"; cat /tmp/k1.log; exit 1; }
    curl -s http://localhost:9001/health > /dev/null || { log "Node2 failed"; exit 1; }
    curl -s http://localhost:9002/health > /dev/null || { log "Node3 failed"; exit 1; }
    log "All nodes healthy"
}

verify_repl() {
    k1=$(curl -s http://localhost:9000/api/v1/stats | jq -r '.data.keyspace.keys // 0')
    k2=$(curl -s http://localhost:9001/api/v1/stats | jq -r '.data.keyspace.keys // 0')
    k3=$(curl -s http://localhost:9002/api/v1/stats | jq -r '.data.keyspace.keys // 0')
    if [[ "$k1" == "$k2" && "$k2" == "$k3" ]]; then
        echo "✓ Repl OK (keys=$k1)"
    else
        echo "✗ MISMATCH: $k1 $k2 $k3"
    fi
}

run_test() {
    local wl=$1
    /tmp/bench -workload "$wl" 2>&1 | grep "Total:" | tail -1 | awk '{print $2}'
}

# === MAIN ===
log "=== ULTRA PRODUCTION BENCHMARK ==="
cleanup

echo ""
echo "======================================================================"
echo "  QUORUM BENCHMARK MATRIX"
echo "======================================================================"
echo "| Quorum | W | R | Workload A | Workload B | Workload C |"
echo "|--------|---|---|-------------|------------|------------|"

# W1-R1
write_config 1 1
fresh_start
start_nodes
a=$(run_test A)
b=$(run_test B)
c=$(run_test C)
printf "| W1-R1  | 1 | 1 | %10s | %10s | %10s |\n" "$a" "$b" "$c"
verify_repl
cleanup

# W2-R1
write_config 2 1
fresh_start
start_nodes
a=$(run_test A)
b=$(run_test B)
c=$(run_test C)
printf "| W2-R1  | 2 | 1 | %10s | %10s | %10s |\n" "$a" "$b" "$c"
verify_repl
cleanup

# W1-R2
write_config 1 2
fresh_start
start_nodes
a=$(run_test A)
b=$(run_test B)
c=$(run_test C)
printf "| W1-R2  | 1 | 2 | %10s | %10s | %10s |\n" "$a" "$b" "$c"
verify_repl
cleanup

# W2-R2
write_config 2 2
fresh_start
start_nodes
a=$(run_test A)
b=$(run_test B)
c=$(run_test C)
printf "| W2-R2  | 2 | 2 | %10s | %10s | %10s |\n" "$a" "$b" "$c"
verify_repl
cleanup

echo ""
echo "======================================================================"
echo "  LATENCY ANALYSIS (W2-R2 Strong)"
echo "======================================================================"
write_config 2 2
fresh_start
start_nodes
echo "Workload A:"
/tmp/bench -workload A 2>&1 | grep "p50=" | head -1
/tmp/bench -workload A 2>&1 | grep "p99=" | head -1
echo "Workload B:"
/tmp/bench -workload B 2>&1 | grep "p50=" | head -1
/tmp/bench -workload B 2>&1 | grep "p99=" | head -1
echo "Workload C:"
/tmp/bench -workload C 2>&1 | grep "p50=" | head -1
/tmp/bench -workload C 2>&1 | grep "p99=" | head -1
cleanup

echo ""
echo "======================================================================"
echo "  FAILOVER TEST"
echo "======================================================================"
write_config 2 2
fresh_start
start_nodes
log "Running workload then killing node2..."
/tmp/bench -workload A > /dev/null 2>&1 &
sleep 3
pkill -f "cluster-node2.yaml"
sleep 2
echo "Write during node2 down: $(/tmp/bench -workload A 2>&1 | grep Total | tail -1)"
/tmp/kas --config "$KASOKU_DIR/configs/cluster-node2.yaml" > /tmp/k2.log 2>&1 &
sleep 5
verify_repl

echo ""
echo "======================================================================"
echo "  DONE"
echo "======================================================================"