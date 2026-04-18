#!/bin/bash
export PATH=$PATH:/usr/local/go/bin

# 1. Setup
echo "Building server and benchmark..."
go build -o kasoku ./cmd/server/main.go
go build -o bench benchmarks/pressure/pressure.go

pkill -f "./kasoku"
sleep 1
rm -rf data/node-*

start_node() {
    id=$1
    port=$2
    addr="http://localhost:$port"
    mkdir -p data/$id
    KASOKU_NODE_ID=$id KASOKU_DATA_DIR=./data/$id KASOKU_HTTP_PORT=$port \
    KASOKU_NODE_ADDR=$addr KASOKU_CONFIG=configs/cluster.yaml \
    ./kasoku > /tmp/kasoku-$id.log 2>&1 &
}

echo "Starting 3-node cluster..."
start_node node-1 9000
sleep 0.5
start_node node-2 9001
sleep 0.5
start_node node-3 9002
sleep 10

run_bench() {
    name=$1
    wrk=$2
    shift 2
    echo "Running $name (Workers: $wrk)..."
    GOMAXPROCS=4 ./bench -nodes=localhost:9000,localhost:9001,localhost:9002 \
            -write-duration=20s -read-duration=20s \
            -warm=5s -workers=$wrk "$@" > "bench_results_$name.txt" 2>&1
}

run_bench "single_node_single_key" 10 -single-node -batch=1
run_bench "single_node_batch" 10 -single-node -batch=50
run_bench "cluster_single_key" 10 -batch=1
run_bench "cluster_batch" 10 -batch=50

pkill -f "./kasoku"
echo "Benchmarks completed."
