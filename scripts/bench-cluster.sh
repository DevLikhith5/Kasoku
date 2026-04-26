#!/bin/bash
set -e

echo "=== Cleanup ==="
pkill -9 -f "kas" 2>/dev/null || true
killall -9 kas 2>/dev/null || true
sleep 8

cd /Users/cvlikhith/kasoku

echo "=== Build ==="
go build -o /tmp/kas cmd/server/main.go

echo "=== Clean data ==="
rm -rf data && mkdir -p data

echo "=== Start cluster ==="
/tmp/kas --config configs/cluster-node1.yaml &
pid1=$!
echo "Node 1 PID: $pid1"

sleep 2

/tmp/kas --config configs/cluster-node2.yaml &
pid2=$!
echo "Node 2 PID: $pid2"

sleep 2

/tmp/kas --config configs/cluster-node3.yaml &
pid3=$!
echo "Node 3 PID: $pid3"

sleep 35

echo "=== Check health ==="
curl -s http://localhost:9000/health && echo ""
curl -s http://localhost:9001/health && echo ""
curl -s http://localhost:9002/health && echo ""

echo "=== Run benchmark ==="
go run cmd/grpc-bench/main.go 2>&1

echo "=== Cleanup ==="
kill $pid1 $pid2 $pid3 2>/dev/null || true
pkill -f "kas" 2>/dev/null || true