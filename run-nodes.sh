#!/usr/bin/env bash
cd /Users/cvlikhith/kasoku
rm -rf data/node-* && mkdir -p data/node-1 data/node-2 data/node-3
./kasoku -config kasoku-node1.yaml &
./kasoku -config kasoku-node2.yaml &
./kasoku -config kasoku-node3.yaml &
sleep 3
curl -sf http://localhost:9000/health || exit 1
curl -sf http://localhost:9001/health || exit 1
curl -sf http://localhost:9002/health || exit 1
echo "All nodes running"
wait