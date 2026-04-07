#!/bin/bash
# Kasoku 3-Node Replication Demo
# Starts 3 separate processes, writes to one, reads from all

set -e

DEMO_DIR="/tmp/kasoku-demo"
rm -rf "$DEMO_DIR"
mkdir -p "$DEMO_DIR/node-1" "$DEMO_DIR/node-2" "$DEMO_DIR/node-3"

echo "=== Kasoku 3-Node Replication Demo ==="
echo ""

# Create minimal config for each node
for i in 1 2 3; do
  cat > "$DEMO_DIR/node-$i.yaml" <<EOF
data_dir: $DEMO_DIR/node-$i/data
port: 1808$i
cluster:
  enabled: true
  node_id: node-$i
  node_addr: http://localhost:1808$i
  peers:
$(for j in 1 2 3; do [ "$j" != "$i" ] && echo "    - http://localhost:1808$j"; done)
  replication_factor: 3
  quorum_size: 2
  vnodes: 150
  rpc_timeout_ms: 5000
EOF
done

# Start 3 nodes
echo "Starting node-1 on :18081..."
KASOKU_CONFIG="$DEMO_DIR/node-1.yaml" go run cmd/server/main.go &
PID1=$!

echo "Starting node-2 on :18082..."
KASOKU_CONFIG="$DEMO_DIR/node-2.yaml" go run cmd/server/main.go &
PID2=$!

echo "Starting node-3 on :18083..."
KASOKU_CONFIG="$DEMO_DIR/node-3.yaml" go run cmd/server/main.go &
PID3=$!

cleanup() {
  echo ""
  echo "Stopping all nodes..."
  kill $PID1 $PID2 $PID3 2>/dev/null || true
  rm -rf "$DEMO_DIR"
  exit 0
}
trap cleanup SIGINT SIGTERM

echo ""
echo "Waiting for servers..."
sleep 3

NODES=("http://localhost:18081" "http://localhost:18082" "http://localhost:18083")

# DEMO 1
echo ""
echo "--- Demo 1: Write user:1=Alice to node-1 ---"
curl -s -X POST "${NODES[0]}/api/v1/put/user:1" \
  -H "Content-Type: application/json" \
  -d '{"value":"Alice"}' > /dev/null 2>&1 || true
sleep 1

for i in 0 1 2; do
  resp=$(curl -s "${NODES[$i]}/api/v1/get/user:1" 2>/dev/null || echo '{"error":"unreachable"}')
  val=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('value','MISSING'))" 2>/dev/null || echo "ERROR")
  if [ "$val" = "Alice" ]; then
    echo "✅ Node $((i+1)) has user:1 = $val"
  else
    echo "❌ Node $((i+1)) missing user:1 (got: $val)"
  fi
done

# DEMO 2
echo ""
echo "--- Demo 2: Write order:100 to node-2 ---"
curl -s -X POST "${NODES[1]}/api/v1/put/order:100" \
  -H "Content-Type: application/json" \
  -d '{"value":"ORD-ABC"}' > /dev/null 2>&1 || true
sleep 1

for i in 0 1 2; do
  resp=$(curl -s "${NODES[$i]}/api/v1/get/order:100" 2>/dev/null || echo '{"error":"unreachable"}')
  val=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('value','MISSING'))" 2>/dev/null || echo "ERROR")
  if [ "$val" = "ORD-ABC" ]; then
    echo "✅ Node $((i+1)) has order:100 = $val"
  else
    echo "❌ Node $((i+1)) missing order:100 (got: $val)"
  fi
done

# DEMO 3
echo ""
echo "--- Demo 3: Delete user:1 via node-1 ---"
curl -s -X DELETE "${NODES[0]}/api/v1/delete/user:1" > /dev/null 2>&1 || true
sleep 1

for i in 0 1 2; do
  status=$(curl -s -o /dev/null -w "%{http_code}" "${NODES[$i]}/api/v1/get/user:1" 2>/dev/null || echo "000")
  if [ "$status" = "404" ]; then
    echo "✅ Node $((i+1)): user:1 deleted (404)"
  else
    echo "❌ Node $((i+1)) still has user:1 (status: $status)"
  fi
done

echo ""
echo "=== Demo Complete ==="
echo "Nodes running. Press Ctrl+C to stop."
wait
