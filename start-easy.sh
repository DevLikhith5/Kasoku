#!/usr/bin/env bash
set -e

# Kasoku Easy Start Script
# Usage: 
#   ./start-easy.sh           - 3-node cluster (default)
#   ./start-easy.sh single  - single node
#   ./start-easy.sh api     - API server only
#   ./start-easy.sh web     - API + web frontend

MODE="${1:-cluster}"

echo "=================================="
echo "   Kasoku Easy Start "
echo "=================================="

# Build
echo "[1/4] Building kasoku..."
go build -o kasoku ./cmd/server/main.go 2>/dev/null || true

# Clean data
echo "[2/4] Cleaning old data..."
rm -rf data/node-1 data/node-2 data/node-3
mkdir -p data/node-1 data/node-2 data/node-3
rm -f data/VERSION*

# Kill old processes
pkill -f "kasoku" 2>/dev/null || true
pkill -f "vite" 2>/dev/null || true
sleep 1

# === MODE: single ===
if [ "$MODE" = "single" ]; then
    echo "[3/4] Starting single node..."
    KASOKU_CONFIG=kasoku-single.yaml ./kasoku >/dev/null 2>&1 &
    sleep 2
    curl -sf http://localhost:9000/health && echo "✓ Running at http://localhost:9000"
    echo ""
    echo "Single node running. Press Ctrl+C to stop."
    trap 'pkill -f "kasoku"' SIGINT SIGTERM
    wait
fi

# === MODE: api ===
if [ "$MODE" = "api" ]; then
    echo "[3/4] Starting API only..."
    KASOKU_CONFIG=kasoku-single.yaml ./kasoku >/dev/null 2>&1 &
    sleep 2
    curl -sf http://localhost:9000/health && echo "✓ API running at http://localhost:9000"
    echo ""
    echo "API server running. Press Ctrl+C to stop."
    trap 'pkill -f "kasoku"' SIGINT SIGTERM
    wait
fi

# === MODE: web ===
if [ "$MODE" = "web" ]; then
    echo "[3/4] Starting API + frontend..."
    KASOKU_CONFIG=kasoku-single.yaml ./kasoku >/dev/null 2>&1 &
    sleep 2
    
    cd web
    npm run dev >/dev/null 2>&1 &
    cd ..
    
    sleep 3
    echo ""
    echo "=================================="
    echo "Running:"
    echo "  Frontend: http://localhost:5173"
    echo "  API:     http://localhost:9000"
    echo "=================================="
    echo ""
    echo "Press Ctrl+C to stop all."
    trap 'pkill -f "kasoku"; pkill -f "vite"' SIGINT SIGTERM
    wait
fi

# === MODE: cluster (default) ===
if [ "$MODE" = "cluster" ] || [ -z "$MODE" ]; then
    echo "[3/4] Starting 3-node cluster..."
    
    # Node 1
    cat > /tmp/kasoku-1.yaml <<EOF
data_dir: ./data/node-1
port: 9000
http_port: 9000
log_level: error
cluster:
    enabled: true
    node_id: node-1
    node_addr: http://localhost:9000
    peers:
        - http://localhost:9001
        - http://localhost:9002
    replication_factor: 3
    quorum_size: 2
EOF

    # Node 2
    cat > /tmp/kasoku-2.yaml <<EOF
data_dir: ./data/node-2
port: 9001
http_port: 9001
log_level: error
cluster:
    enabled: true
    node_id: node-2
    node_addr: http://localhost:9001
    peers:
        - http://localhost:9000
        - http://localhost:9002
    replication_factor: 3
    quorum_size: 2
EOF

    # Node 3
    cat > /tmp/kasoku-3.yaml <<EOF
data_dir: ./data/node-3
port: 9002
http_port: 9002
log_level: error
cluster:
    enabled: true
    node_id: node-3
    node_addr: http://localhost:9002
    peers:
        - http://localhost:9000
        - http://localhost:9001
    replication_factor: 3
    quorum_size: 2
EOF

    KASOKU_CONFIG=/tmp/kasoku-1.yaml ./kasoku > /dev/null 2>&1 &
    sleep 0.5
    KASOKU_CONFIG=/tmp/kasoku-2.yaml ./kasoku > /dev/null 2>&1 &
    sleep 0.5
    KASOKU_CONFIG=/tmp/kasoku-3.yaml ./kasoku > /dev/null 2>&1 &
    
    sleep 4
    
    # Verify
    echo ""
    for port in 9000 9001 9002; do
        if curl -sf http://localhost:$port/health > /dev/null 2>&1; then
            echo "  ✓ Node $port OK"
        else
            echo "  ✗ Node $port failed"
        fi
    done
    
    echo ""
    echo "=================================="
    echo "Cluster running!"
    echo "  http://localhost:9000"
    echo "  http://localhost:9001"
    echo "  http://localhost:9002"
    echo ""
    echo "To also run web dashboard:"
    echo "  cd web && npm run dev"
    echo ""
    echo "Press Ctrl+C to stop."
    echo "=================================="
    
    trap 'pkill -f "kasoku"; echo "Stopped."' SIGINT SIGTERM
    wait
fi