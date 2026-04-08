#!/usr/bin/env bash
set -e

echo "==========================="
echo "   kasoku Launcher      "
echo "==========================="

# 1. Start Prometheus & Grafana
echo "[1/3] Spinning up monitoring stack via Docker Compose..."
cd deploy/monitoring
docker compose up -d
cd ../..

# Ensure everything tears down gracefully on Ctrl+C
cleanup() {
    echo ""
    echo "Caught signal: Shutting down Kasoku node..."
    kill $KASOKU_PID 2>/dev/null || true
    
    echo "Tearing down Docker containers..."
    cd deploy/monitoring
    docker compose down
    
    echo "Goodbye!"
    exit 0
}
trap cleanup SIGINT SIGTERM

# 2. Build Go application
echo "[2/3] Compiling kasoku binary..."
go build -o kasoku ./cmd/server/main.go

# 3. Start Native Node 
# Note: KASOKU_PORT=8080 matches what is defined in prometheus.yml
echo "[3/3] Starting Kasoku native node..."

KASOKU_PORT=8080 ./kasoku &
KASOKU_PID=$!

echo ""
echo "SUCCESS! All services are active."
echo "   - Kasoku API:      http://localhost:8080"
echo "   - Kasoku Metrics:  http://localhost:8080/metrics"
echo "   - Grafana UI:      http://localhost:3000 (log in with admin / admin)"
echo ""
echo "Press Ctrl+C to stop the node and cleanly tear down Docker."


wait $KASOKU_PID
