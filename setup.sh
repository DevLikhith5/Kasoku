#!/bin/bash
set -e

# Kasoku Production Setup Script
# Usage: ./setup.sh [single|cluster|kubernetes]

MODE=${1:-single}

echo "🚀 Setting up Kasoku - $MODE mode"

case $MODE in
    single)
        echo "📦 Starting single-node Kasoku..."
        docker-compose -f docker-compose.single.yml up -d
        echo "✅ Kasoku running at http://localhost:9000"
        echo "   Health check: http://localhost:9000/health"
        echo "   Put key: curl -X PUT http://localhost:9000/kv/mykey -d 'value'"
        echo "   Get key: curl http://localhost:9000/kv/mykey"
        ;;

    cluster)
        echo "📦 Starting 3-node Kasoku cluster..."
        docker-compose -f docker-compose.yml up -d
        echo "✅ Cluster running:"
        echo "   Node 1: http://localhost:9001"
        echo "   Node 2: http://localhost:9002"
        echo "   Node 3: http://localhost:9003"
        echo ""
        echo "   Write to cluster: curl -X PUT http://localhost:9001/kv/mykey -d 'value'"
        echo "   Read from cluster: curl http://localhost:9002/kv/mykey"
        ;;

    cluster-with-monitoring)
        echo "📦 Starting Kasoku cluster with monitoring..."
        docker-compose -f docker-compose.yml --profile monitoring up -d
        echo "✅ Cluster running with monitoring:"
        echo "   Node 1: http://localhost:9001"
        echo "   Node 2: http://localhost:9002"
        echo "   Node 3: http://localhost:9003"
        echo "   Prometheus: http://localhost:9090"
        echo "   Grafana: http://localhost:3000 (admin/admin)"
        ;;

    kubernetes)
        echo "☸️  Deploying to Kubernetes..."
        kubectl apply -f deploy/kubernetes/
        echo "✅ Kasoku deployed to Kubernetes"
        echo "   Check status: kubectl get pods -n kasoku"
        echo "   Access: kubectl port-forward svc/kasoku-http 9000:80"
        ;;

    stop)
        echo "🛑 Stopping Kasoku..."
        docker-compose -f docker-compose.single.yml down 2>/dev/null || true
        docker-compose -f docker-compose.yml down 2>/dev/null || true
        echo "✅ Stopped"
        ;;

    clean)
        echo "🧹 Cleaning up..."
        docker-compose -f docker-compose.single.yml down -v 2>/dev/null || true
        docker-compose -f docker-compose.yml down -v 2>/dev/null || true
        echo "✅ Cleaned up"
        ;;

    *)
        echo "Usage: ./setup.sh [single|cluster|cluster-with-monitoring|kubernetes|stop|clean]"
        echo ""
        echo "  single              - Start single-node Kasoku"
        echo "  cluster             - Start 3-node cluster"
        echo "  cluster-with-monitoring - Start cluster with Prometheus & Grafana"
        echo "  kubernetes          - Deploy to Kubernetes"
        echo "  stop                - Stop all containers"
        echo "  clean               - Stop and remove volumes"
        exit 1
        ;;
esac
