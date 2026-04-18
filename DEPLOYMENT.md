# Deployment Guide

This guide covers production deployment options for Kasoku.

## Prerequisites

- Docker 20.10+ (for containerized deployment)
- Kubernetes 1.24+ (for K8s deployment)
- 4GB RAM minimum per node
- 2 CPU cores minimum per node

---

## Quick Start

```bash
# Clone and build
git clone https://github.com/DevLikhith5/Kasoku.git
cd Kasoku

# Start single node
./setup.sh single

# Verify it's running
curl http://localhost:9000/health
```

---

## Docker Deployment

### Single Node

```bash
# Build and run
docker build -t kasoku .
docker run -p 9000:9000 kasoku

# Or use docker-compose
docker-compose -f docker-compose.single.yml up -d
```

### 3-Node Cluster

```bash
# Build and start cluster
docker-compose -f docker-compose.yml up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f kasoku-node1
```

### With Monitoring

```bash
# Start with Prometheus + Grafana
docker-compose -f docker-compose.yml --profile monitoring up -d

# Access services:
# - Kasoku: http://localhost:9001, :9002, :9003
# - Prometheus: http://localhost:9090
# - Grafana: http://localhost:3000 (admin/admin)
```

---

## Kubernetes Deployment

### Prerequisites

```bash
# Create namespace
kubectl create namespace kasoku

# Apply configuration
kubectl apply -f deploy/kubernetes/kasoku-statefulset.yaml

# Check status
kubectl get pods -n kasoku
```

### Access Services

```bash
# Port forward to local
kubectl port-forward -n kasoku svc/kasoku-http 9000:80

# Or use LoadBalancer (cloud provider)
kubectl expose -n kasoku svc/kasoku-http --type=LoadBalancer
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KASOKU_NODE_ID` | `node-1` | Unique node identifier |
| `KASOKU_HTTP_PORT` | `9000` | HTTP server port |
| `KASOKU_CLUSTER_ENABLED` | `false` | Enable cluster mode |
| `KASOKU_CLUSTER_PEERS` | - | Comma-separated peer addresses |
| `KASOKU_QUORUM_SIZE` | `1` | Write quorum (W) |
| `KASOKU_READ_QUORUM` | `1` | Read quorum (R) |
| `KASOKU_DATA_DIR` | `/data` | Data storage directory |
| `GOMEMLIMIT` | - | Go memory limit (e.g., `1GiB`) |
| `GOMAXPROCS` | - | CPU cores (e.g., `2`) |

---

## Configuration

### Performance Tuning

For high-throughput workloads:

```yaml
# production.yaml
memory:
  memtable_size: 268435456    # 256MB
  max_memtable_bytes: 1073741824  # 1GB
  block_cache_size: 536870912   # 512MB

compaction:
  max_concurrent: 4

wal:
  sync: false                  # Async for throughput
  sync_interval: 100ms
```

### Durability Tuning

For durability-critical workloads:

```yaml
wal:
  sync: true                   # Sync every write
  checkpoint_bytes: 1048576      # 1MB checkpoint

memory:
  memtable_size: 67108864      # 64MB (more flushes)
```

---

## Health Checks

```bash
# Liveness (is node alive?)
curl http://localhost:9000/health

# Readiness (is node ready to serve?)
curl http://localhost:9000/ready

# Detailed status
curl http://localhost:9000/status

# Metrics (Prometheus format)
curl http://localhost:9000/metrics
```

---

## Monitoring

### Prometheus Metrics

Kasoku exposes Prometheus metrics at `/metrics`:

```
kasoku_up{node_id="node-1"} 1
kasoku_peers_healthy{node_id="node-1"} 3
kasoku_hints_pending{node_id="node-1"} 0
kasoku_ring_nodes{node_id="node-1"} 3
```

### Grafana Dashboard

Import `deploy/grafana/kasoku-dashboard.json` into Grafana for pre-built dashboards.

---

## Testing Deployment

```bash
# Write data
curl -X PUT http://localhost:9001/kv/test -d 'hello'

# Read from same node
curl http://localhost:9001/kv/test

# Read from different node (proves replication)
curl http://localhost:9002/kv/test

# Cluster info
curl http://localhost:9001/ring
```

---

## Production Checklist

- [ ] Set `GOMEMLIMIT` to prevent OOM
- [ ] Set `GOMAXPROCS` based on CPU cores
- [ ] Configure resource limits in K8s
- [ ] Set up monitoring (Prometheus/Grafana)
- [ ] Configure log aggregation
- [ ] Set up backup strategy
- [ ] Test node failure recovery
- [ ] Verify hinted handoff works
