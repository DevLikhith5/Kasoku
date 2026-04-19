# Kasoku

High-performance distributed key-value store implementing Amazon Dynamo paper with LSM-tree storage.

## Quick Links

| Document | Description |
|----------|-------------|
| [PAPER.md](docs/PAPER.md) | **Comprehensive research paper** - understand Kasoku without reading code |
| [README.md](docs/README.md) | Project overview and benchmarks |
| [DEPLOYMENT.md](docs/DEPLOYMENT.md) | Production deployment guide |
| [USAGE.md](docs/USAGE.md) | API reference and command usage |

## Quick Start

```bash
cd docs
./setup.sh single   # Start single node
./setup.sh cluster  # Start 3-node cluster
```

## Project Structure

```
kasoku/
├── cmd/            # Server and CLI binaries
├── configs/        # Configuration files
├── deploy/         # Docker, Kubernetes, monitoring
├── docs/           # All documentation
├── internal/       # Source code
└── benchmarks/     # Pressure testing tool
```

## Performance

| Metric | Achieved |
|--------|----------|
| Single-node writes | **197,977 ops/sec** |
| Single-node reads | 371,000 ops/sec |
| Cluster writes (RF=3) | 300,000+ ops/sec |

See [PAPER.md](docs/PAPER.md) for full evaluation details.

## Key Features

- **Dynamo Paper**: Consistent hashing, quorum replication (W=1/R=1), vector clocks
- **LSM-Tree**: WAL, MemTable, SSTable, Bloom filters, compaction
- **Fault Tolerance**: Hinted handoff, read repair, Merkle anti-entropy
- **Production Ready**: Docker, Kubernetes, Prometheus metrics, health checks

## License

Proprietary - see [docs/LICENSE](docs/LICENSE)
