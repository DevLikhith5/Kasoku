# Kasoku - Quick Start Guide

## Build & Run Everything

### 1. Build Both Binaries

```bash
cd /Users/cvlikhith/kasoku

# Build HTTP server
go build -o kasoku-server ./cmd/server

# Build CLI
go build -o kvctl ./cmd/kvctl

# Or build everything at once
make build
```

### 2. Start the HTTP Server

```bash
# Start server (default port 8080)
./kasoku-server

# Or with custom port
KASOKU_ADDR=:9090 ./kasoku-server
```

### 3. Use the CLI

```bash
# Put/Get/Delete
./kvctl put user:1 "Alice"
./kvctl get user:1
./kvctl delete user:1

# List and scan
./kvctl keys
./kvctl scan user:

# Interactive shell
./kvctl shell
```

### 4. Use the HTTP API

```bash
# Health check
curl http://localhost:8080/health

# Put/Get
curl -X PUT http://localhost:8080/api/v1/put/user:1 -d '{"value":"Alice"}'
curl http://localhost:8080/api/v1/get/user:1

# Delete
curl -X DELETE http://localhost:8080/api/v1/delete/user:1

# List keys
curl http://localhost:8080/api/v1/keys
```

## Project Structure

```
kasoku/
├── cmd/
│   ├── server/          # HTTP REST API
│   │   ├── main.go
│   │   ├── handler/     # HTTP handlers
│   │   └── metrics/     # Metrics tracking
│   └── kvctl/           # CLI tool
│       ├── main.go
│       └── commands/    # CLI commands
├── internal/
│   ├── store/           # Storage engine (WAL, HashMap, LSM)
│   └── cluster/         # Cluster coordination
├── data/                # Data directory
└── Makefile             # Build commands
```

## Configuration

### HTTP Server

| Env Variable | Default | Description |
|-------------|---------|-------------|
| `KASOKU_ADDR` | `:8080` | HTTP listen address |
| `KASOKU_NODE_ID` | `node-1` | Unique node identifier |
| `KASOKU_WAL_DIR` | `./data/wal/wal.log` | WAL file path |

### CLI

```bash
# Use custom data directory
./kvctl --dir ./my-data put key value

# Use config file
./kvctl --config kasoku.yaml get key

# Verbose output
./kvctl --verbose get key
```

## Common Workflows

### Development

```bash
# Terminal 1: Watch and rebuild
go run ./cmd/server

# Terminal 2: Test with CLI
./kvctl put test "value"
./kvctl get test
```

### Testing HTTP API

```bash
# Start server
./kasoku-server

# In another terminal, test endpoints
curl http://localhost:8080/health
curl -X PUT http://localhost:8080/api/v1/put/key1 -d '{"value":"val1"}'
curl http://localhost:8080/api/v1/get/key1
curl http://localhost:8080/metrics
```

### Data Backup

```bash
# Export to JSON
./kvctl export backup.json

# Import from JSON
./kvctl import backup.json
```

### Benchmarking

```bash
# Run benchmark (1000 operations)
./kvctl bench --count 1000

# Benchmark with specific key size
./kvctl bench --count 10000 --key-prefix "user:"
```

## Makefile Commands

```bash
make build      # Build all binaries
make server     # Build HTTP server only
make cli        # Build CLI only
make test       # Run tests
make clean      # Remove binaries
make run        # Run server
```

## Documentation

- **HTTP Server**: See `cmd/server/README.md`
- **CLI**: Run `./kvctl --help` or `./kvctl [command] --help`
- **Configuration**: See `CONFIGURATION.md`
- **Usage Guide**: See `USAGE.md`

## Troubleshooting

**Server won't start:**
```bash
# Check if port is in use
lsof -ti:8080 | xargs kill -9

# Check WAL directory exists
mkdir -p data/wal
```

**CLI can't find data:**
```bash
# Specify data directory explicitly
./kvctl --dir ./data get key
```

**Build errors:**
```bash
# Clean and rebuild
make clean
make build
```
