# Kasoku HTTP Server

A RESTful HTTP API for the Kasoku KV store.

## Quick Start

### Run the HTTP Server

```bash
# Option 1: Build and run
cd /Users/cvlikhith/kasoku
go build -o kasoku-server ./cmd/server
./kasoku-server

# Option 2: Run directly with go
go run ./cmd/server

# Option 3: With custom config
KASOKU_ADDR=:9090 KASOKU_NODE_ID=node-2 ./kasoku-server

# Option 4: Custom WAL path
KASOKU_WAL_DIR=/tmp/my-wal.log ./kasoku-server
```

### Run the CLI (kvctl)

```bash
# Build CLI
go build -o kvctl ./cmd/kvctl

# Basic commands
./kvctl put user:1 "Alice"           # Store a value
./kvctl get user:1                   # Retrieve a value
./kvctl delete user:1                # Delete a key
./kvctl keys                         # List all keys
./kvctl scan user:                   # Scan by prefix
./kvctl stats                        # Show statistics

# With custom data directory
./kvctl --dir ./data put key value

# Interactive shell
./kvctl shell

# Export/Import
./kvctl export backup.json
./kvctl import backup.json

# Benchmark
./kvctl bench --count 1000
```

### Use Both Together

```bash
# Terminal 1: Start the server
./kasoku-server

# Terminal 2: Use CLI (direct file access)
./kvctl put user:1 "Alice"

# Terminal 3: Use HTTP API
curl http://localhost:8080/api/v1/get/user:1
```

**Note:** CLI accesses data files directly, HTTP server accesses via REST API. Both use the same WAL/storage engine.

## Folder Structure

```
cmd/server/
├── main.go                 # Entry point (105 lines)
├── handler/
│   ├── server.go           # Server struct, routes (51 lines)
│   ├── handlers.go         # HTTP handlers (231 lines)
│   ├── types.go            # Request/response types (56 lines)
│   ├── middleware.go       # Logging, recovery (44 lines)
│   └── helpers.go          # Error helpers (21 lines)
├── metrics/
│   └── metrics.go          # Metrics tracking (138 lines)
└── README.md               # This file
```

## API Endpoints

### Health Checks

```bash
# Basic health
curl http://localhost:8080/health

# Liveness probe
curl http://localhost:8080/health/live

# Readiness probe (includes stats)
curl http://localhost:8080/health/ready
```

### Key-Value Operations

```bash
# PUT a key
curl -X PUT http://localhost:8080/api/v1/put/mykey \
  -d '{"value":"myvalue"}'

# GET a key
curl http://localhost:8080/api/v1/get/mykey

# DELETE a key
curl -X DELETE http://localhost:8080/api/v1/delete/mykey

# List all keys
curl http://localhost:8080/api/v1/keys

# Scan keys with prefix
curl "http://localhost:8080/api/v1/scan?prefix=user:"
```

### Node & Metrics

```bash
# Node information
curl http://localhost:8080/api/v1/node

# Prometheus-style metrics
curl http://localhost:8080/metrics
```

## Response Format

All responses follow this structure:

**Success:**

```json
{
  "success": true,
  "data": { ... }
}
```

**Error:**

```json
{
  "success": false,
  "error": "error message"
}
```

## Configuration

| Env Variable | Default | Description |
|-------------|---------|-------------|
| `KASOKU_ADDR` | `:8080` | HTTP listen address |
| `KASOKU_NODE_ID` | `node-1` | Unique node identifier |
| `KASOKU_WAL_DIR` | `./data/wal/wal.log` | WAL file path |

## Package Organization

| Package | File | Purpose | Lines |
|---------|------|---------|-------|
| `main` | main.go | Config, lifecycle, shutdown | 105 |
| `handler` | server.go | Server struct, route registration | 51 |
| `handler` | handlers.go | HTTP request handlers | 231 |
| `handler` | types.go | Request/response DTOs | 56 |
| `handler` | middleware.go | Logging, recovery middleware | 44 |
| `handler` | helpers.go | Error checking helpers | 21 |
| `metrics` | metrics.go | Operation tracking | 138 |

**Total:** 646 lines of application code

## Architecture

```
┌─────────────────────────────────────────┐
│  main.go                                │
│  - Config loading                       │
│  - Logger setup                         │
│  - Graceful shutdown                    │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│  handler/server.go                      │
│  - Server struct                        │
│  - Route registration                   │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│  handler/handlers.go                    │
│  - handleGet, handlePut, handleDelete   │
│  - handleScan, handleKeys               │
│  - handleHealth, handleNodeInfo         │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│  storage.StorageEngine                  │
│  - HashMapEngine (with internal WAL)    │
└─────────────────────────────────────────┘
```

## Design Principles

1. **Small, focused files** - No file exceeds 250 lines
2. **Single responsibility** - Each file has one clear purpose
3. **Clear separation** - Types, handlers, middleware in separate files
4. **No WAL in Server** - StorageEngine manages WAL internally

## Example Session

```bash
# Store some data
curl -X PUT http://localhost:8080/api/v1/put/user:1 -d '{"value":"Alice"}'
curl -X PUT http://localhost:8080/api/v1/put/user:2 -d '{"value":"Bob"}'

# Retrieve
curl http://localhost:8080/api/v1/get/user:1

# List keys
curl http://localhost:8080/api/v1/keys

# Scan with prefix
curl "http://localhost:8080/api/v1/scan?prefix=user:"

# Delete
curl -X DELETE http://localhost:8080/api/v1/delete/user:2
```

## Next Steps

- [ ] Add authentication middleware
- [ ] Implement rate limiting
- [ ] Add cluster HTTP endpoints (peer discovery, replication)
- [ ] WebSocket support for real-time updates
- [ ] Admin endpoints (compaction, backup)
