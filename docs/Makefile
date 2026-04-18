.PHONY: build test clean bench run-server

# Build binaries
build: server-bin cli-bin

server-bin:
	go build -o kasoku-server ./cmd/server

cli-bin:
	go build -o kvctl ./cmd/kvctl

# Run all tests
test:
	go test ./... -v

# Run LSM engine tests
test-lsm:
	go test ./internal/store/lsm-engine/... -v

# Run benchmarks
bench:
	./kvctl bench --wal-sync 10

# Clean build artifacts
clean:
	rm -f kvctl kasoku-server
	go clean -cache

# Format code
fmt:
	go fmt ./...

# Lint code
lint:
	go vet ./...

# Run server with default config
run: kasoku-server
	./kasoku-server

# Run server with custom port
run-port:
	KASOKU_ADDR=$(PORT) ./kasoku-server

# Interactive CLI shell
shell:
	./kvctl shell

# Help
help:
	@echo "Kasoku - High-Performance LSM Key-Value Store"
	@echo ""
	@echo "Usage:"
	@echo "  make build       - Build binaries (kvctl, kasoku-server)"
	@echo "  make server-bin  - Build HTTP server"
	@echo "  make cli-bin     - Build CLI tool"
	@echo "  make test        - Run all tests"
	@echo "  make bench       - Run benchmark (10ms WAL sync)"
	@echo "  make clean       - Remove build artifacts"
	@echo "  make run         - Run server (default port 8080)"
	@echo "  make fmt         - Format code"
	@echo "  make lint        - Run go vet"
	@echo "  make shell       - Start CLI interactive shell"
	@echo ""
	@echo "Quick Start:"
	@echo "  make build && make run &"
	@echo "  ./kvctl put user:1 Alice"
	@echo "  curl http://localhost:8080/api/v1/get/user:1"
