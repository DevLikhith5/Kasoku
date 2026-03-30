.PHONY: build test clean bench

# Build binaries
build:
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

# Run server
server:
	./kasoku-server --config kasoku.yaml

# Help
help:
	@echo "Kasoku - High-Performance LSM Key-Value Store"
	@echo ""
	@echo "Usage:"
	@echo "  make build    - Build binaries (kvctl, kasoku-server)"
	@echo "  make test     - Run all tests"
	@echo "  make bench    - Run benchmark (10ms WAL sync)"
	@echo "  make clean    - Remove build artifacts"
	@echo "  make server   - Start Kasoku server"
	@echo "  make fmt      - Format code"
	@echo "  make lint     - Run go vet"
