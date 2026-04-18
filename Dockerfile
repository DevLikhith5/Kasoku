# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /kasoku-server ./cmd/server/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /kvctl ./cmd/kvctl/main.go

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 kasoku && \
    adduser -u 1000 -G kasoku -s /bin/sh -D kasoku

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /kasoku-server .
COPY --from=builder /kvctl .

# Copy default config
COPY configs/single.yaml /app/config.yaml

# Create data directory
RUN mkdir -p /data && chown -R kasoku:kasoku /data

USER kasoku

EXPOSE 9000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q --spider http://localhost:9000/health || exit 1

ENTRYPOINT ["/app/kasoku-server"]
CMD ["--config", "/app/config.yaml"]
