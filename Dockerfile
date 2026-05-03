FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o kasoku-server ./cmd/server/

FROM alpine:latest

RUN apk --no-cache add ca-certificates curl
WORKDIR /app
COPY --from=builder /app/kasoku-server .
COPY --from=builder /app/configs ./configs
COPY --from=builder /app/certs ./certs

EXPOSE 9000 9001 9002

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:9001/health || exit 1

ENTRYPOINT ["./kasoku-server"]