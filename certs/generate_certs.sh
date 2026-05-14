#!/bin/bash
# Generates self-signed certificates for local TLS testing
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Generating self-signed Kasoku certificates in $DIR..."

openssl req -x509 -newkey rsa:4096 -nodes -sha256 -days 3650 \
  -keyout "$DIR/server-key.pem" \
  -out "$DIR/server-cert.pem" \
  -subj "/C=US/ST=State/L=City/O=KasokuDB/OU=Development/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:node1,DNS:node2,DNS:node3,IP:127.0.0.1"

echo "Done! Generated server-cert.pem and server-key.pem"
