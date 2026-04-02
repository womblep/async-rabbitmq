#!/usr/bin/env bash
# Generates a self-signed CA and server certificate for local RabbitMQ TLS testing.
# Called automatically by spec_helper.rb when certs are missing.
# Can also be run manually:
#
#   bash spec/docker/gen-certs.sh
#
# Certificates are written to spec/docker/certs/ and are git-ignored.
# Re-running overwrites existing certificates.

#set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERTS_DIR="$SCRIPT_DIR/certs"

mkdir -p "$CERTS_DIR"

echo "Generating CA key and certificate..."
openssl genrsa -out "$CERTS_DIR/ca_key.pem" 4096 2>/dev/null
openssl req -new -x509 -days 3650 \
  -key "$CERTS_DIR/ca_key.pem" \
  -out "$CERTS_DIR/ca_certificate.pem" \
  -subj '//CN=AsyncRabbitMQ-Test-CA'

echo "Generating server key and certificate..."
openssl genrsa -out "$CERTS_DIR/server_key.pem" 2048 2>/dev/null

openssl req -new \
  -key "$CERTS_DIR/server_key.pem" \
  -out "$CERTS_DIR/server_csr.pem" \
  -subj "//CN=localhost"

openssl x509 -req -days 3650 \
  -in "$CERTS_DIR/server_csr.pem" \
  -CA "$CERTS_DIR/ca_certificate.pem" \
  -CAkey "$CERTS_DIR/ca_key.pem" \
  -CAcreateserial \
  -out "$CERTS_DIR/server_certificate.pem" \
  -extfile "$SCRIPT_DIR/ssl_extfile.txt"

rm -f "$CERTS_DIR/server_csr.pem" "$CERTS_DIR/ca_certificate.srl"
chmod 600 "$CERTS_DIR"/*.pem

echo "Certificates written to $CERTS_DIR:"
ls -1 "$CERTS_DIR"
