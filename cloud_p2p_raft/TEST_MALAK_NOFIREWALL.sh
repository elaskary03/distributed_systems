#!/usr/bin/env bash
# -------------------------------
# Node 1 (Mac) — 10.40.36.59:7001
# -------------------------------

set -e

ID=1
ADDR="0.0.0.0:7001"
PEERS="2=192.168.8.125:7002"   # Windows peer

echo "=== Building project ==="
cargo build

mkdir -p logs
export RUST_LOG=info

echo "=== Starting Raft Node $ID on $ADDR ==="
cargo run --bin node -- --id "$ID" --addr "$ADDR" --peers "$PEERS" | tee "logs/node$ID.log"
