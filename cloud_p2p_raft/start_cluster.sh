#!/bin/bash

set -euo pipefail

echo "🔪 Killing old processes..."
pkill -f "target/debug/node" || true
pkill -f "target/debug/proxy" || true
pkill -f "target/debug/gui" || true
pkill -f "target/debug/client_p2p" || true

echo "🔧 Building project..."
cargo build

mkdir -p logs

open_term() {
osascript <<EOD
tell application "Terminal"
    do script "cd \"$PWD\" && $1"
end tell
EOD
}

echo "🚀 Starting Raft node 1..."
open_term "RUST_LOG=info cargo run --bin node -- \
  --id 1 \
  --addr 127.0.0.1:7001 \
  --client-host 127.0.0.1 \
  --client-base-port 9000 \
  --peers 2=127.0.0.1:7002,3=127.0.0.1:7003 \
  2>&1 | tee logs/node1.log"

sleep 1

echo "🚀 Starting Raft node 2..."
open_term "RUST_LOG=info cargo run --bin node -- \
  --id 2 \
  --addr 127.0.0.1:7002 \
  --client-host 127.0.0.1 \
  --client-base-port 9000 \
  --peers 1=127.0.0.1:7001,3=127.0.0.1:7003 \
  2>&1 | tee logs/node2.log"

sleep 1

echo "🚀 Starting Raft node 3..."
open_term "RUST_LOG=info cargo run --bin node -- \
  --id 3 \
  --addr 127.0.0.1:7003 \
  --client-host 127.0.0.1 \
  --client-base-port 9000 \
  --peers 1=127.0.0.1:7001,2=127.0.0.1:7002 \
  2>&1 | tee logs/node3.log"

sleep 2

echo "🔌 Starting proxy..."
open_term "RUST_LOG=info cargo run --bin proxy -- \
  --listen 127.0.0.1:9100 \
  --seeds 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
  2>&1 | tee logs/proxy.log"

sleep 1

echo "🖥️ Starting GUI..."
open_term "RUST_LOG=info cargo run --bin gui -- \
  --listen 127.0.0.1:8080 \
  --proxy-addr 127.0.0.1:9100 \
  2>&1 | tee logs/gui.log"

sleep 1

echo "🤝 Starting P2P user 'merna'..."
open_term "RUST_LOG=info cargo run --bin client_p2p -- \
  --user merna \
  --port 10000 \
  2>&1 | tee logs/p2p_merna.log"

sleep 1

echo "🤝 Starting P2P user 'ahmed'..."
open_term "RUST_LOG=info cargo run --bin client_p2p -- \
  --user ahmed \
  --port 10001 \
  2>&1 | tee logs/p2p_ahmed.log"

sleep 2

echo "🎉 All components launched!"
echo "🌐 GUI at: http://127.0.0.1:8080"
tail -f logs/*.log
