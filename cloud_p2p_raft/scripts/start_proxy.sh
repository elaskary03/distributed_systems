#!/bin/bash
set -e

source "$(dirname "$0")/cluster_config.env"

if [ "$ROLE" != "proxy" ]; then
  echo "❌ This machine is not the proxy (ROLE=$ROLE)"
  exit 1
fi

echo "🚀 Starting Proxy at $PROXY_IP:$PROXY_PORT"

NODES="1=$NODE1_IP:$CLIENT1_PORT,2=$NODE2_IP:$CLIENT2_PORT,3=$NODE3_IP:$CLIENT3_PORT"

cargo run --release --bin proxy -- \
  --listen "$PROXY_IP:$PROXY_PORT" \
  --nodes "$NODES"
