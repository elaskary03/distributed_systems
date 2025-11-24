#!/bin/bash
set -e

source "$(dirname "$0")/cluster_config.env"

if [ "$ROLE" != "client" ]; then
  echo "❌ This machine is not marked as client (ROLE=$ROLE)"
  exit 1
fi

CLIENTS=${1:-500}
REQUESTS=${2:-400}

echo "🚀 Running load tester ($CLIENTS clients, $REQUESTS requests)"

cargo run --release --bin loadtest -- \
  --proxy "$PROXY_IP:$PROXY_PORT" \
  --clients $CLIENTS \
  --requests $REQUESTS \
  --mode mixed \
  --keep-alive \
  --ramp-up-secs 10
