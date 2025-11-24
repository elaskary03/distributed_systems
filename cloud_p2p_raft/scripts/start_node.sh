#!/bin/bash
set -e

source "$(dirname "$0")/cluster_config.env"

case "$ROLE" in
  node1)
    ID=1
    ADDR="$NODE1_IP:$NODE1_PORT"
    CLIENT_ADDR="$NODE1_IP:$CLIENT1_PORT"
    PEERS="2=$NODE2_IP:$NODE2_PORT,3=$NODE3_IP:$NODE3_PORT"
    ;;
  node2)
    ID=2
    ADDR="$NODE2_IP:$NODE2_PORT"
    CLIENT_ADDR="$NODE2_IP:$CLIENT2_PORT"
    PEERS="1=$NODE1_IP:$NODE1_PORT,3=$NODE3_IP:$NODE3_PORT"
    ;;
  node3)
    ID=3
    ADDR="$NODE3_IP:$NODE3_PORT"
    CLIENT_ADDR="$NODE3_IP:$CLIENT3_PORT"
    PEERS="1=$NODE1_IP:$NODE1_PORT,2=$NODE2_IP:$NODE2_PORT"
    ;;
  *)
    echo "❌ This machine is not configured as a node (ROLE=$ROLE)"
    exit 1
    ;;
esac

echo "🚀 Starting node $ID at $ADDR"

cargo run --release --bin node -- \
  --id $ID \
  --addr $ADDR \
  --peers "$PEERS" \
  --client-addr $CLIENT_ADDR
