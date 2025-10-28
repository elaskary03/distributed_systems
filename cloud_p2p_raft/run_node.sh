#!/bin/bash

# This script runs a single Raft node with the specified configuration
# Usage: ./run_node.sh <node_id> <listen_addr> <peer_list>
# Example: ./run_node.sh 1 192.168.1.100:7001 "2=192.168.1.101:7002,3=192.168.1.102:7003"

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <node_id> <listen_addr> <peer_list>"
    echo "Example: $0 1 192.168.1.100:7001 \"2=192.168.1.101:7002,3=192.168.1.102:7003\""
    exit 1
fi

NODE_ID=$1
LISTEN_ADDR=$2
PEER_LIST=$3

# Create logs directory if it doesn't exist
mkdir -p logs

# Build the project
echo "Building project..."
cargo build

# Run the node with logging
echo "Starting node $NODE_ID listening on $LISTEN_ADDR"
echo "Peers: $PEER_LIST"

RUST_LOG=info cargo run --bin node -- \
    --id "$NODE_ID" \
    --addr "$LISTEN_ADDR" \
    --peers "$PEER_LIST" 2>&1 | tee "logs/node${NODE_ID}.log"