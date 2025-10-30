#!/bin/bash

# This script runs a single Raft node with the specified configuration
# Usage: ./run_node.sh <node_id> <listen_addr> <peer_list>
# Example: ./run_node.sh 1 192.168.1.100:7001 "2=192.168.1.101:7002,3=192.168.1.102:7003"

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
    echo "Usage: $0 <node_id> <listen_addr> <peer_list> [bootstrap_addr]"
    echo "Example (full mesh): $0 1 192.168.1.100:7001 \"2=192.168.1.101:7002,3=192.168.1.102:7003\""
    echo "Example (bootstrap join): $0 4 192.168.1.103:7004 \"\" 192.168.1.100:7001"
    exit 1
fi

NODE_ID=$1
LISTEN_ADDR=$2
PEER_LIST=$3
BOOTSTRAP_ADDR=""
if [ "$#" -eq 4 ]; then
    BOOTSTRAP_ADDR=$4
fi

# Create logs directory if it doesn't exist
mkdir -p logs

# Build the project
echo "Building project..."
cargo build

# Run the node with logging
echo "Starting node $NODE_ID listening on $LISTEN_ADDR"
if [ -n "$BOOTSTRAP_ADDR" ]; then
    echo "Bootstrapping against: $BOOTSTRAP_ADDR"
    RUST_LOG=info cargo run --bin node -- \
        --id "$NODE_ID" \
        --addr "$LISTEN_ADDR" \
        --peers "$PEER_LIST" \
        --bootstrap "$BOOTSTRAP_ADDR" 2>&1 | tee "logs/node${NODE_ID}.log"
else
    echo "Peers: $PEER_LIST"
    RUST_LOG=info cargo run --bin node -- \
        --id "$NODE_ID" \
        --addr "$LISTEN_ADDR" \
        --peers "$PEER_LIST" 2>&1 | tee "logs/node${NODE_ID}.log"
fi