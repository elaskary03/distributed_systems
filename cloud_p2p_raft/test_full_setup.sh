#!/bin/bash

# Starts 3 cloud nodes, a joiner (client) node, and a middleware client
# Each is launched in its own Terminal window (macOS Terminal.app)
# Usage: ./test_full_setup.sh

# Kill any existing node processes
pkill -f "target/debug/node" || true

# Build only the node binary (avoid building other missing/optional binaries like `gui`)
cargo build --bin node

# Create a log directory
mkdir -p logs

# Start three nodes in different terminals and a joiner + middleware client
osascript <<EOD
tell application "Terminal"
  do script "cd \"$PWD\" && ./run_node.sh 1 127.0.0.1:7001 \"2=127.0.0.1:7002,3=127.0.0.1:7003\" 2>&1 | tee logs/node1.log"
end tell

tell application "Terminal"
  delay 1
  do script "cd \"$PWD\" && ./run_node.sh 2 127.0.0.1:7002 \"1=127.0.0.1:7001,3=127.0.0.1:7003\" 2>&1 | tee logs/node2.log"
end tell

tell application "Terminal"
  delay 1
  do script "cd \"$PWD\" && ./run_node.sh 3 127.0.0.1:7003 \"1=127.0.0.1:7001,2=127.0.0.1:7002\" 2>&1 | tee logs/node3.log"
end tell

tell application "Terminal"
  delay 1
  -- Start a joiner node (client node) that bootstraps off node 1
  do script "cd \"$PWD\" && ./run_node.sh 4 127.0.0.1:7004 \"\" 127.0.0.1:7001 2>&1 | tee logs/node4.log"
end tell

# Open a terminal that connects to middleware on node 1 (persistent client)
tell application "Terminal"
  delay 1
  do script "cd \"$PWD\" && echo 'Connecting to middleware on node1 (127.0.0.1:9101)...'; nc 127.0.0.1 9101"
end tell
EOD

# Wait a moment for the nodes to start
sleep 2

echo "All terminals started. Logs: logs/node1.log logs/node2.log logs/node3.log logs/node4.log"
echo "Use 'tail -f logs/node*.log' to watch logs from this shell if desired." 

# Reminder: make executable if necessary
# chmod +x test_full_setup.sh
