#!/bin/bash

# Kill any existing node processes
pkill -f "target/debug/node"

# Build the project
cargo build

# Create a log directory
mkdir -p logs

# Start three nodes in different terminals
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
EOD

# Wait a moment for the nodes to start
sleep 2

# Monitor the logs
echo "Monitoring node logs (Ctrl+C to stop)..."
tail -f logs/node*.log