#!/bin/bash

# Navigate to project root
cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft || exit 1

echo "🗑  Removing duplicates..."
rm -f src/bin/node2.rs src/bin/proxy2.rs

echo "📋 Copying improved versions..."
cp artifacts/node.rs src/bin/node.rs
cp artifacts/proxy.rs src/bin/proxy.rs
cp artifacts/loadtest.rs src/bin/loadtest.rs

echo "⚙️  Building project..."
cargo build --release

echo "🚀 Launching nodes and proxy in new Terminal tabs..."

osascript <<EOD
tell application "Terminal"
    activate

    # Node 1
    do script "cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft && cargo run --release --bin node -- --id 1 --addr 127.0.0.1:7001 --peers '2=127.0.0.1:7002,3=127.0.0.1:7003' --client-addr 127.0.0.1:9001"

    # Node 2
    do script "cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft && cargo run --release --bin node -- --id 2 --addr 127.0.0.1:7002 --peers '1=127.0.0.1:7001,3=127.0.0.1:7003' --client-addr 127.0.0.1:9002"

    # Node 3
    do script "cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft && cargo run --release --bin node -- --id 3 --addr 127.0.0.1:7003 --peers '1=127.0.0.1:7001,2=127.0.0.1:7002' --client-addr 127.0.0.1:9003"

    # Proxy
    do script "cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft && cargo run --release --bin proxy -- --listen 127.0.0.1:9100 --seeds '127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003'"

    # Load test (wait a few seconds to ensure nodes/proxy are up)
    delay 5
    do script "cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft && cargo run --release --bin loadtest -- --clients 100 --requests 10"
end tell
EOD

echo "✅ All commands executed."
