#!/bin/bash

# Navigate to project root
cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft || exit 1

echo "🗑  Cleaning up old processes..."
pkill -f "cargo run.*node"
pkill -f "cargo run.*proxy"
pkill -f "cargo run.*loadtest"
sleep 2

echo "🗑  Removing duplicates..."
rm -f src/bin/node2.rs src/bin/proxy2.rs

echo "📋 Copying improved versions..."
cp artifacts/node.rs src/bin/node.rs 2>/dev/null || echo "   (using existing node.rs)"
cp artifacts/proxy.rs src/bin/proxy.rs 2>/dev/null || echo "   (using existing proxy.rs)"
cp artifacts/loadtest.rs src/bin/loadtest.rs 2>/dev/null || echo "   (using existing loadtest.rs)"

echo "⚙️  Building project..."
cargo build --release

echo "🚀 Launching nodes and proxy in new Terminal tabs..."

osascript <<'EOD'
tell application "Terminal"
    activate

    # Node 1
    set tab1 to do script "cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft && echo '🟢 Starting Node 1...' && cargo run --release --bin node -- --id 1 --addr 127.0.0.1:7001 --peers '2=127.0.0.1:7002,3=127.0.0.1:7003' --client-addr 127.0.0.1:9001"

    # Node 2
    delay 1
    set tab2 to do script "cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft && echo '🟢 Starting Node 2...' && cargo run --release --bin node -- --id 2 --addr 127.0.0.1:7002 --peers '1=127.0.0.1:7001,3=127.0.0.1:7003' --client-addr 127.0.0.1:9002"

    # Node 3
    delay 1
    set tab3 to do script "cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft && echo '🟢 Starting Node 3...' && cargo run --release --bin node -- --id 3 --addr 127.0.0.1:7003 --peers '1=127.0.0.1:7001,2=127.0.0.1:7002' --client-addr 127.0.0.1:9003"

    # Proxy
    delay 3
    set tab4 to do script "cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft && echo '🔗 Starting Proxy...' && cargo run --release --bin proxy -- --listen 127.0.0.1:9100 --seeds '127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003'"

    # Wait for leader election
    delay 8
    
    # Persistent connection load test
    set tab5 to do script "cd /Users/mostafa/Desktop/Distributed/distributed_systems/cloud_p2p_raft && echo '🧪 Starting PERSISTENT CONNECTION load test...' && echo '' && cargo run --release --bin loadtest -- --clients 100 --requests 50 --keep-alive --delay-ms 50 --mode mixed"

end tell
EOD

echo "✅ All processes launched!"
echo ""
echo "📊 What's running:"
echo "   Tabs 1-3: Raft nodes (watch for '👑 Node X became LEADER')"
echo "   Tab 4: Proxy (watch for '📡 Client connected')"
echo "   Tab 5: Load test with PERSISTENT connections"
echo ""
echo "⏱️  Load test will start in ~12 seconds"
echo ""
echo "🔍 In a new terminal, monitor connections:"
echo "   watch -n 1 'echo \"ESTABLISHED: \$(netstat -an | grep \":900[0-9]\" | grep -c ESTABLISHED) | TIME_WAIT: \$(netstat -an | grep \":900[0-9]\" | grep -c TIME_WAIT)\"'"