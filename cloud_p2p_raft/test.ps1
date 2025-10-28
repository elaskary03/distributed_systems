# test.ps1
# ----------------------------------------------

Write-Host "Killing any existing node processes..."
# Kill any running "node" binaries in target/debug
Get-Process | Where-Object { $_.Path -like "*target\debug\node.exe" } | Stop-Process -Force -ErrorAction SilentlyContinue

# Build the project
Write-Host "Building the project..."
cargo build

# Create logs directory if it doesn’t exist
if (!(Test-Path "logs")) {
    New-Item -ItemType Directory -Path "logs" | Out-Null
}

# Save current working directory
$wd = (Get-Location).Path

Write-Host "Starting three Raft nodes in separate PowerShell windows..."

# Node 1
Start-Process powershell.exe -WorkingDirectory $wd -ArgumentList "-NoExit", "-Command", "& {
    `$env:RUST_LOG='info';
    cargo run --bin node -- --id 1 --addr 127.0.0.1:7001 --peers 2=127.0.0.1:7002,3=127.0.0.1:7003 |
    Tee-Object -FilePath 'logs\node1.log' }"

Start-Sleep -Seconds 1

# Node 2
Start-Process powershell.exe -WorkingDirectory $wd -ArgumentList "-NoExit", "-Command", "& {
    `$env:RUST_LOG='info';
    cargo run --bin node -- --id 2 --addr 127.0.0.1:7002 --peers 1=127.0.0.1:7001,3=127.0.0.1:7003 |
    Tee-Object -FilePath 'logs\node2.log' }"

Start-Sleep -Seconds 1

# Node 3
Start-Process powershell.exe -WorkingDirectory $wd -ArgumentList "-NoExit", "-Command", "& {
    `$env:RUST_LOG='info';
    cargo run --bin node -- --id 3 --addr 127.0.0.1:7003 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002 |
    Tee-Object -FilePath 'logs\node3.log' }"

# Wait a bit for nodes to start
Start-Sleep -Seconds 2

# Monitor all logs in real time
Write-Host "Monitoring node logs (Ctrl+C to stop)..."
Get-Content logs\node*.log -Wait
