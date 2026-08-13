#!/bin/bash
# Deploy fair benchmark changes to a Velocity VM.
# Usage: bash deploy_fair_bench.sh
# Run from the VM's repo root (~/velocity-bench or similar).
set -e

REPO_ROOT="${REPO_ROOT:-$HOME/velocity-bench}"
cd "$REPO_ROOT"

echo "=== Updating source files ==="

# The changed files are uploaded to ~/patch/ before running this script.
if [ ! -d "$HOME/patch" ]; then
    echo "ERROR: ~/patch/ directory not found. Upload changed files first."
    exit 1
fi

# Copy patched files into the repo
cp -v "$HOME/patch/benchmark.proto" velocity-bench/proto/benchmark.proto
cp -v "$HOME/patch/main_server.rs" velocity-workflow-server/src/main.rs
cp -v "$HOME/patch/engine.rs" velocity-bench/src/engine.rs
cp -v "$HOME/patch/main_bench.rs" velocity-bench/src/main.rs
cp -v "$HOME/patch/temporal_bridge.rs" velocity-bench/src/temporal_bridge.rs

echo "=== Building release binaries ==="
cargo build --release --bin velocity-server --bin velocity-bench 2>&1 | tail -5

# Copy binaries to well-known locations
cp target/release/velocity-server "$HOME/velocity-server"
cp target/release/velocity-bench "$HOME/velocity-bench-bin"

echo "=== Restarting velocity-server ==="
# Kill existing server
pkill -f "velocity-server.*--real-engine" || true
sleep 2

# Start server with real engine
nohup "$HOME/velocity-server" --real-engine --ip 0.0.0.0 --grpc-port 7235 > /tmp/velocity-server.log 2>&1 &
sleep 3

# Verify server is running
if pgrep -f "velocity-server.*--real-engine" > /dev/null; then
    echo "Server started successfully (PID: $(pgrep -f 'velocity-server.*--real-engine'))"
else
    echo "ERROR: Server failed to start. Check /tmp/velocity-server.log"
    tail -20 /tmp/velocity-server.log
    exit 1
fi

echo "=== Running signal_storm benchmark (3 runs) ==="
"$HOME/velocity-bench-bin" \
    --workload signal_storm \
    --velocity-address http://localhost:7235 \
    --engine velocity \
    --format json \
    -o "$HOME/results/signal_storm_fair.json" \
    --profile standard \
    --runs 3

echo "=== Results ==="
cat "$HOME/results/signal_storm_fair.json" | python3 -m json.tool 2>/dev/null || cat "$HOME/results/signal_storm_fair.json"
echo ""
echo "=== DONE ==="
