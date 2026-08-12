#!/bin/bash
set -e
export PATH="$HOME/.cargo/bin:$PATH"

# Kill any existing processes
pkill -9 -f velocity-bench 2>/dev/null || true
pkill -9 -f velocity-server 2>/dev/null || true
pkill -9 -f temporal-bridge 2>/dev/null || true
sleep 2

# Clean up old files
rm -f ~/temporal_bench.log ~/temporal_results.* ~/velocity.wal

# Use binaries from workspace build
BENCH_DIR=~/velocity-bench
BRIDGE=$BENCH_DIR/target/release/temporal-bridge
SERVER=$BENCH_DIR/target/release/velocity-server
BENCH=$BENCH_DIR/target/release/velocity-bench

# Check binaries exist
for f in $BRIDGE $SERVER $BENCH; do
    if [ ! -f "$f" ]; then
        echo "ERROR: Missing binary: $f"
        exit 1
    fi
done
echo "All binaries found"

# Start velocity-server on port 7233 (VELOCITY engine)
nohup $SERVER --ip 0.0.0.0 --grpc-port 7233 > ~/velocity_server.log 2>&1 &
SERVER_PID=$!
echo "velocity-server started PID=$SERVER_PID on port 7233"
sleep 3

if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: velocity-server failed to start"
    cat ~/velocity_server.log
    exit 1
fi

# Start temporal-bridge on port 7234 (Temporal engine simulation)
nohup $BRIDGE --grpc-port 7234 --ip 0.0.0.0 > ~/temporal_bridge.log 2>&1 &
BRIDGE_PID=$!
echo "temporal-bridge started PID=$BRIDGE_PID on port 7234"
sleep 2

if ! kill -0 $BRIDGE_PID 2>/dev/null; then
    echo "ERROR: temporal-bridge failed to start"
    cat ~/temporal_bridge.log
    exit 1
fi

echo "Both engines running. Starting benchmark..."

# Run benchmark with BOTH engines
nohup $BENCH \
    --workloads all \
    --profile quick \
    --runs 3 \
    --engine both \
    --velocity-address http://localhost:7233 \
    --temporal-address http://localhost:7234 \
    --format all \
    -o ~/temporal_results \
    > ~/temporal_bench.log 2>&1 &
BENCH_PID=$!
echo "Benchmark started PID=$BENCH_PID"
echo "Comparing VELOCITY vs Temporal (event-sourcing simulation)"
