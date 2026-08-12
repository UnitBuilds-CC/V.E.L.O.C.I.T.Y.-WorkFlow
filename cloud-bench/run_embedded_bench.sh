#!/bin/bash
set -e
export PATH="$HOME/.cargo/bin:$PATH"

# Kill any existing processes
pkill -9 -f velocity-bench 2>/dev/null || true
pkill -9 -f velocity-server 2>/dev/null || true
sleep 2

# Clean up old files
rm -f ~/bench.log ~/bench_results.* ~/vel-bench/velocity.wal

# Start server
cd ~/vel-bench
nohup ./target/release/velocity-server --ip 0.0.0.0 > ~/server.log 2>&1 &
SERVER_PID=$!
echo "Server started PID=$SERVER_PID"
sleep 3

# Verify server
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server failed to start"
    cat ~/server.log
    exit 1
fi
echo "Server running OK"

# Start benchmark
nohup ./target/release/velocity-bench \
    --workloads all \
    --profile quick \
    --runs 3 \
    --engine velocity \
    --format all \
    -o ~/bench_results \
    > ~/bench.log 2>&1 &
BENCH_PID=$!
echo "Embedded benchmark started PID=$BENCH_PID"
echo "QUICK profile: 10 workflows, concurrency 4"
