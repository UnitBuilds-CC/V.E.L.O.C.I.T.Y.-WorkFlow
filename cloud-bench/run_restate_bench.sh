#!/bin/bash
set -e
export PATH="$HOME/.cargo/bin:$PATH"

BENCH=~/velocity-bench/target/release/velocity-bench-http

if [ ! -f "$BENCH" ]; then
    echo "ERROR: velocity-bench-http not found"
    exit 1
fi

echo "Starting Restate-only HTTP benchmark..."
echo "Restate is running on port 8080/9070"

# Run benchmark against Restate only
nohup $BENCH \
    --workloads all \
    --engine restate \
    --restate-address http://localhost:8080 \
    --runs 3 \
    --profile quick \
    --format all \
    --output ~/restate_results \
    > ~/restate_bench.log 2>&1 &
BENCH_PID=$!
echo "Restate benchmark started PID=$BENCH_PID"
