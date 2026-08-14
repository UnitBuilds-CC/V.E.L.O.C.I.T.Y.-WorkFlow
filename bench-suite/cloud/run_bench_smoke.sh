#!/bin/bash
# Run smoke benchmark with nohup
set -e
REPO_DIR="$HOME/V.E.L.O.C.I.T.Y.-WorkFlow"
BENCH="$REPO_DIR/target/release/velocity-bench"
ENGINE="${1:-velocity}"
ADDRESS="${2:-http://localhost:7234}"

echo "=== Running smoke benchmark (engine=$ENGINE, addr=$ADDRESS) ==="

nohup $BENCH \
    --engine "$ENGINE" \
    --velocity-address "$ADDRESS" \
    --temporal-address "$ADDRESS" \
    --workloads smoke \
    --profile quick \
    --format all \
    --output /tmp/bench_smoke \
    > /tmp/bench_smoke_stdout.log 2>&1 &

echo $! > /tmp/bench_smoke_pid
echo "PID: $!"
