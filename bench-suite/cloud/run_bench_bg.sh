#!/bin/bash
# Run velocity-bench with nohup so it survives SSH disconnect
set -e
REPO_DIR="$HOME/V.E.L.O.C.I.T.Y.-WorkFlow"
BENCH="$REPO_DIR/target/release/velocity-bench"
PROFILE="${1:-quick}"
ENGINE="${2:-velocity}"
ADDRESS="${3:-http://localhost:7234}"

echo "=== Starting benchmark (profile=$PROFILE, engine=$ENGINE, addr=$ADDRESS) ==="
echo "PID will be logged to /tmp/bench_pid"

nohup $BENCH \
    --engine "$ENGINE" \
    --velocity-address "$ADDRESS" \
    --temporal-address "$ADDRESS" \
    --workloads all \
    --profile "$PROFILE" \
    --format all \
    --output /tmp/bench_results \
    > /tmp/bench_stdout.log 2>&1 &

echo $! > /tmp/bench_pid
echo "Benchmark PID: $(cat /tmp/bench_pid)"
echo "Monitor with: tail -f /tmp/bench_stdout.log"
