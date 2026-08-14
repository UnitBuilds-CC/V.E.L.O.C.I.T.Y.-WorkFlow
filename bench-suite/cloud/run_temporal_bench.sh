#!/bin/bash
# Run velocity-bench against Temporal on a VM
set -e
REPO_DIR="$HOME/V.E.L.O.C.I.T.Y.-WorkFlow"
BENCH="$REPO_DIR/target/release/velocity-bench"
PROFILE="${1:-quick}"

echo "=== Running Temporal benchmark (profile=$PROFILE) on $(hostname) ==="

$BENCH \
    --engine temporal \
    --temporal-address http://localhost:7233 \
    --workloads all \
    --profile "$PROFILE" \
    --format all \
    --output /tmp/bench_results_temporal 2>&1

echo ""
echo "=== Results ==="
cat /tmp/bench_results_temporal.md 2>/dev/null || echo "No markdown results"
echo "=== Done ==="
