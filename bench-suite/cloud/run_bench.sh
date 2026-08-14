#!/bin/bash
# Run velocity-bench on a VM
set -e
REPO_DIR="$HOME/V.E.L.O.C.I.T.Y.-WorkFlow"
BENCH="$REPO_DIR/target/release/velocity-bench"
PROFILE="${1:-quick}"

echo "=== Running benchmark (profile=$PROFILE) on $(hostname) ==="

$BENCH \
    --engine velocity \
    --velocity-address http://localhost:7234 \
    --workloads all \
    --profile "$PROFILE" \
    --format all \
    --output /tmp/bench_results 2>&1

echo ""
echo "=== Results ==="
cat /tmp/bench_results.md 2>/dev/null || echo "No markdown results"
echo ""
echo "=== Files ==="
ls -la /tmp/bench_results* 2>/dev/null
echo "=== Done ==="
