#!/bin/bash
set -e
cd ~/velocity-bench

echo "=== Running HTTP benchmark: Velocity Runtime vs Restate ==="
./target/release/velocity-bench-http \
  --workloads all \
  --engine both \
  --velocity-address http://localhost:8081 \
  --restate-address http://localhost:8080 \
  --runs 3 \
  --profile quick \
  --format all \
  --output ~/http_bench_results 2>&1 | tee ~/http_bench.log

echo ""
echo "=== BENCHMARK COMPLETE ==="
echo "Results:"
ls -la ~/http_bench_results.* 2>/dev/null
echo "---"
cat ~/http_bench_results.md 2>/dev/null || echo "No markdown results yet"
