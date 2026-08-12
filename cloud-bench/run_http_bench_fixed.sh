#!/bin/bash
set -e

echo "=== Checking services ==="
pgrep -f "node service.js" > /dev/null && echo "Node service: RUNNING" || echo "Node service: DOWN"
pgrep -f restate_json_proxy > /dev/null && echo "Proxy: RUNNING (killing)" || echo "Proxy: DOWN"
docker ps --format '{{.Names}} {{.Status}}' 2>/dev/null

# Kill proxy — no longer needed with fixed bench binary
pkill -f restate_json_proxy 2>/dev/null || true

echo "---"
echo "Testing Velocity Runtime (port 8081):"
curl -s --max-time 3 http://localhost:8081/health 2>&1 || echo "DOWN"
echo ""

echo "---"
echo "Testing Restate (port 8080):"
curl -s --max-time 3 -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/json" -d '{}' 2>&1
echo ""

echo "---"
echo "Testing Restate with JSON-wrapped payload (what fixed bench sends):"
curl -s --max-time 3 -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/json" \
  -d '{"data":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}' 2>&1
echo ""

echo "=== Running HTTP benchmark (fixed binary, JSON payloads for Restate) ==="
export PATH="/home/ian_unitbuilds_com/.cargo/bin:$PATH"
cd ~/velocity-bench

# Remove old results
rm -f ~/http_bench_results_fixed.*

./target/release/velocity-bench-http \
  --workloads all \
  --engine both \
  --velocity-address http://localhost:8081 \
  --restate-address http://localhost:8080 \
  --runs 3 \
  --profile standard \
  --format all \
  --output ~/http_bench_results_fixed 2>&1 | tee ~/http_bench_fixed.log

echo ""
echo "=== RESULTS (Fixed) ==="
cat ~/http_bench_results_fixed.md 2>/dev/null
echo ""
echo "=== DONE ==="
