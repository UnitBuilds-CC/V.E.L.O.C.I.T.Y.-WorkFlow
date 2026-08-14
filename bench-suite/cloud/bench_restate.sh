#!/bin/bash
# Run Restate benchmark (smoke first)
set -e
echo "=== Restate Smoke Benchmark ==="
cd ~/restate-production
node client.js smoke --output /tmp/restate_smoke.json 2>&1
echo ""
echo "=== Results ==="
cat /tmp/restate_smoke.json 2>/dev/null | python3 -m json.tool 2>/dev/null || cat /tmp/restate_smoke.json 2>/dev/null || echo "No results file"
