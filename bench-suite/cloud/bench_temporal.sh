#!/bin/bash
# Run Temporal benchmark (smoke first)
set -e
echo "=== Temporal Smoke Benchmark ==="
cd ~/temporal-production
python3 client.py smoke --output /tmp/temporal_smoke.json 2>&1
echo ""
echo "=== Results ==="
cat /tmp/temporal_smoke.json 2>/dev/null | python3 -m json.tool 2>/dev/null || cat /tmp/temporal_smoke.json 2>/dev/null || echo "No results file"
