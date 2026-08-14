#!/bin/bash
# Run DBOS benchmark (smoke first)
set -e
echo "=== DBOS Smoke Benchmark ==="
cd ~/dbos-production
python3 client.py smoke --output /tmp/dbos_smoke.json 2>&1
echo ""
echo "=== Results ==="
cat /tmp/dbos_smoke.json 2>/dev/null | python3 -m json.tool 2>/dev/null || cat /tmp/dbos_smoke.json 2>/dev/null || echo "No results file"
