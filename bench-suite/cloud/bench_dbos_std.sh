#!/bin/bash
# Run DBOS standard benchmark
set -e
echo "=== DBOS Standard Benchmark ==="
cd ~/dbos-production
python3 client.py standard --output /tmp/dbos_standard.json 2>&1
echo ""
echo "=== Results ==="
python3 -c "
import json
with open('/tmp/dbos_standard.json') as f:
    data = json.load(f)
print(f\"Engine: {data.get('engine','?')}\")
print(f\"Profile: {data.get('profile','?')}\")
print(f\"Total workloads: {len(data.get('workloads',[]))}\")
for w in data.get('workloads',[]):
    print(f\"  {w['name']}: {w['ops_per_second']:.1f} ops/s, p99={w['latency_p99_us']:.0f}us, errors={w['error_rate']:.1f}%\")
" 2>/dev/null || cat /tmp/dbos_standard.json 2>/dev/null || echo "No results"
