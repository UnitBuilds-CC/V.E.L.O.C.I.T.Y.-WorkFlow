#!/bin/bash
# Run Restate standard benchmark
set -e
echo "=== Restate Standard Benchmark ==="
cd ~/restate-production
node client.js standard --output /tmp/restate_standard.json 2>&1
echo ""
echo "=== Results ==="
node -e "
const data = JSON.parse(require('fs').readFileSync('/tmp/restate_standard.json','utf8'));
console.log('Engine:', data.engine || '?');
console.log('Profile:', data.profile || '?');
console.log('Total workloads:', (data.workloads||[]).length);
for (const w of data.workloads || []) {
    console.log('  ' + w.name + ': ' + w.ops_per_second.toFixed(1) + ' ops/s, p99=' + w.latency_p99_us.toFixed(0) + 'us, errors=' + (w.error_rate||0).toFixed(1) + '%');
}
" 2>/dev/null || cat /tmp/restate_standard.json 2>/dev/null || echo "No results"
