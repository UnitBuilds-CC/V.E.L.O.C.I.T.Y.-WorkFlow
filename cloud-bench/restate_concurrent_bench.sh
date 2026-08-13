#!/bin/bash
echo "=== Restate Concurrent Benchmark ==="
echo "Testing with parallel requests (Restate's strength)"

# Install hey if not present (fast HTTP benchmarking tool)
if ! command -v hey &> /dev/null; then
    echo "Installing hey..."
    curl -sL https://github.com/rakyll/hey/releases/download/v0.1.4/hey-linux-amd64 -o /tmp/hey
    chmod +x /tmp/hey
    export PATH="/tmp:$PATH"
fi

echo ""
echo "--- 1000 concurrent requests (100 workers) ---"
hey -n 1000 -c 100 -m POST -H 'Content-Type: application/json' -d '{}' http://localhost:8080/bench/invoke 2>&1 | grep -E 'Summary|Requests/sec|Latency'

echo ""
echo "--- 5000 concurrent requests (500 workers) ---"
hey -n 5000 -c 500 -m POST -H 'Content-Type: application/json' -d '{}' http://localhost:8080/bench/invoke 2>&1 | grep -E 'Summary|Requests/sec|Latency'

echo ""
echo "--- Direct to Node.js service (bypass Restate, port 9080) ---"
# First check if direct HTTP works
curl -s -o /dev/null -w "HTTP_CODE:%{http_code}" http://localhost:9080/health 2>/dev/null || echo "No health endpoint"
# Try invoke directly
hey -n 1000 -c 100 -m POST -H 'Content-Type: application/json' -d '{}' http://localhost:9080/bench/invoke 2>&1 | grep -E 'Summary|Requests/sec|Latency' || echo "Direct benchmark failed"
