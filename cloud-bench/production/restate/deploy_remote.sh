#!/bin/bash
# Deploy Restate production benchmark service
# Assumes Restate server is already running in Docker container 'restate'
set -e

echo "=== Restate Service Deploy ==="

# Extract files
BENCH_DIR=~/restate-production
rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR"
tar xf /tmp/restate_bench.tar -C "$BENCH_DIR" --strip-components=1
cd "$BENCH_DIR"

echo "Files extracted:"
ls -la

# Install Restate SDK
echo "Installing npm dependencies..."
npm init -y > /dev/null 2>&1
npm install @restatedev/restate-sdk 2>&1 | tail -5

# Kill any existing service
pkill -f "node service.js" 2>/dev/null || true
sleep 1

# Start the benchmark service
echo "Starting benchmark service..."
nohup node service.js > ~/restate_service.log 2>&1 &
SVC_PID=$!
echo "Service started PID=$SVC_PID"

# Wait for service to be ready
echo "Waiting for service on port 9080..."
for i in $(seq 1 20); do
    if ss -tlnp | grep -q 9080; then
        echo "Service listening on port 9080!"
        break
    fi
    if [ $i -eq 20 ]; then
        echo "ERROR: Service failed to start"
        cat ~/restate_service.log
        exit 1
    fi
    sleep 1
done

# Register service with Restate
echo "Registering service with Restate..."
docker exec restate restate deployments register http://localhost:9080 2>&1 || \
    echo "Registration attempt completed"
sleep 2

# List deployments and services
echo "Deployments:"
docker exec restate restate deployments list 2>&1 || echo "Could not list"
echo "Services:"
docker exec restate restate services list 2>&1 || echo "Could not list"

# Smoke test
echo ""
echo "Running smoke test..."
RESPONSE=$(curl -s -X POST http://localhost:8080/bench/simple \
    -H "Content-Type: application/json" -d '{}' --max-time 30)
echo "Response: $RESPONSE"

if echo "$RESPONSE" | grep -qi "completed\|status\|ok"; then
    echo ""
    echo "=== Restate Production Benchmark Ready ==="
else
    echo "Smoke test may have failed. Checking logs..."
    cat ~/restate_service.log | tail -20
fi
