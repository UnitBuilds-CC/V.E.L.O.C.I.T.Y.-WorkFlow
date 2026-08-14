#!/bin/bash
# Fix Restate production service
set -e

echo "=== Fixing Restate Production Service ==="
BENCH_DIR=~/restate-production
cd "$BENCH_DIR"

# Fix: install correct package
echo "[1/4] Installing correct Restate SDK..."
rm -rf node_modules package-lock.json
npm init -y > /dev/null 2>&1
npm install @restatedev/restate-sdk 2>&1 | tail -5

# Verify SDK
echo "[2/4] Verifying SDK..."
node -e "const r = require('@restatedev/restate-sdk'); console.log('SDK loaded OK')" || {
    echo "ERROR: SDK failed to load"
    exit 1
}

# Restart service
echo "[3/4] Restarting service..."
pkill -f "node service.js" 2>/dev/null || true
sleep 2
nohup node service.js > ~/restate_service.log 2>&1 &
SVC_PID=$!
echo "Service started PID=$SVC_PID"

# Wait for port 9080
for i in $(seq 1 15); do
    if ss -tlnp 2>/dev/null | grep -q 9080; then
        echo "Service listening on port 9080!"
        break
    fi
    if [ $i -eq 15 ]; then
        echo "ERROR: Service not listening after 15s"
        cat ~/restate_service.log
        exit 1
    fi
    sleep 1
done

# Register with Restate server
echo "[4/4] Registering service with Restate..."
# Try both Docker exec and direct CLI
if docker ps | grep -q restate; then
    docker exec restate restate deployments register http://host.docker.internal:9080 2>&1 || \
    docker exec restate restate deployments register http://localhost:9080 2>&1 || \
    echo "Trying curl-based registration..."
    
    # Alternative: use the Restate admin API directly
    curl -s -X POST http://localhost:9070/deployments -H "Content-Type: application/json" \
        -d '{"uri":"http://localhost:9080"}' 2>/dev/null || true
    sleep 2
    
    echo "Deployments:"
    docker exec restate restate deployments list 2>&1 || true
    echo "Services:"
    docker exec restate restate services list 2>&1 || true
fi

# Smoke test
echo ""
echo "=== Smoke Test ==="
RESPONSE=$(curl -s -X POST http://localhost:8080/bench/simple \
    -H "Content-Type: application/json" -d '{"key":"test"}' --max-time 30)
echo "Response: $RESPONSE"

if echo "$RESPONSE" | grep -q "completed\|status\|ok"; then
    echo ""
    echo "=== Restate Service FIXED and Ready ==="
else
    echo "Smoke test may need retry. Checking logs:"
    tail -20 ~/restate_service.log
fi
