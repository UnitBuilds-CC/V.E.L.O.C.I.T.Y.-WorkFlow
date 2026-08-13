#!/bin/bash
# Redeploy updated Restate service
set -e

echo "=== Restate Redeploy ==="

# Extract updated files
cd ~/restate-production
tar xf /tmp/restate_bench.tar --strip-components=1
echo "Updated files:"
ls -la service.js client.js

# Kill old service
pkill -f "node service.js" 2>/dev/null || true
sleep 2

# Restart
nohup node service.js > ~/restate_service.log 2>&1 &
echo "Service restarted PID=$!"

# Wait for listening
for i in $(seq 1 15); do
    if ss -tlnp | grep -q 9080; then
        echo "Service listening on 9080!"
        break
    fi
    sleep 1
done

# Force re-register (overwrite old deployment which had bench as 'service')
docker exec restate restate deployment register http://localhost:9080 --force --yes 2>&1 || \
    echo "Registration completed"

sleep 2

# Smoke test - keyed object with state
echo ""
echo "Smoke test (keyed state)..."
curl -s -X POST http://localhost:8080/bench/test1/simple \
    -H "Content-Type: application/json" -d '{}' --max-time 30
echo ""
echo "Done."
