#!/bin/bash
# Restart Temporal service with fixed workflows and run benchmark
set -e

export PATH="$HOME/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export TEMPORAL_ADDRESS="localhost:7233"
export TEMPORAL_NAMESPACE="default"
export TEMPORAL_TASK_QUEUE="bench-queue"
export TEMPORAL_HTTP_PORT=8080

# Kill existing service and client
pkill -f "service.py" 2>/dev/null || true
pkill -f "client.py" 2>/dev/null || true
sleep 2

# Restart service
cd ~/temporal-production
nohup python3 service.py server > ~/temporal_service.log 2>&1 &
echo "Service restarted PID=$!"

# Wait for ready
for i in $(seq 1 30); do
    if curl -s --max-time 2 http://localhost:8080/health > /dev/null 2>&1; then
        echo "Service ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Service failed to start"
        cat ~/temporal_service.log | tail -20
        exit 1
    fi
    sleep 1
done

# Smoke test
echo "Smoke test..."
RESPONSE=$(curl -s -X POST http://localhost:8080/bench/simple_workflow \
    -H "Content-Type: application/json" -d '{}' --max-time 60)
echo "Response: $RESPONSE"

if echo "$RESPONSE" | grep -q "completed"; then
    echo "Smoke test passed! Running benchmark..."
    python3 client.py standard
else
    echo "Smoke test failed!"
    cat ~/temporal_service.log | tail -30
    exit 1
fi
