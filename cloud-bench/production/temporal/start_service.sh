#!/bin/bash
# Start Temporal benchmark service
set -e

export TEMPORAL_ADDRESS="localhost:7233"
export TEMPORAL_NAMESPACE="default"
export TEMPORAL_TASK_QUEUE="bench-queue"
export TEMPORAL_HTTP_PORT=8080
export PATH="$HOME/.local/bin:$PATH"

cd ~/temporal-production

# Kill any existing service
pkill -f "service.py" 2>/dev/null || true
sleep 2

# Start service
nohup python3 service.py server > ~/temporal_service.log 2>&1 &
echo "Service started PID=$!"

# Wait for ready
echo "Waiting for service..."
for i in $(seq 1 30); do
    if curl -s --max-time 2 http://localhost:8080/health > /dev/null 2>&1; then
        echo "Service is ready!"
        curl -s http://localhost:8080/health
        echo ""
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Service failed to start. Log:"
        cat ~/temporal_service.log | tail -30
        exit 1
    fi
    sleep 1
done
