#!/bin/bash
# Deploy Temporal production benchmark service
# Assumes Temporal server is already running in Docker container 'temporal'
set -e

echo "=== Temporal Service Deploy ==="

# Extract files
BENCH_DIR=~/temporal-production
rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR"
tar xf /tmp/temporal_bench.tar -C "$BENCH_DIR" --strip-components=1
cd "$BENCH_DIR"

echo "Files extracted:"
ls -la

# Install Python dependencies
echo "Installing Python dependencies..."
pip3 install --quiet temporalio fastapi uvicorn aiohttp 2>&1 | tail -5

# Verify temporalio
python3 -c "import temporalio; print('temporalio imported successfully')"

# Kill any existing service
pkill -f "service.py" 2>/dev/null || true
sleep 2

# Check Temporal server connectivity
echo "Checking Temporal server on port 7233..."
python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(3)
try:
    s.connect(('localhost', 7233))
    print('Temporal server port 7233 is open')
    s.close()
except Exception as e:
    print(f'Cannot connect to Temporal: {e}')
    exit(1)
"

# Start the benchmark service
echo "Starting Temporal benchmark service..."
export TEMPORAL_ADDRESS="localhost:7233"
export TEMPORAL_NAMESPACE="default"
export TEMPORAL_TASK_QUEUE="bench-queue"
export TEMPORAL_HTTP_PORT=8080

nohup python3 service.py server > ~/temporal_service.log 2>&1 &
SVC_PID=$!
echo "Service started PID=$SVC_PID"

# Wait for service to be ready
echo "Waiting for service on port 8080..."
for i in $(seq 1 30); do
    if curl -s --max-time 2 http://localhost:8080/health > /dev/null 2>&1; then
        echo "Service is ready!"
        curl -s http://localhost:8080/health
        echo ""
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: Service failed to start within 30s"
        cat ~/temporal_service.log | tail -30
        exit 1
    fi
    sleep 1
done

# Smoke test
echo ""
echo "Running smoke test..."
RESPONSE=$(curl -s -X POST http://localhost:8080/bench/simple_workflow \
    -H "Content-Type: application/json" -d '{}' --max-time 60)
echo "Response: $RESPONSE"

if echo "$RESPONSE" | grep -q "completed"; then
    echo ""
    echo "=== Temporal Production Benchmark Ready ==="
else
    echo "Smoke test may have failed. Checking logs..."
    cat ~/temporal_service.log | tail -30
fi
