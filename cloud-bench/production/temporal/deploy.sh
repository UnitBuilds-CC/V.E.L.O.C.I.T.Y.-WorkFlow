#!/bin/bash
# Temporal Production Benchmark — Deploy on a fresh GCE VM.
#
# Installs: Docker, Temporal CLI dev server, Python 3, temporalio, FastAPI
# Starts: Temporal dev server + benchmark service (FastAPI + Worker)
set -e

echo "=== Temporal Production Benchmark Deploy ==="

# ─── Docker ──────────────────────────────────────────────────────────────────
echo "[1/5] Installing Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com | sh
    sudo usermod -aG docker "$USER"
fi
sudo systemctl enable docker
sudo systemctl start docker
echo "Docker ready."

# ─── Temporal Dev Server ────────────────────────────────────────────────────
echo "[2/5] Starting Temporal dev server..."

# Use Temporal CLI dev server (single binary, real Temporal stack)
# Download if not present
if ! command -v temporal &> /dev/null; then
    echo "Downloading Temporal CLI..."
    # Use Docker to run Temporal server
    docker rm -f temporal-dev 2>/dev/null || true
    docker run -d \
        --name temporal-dev \
        --network host \
        --restart unless-stopped \
        temporalio/auto-setup:latest \
        server start-dev \
        --ip 0.0.0.0 \
        --port 7233 \
        --namespace default \
        --db-filename /tmp/temporal-dev.db
    echo "Temporal dev server started via Docker."
else
    # If temporal CLI is installed, use it directly
    pkill -f "temporal server" 2>/dev/null || true
    sleep 1
    nohup temporal server start-dev \
        --ip 0.0.0.0 \
        --port 7233 \
        --namespace default \
        --db-filename /tmp/temporal-dev.db > ~/temporal_server.log 2>&1 &
    echo "Temporal dev server started PID=$!"
fi

# Wait for Temporal to be ready
echo "Waiting for Temporal server to initialize..."
for i in $(seq 1 60); do
    if docker exec temporal-dev temporal operator cluster health 2>/dev/null | grep -q "SERVING"; then
        echo "Temporal server ready!"
        break
    fi
    # Fallback: try gRPC health check
    if python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(1)
try:
    s.connect(('localhost', 7233))
    s.close()
    exit(0)
except:
    exit(1)
" 2>/dev/null; then
        echo "Temporal server port 7233 is open."
        sleep 3  # Give it a moment to fully initialize
        break
    fi
    if [ $i -eq 60 ]; then
        echo "ERROR: Temporal failed to start within 60s"
        docker logs temporal-dev 2>/dev/null || cat ~/temporal_server.log 2>/dev/null
        exit 1
    fi
    sleep 1
done

# ─── Python Dependencies ─────────────────────────────────────────────────────
echo "[3/5] Installing Python dependencies..."
pip3 install --quiet temporalio fastapi uvicorn aiohttp 2>&1 | tail -3

# Verify temporalio
python3 -c "import temporalio; print('temporalio version:', temporalio.__version__)"

# ─── Deploy Service ──────────────────────────────────────────────────────────
echo "[4/5] Deploying Temporal benchmark service..."
BENCH_DIR=~/temporal-production
mkdir -p "$BENCH_DIR"

# Copy files (skip if already in place)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "$(realpath "$SCRIPT_DIR")" != "$(realpath "$BENCH_DIR")" ]; then
    cp "$SCRIPT_DIR/workflows.py" "$BENCH_DIR/workflows.py"
    cp "$SCRIPT_DIR/service.py" "$BENCH_DIR/service.py"
    cp "$SCRIPT_DIR/client.py" "$BENCH_DIR/client.py"
fi

# Kill any existing service
pkill -f "service.py" 2>/dev/null || true
sleep 1

# ─── Start Service ───────────────────────────────────────────────────────────
echo "[5/5] Starting Temporal benchmark service..."
export TEMPORAL_ADDRESS="localhost:7233"
export TEMPORAL_NAMESPACE="default"
export TEMPORAL_TASK_QUEUE="bench-queue"
export TEMPORAL_HTTP_PORT=8080

cd "$BENCH_DIR"
nohup python3 service.py server > ~/temporal_service.log 2>&1 &
SVC_PID=$!
echo "Temporal service started PID=$SVC_PID"

# Wait for service to be ready
echo "Waiting for service to initialize..."
for i in $(seq 1 30); do
    if curl -s --max-time 2 http://localhost:8080/health > /dev/null 2>&1; then
        echo "Service is ready!"
        curl -s http://localhost:8080/health
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: Service failed to start within 30s"
        cat ~/temporal_service.log
        exit 1
    fi
    sleep 1
done

# ─── Smoke Test ──────────────────────────────────────────────────────────────
echo ""
echo "Running smoke test..."
RESPONSE=$(curl -s -X POST http://localhost:8080/bench/simple_workflow \
    -H "Content-Type: application/json" -d '{}' --max-time 60)
echo "simple_workflow response: $RESPONSE"

if echo "$RESPONSE" | grep -q "completed"; then
    echo ""
    echo "=== Temporal Production Benchmark Ready ==="
    echo "Temporal server: localhost:7233"
    echo "Service: http://localhost:8080"
    echo "Run benchmark: python3 ~/temporal-production/client.py standard"
else
    echo "ERROR: Smoke test failed"
    echo "Service log:"
    cat ~/temporal_service.log | tail -30
    exit 1
fi
