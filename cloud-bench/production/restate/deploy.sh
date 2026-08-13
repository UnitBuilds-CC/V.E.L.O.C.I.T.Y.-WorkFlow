#!/bin/bash
# Restate Production Benchmark — Deploy on a fresh GCE VM.
#
# Installs: Docker, Node.js, Restate server, benchmark service
# Starts: Restate server (Docker) + benchmark service (Node.js on port 9080)
set -e

echo "=== Restate Production Benchmark Deploy ==="

# ─── Docker ──────────────────────────────────────────────────────────────────
echo "[1/5] Installing Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com | sh
    sudo usermod -aG docker "$USER"
fi
sudo systemctl enable docker
sudo systemctl start docker
echo "Docker ready."

# ─── Restate Server ──────────────────────────────────────────────────────────
echo "[2/5] Starting Restate server..."
docker rm -f restate 2>/dev/null || true
docker run -d \
    --name restate \
    --network host \
    --restart unless-stopped \
    docker.io/restatedev/restate:latest
echo "Restate server started."

# Wait for Restate to be ready
echo "Waiting for Restate to initialize..."
for i in $(seq 1 30); do
    if curl -s --max-time 2 http://localhost:9070/health > /dev/null 2>&1; then
        echo "Restate server ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: Restate failed to start"
        docker logs restate
        exit 1
    fi
    sleep 1
done

# ─── Node.js ─────────────────────────────────────────────────────────────────
echo "[3/5] Installing Node.js..."
if ! command -v node &> /dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash - > /dev/null 2>&1
    sudo apt-get install -y -qq nodejs > /dev/null 2>&1
fi
echo "Node.js $(node --version) ready."

# ─── Deploy Service ──────────────────────────────────────────────────────────
echo "[4/5] Deploying benchmark service..."
BENCH_DIR=~/restate-production
mkdir -p "$BENCH_DIR"

# Copy service file (skip if already in place)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "$(realpath "$SCRIPT_DIR")" != "$(realpath "$BENCH_DIR")" ]; then
    cp "$SCRIPT_DIR/service.js" "$BENCH_DIR/service.js"
    cp "$SCRIPT_DIR/client.js" "$BENCH_DIR/client.js"
fi

# Install Restate SDK
cd "$BENCH_DIR"
npm init -y > /dev/null 2>&1
npm install @restatedev/restate-sdk 2>&1 | tail -3

# Kill any existing service
pkill -f "node service.js" 2>/dev/null || true
sleep 1

# Start the benchmark service
nohup node service.js > ~/restate_service.log 2>&1 &
SVC_PID=$!
echo "Benchmark service started PID=$SVC_PID"

# Wait for service to be ready
for i in $(seq 1 15); do
    if ss -tlnp | grep -q 9080; then
        echo "Service listening on port 9080"
        break
    fi
    if [ $i -eq 15 ]; then
        echo "ERROR: Service failed to start"
        cat ~/restate_service.log
        exit 1
    fi
    sleep 1
done

# Register service with Restate
echo "Registering service with Restate..."
docker exec restate restate deployments register http://localhost:9080 2>&1 || \
    echo "Registration may have failed, checking..."
sleep 2

# List deployments
docker exec restate restate deployments list 2>&1 || echo "Could not list deployments"

# ─── Smoke Test ──────────────────────────────────────────────────────────────
echo ""
echo "[5/5] Running smoke test..."
RESPONSE=$(curl -s -X POST http://localhost:8080/bench/simple \
    -H "Content-Type: application/json" -d '{}' --max-time 30)
echo "simple_workflow response: $RESPONSE"

if echo "$RESPONSE" | grep -q "completed\|status"; then
    echo ""
    echo "=== Restate Production Benchmark Ready ==="
    echo "Restate ingress: http://localhost:8080"
    echo "Run benchmark: node ~/restate-production/client.js standard"
else
    echo "WARNING: Smoke test may have failed. Checking service status..."
    docker exec restate restate deployments list 2>&1
    docker exec restate restate services list 2>&1
    echo "Service log:"
    cat ~/restate_service.log | tail -20
fi
