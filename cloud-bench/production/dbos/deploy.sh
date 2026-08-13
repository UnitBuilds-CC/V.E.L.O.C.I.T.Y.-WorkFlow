#!/bin/bash
# DBOS Production Benchmark — Deploy on a fresh GCE VM.
#
# Installs: PostgreSQL 15, Python 3, DBOS SDK, aiohttp, FastAPI, uvicorn
# Creates: dbos_bench database + user
# Starts: DBOS benchmark service on port 8080
set -e

echo "=== DBOS Production Benchmark Deploy ==="

# ─── PostgreSQL ──────────────────────────────────────────────────────────────
echo "[1/5] Installing PostgreSQL..."
sudo apt-get update -qq
sudo apt-get install -y -qq postgresql postgresql-contrib > /dev/null 2>&1
sudo systemctl enable postgresql
sudo systemctl start postgresql

# Create database and user (always reset password to ensure consistency)
sudo -u postgres psql -c "CREATE USER dbos WITH PASSWORD 'dbos_bench';" 2>/dev/null || true
sudo -u postgres psql -c "ALTER USER dbos WITH PASSWORD 'dbos_bench';"
sudo -u postgres psql -c "CREATE DATABASE dbos_bench OWNER dbos;" 2>/dev/null || true
sudo -u postgres psql -d dbos_bench -c "GRANT ALL ON SCHEMA public TO dbos;" 2>/dev/null || true
echo "PostgreSQL ready."

# ─── Python Dependencies ─────────────────────────────────────────────────────
echo "[2/5] Installing Python dependencies..."
pip3 install --quiet dbos fastapi uvicorn aiohttp psycopg[binary] 2>&1 | tail -3

# Verify DBOS
python3 -c "from dbos import DBOS; print('DBOS imported successfully')"

# ─── Deploy Service ──────────────────────────────────────────────────────────
echo "[3/5] Deploying DBOS benchmark service..."
BENCH_DIR=~/dbos-production
mkdir -p "$BENCH_DIR"

# Copy service file (skip if already in place)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "$(realpath "$SCRIPT_DIR")" != "$(realpath "$BENCH_DIR")" ]; then
    cp "$SCRIPT_DIR/service.py" "$BENCH_DIR/service.py"
    cp "$SCRIPT_DIR/client.py" "$BENCH_DIR/client.py"
fi

# ─── Start Service ───────────────────────────────────────────────────────────
echo "[4/5] Starting DBOS service..."
pkill -f "service.py" 2>/dev/null || true
sleep 1

export DBOS_DATABASE_URL="postgresql://dbos:dbos_bench@localhost:5432/dbos_bench"
export DBOS_HTTP_PORT=8080

cd "$BENCH_DIR"
nohup python3 service.py server > ~/dbos_service.log 2>&1 &
SVC_PID=$!
echo "DBOS service started PID=$SVC_PID"

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
        cat ~/dbos_service.log
        exit 1
    fi
    sleep 1
done

# ─── Smoke Test ──────────────────────────────────────────────────────────────
echo ""
echo "[5/5] Running smoke test..."
RESPONSE=$(curl -s -X POST http://localhost:8080/bench/simple_workflow \
    -H "Content-Type: application/json" -d '{}' --max-time 30)
echo "simple_workflow response: $RESPONSE"

if echo "$RESPONSE" | grep -q "completed"; then
    echo ""
    echo "=== DBOS Production Benchmark Ready ==="
    echo "Service: http://localhost:8080"
    echo "Run benchmark: python3 ~/dbos-production/client.py standard"
else
    echo "ERROR: Smoke test failed"
    cat ~/dbos_service.log
    exit 1
fi
