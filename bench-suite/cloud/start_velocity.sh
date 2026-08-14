#!/bin/bash
# Start Velocity server on a GCE VM
# Usage: start_velocity.sh [classic|runtime|embedded]
set -e

MODE="${1:-classic}"
REPO_DIR="$HOME/V.E.L.O.C.I.T.Y.-WorkFlow"
SERVER="$REPO_DIR/target/release/velocity-server"

echo "=== Starting Velocity ($MODE mode) ==="

# Ensure postgres is running for Velocity
if ! docker ps --format '{{.Names}}' | grep -q postgres; then
    echo "Starting PostgreSQL..."
    cd "$REPO_DIR"
    docker compose up -d postgres 2>&1 || {
        # If compose fails, start standalone postgres
        docker run -d --name postgres \
            -e POSTGRES_PASSWORD=velocity \
            -e POSTGRES_USER=velocity \
            -e POSTGRES_DB=velocity \
            -p 5432:5432 \
            postgres:16 2>&1 || true
    }
    sleep 3
fi

# Wait for postgres
for i in $(seq 1 10); do
    if pg_isready -h localhost -p 5432 -U velocity 2>/dev/null || docker exec $(docker ps -q --filter name=postgres) pg_isready 2>/dev/null; then
        echo "PostgreSQL ready"
        break
    fi
    echo "Waiting for postgres... ($i)"
    sleep 2
done

# Set WAL path based on mode
WAL_PATH="/tmp/velocity-${MODE}.wal"

# Kill any existing velocity-server
pkill -f velocity-server 2>/dev/null || true
sleep 1

# Start velocity-server
echo "Starting velocity-server (mode=$MODE, wal=$WAL_PATH)..."
nohup $SERVER \
    --ip 0.0.0.0 \
    --grpc-port 7234 \
    --real-engine \
    --wal-path "$WAL_PATH" \
    > /tmp/velocity-${MODE}.log 2>&1 &

echo "PID: $!"

# Wait for server to be ready
for i in $(seq 1 15); do
    if nc -z localhost 7234 2>/dev/null; then
        echo "Velocity ($MODE) ready on :7234"
        exit 0
    fi
    sleep 1
done

echo "ERROR: Velocity did not start"
cat /tmp/velocity-${MODE}.log | tail -20
exit 1
