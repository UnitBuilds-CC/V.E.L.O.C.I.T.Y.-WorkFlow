#!/bin/bash
set -e

echo "=== Fixing DBOS PostgreSQL access ==="

# Reset the dbos user password
sudo -u postgres psql -c "ALTER ROLE dbos WITH PASSWORD 'dbos';"
sudo -u postgres psql -c "ALTER ROLE dbos WITH CREATEDB SUPERUSER LOGIN;"

# Verify connection
echo "Testing connection..."
PGPASSWORD=dbos psql -h localhost -U dbos -d dbos_bench -c "SELECT 1 AS test;"

# Create system database if not exists
sudo -u postgres psql -c "SELECT 1 FROM pg_database WHERE datname='dbos_bench_dbos_sys'" | grep -q 1 || \
    sudo -u postgres createdb -O dbos dbos_bench_dbos_sys

echo "Granting privileges..."
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE dbos_bench TO dbos;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE dbos_bench_dbos_sys TO dbos;"

echo "=== PostgreSQL setup complete ==="

# Kill any existing DBOS server
pkill -f "dbos_benchmark.py" 2>/dev/null || true
pkill -f "dbos_service.py" 2>/dev/null || true
sleep 2

# Start the DBOS benchmark server
echo "Starting DBOS benchmark server..."
nohup python3 ~/dbos_benchmark.py server > /tmp/dbos_server.log 2>&1 &
echo "Server PID: $!"
sleep 10

# Check if server started
echo "=== Server log ==="
cat /tmp/dbos_server.log | tail -20

echo ""
echo "=== Testing health ==="
curl -s http://localhost:8080/health 2>&1 || echo "HEALTH CHECK FAILED"

echo ""
echo "=== Testing bench invoke ==="
curl -s -X POST http://localhost:8080/bench/invoke -H 'Content-Type: application/json' -d 'test_data' 2>&1 || echo "BENCH INVOKE FAILED"

echo ""
echo "=== DONE ==="
