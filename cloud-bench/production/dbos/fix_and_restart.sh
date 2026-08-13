#!/bin/bash
# Fix DBOS password and restart service
set -e

# Kill old service
pkill -9 -f service.py 2>/dev/null || true
sleep 2

# Fix PostgreSQL password
sudo -u postgres psql << 'SQLEOF'
ALTER USER dbos WITH PASSWORD 'dbos_bench';
SQLEOF

# Restart service
cd ~/dbos-production
rm -f ~/dbos_service.log
export DBOS_DATABASE_URL="postgresql://dbos:dbos_bench@localhost:5432/dbos_bench"
export DBOS_HTTP_PORT=8080

nohup python3 service.py server > ~/dbos_service.log 2>&1 &
echo "Service started PID=$!"

# Wait for service
for i in $(seq 1 15); do
    if curl -s --max-time 2 http://localhost:8080/health > /dev/null 2>&1; then
        echo "Service ready!"
        break
    fi
    sleep 1
done

# Smoke test
echo "Running smoke test..."
curl -s -X POST http://localhost:8080/bench/simple_workflow \
    -H "Content-Type: application/json" -d '{}' --max-time 30
echo ""
echo "Done."
