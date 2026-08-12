#!/bin/bash
set -e
# Restart Temporal with correct PostgreSQL connection
docker rm -f temporal 2>/dev/null || true

# PostgreSQL is on 127.0.0.1:5432
docker run -d --name temporal \
  -e DB=postgres12 \
  -e DB_PORT=5432 \
  -e POSTGRES_USER=temporal \
  -e POSTGRES_PWD=temporal \
  -e POSTGRES_SEEDS=127.0.0.1 \
  --network host \
  temporalio/auto-setup:latest

echo "Temporal restarting..."
sleep 20
docker logs temporal 2>&1 | tail -10
echo "---"
ss -tlnp 2>/dev/null | grep 7233 || echo "Port 7233 not yet listening"
