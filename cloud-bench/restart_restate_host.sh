#!/bin/bash
set -e

echo "=== Stopping Restate container ==="
docker stop restate 2>/dev/null || true
docker rm restate 2>/dev/null || true

echo "=== Starting Restate with --network host ==="
docker run -d --name restate --network host \
  restatedev/restate:latest

echo "=== Waiting for Restate to start ==="
sleep 5

# Check Restate is running
docker ps | grep restate
echo "=== Testing Restate ==="
curl -s http://localhost:8080/ 2>&1 | head -3
curl -s http://localhost:9070/health 2>&1 | head -3

echo "=== Registering deployment ==="
# The node service is already running on port 9080
RESP=$(curl -s -X POST "http://localhost:9070/deployments" \
  -H "content-type: application/json" \
  -d '{"uri":"http://localhost:9080"}' 2>&1)
echo "Register response: $RESP"

sleep 2

echo "=== Testing via ingress ==="
RESP2=$(curl -s -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/json" \
  -d '{}' 2>&1)
echo "Ingress response: $RESP2"

echo "=== DONE ==="
