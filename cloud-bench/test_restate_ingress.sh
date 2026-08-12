#!/bin/bash
echo "=== Test 1: Direct to service ==="
curl -v --max-time 5 http://localhost:9080/ 2>&1 | head -20

echo ""
echo "=== Test 2: Via Restate ingress with verbose ==="
curl -v --max-time 10 -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/json" \
  -d '{}' 2>&1

echo ""
echo "=== Test 3: Check services list ==="
curl -s http://localhost:9070/services 2>&1

echo ""
echo "=== Test 4: Check if service node is healthy ==="
curl -v --max-time 5 -X POST http://localhost:9080/discover 2>&1 | head -20

echo ""
echo "=== Test 5: Service log ==="
cat ~/restate_service.log | tail -10

echo "=== DONE ==="
