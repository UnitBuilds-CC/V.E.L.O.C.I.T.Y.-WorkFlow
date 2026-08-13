#!/bin/bash
echo "=== Restate Performance Diagnostic ==="

echo ""
echo "--- Direct to service (port 9080, no Restate) ---"
curl -s -w '\nHTTP:%{http_code} TIME:%{time_total}s\n' -X POST http://localhost:9080/bench/invoke -H 'Content-Type: application/json' -d '{}'

echo ""
echo "--- Via Restate ingress (port 8080, full durable path) ---"
curl -s -w '\nHTTP:%{http_code} TIME:%{time_total}s\n' -X POST http://localhost:8080/bench/invoke -H 'Content-Type: application/json' -d '{}'

echo ""
echo "--- 20 rapid-fire calls via ingress ---"
for i in $(seq 1 20); do
    curl -s -o /dev/null -w "%{time_total}s " -X POST http://localhost:8080/bench/invoke -H 'Content-Type: application/json' -d '{}'
done
echo ""

echo ""
echo "--- Restate version ---"
docker exec restate restate --version 2>&1

echo ""
echo "--- Restate container resources ---"
docker stats restate --no-stream 2>&1

echo ""
echo "--- Service node process resources ---"
ps aux | grep "node service" | grep -v grep

echo ""
echo "--- DBOS Performance Diagnostic ---"
echo ""
echo "--- Direct to DBOS server ---"
curl -s -w '\nHTTP:%{http_code} TIME:%{time_total}s\n' -X POST http://localhost:8080/bench/invoke -H 'Content-Type: application/json' -d 'test'

echo ""
echo "--- 20 rapid-fire DBOS calls ---"
for i in $(seq 1 20); do
    curl -s -o /dev/null -w "%{time_total}s " -X POST http://localhost:8080/bench/invoke -H 'Content-Type: application/json' -d 'test'
done
echo ""
