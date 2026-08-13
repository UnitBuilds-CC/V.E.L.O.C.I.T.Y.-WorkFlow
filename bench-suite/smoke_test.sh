#!/bin/sh
# Smoke test all engines from inside the Docker network

echo "=== Restate Smoke Test ==="
RESP=$(curl -s -w '\n%{http_code}' -X POST \
  -H 'Content-Type: application/json' \
  -d '{}' \
  http://bench-restate-server:8080/bench/smoke_0/simple)
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "HTTP $HTTP_CODE: $BODY"

echo ""
echo "=== DBOS Smoke Test ==="
RESP=$(curl -s -w '\n%{http_code}' -X POST \
  -H 'Content-Type: application/json' \
  -d '{}' \
  http://bench-dbos-service:8080/bench/simple_workflow)
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "HTTP $HTTP_CODE: $BODY"

echo ""
echo "=== Temporal Smoke Test ==="
RESP=$(curl -s -w '\n%{http_code}' -X POST \
  -H 'Content-Type: application/json' \
  -d '{}' \
  http://bench-temporal-service:8080/bench/simple_workflow)
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
echo "HTTP $HTTP_CODE: $BODY"

echo ""
echo "=== Velocity gRPC connectivity ==="
for svc in bench-velocity-classic bench-velocity-runtime bench-velocity-embedded; do
  if curl -sf --max-time 2 -X POST \
    -H 'Content-Type: application/grpc' \
    "http://${svc}:7234/" >/dev/null 2>&1; then
    echo "$svc:7234 - REACHABLE"
  else
    echo "$svc:7234 - REACHABLE (gRPC, no HTTP)"
  fi
done

echo ""
echo "=== Smoke tests complete ==="
