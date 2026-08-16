#!/bin/sh
curl -s -X POST http://localhost:9070/deployments \
  -H 'Content-Type: application/json' \
  -d '{"uri":"http://bench-restate-service:9080"}'
echo ""
