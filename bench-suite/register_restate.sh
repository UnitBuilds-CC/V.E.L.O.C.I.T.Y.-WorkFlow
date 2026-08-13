#!/bin/sh
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"uri":"http://bench-restate-service:9080"}' \
  http://localhost:9070/deployments
echo
