#!/bin/sh
# Start the Restate bench service, then register it with the Restate server.
# The SDK's serve() starts an HTTP server — the admin API registration is
# a separate step that tells the server where to find the handlers.

ADMIN_URL="${RESTATE_ADMIN_URL:-http://bench-restate-server:9070}"
SERVICE_URL="${RESTATE_SERVICE_URL:-http://bench-restate-service:9080}"

# Start the Node.js service in the background
node service.js &
NODE_PID=$!

# Give the SDK a moment to start listening
sleep 3

# Wait for Restate admin API to be ready
echo "Waiting for Restate admin at ${ADMIN_URL}..."
for i in $(seq 1 60); do
  if curl -sf "${ADMIN_URL}/health" -o /dev/null 2>&1; then
    echo "Restate admin ready."
    break
  fi
  sleep 1
done

# Register the service with the Restate server
echo "Registering bench service at ${SERVICE_URL}..."
RESP=$(curl -s -X POST "${ADMIN_URL}/deployments" \
  -H 'Content-Type: application/json' \
  -d "{\"uri\":\"${SERVICE_URL}\"}")
echo "Registration response: ${RESP}"

# Keep the container running by waiting on the Node.js process
wait $NODE_PID
