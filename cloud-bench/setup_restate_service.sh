#!/bin/bash
set -e
cd ~/bench-service

# Create the benchmark service with correct API
cat > service.js << 'SERVICEEOF'
const restate = require("@restatedev/restate-sdk");

const bench = restate.service({
  name: "Bench",
  handlers: {
    async invoke(ctx, input) {
      return { status: "ok", ts: Date.now() };
    },
    async stateful(ctx, input) {
      const count = (await ctx.get("count")) || 0;
      await ctx.set("count", count + 1);
      return { count: count + 1 };
    },
    async echo(ctx, input) {
      return input;
    },
    async promise(ctx, input) {
      const id = await ctx.rand();
      return { id: id, status: "resolved" };
    },
  },
});

restate
  .endpoint({ port: 9080 })
  .listen(bench);
SERVICEEOF

echo "Service file created."

# Kill any existing service
pkill -f "node service.js" 2>/dev/null || true
sleep 1

# Start the service
nohup node service.js > ~/restate_service.log 2>&1 &
echo "Service starting..."
sleep 3

# Check if service is running
if pgrep -f "node service.js" > /dev/null; then
  echo "Service is running on port 9080"
  cat ~/restate_service.log
else
  echo "Service failed to start"
  cat ~/restate_service.log
  exit 1
fi

# Wait for Restate to discover the service
echo "Waiting for Restate to discover service..."
sleep 3

# Register service with Restate using the CLI
echo "Registering service with Restate..."
npx @restatedev/restate deployments register http://localhost:9080 2>&1 || echo "CLI register failed, trying manual..."

# Test the service via Restate ingress
echo "Testing via Restate ingress (port 8080)..."
RESPONSE=$(curl -s -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/json" \
  -d '{}' 2>&1)
echo "Response: $RESPONSE"

if echo "$RESPONSE" | grep -q "status\|ok"; then
  echo "SUCCESS: Restate service is working!"
else
  echo "Service not yet registered. Checking status..."
  npx @restatedev/restate deployments list 2>&1 || echo "no deployments"
fi
