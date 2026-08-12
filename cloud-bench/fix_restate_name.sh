#!/bin/bash
set -e
cd ~/bench-service

# Kill existing service
pkill -f "node service.js" 2>/dev/null || true
sleep 1

# Fix service name to lowercase "bench" to match benchmark URL pattern
cat > service.js << 'JSEOF'
const restate = require("@restatedev/restate-sdk");
const bench = restate.service({
  name: "bench",
  handlers: {
    async invoke(ctx, input) { return { status: "ok", ts: Date.now() }; },
    async stateful(ctx, input) { const c = (await ctx.get("count")) || 0; await ctx.set("count", c+1); return { count: c+1 }; },
    async echo(ctx, input) { return input; },
    async promise(ctx, input) { const id = await ctx.rand(); return { id: id, status: "resolved" }; },
  },
});
restate.endpoint().bind(bench).listen(9080);
JSEOF

echo "=== Starting service ==="
nohup node service.js > ~/restate_service.log 2>&1 &
sleep 3

echo "=== Process check ==="
pgrep -af "node service.js" || { echo "NOT RUNNING"; cat ~/restate_service.log; exit 1; }

echo "=== Create new deployment ==="
RESP=$(curl -s -X POST "http://localhost:9070/deployments" \
  -H "content-type: application/json" \
  -d '{"uri":"http://localhost:9080","force":true}' 2>&1)
echo "Register: $RESP"

sleep 2

echo "=== Test via ingress ==="
RESP2=$(curl -s -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/json" \
  -d '{}' 2>&1)
echo "Response: $RESP2"

if echo "$RESP2" | grep -q "status"; then
  echo "=== SUCCESS ==="
else
  echo "=== Trying to list services ==="
  curl -s http://localhost:9070/services 2>&1 | head -20
fi
