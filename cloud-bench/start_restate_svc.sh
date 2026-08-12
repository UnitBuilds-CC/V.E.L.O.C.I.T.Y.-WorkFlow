#!/bin/bash
set -e
cd ~/bench-service

# Kill existing
pkill -f "node service.js" 2>/dev/null || true
sleep 1

# Write service file with CORRECT API: endpoint().bind(service).listen(port)
cat > service.js << 'JSEOF'
const restate = require("@restatedev/restate-sdk");
const bench = restate.service({
  name: "Bench",
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
sleep 4

echo "=== Process check ==="
pgrep -af "node service.js" || echo "NOT RUNNING"

echo "=== Service log ==="
cat ~/restate_service.log

echo "=== Register with Restate ==="
# Use the Restate admin API to register the deployment
curl -s -X POST "http://localhost:9070/deployments" \
  -H "content-type: application/json" \
  -d '{"uri":"http://localhost:9080"}' 2>&1 || echo "admin register failed"

sleep 2

echo "=== Test via Restate ingress ==="
curl -s -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/json" \
  -d '{}' 2>&1
echo ""
echo "=== DONE ==="
