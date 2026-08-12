#!/bin/bash
set -e
cd ~/bench-service

# Kill existing service
pkill -f "node service.js" 2>/dev/null || true
sleep 1

# Fix service to handle raw (non-JSON) input by wrapping in try/catch
cat > service.js << 'JSEOF'
const restate = require("@restatedev/restate-sdk");

const bench = restate.service({
  name: "bench",
  handlers: {
    async invoke(ctx, input) {
      return { status: "ok", ts: Date.now() };
    },
    async stateful(ctx, input) {
      const c = (await ctx.get("count")) || 0;
      await ctx.set("count", c + 1);
      return { count: c + 1 };
    },
    async echo(ctx, input) {
      return { echo: true, len: typeof input === "string" ? input.length : 0 };
    },
    async promise(ctx, input) {
      const id = await ctx.rand();
      return { id: String(id), status: "resolved" };
    },
  },
  // Accept any input type including raw bytes
  input: "application/octet-stream",
});

restate.endpoint().bind(bench).listen(9080);
JSEOF

echo "=== Starting service ==="
nohup node service.js > ~/restate_service.log 2>&1 &
sleep 3
pgrep -f "node service.js" && echo "RUNNING" || { echo "FAILED"; cat ~/restate_service.log; exit 1; }

echo "=== Re-register with Restate ==="
curl -s -X POST "http://localhost:9070/deployments" \
  -H "content-type: application/json" \
  -d '{"uri":"http://localhost:9080","force":true}' 2>&1 | python3 -c "import sys,json; d=json.load(sys.stdin); print('Services:', [s['name'] for s in d.get('services',[])])" 2>/dev/null || echo "registered"

sleep 2

echo "=== Quick test ==="
# Test with raw bytes (like the benchmark sends)
printf 'xxxxxxxxxx' | curl -s --max-time 5 -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/octet-stream" \
  --data-binary @- 2>&1
echo ""

# Test with JSON too
curl -s --max-time 5 -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/json" \
  -d '{}' 2>&1
echo ""
echo "=== DONE ==="
