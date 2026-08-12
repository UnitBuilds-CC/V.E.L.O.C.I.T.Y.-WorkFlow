#!/bin/bash
set -e
cd ~/bench-service

pkill -f "node service.js" 2>/dev/null || true
sleep 1

# Try using Buffer type for raw binary input
cat > service.js << 'JSEOF'
const restate = require("@restatedev/restate-sdk");

const bench = restate.service({
  name: "bench",
  handlers: {
    async invoke(ctx) {
      return { status: "ok", ts: Date.now() };
    },
    async stateful(ctx) {
      const c = (await ctx.get("count")) || 0;
      await ctx.set("count", c + 1);
      return { count: c + 1 };
    },
    async echo(ctx) {
      return { echo: true };
    },
    async promise(ctx) {
      const id = await ctx.rand();
      return { id: String(id), status: "resolved" };
    },
  },
});

restate.endpoint().bind(bench).listen(9080);
JSEOF

nohup node service.js > ~/restate_service.log 2>&1 &
sleep 3
pgrep -f "node service.js" && echo "RUNNING" || { echo "FAILED"; cat ~/restate_service.log; exit 1; }

# Re-register
curl -s -X POST "http://localhost:9070/deployments" \
  -H "content-type: application/json" \
  -d '{"uri":"http://localhost:9080","force":true}' > /dev/null 2>&1
sleep 2

# Test with raw bytes AND content-type: application/json (matching what benchmark sends)
echo -n "Test raw bytes with json content-type: "
curl -s --max-time 5 -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/json" \
  -d 'xxxxxxxxxx' 2>&1
echo ""

echo -n "Test empty JSON: "
curl -s --max-time 5 -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/json" \
  -d '{}' 2>&1
echo ""
echo "DONE"
