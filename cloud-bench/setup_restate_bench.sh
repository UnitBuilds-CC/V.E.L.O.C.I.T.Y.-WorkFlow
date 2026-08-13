#!/bin/bash
set -e
cd ~/restate-bench

# Kill any existing bench service
pkill -f "node service.js" 2>/dev/null || true
sleep 1

# Create the benchmark service using correct Restate SDK API
cat > service.js << 'SERVICEEOF'
const restate = require("@restatedev/restate-sdk");

// Benchmark service — mirrors velocity-bench HTTP workloads
const benchService = restate.service({
  name: "bench",
  handlers: {
    invoke: async (ctx, input) => {
      return { status: "ok", handler: "invoke", input_size: input ? (typeof input === 'string' ? input.length : JSON.stringify(input).length) : 0 };
    },
    echo: async (ctx, input) => {
      return { status: "ok", handler: "echo", data: input };
    },
    stateful: async (ctx, input) => {
      const count = ((await ctx.get("counter")) || 0) + 1;
      ctx.set("counter", count);
      return { status: "ok", handler: "stateful", count };
    },
    multiStep: async (ctx, input) => {
      const steps = (input && input.steps) || 5;
      let result = 0;
      for (let i = 0; i < steps; i++) {
        const prev = (await ctx.get("step_result")) || 0;
        ctx.set("step_result", prev + 1);
        result = prev + 1;
      }
      return { status: "ok", handler: "multiStep", steps_completed: result };
    },
    payload: async (ctx, input) => {
      return { status: "ok", handler: "payload", size: input ? (typeof input === 'string' ? input.length : JSON.stringify(input).length) : 0 };
    },
    durablePromise: async (ctx, input) => {
      ctx.set("promise_result", { resolved: true });
      const result = await ctx.get("promise_result");
      return result;
    },
  },
});

// Keyed (virtual object) benchmark service
const keyedBenchService = restate.object({
  name: "keyed_bench",
  handlers: {
    stateful: async (ctx, input) => {
      const count = ((await ctx.get("counter")) || 0) + 1;
      ctx.set("counter", count);
      return { status: "ok", handler: "stateful", key: ctx.key, count };
    },
    invoke: async (ctx, input) => {
      return { status: "ok", handler: "invoke", key: ctx.key };
    },
  },
});

restate.serve({ services: [benchService, keyedBenchService], port: 9080 });
console.log("Restate bench service listening on port 9080");
SERVICEEOF

echo "Service file created"

# Start the service in background
echo "Starting Restate bench service..."
nohup node service.js > /tmp/restate_bench_svc.log 2>&1 &
echo "Service PID: $!"
sleep 3
cat /tmp/restate_bench_svc.log

# Check if service is listening
ss -tlnp | grep 9080 && echo "SERVICE LISTENING on 9080" || echo "SERVICE NOT LISTENING"

# Register the service with Restate using the correct CLI command
echo ""
echo "Registering deployment with Restate..."
# The Restate container uses host networking, so it can reach localhost:9080
docker exec restate restate dep register http://localhost:9080 2>&1 || echo "Registration might have failed"

sleep 2
echo ""
echo "=== Checking deployments ==="
docker exec restate restate dep list 2>&1 || echo "Could not list deployments"

echo ""
echo "=== Testing service ==="
# Test a simple invocation through Restate ingress
curl -s -X POST http://localhost:8080/bench/invoke -H 'Content-Type: application/json' -d '{}' 2>&1 | head -5
echo ""
echo "=== DONE ==="
