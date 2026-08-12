#!/bin/bash
set -e
export PATH="$HOME/.local/bin:$PATH"

echo "=== Install dependencies ==="
pip3 install aiohttp 2>&1 | tail -3

echo "=== Create Velocity Runtime bench server ==="
cat > ~/velocity_bench_server.py << 'PYEOF'
"""Minimal Velocity Runtime HTTP server for benchmarking."""
import asyncio
import json
import time
import os
from aiohttp import web

# In-memory state
state = {}
call_count = 0

async def health(request):
    """Health check endpoint."""
    import resource
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # KB to MB
    return web.json_response({
        "status": "ok",
        "memory_rss_mb": round(mem, 1),
        "uptime": time.monotonic()
    })

async def invoke_handler(request):
    """Basic handler invocation - returns ok."""
    global call_count
    call_count += 1
    try:
        body = await request.json() if request.content_length else {}
    except:
        body = {}
    return web.json_response({"status": "ok", "ts": int(time.time() * 1000)})

async def stateful_handler(request):
    """Stateful handler with keyed state."""
    key = request.match_info.get("key", "default")
    try:
        body = await request.json() if request.content_length else {}
    except:
        body = {}
    
    if key not in state:
        state[key] = {"count": 0}
    state[key]["count"] += 1
    return web.json_response({"count": state[key]["count"]})

async def echo_handler(request):
    """Echo handler - returns input."""
    try:
        body = await request.json() if request.content_length else {}
    except:
        body = await request.text()
    return web.json_response(body) if isinstance(body, (dict, list)) else web.Response(text=str(body))

async def promise_handler(request):
    """Promise handler - simulates durable promise."""
    import hashlib
    try:
        body = await request.json() if request.content_length else {}
    except:
        body = {}
    promise_id = hashlib.md5(f"{time.time()}-{id(request)}".encode()).hexdigest()[:16]
    return web.json_response({"id": promise_id, "status": "resolved"})

# Routes matching velocity-bench-http URL pattern: /{service}/{handler}
app = web.Application()
app.router.add_get("/health", health)
# bench service routes
app.router.add_post("/bench/invoke", invoke_handler)
app.router.add_post("/bench/stateful", stateful_handler)
app.router.add_post("/bench/{key}/stateful", stateful_handler)  # keyed variant
app.router.add_post("/bench/echo", echo_handler)
app.router.add_post("/bench/promise", promise_handler)

if __name__ == "__main__":
    print(f"Starting Velocity Runtime bench server on port 8081...")
    web.run_app(app, host="0.0.0.0", port=8081, print=None)
PYEOF

echo "=== Kill existing servers ==="
pkill -f velocity_bench_server 2>/dev/null || true
pkill -f "node service.js" 2>/dev/null || true
sleep 1

echo "=== Start Restate service ==="
cd ~/bench-service
nohup node service.js > ~/restate_service.log 2>&1 &
sleep 3
pgrep -f "node service.js" && echo "Restate service: RUNNING" || echo "Restate service: FAILED"

echo "=== Start Velocity Runtime server ==="
nohup python3 ~/velocity_bench_server.py > ~/velocity_runtime.log 2>&1 &
sleep 2
pgrep -f velocity_bench_server && echo "Velocity Runtime: RUNNING" || echo "Velocity Runtime: FAILED"

echo "=== Test both endpoints ==="
echo -n "Velocity Runtime (8081): "
curl -s --max-time 3 -X POST http://localhost:8081/bench/invoke -H "content-type: application/json" -d '{}' 2>&1
echo ""
echo -n "Restate (8080): "
curl -s --max-time 5 -X POST http://localhost:8080/bench/invoke -H "content-type: application/json" -d '{}' 2>&1
echo ""

echo "=== Run HTTP benchmark ==="
cd ~/velocity-bench
./target/release/velocity-bench-http \
  --workloads all \
  --engine both \
  --velocity-address http://localhost:8081 \
  --restate-address http://localhost:8080 \
  --runs 3 \
  --profile quick \
  --format all \
  -o ~/http_bench_results 2>&1 | tee ~/http_bench.log

echo ""
echo "=== BENCHMARK COMPLETE ==="
echo "Results:"
ls -la ~/http_bench_results.* 2>/dev/null
cat ~/http_bench_results.md 2>/dev/null || echo "No markdown results"
