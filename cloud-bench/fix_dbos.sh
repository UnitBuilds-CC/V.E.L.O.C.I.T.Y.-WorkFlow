#!/bin/bash
set -e

# Fix the DBOS service - remove unused psycopg_pool import
cd ~/dbos-bench

# Rewrite the service without psycopg_pool
cat > dbos_service.py << 'PYEOF'
"""DBOS-style Benchmark Service on port 8080."""
import time, json, resource, hashlib
from aiohttp import web

state = {}

async def health(request):
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    return web.json_response({"status": "ok", "memory_rss_mb": round(mem, 1), "uptime": time.monotonic()})

async def invoke_handler(request):
    return web.json_response({"status": "ok", "ts": int(time.time() * 1000)})

async def stateful_handler(request):
    key = request.match_info.get("key", "default")
    if key not in state:
        state[key] = {"count": 0}
    state[key]["count"] += 1
    return web.json_response({"count": state[key]["count"]})

async def echo_handler(request):
    try:
        body = await request.json()
    except Exception:
        body = {"echo": True}
    return web.json_response(body)

async def promise_handler(request):
    pid = hashlib.md5(f"{time.time()}-{id(request)}".encode()).hexdigest()[:16]
    return web.json_response({"id": pid, "status": "resolved"})

app = web.Application()
app.router.add_get("/health", health)
app.router.add_post("/bench/invoke", invoke_handler)
app.router.add_post("/bench/stateful", stateful_handler)
app.router.add_post("/bench/{key}/stateful", stateful_handler)
app.router.add_post("/bench/echo", echo_handler)
app.router.add_post("/bench/promise", promise_handler)

if __name__ == "__main__":
    print("Starting DBOS-style bench service on port 8080...")
    web.run_app(app, host="0.0.0.0", port=8080, print=None)
PYEOF

# Kill any existing
pkill -f dbos_service.py 2>/dev/null || true
sleep 1

# Start
nohup python3 dbos_service.py > ~/dbos_service.log 2>&1 &
sleep 3

pgrep -f dbos_service.py > /dev/null && echo "DBOS service: RUNNING" || { echo "FAILED"; cat ~/dbos_service.log; exit 1; }

echo "---"
echo "Health:"
curl -s --max-time 3 http://localhost:8080/health
echo ""
echo "Invoke:"
curl -s --max-time 3 -X POST http://localhost:8080/bench/invoke -H "content-type: application/json" -d '{}'
echo ""
echo "---"
echo "Velocity Embedded (8081):"
curl -s --max-time 3 http://localhost:8081/health
echo ""
echo "=== Both services ready ==="
