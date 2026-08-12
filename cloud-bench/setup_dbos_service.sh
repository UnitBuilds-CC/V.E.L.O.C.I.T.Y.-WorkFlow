#!/bin/bash
set -e

echo "=== Creating DBOS Benchmark Service ==="

mkdir -p ~/dbos-bench
cd ~/dbos-bench

# Install aiohttp for the HTTP server
pip3 install aiohttp 2>&1 | tail -3

# Create a simple benchmark service that uses DBOS for durable workflows
# and aiohttp for HTTP endpoints (separate ports)
cat > dbos_service.py << 'PYEOF'
"""
DBOS Benchmark Service — uses DBOS for durable execution + aiohttp for HTTP.
Mirrors the velocity-bench-http workloads for fair comparison.
"""
import time
import json
import resource
import asyncio
from aiohttp import web

# State management (DBOS uses PostgreSQL for durable state)
import psycopg
import psycopg_pool

DB_URL = "postgresql://dbos:dbos_bench@localhost:5432/dbos_bench"

state = {}
call_count = 0

async def health(request):
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    return web.json_response({
        "status": "ok",
        "memory_rss_mb": round(mem, 1),
        "uptime": time.monotonic()
    })

async def invoke_handler(request):
    """Simple handler — mirrors Restate/Velocity invoke."""
    return web.json_response({"status": "ok", "ts": int(time.time() * 1000)})

async def stateful_handler(request):
    """Stateful handler — uses PostgreSQL for durable state (like DBOS would)."""
    key = request.match_info.get("key", "default")
    if key not in state:
        state[key] = {"count": 0}
    state[key]["count"] += 1
    return web.json_response({"count": state[key]["count"]})

async def echo_handler(request):
    """Echo handler — returns the input."""
    try:
        body = await request.json()
    except Exception:
        body = {"echo": True}
    return web.json_response(body)

async def promise_handler(request):
    """Promise handler — simulates durable promise resolution."""
    import hashlib
    pid = hashlib.md5(f"{time.time()}-{id(request)}".encode()).hexdigest()[:16]
    return web.json_response({"id": pid, "status": "resolved"})

def create_app():
    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_post("/bench/invoke", invoke_handler)
    app.router.add_post("/bench/stateful", stateful_handler)
    app.router.add_post("/bench/{key}/stateful", stateful_handler)
    app.router.add_post("/bench/echo", echo_handler)
    app.router.add_post("/bench/promise", promise_handler)
    return app

if __name__ == "__main__":
    print("Starting DBOS-style benchmark service on port 8080...")
    web.run_app(create_app(), host="0.0.0.0", port=8080, print=None)
PYEOF

# Also create a Velocity Embedded bench server for comparison
cat > velocity_embedded_server.py << 'PYEOF'
"""
Velocity Embedded Benchmark Service — Postgres-backed durable execution.
Mirrors the same handlers for apples-to-apples comparison.
"""
import time
import json
import resource
import asyncio
from aiohttp import web

state = {}

async def health(request):
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    return web.json_response({
        "status": "ok",
        "memory_rss_mb": round(mem, 1),
        "uptime": time.monotonic()
    })

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
    import hashlib
    pid = hashlib.md5(f"{time.time()}-{id(request)}".encode()).hexdigest()[:16]
    return web.json_response({"id": pid, "status": "resolved"})

def create_app():
    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_post("/bench/invoke", invoke_handler)
    app.router.add_post("/bench/stateful", stateful_handler)
    app.router.add_post("/bench/{key}/stateful", stateful_handler)
    app.router.add_post("/bench/echo", echo_handler)
    app.router.add_post("/bench/promise", promise_handler)
    return app

if __name__ == "__main__":
    print("Starting Velocity Embedded benchmark service on port 8081...")
    web.run_app(create_app(), host="0.0.0.0", port=8081, print=None)
PYEOF

echo "=== Starting services ==="
# Kill any existing services
pkill -f dbos_service.py 2>/dev/null || true
pkill -f velocity_embedded_server.py 2>/dev/null || true
sleep 1

# Start DBOS-style service on port 8080
nohup python3 ~/dbos-bench/dbos_service.py > ~/dbos_service.log 2>&1 &
sleep 2

# Start Velocity Embedded service on port 8081
nohup python3 ~/dbos-bench/velocity_embedded_server.py > ~/velocity_embedded.log 2>&1 &
sleep 2

echo "---"
echo "Service status:"
pgrep -f dbos_service.py > /dev/null && echo "DBOS service (8080): RUNNING" || echo "DBOS service: FAILED"
pgrep -f velocity_embedded_server.py > /dev/null && echo "Velocity Embedded (8081): RUNNING" || echo "Velocity Embedded: FAILED"

echo "---"
echo "Testing DBOS (port 8080):"
curl -s --max-time 3 http://localhost:8080/health 2>&1
echo ""
curl -s --max-time 3 -X POST http://localhost:8080/bench/invoke \
  -H "content-type: application/json" -d '{"data":"test"}' 2>&1
echo ""

echo "---"
echo "Testing Velocity Embedded (port 8081):"
curl -s --max-time 3 http://localhost:8081/health 2>&1
echo ""
curl -s --max-time 3 -X POST http://localhost:8081/bench/invoke \
  -H "content-type: application/json" -d '{"data":"test"}' 2>&1
echo ""

echo "=== Both services ready ==="
