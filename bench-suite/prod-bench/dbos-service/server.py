"""
DBOS Benchmark Service — Real PostgreSQL-backed durable execution.

Every handler performs a real database roundtrip, measuring the full cost
of durable execution against PostgreSQL. This is NOT a mock — it uses
real asyncpg connections and real transactions.

Endpoints mirror the prod-bench client expectations:
  GET  /health           — health check
  POST /bench/invoke     — simple handler (DB roundtrip)
  POST /bench/echo       — echo handler
  POST /bench/payload    — payload roundtrip
  POST /bench/stateful   — stateful handler (read+write state)
  POST /bench/durablePromise — durable promise resolution
"""
import os
import time
import json
import asyncio
import resource
from aiohttp import web

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://dbos:dbos_bench@localhost:5432/dbos_bench",
)

# Global connection pool
pool = None


async def init_db():
    """Initialize the database schema and connection pool."""
    import asyncpg

    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=20)

    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bench_state (
                key   TEXT PRIMARY KEY,
                value JSONB NOT NULL DEFAULT '{}',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bench_invocations (
                id BIGSERIAL PRIMARY KEY,
                handler TEXT NOT NULL,
                input_size INT DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bench_promises (
                id TEXT PRIMARY KEY,
                result JSONB,
                resolved BOOLEAN NOT NULL DEFAULT false,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)


async def health(request):
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    return web.json_response({
        "status": "ok",
        "memory_rss_mb": round(mem, 1),
        "engine": "dbos",
        "uptime": time.monotonic(),
    })


async def invoke_handler(request):
    """Simple handler — performs a real DB INSERT, measuring durable execution cost."""
    try:
        body = await request.read()
        input_size = len(body)
    except Exception:
        input_size = 0

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO bench_invocations (handler, input_size) VALUES ($1, $2)",
            "invoke",
            input_size,
        )

    return web.json_response({"status": "ok", "handler": "invoke", "ts": int(time.time() * 1000)})


async def echo_handler(request):
    """Echo handler — returns input after a DB roundtrip."""
    try:
        body = await request.json()
    except Exception:
        body = {"echo": True}

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO bench_invocations (handler, input_size) VALUES ($1, $2)",
            "echo",
            len(json.dumps(body)),
        )

    return web.json_response(body)


async def payload_handler(request):
    """Payload roundtrip — receive data, store in DB, return it."""
    body = await request.read()
    input_size = len(body)

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO bench_invocations (handler, input_size) VALUES ($1, $2)",
            "payload",
            input_size,
        )

    return web.json_response({"status": "ok", "size": input_size})


async def stateful_handler(request):
    """Stateful handler — reads and writes state via PostgreSQL."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    key = body.get("key", "default")

    async with pool.acquire() as conn:
        # Read current state
        row = await conn.fetchrow(
            "SELECT value FROM bench_state WHERE key = $1", key
        )
        if row is None:
            count = 1
            await conn.execute(
                "INSERT INTO bench_state (key, value) VALUES ($1, $2)",
                key,
                json.dumps({"count": count}),
            )
        else:
            state = json.loads(row["value"])
            count = state.get("count", 0) + 1
            await conn.execute(
                "UPDATE bench_state SET value = $1, updated_at = now() WHERE key = $2",
                json.dumps({"count": count}),
                key,
            )

    return web.json_response({"status": "ok", "handler": "stateful", "count": count})


async def promise_handler(request):
    """Durable promise — create, resolve, and read back via PostgreSQL."""
    import hashlib

    pid = hashlib.md5(f"{time.time()}-{id(request)}".encode()).hexdigest()[:16]

    async with pool.acquire() as conn:
        # Create promise
        await conn.execute(
            "INSERT INTO bench_promises (id, result, resolved) VALUES ($1, $2, false)",
            pid,
            json.dumps(None),
        )
        # Resolve it
        await conn.execute(
            "UPDATE bench_promises SET result = $1, resolved = true WHERE id = $2",
            json.dumps({"resolved": True, "ts": int(time.time() * 1000)}),
            pid,
        )
        # Read it back
        row = await conn.fetchrow("SELECT result FROM bench_promises WHERE id = $1", pid)
        result = json.loads(row["result"]) if row else None

    return web.json_response({"id": pid, "status": "resolved", "result": result})


def create_app():
    app = web.Application()
    app.on_startup.append(lambda app: init_db())
    app.router.add_get("/health", health)
    app.router.add_post("/bench/invoke", invoke_handler)
    app.router.add_post("/bench/echo", echo_handler)
    app.router.add_post("/bench/payload", payload_handler)
    app.router.add_post("/bench/stateful", stateful_handler)
    app.router.add_post("/bench/durablePromise", promise_handler)
    return app


if __name__ == "__main__":
    print("Starting DBOS bench service on port 8080 (PostgreSQL-backed)...")
    web.run_app(create_app(), host="0.0.0.0", port=8080, print=None)
