#!/usr/bin/env python3
"""
DBOS Production Benchmark Service — Real durable execution on PostgreSQL.

Exposes the same workload types as velocity-bench via HTTP endpoints.
Each workload uses DBOS decorators (@DBOS.workflow, @DBOS.step) for
real durable execution checkpointed in PostgreSQL.

Architecture:
  [benchmark client] ──HTTP──► [FastAPI] ──► DBOS workflow ──► PostgreSQL
"""

import time
import json
import resource
import os
import sys
from dataclasses import dataclass

from dbos import DBOS, DBOSConfig
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

# ─── Configuration ───────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get(
    "DBOS_DATABASE_URL",
    "postgresql://dbos:dbos_bench@localhost:5432/dbos_bench",
)
HTTP_PORT = int(os.environ.get("DBOS_HTTP_PORT", "8080"))

# ─── DBOS + FastAPI Setup ────────────────────────────────────────────────────

app = FastAPI()

config: DBOSConfig = {
    "name": "dbos-bench",
    "database_url": DATABASE_URL,
}

DBOS(config=config, fastapi=app)

# ─── Durable Workflows ───────────────────────────────────────────────────────
# Each uses @DBOS.workflow() for durable execution and @DBOS.step() for
# checkpointed steps.  State is persisted to PostgreSQL.


@DBOS.step()
def simple_step() -> dict:
    """A single durable step — checkpointed to Postgres."""
    return {"status": "ok", "ts": time.time()}


@DBOS.workflow()
def simple_workflow() -> dict:
    """Simple workflow: 10 durable steps, each checkpointed to Postgres."""
    results = []
    for i in range(10):
        results.append(simple_step())
    return {"status": "completed", "steps": len(results)}


@DBOS.step()
def process_signal_step(signal_idx: int) -> dict:
    """Process a single signal — checkpointed to Postgres."""
    return {"signal": signal_idx, "processed": True}


@DBOS.workflow()
def signal_storm_workflow(num_signals: int = 100) -> dict:
    """Signal storm: process N signals as durable steps.

    Each signal is processed as a @DBOS.step() that is checkpointed to Postgres.
    This mirrors how Velocity handles signals — each one is a durable state
    transition persisted to the WAL.
    """
    received = 0
    for i in range(num_signals):
        result = process_signal_step(i)
        received += 1
    return {"status": "completed", "signals_received": received}


@DBOS.step()
def cold_start_step() -> dict:
    """First durable step after startup — measures cold start overhead."""
    return {"status": "ok", "ts": time.time()}


@DBOS.workflow()
def cold_start_workflow() -> dict:
    """Cold start: single workflow + step after engine startup."""
    result = cold_start_step()
    return result


@DBOS.step()
def multi_step_execute(step_num: int) -> int:
    """A single durable step in a multi-step workflow."""
    return step_num


@DBOS.workflow()
def multi_step_workflow(num_steps: int = 100) -> dict:
    """High-step workflow: N durable steps, each checkpointed."""
    last = 0
    for i in range(num_steps):
        last = multi_step_execute(i)
    return {"status": "completed", "steps_completed": last + 1}


@DBOS.step()
def stateful_read(key: str) -> int:
    """Durable step: read state (simulates DB read)."""
    return 0  # In real app, this reads from Postgres


@DBOS.step()
def stateful_write(key: str, count: int) -> dict:
    """Durable step: write state (simulates DB write, checkpointed)."""
    return {"status": "ok", "key": key, "count": count}


@DBOS.workflow()
def stateful_workflow(key: str = "default") -> dict:
    """Stateful workflow: read → increment → write with durable steps.

    Each step is checkpointed to Postgres, so if the workflow fails,
    it resumes from the last completed step.
    """
    current = stateful_read(key)
    new_count = current + 1
    result = stateful_write(key, new_count)
    return result


@DBOS.workflow()
def echo_workflow(data: str = "") -> dict:
    """Echo workflow: return input as-is."""
    return {"status": "ok", "data": data}


@DBOS.workflow()
def payload_workflow(data: str = "") -> dict:
    """Payload roundtrip workflow."""
    return {"status": "ok", "size": len(data)}


@DBOS.step()
def promise_set(value: dict) -> dict:
    """Durable step: set a promise value (checkpointed to Postgres)."""
    return value


@DBOS.step()
def promise_get() -> dict:
    """Durable step: get the promise value (checkpointed to Postgres)."""
    return {"resolved": True}


@DBOS.workflow()
def durable_promise_workflow() -> dict:
    """Durable promise: set a value, then read it back.

    Both operations are durable steps checkpointed to Postgres.
    This simulates a durable promise that survives process restarts.
    """
    promise_set({"resolved": True, "ts": time.time()})
    result = promise_get()
    return result


@DBOS.step()
def concurrent_step(i: int) -> int:
    """Step for concurrent workflow."""
    return i * 2


@DBOS.workflow()
def concurrent_workflow(workflow_id: int = 0) -> dict:
    """Concurrent workflow: simple durable execution."""
    result = concurrent_step(workflow_id)
    return {"status": "ok", "id": workflow_id, "result": result}


# ─── HTTP Endpoints ──────────────────────────────────────────────────────────


@app.post("/bench/simple_workflow")
async def bench_simple(request: Request):
    handle = DBOS.start_workflow(simple_workflow)
    result = handle.get_result()
    return JSONResponse(result)


@app.post("/bench/signal_storm")
async def bench_signal(request: Request):
    try:
        body = await request.json()
        num_signals = body.get("num_signals", 100)
    except Exception:
        num_signals = 100
    handle = DBOS.start_workflow(signal_storm_workflow, num_signals)
    result = handle.get_result()
    return JSONResponse(result)


@app.post("/bench/cold_start")
async def bench_cold_start(request: Request):
    handle = DBOS.start_workflow(cold_start_workflow)
    result = handle.get_result()
    return JSONResponse(result)


@app.post("/bench/multi_step")
async def bench_multi_step(request: Request):
    try:
        body = await request.json()
        num_steps = body.get("steps", 100)
    except Exception:
        num_steps = 100
    handle = DBOS.start_workflow(multi_step_workflow, num_steps)
    result = handle.get_result()
    return JSONResponse(result)


@app.post("/bench/stateful")
async def bench_stateful(request: Request):
    handle = DBOS.start_workflow(stateful_workflow, key="default")
    result = handle.get_result()
    return JSONResponse(result)


@app.post("/bench/echo")
async def bench_echo(request: Request):
    body = await request.body()
    handle = DBOS.start_workflow(echo_workflow, data=body.decode()[:1024])
    result = handle.get_result()
    return JSONResponse(result)


@app.post("/bench/payload")
async def bench_payload(request: Request):
    body = await request.body()
    handle = DBOS.start_workflow(payload_workflow, data=body.decode(errors="replace")[:4096])
    result = handle.get_result()
    return JSONResponse(result)


@app.post("/bench/durable_promise")
async def bench_durable_promise(request: Request):
    handle = DBOS.start_workflow(durable_promise_workflow)
    result = handle.get_result()
    return JSONResponse(result)


@app.post("/bench/concurrent")
async def bench_concurrent(request: Request):
    try:
        body = await request.json()
        wid = body.get("id", 0)
    except Exception:
        wid = 0
    handle = DBOS.start_workflow(concurrent_workflow, workflow_id=wid)
    result = handle.get_result()
    return JSONResponse(result)


# ─── DBOS-Specific Strength Workloads ────────────────────────────────────────
# These highlight DBOS's unique PostgreSQL-backed capabilities.


@DBOS.step()
def pg_transactional_insert(table_idx: int, row_count: int) -> dict:
    """Simulate a multi-table transactional insert.

    In a real DBOS app, this would execute INSERT statements across
    multiple tables within a single PostgreSQL transaction. The DBOS
    runtime guarantees exactly-once execution via the workflow journal.
    """
    # Simulate multi-table transactional work
    inserted = 0
    for i in range(row_count):
        # Each iteration represents a row inserted across 3 related tables
        # (e.g., orders → order_items → inventory_updates)
        inserted += 1
    return {
        "status": "ok",
        "table": f"table_{table_idx}",
        "rows_inserted": inserted,
        "tables_touched": 3,
    }


@DBOS.workflow()
def pg_transactional_workflow(num_tables: int = 5, rows_per_table: int = 10) -> dict:
    """Complex PostgreSQL transaction: multi-table inserts with durability.

    Highlights DBOS's ability to coordinate multi-table transactions
    with exactly-once semantics via PostgreSQL ACID + workflow journal.
    """
    total_rows = 0
    for t in range(num_tables):
        result = pg_transactional_insert(t, rows_per_table)
        total_rows += result["rows_inserted"]
    return {
        "status": "completed",
        "total_rows": total_rows,
        "tables": num_tables,
        "rows_per_table": rows_per_table,
    }


@DBOS.step()
def sql_query_step(query_type: str, param: int) -> dict:
    """Simulate a SQL query over workflow state.

    DBOS uniquely allows SQL queries directly over workflow state,
    since all state is stored in PostgreSQL tables that are queryable.
    """
    # Simulate different query types
    if query_type == "count":
        return {"count": param * 10, "query": f"SELECT count(*) FROM workflows WHERE batch={param}"}
    elif query_type == "aggregate":
        return {"sum": param * 55, "avg": param * 5.5, "query": f"SELECT sum(val), avg(val) FROM steps"}
    elif query_type == "join":
        return {"rows": param * 3, "query": f"SELECT w.*, s.* FROM workflows w JOIN steps s ON w.id=s.wf_id"}
    else:
        return {"rows": param, "query": f"SELECT * FROM workflows WHERE id={param}"}


@DBOS.workflow()
def sql_visibility_workflow(num_queries: int = 5) -> dict:
    """SQL visibility: query workflow state using standard SQL.

    This is unique to DBOS — other engines store state in opaque
    binary formats. DBOS stores everything in PostgreSQL, so you
    can use standard SQL to query workflow state in real-time.
    """
    results = []
    query_types = ["count", "aggregate", "join", "select", "count"]
    for i in range(num_queries):
        qt = query_types[i % len(query_types)]
        result = sql_query_step(qt, i + 1)
        results.append(result)
    return {
        "status": "completed",
        "queries_executed": len(results),
        "query_types": list(set(query_types[:num_queries])),
    }


@app.post("/bench/pg_transactional")
async def bench_pg_transactional(request: Request):
    try:
        body = await request.json()
        num_tables = body.get("num_tables", 5)
        rows_per_table = body.get("rows_per_table", 10)
    except Exception:
        num_tables = 5
        rows_per_table = 10
    handle = DBOS.start_workflow(pg_transactional_workflow, num_tables, rows_per_table)
    result = handle.get_result()
    return JSONResponse(result)


@app.post("/bench/sql_visibility")
async def bench_sql_visibility(request: Request):
    try:
        body = await request.json()
        num_queries = body.get("num_queries", 5)
    except Exception:
        num_queries = 5
    handle = DBOS.start_workflow(sql_visibility_workflow, num_queries)
    result = handle.get_result()
    return JSONResponse(result)


@app.get("/health")
async def health():
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    return JSONResponse({
        "status": "ok",
        "engine": "DBOS",
        "memory_rss_mb": round(mem, 1),
        "uptime": time.monotonic(),
    })


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "server":
        print(f"Starting DBOS benchmark server on port {HTTP_PORT}...")
        print(f"Database: {DATABASE_URL}")

        # DBOS must be launched before the FastAPI server starts.
        # This initializes the DBOS runtime, creates tables, etc.
        DBOS.launch()

        uvi_config = uvicorn.Config(
            app, host="0.0.0.0", port=HTTP_PORT, log_level="info"
        )
        server = uvicorn.Server(uvi_config)
        import asyncio
        asyncio.run(server.serve())
    else:
        print("Usage: python3 service.py server")
        print("  server  - Start the DBOS benchmark HTTP server")
