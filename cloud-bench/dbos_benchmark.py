#!/usr/bin/env python3
"""
DBOS Benchmark — mirrors velocity-bench workloads for fair comparison.

Measures:
  - Handler invocation throughput (ops/sec)
  - P99 latency (microseconds)
  - Peak memory (MB)

Uses DBOS durable execution framework with FastAPI HTTP endpoints.
"""

import asyncio
import json
import time
import os
import sys
import resource
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

# DBOS imports
from dbos import DBOS, DBOSConfig

# FastAPI for HTTP endpoints
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

# ─── Configuration ───────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get(
    "DBOS_DATABASE_URL",
    "postgresql://dbos:dbos@localhost:5432/dbos_bench"
)
HTTP_PORT = int(os.environ.get("DBOS_HTTP_PORT", "8080"))

# ─── DBOS + FastAPI Setup ────────────────────────────────────────────────────

app = FastAPI()

config: DBOSConfig = {
    "name": "dbos-bench",
    "database_url": DATABASE_URL,
}

# Initialize DBOS with FastAPI integration
DBOS(config=config, fastapi=app)

# ─── DBOS Workflows ──────────────────────────────────────────────────────────

@DBOS.workflow()
def simple_workflow(input_data: str = "") -> dict:
    """Simple workflow — single step, minimal overhead."""
    return {"status": "ok", "handler": "invoke", "input_size": len(input_data)}

@DBOS.workflow()
def stateful_workflow(key: str = "default", input_data: str = "") -> dict:
    """Stateful workflow — read/write state via DBOS events."""
    current = DBOS.get_event("stateful", f"counter_{key}")
    count = (int(current) if current else 0) + 1
    DBOS.set_event("stateful", f"counter_{key}", str(count))
    return {"status": "ok", "handler": "stateful", "key": key, "count": count}

@DBOS.workflow()
def multi_step_workflow(steps: int = 5) -> dict:
    """Multi-step workflow — N sequential steps with state."""
    result = 0
    for i in range(steps):
        prev = DBOS.get_event("multistep", "step_result")
        prev = int(prev) if prev else 0
        result = prev + 1
        DBOS.set_event("multistep", "step_result", str(result))
    return {"status": "ok", "handler": "multiStep", "steps_completed": result}

@DBOS.workflow()
def echo_workflow(input_data: str = "") -> dict:
    """Echo workflow — return input as-is."""
    return {"status": "ok", "handler": "echo", "data": input_data}

@DBOS.workflow()
def payload_workflow(input_data: str = "") -> dict:
    """Payload roundtrip workflow."""
    return {"status": "ok", "handler": "payload", "size": len(input_data)}

@DBOS.workflow()
def durable_promise_workflow(input_data: str = "") -> dict:
    """Durable promise — set and resolve."""
    DBOS.set_event("promise", "result", json.dumps({"resolved": True}))
    result = DBOS.get_event("promise", "result")
    return json.loads(result) if result else {"resolved": False}

# ─── HTTP Endpoints ──────────────────────────────────────────────────────────

@app.post("/bench/invoke")
async def bench_invoke(request: Request):
    body = await request.body()
    handle = DBOS.start_workflow(simple_workflow, input_data=body.decode())
    result = handle.get_result()
    return JSONResponse(result)

@app.post("/bench/echo")
async def bench_echo(request: Request):
    body = await request.body()
    handle = DBOS.start_workflow(echo_workflow, input_data=body.decode())
    result = handle.get_result()
    return JSONResponse(result)

@app.post("/bench/stateful")
async def bench_stateful(request: Request):
    body = await request.body()
    handle = DBOS.start_workflow(stateful_workflow, key="default", input_data=body.decode())
    result = handle.get_result()
    return JSONResponse(result)

@app.post("/bench/{key}/stateful")
async def bench_keyed_stateful(key: str, request: Request):
    body = await request.body()
    handle = DBOS.start_workflow(stateful_workflow, key=key, input_data=body.decode())
    result = handle.get_result()
    return JSONResponse(result)

@app.post("/bench/multiStep")
async def bench_multi_step(request: Request):
    body = await request.body()
    try:
        data = json.loads(body)
        steps = data.get("steps", 5)
    except Exception:
        steps = 5
    handle = DBOS.start_workflow(multi_step_workflow, steps=steps)
    result = handle.get_result()
    return JSONResponse(result)

@app.post("/bench/payload")
async def bench_payload(request: Request):
    body = await request.body()
    handle = DBOS.start_workflow(payload_workflow, input_data=body.decode())
    result = handle.get_result()
    return JSONResponse(result)

@app.post("/bench/durablePromise")
async def bench_durable_promise(request: Request):
    body = await request.body()
    handle = DBOS.start_workflow(durable_promise_workflow, input_data=body.decode())
    result = handle.get_result()
    return JSONResponse(result)

@app.get("/health")
async def health():
    return {"status": "ok"}

# ─── Benchmark Runner ────────────────────────────────────────────────────────

@dataclass
class WorkloadResult:
    name: str
    operations: int
    success_count: int
    fail_count: int
    ops_per_second: float
    latency_p50_us: float
    latency_p99_us: float
    latency_p999_us: float
    peak_memory_mb: float

@dataclass
class BenchmarkReport:
    generated_at: str
    engine: str
    engine_version: str
    profile: str
    workloads: list

def get_peak_memory_mb() -> float:
    """Get peak RSS in MB."""
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_maxrss / 1024.0  # Linux: kB -> MB

async def run_workload(name, url, payload, count, concurrency=1):
    """Run a single workload and return results."""
    import aiohttp

    latencies = []
    success = 0
    fail = 0

    connector = aiohttp.TCPConnector(limit=concurrency * 2)
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        if concurrency <= 1:
            for _ in range(count):
                start = time.perf_counter()
                try:
                    async with session.post(url, data=payload, headers={"Content-Type": "application/json"}) as resp:
                        if resp.status == 200:
                            success += 1
                        else:
                            fail += 1
                except Exception as e:
                    fail += 1
                elapsed_us = (time.perf_counter() - start) * 1_000_000
                latencies.append(elapsed_us)
        else:
            sem = asyncio.Semaphore(concurrency)
            async def run_one():
                nonlocal success, fail
                async with sem:
                    start = time.perf_counter()
                    try:
                        async with session.post(url, data=payload, headers={"Content-Type": "application/json"}) as resp:
                            if resp.status == 200:
                                success += 1
                            else:
                                fail += 1
                    except Exception:
                        fail += 1
                    elapsed_us = (time.perf_counter() - start) * 1_000_000
                    latencies.append(elapsed_us)

            tasks = [run_one() for _ in range(count)]
            await asyncio.gather(*tasks)

    total_time = sum(latencies) / 1_000_000
    ops_per_sec = success / total_time if total_time > 0 else 0

    latencies.sort()
    n = len(latencies)
    p50 = latencies[int(n * 0.50)] if n > 0 else 0
    p99 = latencies[int(n * 0.99)] if n > 0 else 0
    p999 = latencies[int(n * 0.999)] if n > 0 else 0

    return WorkloadResult(
        name=name,
        operations=count,
        success_count=success,
        fail_count=fail,
        ops_per_second=round(ops_per_sec, 1),
        latency_p50_us=round(p50, 1),
        latency_p99_us=round(p99, 1),
        latency_p999_us=round(p999, 1),
        peak_memory_mb=round(get_peak_memory_mb(), 2),
    )

async def run_all_benchmarks(profile="standard"):
    """Run all benchmark workloads."""
    base_url = f"http://localhost:{HTTP_PORT}"
    mult = {"quick": 0.1, "stress": 10.0}.get(profile, 1.0)

    workloads = [
        ("handler_invocation", f"{base_url}/bench/invoke", b"x" * 64, int(200 * mult), 1),
        ("stateful_handler", f"{base_url}/bench/stateful", b"x" * 128, int(50 * mult), 1),
        ("concurrent_handlers", f"{base_url}/bench/invoke", b"x" * 64, int(50 * mult), 20),
        ("payload_roundtrip_1kb", f"{base_url}/bench/payload", b"x" * 1024, int(100 * mult), 1),
        ("mixed_operations", f"{base_url}/bench/invoke", b"x" * 128, int(100 * mult), 5),
        ("durable_promise", f"{base_url}/bench/durablePromise", b"x" * 64, int(50 * mult), 1),
        ("echo_handler", f"{base_url}/bench/echo", b"x" * 256, int(200 * mult), 1),
    ]

    results = []
    for name, url, payload, count, concurrency in workloads:
        print(f"  Running {name} ({count} ops, concurrency={concurrency})...")
        result = await run_workload(name, url, payload, count, concurrency)
        print(f"    -> {result.ops_per_second} ops/sec, p99={result.latency_p99_us}us, mem={result.peak_memory_mb}MB")
        results.append(result)

    return BenchmarkReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        engine="DBOS",
        engine_version="2.29.0",
        profile=profile,
        workloads=[asdict(r) for r in results],
    )

async def main():
    """Run benchmarks or start HTTP server."""
    if len(sys.argv) > 1 and sys.argv[1] == "server":
        # Server mode
        print("Starting DBOS benchmark server...")
        DBOS.launch()
        uvi_config = uvicorn.Config(app, host="0.0.0.0", port=HTTP_PORT, log_level="info")
        server = uvicorn.Server(uvi_config)
        await server.serve()
    elif len(sys.argv) > 1 and sys.argv[1] == "bench":
        # Benchmark mode
        profile = sys.argv[2] if len(sys.argv) > 2 else "standard"
        print(f"=== DBOS Benchmark (profile: {profile}) ===")
        print()
        report = await run_all_benchmarks(profile)
        output_path = "/tmp/dbos_bench_results.json"
        with open(output_path, "w") as f:
            json.dump(asdict(report), f, indent=2)
        print()
        print(f"Results written to {output_path}")
        print(f"Summary: {len(report.workloads)} workloads completed")
        for w in report.workloads:
            print(f"  {w['name']}: {w['ops_per_second']} ops/sec, p99={w['latency_p99_us']}us")
    else:
        print("Usage: python3 dbos_benchmark.py [server|bench] [profile]")
        print("  server  - Start the HTTP server")
        print("  bench   - Run benchmarks (server must be running)")

if __name__ == "__main__":
    asyncio.run(main())
