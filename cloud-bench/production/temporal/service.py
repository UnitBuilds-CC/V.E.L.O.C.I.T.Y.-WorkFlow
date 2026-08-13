#!/usr/bin/env python3
"""
Temporal Production Benchmark — FastAPI Service + Worker.

Runs a Temporal worker that executes benchmark workflows, and exposes
HTTP endpoints for the benchmark client to trigger workflows.

Architecture:
  [benchmark client] ──HTTP──► [FastAPI :8080] ──► [Temporal Client]
                                                          │
  [Worker] ◄──────────────────────────────────────────────┘
     └── executes workflows via Temporal server

Usage:
  python3 service.py server    — Start FastAPI + Worker
  python3 service.py worker    — Start worker only
"""

import asyncio
import json
import time
import resource
import os
import sys
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

from temporalio.client import Client as TemporalClient
from temporalio.worker import Worker
from temporalio.service import TLSConfig

from workflows import (
    ALL_ACTIVITIES,
    ALL_WORKFLOWS,
    SimpleWorkflow,
    SignalStormWorkflow,
    ColdStartWorkflow,
    MultiStepWorkflow,
    EchoWorkflow,
    PayloadWorkflow,
    StatefulWorkflow,
    ConcurrentWorkflow,
    DurablePromiseWorkflow,
)

# ─── Configuration ───────────────────────────────────────────────────────────

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
HTTP_PORT = int(os.environ.get("TEMPORAL_HTTP_PORT", "8080"))
TASK_QUEUE = os.environ.get("TEMPORAL_TASK_QUEUE", "bench-queue")

# ─── Globals ─────────────────────────────────────────────────────────────────

temporal_client: TemporalClient = None
worker: Worker = None


# ─── FastAPI App ─────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    global temporal_client, worker
    # Connect to Temporal
    temporal_client = await TemporalClient.connect(
        TEMPORAL_ADDRESS,
        namespace=TEMPORAL_NAMESPACE,
    )
    print(f"Connected to Temporal at {TEMPORAL_ADDRESS}")

    # Start worker
    worker = Worker(
        client=temporal_client,
        task_queue=TASK_QUEUE,
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
    )
    worker_task = asyncio.create_task(worker.run())
    print(f"Worker started on task queue '{TASK_QUEUE}'")

    yield

    # Shutdown
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


# ─── HTTP Endpoints ──────────────────────────────────────────────────────────


@app.post("/bench/simple_workflow")
async def bench_simple(request: Request):
    wf_id = f"simple-{uuid.uuid4().hex[:12]}"
    handle = await temporal_client.start_workflow(
        SimpleWorkflow.run,
        id=wf_id,
        task_queue=TASK_QUEUE,
    )
    result = await handle.result()
    return JSONResponse(result)


@app.post("/bench/signal_storm")
async def bench_signal(request: Request):
    try:
        body = await request.json()
        num_signals = body.get("num_signals", 50)
    except Exception:
        num_signals = 50

    wf_id = f"signal-{uuid.uuid4().hex[:12]}"
    handle = await temporal_client.start_workflow(
        SignalStormWorkflow.run,
        args=[num_signals],
        id=wf_id,
        task_queue=TASK_QUEUE,
    )

    # Send signals
    for i in range(num_signals):
        await handle.signal(SignalStormWorkflow.receive_signal, i)

    result = await handle.result()
    return JSONResponse(result)


@app.post("/bench/cold_start")
async def bench_cold_start(request: Request):
    wf_id = f"cold-{uuid.uuid4().hex[:12]}"
    handle = await temporal_client.start_workflow(
        ColdStartWorkflow.run,
        id=wf_id,
        task_queue=TASK_QUEUE,
    )
    result = await handle.result()
    return JSONResponse(result)


@app.post("/bench/multi_step")
async def bench_multi_step(request: Request):
    try:
        body = await request.json()
        num_steps = body.get("steps", 100)
    except Exception:
        num_steps = 100

    wf_id = f"multi-{uuid.uuid4().hex[:12]}"
    handle = await temporal_client.start_workflow(
        MultiStepWorkflow.run,
        args=[num_steps],
        id=wf_id,
        task_queue=TASK_QUEUE,
    )
    result = await handle.result()
    return JSONResponse(result)


@app.post("/bench/echo")
async def bench_echo(request: Request):
    body = await request.body()
    data = body.decode(errors="replace")[:1024]
    wf_id = f"echo-{uuid.uuid4().hex[:12]}"
    handle = await temporal_client.start_workflow(
        EchoWorkflow.run,
        args=[data],
        id=wf_id,
        task_queue=TASK_QUEUE,
    )
    result = await handle.result()
    return JSONResponse(result)


@app.post("/bench/payload")
async def bench_payload(request: Request):
    body = await request.body()
    data = body.decode(errors="replace")[:4096]
    wf_id = f"payload-{uuid.uuid4().hex[:12]}"
    handle = await temporal_client.start_workflow(
        PayloadWorkflow.run,
        args=[data],
        id=wf_id,
        task_queue=TASK_QUEUE,
    )
    result = await handle.result()
    return JSONResponse(result)


@app.post("/bench/stateful")
async def bench_stateful(request: Request):
    wf_id = f"state-{uuid.uuid4().hex[:12]}"
    handle = await temporal_client.start_workflow(
        StatefulWorkflow.run,
        args=["default"],
        id=wf_id,
        task_queue=TASK_QUEUE,
    )
    result = await handle.result()
    return JSONResponse(result)


@app.post("/bench/concurrent")
async def bench_concurrent(request: Request):
    try:
        body = await request.json()
        wid = body.get("id", 0)
    except Exception:
        wid = 0
    wf_id = f"conc-{uuid.uuid4().hex[:12]}"
    handle = await temporal_client.start_workflow(
        ConcurrentWorkflow.run,
        args=[wid],
        id=wf_id,
        task_queue=TASK_QUEUE,
    )
    result = await handle.result()
    return JSONResponse(result)


@app.post("/bench/durable_promise")
async def bench_durable_promise(request: Request):
    wf_id = f"promise-{uuid.uuid4().hex[:12]}"
    handle = await temporal_client.start_workflow(
        DurablePromiseWorkflow.run,
        id=wf_id,
        task_queue=TASK_QUEUE,
    )
    result = await handle.result()
    return JSONResponse(result)


@app.get("/health")
async def health():
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    return JSONResponse({
        "status": "ok",
        "engine": "Temporal",
        "temporal_address": TEMPORAL_ADDRESS,
        "task_queue": TASK_QUEUE,
        "memory_rss_mb": round(mem, 1),
        "uptime": time.monotonic(),
    })


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "server":
        print(f"Starting Temporal benchmark service on port {HTTP_PORT}...")
        print(f"Temporal: {TEMPORAL_ADDRESS}, Task Queue: {TASK_QUEUE}")
        uvi_config = uvicorn.Config(
            app, host="0.0.0.0", port=HTTP_PORT, log_level="warning"
        )
        server = uvicorn.Server(uvi_config)
        asyncio.run(server.serve())
    else:
        print("Usage: python3 service.py server")
        print("  server  - Start FastAPI + Temporal Worker")
