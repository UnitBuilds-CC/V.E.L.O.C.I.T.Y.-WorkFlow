#!/usr/bin/env python3
"""
DBOS Benchmark Client — benchmarks a running DBOS server via HTTP.

This script sends HTTP requests to the DBOS benchmark server and measures
throughput, latency, and memory.

Usage:
  python3 dbos_bench_client.py [profile]
  profile: quick, standard, stress (default: standard)
"""

import asyncio
import json
import time
import os
import sys
import resource
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

HTTP_PORT = int(os.environ.get("DBOS_HTTP_PORT", "8080"))

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
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_maxrss / 1024.0

async def run_workload(name, url, payload, count, concurrency=1):
    import aiohttp

    latencies = []
    success = 0
    fail = 0

    connector = aiohttp.TCPConnector(limit=max(concurrency * 2, 10))
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        bench_start = time.perf_counter()
        if concurrency <= 1:
            for _ in range(count):
                start = time.perf_counter()
                try:
                    async with session.post(url, data=payload, headers={"Content-Type": "application/json"}) as resp:
                        await resp.read()
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
                            await resp.read()
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
        wall_clock = time.perf_counter() - bench_start

    # Calculate throughput from actual wall-clock time
    ops_per_sec = success / wall_clock if wall_clock > 0 else 0

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
        print(f"    -> {result.ops_per_second} ops/sec, p99={result.latency_p99_us}us, mem={result.peak_memory_mb}MB, success={result.success_count}/{result.operations}")
        results.append(result)

    return BenchmarkReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        engine="DBOS",
        engine_version="2.29.0",
        profile=profile,
        workloads=[asdict(r) for r in results],
    )

async def main():
    profile = sys.argv[1] if len(sys.argv) > 1 else "standard"
    print(f"=== DBOS Benchmark Client (profile: {profile}) ===")
    print(f"Target: http://localhost:{HTTP_PORT}")
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

if __name__ == "__main__":
    asyncio.run(main())
