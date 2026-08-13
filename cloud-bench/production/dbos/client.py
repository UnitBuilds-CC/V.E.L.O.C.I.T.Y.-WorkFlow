#!/usr/bin/env python3
"""
DBOS Production Benchmark Client — measures real throughput via HTTP.

Sends requests to the DBOS service endpoints and measures:
  - ops/sec (throughput)
  - p50, p99, p999 latency (microseconds)
  - error rate
  - peak memory (client-side)

Usage:
  python3 client.py [profile]
  profile: smoke, standard, stress (default: standard)
"""

import asyncio
import json
import time
import resource
import sys
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import argparse

# Parse CLI arguments first
parser = argparse.ArgumentParser(description="DBOS Production Benchmark Client")
parser.add_argument("profile", nargs="?", default="standard",
                    help="Benchmark profile: smoke, short, standard, stress")
parser.add_argument("--base-url", default=None,
                    help="DBOS service base URL (default: from DBOS_HTTP_PORT env or http://localhost:8080)")
parser.add_argument("--output", "-o", default=None,
                    help="Output file path (default: /tmp/dbos_bench_results.json)")
_cli_args = parser.parse_args()

HTTP_PORT = int(os.environ.get("DBOS_HTTP_PORT", "8080"))
BASE_URL = _cli_args.base_url or f"http://localhost:{HTTP_PORT}"
OUTPUT_PATH = _cli_args.output or "/tmp/dbos_bench_results.json"


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
    latency_mean_us: float
    peak_memory_mb: float


@dataclass
class BenchmarkReport:
    generated_at: str
    engine: str
    engine_version: str
    profile: str
    workloads: list
    total_ops: int
    total_success: int
    total_fail: int
    overall_ops_per_sec: float


def get_peak_memory_mb() -> float:
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_maxrss / 1024.0


async def run_workload(name, url, payload, count, concurrency=1, timeout_sec=120):
    """Run a single workload and return results."""
    import aiohttp

    latencies = []
    success = 0
    fail = 0

    connector = aiohttp.TCPConnector(limit=max(concurrency * 2, 10))
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        wall_start = time.perf_counter()

        if concurrency <= 1:
            for _ in range(count):
                start = time.perf_counter()
                try:
                    async with session.post(
                        url,
                        data=payload,
                        headers={"Content-Type": "application/json"},
                    ) as resp:
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
                        async with session.post(
                            url,
                            data=payload,
                            headers={"Content-Type": "application/json"},
                        ) as resp:
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

        wall_clock = time.perf_counter() - wall_start

    ops_per_sec = success / wall_clock if wall_clock > 0 else 0

    latencies.sort()
    n = len(latencies)
    p50 = latencies[int(n * 0.50)] if n > 0 else 0
    p99 = latencies[int(n * 0.99)] if n > 0 else 0
    p999 = latencies[int(n * 0.999)] if n > 0 else 0
    mean = sum(latencies) / n if n > 0 else 0

    return WorkloadResult(
        name=name,
        operations=count,
        success_count=success,
        fail_count=fail,
        ops_per_second=round(ops_per_sec, 1),
        latency_p50_us=round(p50, 1),
        latency_p99_us=round(p99, 1),
        latency_p999_us=round(p999, 1),
        latency_mean_us=round(mean, 1),
        peak_memory_mb=round(get_peak_memory_mb(), 2),
    )


async def run_all_benchmarks(profile="standard"):
    """Run all benchmark workloads."""
    mult = {"smoke": 0.1, "stress": 10.0}.get(profile, 1.0)

    # Workload definitions matching velocity-bench workloads.rs
    workloads = [
        # simple_workflow: start → 10 durable steps → complete
        (
            "simple_workflow",
            f"{BASE_URL}/bench/simple_workflow",
            b"{}",
            int(50 * mult),
            1,
            120,
        ),
        # signal_storm: start → N signals → complete
        (
            "signal_storm",
            f"{BASE_URL}/bench/signal_storm",
            json.dumps({"num_signals": 50}).encode(),
            int(20 * mult),
            1,
            300,
        ),
        # cold_start: first workflow after startup
        (
            "cold_start",
            f"{BASE_URL}/bench/cold_start",
            b"{}",
            int(10 * mult),
            1,
            60,
        ),
        # multi_step: 100 durable steps
        (
            "multi_step",
            f"{BASE_URL}/bench/multi_step",
            json.dumps({"steps": 100}).encode(),
            int(10 * mult),
            1,
            300,
        ),
        # stateful: durable state via events
        (
            "stateful",
            f"{BASE_URL}/bench/stateful",
            b"{}",
            int(50 * mult),
            1,
            120,
        ),
        # echo: payload roundtrip
        (
            "echo",
            f"{BASE_URL}/bench/echo",
            b"x" * 256,
            int(100 * mult),
            1,
            60,
        ),
        # payload_1kb: 1KB payload roundtrip
        (
            "payload_1kb",
            f"{BASE_URL}/bench/payload",
            b"x" * 1024,
            int(100 * mult),
            1,
            60,
        ),
        # durable_promise: set + resolve
        (
            "durable_promise",
            f"{BASE_URL}/bench/durable_promise",
            b"{}",
            int(50 * mult),
            1,
            120,
        ),
        # concurrent: 20 parallel workflows
        (
            "concurrent_20",
            f"{BASE_URL}/bench/concurrent",
            json.dumps({"id": 0}).encode(),
            int(50 * mult),
            20,
            120,
        ),
    ]

    results = []
    for name, url, payload, count, concurrency, timeout in workloads:
        print(f"  Running {name} ({count} ops, concurrency={concurrency})...")
        result = await run_workload(name, url, payload, count, concurrency, timeout)
        print(
            f"    -> {result.ops_per_second} ops/sec, "
            f"p99={result.latency_p99_us}us, "
            f"success={result.success_count}/{result.operations}, "
            f"mem={result.peak_memory_mb}MB"
        )
        results.append(result)

    total_ops = sum(r.operations for r in results)
    total_success = sum(r.success_count for r in results)
    total_fail = sum(r.fail_count for r in results)
    total_time = sum(
        r.operations / r.ops_per_second if r.ops_per_second > 0 else 0
        for r in results
    )
    overall_ops = total_success / total_time if total_time > 0 else 0

    return BenchmarkReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        engine="DBOS",
        engine_version="latest",
        profile=profile,
        workloads=[asdict(r) for r in results],
        total_ops=total_ops,
        total_success=total_success,
        total_fail=total_fail,
        overall_ops_per_sec=round(overall_ops, 1),
    )


async def main():
    profile = _cli_args.profile
    print(f"=== DBOS Production Benchmark (profile: {profile}) ===")
    print(f"Target: {BASE_URL}")
    print()

    # Health check
    import aiohttp

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BASE_URL}/health") as resp:
                health = await resp.json()
                print(f"Server health: {health}")
    except Exception as e:
        print(f"ERROR: Cannot connect to DBOS server: {e}")
        sys.exit(1)

    print()
    report = await run_all_benchmarks(profile)

    output_path = OUTPUT_PATH
    with open(output_path, "w") as f:
        json.dump(asdict(report), f, indent=2)

    print()
    print(f"Results written to {output_path}")
    print(f"Summary: {len(report.workloads)} workloads, "
          f"{report.total_success}/{report.total_ops} success, "
          f"{report.total_fail} failures")
    print()
    for w in report.workloads:
        err_rate = (w['fail_count'] / w['operations'] * 100) if w['operations'] > 0 else 0
        print(f"  {w['name']}: {w['ops_per_second']} ops/sec, "
              f"p99={w['latency_p99_us']}us, "
              f"errors={err_rate:.1f}%")


if __name__ == "__main__":
    asyncio.run(main())
