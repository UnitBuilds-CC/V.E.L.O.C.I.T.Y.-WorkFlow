#!/usr/bin/env python3
"""
HTTP Benchmark: Velocity Embedded vs DBOS
Runs identical workloads against both services on the DBOS VM.
"""
import asyncio
import aiohttp
import time
import json
import resource
import statistics
import sys

VELOCITY_URL = "http://localhost:8081"
DBOS_URL = "http://localhost:8080"
RUNS = 3

async def bench_handler_invocation(session, base_url, n=1000):
    """1000 sequential handler calls."""
    latencies = []
    url = f"{base_url}/bench/invoke"
    payload = json.dumps({"data": "x" * 64})
    for _ in range(n):
        start = time.perf_counter()
        async with session.post(url, data=payload, headers={"content-type": "application/json"}) as resp:
            await resp.read()
        latencies.append((time.perf_counter() - start) * 1_000_000)  # µs
    return latencies

async def bench_stateful_handler(session, base_url, n=100):
    """100 keyed handler calls with state."""
    latencies = []
    payload = json.dumps({"data": "x" * 128})
    for i in range(n):
        url = f"{base_url}/bench/key{i}/stateful"
        start = time.perf_counter()
        async with session.post(url, data=payload, headers={"content-type": "application/json"}) as resp:
            await resp.read()
        latencies.append((time.perf_counter() - start) * 1_000_000)
    return latencies

async def bench_concurrent_handlers(session, base_url, n=100, concurrency=100):
    """100 concurrent handler invocations."""
    url = f"{base_url}/bench/invoke"
    payload = json.dumps({"data": "x" * 64})
    
    async def do_one(i):
        start = time.perf_counter()
        async with session.post(url, data=payload, headers={"content-type": "application/json"}) as resp:
            await resp.read()
        return (time.perf_counter() - start) * 1_000_000
    
    tasks = [do_one(i) for i in range(n)]
    latencies = await asyncio.gather(*tasks)
    return list(latencies)

async def bench_payload_roundtrip(session, base_url, n=500, payload_size=1024):
    """500 calls with 1KB payloads."""
    latencies = []
    url = f"{base_url}/bench/echo"
    payload = json.dumps({"data": "x" * payload_size})
    for _ in range(n):
        start = time.perf_counter()
        async with session.post(url, data=payload, headers={"content-type": "application/json"}) as resp:
            await resp.read()
        latencies.append((time.perf_counter() - start) * 1_000_000)
    return latencies

async def bench_sustained_load(session, base_url, duration=30, concurrency=50):
    """30s sustained load at concurrency 50."""
    url = f"{base_url}/bench/invoke"
    payload = json.dumps({"data": "x" * 64})
    latencies = []
    end_time = time.time() + duration
    
    async def worker():
        while time.time() < end_time:
            start = time.perf_counter()
            async with session.post(url, data=payload, headers={"content-type": "application/json"}) as resp:
                await resp.read()
            latencies.append((time.perf_counter() - start) * 1_000_000)
    
    tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
    await asyncio.gather(*tasks)
    return latencies

async def bench_mixed_operations(session, base_url, n=500):
    """500 mixed calls: 70% invoke, 20% stateful, 10% echo."""
    latencies = []
    for i in range(n):
        r = i % 10
        if r < 7:
            url = f"{base_url}/bench/invoke"
        elif r < 9:
            url = f"{base_url}/bench/key{i}/stateful"
        else:
            url = f"{base_url}/bench/echo"
        payload = json.dumps({"data": "x" * 128})
        start = time.perf_counter()
        async with session.post(url, data=payload, headers={"content-type": "application/json"}) as resp:
            await resp.read()
        latencies.append((time.perf_counter() - start) * 1_000_000)
    return latencies

async def bench_cold_start(session, base_url, n=10):
    """10 calls after 5s idle."""
    await asyncio.sleep(5)
    latencies = []
    url = f"{base_url}/bench/invoke"
    payload = json.dumps({"data": "x" * 64})
    for _ in range(n):
        start = time.perf_counter()
        async with session.post(url, data=payload, headers={"content-type": "application/json"}) as resp:
            await resp.read()
        latencies.append((time.perf_counter() - start) * 1_000_000)
    return latencies

async def bench_durable_promise(session, base_url, n=50):
    """50 promise handler calls."""
    latencies = []
    url = f"{base_url}/bench/promise"
    payload = json.dumps({"data": "x" * 64})
    for _ in range(n):
        start = time.perf_counter()
        async with session.post(url, data=payload, headers={"content-type": "application/json"}) as resp:
            await resp.read()
        latencies.append((time.perf_counter() - start) * 1_000_000)
    return latencies

def get_memory_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

def summarize(latencies, duration_ms=0):
    if not latencies:
        return {"ops_sec": 0, "p50": 0, "p99": 0, "p999": 0, "mean": 0, "min": 0, "max": 0}
    s = sorted(latencies)
    n = len(s)
    total_sec = sum(s) / 1_000_000
    ops_sec = n / total_sec if total_sec > 0 else 0
    if duration_ms > 0:
        ops_sec = n / (duration_ms / 1000)
    return {
        "ops_sec": round(ops_sec),
        "p50": round(s[int(n * 0.50)]),
        "p95": round(s[int(n * 0.95)]) if n > 20 else round(s[-1]),
        "p99": round(s[int(n * 0.99)]) if n > 100 else round(s[int(n * 0.99)] if n > 1 else s[-1]),
        "p999": round(s[int(n * 0.999)]) if n > 1000 else round(s[-1]),
        "mean": round(statistics.mean(s)),
        "min": round(s[0]),
        "max": round(s[-1]),
        "count": n,
    }

async def run_workload(name, func, session, url, **kwargs):
    results = []
    for run in range(RUNS):
        latencies = await func(session, url, **kwargs)
        dur = kwargs.get("duration", 0)
        s = summarize(latencies, dur * 1000 if dur else 0)
        results.append(s)
        print(f"  {name}: Run {run+1}/{RUNS}: {s['ops_sec']} ops/sec, p99={s['p99']}µs, count={s['count']}")
    return results

async def main():
    workloads = [
        ("handler_invocation", bench_handler_invocation, {"n": 1000}),
        ("stateful_handler", bench_stateful_handler, {"n": 100}),
        ("concurrent_handlers", bench_concurrent_handlers, {"n": 100}),
        ("payload_roundtrip", bench_payload_roundtrip, {"n": 500}),
        ("sustained_load", bench_sustained_load, {"duration": 30, "concurrency": 50}),
        ("mixed_operations", bench_mixed_operations, {"n": 500}),
        ("cold_start", bench_cold_start, {"n": 10}),
        ("durable_promise", bench_durable_promise, {"n": 50}),
    ]
    
    all_results = {}
    
    for engine_name, url in [("Velocity Embedded", VELOCITY_URL), ("DBOS", DBOS_URL)]:
        print(f"\n{'='*60}")
        print(f"  Benchmarking: {engine_name} ({url})")
        print(f"{'='*60}")
        all_results[engine_name] = {}
        
        timeout = aiohttp.ClientTimeout(total=120)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Health check
            try:
                async with session.get(f"{url}/health") as resp:
                    health = await resp.json()
                    print(f"  Health: {health}")
            except Exception as e:
                print(f"  Health check failed: {e}")
                continue
            
            for wl_name, wl_func, wl_kwargs in workloads:
                print(f"\n  --- {wl_name} ---")
                results = await run_workload(wl_name, wl_func, session, url, **wl_kwargs)
                all_results[engine_name][wl_name] = results
    
    # Save results
    with open("/home/ian_unitbuilds_com/embedded_vs_dbos_results.json", "w") as f:
        json.dump(all_results, f, indent=2)
    
    # Print summary table
    print(f"\n\n{'='*80}")
    print("  SUMMARY: Velocity Embedded vs DBOS")
    print(f"{'='*80}")
    print(f"{'Workload':<25} {'Engine':<20} {'ops/sec':>10} {'p50(µs)':>10} {'p99(µs)':>12} {'p999(µs)':>12}")
    print("-" * 90)
    
    for wl_name, _, _ in workloads:
        for engine in ["Velocity Embedded", "DBOS"]:
            if engine in all_results and wl_name in all_results[engine]:
                runs = all_results[engine][wl_name]
                # Average across runs
                avg_ops = round(statistics.mean([r["ops_sec"] for r in runs]))
                avg_p50 = round(statistics.mean([r["p50"] for r in runs]))
                avg_p99 = round(statistics.mean([r["p99"] for r in runs]))
                avg_p999 = round(statistics.mean([r.get("p999", r.get("max", 0)) for r in runs]))
                print(f"{wl_name:<25} {engine:<20} {avg_ops:>10} {avg_p50:>10} {avg_p99:>12} {avg_p999:>12}")
        print()
    
    print("Results saved to ~/embedded_vs_dbos_results.json")

if __name__ == "__main__":
    asyncio.run(main())
