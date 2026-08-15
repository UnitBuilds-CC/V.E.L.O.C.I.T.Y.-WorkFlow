#!/usr/bin/env python3
"""
Competitor Throughput Benchmark — DBOS, Restate, Temporal.

Runs 1000 sequential durable workflows against each competitor,
measuring throughput (wf/s) and latency (ms/wf).

Fair comparison with Velocity's 3-flavor PostgreSQL benchmark:
  - Same workload: 10 durable steps per workflow
  - Same measurement: sequential request/response
  - Same count: configurable (default 1000 for speed, set ITERATIONS env)

Usage:
  python bench_competitors.py                    # benchmark all 3
  python bench_competitors.py --only dbos        # benchmark one
  ITERATIONS=500 python bench_competitors.py     # custom count
"""

import requests
import time
import sys
import os

ITERATIONS = int(os.environ.get("ITERATIONS", "1000"))
WARMUP = 10

TARGETS = {
    "dbos": {
        "name": "DBOS",
        "url": "http://localhost:8081",
        "bench_endpoint": "/bench/simple_workflow",
        "health_endpoint": "/health",
    },
    "restate": {
        "name": "Restate",
        "url": "http://localhost:8082",
        "bench_endpoint": "/bench/smoke_0/simple",
        "health_endpoint": None,  # Restate ingress doesn't expose /health directly
    },
    "temporal": {
        "name": "Temporal",
        "url": "http://localhost:8083",
        "bench_endpoint": "/bench/simple_workflow",
        "health_endpoint": "/health",
    },
}


def bench_engine(key: str, config: dict, iterations: int):
    """Run throughput benchmark against a single engine."""
    name = config["name"]
    url = config["url"]
    endpoint = config["bench_endpoint"]
    full_url = f"{url}{endpoint}"

    print(f"\n{'='*60}")
    print(f"  {name} Throughput Benchmark")
    print(f"  Target: {full_url}")
    print(f"  Iterations: {iterations}")
    print(f"{'='*60}")

    # Health check (if available)
    if config["health_endpoint"]:
        try:
            r = requests.get(f"{url}{config['health_endpoint']}", timeout=5)
            print(f"  Health: {r.json()}")
        except Exception as e:
            print(f"  Health check failed: {e}")

    # Warmup
    print(f"\n  Warming up ({WARMUP} workflows)...")
    warmup_ok = 0
    for i in range(WARMUP):
        try:
            r = requests.post(full_url, json={}, timeout=60)
            if r.status_code == 200:
                warmup_ok += 1
            else:
                print(f"    Warmup {i}: HTTP {r.status_code} — {r.text[:100]}")
        except Exception as e:
            print(f"    Warmup {i}: {e}")
    print(f"  Warmup: {warmup_ok}/{WARMUP} OK")

    if warmup_ok == 0:
        print(f"  SKIP — {name} not responding")
        return None

    # Main benchmark
    print(f"\n  Running {iterations} workflows...")
    successes = 0
    failures = 0
    latencies = []

    start_time = time.time()
    for i in range(iterations):
        try:
            t0 = time.time()
            r = requests.post(full_url, json={}, timeout=120)
            elapsed_ms = (time.time() - t0) * 1000
            latencies.append(elapsed_ms)

            if r.status_code == 200:
                successes += 1
            else:
                failures += 1
                if failures <= 3:
                    print(f"    FAIL {i}: HTTP {r.status_code} — {r.text[:100]}")
        except Exception as e:
            failures += 1
            if failures <= 3:
                print(f"    ERROR {i}: {e}")

    total_time = time.time() - start_time

    # Results
    print(f"\n  --- {name} Results ---")
    print(f"  Success: {successes}/{iterations}")
    print(f"  Failures: {failures}")
    print(f"  Total time: {total_time:.3f}s")

    if successes > 0:
        throughput = successes / total_time
        avg_latency = total_time / successes * 1000
        p50 = sorted(latencies)[len(latencies) // 2]
        p95 = sorted(latencies)[int(len(latencies) * 0.95)]
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]
        min_lat = min(latencies)
        max_lat = max(latencies)

        print(f"  Throughput: {throughput:.1f} wf/s")
        print(f"  Avg latency: {avg_latency:.1f} ms/wf")
        print(f"  Min latency: {min_lat:.1f} ms")
        print(f"  P50 latency: {p50:.1f} ms")
        print(f"  P95 latency: {p95:.1f} ms")
        print(f"  P99 latency: {p99:.1f} ms")
        print(f"  Max latency: {max_lat:.1f} ms")

        return {
            "engine": name,
            "successes": successes,
            "failures": failures,
            "total_time": total_time,
            "throughput": throughput,
            "avg_latency": avg_latency,
            "p50": p50,
            "p95": p95,
            "p99": p99,
            "min": min_lat,
            "max": max_lat,
        }
    return None


def main():
    only = None
    if "--only" in sys.argv:
        idx = sys.argv.index("--only")
        if idx + 1 < len(sys.argv):
            only = sys.argv[idx + 1]

    engines = [only] if only else list(TARGETS.keys())
    results = []

    print("=" * 60)
    print("  Competitor Throughput Benchmark")
    print(f"  Iterations per engine: {ITERATIONS}")
    print(f"  Workload: 10 durable steps (simple_workflow)")
    print("=" * 60)

    for key in engines:
        if key not in TARGETS:
            print(f"Unknown engine: {key}")
            continue
        result = bench_engine(key, TARGETS[key], ITERATIONS)
        if result:
            results.append(result)

    # Summary table
    if results:
        print(f"\n\n{'='*70}")
        print("  SUMMARY — Competitor Throughput (10 durable steps/workflow)")
        print(f"{'='*70}")
        print(f"  {'Engine':<12} {'wf/s':>8} {'avg ms':>8} {'P50':>8} {'P95':>8} {'P99':>8} {'OK':>6}")
        print(f"  {'-'*12} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*6}")
        for r in results:
            print(
                f"  {r['engine']:<12} {r['throughput']:>8.1f} {r['avg_latency']:>8.1f} "
                f"{r['p50']:>8.1f} {r['p95']:>8.1f} {r['p99']:>8.1f} {r['successes']:>6}"
            )
        print()


if __name__ == "__main__":
    main()
