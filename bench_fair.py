#!/usr/bin/env python3
"""Fair cross-engine benchmark — same HTTP protocol, same compute work, same network.

All engines are accessed via HTTP POST.  Each workflow executes 10 steps,
each doing SHA-256 hash chain computation (2000 iterations) and per-step
persistence to PostgreSQL.

Engines benchmarked:
  - Velocity (HTTP bench endpoint on workflow-server:8080)
  - DBOS     (FastAPI on dbos-service:8080)
  - Restate  (ingress on restate-server:8080)
  - Temporal (FastAPI on temporal-service:8080)

Usage:
  python3 bench_fair.py [iterations]
  Default: 500 iterations per engine.
"""

import http.client
import json
import time
import sys
import statistics

# Engine endpoints — all HTTP, all on the same Docker network.
# Service names match docker-compose.fair-bench.yml service definitions.
ENGINES = {
    "Velocity": ("workflow-server", 8080, "/bench/simple_workflow"),
    "DBOS":     ("dbos-service", 8080, "/bench/simple_workflow"),
    "Restate":  ("restate-server", 8080, "/bench/simple"),
    "Temporal": ("temporal-service", 8080, "/bench/simple_workflow"),
}

ITERATIONS = int(sys.argv[1]) if len(sys.argv) > 1 else 500
WARMUP = 5
TIMEOUT = 30  # seconds per request


def bench_engine(name: str, host: str, port: int, path: str, n: int):
    """Run n workflows against a single engine over HTTP."""
    # Warmup
    for i in range(WARMUP):
        try:
            conn = http.client.HTTPConnection(host, port, timeout=TIMEOUT)
            conn.request("POST", path,
                         body=json.dumps({"warmup": i}),
                         headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            resp.read()
            conn.close()
        except Exception as e:
            print(f"  {name}: warmup failed ({e}) — skipping")
            return None

    # Benchmark
    latencies = []
    failures = 0
    start = time.time()
    for i in range(n):
        t0 = time.monotonic()
        try:
            conn = http.client.HTTPConnection(host, port, timeout=TIMEOUT)
            conn.request("POST", path,
                         body=json.dumps({"id": i}),
                         headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            body = resp.read()
            conn.close()
            if resp.status != 200:
                failures += 1
        except Exception as e:
            failures += 1
        latencies.append((time.monotonic() - t0) * 1000)  # ms

    elapsed = time.time() - start
    ok = n - failures

    if ok == 0:
        print(f"  {name}: ALL {n} FAILED — engine unreachable at {host}:{port}")
        return None

    avg = statistics.mean(latencies)
    p50 = statistics.median(latencies)
    sorted_lat = sorted(latencies)
    p95 = sorted_lat[int(len(sorted_lat) * 0.95)] if len(sorted_lat) > 1 else avg
    p99 = sorted_lat[int(len(sorted_lat) * 0.99)] if len(sorted_lat) > 1 else avg

    print(f"  {name}: {ok}/{n} ok, {failures} fail | "
          f"{ok/elapsed:.1f} wf/s | "
          f"avg={avg:.1f}ms p50={p50:.1f}ms p95={p95:.1f}ms p99={p99:.1f}ms")

    return {
        "engine": name,
        "ok": ok,
        "failures": failures,
        "wf_s": ok / elapsed,
        "avg_ms": avg,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
    }


def main():
    print(f"=== Fair Cross-Engine Benchmark ===")
    print(f"Protocol: HTTP POST (same for all engines)")
    print(f"Workload: 10 steps/wf, SHA-256 x2000/step, per-step PG persist")
    print(f"Iterations: {ITERATIONS} per engine + {WARMUP} warmup")
    print()

    results = []
    for name, (host, port, path) in ENGINES.items():
        print(f"Benchmarking {name} ({host}:{port}{path})...")
        r = bench_engine(name, host, port, path, ITERATIONS)
        if r:
            results.append(r)
        print()

    # Summary table
    if results:
        print("=== Summary ===")
        print(f"{'Engine':<12} {'wf/s':>8} {'avg ms':>8} {'p50 ms':>8} {'p95 ms':>8} {'p99 ms':>8} {'fail':>6}")
        print("-" * 64)
        for r in sorted(results, key=lambda x: x["wf_s"], reverse=True):
            print(f"{r['engine']:<12} {r['wf_s']:>8.1f} {r['avg_ms']:>8.1f} "
                  f"{r['p50_ms']:>8.1f} {r['p95_ms']:>8.1f} {r['p99_ms']:>8.1f} "
                  f"{r['failures']:>6}")


if __name__ == "__main__":
    main()
