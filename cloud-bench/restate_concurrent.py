#!/usr/bin/env python3
"""Concurrent benchmark for Restate - test concurrent throughput."""
import asyncio
import aiohttp
import time
import sys

async def benchmark_concurrent(url, total_requests, concurrency):
    """Run concurrent requests and measure throughput."""
    connector = aiohttp.TCPConnector(limit=concurrency * 2)
    timeout = aiohttp.ClientTimeout(total=60)
    
    success = 0
    fail = 0
    latencies = []
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        sem = asyncio.Semaphore(concurrency)
        
        async def run_one():
            nonlocal success, fail
            async with sem:
                start = time.perf_counter()
                try:
                    async with session.post(url, data=b'{}', headers={"Content-Type": "application/json"}) as resp:
                        await resp.read()
                        if resp.status == 200:
                            success += 1
                        else:
                            fail += 1
                except Exception as e:
                    fail += 1
                elapsed_us = (time.perf_counter() - start) * 1_000_000
                latencies.append(elapsed_us)
        
        bench_start = time.perf_counter()
        tasks = [run_one() for _ in range(total_requests)]
        await asyncio.gather(*tasks)
        wall_clock = time.perf_counter() - bench_start
    
    ops_per_sec = success / wall_clock if wall_clock > 0 else 0
    latencies.sort()
    n = len(latencies)
    p50 = latencies[int(n * 0.5)] if n > 0 else 0
    p99 = latencies[int(n * 0.99)] if n > 0 else 0
    p999 = latencies[int(n * 0.999)] if n > 0 else 0
    
    return {
        'total_requests': total_requests,
        'concurrency': concurrency,
        'success': success,
        'fail': fail,
        'wall_clock_sec': wall_clock,
        'ops_per_sec': ops_per_sec,
        'p50_us': p50,
        'p99_us': p99,
        'p999_us': p999,
    }

async def main():
    url = "http://localhost:8080/bench/invoke"
    
    print("=== Restate Concurrent Benchmark ===")
    print(f"URL: {url}")
    print()
    
    # Test different concurrency levels
    test_cases = [
        (100, 1),      # 100 requests, 1 concurrent (baseline)
        (1000, 10),    # 1000 requests, 10 concurrent
        (2000, 50),    # 2000 requests, 50 concurrent
        (5000, 100),   # 5000 requests, 100 concurrent
        (10000, 200),  # 10000 requests, 200 concurrent
    ]
    
    for total, concurrency in test_cases:
        print(f"Running {total} requests with concurrency={concurrency}...")
        result = await benchmark_concurrent(url, total, concurrency)
        print(f"  -> {result['ops_per_sec']:.1f} ops/sec, "
              f"p50={result['p50_us']:.0f}µs, "
              f"p99={result['p99_us']:.0f}µs, "
              f"p999={result['p999_us']:.0f}µs, "
              f"success={result['success']}/{result['total_requests']}")
        print()

if __name__ == "__main__":
    asyncio.run(main())
