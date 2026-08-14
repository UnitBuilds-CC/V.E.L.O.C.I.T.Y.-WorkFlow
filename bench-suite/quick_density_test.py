#!/usr/bin/env python3
"""Quick storage density: 20 ops per HTTP engine, then we measure storage."""
import requests, time, sys

N = 20

def run_test(name, url, n):
    ok = 0
    t0 = time.time()
    for i in range(n):
        try:
            r = requests.post(url, json={"data": f"op_{i}"}, timeout=30)
            if r.status_code == 200:
                ok += 1
        except Exception as e:
            print(f"  {name} op {i}: {e}")
    elapsed = time.time() - t0
    print(f"  {name}: {ok}/{n} ok in {elapsed:.1f}s ({ok/max(elapsed,0.1):.1f} ops/s)")
    return ok

print("=== STORAGE DENSITY: 20 ops per engine ===\n")

print("[DBOS simple_workflow]")
run_test("DBOS", "http://localhost:8081/bench/simple_workflow", N)

print("\n[Temporal simple_workflow]")
run_test("Temporal", "http://localhost:8083/bench/simple_workflow", N)

print("\n[Restate simple]")
ok = 0
t0 = time.time()
for i in range(N):
    try:
        r = requests.post(f"http://localhost:8082/bench/density_{i}/simple", json={}, timeout=30)
        if r.status_code == 200:
            ok += 1
    except:
        pass
elapsed = time.time() - t0
print(f"  Restate: {ok}/{N} ok in {elapsed:.1f}s")

print("\nDone. Now measure storage.")
