#!/usr/bin/env python3
"""Storage density test: run N operations per engine, measure storage delta."""
import requests, time, json, sys

N = 100  # operations per engine

def bench_velocity(host, port, label):
    """Velocity uses gRPC, so we use the bench client or just count WAL before/after."""
    # For Velocity we can't easily send gRPC from Python, so we'll use the velocity-bench binary
    # Instead, we'll just report the WAL size from docker
    print(f"  {label}: Skipping HTTP test (gRPC engine) - will measure WAL separately")
    return 0

def bench_dbos(n):
    BASE = "http://localhost:8081"
    ok = 0
    for i in range(n):
        try:
            r = requests.post(f"{BASE}/bench/simple_workflow", json={}, timeout=30)
            if r.status_code == 200:
                ok += 1
        except Exception as e:
            pass
        if (i+1) % 20 == 0:
            print(f"  DBOS: {i+1}/{n} ({ok} ok)")
    return ok

def bench_temporal(n):
    BASE = "http://localhost:8083"
    ok = 0
    for i in range(n):
        try:
            r = requests.post(f"{BASE}/bench/simple_workflow", json={}, timeout=60)
            if r.status_code == 200:
                ok += 1
        except Exception as e:
            pass
        if (i+1) % 20 == 0:
            print(f"  Temporal: {i+1}/{n} ({ok} ok)")
    return ok

def bench_restate(n):
    INGRESS = "http://localhost:8082"
    ok = 0
    for i in range(n):
        try:
            r = requests.post(f"{INGRESS}/bench/storage_{i}/simple", json={}, timeout=30)
            if r.status_code == 200:
                ok += 1
        except Exception as e:
            pass
        if (i+1) % 20 == 0:
            print(f"  Restate: {i+1}/{n} ({ok} ok)")
    return ok

if __name__ == "__main__":
    print(f"=== STORAGE DENSITY TEST: {N} ops per engine ===\n")
    
    print("[DBOS]")
    dbos_ok = bench_dbos(N)
    print(f"  DBOS complete: {dbos_ok}/{N} succeeded\n")
    
    print("[Temporal]")
    temporal_ok = bench_temporal(N)
    print(f"  Temporal complete: {temporal_ok}/{N} succeeded\n")
    
    print("[Restate]")
    restate_ok = bench_restate(N)
    print(f"  Restate complete: {restate_ok}/{N} succeeded\n")
    
    print(f"=== SUMMARY ===")
    print(f"  DBOS:    {dbos_ok}/{N} ops")
    print(f"  Temporal: {temporal_ok}/{N} ops")
    print(f"  Restate: {restate_ok}/{N} ops")
    print(f"\nNow measure storage with: docker exec <container> ...")
