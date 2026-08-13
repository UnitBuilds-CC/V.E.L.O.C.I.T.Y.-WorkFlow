#!/usr/bin/env python3
"""Quick DBOS benchmark via the local Docker container."""
import requests, sys, time, json

BASE = "http://localhost:8081"

# Health check
r = requests.get(f"{BASE}/health", timeout=5)
print(f"DBOS health: {r.json()}")

# Run simple_workflow smoke
print("\nRunning simple_workflow smoke test...")
start = time.time()
r = requests.post(f"{BASE}/bench/simple_workflow", json={}, timeout=30)
elapsed = time.time() - start
print(f"  HTTP {r.status_code}: {r.json()} ({elapsed:.2f}s)")

# Run cold_start
print("Running cold_start smoke test...")
start = time.time()
r = requests.post(f"{BASE}/bench/cold_start", json={}, timeout=30)
elapsed = time.time() - start
print(f"  HTTP {r.status_code}: {r.json()} ({elapsed:.2f}s)")

# Run echo
print("Running echo smoke test...")
start = time.time()
r = requests.post(f"{BASE}/bench/echo", json={"data": "x" * 256}, timeout=30)
elapsed = time.time() - start
print(f"  HTTP {r.status_code}: {r.json()} ({elapsed:.2f}s)")

print("\nDBOS smoke tests complete!")
