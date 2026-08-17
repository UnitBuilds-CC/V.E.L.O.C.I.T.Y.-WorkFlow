# Universal Benchmark: velocity-runtime Engines

**Date:** 2026-08-17T13:57:03.928868600+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 1

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | velocity-runtime | 0.2 | 5654 | 5654 | 5654 | 3 | 0 |
| concurrent | velocity-runtime | 205.6 | 26581 | 95166 | 95166 | 120 | 0 |
| durable_promise | velocity-runtime | 170.5 | 5829 | 7532 | 7532 | 30 | 0 |
| echo | velocity-runtime | 197.8 | 4491 | 11815 | 11815 | 60 | 0 |
| multi_step | velocity-runtime | 5.8 | 183635 | 229664 | 229664 | 12 | 0 |
| payload | velocity-runtime | 216.2 | 4641 | 5199 | 5199 | 30 | 0 |
| simple_workflow | velocity-runtime | 56.5 | 17467 | 23388 | 23388 | 30 | 0 |
| stateful | velocity-runtime | 158.5 | 6014 | 10081 | 10081 | 30 | 0 |
