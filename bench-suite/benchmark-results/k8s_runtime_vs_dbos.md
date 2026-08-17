# Universal Benchmark: velocity-runtime vs dbos Engines

**Date:** 2026-08-17T13:17:15.270654+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 2

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | dbos | 0.1 | 7404 | 7404 | 7404 | 1 | 2 |
| cold_start | velocity-runtime | 0.2 | 5945 | 5945 | 5945 | 3 | 0 |
| concurrent | dbos | 91.3 | 102557 | 125536 | 125536 | 120 | 0 |
| concurrent | velocity-runtime | 246.9 | 25155 | 44626 | 44626 | 120 | 0 |
| durable_promise | dbos | 68.4 | 14461 | 16171 | 16171 | 30 | 0 |
| durable_promise | velocity-runtime | 145.9 | 6625 | 8589 | 8589 | 30 | 0 |
| echo | dbos | 131.7 | 7572 | 9449 | 9449 | 60 | 0 |
| echo | velocity-runtime | 150.5 | 5204 | 59000 | 59000 | 60 | 0 |
| multi_step | dbos | 2.8 | 356889 | 365627 | 365627 | 12 | 0 |
| multi_step | velocity-runtime | 7.3 | 137883 | 144501 | 144501 | 12 | 0 |
| payload | dbos | 133.3 | 7532 | 8882 | 8882 | 30 | 0 |
| payload | velocity-runtime | 183.7 | 5398 | 6384 | 6384 | 30 | 0 |
| simple_workflow | dbos | 21.3 | 43237 | 68741 | 68741 | 30 | 0 |
| simple_workflow | velocity-runtime | 53.7 | 17934 | 27212 | 27212 | 30 | 0 |
| stateful | dbos | 68.1 | 14822 | 15692 | 15692 | 30 | 0 |
| stateful | velocity-runtime | 151.1 | 6613 | 7424 | 7424 | 30 | 0 |

## Head-to-Head Comparison

| Workload | Engine 1 (ops/s) | Engine 2 (ops/s) | Delta | Winner |
|----------|------------------:|------------------:|------:|--------|
| cold_start | 0.2 | 0.1 | +201.0% | velocity-runtime |
| simple_workflow | 53.7 | 21.3 | +152.5% | velocity-runtime |
| echo | 150.5 | 131.7 | +14.3% | velocity-runtime |
| multi_step | 7.3 | 2.8 | +157.0% | velocity-runtime |
| payload | 183.7 | 133.3 | +37.8% | velocity-runtime |
| stateful | 151.1 | 68.1 | +121.9% | velocity-runtime |
| concurrent | 246.9 | 91.3 | +170.5% | velocity-runtime |
| durable_promise | 145.9 | 68.4 | +113.4% | velocity-runtime |

**velocity-runtime wins: 8 | dbos wins: 0**
