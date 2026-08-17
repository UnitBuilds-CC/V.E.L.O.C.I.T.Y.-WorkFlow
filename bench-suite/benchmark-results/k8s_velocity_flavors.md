# Universal Benchmark: velocity-runtime vs velocity-classic Engines

**Date:** 2026-08-17T13:09:54.986823700+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 2

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | velocity-classic | 0.2 | 6237 | 6237 | 6237 | 3 | 0 |
| cold_start | velocity-runtime | 0.2 | 6518 | 6518 | 6518 | 3 | 0 |
| concurrent | velocity-classic | 189.2 | 32215 | 58587 | 58587 | 120 | 0 |
| concurrent | velocity-runtime | 156.7 | 32125 | 118681 | 118681 | 120 | 0 |
| durable_promise | velocity-classic | 152.0 | 6477 | 7349 | 7349 | 30 | 0 |
| durable_promise | velocity-runtime | 130.2 | 8050 | 8807 | 8807 | 30 | 0 |
| echo | velocity-classic | 177.5 | 5169 | 9848 | 9848 | 60 | 0 |
| echo | velocity-runtime | 179.5 | 5567 | 7450 | 7450 | 60 | 0 |
| multi_step | velocity-classic | 7.5 | 132543 | 135590 | 135590 | 12 | 0 |
| multi_step | velocity-runtime | 7.3 | 137074 | 141373 | 141373 | 12 | 0 |
| payload | velocity-classic | 183.4 | 5360 | 7036 | 7036 | 30 | 0 |
| payload | velocity-runtime | 192.4 | 5139 | 6003 | 6003 | 30 | 0 |
| simple_workflow | velocity-classic | 56.7 | 17431 | 19694 | 19694 | 30 | 0 |
| simple_workflow | velocity-runtime | 56.5 | 17125 | 22218 | 22218 | 30 | 0 |
| stateful | velocity-classic | 113.4 | 7827 | 37282 | 37282 | 30 | 0 |
| stateful | velocity-runtime | 153.7 | 6568 | 7379 | 7379 | 30 | 0 |

## Head-to-Head Comparison

| Workload | Engine 1 (ops/s) | Engine 2 (ops/s) | Delta | Winner |
|----------|------------------:|------------------:|------:|--------|
| echo | 179.5 | 177.5 | +1.1% | tie |
| multi_step | 7.3 | 7.5 | -2.5% | tie |
| concurrent | 156.7 | 189.2 | -17.2% | velocity-classic |
| stateful | 153.7 | 113.4 | +35.5% | velocity-runtime |
| payload | 192.4 | 183.4 | +4.9% | tie |
| durable_promise | 130.2 | 152.0 | -14.3% | velocity-classic |
| cold_start | 0.2 | 0.2 | -0.2% | tie |
| simple_workflow | 56.5 | 56.7 | -0.2% | tie |

**velocity-runtime wins: 1 | velocity-classic wins: 2**
