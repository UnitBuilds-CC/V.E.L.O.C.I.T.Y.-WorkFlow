# Universal Benchmark: velocity-runtime vs restate Engines

**Date:** 2026-08-17T13:00:43.362801400+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 2

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | restate | 0.2 | 9137 | 9137 | 9137 | 3 | 0 |
| cold_start | velocity-runtime | 0.2 | 4925 | 4925 | 4925 | 3 | 0 |
| concurrent | restate | 152.4 | 39140 | 71829 | 71829 | 120 | 0 |
| concurrent | velocity-runtime | 236.7 | 24536 | 45510 | 45510 | 120 | 0 |
| durable_promise | restate | 122.1 | 7446 | 11838 | 11838 | 30 | 0 |
| durable_promise | velocity-runtime | 166.8 | 5548 | 8950 | 8950 | 30 | 0 |
| echo | restate | 110.7 | 9248 | 12589 | 12589 | 60 | 0 |
| echo | velocity-runtime | 217.8 | 4289 | 8043 | 8043 | 60 | 0 |
| multi_step | restate | 78.5 | 13031 | 14680 | 14680 | 12 | 0 |
| multi_step | velocity-runtime | 7.2 | 140696 | 156710 | 156710 | 12 | 0 |
| payload | restate | 128.6 | 7596 | 11166 | 11166 | 30 | 0 |
| payload | velocity-runtime | 236.4 | 4170 | 4870 | 4870 | 30 | 0 |
| simple_workflow | restate | 33.1 | 29688 | 34017 | 34017 | 30 | 0 |
| simple_workflow | velocity-runtime | 52.9 | 15584 | 72467 | 72467 | 30 | 0 |
| stateful | restate | 113.3 | 7521 | 20514 | 20514 | 30 | 0 |
| stateful | velocity-runtime | 164.1 | 5619 | 9409 | 9409 | 30 | 0 |

## Head-to-Head Comparison

| Workload | Engine 1 (ops/s) | Engine 2 (ops/s) | Delta | Winner |
|----------|------------------:|------------------:|------:|--------|
| simple_workflow | 52.9 | 33.1 | +59.7% | velocity-runtime |
| payload | 236.4 | 128.6 | +83.8% | velocity-runtime |
| cold_start | 0.2 | 0.2 | +0.1% | tie |
| echo | 217.8 | 110.7 | +96.8% | velocity-runtime |
| concurrent | 236.7 | 152.4 | +55.3% | velocity-runtime |
| durable_promise | 166.8 | 122.1 | +36.7% | velocity-runtime |
| multi_step | 7.2 | 78.5 | -90.9% | restate |
| stateful | 164.1 | 113.3 | +44.8% | velocity-runtime |

**velocity-runtime wins: 6 | restate wins: 1**
