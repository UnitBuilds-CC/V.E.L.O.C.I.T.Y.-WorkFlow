# Universal Benchmark: velocity-classic vs temporal Engines

**Date:** 2026-08-17T12:27:53.989314900+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 2

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | temporal | 0.2 | 61944 | 61944 | 61944 | 3 | 0 |
| cold_start | velocity-classic | 0.2 | 6455 | 6455 | 6455 | 3 | 0 |
| concurrent | temporal | 21.5 | 179107 | 1530120 | 1530120 | 120 | 0 |
| concurrent | velocity-classic | 209.0 | 22100 | 97652 | 97652 | 120 | 0 |
| durable_promise | temporal | 10.3 | 90313 | 195328 | 195328 | 30 | 0 |
| durable_promise | velocity-classic | 117.6 | 8355 | 10986 | 10986 | 30 | 0 |
| echo | temporal | 16.6 | 55978 | 96042 | 96042 | 60 | 0 |
| echo | velocity-classic | 223.8 | 4246 | 7209 | 7209 | 60 | 0 |
| multi_step | temporal | 0.1 | 9094649 | 10051341 | 10051341 | 12 | 0 |
| multi_step | velocity-classic | 7.1 | 138296 | 166280 | 166280 | 12 | 0 |
| payload | temporal | 16.7 | 59555 | 74704 | 74704 | 30 | 0 |
| payload | velocity-classic | 185.2 | 5341 | 7813 | 7813 | 30 | 0 |
| simple_workflow | temporal | 1.7 | 700294 | 1056108 | 1056108 | 30 | 0 |
| simple_workflow | velocity-classic | 61.8 | 16357 | 17595 | 17595 | 30 | 0 |
| stateful | temporal | 3.6 | 263783 | 379561 | 379561 | 30 | 0 |
| stateful | velocity-classic | 92.0 | 9811 | 19266 | 19266 | 30 | 0 |

## Head-to-Head Comparison

| Workload | Engine 1 (ops/s) | Engine 2 (ops/s) | Delta | Winner |
|----------|------------------:|------------------:|------:|--------|
| multi_step | 7.1 | 0.1 | +6474.0% | velocity-classic |
| simple_workflow | 61.8 | 1.7 | +3570.7% | velocity-classic |
| durable_promise | 117.6 | 10.3 | +1039.1% | velocity-classic |
| cold_start | 0.2 | 0.2 | +1.2% | tie |
| stateful | 92.0 | 3.6 | +2436.9% | velocity-classic |
| concurrent | 209.0 | 21.5 | +871.7% | velocity-classic |
| echo | 223.8 | 16.6 | +1247.9% | velocity-classic |
| payload | 185.2 | 16.7 | +1007.1% | velocity-classic |

**velocity-classic wins: 7 | temporal wins: 0**
