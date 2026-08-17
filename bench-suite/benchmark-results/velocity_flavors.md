# Universal Benchmark: velocity-runtime vs velocity-classic Engines

**Date:** 2026-08-17T12:28:56.356003400+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 2

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | velocity-classic | 0.2 | 4683 | 4683 | 4683 | 3 | 0 |
| cold_start | velocity-runtime | 0.0 | 0 | 0 | 0 | 0 | 3 |
| concurrent | velocity-classic | 271.3 | 21700 | 39752 | 39752 | 120 | 0 |
| concurrent | velocity-runtime | 259.5 | 22983 | 41723 | 41723 | 120 | 0 |
| durable_promise | velocity-classic | 173.6 | 5636 | 7484 | 7484 | 30 | 0 |
| durable_promise | velocity-runtime | 173.8 | 5663 | 6946 | 6946 | 30 | 0 |
| echo | velocity-classic | 231.8 | 4106 | 6251 | 6251 | 60 | 0 |
| echo | velocity-runtime | 225.7 | 4258 | 6930 | 6930 | 60 | 0 |
| multi_step | velocity-classic | 7.8 | 128319 | 133699 | 133699 | 12 | 0 |
| multi_step | velocity-runtime | 0.0 | 0 | 0 | 0 | 0 | 12 |
| payload | velocity-classic | 227.0 | 4414 | 5117 | 5117 | 30 | 0 |
| payload | velocity-runtime | 227.8 | 4412 | 4978 | 4978 | 30 | 0 |
| simple_workflow | velocity-classic | 48.9 | 15773 | 67887 | 67887 | 30 | 0 |
| simple_workflow | velocity-runtime | 229.4 | 4230 | 5488 | 5488 | 30 | 0 |
| stateful | velocity-classic | 177.4 | 5389 | 7250 | 7250 | 30 | 0 |
| stateful | velocity-runtime | 180.6 | 5407 | 6718 | 6718 | 30 | 0 |

## Head-to-Head Comparison

| Workload | Engine 1 (ops/s) | Engine 2 (ops/s) | Delta | Winner |
|----------|------------------:|------------------:|------:|--------|
| cold_start | 0.0 | 0.2 | -100.0% | velocity-classic |
| simple_workflow | 229.4 | 48.9 | +369.3% | velocity-runtime |
| payload | 227.8 | 227.0 | +0.3% | tie |
| stateful | 180.6 | 177.4 | +1.8% | tie |
| concurrent | 259.5 | 271.3 | -4.3% | tie |
| durable_promise | 173.8 | 173.6 | +0.1% | tie |
| echo | 225.7 | 231.8 | -2.6% | tie |
| multi_step | 0.0 | 7.8 | -100.0% | velocity-classic |

**velocity-runtime wins: 1 | velocity-classic wins: 2**
