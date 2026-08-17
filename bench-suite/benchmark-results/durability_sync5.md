# Universal Benchmark: velocity-runtime Engines

**Date:** 2026-08-17T13:56:39.696724100+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 1

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | velocity-runtime | 0.2 | 5951 | 5951 | 5951 | 3 | 0 |
| concurrent | velocity-runtime | 227.2 | 24921 | 49850 | 49850 | 120 | 0 |
| durable_promise | velocity-runtime | 156.5 | 5997 | 11581 | 11581 | 30 | 0 |
| echo | velocity-runtime | 194.6 | 4785 | 8104 | 8104 | 60 | 0 |
| multi_step | velocity-runtime | 7.4 | 138175 | 139665 | 139665 | 12 | 0 |
| payload | velocity-runtime | 210.8 | 4800 | 5833 | 5833 | 30 | 0 |
| simple_workflow | velocity-runtime | 58.9 | 16400 | 21174 | 21174 | 30 | 0 |
| stateful | velocity-runtime | 161.8 | 5935 | 8545 | 8545 | 30 | 0 |
