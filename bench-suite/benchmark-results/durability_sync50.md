# Universal Benchmark: velocity-runtime Engines

**Date:** 2026-08-17T13:57:27.787658700+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 1

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | velocity-runtime | 0.2 | 7480 | 7480 | 7480 | 3 | 0 |
| concurrent | velocity-runtime | 224.1 | 28409 | 54585 | 54585 | 120 | 0 |
| durable_promise | velocity-runtime | 163.8 | 5955 | 7978 | 7978 | 30 | 0 |
| echo | velocity-runtime | 216.2 | 4362 | 7758 | 7758 | 60 | 0 |
| multi_step | velocity-runtime | 6.5 | 158364 | 175168 | 175168 | 12 | 0 |
| payload | velocity-runtime | 217.6 | 4362 | 6211 | 6211 | 30 | 0 |
| simple_workflow | velocity-runtime | 53.4 | 17283 | 32414 | 32414 | 30 | 0 |
| stateful | velocity-runtime | 172.2 | 5787 | 7119 | 7119 | 30 | 0 |
