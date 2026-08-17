# Universal Benchmark: velocity-runtime Engines

**Date:** 2026-08-17T14:10:59.260673400+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 1

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | velocity-runtime | 0.2 | 5408 | 5408 | 5408 | 3 | 0 |
| concurrent | velocity-runtime | 248.8 | 24824 | 43476 | 43476 | 120 | 0 |
| durable_promise | velocity-runtime | 116.5 | 7455 | 17070 | 17070 | 30 | 0 |
| echo | velocity-runtime | 203.8 | 4692 | 7381 | 7381 | 60 | 0 |
| multi_step | velocity-runtime | 6.3 | 152514 | 211956 | 211956 | 12 | 0 |
| payload | velocity-runtime | 191.9 | 4912 | 7711 | 7711 | 30 | 0 |
| simple_workflow | velocity-runtime | 50.5 | 20013 | 29764 | 29764 | 30 | 0 |
| stateful | velocity-runtime | 108.5 | 7786 | 18691 | 18691 | 30 | 0 |
