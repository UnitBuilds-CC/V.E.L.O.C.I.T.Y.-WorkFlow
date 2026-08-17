# Universal Benchmark: velocity-runtime Engines

**Date:** 2026-08-17T14:10:12.780449+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 1

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | velocity-runtime | 0.2 | 5341 | 5341 | 5341 | 3 | 0 |
| concurrent | velocity-runtime | 225.3 | 27592 | 51456 | 51456 | 120 | 0 |
| durable_promise | velocity-runtime | 130.4 | 7793 | 11351 | 11351 | 30 | 0 |
| echo | velocity-runtime | 194.8 | 4656 | 10096 | 10096 | 60 | 0 |
| multi_step | velocity-runtime | 6.4 | 162877 | 171590 | 171590 | 12 | 0 |
| payload | velocity-runtime | 195.3 | 4821 | 7927 | 7927 | 30 | 0 |
| simple_workflow | velocity-runtime | 48.6 | 17068 | 77671 | 77671 | 30 | 0 |
| stateful | velocity-runtime | 154.9 | 6203 | 9092 | 9092 | 30 | 0 |
