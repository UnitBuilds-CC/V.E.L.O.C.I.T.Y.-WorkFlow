# Universal Benchmark: velocity-runtime Engines

**Date:** 2026-08-17T13:56:16.072032200+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 1

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | velocity-runtime | 0.2 | 7271 | 7271 | 7271 | 3 | 0 |
| concurrent | velocity-runtime | 199.6 | 31080 | 66612 | 66612 | 120 | 0 |
| durable_promise | velocity-runtime | 147.3 | 6112 | 12804 | 12804 | 30 | 0 |
| echo | velocity-runtime | 174.7 | 5269 | 8948 | 8948 | 60 | 0 |
| multi_step | velocity-runtime | 6.3 | 160321 | 211498 | 211498 | 12 | 0 |
| payload | velocity-runtime | 178.6 | 5646 | 9964 | 9964 | 30 | 0 |
| simple_workflow | velocity-runtime | 62.3 | 16013 | 17783 | 17783 | 30 | 0 |
| stateful | velocity-runtime | 149.0 | 6593 | 9270 | 9270 | 30 | 0 |
