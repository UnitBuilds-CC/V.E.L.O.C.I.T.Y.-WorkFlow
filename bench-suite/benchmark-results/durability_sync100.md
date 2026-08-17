# Universal Benchmark: velocity-runtime Engines

**Date:** 2026-08-17T13:57:52.326550200+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 1

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | velocity-runtime | 0.2 | 5258 | 5258 | 5258 | 3 | 0 |
| concurrent | velocity-runtime | 214.8 | 27303 | 52584 | 52584 | 120 | 0 |
| durable_promise | velocity-runtime | 143.4 | 6318 | 10981 | 10981 | 30 | 0 |
| echo | velocity-runtime | 196.8 | 4947 | 6698 | 6698 | 60 | 0 |
| multi_step | velocity-runtime | 5.2 | 199010 | 240789 | 240789 | 12 | 0 |
| payload | velocity-runtime | 181.8 | 5241 | 9039 | 9039 | 30 | 0 |
| simple_workflow | velocity-runtime | 49.7 | 18627 | 32527 | 32527 | 30 | 0 |
| stateful | velocity-runtime | 142.2 | 7000 | 8856 | 8856 | 30 | 0 |
