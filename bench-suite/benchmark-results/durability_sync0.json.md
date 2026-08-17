# Universal Benchmark: velocity-runtime Engines

**Date:** 2026-08-17T13:55:20.344391100+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 1

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | velocity-runtime | 0.2 | 6675 | 6675 | 6675 | 3 | 0 |
| concurrent | velocity-runtime | 213.6 | 27512 | 52800 | 52800 | 120 | 0 |
| durable_promise | velocity-runtime | 149.7 | 6606 | 8570 | 8570 | 30 | 0 |
| echo | velocity-runtime | 151.3 | 5259 | 59048 | 59048 | 60 | 0 |
| multi_step | velocity-runtime | 7.5 | 132448 | 137905 | 137905 | 12 | 0 |
| payload | velocity-runtime | 144.2 | 6723 | 8549 | 8549 | 30 | 0 |
| simple_workflow | velocity-runtime | 61.0 | 16393 | 18116 | 18116 | 30 | 0 |
| stateful | velocity-runtime | 176.7 | 5484 | 6765 | 6765 | 30 | 0 |
