# Universal Benchmark: restate Engines

**Date:** 2026-08-17T12:44:33.451353400+00:00

**Profile:** quick | **Workloads:** 8 | **Engines:** 1

## Per-Workload Averages

| Workload | Engine | ops/s | p50 (µs) | p99 (µs) | p999 (µs) | Success | Fail |
|----------|--------|------:|---------:|---------:|----------:|--------:|-----:|
| cold_start | restate | 0.2 | 10625 | 10625 | 10625 | 1 | 0 |
| concurrent | restate | 129.4 | 45176 | 80914 | 80914 | 40 | 0 |
| durable_promise | restate | 128.2 | 7788 | 9312 | 9312 | 10 | 0 |
| echo | restate | 127.8 | 7801 | 8813 | 8813 | 20 | 0 |
| multi_step | restate | 51.4 | 21665 | 22511 | 22511 | 4 | 0 |
| payload | restate | 120.8 | 7958 | 13190 | 13190 | 10 | 0 |
| simple_workflow | restate | 29.3 | 31614 | 54961 | 54961 | 10 | 0 |
| stateful | restate | 110.1 | 9363 | 10907 | 10907 | 10 | 0 |
