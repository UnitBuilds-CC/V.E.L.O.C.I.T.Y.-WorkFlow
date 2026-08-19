# HTTP Benchmark: Velocity Runtime vs Restate

**Date:** 2026-08-19T07:32:58.904527700+00:00

**Profile:** standard

## Summary

| Metric | Value |
|--------|-------|
| Velocity Runtime wins | 0 |
| Restate wins | 0 |
| Comparable | 0 |
| Avg throughput delta | +0.0% |

## Detailed Comparison

| Workload | Engine | ops/sec | p50 (us) | p99 (us) | p999 (us) | Mem (MB) |
|----------|--------|---------|----------|----------|-----------|----------|
| payload_roundtrip | Velocity Runtime | 608 | 1524 | 2235 | 4894 | 22.7 |
| mixed_operations | Velocity Runtime | 634 | 1474 | 1997 | 5710 | 45.2 |
| handler_invocation | Velocity Runtime | 622 | 1498 | 2054 | 3915 | 18.1 |
| sustained_load | Velocity Runtime | 658 | 68527 | 237390 | 924073 | 39.6 |
| cold_start | Velocity Runtime | 623 | 1460 | 1950 | 1950 | 45.3 |
| stateful_handler | Velocity Runtime | 503 | 1936 | 2532 | 4231 | 18.6 |
| durable_promise | Velocity Runtime | 513 | 1898 | 2508 | 2951 | 45.4 |
| concurrent_handlers | Velocity Runtime | 698 | 80113 | 142134 | 142134 | 20.4 |
