# HTTP Benchmark: Velocity Runtime vs Restate

**Date:** 2026-08-19T07:13:34.821265500+00:00

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
| stateful_handler | Velocity Runtime | 508 | 1894 | 3865 | 3865 | 12.6 |
| sustained_load | Velocity Runtime | 676 | 67785 | 228518 | 284407 | 24.3 |
| payload_roundtrip | Velocity Runtime | 668 | 1450 | 2029 | 2306 | 14.2 |
| cold_start | Velocity Runtime | 510 | 1558 | 2401 | 2401 | 24.9 |
| concurrent_handlers | Velocity Runtime | 691 | 75575 | 142736 | 142736 | 14.1 |
| mixed_operations | Velocity Runtime | 668 | 1455 | 1947 | 2281 | 24.9 |
| durable_promise | Velocity Runtime | 509 | 1936 | 2464 | 2464 | 25.0 |
| handler_invocation | Velocity Runtime | 657 | 1477 | 1998 | 8066 | 12.5 |
