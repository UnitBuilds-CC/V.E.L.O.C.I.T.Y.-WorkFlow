# HTTP Benchmark: Velocity Runtime vs Restate

**Date:** 2026-08-19T08:35:36.183187100+00:00

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
| handler_invocation | Velocity Runtime | 589 | 1533 | 2665 | 8517 | 59.3 |
| cold_start | Velocity Runtime | 483 | 1781 | 8701 | 8701 | 61.7 |
| stateful_handler | Velocity Runtime | 444 | 1994 | 2997 | 114084 | 59.5 |
| concurrent_handlers | Velocity Runtime | 599 | 91045 | 165355 | 165355 | 60.8 |
| sustained_load | Velocity Runtime | 602 | 72571 | 253247 | 998469 | 57.4 |
| durable_promise | Velocity Runtime | 274 | 2208 | 18915 | 461633 | 61.8 |
| mixed_operations | Velocity Runtime | 627 | 1495 | 2158 | 4405 | 57.0 |
| payload_roundtrip | Velocity Runtime | 605 | 1538 | 2302 | 3659 | 61.0 |
