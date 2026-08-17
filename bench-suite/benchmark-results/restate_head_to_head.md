# HTTP Benchmark: Velocity Runtime vs Restate

**Date:** 2026-08-17T10:46:13.553492200+00:00

**Profile:** standard

## Summary

| Metric | Value |
|--------|-------|
| Velocity Runtime wins | 7 |
| Restate wins | 0 |
| Comparable | 8 |
| Avg throughput delta | +54.3% |

## Detailed Comparison

| Workload | Engine | ops/sec | p50 (us) | p99 (us) | p999 (us) | Mem (MB) |
|----------|--------|---------|----------|----------|-----------|----------|
| payload_roundtrip | Velocity Runtime | 211 | 4406 | 11839 | 20327 | 0.0 |
| payload_roundtrip | Velocity Runtime | 207 | 4456 | 11021 | 14820 | 0.0 |
| payload_roundtrip | Velocity Runtime | 201 | 4449 | 9959 | 164840 | 0.0 |
| payload_roundtrip | Restate | 122 | 8043 | 13532 | 15919 | 0.0 |
| payload_roundtrip | Restate | 114 | 8098 | 16063 | 174195 | 0.0 |
| payload_roundtrip | Restate | 120 | 8088 | 14444 | 15365 | 0.0 |
| mixed_operations | Velocity Runtime | 200 | 4570 | 12227 | 26652 | 0.0 |
| mixed_operations | Velocity Runtime | 196 | 4536 | 12788 | 22788 | 0.0 |
| mixed_operations | Velocity Runtime | 184 | 4510 | 18181 | 167554 | 0.0 |
| mixed_operations | Restate | 104 | 8929 | 18350 | 57416 | 0.0 |
| mixed_operations | Restate | 104 | 8893 | 23605 | 149015 | 0.0 |
| mixed_operations | Restate | 106 | 8745 | 22739 | 34739 | 0.0 |
| durable_promise | Velocity Runtime | 171 | 5711 | 7532 | 7532 | 0.0 |
| durable_promise | Velocity Runtime | 170 | 5681 | 8511 | 8511 | 0.0 |
| durable_promise | Velocity Runtime | 167 | 5831 | 7431 | 7431 | 0.0 |
| durable_promise | Restate | 99 | 9563 | 16995 | 16995 | 0.0 |
| durable_promise | Restate | 115 | 8779 | 10380 | 10380 | 0.0 |
| durable_promise | Restate | 110 | 8758 | 14601 | 14601 | 0.0 |
| sustained_load | Velocity Runtime | 243 | 124686 | 331069 | 423955 | 0.0 |
| sustained_load | Velocity Runtime | 246 | 127058 | 284745 | 346077 | 0.0 |
| sustained_load | Velocity Runtime | 18 | 137638 | 227320 | 232348 | 0.0 |
| sustained_load | Restate | 27 | 150396 | 313947 | 333060 | 0.0 |
| sustained_load | Restate | 150 | 170809 | 402042 | 567099 | 0.0 |
| sustained_load | Restate | 151 | 173146 | 378608 | 468199 | 0.0 |
| concurrent_handlers | Velocity Runtime | 265 | 217794 | 374722 | 374722 | 0.0 |
| concurrent_handlers | Velocity Runtime | 251 | 248378 | 398603 | 398603 | 0.0 |
| concurrent_handlers | Velocity Runtime | 256 | 217810 | 390100 | 390100 | 0.0 |
| concurrent_handlers | Restate | 181 | 280071 | 552185 | 552185 | 0.0 |
| concurrent_handlers | Restate | 183 | 278248 | 546366 | 546366 | 0.0 |
| concurrent_handlers | Restate | 139 | 447684 | 720182 | 720182 | 0.0 |
| cold_start | Velocity Runtime | 2 | 5753 | 21354 | 21354 | 0.0 |
| cold_start | Velocity Runtime | 2 | 4602 | 4878 | 4878 | 0.0 |
| cold_start | Velocity Runtime | 2 | 4879 | 94523 | 94523 | 0.0 |
| cold_start | Restate | 2 | 8413 | 10203 | 10203 | 0.0 |
| cold_start | Restate | 2 | 8631 | 10454 | 10454 | 0.0 |
| cold_start | Restate | 2 | 8679 | 9707 | 9707 | 0.0 |
| handler_invocation | Velocity Runtime | 208 | 4393 | 10314 | 160359 | 0.0 |
| handler_invocation | Velocity Runtime | 200 | 4454 | 13322 | 139971 | 0.0 |
| handler_invocation | Velocity Runtime | 191 | 4633 | 14132 | 158929 | 0.0 |
| handler_invocation | Restate | 118 | 7959 | 16841 | 169971 | 0.0 |
| handler_invocation | Restate | 114 | 8001 | 19511 | 165474 | 0.0 |
| handler_invocation | Restate | 119 | 8076 | 15748 | 25090 | 0.0 |
| stateful_handler | Velocity Runtime | 133 | 5670 | 171885 | 171885 | 0.0 |
| stateful_handler | Velocity Runtime | 170 | 5738 | 12910 | 12910 | 0.0 |
| stateful_handler | Velocity Runtime | 168 | 5649 | 14797 | 14797 | 0.0 |
| stateful_handler | Restate | 109 | 8446 | 21691 | 21691 | 0.0 |
| stateful_handler | Restate | 112 | 8439 | 19946 | 19946 | 0.0 |
| stateful_handler | Restate | 114 | 8039 | 29687 | 29687 | 0.0 |
