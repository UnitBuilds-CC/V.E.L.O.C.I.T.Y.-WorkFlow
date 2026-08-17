# HTTP Benchmark: Velocity Runtime vs Restate

**Date:** 2026-08-17T10:57:07.062213500+00:00

**Profile:** standard

## Summary

| Metric | Value |
|--------|-------|
| Velocity Runtime wins | 7 |
| Restate wins | 0 |
| Comparable | 8 |
| Avg throughput delta | +52.9% |

## Detailed Comparison

| Workload | Engine | ops/sec | p50 (us) | p99 (us) | p999 (us) | Mem (MB) |
|----------|--------|---------|----------|----------|-----------|----------|
| payload_roundtrip | Velocity Runtime | 189 | 4667 | 14434 | 21441 | 0.0 |
| payload_roundtrip | Velocity Runtime | 208 | 4517 | 7909 | 12465 | 0.0 |
| payload_roundtrip | Velocity Runtime | 196 | 4490 | 10411 | 162370 | 0.0 |
| payload_roundtrip | Velocity Runtime | 215 | 4445 | 11193 | 12300 | 0.0 |
| payload_roundtrip | Velocity Runtime | 195 | 4474 | 11071 | 165401 | 0.0 |
| payload_roundtrip | Restate | 114 | 8220 | 17547 | 33709 | 0.0 |
| payload_roundtrip | Restate | 116 | 8020 | 14516 | 99432 | 0.0 |
| payload_roundtrip | Restate | 118 | 8133 | 14736 | 50809 | 0.0 |
| payload_roundtrip | Restate | 116 | 8109 | 18329 | 36811 | 0.0 |
| payload_roundtrip | Restate | 115 | 8133 | 15448 | 163357 | 0.0 |
| durable_promise | Velocity Runtime | 95 | 6198 | 181218 | 181218 | 0.0 |
| durable_promise | Velocity Runtime | 160 | 5928 | 13344 | 13344 | 0.0 |
| durable_promise | Velocity Runtime | 164 | 5682 | 13830 | 13830 | 0.0 |
| durable_promise | Velocity Runtime | 158 | 6075 | 16145 | 16145 | 0.0 |
| durable_promise | Velocity Runtime | 161 | 6048 | 9730 | 9730 | 0.0 |
| durable_promise | Restate | 111 | 8826 | 12778 | 12778 | 0.0 |
| durable_promise | Restate | 116 | 8597 | 10351 | 10351 | 0.0 |
| durable_promise | Restate | 117 | 8456 | 12644 | 12644 | 0.0 |
| durable_promise | Restate | 114 | 8506 | 18189 | 18189 | 0.0 |
| durable_promise | Restate | 117 | 8178 | 16979 | 16979 | 0.0 |
| cold_start | Velocity Runtime | 2 | 4499 | 9154 | 9154 | 0.0 |
| cold_start | Velocity Runtime | 2 | 5052 | 7081 | 7081 | 0.0 |
| cold_start | Velocity Runtime | 2 | 4975 | 7454 | 7454 | 0.0 |
| cold_start | Velocity Runtime | 2 | 4617 | 5156 | 5156 | 0.0 |
| cold_start | Velocity Runtime | 2 | 4721 | 7582 | 7582 | 0.0 |
| cold_start | Restate | 2 | 8193 | 8808 | 8808 | 0.0 |
| cold_start | Restate | 2 | 10404 | 34110 | 34110 | 0.0 |
| cold_start | Restate | 2 | 11967 | 14208 | 14208 | 0.0 |
| cold_start | Restate | 2 | 10193 | 12182 | 12182 | 0.0 |
| cold_start | Restate | 2 | 9607 | 10354 | 10354 | 0.0 |
| stateful_handler | Velocity Runtime | 162 | 5728 | 11873 | 11873 | 0.0 |
| stateful_handler | Velocity Runtime | 166 | 5724 | 12207 | 12207 | 0.0 |
| stateful_handler | Velocity Runtime | 161 | 5813 | 17605 | 17605 | 0.0 |
| stateful_handler | Velocity Runtime | 167 | 5869 | 12307 | 12307 | 0.0 |
| stateful_handler | Velocity Runtime | 171 | 5829 | 7101 | 7101 | 0.0 |
| stateful_handler | Restate | 114 | 8318 | 15662 | 15662 | 0.0 |
| stateful_handler | Restate | 124 | 7874 | 11731 | 11731 | 0.0 |
| stateful_handler | Restate | 54 | 8562 | 940440 | 940440 | 0.0 |
| stateful_handler | Restate | 115 | 8001 | 23992 | 23992 | 0.0 |
| stateful_handler | Restate | 115 | 8191 | 16404 | 16404 | 0.0 |
| handler_invocation | Velocity Runtime | 211 | 4483 | 9167 | 17131 | 0.0 |
| handler_invocation | Velocity Runtime | 187 | 4421 | 17999 | 162393 | 0.0 |
| handler_invocation | Velocity Runtime | 220 | 4334 | 7546 | 17555 | 0.0 |
| handler_invocation | Velocity Runtime | 209 | 4336 | 10164 | 165283 | 0.0 |
| handler_invocation | Velocity Runtime | 204 | 4407 | 13269 | 94188 | 0.0 |
| handler_invocation | Restate | 107 | 8892 | 16418 | 173516 | 0.0 |
| handler_invocation | Restate | 114 | 8332 | 15183 | 175403 | 0.0 |
| handler_invocation | Restate | 116 | 8037 | 16837 | 172255 | 0.0 |
| handler_invocation | Restate | 115 | 8205 | 20113 | 163089 | 0.0 |
| handler_invocation | Restate | 107 | 8312 | 23449 | 176994 | 0.0 |
| mixed_operations | Velocity Runtime | 187 | 4469 | 24525 | 161821 | 0.0 |
| mixed_operations | Velocity Runtime | 196 | 4598 | 13015 | 51729 | 0.0 |
| mixed_operations | Velocity Runtime | 195 | 4540 | 12915 | 52878 | 0.0 |
| mixed_operations | Velocity Runtime | 191 | 4508 | 23374 | 118346 | 0.0 |
| mixed_operations | Velocity Runtime | 201 | 4513 | 14333 | 40281 | 0.0 |
| mixed_operations | Restate | 93 | 9186 | 31320 | 176510 | 0.0 |
| mixed_operations | Restate | 102 | 8594 | 35249 | 173966 | 0.0 |
| mixed_operations | Restate | 108 | 8662 | 23851 | 52186 | 0.0 |
| mixed_operations | Restate | 104 | 8686 | 32357 | 59066 | 0.0 |
| mixed_operations | Restate | 106 | 8616 | 22320 | 164582 | 0.0 |
| concurrent_handlers | Velocity Runtime | 253 | 222072 | 392150 | 392150 | 0.0 |
| concurrent_handlers | Velocity Runtime | 256 | 208837 | 390414 | 390414 | 0.0 |
| concurrent_handlers | Velocity Runtime | 257 | 211196 | 387674 | 387674 | 0.0 |
| concurrent_handlers | Velocity Runtime | 264 | 231783 | 378914 | 378914 | 0.0 |
| concurrent_handlers | Velocity Runtime | 260 | 218129 | 384006 | 384006 | 0.0 |
| concurrent_handlers | Restate | 175 | 298924 | 571820 | 571820 | 0.0 |
| concurrent_handlers | Restate | 175 | 302754 | 569942 | 569942 | 0.0 |
| concurrent_handlers | Restate | 162 | 314384 | 616131 | 616131 | 0.0 |
| concurrent_handlers | Restate | 164 | 321874 | 610551 | 610551 | 0.0 |
| concurrent_handlers | Restate | 125 | 362085 | 801633 | 801633 | 0.0 |
| sustained_load | Velocity Runtime | 237 | 130839 | 330906 | 443075 | 0.0 |
| sustained_load | Velocity Runtime | 233 | 130337 | 351686 | 1130519 | 0.0 |
| sustained_load | Velocity Runtime | 56 | 132841 | 365636 | 389172 | 0.0 |
| sustained_load | Velocity Runtime | 3 | 99815 | 213283 | 214710 | 0.0 |
| sustained_load | Velocity Runtime | 229 | 133459 | 309355 | 412481 | 0.0 |
| sustained_load | Restate | 152 | 171703 | 414424 | 501515 | 0.0 |
| sustained_load | Restate | 140 | 178465 | 396418 | 521008 | 0.0 |
| sustained_load | Restate | 5 | 178680 | 382305 | 390224 | 0.0 |
| sustained_load | Restate | 150 | 171645 | 430520 | 536878 | 0.0 |
| sustained_load | Restate | 151 | 172720 | 378407 | 481838 | 0.0 |
