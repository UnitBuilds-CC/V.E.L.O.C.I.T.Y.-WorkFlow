# VELOCITY-WorkFlow vs Temporal — Benchmark Report

**Generated:** 2026-08-09T15:05:42.621258300+00:00  
**VELOCITY version:** 0.1.0  
**Temporal version:** 1.26+  

## Summary

| Metric | Value |
|--------|-------|
| Total workloads | 18 |
| VELOCITY wins | 0 |
| Temporal wins | 0 |
| Comparable | 18 |
| Avg throughput delta | +0.7% |
| Avg p99 latency delta | +102.4% |
| Avg memory delta | -0.5% |

**Overall verdict:** VELOCITY and Temporal are roughly comparable

## Detailed Comparison

| Workload | VELOCITY ops/s | Temporal ops/s | Δ Throughput | VELOCITY p99 | Temporal p99 | Δ p99 | VELOCITY Mem | Temporal Mem | Verdict |
|----------|---------------|----------------|-------------|-------------|-------------|-------|-------------|-------------|----------|
| simple_workflow | 847 | 749 | +13.2% | 553µs | 525µs | +5.3% | 12.0MB | 12.6MB | Comparable |
| signal_storm | 0 | 0 | +0.0% | 23593µs | 1220µs | +1833.9% | 12.1MB | 12.1MB | Comparable |
| query_burst | 0 | 0 | +0.0% | 457µs | 1534µs | -70.2% | 12.1MB | 12.1MB | Comparable |
| high_step | 0 | 0 | +0.0% | 448µs | 459µs | -2.4% | 12.2MB | 12.2MB | Comparable |
| concurrent_1k | 0 | 0 | +0.0% | 462µs | 509µs | -9.2% | 12.2MB | 12.2MB | Comparable |
| child_workflows | 0 | 0 | +0.0% | 480µs | 547µs | -12.2% | 12.2MB | 12.7MB | Comparable |
| saga_pattern | 0 | 0 | +0.0% | 534µs | 507µs | +5.3% | 12.7MB | 12.3MB | Comparable |
| timer_workflow | 0 | 0 | +0.0% | 495µs | 443µs | +11.7% | 12.2MB | 12.2MB | Comparable |
| search_attributes | 0 | 0 | +0.0% | 529µs | 473µs | +11.8% | 12.2MB | 12.8MB | Comparable |
| signal_query_mix | 0 | 0 | +0.0% | 513µs | 587µs | -12.6% | 12.7MB | 12.7MB | Comparable |
| batch_operations | 0 | 0 | +0.0% | 528µs | 461µs | +14.5% | 12.8MB | 12.7MB | Comparable |
| payload_1kb | 0 | 0 | +0.0% | 465µs | 504µs | -7.7% | 12.3MB | 12.9MB | Comparable |
| payload_1mb | 0 | 0 | +0.0% | 575µs | 588µs | -2.2% | 12.8MB | 12.8MB | Comparable |
| namespace_isolation | 0 | 0 | +0.0% | 596µs | 533µs | +11.8% | 12.7MB | 12.8MB | Comparable |
| throughput_ceiling | 0 | 0 | +0.0% | 578µs | 466µs | +24.0% | 12.8MB | 12.8MB | Comparable |
| memory_scaling | 0 | 0 | +0.0% | 476µs | 445µs | +7.0% | 12.8MB | 12.7MB | Comparable |
| cold_start | 0 | 0 | +0.0% | 467µs | 386µs | +21.0% | 12.8MB | 12.7MB | Comparable |
| crash_recovery | 0 | 0 | +0.0% | 584µs | 519µs | +12.5% | 13.3MB | 12.9MB | Comparable |

## Per-Workload Details

### simple_workflow

*Start → execute 10 steps → complete. Measures basic throughput and latency.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 847 | 749 | +13.2% |
| p50 latency | 347µs | 370µs | -6.2% |
| p95 latency | 458µs | 509µs | — |
| p99 latency | 553µs | 525µs | +5.3% |
| p999 latency | 555µs | 1141µs | — |
| Peak memory | 12.0MB | 12.6MB | -4.7% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 200 | 200 | — |

**Verdict:** Comparable

### signal_storm

*Start workflow → send 100 signals → complete. Measures signal throughput.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 373µs | 351µs | +6.3% |
| p95 latency | 23593µs | 1220µs | — |
| p99 latency | 23593µs | 1220µs | +1833.9% |
| p999 latency | 23593µs | 1220µs | — |
| Peak memory | 12.1MB | 12.1MB | +0.1% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 10 | 10 | — |

**Verdict:** Comparable

### query_burst

*Start workflow → send 100 queries → complete. Measures query throughput.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 376µs | 356µs | +5.6% |
| p95 latency | 457µs | 1534µs | — |
| p99 latency | 457µs | 1534µs | -70.2% |
| p999 latency | 457µs | 1534µs | — |
| Peak memory | 12.1MB | 12.1MB | +0.2% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 10 | 10 | — |

**Verdict:** Comparable

### high_step

*Single workflow with 10K steps. Measures step execution overhead.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 354µs | 347µs | +2.0% |
| p95 latency | 413µs | 439µs | — |
| p99 latency | 448µs | 459µs | -2.4% |
| p999 latency | 556µs | 503µs | — |
| Peak memory | 12.2MB | 12.2MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### concurrent_1k

*1000 concurrent workflows. Measures concurrent scheduling overhead.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 357µs | 358µs | -0.3% |
| p95 latency | 405µs | 441µs | — |
| p99 latency | 462µs | 509µs | -9.2% |
| p999 latency | 495µs | 517µs | — |
| Peak memory | 12.2MB | 12.2MB | -0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### child_workflows

*Parent spawns 10 children, waits for all. Measures hierarchy overhead.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 371µs | 360µs | +3.1% |
| p95 latency | 459µs | 436µs | — |
| p99 latency | 480µs | 547µs | -12.2% |
| p999 latency | 483µs | 781µs | — |
| Peak memory | 12.2MB | 12.7MB | -4.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### saga_pattern

*5-step saga with compensation. Measures transaction overhead.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 372µs | 348µs | +6.9% |
| p95 latency | 456µs | 419µs | — |
| p99 latency | 534µs | 507µs | +5.3% |
| p999 latency | 592µs | 558µs | — |
| Peak memory | 12.7MB | 12.3MB | +3.8% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### timer_workflow

*Workflow with timer (sleep). Measures timer scheduling accuracy.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 350µs | 353µs | -0.8% |
| p95 latency | 435µs | 423µs | — |
| p99 latency | 495µs | 443µs | +11.7% |
| p999 latency | 541µs | 456µs | — |
| Peak memory | 12.2MB | 12.2MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### search_attributes

*Start with attributes → query by attributes. Measures visibility performance.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 359µs | 363µs | -1.1% |
| p95 latency | 458µs | 443µs | — |
| p99 latency | 529µs | 473µs | +11.8% |
| p999 latency | 540µs | 574µs | — |
| Peak memory | 12.2MB | 12.8MB | -4.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### signal_query_mix

*Interleaved signals and queries. Measures mixed workload performance.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 385µs | 399µs | -3.5% |
| p95 latency | 470µs | 493µs | — |
| p99 latency | 513µs | 587µs | -12.6% |
| p999 latency | 545µs | 606µs | — |
| Peak memory | 12.7MB | 12.7MB | -0.2% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### batch_operations

*Batch start/terminate/query 5000 workflows. Measures admin throughput.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 391µs | 363µs | +7.7% |
| p95 latency | 476µs | 427µs | — |
| p99 latency | 528µs | 461µs | +14.5% |
| p999 latency | 555µs | 496µs | — |
| Peak memory | 12.8MB | 12.7MB | +0.2% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### payload_1kb

*1KB payloads. Measures serialization overhead at typical size.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 393µs | 415µs | -5.3% |
| p95 latency | 447µs | 472µs | — |
| p99 latency | 465µs | 504µs | -7.7% |
| p999 latency | 466µs | 514µs | — |
| Peak memory | 12.3MB | 12.9MB | -4.6% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### payload_1mb

*1MB payloads. Measures large payload handling.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 416µs | 396µs | +5.1% |
| p95 latency | 518µs | 487µs | — |
| p99 latency | 575µs | 588µs | -2.2% |
| p999 latency | 765µs | 835µs | — |
| Peak memory | 12.8MB | 12.8MB | +0.5% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### namespace_isolation

*Workflows across 5 namespaces. Measures isolation overhead.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 414µs | 379µs | +9.2% |
| p95 latency | 499µs | 456µs | — |
| p99 latency | 596µs | 533µs | +11.8% |
| p999 latency | 687µs | 616µs | — |
| Peak memory | 12.7MB | 12.8MB | -0.1% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### throughput_ceiling

*Maximum sustainable throughput. Pushes engine to its limits.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 379µs | 358µs | +5.9% |
| p95 latency | 457µs | 426µs | — |
| p99 latency | 578µs | 466µs | +24.0% |
| p999 latency | 836µs | 497µs | — |
| Peak memory | 12.8MB | 12.8MB | -0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### memory_scaling

*Measure memory at 1K, 10K, 100K active workflows.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 372µs | 342µs | +8.8% |
| p95 latency | 439µs | 414µs | — |
| p99 latency | 476µs | 445µs | +7.0% |
| p999 latency | 484µs | 640µs | — |
| Peak memory | 12.8MB | 12.7MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

### cold_start

*First workflow after engine startup. Measures cold start latency.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 467µs | 386µs | +21.0% |
| p95 latency | 467µs | 386µs | — |
| p99 latency | 467µs | 386µs | +21.0% |
| p999 latency | 467µs | 386µs | — |
| Peak memory | 12.8MB | 12.7MB | +0.1% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 1 | 1 | — |

**Verdict:** Comparable

### crash_recovery

*Start workflows → simulate crash → restart → verify recovery.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 0 | 0 | +0.0% |
| p50 latency | 391µs | 393µs | -0.5% |
| p95 latency | 478µs | 482µs | — |
| p99 latency | 584µs | 519µs | +12.5% |
| p999 latency | 2049µs | 548µs | — |
| Peak memory | 13.3MB | 12.9MB | +3.4% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 100 | — |

**Verdict:** Comparable

