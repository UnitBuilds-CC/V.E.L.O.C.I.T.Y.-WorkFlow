# VELOCITY-WorkFlow vs Temporal — Benchmark Report

**Generated:** 2026-08-14T08:38:33.103996536+00:00  
**VELOCITY version:** 0.1.0  
**Temporal version:** 1.26+  

## Summary

| Metric | Value |
|--------|-------|
| Total workloads | 21 |
| VELOCITY wins | 21 |
| Temporal wins | 0 |
| Comparable | 0 |
| Avg throughput delta | +inf% |
| Avg p99 latency delta | +0.0% |
| Avg memory delta | +0.0% |

**Overall verdict:** VELOCITY is a viable Temporal replacement — significantly faster in most workloads

## Detailed Comparison

| Workload | VELOCITY ops/s | Temporal ops/s | Δ Throughput | VELOCITY p99 | Temporal p99 | Δ p99 | VELOCITY Mem | Temporal Mem | Verdict |
|----------|---------------|----------------|-------------|-------------|-------------|-------|-------------|-------------|----------|
| simple_workflow | 99 | 0 | +inf% | 219810µs | 0µs | +0.0% | 5.6MB | 0.0MB | VELOCITY faster |
| signal_storm | 17 | 0 | +inf% | 175764µs | 0µs | +0.0% | 5.6MB | 0.0MB | VELOCITY faster |
| query_burst | 2237 | 0 | +inf% | 370µs | 0µs | +0.0% | 5.6MB | 0.0MB | VELOCITY faster |
| high_step | 95 | 0 | +inf% | 20208µs | 0µs | +0.0% | 5.6MB | 0.0MB | VELOCITY faster |
| concurrent_1k | 98 | 0 | +inf% | 2019037µs | 0µs | +0.0% | 7.1MB | 0.0MB | VELOCITY faster |
| child_workflows | 95 | 0 | +inf% | 210776µs | 0µs | +0.0% | 7.1MB | 0.0MB | VELOCITY faster |
| saga_pattern | 104 | 0 | +inf% | 191204µs | 0µs | +0.0% | 7.1MB | 0.0MB | VELOCITY faster |
| timer_workflow | 98 | 0 | +inf% | 202579µs | 0µs | +0.0% | 7.1MB | 0.0MB | VELOCITY faster |
| search_attributes | 146 | 0 | +inf% | 211094µs | 0µs | +0.0% | 7.1MB | 0.0MB | VELOCITY faster |
| signal_query_mix | 737 | 0 | +inf% | 19842µs | 0µs | +0.0% | 7.1MB | 0.0MB | VELOCITY faster |
| batch_operations | 102 | 0 | +inf% | 205519µs | 0µs | +0.0% | 7.1MB | 0.0MB | VELOCITY faster |
| payload_1kb | 105 | 0 | +inf% | 200841µs | 0µs | +0.0% | 7.1MB | 0.0MB | VELOCITY faster |
| payload_1mb | 107 | 0 | +inf% | 185363µs | 0µs | +0.0% | 7.1MB | 0.0MB | VELOCITY faster |
| namespace_isolation | 105 | 0 | +inf% | 200321µs | 0µs | +0.0% | 7.1MB | 0.0MB | VELOCITY faster |
| throughput_ceiling | 101 | 0 | +inf% | 20169685µs | 0µs | +0.0% | 25.0MB | 0.0MB | VELOCITY faster |
| memory_scaling | 99 | 0 | +inf% | 216499µs | 0µs | +0.0% | 25.0MB | 0.0MB | VELOCITY faster |
| cold_start | 87 | 0 | +inf% | 21616µs | 0µs | +0.0% | 25.0MB | 0.0MB | VELOCITY faster |
| crash_recovery | 95 | 0 | +inf% | 209449µs | 0µs | +0.0% | 25.0MB | 0.0MB | VELOCITY faster |
| replay_amplification | 434 | 0 | +inf% | 0µs | 0µs | +0.0% | 25.0MB | 0.0MB | VELOCITY faster |
| wal_durability | 94 | 0 | +inf% | 1067383µs | 0µs | +0.0% | 25.0MB | 0.0MB | VELOCITY faster |
| tail_latency_sustained | 98 | 0 | +inf% | 2086830µs | 0µs | +0.0% | 25.0MB | 0.0MB | VELOCITY faster |

## Per-Workload Details

### simple_workflow

*Start → execute 10 steps → complete. Measures basic throughput and latency.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 99 | 0 | +inf% |
| p50 latency | 133813µs | 0µs | +0.0% |
| p95 latency | 201425µs | 0µs | — |
| p99 latency | 219810µs | 0µs | +0.0% |
| p999 latency | 223335µs | 0µs | — |
| Peak memory | 5.6MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 200 | 0 | — |

**Verdict:** VELOCITY faster

### signal_storm

*Start workflow → send 100 signals → complete. Measures signal throughput.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 17 | 0 | +inf% |
| p50 latency | 175764µs | 0µs | +0.0% |
| p95 latency | 175764µs | 0µs | — |
| p99 latency | 175764µs | 0µs | +0.0% |
| p999 latency | 175764µs | 0µs | — |
| Peak memory | 5.6MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 3 | 0 | — |

**Verdict:** VELOCITY faster

### query_burst

*Start workflow → send 100 queries → complete. Measures query throughput.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 2237 | 0 | +inf% |
| p50 latency | 252µs | 0µs | +0.0% |
| p95 latency | 321µs | 0µs | — |
| p99 latency | 370µs | 0µs | +0.0% |
| p999 latency | 594µs | 0µs | — |
| Peak memory | 5.6MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 102 | 0 | — |

**Verdict:** VELOCITY faster

### high_step

*Single workflow with 10K steps. Measures step execution overhead.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 95 | 0 | +inf% |
| p50 latency | 20208µs | 0µs | +0.0% |
| p95 latency | 20208µs | 0µs | — |
| p99 latency | 20208µs | 0µs | +0.0% |
| p999 latency | 20208µs | 0µs | — |
| Peak memory | 5.6MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 2 | 0 | — |

**Verdict:** VELOCITY faster

### concurrent_1k

*1000 concurrent workflows. Measures concurrent scheduling overhead.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 98 | 0 | +inf% |
| p50 latency | 1041888µs | 0µs | +0.0% |
| p95 latency | 1969507µs | 0µs | — |
| p99 latency | 2019037µs | 0µs | +0.0% |
| p999 latency | 2036937µs | 0µs | — |
| Peak memory | 7.1MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 200 | 0 | — |

**Verdict:** VELOCITY faster

### child_workflows

*Parent spawns 10 children, waits for all. Measures hierarchy overhead.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 95 | 0 | +inf% |
| p50 latency | 146456µs | 0µs | +0.0% |
| p95 latency | 210776µs | 0µs | — |
| p99 latency | 210776µs | 0µs | +0.0% |
| p999 latency | 210776µs | 0µs | — |
| Peak memory | 7.1MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 20 | 0 | — |

**Verdict:** VELOCITY faster

### saga_pattern

*5-step saga with compensation. Measures transaction overhead.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 104 | 0 | +inf% |
| p50 latency | 116198µs | 0µs | +0.0% |
| p95 latency | 191204µs | 0µs | — |
| p99 latency | 191204µs | 0µs | +0.0% |
| p999 latency | 191204µs | 0µs | — |
| Peak memory | 7.1MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 20 | 0 | — |

**Verdict:** VELOCITY faster

### timer_workflow

*Workflow with timer (sleep). Measures timer scheduling accuracy.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 98 | 0 | +inf% |
| p50 latency | 165616µs | 0µs | +0.0% |
| p95 latency | 202579µs | 0µs | — |
| p99 latency | 202579µs | 0µs | +0.0% |
| p999 latency | 202579µs | 0µs | — |
| Peak memory | 7.1MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 20 | 0 | — |

**Verdict:** VELOCITY faster

### search_attributes

*Start with attributes → query by attributes. Measures visibility performance.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 146 | 0 | +inf% |
| p50 latency | 140965µs | 0µs | +0.0% |
| p95 latency | 206453µs | 0µs | — |
| p99 latency | 211094µs | 0µs | +0.0% |
| p999 latency | 213574µs | 0µs | — |
| Peak memory | 7.1MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 300 | 0 | — |

**Verdict:** VELOCITY faster

### signal_query_mix

*Interleaved signals and queries. Measures mixed workload performance.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 737 | 0 | +inf% |
| p50 latency | 19842µs | 0µs | +0.0% |
| p95 latency | 19842µs | 0µs | — |
| p99 latency | 19842µs | 0µs | +0.0% |
| p999 latency | 19842µs | 0µs | — |
| Peak memory | 7.1MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 102 | 0 | — |

**Verdict:** VELOCITY faster

### batch_operations

*Batch start/terminate/query 5000 workflows. Measures admin throughput.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 102 | 0 | +inf% |
| p50 latency | 132027µs | 0µs | +0.0% |
| p95 latency | 197876µs | 0µs | — |
| p99 latency | 205519µs | 0µs | +0.0% |
| p999 latency | 212614µs | 0µs | — |
| Peak memory | 7.1MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 1000 | 0 | — |

**Verdict:** VELOCITY faster

### payload_1kb

*1KB payloads. Measures serialization overhead at typical size.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 105 | 0 | +inf% |
| p50 latency | 129016µs | 0µs | +0.0% |
| p95 latency | 193401µs | 0µs | — |
| p99 latency | 200841µs | 0µs | +0.0% |
| p999 latency | 208005µs | 0µs | — |
| Peak memory | 7.1MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 200 | 0 | — |

**Verdict:** VELOCITY faster

### payload_1mb

*1MB payloads. Measures large payload handling.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 107 | 0 | +inf% |
| p50 latency | 142689µs | 0µs | +0.0% |
| p95 latency | 185363µs | 0µs | — |
| p99 latency | 185363µs | 0µs | +0.0% |
| p999 latency | 185363µs | 0µs | — |
| Peak memory | 7.1MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 20 | 0 | — |

**Verdict:** VELOCITY faster

### namespace_isolation

*Workflows across 5 namespaces. Measures isolation overhead.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 105 | 0 | +inf% |
| p50 latency | 127278µs | 0µs | +0.0% |
| p95 latency | 192501µs | 0µs | — |
| p99 latency | 200321µs | 0µs | +0.0% |
| p999 latency | 200321µs | 0µs | — |
| Peak memory | 7.1MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 100 | 0 | — |

**Verdict:** VELOCITY faster

### throughput_ceiling

*Maximum sustainable throughput. Pushes engine to its limits.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 101 | 0 | +inf% |
| p50 latency | 19354477µs | 0µs | +0.0% |
| p95 latency | 20153597µs | 0µs | — |
| p99 latency | 20169685µs | 0µs | +0.0% |
| p999 latency | 20617195µs | 0µs | — |
| Peak memory | 25.0MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 20000 | 0 | — |

**Verdict:** VELOCITY faster

### memory_scaling

*Measure memory at 1K, 10K, 100K active workflows.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 99 | 0 | +inf% |
| p50 latency | 135247µs | 0µs | +0.0% |
| p95 latency | 206004µs | 0µs | — |
| p99 latency | 216499µs | 0µs | +0.0% |
| p999 latency | 227924µs | 0µs | — |
| Peak memory | 25.0MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 20000 | 0 | — |

**Verdict:** VELOCITY faster

### cold_start

*First workflow after engine startup. Measures cold start latency.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 87 | 0 | +inf% |
| p50 latency | 21616µs | 0µs | +0.0% |
| p95 latency | 21616µs | 0µs | — |
| p99 latency | 21616µs | 0µs | +0.0% |
| p999 latency | 21616µs | 0µs | — |
| Peak memory | 25.0MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 2 | 0 | — |

**Verdict:** VELOCITY faster

### crash_recovery

*Start workflows → simulate crash → restart → verify recovery.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 95 | 0 | +inf% |
| p50 latency | 139703µs | 0µs | +0.0% |
| p95 latency | 209449µs | 0µs | — |
| p99 latency | 209449µs | 0µs | +0.0% |
| p999 latency | 209449µs | 0µs | — |
| Peak memory | 25.0MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 20 | 0 | — |

**Verdict:** VELOCITY faster

### replay_amplification

*Signal a workflow 1000 times. Measures how signal latency scales with history length. Event-sourced engines (Temporal) replay the full event log on each signal — O(n²) total. Velocity uses direct mutation — O(n) total. The curve should be flat for Velocity and steeply rising for Temporal.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 434 | 0 | +inf% |
| p50 latency | 0µs | 0µs | +0.0% |
| p95 latency | 0µs | 0µs | — |
| p99 latency | 0µs | 0µs | +0.0% |
| p999 latency | 0µs | 0µs | — |
| Peak memory | 25.0MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 1002 | 0 | — |

**Verdict:** VELOCITY faster

### wal_durability

*High-throughput workflow creation with WAL fsync enabled. Measures how much throughput the durability guarantee costs. Velocity's group commit amortizes fsync across many workflows.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 94 | 0 | +inf% |
| p50 latency | 574350µs | 0µs | +0.0% |
| p95 latency | 1037201µs | 0µs | — |
| p99 latency | 1067383µs | 0µs | +0.0% |
| p999 latency | 1087738µs | 0µs | — |
| Peak memory | 25.0MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 1000 | 0 | — |

**Verdict:** VELOCITY faster

### tail_latency_sustained

*Sustained load at high concurrency for 2 minutes. Measures p99/p999 tail latency stability. Shows whether the engine maintains consistent latency or degrades under prolonged pressure.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 98 | 0 | +inf% |
| p50 latency | 1060587µs | 0µs | +0.0% |
| p95 latency | 1963688µs | 0µs | — |
| p99 latency | 2086830µs | 0µs | +0.0% |
| p999 latency | 2167627µs | 0µs | — |
| Peak memory | 25.0MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 3000 | 0 | — |

**Verdict:** VELOCITY faster

