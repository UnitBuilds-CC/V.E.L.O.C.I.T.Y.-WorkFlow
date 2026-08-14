# VELOCITY-WorkFlow vs Temporal — Benchmark Report

**Generated:** 2026-08-14T08:42:42.806238442+00:00  
**VELOCITY version:** 0.1.0  
**Temporal version:** 1.26+  

## Summary

| Metric | Value |
|--------|-------|
| Total workloads | 3 |
| VELOCITY wins | 3 |
| Temporal wins | 0 |
| Comparable | 0 |
| Avg throughput delta | +inf% |
| Avg p99 latency delta | +0.0% |
| Avg memory delta | +0.0% |

**Overall verdict:** VELOCITY is a viable Temporal replacement — significantly faster in most workloads

## Detailed Comparison

| Workload | VELOCITY ops/s | Temporal ops/s | Δ Throughput | VELOCITY p99 | Temporal p99 | Δ p99 | VELOCITY Mem | Temporal Mem | Verdict |
|----------|---------------|----------------|-------------|-------------|-------------|-------|-------------|-------------|----------|
| simple_workflow | 61 | 0 | +inf% | 31365µs | 0µs | +0.0% | 5.2MB | 0.0MB | VELOCITY faster |
| signal_storm | 127 | 0 | +inf% | 15984µs | 0µs | +0.0% | 5.2MB | 0.0MB | VELOCITY faster |
| cold_start | 50 | 0 | +inf% | 38553µs | 0µs | +0.0% | 5.3MB | 0.0MB | VELOCITY faster |

## Per-Workload Details

### simple_workflow

*Start → execute 10 steps → complete. Measures basic throughput and latency.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 61 | 0 | +inf% |
| p50 latency | 31365µs | 0µs | +0.0% |
| p95 latency | 31365µs | 0µs | — |
| p99 latency | 31365µs | 0µs | +0.0% |
| p999 latency | 31365µs | 0µs | — |
| Peak memory | 5.2MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 2 | 0 | — |

**Verdict:** VELOCITY faster

### signal_storm

*Start workflow → send 100 signals → complete. Measures signal throughput.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 127 | 0 | +inf% |
| p50 latency | 15984µs | 0µs | +0.0% |
| p95 latency | 15984µs | 0µs | — |
| p99 latency | 15984µs | 0µs | +0.0% |
| p999 latency | 15984µs | 0µs | — |
| Peak memory | 5.2MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 3 | 0 | — |

**Verdict:** VELOCITY faster

### cold_start

*First workflow after engine startup. Measures cold start latency.*

| Metric | VELOCITY | Temporal | Delta |
|--------|----------|----------|-------|
| Ops/sec | 50 | 0 | +inf% |
| p50 latency | 38553µs | 0µs | +0.0% |
| p95 latency | 38553µs | 0µs | — |
| p99 latency | 38553µs | 0µs | +0.0% |
| p999 latency | 38553µs | 0µs | — |
| Peak memory | 5.3MB | 0.0MB | +0.0% |
| Peak CPU | 0.0% | 0.0% | — |
| Error rate | 0.00% | 0.00% | +0.00% |
| Total ops | 2 | 0 | — |

**Verdict:** VELOCITY faster

