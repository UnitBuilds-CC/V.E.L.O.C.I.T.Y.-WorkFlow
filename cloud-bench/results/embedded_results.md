# VELOCITY-WorkFlow Embedded vs DBOS — Benchmark Report

**Status:** Pending  
**VELOCITY version:** 0.1.0  
**DBOS version:** Latest  
**Test Configuration:** GCE e2-standard-4 VMs, us-east1-b zone

## Summary

The Velocity Embedded vs DBOS comparison benchmark is pending completion.

For the primary Temporal comparison results (Velocity Classic vs Temporal), see:
- [temporal_comparison.md](./temporal_comparison.md)
- [classic_results.md](./classic_results.md)

## Planned Test Configuration

- **Velocity Embedded**: In-process workflow engine with Postgres persistence (DBOS replacement)
- **DBOS**: Latest stable version with PostgreSQL
- **Workloads**: All 21 standard benchmark workloads
- **Profile**: Standard (1x multiplier)
- **Format**: JSON with statistical aggregation

## Expected Metrics

- Throughput (ops/sec)
- p50/p95/p99/p999 latency
- Peak memory usage
- Peak CPU usage
- Error rate

## Notes

The Embedded flavor runs in-process with Postgres persistence, making direct comparison with DBOS's Postgres-based durable execution. This comparison will demonstrate Velocity's performance advantage for the DBOS replacement use case.
