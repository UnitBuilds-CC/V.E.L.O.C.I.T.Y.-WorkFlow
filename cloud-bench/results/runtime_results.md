# VELOCITY-WorkFlow Runtime vs Restate — Benchmark Report

**Status:** Pending  
**VELOCITY version:** 0.1.0  
**Restate version:** Latest  
**Test Configuration:** GCE e2-standard-4 VMs, us-east1-b zone

## Summary

The Velocity Runtime vs Restate comparison benchmark is pending completion. 

For the primary Temporal comparison results (Velocity Classic vs Temporal), see:
- [temporal_comparison.md](./temporal_comparison.md)
- [classic_results.md](./classic_results.md)

## Planned Test Configuration

- **Velocity Runtime**: HTTP-based workflow engine (Restate replacement)
- **Restate**: Latest stable version
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

The Runtime flavor uses HTTP APIs instead of gRPC, making direct comparison with Restate's HTTP API. This comparison will demonstrate Velocity's performance advantage for the Restate replacement use case.
