# Benchmark Scenarios

Canonical workload definitions for the Velocity multi-engine benchmark suite.

## Workload Matrix

### Core Workloads (all 6 engines)

| Workload | Description | Key Metric |
|----------|-------------|------------|
| `simple_workflow` | 10 durable steps, sequential | Baseline ops/sec |
| `cold_start` | First execution after startup | Startup latency |
| `signal_storm` | 100 durable signals per workflow | Signal throughput |
| `high_step` | 50-step sequential workflow | Per-step overhead |
| `concurrent_100` | 100 parallel workflows | Concurrency handling |
| `memory_scaling` | RSS at 1K/10K/100K active workflows | Memory efficiency |
| `payload_1kb_to_1mb` | Varying payload sizes | Payload throughput |

### Engine-Specific Strength Workloads

| Engine | Workload | What It Highlights |
|--------|----------|-------------------|
| Velocity Classic | `wal_group_commit` | Group commit amortization — 1000 ops with fsync |
| Velocity Classic | `crash_recovery` | WAL replay after kill — zero data loss |
| Velocity Embedded | `embedded_no_network` | In-process execution, zero network overhead |
| Velocity Runtime | `http_throughput` | HTTP/2 multiplexing under load |
| DBOS | `pg_transactional` | Complex PostgreSQL transactions |
| DBOS | `sql_visibility` | SQL queries over workflow state |
| Restate | `virtual_object_contention` | 1000 concurrent mutations on same keyed object |
| Restate | `reactive_chain` | Handler-to-handler durable calls |
| Temporal | `activity_scheduling` | Real activity execution with retry/timeout |
| Temporal | `long_running` | 5-minute workflow with timers + signals |

## Profiles

| Profile | Ops | Steps | Concurrency | Timeout | Duration |
|---------|-----|-------|-------------|---------|----------|
| `smoke` | 10 | 5 | 1 | 10s | ~30 seconds |
| `short` | 100 | 10 | 1 | 30s | ~5 minutes |
| `standard` | 1000 | 10 | 1 | 60s | ~10-15 minutes |
| `stress` | 10000 | 10 | 100 | 120s | ~30 minutes |

## File Format

`workloads.json` contains the full workload matrix with:
- `_profiles`: Profile definitions (ops, steps, concurrency, timeout)
- `core_workloads`: Workloads that run on all 6 engines
- `engine_strength_workloads`: Engine-specific workloads highlighting unique capabilities

Each workload specifies:
- `config`: ops, steps, signals, queries, payload_bytes, concurrency
- `primary_metrics`: Which metrics to focus on
- `engines`: Which engines should run this workload
