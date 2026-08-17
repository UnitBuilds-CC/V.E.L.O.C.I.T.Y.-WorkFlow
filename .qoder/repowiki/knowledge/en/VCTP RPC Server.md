# VCTP RPC Server

## Overview

The VCTP RPC server (`VctpRpcServer`) processes incoming VCTP packets through a full security and dispatch pipeline. It implements circuit breaker protection, per-client rate limiting, authentication, idempotency checking, inflight tracking, heartbeat mechanism, and graceful drain.

## Request Processing Pipeline

Every incoming VCTP packet goes through this pipeline in order:

```
UDP receive → Reorder buffer → JSON parse → Drain check → Circuit breaker →
Rate limit → Auth check → Idempotency check → Inflight tracking → Dispatch →
Response send
```

### Pipeline Stages

| Stage | Action | Rejection |
|-------|--------|-----------|
| Drain check | Reject if `draining` flag is set | 503 "server draining" |
| Circuit breaker | Check if overloaded (Closed/Open/HalfOpen) | 503 "service overloaded" |
| Rate limit | Token bucket per client IP | 429 "rate limit exceeded" |
| Auth check | JWT or API key validation | 401 "authentication required" |
| Idempotency | Duplicate request detection | 409 "duplicate idempotency key" |
| Inflight tracking | Count concurrent requests | Trip circuit if ≥ max_inflight |
| Dispatch | Route to method handler | 404 "unknown method" |

## Circuit Breaker

Three-state circuit breaker for graceful degradation:

```
Closed → (overload) → Open → (cooldown) → HalfOpen → (probe success) → Closed
                                              → (probe failure) → Open
```

- **Closed:** Normal operation, all requests processed
- **Open:** Reject all requests with 503, cooldown timer running
- **HalfOpen:** Allow one probe request; success → Closed, failure → Open
- **Config:** `max_inflight` (default 10,000), `cooldown_ms` (default 5,000)

## Heartbeat Mechanism

Periodic heartbeat for connection health monitoring:

- **Interval:** 30 seconds (configurable)
- **Tracking:** Per-client `last_seen` timestamp in `client_info: HashMap<SocketAddr, ClientInfo>`
- **Payload:** JSON with circuit state, inflight count, request/error stats
- **Eviction:** Clients not seen for `timeout_secs` (default 90s) are evicted

## Graceful Drain

Zero-downtime shutdown for K8s rolling updates:

1. `begin_drain()` sets `draining` flag
2. New requests receive 503 "server draining"
3. In-flight requests complete normally
4. K8s `preStop` hook sleeps `drainTimeoutSeconds` (default 30s) before SIGTERM
5. `wait_for_drain()` waits up to timeout for in-flight count to reach 0

## Method Dispatch

Routes VCTP method IDs to handler functions:

| Method ID | Name | Handler |
|-----------|------|---------|
| 100 | START_WORKFLOW | `handle_start_workflow()` |
| 101 | SIGNAL_WORKFLOW | `handle_signal_workflow()` |
| 102 | QUERY_WORKFLOW | `handle_query_workflow()` |
| 103 | CANCEL_WORKFLOW | `handle_cancel_workflow()` |
| 104 | TERMINATE_WORKFLOW | `handle_terminate_workflow()` |
| 105 | DESCRIBE_WORKFLOW | `handle_describe_workflow()` |
| 106 | COMPLETE_WORKFLOW | `handle_complete_workflow()` |
| 107 | UPDATE_WORKFLOW | `handle_update_workflow()` |
| 108 | RESET_WORKFLOW | `handle_reset_workflow()` |
| 200 | HEALTH_CHECK | `handle_health_check()` |
| 501 | RECORD_HEARTBEAT | `handle_record_heartbeat()` |
| 502 | COUNT_WORKFLOWS | `handle_count_workflows()` |
| 503 | BATCH_SIGNAL | `handle_batch_signal()` |
| 606 | SIGNAL_WITH_START | `handle_signal_with_start()` |

## Async Worker Pool

Tokio-based async processing with configurable worker count:

```rust
pub async fn run_async(self: &Arc<Self>, num_workers: usize) {
    let (tx, rx) = tokio::sync::mpsc::channel(4096);
    for worker_id in 0..num_workers {
        tokio::spawn(async move {
            loop {
                let item = rx.recv().await;
                // process_request(payload, addr)
            }
        });
    }
}
```

- Channel buffer: 4,096 pending requests
- Each worker holds `Arc<VctpRpcServer>`
- Workers process requests concurrently

## Prometheus Metrics

`export_prometheus_metrics()` generates standard Prometheus text format:

```
# HELP vctp_requests_total Total VCTP requests received.
# TYPE vctp_requests_total counter
vctp_requests_total 1000
# HELP vctp_responses_total Total VCTP responses sent.
vctp_responses_total 990
# HELP vctp_errors_total Total VCTP errors.
vctp_errors_total 10
# HELP vctp_request_duration_seconds Request duration histogram.
vctp_request_duration_seconds_count 990
```

## Source Files

| File | Role |
|------|------|
| `velocity-workflow-engine/src/vctp_rpc.rs` | VctpRpcServer, pipeline, dispatch, heartbeat, drain, metrics |
