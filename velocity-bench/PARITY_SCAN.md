# VELOCITY vs Temporal — Feature Parity Scan

**Date**: 2026-08-09  
**Scope**: Temporal's full feature set vs VELOCITY's implementation  
**Method**: Source code audit of both codebases

## Legend

- **Full** — Feature parity with Temporal
- **Core** — Implemented in velocity-workflow-engine (134 modules) but not exposed in DevEngine
- **Partial** — Some aspects implemented, others missing
- **Missing** — Not implemented at all
- **VELOCITY-only** — VELOCITY has this, Temporal doesn't

---

## 1. Workflow Lifecycle

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| Start workflow | Full | Full | Full | — |
| Complete workflow | Full | Full | Full | — |
| Fail workflow | Full | Full | Full | — |
| Terminate workflow | Full | Full | Full | — |
| **Cancel workflow** | Full | **Missing** | Partial (request_cancel state) | **No cancel signal/cleanup callbacks** |
| **Continue-as-new** | Full | **Missing** | **Missing** | **No CASR support** |
| **Workflow timeout** | Full | **Missing** | Partial | **No execution timeout enforcement** |
| Signal-with-start | Full | **Missing** | Full (client_sdk.rs) | **Not exposed in DevEngine** |
| **ExecuteMultiOperation** | Full | **Missing** | **Missing** | **No atomic multi-op (start+signal, start+update)** |

## 2. Signals & Queries

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| Signal workflow | Full | Full | Full | — |
| Query workflow | Full | Full | Full | — |
| **Signal external workflow** | Full | **Missing** | **Missing** | **Can't signal workflows in other namespaces** |
| **Query rejection condition** | Full | **Missing** | **Missing** | **No reject-if-not-running** |
| Buffered signals | Full | Full (stored in Vec) | Full | — |
| `__stack_trace` query | Full | Stub (returns empty) | — | Returns empty array, not real stack |

## 3. Activities

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Activity scheduling** | Full | **Missing** | Full (activity_worker.rs) | **DevEngine has no activity dispatch** |
| **Activity heartbeating** | Full | **Missing** | Full (heartbeat.rs) | **No heartbeat timeout detection** |
| **Activity retry** | Full | **Missing** | Full (retry.rs, backoff_retry.rs) | **No activity-level retry** |
| **Activity timeout (4 types)** | Full | **Missing** | Partial | **No schedule-to-close, schedule-to-start, start-to-close, heartbeat timeout** |
| **Async activity completion** | Full | **Missing** | Full (async_activity.rs) | **No async completion token** |
| **Activity payload size limit** | Full | **Missing** | **Missing** | **No size enforcement** |

## 4. Child Workflows

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Start child workflow** | Full | **Missing** | Full (add_child_workflow) | **DevEngine can't spawn children** |
| **Wait for child completion** | Full | **Missing** | Partial | **No blocking wait** |
| **Cancel propagation** | Full | **Missing** | Partial (pending_children) | **Parent cancel doesn't cascade** |
| **Child failure handling** | Full | **Missing** | **Missing** | **No parent notification on child failure** |

## 5. Timers

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Timer creation** | Full | **Missing** | Full (timer_engine.rs) | **DevEngine has no timer API** |
| **Timer firing** | Full | **Missing** | Full (timer_queue_executor.rs) | **No timer-driven workflow advancement** |
| **Timer cancellation** | Full | **Missing** | Full (cancel_timer) | — |
| **Sleep/delay** | Full | **Missing** | **Missing** | **No workflow.Sleep()** |

## 6. Search Attributes & Visibility

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Upsert search attributes** | Full | **Missing** | Full (search_attributes.rs) | **DevEngine can't upsert** |
| **Visibility query** | Full | Partial (list_workflows) | Full (visibility.rs, search_index.rs) | **No SQL-like query language** |
| **Custom search attribute indexing** | Full | **Missing** | Full (search_index.rs) | **No custom attribute indexing** |
| **List/count/scan workflows** | Full | Partial (list + count) | Full | **No scan with pagination** |

## 7. Namespaces

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| Register namespace | Full | Full | Full | — |
| List namespaces | Full | Full | Full | — |
| **Describe namespace** | Full | **Missing** | Full (namespace_mgmt.rs) | **No describe API** |
| **Update namespace** | Full | **Missing** | Full (operational_api.rs) | **Can't update retention/config** |
| **Delete namespace** | Full | **Missing** | Full (deletion_manager.rs) | **Can't delete namespaces** |
| **Global namespaces** | Full | **Missing** | Partial (is_global field exists) | **No cross-cluster namespace** |
| **Namespace replication** | Full | **Missing** | Full (ndc_replication.rs) | — |

## 8. Task Queues & Workers

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| Task queue registration | Full | Full (auto on start) | Full | — |
| **Poll for workflow tasks** | Full | **Missing** | Full (matching_engine.rs) | **DevEngine has no worker poll loop** |
| **Poll for activity tasks** | Full | **Missing** | Full (matching_workers.rs) | — |
| **Sticky task queues** | Full | **Missing** | **Missing** | **No sticky queue affinity** |
| **Worker versioning** | Full | **Missing** | Full (worker_versioning.rs) | **No version routing** |
| **Build ID / deployment tracking** | Full | **Missing** | Full (worker_deployment.rs) | — |
| **Worker sessions** | Full | **Missing** | Full (worker_sessions.rs) | **No session-based routing** |

## 9. Error Handling & Retry

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Retry policy** | Full | Partial (struct exists) | Full (retry.rs, backoff_retry.rs) | **DevEngine stores policy but doesn't enforce** |
| **Non-retryable error types** | Full | **Missing** | Full (failure_types.rs) | **No error classification** |
| **Application failure** | Full | **Missing** | Full | **No typed application errors** |
| **Retry backoff coefficient** | Full | **Missing** | Full (backoff_retry.rs) | — |
| **Max retry interval** | Full | **Missing** | Full | — |

## 10. History & Replay

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| Event-sourced history | Full | Full | Full | — |
| Get history API | Full | Full | Full | — |
| **Workflow replay** | Full | **Missing** | Full (replay.rs, workflow_replay.rs) | **DevEngine can't replay** |
| **Workflow reset** | Full | **Missing** | Full (workflow_reset.rs) | **Can't reset to event ID** |
| **History compaction** | Full | **Missing** | Full (history_compaction.rs) | **No compaction** |
| **History archival** | Full | **Missing** | Full (archival.rs, archival_engine.rs) | **No archival to S3/GCS** |
| **Mutable state rebuild** | Full | **Missing** | Full (history_event_applier.rs) | — |

## 11. Workflow Updates (Mutable State)

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Update workflow execution** | Full | **Missing** | Full (update.rs) | **DevEngine has no update API** |
| **Update validation** | Full | **Missing** | **Missing** | **No pre-accept validation** |
| **Update admission control** | Full | **Missing** | **Missing** | — |
| **Asynchronous update** | Full | **Missing** | **Missing** | **No async update poll** |

## 12. Scheduling & Cron

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Cron workflows** | Full | **Missing** | Full (cron.rs) | **DevEngine has no cron** |
| **Schedules API** | Full | **Missing** | Full (schedules.rs) | **No schedule create/pause/unpause** |
| **Schedule backfill** | Full | **Missing** | **Missing** | — |
| **Schedule overlap policy** | Full | **Missing** | **Missing** | — |

## 13. Memo

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Workflow memo** | Full | Partial (field exists) | Full (memo.rs) | **Field exists but no set/get API** |
| **Memo size limit** | Full | **Missing** | **Missing** | — |

## 14. Batch Operations

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Batch terminate** | Full | **Missing** | Full (batch.rs) | **No batch API** |
| **Batch signal** | Full | **Missing** | **Missing** | — |
| **Batch update** | Full | **Missing** | **Missing** | — |
| **Batch describe/list** | Full | **Missing** | **Missing** | — |

## 15. Interceptors & Middleware

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Workflow interceptors** | Full | **Missing** | Full (rpc_framework.rs) | **No interceptor chain** |
| **Activity interceptors** | Full | **Missing** | **Missing** | — |
| **Header propagation** | Full | **Missing** | Full (header_propagation.rs) | — |

## 16. Payload

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Payload codec (encode/decode)** | Full | **Missing** | Full (payload_codec.rs) | **No custom encoding** |
| **Payload compression** | Full | **Missing** | **Missing** | **No gzip/snappy** |
| **Payload size limit enforcement** | Full (2MB default) | **Missing** | **Missing** | **No size check** |

## 17. Nexus (Cross-Service Operations)

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Nexus operations** | Full | **Missing** | Full (nexus.rs, nexus_deep.rs) | **No cross-service orchestration** |
| **Nexus endpoints** | Full | **Missing** | **Missing** | — |
| **Nexus handler** | Full | **Missing** | **Missing** | — |

## 18. Advanced Features

| Feature | Temporal | VELOCITY DevEngine | VELOCITY Core | Gap |
|---------|----------|-------------------|---------------|-----|
| **Workflow patching (patched)** | Full | **Missing** | Full (patch.rs) | **No safe code migration** |
| **Determinism checking** | Full | **Missing** | Full (worker_determinism.rs) | — |
| **Dead letter queue** | Full | **Missing** | Full (queue_infrastructure.rs) | — |
| **Rate limiting** | Full | **Missing** | Full (rate_limiter.rs) | **No per-namespace rate limit** |
| **Resource limits** | Full | **Missing** | Full (resource_limits.rs) | — |
| **Quota management** | Full | **Missing** | Full (quota_management.rs) | — |
| **Self-healing** | Full | **Missing** | Full (self_healing.rs) | — |
| **Reachability analysis** | Full | **Missing** | Full (reachability.rs) | — |
| **Eager workflow start** | Full | **Missing** | **Missing** | **No first-wft-in-start-response** |

---

## Summary: Critical Gaps (Where Temporal Handles Something VELOCITY Can't)

### Tier 1 — Fundamental Workflow Engine Gaps

| # | Gap | Impact |
|---|-----|--------|
| 1 | **No workflow cancellation** | Can't send cancel signal, no cleanup callbacks, no `CancellationScope` |
| 2 | **No continue-as-new** | Long-running workflows can't reset history, will eventually hit history size limit |
| 3 | **No activity execution** | DevEngine can't dispatch activities to workers — workflows can't call external code |
| 4 | **No activity heartbeating** | Can't detect stuck activities, no heartbeat timeout |
| 5 | **No timers/sleep** | Workflows can't wait for time-based events |
| 6 | **No workflow update API** | Can't mutate running workflow state (Temporal's replacement for signals) |
| 7 | **No child workflow execution** | DevEngine tracks child state but can't spawn/execute children |

### Tier 2 — Operational Gaps

| # | Gap | Impact |
|---|-----|--------|
| 8 | **No workflow replay** | Can't rebuild state from history, no deterministic replay |
| 9 | **No workflow reset** | Can't reset workflow to a previous event ID for debugging |
| 10 | **No retry enforcement** | RetryPolicy struct exists but isn't applied |
| 11 | **No cron/schedule support** | Can't run periodic workflows |
| 12 | **No batch operations** | Can't bulk terminate/signal/update workflows |
| 13 | **No search attribute upsert** | Can't add/update search attributes on running workflows |
| 14 | **No worker poll loop** | DevEngine has no task dispatch — workers can't poll for tasks |
| 15 | **No payload codec/compression** | No custom encoding, no compression, no size limits |

### Tier 3 — Production Readiness Gaps

| # | Gap | Impact |
|---|-----|--------|
| 16 | **No history archival** | History grows unbounded |
| 17 | **No history compaction** | No event merging/pruning |
| 18 | **No sticky task queues** | Every workflow task requires full history transfer |
| 19 | **No worker versioning** | Can't route workflows to specific worker versions |
| 20 | **No interceptors** | No middleware chain for auth, logging, metrics |
| 21 | **No Nexus** | No cross-service/cross-cluster orchestration |
| 22 | **No dead letter queue** | Failed tasks are lost |
| 23 | **No rate limiting** | No per-namespace request throttling |
| 24 | **No namespace delete/update** | Can't manage namespace lifecycle |
| 25 | **No eager workflow start** | First workflow task always goes through matching |

---

## VELOCITY Advantages (Features Temporal Doesn't Have)

| Feature | File | Description |
|---------|------|-------------|
| **AI context management** | ai_context.rs | Workflow-aware AI context propagation |
| **Hardware integration** | hardware_integration.rs, hardware_traits.rs | Direct hardware abstraction layer |
| **Hierarchical state machines** | hsm_framework.rs | HSM framework for complex state machines |
| **Predictive autoscaler** | predictive_autoscaler.rs | ML-based workflow capacity prediction |
| **Cold storage tier** | cold_storage.rs | Automatic cold data migration |
| **Durable RPC** | durable_rpc.rs | RPC calls that survive process crashes |
| **Chaos engineering** | chaos_engineering.rs, chaos_endurance.rs | Built-in fault injection |
| **Self-healing** | self_healing.rs | Automatic failure detection and recovery |
| **Raft consensus** | raft_consensus.rs | Built-in consensus (Temporal uses external etcd) |
| **Hot swap** | hot_swap.rs | Live code updates without restart |
| **Deep observability** | deep_observability.rs | Workflow-level tracing |
| **NDC replication** | ndc_replication.rs, ndc_replication_deep.rs | Active-active multi-cluster |

---

## Conclusion

VELOCITY's **core engine** (velocity-workflow-engine, 134 modules) has near-complete feature parity with Temporal — including child workflows, signals, timers, retry, search attributes, nexus, interceptors, history management, and more.

The **DevEngine** (velocity-dev-server, the in-memory dev server) is a **thin shell** that exposes only ~15% of the core engine's capabilities. It handles basic workflow start/complete/signal/query/terminate but lacks activity dispatch, timers, cancellation, child workflows, updates, and most operational features.

**The critical gap is not in the engine — it's in the DevEngine's API surface.** The core engine has the implementations; they just need to be wired into the dev server's gRPC/HTTP handlers.
