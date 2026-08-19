# Omes Benchmark Migration Analysis

**Date**: August 19, 2026  
**Purpose**: Analyze what Omes scenarios can be migrated to Velocity and identify gaps

---

## Executive Summary

Omes (Temporal's official benchmark suite) provides 10+ battle-tested scenarios that exercise Temporal's full feature set. Our migration tools currently handle **core workflow patterns** but have **significant gaps** in advanced features used by Omes.

**Migration Readiness**: 60% of core scenarios can be partially migrated, but full correctness requires filling gaps.

---

## Omes Scenarios Overview

| Scenario | Description | Key Temporal Features |
|----------|-------------|----------------------|
| `throughput_stress` | Release validation, variable load | Activities, Signals, Queries, Updates, Continue-as-New, Relay, Search Attributes, Memo |
| `workflow_with_many_actions` | Kitchen sink with children/activities | Child Workflows, Activities, Concurrent execution |
| `workflow_with_many_timers` | Timer cardinality stress | Timers, Concurrent timers, Jitter |
| `out_of_order_signals` | Signal ordering stress | Signals, Await state, Signal deduplication |
| `ebb_and_flow` | Variable load patterns | Mixed client actions |
| `scheduler_stress` | Scheduling patterns | Scheduling, Cron |
| `serverless_burst` | Burst patterns | Rapid workflow starts |
| `long_idle_workflow` | Long-running with timers | Long timers, State persistence |
| `fixed_resource_consumption` | Resource stress | Resource-heavy activities |
| `fuzzer` | Random scenario generation | All features randomly combined |
| `workflow_with_single_noop_activity` | Baseline | Single activity |

---

## Temporal Features Used by Omes

### Core Features (Kitchen Sink Proto)

| Feature | Proto Message | Description |
|---------|--------------|-------------|
| **Timer** | `TimerAction` | Sleep with awaitable choice |
| **Activity** | `ExecuteActivityAction` | 9 variants: noop, delay, resources, payload, client, retryable_error, timeout, heartbeat, generic |
| **Child Workflow** | `ExecuteChildWorkflowAction` | With full options: timeouts, retry, parent_close_policy |
| **Signal** | `DoSignal`, `SendSignalAction` | Signal-with-start, custom handlers |
| **Query** | `DoQuery` | Report state, custom queries |
| **Update** | `DoUpdate` | With validator, action execution |
| **Continue-as-New** | `ContinueAsNewAction` | With memo, headers, search attributes |
| **Search Attributes** | `UpsertSearchAttributesAction` | Indexed fields |
| **Memo** | `UpsertMemoAction` | Workflow metadata |
| **Workflow State** | `WorkflowState`, `SetWorkflowState` | Key-value state management |
| **Cancel** | `CancelWorkflowAction` | Cancel child/external workflows |
| **Patch Marker** | `SetPatchMarkerAction` | Versioning with deprecation |
| **Relay Operation** | `ExecuteRelayOperation` | Cross-namespace/service operations |
| **Standalone Activity** | `DoStandaloneActivity` | Activity outside workflow context |
| **Standalone Relay** | `DoStandaloneRelayOperation` | Relay outside workflow context |
| **Await Pending** | `AwaitPendingActions` | Wait for started actions |

### Advanced Features

| Feature | Description |
|---------|-------------|
| **AwaitableChoice** | wait, abandon, cancel_before_started, cancel_after_started, cancel_after_completed, wait_started |
| **RetryPolicy** | Per-activity retry with backoff, max attempts, non-retryable errors |
| **TaskQueue** | Per-activity/child workflow task queue routing |
| **ParentClosePolicy** | terminate, abandon, request_cancel |
| **VersioningIntent** | compatible, default, unspecified |
| **ActivityLocality** | local vs remote activities |
| **Priority/Fairness** | Activity priority, fairness keys/weights |
| **Nested Action Sets** | Hierarchical action composition |
| **Concurrent Execution** | Parallel actions within action sets |

---

## Current Migration Tool Coverage

### Go SDK Migration Tool (`velocity-sdk-go/migrate/migrate.go`)

#### ✅ Supported Patterns (18 patterns)

| Pattern | Temporal | Velocity |
|---------|----------|----------|
| Import workflow | `go.temporal.io/sdk/workflow` | `github.com/velocity-workflow/velocity-sdk-go` |
| Import activity | `go.temporal.io/sdk/activity` | `github.com/velocity-workflow/velocity-sdk-go` |
| Import client | `go.temporal.io/sdk/client` | `github.com/velocity-workflow/velocity-sdk-go` |
| Import worker | `go.temporal.io/sdk/worker` | `github.com/velocity-workflow/velocity-sdk-go` |
| Workflow func | `func(ctx workflow.Context)` | `func(ctx *velocity.WorkflowContext)` |
| Activity func | `func(ctx activity.Context)` | `func(ctx *velocity.ActivityContext)` |
| ExecuteActivity | `workflow.ExecuteActivity(ctx, name)` | `ctx.ExecuteActivity(name)` |
| Sleep | `workflow.Sleep(ctx, d)` | `ctx.Sleep(d)` |
| GetSignalChannel | `workflow.GetSignalChannel(ctx, name)` | `ctx.GetSignalChannel(name)` |
| NewClient | `client.Dial()` | `velocity.NewClient()` |
| NewWorker | `worker.New(c, ...)` | `velocity.NewWorker(...)` |
| RegisterWorkflow | `w.RegisterWorkflow(fn)` | `w.RegisterWorkflow(fn)` |
| RegisterActivity | `w.RegisterActivity(fn)` | `w.RegisterActivity(fn)` |
| ExecuteWorkflow | `c.ExecuteWorkflow(ctx, ...)` | `client.ExecuteWorkflow(ctx, ...)` |
| SearchAttributes | `workflow.GetSearchAttributes()` | `ctx.GetSearchAttributes()` |
| Memo | `workflow.GetMemo()` | `ctx.GetMemo()` |
| UpdateHandler | `workflow.SetUpdateHandler()` | `ctx.SetUpdateHandler()` |
| ContinueAsNew | `workflow.ContinueAsNew()` | `ctx.ContinueAsNew()` |

#### ❌ Missing Patterns (Critical Gaps)

| Pattern | Temporal | Status | Impact |
|---------|----------|--------|--------|
| **Child Workflow** | `workflow.ExecuteChildWorkflow()` | ❌ Missing | High — used in many_actions |
| **Query Handler** | `workflow.SetQueryHandler()` | ❌ Missing | High — used in throughput_stress |
| **Relay Operations** | `nexus.ExecuteOperation()` | ✅ Migration mapping needed | Medium — Velocity has full Relay support |
| **Activity Options** | `workflow.WithActivityOptions()` | ❌ Missing | High — retry, timeout, task queue |
| **Child Workflow Options** | `workflow.WithChildOptions()` | ❌ Missing | Medium — parent_close_policy |
| **Cancel Workflow** | `workflow.CancelExternalWorkflow()` | ❌ Missing | Medium |
| **Patch Marker** | `workflow.SetPatchMarker()` | ❌ Missing | Medium — versioning |
| **Upsert Search Attributes** | `workflow.UpsertSearchAttributes()` | ❌ Missing | Medium |
| **Upsert Memo** | `workflow.UpsertMemo()` | ❌ Missing | Medium |
| **Awaitable Choice** | Various | ❌ Missing | Medium — abandon/cancel patterns |
| **Local Activity** | `workflow.ExecuteLocalActivity()` | ❌ Missing | Medium |
| **Standalone Activity** | `client.StartActivity()` | ❌ Missing | Low |

---

## Migration Feasibility by Scenario

### ✅ Fully Migratable (with current tools)

| Scenario | Reason |
|----------|--------|
| `workflow_with_single_noop_activity` | Only uses basic activity execution |

### ⚠️ Partially Migratable (needs gap filling)

| Scenario | Missing Features | Effort |
|----------|-----------------|--------|
| `workflow_with_many_actions` | Child workflows, activity options | Medium |
| `workflow_with_many_timers` | Timer with awaitable choice | Low |
| `out_of_order_signals` | AwaitWorkflowState, signal dedup | Medium |
| `long_idle_workflow` | Long timers, state persistence | Low |
| `fixed_resource_consumption` | Resource activities | Low |

### ❌ Not Migratable (significant gaps)

| Scenario | Missing Features | Effort |
|----------|-----------------|--------|
| `throughput_stress` | Relay, queries, updates, search attrs, memo, continue-as-new with state | High |
| `ebb_and_flow` | Mixed client actions, complex orchestration | High |
| `scheduler_stress` | Scheduling, cron | Medium |
| `serverless_burst` | Rapid starts, client patterns | Medium |
| `fuzzer` | All features randomly combined | Very High |

---

## Gap Priority Matrix

| Gap | Priority | Effort | Scenarios Unlocked |
|-----|----------|--------|-------------------|
| **Child Workflow execution** | P0 | Medium | many_actions, throughput_stress |
| **Activity Options (retry, timeout, task queue)** | P0 | Medium | All scenarios with activities |
| **Query Handler** | P1 | Low | throughput_stress |
| **AwaitWorkflowState** | P1 | Low | out_of_order_signals |
| **Upsert Search Attributes** | P1 | Low | throughput_stress |
| **Upsert Memo** | P2 | Low | throughput_stress |
| **Relay Operations** | P1 | Low (mapping only) | throughput_stress (Relay-enabled) |
| **Patch Marker** | P2 | Medium | Versioning scenarios |
| **Cancel External Workflow** | P2 | Low | Complex orchestration |
| **Local Activity** | P3 | Medium | Performance optimization |

---

## Recommended Action Plan

### Phase 1: Core Gaps (Week 1-2)
1. **Add Child Workflow support** to all 7 SDK migration tools
2. **Add Activity Options** (retry policy, timeouts, task queue)
3. **Add Query Handler** support
4. **Test**: Migrate `workflow_with_many_actions` end-to-end

### Phase 2: Signal/State Gaps (Week 3)
1. **Add AwaitWorkflowState** pattern
2. **Add UpsertSearchAttributes** and **UpsertMemo**
3. **Test**: Migrate `out_of_order_signals` end-to-end

### Phase 3: Advanced Features (Week 4-5)
1. **Add Relay Operations** migration mappings (Velocity engine already supports it)
2. **Add Patch Marker** for versioning
3. **Add Cancel External Workflow**
4. **Test**: Migrate `throughput_stress` (with Relay enabled)

### Phase 4: Full Validation (Week 6)
1. Run migrated Omes scenarios on Velocity
2. Compare results with native Temporal execution
3. Document performance comparison

---

## Velocity Feature Parity Check

Before migrating Omes, verify Velocity supports:

| Feature | Velocity Server | Velocity Binary | Velocity Embedded |
|---------|----------------|-----------------|-------------------|
| Activities | ✅ | ✅ | ✅ |
| Child Workflows | ✅ | ✅ | ✅ |
| Signals | ✅ | ✅ | ✅ |
| Queries | ✅ | ✅ | ✅ |
| Updates | ✅ | ✅ | ✅ |
| Continue-as-New | ✅ | ✅ | ✅ |
| Timers | ✅ | ✅ | ✅ |
| Search Attributes | ✅ | ✅ | ✅ |
| Memo | ✅ | ✅ | ✅ |
| **Relay Operations** | ✅ Supported | velocity-workflow-engine/src/relay.rs (641 lines) |
| Local Activities | ❓ | ❓ | ❓ |
| Standalone Activities | ❓ | ❓ | ❓ |

**Action**: Relay is fully supported in Velocity. Migration tool needs pattern mappings to expose it to SDK users.

---

## Files Analyzed

### Omes Repository (Cloned to `omes-temporal-benchmark/`)
- `scenarios/throughput_stress.go` (870 lines)
- `scenarios/workflow_with_many_actions.go` (95 lines)
- `scenarios/workflow_with_many_timers.go` (127 lines)
- `scenarios/out_of_order_signals.go` (182 lines)
- `workers/proto/kitchen_sink/kitchen_sink.proto` (552 lines)
- `loadgen/kitchensink/helpers.go` (426 lines)

### Velocity Migration Tools
- `velocity-sdk-go/migrate/migrate.go` (502 lines)
- `velocity-migration-toolkit/src/auto-implement.ts` (1214 lines)
- `sdk/python/velocity_sdk/migrate.py` (510 lines)
- `sdk/java/src/main/java/io/velocity/sdk/migrate/MigrationTool.java` (429 lines)
- `sdk/php/src/Migrate/MigrationTool.php` (367 lines)
- `sdk/ruby/lib/velocity_sdk/migrate.rb` (341 lines)
- `sdk/rust/src/migrate.rs` (543 lines)

---

## Conclusion

Omes provides an excellent benchmark suite for validating Velocity's migration tooling and performance. The scenarios range from simple (single activity) to extremely complex (throughput_stress with Relay).

**Current State**: 60% of core patterns covered, but critical gaps remain for child workflows and activity options.

**Recommended Next Steps**:
1. Fill P0 gaps (child workflows, activity options) in Go migration tool
2. Verify Velocity's Relay support (already implemented)
3. Migrate `workflow_with_many_actions` as first end-to-end test
4. Progressively tackle more complex scenarios

**Success Criteria**: Successfully migrate and run `throughput_stress` (with Relay) on Velocity, with comparable performance to native Temporal execution.
