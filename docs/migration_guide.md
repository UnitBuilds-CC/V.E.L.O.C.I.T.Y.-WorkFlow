# Migration Guide: Temporal → VELOCITY-WorkFlow

> Step-by-step guide for migrating workflow applications from Temporal to VELOCITY-WorkFlow.

---

## Table of Contents

1. [Overview](#overview)
2. [Feature Mapping Table](#feature-mapping-table)
3. [Workflow Code Conversion](#workflow-code-conversion)
4. [Using the AST Transpiler](#using-the-ast-transpiler)
5. [Data Migration Strategies](#data-migration-strategies)
6. [SDK Migration](#sdk-migration)
7. [Common Pitfalls and Workarounds](#common-pitfalls-and-workarounds)
8. [Performance Comparison Expectations](#performance-comparison-expectations)
9. [Migration Checklist](#migration-checklist)

---

## Overview

VELOCITY-WorkFlow is a hardware-native workflow engine designed as a high-performance alternative to Temporal. It shares Temporal's core concepts — workflows, activities, signals, queries, task queues, and namespaces — but implements them with a zero-allocation Rust engine, slab memory model, and Merkle-verified state.

### Why Migrate?

| Benefit | Temporal | VELOCITY-WorkFlow |
|---------|----------|-------------------|
| GC pressure | Java/Go GC pauses | Zero-GC Rust runtime |
| Memory model | Managed heap | Slab allocator with bitmask tracking |
| State verification | Event sourcing replay | SHA-256 Merkle root per slab |
| Task dispatch | Matching service | Zero-alloc task queue with priority |
| Persistence | Cassandra/MySQL/PostgreSQL | WAL + PostgreSQL adapter |
| Latency | ms-range for dispatch | O(1) resumption, sub-ms dispatch |

---

## Feature Mapping Table

| Temporal Feature | VELOCITY-WorkFlow Equivalent | Notes |
|-----------------|------------------------------|-------|
| `WorkflowService` gRPC | `WorkflowService` gRPC | Same service name, compatible proto structure |
| `StartWorkflowExecution` | `StartWorkflowExecution` | Adds `total_steps` for slab engine |
| `SignalWorkflowExecution` | `SignalWorkflowExecution` | Adds `signal_name_id` for fast routing |
| `QueryWorkflow` | `QueryWorkflow` | Adds `query_name_id` |
| `SignalWithStartWorkflowExecution` | `SignalWithStartWorkflowExecution` | Same atomic semantics |
| `CancelWorkflowExecution` | `CancelWorkflowExecution` | Same graceful cancellation |
| `TerminateWorkflowExecution` | `TerminateWorkflowExecution` | Same forceful termination |
| `DescribeWorkflowExecution` | `DescribeWorkflowExecution` | Same response structure |
| `ListWorkflowExecutions` | `ListWorkflowExecutions` | Adds status/type/time filters |
| `GetWorkflowExecutionHistory` | `GetWorkflowExecutionHistory` | Same pagination model |
| `PollWorkflowTaskQueue` | `PollWorkflowTaskQueue` | Returns `workflow_key` + `step_index` |
| `PollActivityTaskQueue` | `PollActivityTaskQueue` | Same semantics |
| `RespondWorkflowTaskCompleted` | `RespondWorkflowTaskCompleted` | Commands are identical |
| `RespondActivityTaskCompleted` | `RespondActivityTaskCompleted` | Adds `workflow_key` + `step_index` |
| `RespondActivityTaskFailed` | `RespondActivityTaskFailed` | Same failure reporting |
| `RegisterNamespace` | `RegisterNamespace` | Adds `max_concurrent_workflows` |
| `DescribeNamespace` | `DescribeNamespace` | Same |
| `ListNamespaces` | `ListNamespaces` | Same pagination |
| `UpdateNamespace` | `UpdateNamespace` | Same |
| `GetSystemInfo` | `GetSystemInfo` | Returns engine capabilities |
| Workflow Execution | Workflow Execution | Same `workflow_id` + `run_id` model |
| Activity | Activity | Same scheduling and retry semantics |
| Signal | Signal | Same named signal delivery |
| Query | Query | Same synchronous query model |
| Child Workflow | Child Workflow | Same parent close policy |
| Timer | Timer | BinaryHeap-based timer engine |
| Cron Schedule | Cron Schedule | Built-in `CronScheduler` |
| Retry Policy | Retry Policy | Same exponential backoff model |
| Search Attributes | Search Attributes | SQL-like visibility queries |
| Memo | Memo | Same key-value metadata |
| History Events | History Events | Same event-sourced model |
| Saga (manual) | `SagaOrchestrator` | Built-in saga with compensating steps |
| Batch Operations | `BatchExecutor` | Built-in batch workflow operations |
| Worker Versioning | `WorkerVersioning` + `WorkerVersioningV2` | Build ID-based versioning |
| History Compaction | `HistoryCompactor` | Multi-level event compaction |
| Workflow Reset | `WorkflowResetter` | Reset to a previous event |
| Multi-Region | `MultiRegionReplicator` | Active/standby with failover |
| Nexus Operations | `NexusManager` | Cross-service operations |
| Visibility SQL | `VisibilityQuery` | SQL-like query parser |
| Payload Codec | `CodecChain` | Composable encoding pipeline |
| Replay | `ReplayEngine` | Deterministic replay verification |

---

## Workflow Code Conversion

### Go SDK

**Temporal:**
```go
func (w *Workflows) OrderProcessing(ctx workflow.Context, input OrderInput) error {
    // Validate
    err := workflow.ExecuteActivity(ctx, w.ValidateOrder, input).Get(ctx, nil)
    if err != nil {
        return err
    }

    // Process payment
    var payment PaymentResult
    err = workflow.ExecuteActivity(ctx, w.ProcessPayment, input).Get(ctx, &payment)
    if err != nil {
        // Compensate
        workflow.ExecuteActivity(ctx, w.RefundOrder, input)
        return err
    }

    // Wait for shipping signal
    var shippingInfo ShippingInfo
    signalChan := workflow.GetSignalChannel(ctx, "shipping_update")
    signalChan.Receive(ctx, &shippingInfo)

    return workflow.ExecuteActivity(ctx, w.CompleteOrder, input, shippingInfo).Get(ctx, nil)
}
```

**VELOCITY-WorkFlow:**
```go
func (w *Workflows) OrderProcessing(ctx sdk.WorkflowContext, input OrderInput) error {
    // Validate
    err := ctx.ExecuteActivity("ValidateOrder", input).Get(nil)
    if err != nil {
        return err
    }

    // Process payment
    var payment PaymentResult
    err = ctx.ExecuteActivity("ProcessPayment", input).Get(&payment)
    if err != nil {
        ctx.ExecuteActivity("RefundOrder", input)
        return err
    }

    // Wait for shipping signal
    var shippingInfo ShippingInfo
    err = ctx.WaitForSignal("shipping_update", &shippingInfo)
    if err != nil {
        return err
    }

    return ctx.ExecuteActivity("CompleteOrder", input, shippingInfo).Get(nil)
}
```

### TypeScript SDK

**Temporal:**
```typescript
export async function orderProcessing(input: OrderInput): Promise<void> {
  await validateOrder(input);
  const payment = await processPayment(input);
  const shippingInfo = await waitForSignal<ShippingInfo>('shipping_update');
  await completeOrder(input, shippingInfo);
}
```

**VELOCITY-WorkFlow:**
```typescript
export async function orderProcessing(ctx: WorkflowContext, input: OrderInput): Promise<void> {
  await ctx.activity('ValidateOrder', input);
  const payment = await ctx.activity<PaymentResult>('ProcessPayment', input);
  const shippingInfo = await ctx.signal<ShippingInfo>('shipping_update');
  await ctx.activity('CompleteOrder', input, shippingInfo);
}
```

### Key Conversion Rules

| Temporal Pattern | VELOCITY Pattern |
|-----------------|-----------------|
| `workflow.ExecuteActivity(ctx, fn, args)` | `ctx.ExecuteActivity("ActivityName", args)` |
| `workflow.GetSignalChannel(ctx, name)` | `ctx.WaitForSignal(name, &result)` |
| `workflow.QueryWorkflow(ctx, name)` | `ctx.Query(name, args)` |
| `workflow.NewTimer(ctx, duration)` | `ctx.Sleep(duration)` |
| `workflow.NewChildWorkflow(ctx, opts)` | `ctx.StartChildWorkflow(name, opts, input)` |
| `workflow.SideEffect(ctx, fn)` | `ctx.SideEffect(fn)` |
| `workflow.Go(ctx, fn)` | `ctx.Spawn(fn)` |
| `workflow.GetInfo(ctx).WorkflowExecution.ID` | `ctx.WorkflowID()` |
| `activity.GetInfo(ctx).Attempt` | `ctx.Attempt()` |
| `activity.RecordHeartbeat(ctx, details)` | `ctx.Heartbeat(details)` |

---

## Using the AST Transpiler

VELOCITY-WorkFlow includes a `temporal2velocity` transpiler tool that automates the conversion of Temporal workflow code.

### Location

```
tools/temporal2velocity/
├── Program.cs              # CLI entry point
├── TranspilerEngine.cs     # Core AST transformation engine
├── AstTranspilerEngine.cs  # Language-specific AST transpiler
└── temporal2velocity.csproj
```

### Supported Languages

| Source Language | Target Language | Status |
|----------------|----------------|--------|
| Go (Temporal SDK) | Go (Velocity SDK) | Supported |
| TypeScript (Temporal SDK) | TypeScript (Velocity SDK) | Supported |
| Java (Temporal SDK) | Java (Velocity SDK) | Planned |
| Python (Temporal SDK) | Python (Velocity SDK) | Planned |

### Usage

```bash
# Transpile a Go workflow file
dotnet run --project tools/temporal2velocity -- \
  --source ./workflows/order.go \
  --language go \
  --output ./workflows/order_velocity.go

# Transpile a TypeScript workflow file
dotnet run --project tools/temporal2velocity -- \
  --source ./workflows/order.ts \
  --language typescript \
  --output ./workflows/order_velocity.ts

# Transpile an entire directory
dotnet run --project tools/temporal2velocity -- \
  --source ./workflows/ \
  --language go \
  --output ./workflows_velocity/ \
  --recursive
```

### What the Transpiler Does

1. **Parses** the source file into an AST
2. **Identifies** Temporal SDK imports and API calls
3. **Rewrites** imports from `go.temporal.io/sdk` to `go.velocity.dev/sdk`
4. **Converts** API calls (e.g., `workflow.ExecuteActivity` → `ctx.ExecuteActivity`)
5. **Updates** function signatures (adds `ctx` parameter where needed)
6. **Preserves** business logic unchanged
7. **Outputs** the converted file with velocity-compatible code

### Transpiler Limitations

- Custom interceptors need manual migration
- Complex `workflow.Context` propagation patterns may require review
- Test files using `testsuite` need conversion to velocity test utilities
- Custom payload converters must be reimplemented using `CodecChain`

---

## Data Migration Strategies

### Strategy 1: Dual-Write (Zero Downtime)

Run both Temporal and VELOCITY in parallel during migration:

1. Deploy VELOCITY alongside Temporal
2. Start new workflows in VELOCITY
3. Let existing Temporal workflows complete naturally
4. Monitor both systems until Temporal is drained

```
                    ┌──────────────┐
  New Workflows ──► │ VELOCITY     │
                    └──────────────┘
                    ┌──────────────┐
  Existing WFs  ──► │ Temporal     │ ──► Complete naturally
                    └──────────────┘
```

### Strategy 2: History Export

Export workflow history from Temporal and import into VELOCITY:

1. Export completed workflow histories from Temporal's database
2. Transform the event format to VELOCITY's `HistoryEvent` structure
3. Import via the `HistoryStore` API
4. Use `ReplayEngine` to verify replay consistency

### Strategy 3: Cold Start

For non-critical migrations, simply start fresh:

1. Stop Temporal workers
2. Deploy VELOCITY
3. Restart workflows from scratch
4. Accept loss of in-progress workflow state

### Database Migration

Temporal uses Cassandra, MySQL, or PostgreSQL. VELOCITY uses PostgreSQL with the `PostgresAdapter`:

```bash
# Export from Temporal's PostgreSQL
pg_dump -h temporal-db -U temporal temporal_db > temporal_export.sql

# Transform schema (Temporal tables → VELOCITY tables)
# VELOCITY schema is in velocity-workflow-engine/src/schema.sql

# Import into VELOCITY's PostgreSQL
psql -h velocity-db -U velocity velocity_db < velocity_schema.sql
```

---

## SDK Migration

### Go SDK

| Temporal Import | VELOCITY Import |
|----------------|-----------------|
| `go.temporal.io/sdk/client` | `go.velocity.dev/sdk/velocity_sdk` |
| `go.temporal.io/sdk/workflow` | `go.velocity.dev/sdk/velocity_sdk` |
| `go.temporal.io/sdk/activity` | `go.velocity.dev/sdk/velocity_sdk` |
| `go.temporal.io/sdk/testsuite` | `go.velocity.dev/sdk/testing` |
| `go.temporal.io/sdk/interceptor` | `go.velocity.dev/sdk/interceptors` |
| `go.temporal.io/sdk/temporal` (errors) | `go.velocity.dev/sdk/errors` |

### TypeScript SDK

| Temporal Import | VELOCITY Import |
|----------------|-----------------|
| `@temporalio/client` | `@velocity-sdk/client` |
| `@temporalio/worker` | `@velocity-sdk/worker` |
| `@temporalio/workflow` | `@velocity-sdk/workflow` |
| `@temporalio/activity` | `@velocity-sdk/activity` |
| `@temporalio/testing` | `@velocity-sdk/testing` |

### Python SDK

| Temporal Import | VELOCITY Import |
|----------------|-----------------|
| `temporalio.client` | `velocity_sdk.client` |
| `temporalio.worker` | `velocity_sdk.worker` |
| `temporalio.workflow` | `velocity_sdk.workflow` |
| `temporalio.activity` | `velocity_sdk.activity` |

---

## Common Pitfalls and Workarounds

### 1. Workflow ID Reuse

**Issue:** Temporal allows reusing workflow IDs after retention period. VELOCITY uses a slab allocator where workflow keys are unique `u64` values.

**Workaround:** Use unique workflow IDs or call `TerminateWorkflowExecution` before reusing an ID.

### 2. Large History Events

**Issue:** Workflows with very long histories may experience slower replay.

**Workaround:** Enable `HistoryCompactor` to compact old events. Use `ContinueAsNew` for long-running workflows to reset history.

### 3. Activity Heartbeat Throttling

**Issue:** VELOCITY's `HeartbeatTracker` batches heartbeats differently than Temporal.

**Workaround:** Configure heartbeat intervals explicitly in the activity options. The `HeartbeatTracker` aggregates heartbeats to reduce WAL writes.

### 4. Search Attribute Types

**Issue:** Temporal supports custom search attribute types. VELOCITY supports: `string`, `integer`, `double`, `bool`, `datetime`, `keyword`.

**Workaround:** Map unsupported types to the closest VELOCITY type. Use `keyword` for exact-match strings.

### 5. Query Timeout

**Issue:** Temporal queries have a default timeout. VELOCITY queries are synchronous and block until the workflow responds.

**Workaround:** Implement query timeouts in the SDK client. The gRPC deadline propagates as a cancellation.

### 6. Namespace Isolation

**Issue:** Temporal namespaces are cluster-wide. VELOCITY namespaces have per-namespace rate limits and concurrency limits.

**Workaround:** Configure `max_concurrent_workflows` per namespace via `RegisterNamespace`. Set appropriate rate limits via `RateLimiter`.

### 7. Deterministic Replay

**Issue:** VELOCITY's `ReplayEngine` verifies deterministic execution. Non-deterministic workflows (using `time.Now()`, random numbers) will fail replay.

**Workaround:** Use `ctx.Now()` instead of `time.Now()`. Use `ctx.Random()` for random numbers. The engine provides deterministic primitives.

---

## Performance Comparison Expectations

| Metric | Temporal | VELOCITY-WorkFlow | Improvement |
|--------|----------|-------------------|-------------|
| Workflow start latency | 2-5 ms | <1 ms | 2-5x |
| Signal delivery | 1-3 ms | <0.5 ms | 2-6x |
| Task dispatch | 5-15 ms | <2 ms | 3-7x |
| Memory per workflow | ~2 KB (Go) | ~256 bytes (slab) | 8x |
| GC pause impact | 1-10 ms | 0 ms | Eliminated |
| Query response | 2-5 ms | <1 ms | 2-5x |
| History replay | Full scan | O(1) slab resumption | Orders of magnitude |
| Throughput (single node) | ~10K wf/s | ~100K+ wf/s | 10x+ |

*Numbers are approximate and depend on workload characteristics.*

---

## Migration Checklist

- [ ] Inventory all Temporal workflows and activities
- [ ] Run the AST transpiler on workflow source files
- [ ] Review transpiler output for manual fixes
- [ ] Set up VELOCITY infrastructure (PostgreSQL, engine nodes)
- [ ] Configure namespaces with matching rate limits
- [ ] Migrate worker processes to use VELOCITY SDKs
- [ ] Run integration tests against VELOCITY
- [ ] Verify replay consistency with `ReplayEngine`
- [ ] Set up monitoring (Prometheus + Grafana)
- [ ] Plan data migration strategy (dual-write, export, or cold start)
- [ ] Execute migration during low-traffic window
- [ ] Monitor both systems during transition
- [ ] Decommission Temporal after all workflows complete
- [ ] Update documentation and runbooks
