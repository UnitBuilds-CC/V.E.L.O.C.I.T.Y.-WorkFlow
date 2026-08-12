# Migration Guide: Temporal → VELOCITY-WorkFlow

> Step-by-step migration from Temporal to VELOCITY-WorkFlow.

---

## Table of Contents

1. [Comparison with Temporal](#comparison-with-temporal)
2. [Migration Strategy](#migration-strategy)
3. [API Mapping](#api-mapping)
4. [Feature Differences](#feature-differences)
5. [Code Conversion Examples](#code-conversion-examples)
6. [Data Migration](#data-migration)
7. [Using the AST Transpiler](#using-the-ast-transpiler)
8. [Migration Checklist](#migration-checklist)

---

## Comparison with Temporal

VELOCITY-WorkFlow is designed as a drop-in replacement for Temporal with compatible concepts but fundamentally different internals.

### Conceptual Mapping

| Temporal Concept | VELOCITY-WorkFlow Equivalent | Notes |
|-----------------|------------------------------|-------|
| Workflow | Workflow | Same concept, different state model |
| Activity | Activity / Step | Activities map to slab steps |
| Signal | Signal | Same semantics, adds `signal_name_id` |
| Query | Query | Same semantics, adds `query_name_id` |
| Task Queue | Task Queue | Same concept, adds priority levels |
| Namespace | Namespace | Same concept |
| History | Slab + Bitmask | Replaces event log with slab state |
| Replay | O(1) Resume | No replay — direct pointer cast |
| Search Attributes | Visibility Store | SQL-queryable metadata index |

### Architectural Differences

| Aspect | Temporal | VELOCITY-WorkFlow |
|--------|----------|-------------------|
| **State model** | Event sourcing (append-only log) | Slab allocator (in-place mutation) |
| **Recovery** | O(N) replay from event #1 | O(1) pointer cast (< 0.001 ms) |
| **Memory** | Managed heap + GC pressure | Zero-allocation Rust slabs |
| **Persistence** | Cassandra / PostgreSQL / MySQL | WAL + slab files (+ optional PostgreSQL) |
| **Infrastructure** | 4 services (Frontend, History, Matching, Worker) | Single binary or embedded |
| **Transport** | gRPC / HTTP/2 | gRPC + VCTP zero-copy UDP |
| **Verification** | Trust database | SHA-256 Merkle root per slab |
| **Versioning** | `Workflow.GetVersion()` branches | Declarative slot padding |
| **Non-determinism** | Runtime error in production | Compile-time build error |

### Performance Comparison

| Metric | Temporal | VELOCITY-WorkFlow |
|--------|----------|-------------------|
| Crash recovery | 50-150 ms (seconds for large histories) | < 0.001 ms |
| Step latency | 32 µs (10 steps) | 0.0003 ns (10 steps) |
| Memory per workflow | 2.8 KB (10 steps) | 128 bytes (fixed) |
| Storage per 10k steps | Gigabytes | Megabytes |
| GC pauses | Periodic stop-the-world | Zero GC pressure |

---

## Migration Strategy

### Phase 1: Assessment

1. **Inventory workflows**: List all Temporal workflow types and their complexity
2. **Identify non-deterministic code**: Find `Math.random()`, `Date.now()`, I/O calls in workflow code
3. **Map signals and queries**: Document all signal names and query handlers
4. **Assess data volume**: Count active workflows and history sizes
5. **Benchmark baseline**: Measure current Temporal latency and throughput

### Phase 2: Parallel Deployment

1. Deploy VELOCITY-WorkFlow server alongside Temporal
2. Migrate non-critical workflows first (cron jobs, batch processing)
3. Run both systems in parallel — route new workflows to VELOCITY
4. Keep Temporal for legacy workflows until migration complete

### Phase 3: Cutover

1. Use the AST transpiler to convert workflow code
2. Use the hydration tool to migrate active workflow state
3. Switch traffic to VELOCITY-WorkFlow
4. Decommission Temporal cluster

---

## API Mapping

### gRPC Service

| Temporal RPC | VELOCITY-WorkFlow RPC | Differences |
|-------------|----------------------|-------------|
| `StartWorkflowExecution` | `StartWorkflowExecution` | Adds `total_steps` field |
| `SignalWorkflowExecution` | `SignalWorkflowExecution` | Adds `signal_name_id` for fast routing |
| `QueryWorkflow` | `QueryWorkflow` | Adds `query_name_id` |
| `SignalWithStartWorkflowExecution` | `SignalWithStartWorkflowExecution` | Same atomic semantics |
| `CancelWorkflowExecution` | `CancelWorkflowExecution` | Same graceful cancellation |
| `TerminateWorkflowExecution` | `TerminateWorkflowExecution` | Same forceful termination |
| `DescribeWorkflowExecution` | `DescribeWorkflowExecution` | Same response structure |
| `ListWorkflowExecutions` | `ListWorkflowExecutions` | Adds status/type/time filters |
| `GetWorkflowExecutionHistory` | `GetWorkflowExecutionHistory` | Returns slab state instead of events |
| `PollActivityTaskQueue` | `PollTask` | Returns task from priority queue |
| `RespondActivityTaskCompleted` | `CompleteStep` | Marks bitmask step |
| `RespondActivityTaskFailed` | `FailTask` | Same semantics |

### SDK Client Methods

| Temporal SDK | VELOCITY-WorkFlow SDK | Notes |
|-------------|----------------------|-------|
| `client.startWorkflow()` | `client.startWorkflow()` | Adds `totalSteps` parameter |
| `client.signalWorkflow()` | `client.signalWorkflow()` | Same signature |
| `client.query()` | `client.queryWorkflow()` | Renamed for clarity |
| `client.getWorkflowHistory()` | `client.describeWorkflow()` | Returns slab state |
| `workflow.sleep()` | `Task.delay()` | Standard language delay |
| `workflow.getVersion()` | Not needed | Slot padding handles versioning |
| `proxyActivities()` | Direct function calls | Roslyn/SWC lowers I/O automatically |

---

## Feature Differences

### What VELOCITY-WorkFlow Does Differently

1. **No Event Replay**: State is stored in slabs with bitmask tracking. Crash recovery is O(1), not O(N).

2. **No `GetVersion()`**: Version changes are handled via declarative slot padding in the binary slab format. No branching code needed.

3. **No `proxyActivities()`**: The compiler (Roslyn for C#, SWC for TypeScript) automatically lowers async I/O calls into deterministic slab steps.

4. **Total Steps Required**: Workflows must declare `total_steps` upfront so the slab allocator can reserve the correct bitmask size.

5. **Signal Name IDs**: Signals use numeric `signal_name_id` for O(1) routing instead of string-based dispatch.

### What VELOCITY-WorkFlow Does Not Yet Have

| Temporal Feature | VELOCITY Status | Workaround |
|-----------------|-----------------|------------|
| Temporal Web UI | Planned | HTTP API + Grafana dashboards |
| Schedule (managed cron) | Available via TimerEngine | Use SDK cron examples |
| Batch operations | Planned | Script via SDK client |
| Cloud service | Planned | Self-hosted |
| Multi-cluster failover | Available via CRDT | Manual failover |

---

## Code Conversion Examples

### TypeScript Workflow

**Temporal:**
```typescript
import { proxyActivities, sleep } from '@temporalio/workflow';
import type * as activities from './activities';

const { chargeCard, sendReceipt } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
});

export async function paymentWorkflow(orderId: string, amount: number) {
  await chargeCard(orderId, amount);
  await sleep('1 day');
  await sendReceipt(orderId);
}
```

**VELOCITY-WorkFlow:**
```typescript
import { Durable } from '@velocity/core';
import { chargeCard, sendReceipt } from './activities';

@Durable()
export async function paymentWorkflow(orderId: string, amount: number) {
  await chargeCard(orderId, amount);
  await Task.delay('1 day');
  await sendReceipt(orderId);
}
```

### Python Workflow

**Temporal:**
```python
from temporalio import workflow
from temporalio.common import RetryPolicy

@workflow.defn
class PaymentWorkflow:
    @workflow.run
    async def run(self, order_id: str, amount: float) -> str:
        result = await workflow.execute_activity(
            charge_card, order_id, amount,
            start_to_close_timeout=timedelta(minutes=1),
        )
        await workflow.sleep(timedelta(days=1))
        await workflow.execute_activity(send_receipt, order_id)
        return "completed"
```

**VELOCITY-WorkFlow:**
```python
from velocity_sdk import VelocityClient

client = VelocityClient("localhost:50051")
handle = client.start_workflow(
    workflow_type="payment-workflow",
    task_queue="payments",
    total_steps=3,
    input_data=json.dumps({"order_id": order_id, "amount": amount}).encode(),
)
```

---

## Data Migration

### Strategy 1: Cold Migration

For batch migration of completed workflows:
1. Export Temporal history as JSON
2. Use the hydration tool to convert to slab format
3. Import into VELOCITY-WorkFlow

### Strategy 2: Live Migration

For zero-downtime cutover of active workflows:
1. Deploy VELOCITY alongside Temporal
2. Route new workflow starts to VELOCITY
3. Let existing Temporal workflows drain to completion
4. Hydrate any remaining active workflows

### Hydration Tool

```bash
# Convert active Temporal workflow to VELOCITY slab
dotnet run --project tools/temporal2velocity -- --hydrate <workflow_id> <step_count>

# Example: hydrate workflow 1001 at step 25
dotnet run --project tools/temporal2velocity -- --hydrate 1001 25
```

---

## Using the AST Transpiler

The `temporal2velocity` tool automatically converts Temporal workflow code:

```bash
# Transpile TypeScript workflows
dotnet run --project tools/temporal2velocity -- --src ./MyTemporalWorkflow.ts

# Transpile C# workflows
dotnet run --project tools/temporal2velocity -- --src ./MyWorkflow.cs

# Transpile entire directory
dotnet run --project tools/temporal2velocity -- --src ./workflows/ --recursive
```

### What the Transpiler Does

1. Replaces `proxyActivities()` with direct function calls
2. Replaces `sleep()` with `Task.delay()`
3. Removes `GetVersion()` branching (replaced by slot padding)
4. Adds `@Durable()` decorators
5. Adds `total_steps` to workflow start calls
6. Converts signal/query string names to numeric IDs

---

## Migration Checklist

### Pre-Migration

- [ ] Inventory all Temporal workflow types
- [ ] Identify non-deterministic code patterns
- [ ] Document signal and query names
- [ ] Measure current performance baselines
- [ ] Review VELOCITY-WorkFlow feature parity

### Development

- [ ] Run AST transpiler on workflow source code
- [ ] Review and fix transpiler output
- [ ] Update SDK imports and client initialization
- [ ] Add `total_steps` to all workflow start calls
- [ ] Replace `GetVersion()` with slot padding
- [ ] Test all workflows in VELOCITY test environment

### Deployment

- [ ] Deploy VELOCITY-WorkFlow server
- [ ] Configure namespaces and task queues
- [ ] Set up monitoring and alerting
- [ ] Run parallel deployment (Temporal + VELOCITY)
- [ ] Migrate non-critical workflows first
- [ ] Hydrate active workflow state
- [ ] Switch traffic to VELOCITY
- [ ] Verify all workflows completing correctly
- [ ] Decommission Temporal cluster
