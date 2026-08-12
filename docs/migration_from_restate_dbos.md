# Migration Guide: Restate & DBOS → VELOCITY-WorkFlow

> Step-by-step migration from Restate (HTTP) and DBOS (PostgreSQL) to VELOCITY-WorkFlow.

---

## Table of Contents

1. [Overview](#overview)
2. [Migrating from Restate](#migrating-from-restate)
3. [Migrating from DBOS](#migrating-from-dbos)
4. [API Comparison](#api-comparison)
5. [Code Conversion Examples](#code-conversion-examples)
6. [Data Migration](#data-migration)
7. [Migration Checklist](#migration-checklist)

---

## Overview

VELOCITY-WorkFlow provides two flavors that directly replace Restate and DBOS:

| Legacy Engine | VELOCITY Flavor | Protocol | Key Difference |
|---------------|-----------------|----------|----------------|
| **Restate** | Velocity Runtime | HTTP/1.1 JSON | Same REST API, slab-based state instead of event sourcing |
| **DBOS** | Velocity Embedded | HTTP + PostgreSQL | Same Postgres backend, zero-event-replay recovery |

### Why Migrate?

| Feature | Restate | DBOS | VELOCITY-WorkFlow |
|---------|---------|------|-------------------|
| Crash recovery | Event replay | Transaction replay | O(1) pointer cast (< 0.001 ms) |
| Memory model | Managed heap | JVM/managed | Zero-allocation slabs (128 bytes/workflow) |
| Encryption | TLS only | TLS only | AES-256-GCM at rest + key rotation + TLS |
| Infrastructure | Single server | Postgres required | Single binary (no deps) or Postgres |
| Observability | Basic logs | Basic logs | Prometheus metrics, deep health, X-Request-Id |
| gRPC support | No | No | Full 33-RPC BenchmarkService |
| State verification | Trust DB | Trust DB | SHA-256 Merkle root per slab |

---

## Migrating from Restate

### Step 1: Understand the API Mapping

Velocity Runtime is API-compatible with Restate's HTTP workflow API:

| Restate Endpoint | VELOCITY Endpoint | Notes |
|-----------------|-------------------|-------|
| `POST /restate/workflow/{name}` | `POST /api/v1/namespaces/{ns}/workflows` | Adds namespace |
| `GET /restate/workflow/{name}/{id}` | `GET /api/v1/namespaces/{ns}/workflows/{id}` | Same semantics |
| `POST /restate/workflow/{name}/{id}/send` | `POST /api/v1/namespaces/{ns}/workflows/{id}/signal` | Signal naming |
| `GET /health` | `GET /health` | Enhanced: includes version, uptime, counts |
| — | `GET /metrics` | Prometheus metrics (new) |

### Step 2: Update Your Client Code

**Restate SDK (TypeScript):**
```typescript
import { restate } from '@restatedev/restate-sdk';

const endpoint = restate.endpoint();

endpoint.addHandler('myWorkflow', async (ctx: Context, input: string) => {
  const result = await ctx.run(async () => {
    return await fetch('https://api.example.com/data').then(r => r.json());
  });
  
  await ctx.sleep(5000);
  
  await ctx.run(async () => {
    await sendNotification(result);
  });
  
  return result;
});

endpoint.listen(9080);
```

**VELOCITY-WorkFlow (TypeScript):**
```typescript
import { Client, Worker, WorkflowContext, WorkflowHelpers, ActivityContext } from 'velocity-sdk-typescript';

// Define activities (non-deterministic I/O)
async function fetchData(ctx: ActivityContext, input: string) {
  return await fetch('https://api.example.com/data').then(r => r.json());
}

async function sendNotification(ctx: ActivityContext, data: any) {
  console.log(`Sending notification: ${data}`);
}

// Define workflow (deterministic orchestration)
async function myWorkflow(ctx: WorkflowContext, input: string) {
  const result = await WorkflowHelpers.executeActivity({
    taskQueue: 'my-queue',
    activityType: 'fetchData',
    input: input,
  });
  
  await WorkflowHelpers.sleep(5000);
  
  await WorkflowHelpers.executeActivity({
    taskQueue: 'my-queue',
    activityType: 'sendNotification',
    input: result,
  });
  
  return result;
}

// Start worker
const worker = new Worker({
  taskQueue: 'my-queue',
  workflows: new Map([['myWorkflow', myWorkflow]]),
  activities: new Map([['fetchData', fetchData], ['sendNotification', sendNotification]]),
});
await worker.start();

// Start workflow via client
const client = new Client({ connection: { address: 'localhost:7233' } });
const result = await client.execute({
  workflowId: 'my-workflow-1',
  workflowType: 'myWorkflow',
  taskQueue: 'my-queue',
  input: 'hello',
});
```

### Step 3: Key Differences

| Aspect | Restate | VELOCITY Runtime |
|--------|---------|-----------------|
| **Context API** | `ctx.run()` for durable steps | `WorkflowHelpers.executeActivity()` |
| **Sleep** | `ctx.sleep(ms)` | `WorkflowHelpers.sleep(ms)` |
| **State** | Implicit via re-execution | Explicit slab bitmask |
| **Deployment** | Restate server + your code | Single binary (no external deps) |
| **Services** | Separate service processes | Workers poll task queues |
| **Journaling** | Automatic via Restate | WAL with vectorized fsync |

### Step 4: Deploy

```bash
# Start VELOCITY Runtime (drop-in Restate replacement)
cargo run --release -p velocity-dev-server -- --port 7233

# Your workflows run as workers connecting to this server
# No separate "service" deployment needed
```

---

## Migrating from DBOS

### Step 1: Understand the Architecture

DBOS uses PostgreSQL as its primary persistence layer. VELOCITY Embedded does the same, but replaces the event-sourcing replay with O(1) slab-based recovery.

| Aspect | DBOS | VELOCITY Embedded |
|--------|------|-------------------|
| **Persistence** | PostgreSQL | PostgreSQL (same) |
| **Recovery** | Transaction replay from PG | O(1) slab pointer cast |
| **API** | HTTP + direct PG access | HTTP + direct PG access (same) |
| **Workflow state** | Stored in PG tables | Stored in slab files + PG |
| **Encryption** | TLS to PG | AES-256-GCM at rest + TLS |
| **Metrics** | Basic | Prometheus /metrics |

### Step 2: Update Your Client Code

**DBOS (TypeScript):**
```typescript
import { DBOS, TransactionContext, WorkflowContext } from '@dbos-inc/dbos-sdk';

class MyWorkflow {
  @DBOS.transaction()
  static async fetchOrder(txn: TransactionContext, orderId: string) {
    const result = await txn.client.query('SELECT * FROM orders WHERE id = $1', [orderId]);
    return result.rows[0];
  }

  @DBOS.workflow()
  static async processOrder(ctx: WorkflowContext, orderId: string) {
    const order = await MyWorkflow.fetchOrder(orderId);
    
    // Durable sleep
    await DBOS.sleep(5000);
    
    // Charge payment
    const receipt = await MyWorkflow.chargePayment(order.amount);
    return receipt;
  }
}
```

**VELOCITY Embedded (Python):**
```python
from velocity import (
    Client, ClientOptions, Worker, WorkerOptions,
    WorkflowContext, ActivityContext,
    WorkflowOptions, register_workflow, register_activity,
)

@register_activity("fetch_order")
async def fetch_order(ctx: ActivityContext, input_data: dict) -> dict:
    """Fetch order from PostgreSQL."""
    order_id = input_data.get("order_id")
    # Direct Postgres access for hybrid durability
    import asyncpg
    conn = await asyncpg.connect(password="velocity")
    row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
    await conn.close()
    return dict(row)

@register_activity("charge_payment")
async def charge_payment(ctx: ActivityContext, input_data: dict) -> dict:
    """Charge payment."""
    amount = input_data.get("amount", 0)
    return {"transaction_id": f"txn-{ctx.attempt}", "amount": amount, "status": "charged"}

@register_workflow("process_order")
async def process_order(ctx: WorkflowContext, input_data: dict) -> dict:
    """Process an order."""
    order_id = input_data.get("order_id")
    # Workflow logic orchestrates activities
    return {"order_id": order_id, "status": "completed"}

# Start the VELOCITY Embedded server first:
# cargo run --release -p velocity-dev-server -- --embedded-mode --port 7233

# Start worker
worker = Worker(WorkerOptions(
    task_queue="orders",
    workflows={"process_order": process_order},
    activities={"fetch_order": fetch_order, "charge_payment": charge_payment},
))
await worker.start()
```

### Step 3: Start VELOCITY Embedded

```bash
# Start PostgreSQL (same as DBOS)
docker run -d --name velocity-pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=velocity \
  -e POSTGRES_DB=velocity \
  postgres:16

# Start VELOCITY in embedded mode
cargo run --release -p velocity-dev-server -- --embedded-mode --port 7233

# Access via HTTP API (same as DBOS)
curl -X POST http://localhost:7233/api/v1/namespaces/default/workflows \
  -H "Content-Type: application/json" \
  -d '{"workflow_type": "process_order", "task_queue": "orders", "input": {"order_id": "ORD-123"}}'

# Direct PostgreSQL access (same as DBOS)
psql -h localhost -U velocity -d velocity -c "SELECT * FROM workflows;"
```

### Step 4: Key Differences

| Aspect | DBOS | VELOCITY Embedded |
|--------|------|-------------------|
| **Decorators** | `@DBOS.workflow()`, `@DBOS.transaction()` | `@register_workflow()`, `@register_activity()` |
| **Transactions** | Inline via `@DBOS.transaction()` | Via activities (external I/O) |
| **Sleep** | `DBOS.sleep(timedelta)` | `WorkflowHelpers.sleep(ms)` |
| **Recovery** | Re-execute PG transactions | O(1) slab bitmask resume |
| **Monitoring** | Basic | Prometheus + deep health checks |

---

## API Comparison

### HTTP API (Velocity Runtime vs Restate)

| Operation | Restate | VELOCITY Runtime |
|-----------|---------|-----------------|
| Start workflow | `POST /restate/workflow/{type}` | `POST /api/v1/namespaces/{ns}/workflows` |
| Get status | `GET /restate/workflow/{type}/{id}` | `GET /api/v1/namespaces/{ns}/workflows/{id}` |
| Send signal | `POST /restate/workflow/{type}/{id}/send` | `POST /api/v1/namespaces/{ns}/workflows/{id}/signal` |
| List workflows | `GET /restate/workflows` | `GET /api/v1/namespaces/{ns}/workflows` |
| Health | `GET /health` | `GET /health` (enhanced JSON) |
| Metrics | — | `GET /metrics` (Prometheus) |

### HTTP API (Velocity Embedded vs DBOS)

| Operation | DBOS | VELOCITY Embedded |
|-----------|------|-------------------|
| Start workflow | `POST /workflows` | `POST /api/v1/namespaces/{ns}/workflows` |
| Get status | `GET /workflows/{id}` | `GET /api/v1/namespaces/{ns}/workflows/{id}` |
| Direct PG | Yes | Yes (same) |
| pgbench | No | Yes (raw Postgres TPS baseline) |

---

## Code Conversion Examples

### TypeScript: Restate → VELOCITY

**Restate:**
```typescript
import { restate, Context } from '@restatedev/restate-sdk';

const handler = restate.handler(async (ctx: Context, name: string) => {
  const greeting = await ctx.run(async () => {
    const resp = await fetch(`https://api.example.com/greet?name=${name}`);
    return resp.text();
  });
  
  await ctx.sleep(1000);
  
  await ctx.run(async () => {
    await sendEmail(greeting);
  });
  
  return greeting;
});
```

**VELOCITY:**
```typescript
import { Client, Worker, WorkflowContext, WorkflowHelpers, ActivityContext } from 'velocity-sdk-typescript';

async function fetchGreeting(ctx: ActivityContext, name: string) {
  const resp = await fetch(`https://api.example.com/greet?name=${name}`);
  return resp.text();
}

async function sendEmail(ctx: ActivityContext, greeting: string) {
  console.log(`Email: ${greeting}`);
}

async function greetingWorkflow(ctx: WorkflowContext, input: { name: string }) {
  const greeting = await WorkflowHelpers.executeActivity({
    taskQueue: 'greetings',
    activityType: 'fetchGreeting',
    input: input.name,
  });
  
  await WorkflowHelpers.sleep(1000);
  
  await WorkflowHelpers.executeActivity({
    taskQueue: 'greetings',
    activityType: 'sendEmail',
    input: greeting,
  });
  
  return greeting;
}
```

### Python: DBOS → VELOCITY

**DBOS:**
```python
from dbos import DBOS

dbos = DBOS()

@dbos.transaction()
def fetch_order(order_id: str):
    row = dbos.sql.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
    return row

@dbos.workflow()
def process_order(order_id: str):
    order = fetch_order(order_id)
    DBOS.sleep(5)
    receipt = charge_payment(order["amount"])
    return receipt
```

**VELOCITY:**
```python
from velocity import (
    Client, ClientOptions, Worker, WorkerOptions,
    WorkflowContext, ActivityContext,
    register_workflow, register_activity, WorkflowOptions,
)

@register_activity("fetch_order")
async def fetch_order(ctx: ActivityContext, input_data: dict) -> dict:
    import asyncpg
    conn = await asyncpg.connect(password="velocity")
    row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", input_data["order_id"])
    await conn.close()
    return dict(row)

@register_activity("charge_payment")
async def charge_payment(ctx: ActivityContext, input_data: dict) -> dict:
    return {"transaction_id": f"txn-{ctx.attempt}", "status": "charged"}

@register_workflow("process_order")
async def process_order(ctx: WorkflowContext, input_data: dict) -> dict:
    return {"order_id": input_data["order_id"], "status": "completed"}
```

---

## Data Migration

### From Restate

Restate stores workflow state internally. To migrate:

1. **Export from Restate**: Use Restate's admin API to list active workflows
2. **Map to VELOCITY**: Convert each workflow to VELOCITY's slab format
3. **Import to VELOCITY**: Use the VELOCITY HTTP API to recreate workflows

```bash
# List active Restate workflows
curl http://localhost:8080/restate/workflows | jq '.[]'

# Recreate in VELOCITY
curl -X POST http://localhost:7233/api/v1/namespaces/default/workflows \
  -H "Content-Type: application/json" \
  -d '{"workflow_type": "myWorkflow", "task_queue": "default", "input": {...}}'
```

### From DBOS

DBOS stores workflow state in PostgreSQL. VELOCITY Embedded can read the same database:

1. **Keep PostgreSQL running** — VELOCITY Embedded connects to the same PG instance
2. **Run migration queries** — Convert DBOS workflow tables to VELOCITY format
3. **Hydrate active workflows** — Use the hydration tool for in-flight workflows

```sql
-- DBOS workflow table
SELECT * FROM dbos.workflow_status WHERE status = 'PENDING';

-- Map to VELOCITY format
INSERT INTO velocity.workflows (workflow_type, task_queue, status, input_data)
SELECT workflow_name, 'default', 'RUNNING', input
FROM dbos.workflow_status
WHERE status = 'PENDING';
```

---

## Migration Checklist

### From Restate

- [ ] Inventory all Restate handlers and services
- [ ] Map `ctx.run()` calls to VELOCITY activities
- [ ] Map `ctx.sleep()` calls to `WorkflowHelpers.sleep()`
- [ ] Update client code to use VELOCITY HTTP API
- [ ] Deploy VELOCITY Runtime server
- [ ] Start workers with converted workflow code
- [ ] Test all workflows
- [ ] Switch traffic from Restate to VELOCITY
- [ ] Decommission Restate server

### From DBOS

- [ ] Inventory all `@DBOS.workflow()` and `@DBOS.transaction()` functions
- [ ] Convert `@DBOS.transaction()` to activities with direct PG access
- [ ] Convert `@DBOS.workflow()` to `@register_workflow()`
- [ ] Update client code to use VELOCITY HTTP API
- [ ] Deploy VELOCITY Embedded server (same PG instance)
- [ ] Migrate workflow data if needed
- [ ] Test all workflows
- [ ] Switch traffic from DBOS to VELOCITY
- [ ] Decommission DBOS runtime
