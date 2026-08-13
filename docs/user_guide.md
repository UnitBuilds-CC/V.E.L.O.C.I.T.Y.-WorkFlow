# VELOCITY-WorkFlow — Complete User Guide

> From zero to production-grade durable workflows. This guide covers everything you need to know.

---

## Table of Contents

1. [What is VELOCITY-WorkFlow?](#1-what-is-velocity-workflow)
2. [Core Concepts](#2-core-concepts)
3. [Installation and Setup](#3-installation-and-setup)
4. [Your First Workflow](#4-your-first-workflow)
5. [Working with SDKs](#5-working-with-sdks)
6. [Activity Design Patterns](#6-activity-design-patterns)
7. [Signals and Queries](#7-signals-and-queries)
8. [Child Workflows](#8-child-workflows)
9. [Timers, Sleep, and Cron Schedules](#9-timers-sleep-and-cron-schedules)
10. [Error Handling and Retry](#10-error-handling-and-retry)
11. [Saga Pattern (Distributed Transactions)](#11-saga-pattern-distributed-transactions)
12. [Search Attributes and Visibility](#12-search-attributes-and-visibility)
13. [Namespaces and Multi-Tenancy](#13-namespaces-and-multi-tenancy)
14. [Testing Workflows](#14-testing-workflows)
15. [Worker Versioning](#15-worker-versioning)
16. [Observability: Metrics, Logs, and Traces](#16-observability-metrics-logs-and-traces)
17. [Production Deployment](#17-production-deployment)
18. [Security Hardening](#18-security-hardening)
19. [Performance Tuning](#19-performance-tuning)
20. [Migration from Other Engines](#20-migration-from-other-engines)
21. [Troubleshooting and FAQ](#21-troubleshooting-and-faq)

---

## 1. What is VELOCITY-WorkFlow?

VELOCITY-WorkFlow is a **durable execution engine** — a system that guarantees your application code runs to completion, even if the server crashes, restarts, or loses network connectivity. It provides the same programming model as Temporal (workflows, activities, signals, queries, task queues) but replaces the event-sourcing replay architecture with an O(1) slab pointer-cast model built on `#![no_std]` Rust.

### Why VELOCITY?

| Aspect | Traditional Engines (Temporal, etc.) | VELOCITY-WorkFlow |
|--------|--------------------------------------|-------------------|
| **Crash recovery** | O(N) event replay from history | O(1) pointer cast (< 0.001 ms) |
| **Memory model** | Managed heap + GC pauses | Zero-allocation slab allocator |
| **State verification** | Trust database admin | SHA-256 Merkle root per slab |
| **Infrastructure** | 4+ services + external database | Single binary or embedded |
| **p99 latency** | 5–15 ms dispatch | Sub-4 ms on production hardware |
| **Memory footprint** | 50–200 MB | ~5 MB |

### Three Flavors

VELOCITY ships in three deployment flavors, each sharing the same core engine:

| Flavor | Protocol | Best For | Default Port |
|--------|----------|----------|:------------:|
| **Classic** | gRPC (HTTP/2) | Temporal migration; full-featured | 7234 |
| **Runtime** | HTTP/1.1 JSON | Lightweight; serverless; Restate migration | 7233 |
| **Embedded** | HTTP + PostgreSQL | DBOS migration; embedded durability | 7233 + 5432 |

---

## 2. Core Concepts

### Workflows

A **workflow** is a durable, long-running function that is automatically persisted and survives crashes. When you write a workflow, you write normal async code — the engine handles persistence, retry, and state tracking.

**Key rule:** Workflows must be **deterministic**. No direct I/O, no random numbers, no system time calls. Use activities for non-deterministic operations.

```python
# Good — deterministic workflow
async def order_workflow(ctx, order_id):
    result = await ctx.activity("validate_order", order_id)     # Activity (I/O)
    await ctx.activity("charge_payment", order_id, result)      # Activity (I/O)
    await ctx.sleep(3600)                                        # Engine sleep (deterministic)
    await ctx.activity("ship_order", order_id)                   # Activity (I/O)
```

### Activities

An **activity** is a unit of work that performs non-deterministic operations (HTTP calls, database queries, file I/O). Activities are automatically retried on failure according to a configurable retry policy.

```python
@activity_def("charge_payment")
async def charge_payment(ctx, order_id, amount):
    # This is NOT deterministic — it calls an external payment API
    response = await payment_api.charge(order_id, amount)
    return response.transaction_id
```

### Task Queues

A **task queue** is a named channel that workers poll for tasks. Workflows and activities are dispatched to specific task queues. Workers register themselves on one or more task queues.

```
Worker A ──polls──► Task Queue "orders"
Worker B ──polls──► Task Queue "payments"
Worker C ──polls──► Task Queue "notifications"
```

### Signals

**Signals** inject external events into a running workflow. They are fire-and-forget — the sender does not wait for a response.

```python
# From outside the workflow (client process)
client.signal_workflow(workflow_key, "payment_confirmed", b'{"amount": 99.99}')
```

### Queries

**Queries** read the current state of a running workflow without modifying it. They are synchronous and return immediately. Use the `WorkflowStub` to query:

```python
# Using WorkflowStub (recommended)
stub = WorkflowStub(client, WorkflowStubOptions(workflow_type="order_workflow"))
status = stub.query("get_order_status")
```

### Namespaces

A **namespace** provides isolation between workflow applications. Each namespace has its own task queues, rate limits, and retention policies.

### The Slab Model

Under the hood, each workflow is represented by a 128-byte **slab header** — a `repr(C)` struct with a SHA-256 Merkle root for cryptographic state verification. Step completion is tracked with a 256-bit bitmask, giving O(1) crash recovery regardless of workflow history size.

---

## 3. Installation and Setup

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| **Rust** | 1.82+ (stable) | Build the engine and servers |
| **protoc** | 25+ | Protocol Buffers compiler (for gRPC) |
| **Your language SDK** | See SDK table | Write workflows |

### Install the Dev Server

The dev server provides a complete in-memory engine with zero external dependencies:

```bash
# Clone the repository
git clone https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow.git
cd VELOCITY-WorkFlow

# Build the dev server
cargo build --release -p velocity-dev-server
```

### Start the Dev Server

```bash
# Start with defaults (HTTP on 7233, gRPC on 7234, UI on 8233)
cargo run --release -p velocity-dev-server

# Start with gRPC only (Classic flavor)
cargo run --release -p velocity-dev-server -- --grpc-port 7234

# Start with custom options
cargo run --release -p velocity-dev-server -- \
  --port 7233 \
  --grpc-port 7234 \
  --namespace my-app \
  --log-level debug
```

The server is ready when you see:
```
[VELOCITY] Dev Server starting...
[VELOCITY] HTTP API: http://127.0.0.1:7233
[VELOCITY] gRPC:     http://127.0.0.1:7234
[VELOCITY] Health:   http://127.0.0.1:7233/health
[VELOCITY] Ready to accept connections
```

### Verify It's Running

```bash
# Health check
curl http://localhost:7233/health

# Expected response:
# {"status":"healthy","version":"0.1.0","uptime_secs":5,...}
```

### Start the Production Server

For production use, the `velocity-workflow-server` binary provides WAL persistence, encryption, and slab file durability:

```bash
cargo run --release -p velocity-workflow-server -- \
  --ip 0.0.0.0 \
  --grpc-port 7234 \
  --wal-path /data/velocity.wal \
  --log-level info
```

### Install an SDK

| Language | Package | Install |
|----------|---------|---------|
| **TypeScript** | `@velocity-workflow/sdk` | `npm install @velocity-workflow/sdk` |
| **Python** | `velocity-workflow` | `pip install velocity-workflow` |
| **Go** | `github.com/velocity-workflow/sdk-go` | `go get github.com/velocity-workflow/sdk-go` |
| **Java** | `io.velocity:velocity-sdk-java` | Maven/Gradle dependency |
| **Rust** | `velocity-sdk` | `cargo add velocity-sdk` |
| **PHP** | `velocity/workflow-sdk` | `composer require velocity/workflow-sdk` |
| **Ruby** | `velocity_sdk` | `gem install velocity_sdk` |

---

## 4. Your First Workflow

### Step 1: Define the Workflow

**TypeScript:**
```typescript
import { WorkflowContext } from '@velocity-workflow/sdk';

export async function greetingWorkflow(ctx: WorkflowContext, name: string): Promise<string> {
  // Activities handle non-deterministic operations
  const greeting = await ctx.activity('generateGreeting', name);
  await ctx.activity('sendNotification', greeting);
  return greeting;
}
```

**Python:**
```python
from velocity_sdk import WorkflowContext, register_workflow

@register_workflow("greeting_workflow")
async def greeting_workflow(ctx: WorkflowContext, name: str) -> str:
    greeting = await ctx.activity("generate_greeting", name)
    await ctx.activity("send_notification", greeting)
    return greeting
```

**Go:**
```go
func GreetingWorkflow(ctx velocity.WorkflowContext, name string) (string, error) {
    var greeting string
    err := ctx.ExecuteActivity("GenerateGreeting", name).Get(&greeting)
    if err != nil {
        return "", err
    }
    err = ctx.ExecuteActivity("SendNotification", greeting).Get(nil)
    return greeting, err
}
```

### Step 2: Define Activities

**Python:**
```python
from velocity_sdk import register_activity

@register_activity("generate_greeting")
async def generate_greeting(ctx, name: str) -> str:
    return f"Hello, {name}! Welcome to VELOCITY-WorkFlow."

@register_activity("send_notification")
async def send_notification(ctx, message: str) -> None:
    print(f"[notification] {message}")
```

### Step 3: Start a Worker

The worker polls the server for tasks and executes your workflow and activity code:

**Python:**
```python
from velocity_sdk import Worker, WorkerOptions

worker = Worker(WorkerOptions(
    task_queue="greetings",
    workflows=[greeting_workflow],
    activities=[generate_greeting, send_notification],
))

await worker.start()
print("Worker is polling for tasks...")
```

### Step 4: Start a Workflow Execution

**Python:**
```python
from velocity_sdk import VelocityClient, WorkflowStub, WorkflowStubOptions

client = VelocityClient("localhost:7234")

# Use WorkflowStub for typed workflow execution
stub = WorkflowStub(client, WorkflowStubOptions(
    workflow_type="greeting_workflow",
    task_queue="greetings",
))
handle = stub.start({"name": "World"})
result = stub.result()
print(f"Result: {result}")
# Output: Result: Hello, World! Welcome to VELOCITY-WorkFlow.
```

**TypeScript:**
```typescript
import { VelocityClient, WorkflowStub } from '@velocity-workflow/sdk';

const client = new VelocityClient('localhost:7234');
await client.connect();

const stub = new WorkflowStub(client, {
  workflowType: 'greetingWorkflow',
  taskQueue: 'greetings',
});
const handle = await stub.start({ name: 'World' });
const result = await stub.result();
console.log(`Result: ${result}`);
await client.close();
```

**Go:**
```go
client, err := velocity_sdk.NewClient("localhost:7234", "")
if err != nil {
    log.Fatal(err)
}
defer client.Close()

handle, err := client.StartWorkflow(ctx, &velocity_sdk.StartWorkflowOptions{
    WorkflowType: "GreetingWorkflow",
    TaskQueue:    "greetings",
    Input:        []byte(`{"name": "World"}`),
})
```

---

## 5. Working with SDKs

All seven SDKs follow the same architecture:

```
Your Code → SDK Client → gRPC (HTTP/2) → VELOCITY Server
```

### Client Connection

Create one client per process and reuse it across workflows:

```python
# Python
client = VelocityClient("localhost:7234")
```

```typescript
// TypeScript
const client = new VelocityClient('localhost:7234');
await client.connect();
```

```go
// Go
client, err := velocity_sdk.NewClient("localhost:7234", "")
```

### Workflow Lifecycle

Every SDK provides these core operations:

| Operation | Python SDK | TypeScript SDK | Go SDK |
|-----------|-----------|----------------|--------|
| Start workflow | `client.start_workflow(...)` | `client.startWorkflow(...)` | `client.StartWorkflow(...)` |
| Signal workflow | `client.signal_workflow(...)` | `client.signalWorkflow(...)` | `client.SignalWorkflow(...)` |
| Cancel workflow | `client.cancel_workflow(...)` | `client.cancelWorkflow(...)` | `client.CancelWorkflow(...)` |
| Terminate workflow | `client.terminate_workflow(...)` | `client.terminateWorkflow(...)` | `client.TerminateWorkflow(...)` |
| Describe workflow | `client.describe_workflow(...)` | `client.describeWorkflow(...)` | `client.DescribeWorkflow(...)` |
| List workflows | `client.list_workflows(...)` | `client.listWorkflows(...)` | `client.ListWorkflows(...)` |

For queries, use the `WorkflowStub` (Python/TypeScript) which provides `stub.query(...)`.

### Worker Registration

Workers must register workflows and activities before starting:

```python
worker = Worker(WorkerOptions(
    task_queue="orders",
    workflows=[order_workflow, payment_workflow],
    activities=[validate_order, charge_payment, send_receipt],
))
await worker.start()
```

---

## 6. Activity Design Patterns

### Simple Activity

```python
@register_activity("lookup_user")
async def lookup_user(ctx, user_id: str) -> dict:
    return await db.users.find_one(user_id)
```

### Activity with Retry Policy

```python
@register_activity("process_payment")
async def process_payment(ctx, order_id: str, amount: float) -> str:
    retry_policy = RetryPolicy(
        initial_interval=1.0,
        backoff_coefficient=2.0,
        max_interval=30.0,
        max_attempts=5,
        non_retryable_errors=["InvalidCardError"],
    )
    return await payment_gateway.charge(order_id, amount, retry_policy=retry_policy)
```

### Activity Heartbeating

For long-running activities, record heartbeats so the engine knows the activity is still alive:

```python
@register_activity("process_large_file")
async def process_large_file(ctx, file_path: str) -> str:
    chunks = read_file_chunks(file_path)
    for i, chunk in enumerate(chunks):
        process_chunk(chunk)
        ctx.heartbeat(progress=i / len(chunks))  # Reports progress
    return "completed"
```

If the activity worker crashes, the engine detects the missed heartbeat and reschedules the activity from the last heartbeat point.

### Side Effects (Non-Deterministic Values)

Use `side_effect` to capture non-deterministic values in a deterministic way:

```python
async def my_workflow(ctx):
    # This is deterministic — the UUID is captured once and replayed consistently
    request_id = await ctx.side_effect(lambda: str(uuid.uuid4()))
    await ctx.activity("process_request", request_id)
```

---

## 7. Signals and Queries

### Sending Signals

Signals inject external events into running workflows:

```python
# From a client process (payload must be bytes)
client.signal_workflow(workflow_key, "payment_confirmed", b'{"amount": 99.99}')
```

### Receiving Signals in a Workflow

```python
async def order_workflow(ctx, order_id):
    await ctx.activity("validate_order", order_id)

    # Wait for an external signal
    payment = await ctx.wait_for_signal("payment_confirmed")
    amount = payment["amount"]

    await ctx.activity("ship_order", order_id, amount)
```

### Signal with Start

Atomically start a workflow or signal it if already running:

```python
handle = client.signal_with_start(
    workflow_type="cart_workflow",
    signal_name="item_added",
    signal_payload=b'{"item": "SKU-456"}',
    task_queue="orders",
)
```

### Queries

Queries read workflow state without side effects. Use the `WorkflowStub` for typed queries:

```python
from velocity_sdk import VelocityClient, WorkflowStub, WorkflowStubOptions

client = VelocityClient("localhost:7234")
stub = WorkflowStub(client, WorkflowStubOptions(workflow_type="order_workflow"))

# Start the workflow first, then query
handle = stub.start({"order_id": "order-123"})
status = stub.query("get_status")
print(f"Current status: {status}")
```

In TypeScript:
```typescript
const stub = new WorkflowStub(client, { workflowType: 'order_workflow' });
await stub.start({ orderId: 'order-123' });
const status = await stub.query('get_status');
```

---

## 8. Child Workflows

Parent workflows can start child workflows for orchestration:

```python
async def batch_order_workflow(ctx, order_ids):
    # Start a child workflow for each order
    child_handles = []
    for order_id in order_ids:
        handle = await ctx.start_child_workflow(
            "process_single_order",
            args=[order_id],
            task_queue="orders",
        )
        child_handles.append(handle)

    # Wait for all children to complete
    results = await asyncio.gather(*[h.result() for h in child_handles])
    return results
```

### Parent Close Policy

Control what happens to child workflows when the parent completes:

| Policy | Behavior |
|--------|----------|
| `TERMINATE` | Child workflows are forcefully terminated (default) |
| `CANCEL` | Child workflows receive a cancellation request |
| `ABANDON` | Child workflows continue running independently |

```python
handle = await ctx.start_child_workflow(
    "process_order",
    args=[order_id],
    parent_close_policy="ABANDON",  # Child continues even if parent finishes
)
```

---

## 9. Timers, Sleep, and Cron Schedules

### Engine Sleep

Always use the engine's sleep — never `time.sleep()` or `setTimeout()`:

```python
async def reminder_workflow(ctx, user_id):
    await ctx.activity("send_initial_reminder", user_id)
    await ctx.sleep(86400)  # Wait 24 hours (deterministic)
    await ctx.activity("send_followup_reminder", user_id)
```

### Cron Schedules

Start a workflow that runs on a recurring schedule:

```python
handle = client.start_workflow(
    workflow_type="nightly_cleanup",
    task_queue="maintenance",
    cron_schedule="0 2 * * *",  # Every day at 2:00 AM
)
```

Each cron execution creates a new run with its own history. Previous run results are accessible to the next execution.

---

## 10. Error Handling and Retry

### Activity Retry Policy

Activities are automatically retried on failure:

```python
retry_policy = RetryPolicy(
    initial_interval=1.0,        # First retry after 1 second
    backoff_coefficient=2.0,     # Double the interval each retry
    max_interval=60.0,           # Cap at 60 seconds
    max_attempts=10,             # Give up after 10 attempts (0 = unlimited)
    non_retryable_errors=[       # Don't retry these error types
        "InvalidInputError",
        "PermissionDeniedError",
    ],
)
```

### Workflow Error Handling

Handle activity failures in the workflow:

```python
async def payment_workflow(ctx, order_id):
    try:
        result = await ctx.activity("charge_payment", order_id)
    except ActivityError as e:
        if e.type == "InsufficientFundsError":
            await ctx.activity("notify_user", order_id, "Payment failed: insufficient funds")
            return {"status": "failed", "reason": "insufficient_funds"}
        raise  # Re-raise unexpected errors
```

### Workflow Failure

If all retries are exhausted, the workflow fails:

```python
async def robust_workflow(ctx):
    try:
        await ctx.activity("risky_operation")
    except ActivityError:
        await ctx.activity("compensating_action")
        raise WorkflowFailure("risky_operation failed after all retries")
```

---

## 11. Saga Pattern (Distributed Transactions)

VELOCITY includes a built-in `SagaOrchestrator` for distributed transactions with compensating actions:

```python
async def travel_booking_workflow(ctx, trip_details):
    saga = ctx.create_saga()

    try:
        # Step 1: Book flight
        flight = await ctx.activity("book_flight", trip_details)
        saga.add_compensation("cancel_flight", flight.booking_id)

        # Step 2: Book hotel
        hotel = await ctx.activity("book_hotel", trip_details)
        saga.add_compensation("cancel_hotel", hotel.booking_id)

        # Step 3: Book car rental
        car = await ctx.activity("book_car", trip_details)
        saga.add_compensation("cancel_car", car.booking_id)

        return {"flight": flight, "hotel": hotel, "car": car}

    except Exception as e:
        # Automatically runs all compensations in reverse order
        await saga.compensate()
        raise
```

If any step fails, the saga automatically executes compensating actions in reverse order (cancel car → cancel hotel → cancel flight).

---

## 12. Search Attributes and Visibility

### Setting Search Attributes

Attach indexed metadata to workflows for visibility queries. Search attributes are set via the gRPC API when starting a workflow:

```python
# Using the low-level gRPC stub
from velocity_sdk import VelocityClient

client = VelocityClient("localhost:7234")
# Search attributes are passed in the StartWorkflowExecutionRequest
# via the gRPC proto (see api_reference.md for field details)
```

```bash
# Via grpcurl
grpcurl -plaintext localhost:7234 \
  velocity.v1.WorkflowService/StartWorkflowExecution \
  -d '{
    "namespace": "default",
    "workflow_type": {"name": "order_workflow"},
    "task_queue": {"name": "orders"},
    "search_attributes": {
      "indexed_fields": {
        "customer_id": {"string_value": "CUST-123"},
        "order_total": {"double_value": 299.99}
      }
    }
  }'
```

### Querying Workflows

Use the `search_workflows` method with SQL-like visibility queries:

```python
# List all workflows
workflows = client.list_workflows(page_size=100)

# SQL-like visibility queries (when PostgreSQL is configured)
workflows = client.search_workflows(
    'WorkflowType = "order_workflow" AND Namespace = "default"'
)
```

---

## 13. Namespaces and Multi-Tenancy

### Creating Namespaces

Namespaces are managed via the gRPC `NamespaceService` or the server CLI. Each namespace has independent task queues, rate limits, and retention policies:

```bash
# Via grpcurl
grpcurl -plaintext localhost:7234 \
  velocity.v1.WorkflowService/RegisterNamespace \
  -d '{"namespace": "production", "retention_days": 30}'
```

### Multi-Tenant Isolation

Use namespaces to isolate different tenants, environments, or applications:

```
Namespace "tenant-acme"    → rate limit: 500 ops/s, 10K concurrent
Namespace "tenant-globex"  → rate limit: 200 ops/s, 5K concurrent
Namespace "staging"        → rate limit: unlimited, 1K concurrent
```

---

## 14. Testing Workflows

### Unit Testing with Mock Client

Each SDK provides a test environment that simulates the server in-memory:

**Python:**
```python
from velocity_sdk.testing import WorkflowTestEnvironment, MockVelocityClient

async def test_greeting_workflow():
    env = WorkflowTestEnvironment()
    client = MockVelocityClient()

    # Start a workflow using the mock client
    handle = client.start_workflow(
        workflow_type="greeting_workflow",
        task_queue="test",
        total_steps=1,
    )

    # Verify the workflow was started
    desc = client.describe_workflow(handle.workflow_key)
    assert desc.status == WorkflowStatus.RUNNING
```

**TypeScript:**
```typescript
import { TestWorkflowEnvironment, MockVelocityClient } from '@velocity-workflow/sdk';

test('greeting workflow', async () => {
  const client = new MockVelocityClient();

  const handle = client.startWorkflow({
    workflowType: 'greetingWorkflow',
    taskQueue: 'test',
    totalSteps: 1,
  });

  const desc = await client.describeWorkflow(handle.workflowKey);
  expect(desc.status).toBe(WorkflowStatus.Running);
});
```

### Testing Signals

```python
async def test_signal_handling():
    client = MockVelocityClient()

    handle = client.start_workflow(
        workflow_type="waiting_workflow",
        task_queue="test",
    )

    # Send a signal (payload must be bytes)
    client.signal_workflow(handle.workflow_key, "approve", b'{"approved": true}')

    # Verify the signal was received
    desc = client.describe_workflow(handle.workflow_key)
    assert desc.status == WorkflowStatus.RUNNING
```

### Testing Timeouts and Failures

```python
async def test_activity_failure():
    client = MockVelocityClient()

    handle = client.start_workflow(
        workflow_type="retry_workflow",
        task_queue="test",
    )

    # Verify the workflow is running (mock doesn't execute activities)
    desc = client.describe_workflow(handle.workflow_key)
    assert desc.status == WorkflowStatus.RUNNING
    assert desc.total_steps >= 1
```

---

## 15. Worker Versioning

Use worker versioning to deploy new workflow code without disrupting running workflows:

```python
worker = Worker(WorkerOptions(
    task_queue="orders",
    workflows=[order_workflow_v2],
    activities=[validate_v2, charge_v2],
    build_id="v2.0.0",           # Unique build identifier
    use_versioning=True,
))
```

When you deploy a new version:
1. Old workflows continue on workers with the old `build_id`
2. New workflows are dispatched to workers with the latest `build_id`
3. No workflow is interrupted during deployment

---

## 16. Observability: Metrics, Logs, and Traces

### Prometheus Metrics

The server exports Prometheus metrics at `GET /metrics`:

```bash
curl http://localhost:7233/metrics
```

Key metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `velocity_workflows_started_total` | Counter | Total workflows started |
| `velocity_workflows_completed_total` | Counter | Total workflows completed |
| `velocity_workflows_failed_total` | Counter | Total workflows failed |
| `velocity_workflow_duration_seconds` | Histogram | Duration distribution |
| `velocity_task_queue_depth` | Gauge | Current queue depth |
| `velocity_active_workflows` | Gauge | Currently running |
| `velocity_signal_count_total` | Counter | Signals delivered |
| `velocity_query_count_total` | Counter | Queries served |
| `velocity_wal_records_written` | Counter | WAL records written |

### Structured Logging

Enable JSON structured logging:

```bash
cargo run --release -p velocity-dev-server -- --log-level debug
```

Log output:
```json
{
  "timestamp": "2026-08-13T12:00:00Z",
  "level": "INFO",
  "service": "velocity-workflow-engine",
  "message": "Workflow completed",
  "workflow_key": 42,
  "duration_ms": 1234
}
```

### Distributed Tracing (OpenTelemetry)

Enable OpenTelemetry tracing:

```bash
cargo run --release -p velocity-dev-server -- --otel --otel-endpoint http://jaeger:4317
```

Each workflow execution creates a root span, with child spans for activities, signals, and queries.

---

## 17. Production Deployment

### Docker

```bash
# Production server
docker build -f deploy/Dockerfile.production-server -t velocity-server .
docker run -p 7234:7234 -v /data:/data velocity-server

# Dev server
docker build -f deploy/Dockerfile.dev-server -t velocity-dev .
docker run -p 7233:7233 -p 7234:7234 velocity-dev
```

### Docker Compose (Full Stack)

```bash
# Starts: velocity-server, postgres, prometheus, grafana
docker compose up -d
```

### Kubernetes

```bash
# Helm
helm install velocity deploy/helm/velocity/ \
  --namespace velocity-system --create-namespace \
  --set server.replicas=3

# Kustomize
kubectl apply -k deploy/kustomize/overlays/production/
```

### Production Checklist

- [ ] Use `velocity-workflow-server` (not dev server)
- [ ] Configure WAL path on persistent storage
- [ ] Enable TLS for gRPC and HTTP
- [ ] Set up Prometheus monitoring and alerting
- [ ] Configure namespace rate limits
- [ ] Enable structured logging (JSON)
- [ ] Set up health checks (`/health` for liveness, `/ready` for readiness)
- [ ] Configure backup strategy for WAL and slab files
- [ ] Set resource limits (CPU, memory) in container orchestrator
- [ ] Run as non-root user

---

## 18. Security Hardening

### TLS

```bash
# Enable TLS on the server
cargo run --release -p velocity-dev-server -- \
  --tls-cert /certs/server.crt \
  --tls-key /certs/server.key
```

### Authentication

VELOCITY supports API key and JWT authentication:

```bash
# API Key
grpcurl -H "authorization: Bearer YOUR_API_KEY" localhost:7234 ...

# JWT Token
grpcurl -H "authorization: Bearer eyJhbG..." localhost:7234 ...
```

### Encryption at Rest

The engine uses AES-256-GCM with automatic key rotation for workflow state at rest. Key rotation is seamless — old keys are retained for decrypting existing data while new data uses the fresh key.

### Request Validation

- **Content-Type enforcement**: POST/PUT/PATCH to `/api/` requires `application/json` (returns 415 on mismatch)
- **Body size limit**: 10 MB max request body with Content-Length fast-path rejection
- **Request correlation**: X-Request-Id header propagation for tracing

---

## 19. Performance Tuning

### Server Tuning

| Setting | Default | Recommendation |
|---------|---------|----------------|
| WAL fsync | Every commit | For max throughput, batch fsync every 100ms |
| Shard count | 4 | Increase for multi-core servers |
| jemalloc | Enabled | Keep enabled (significant perf gain) |

### Client Tuning

- **Connection pooling**: Reuse one client per process
- **Batch operations**: Use `BatchExecutor` for bulk workflow starts
- **Task queue design**: Separate long-running and short-running tasks onto different queues

### Benchmarking

```bash
# Quick smoke test (3 workloads)
cargo run --release -p velocity-bench --bin velocity-bench -- \
  --workloads smoke --engine velocity \
  --velocity-address http://localhost:7234

# Full benchmark (18 workloads, ~15 min)
cargo run --release -p velocity-bench --bin velocity-bench -- \
  --workloads all --profile standard --engine velocity \
  --velocity-address http://localhost:7234 \
  --output bench_results.json
```

---

## 20. Migration from Other Engines

### From Temporal

VELOCITY provides a compatible gRPC API — many Temporal workflows can connect directly:

1. **API compatibility**: Same `WorkflowService` gRPC with 21+ RPCs
2. **AST transpiler**: `temporal2velocity` converts workflow source code automatically
3. **History hydration**: Migrate active workflows without downtime

```bash
# Transpile a Temporal workflow
dotnet run --project tools/temporal2velocity -- \
  --source ./my_temporal_workflow.ts \
  --language typescript \
  --output ./my_velocity_workflow.ts
```

See [Migration from Temporal](migration_from_temporal.md) for the complete guide.

### From Restate

The Runtime flavor provides HTTP/JSON compatibility:

```bash
# Start in Runtime mode
cargo run --release -p velocity-dev-server -- --port 7233
```

See [Migration from Restate & DBOS](migration_from_restate_dbos.md) for details.

### From DBOS

The Embedded flavor uses PostgreSQL for direct compatibility:

```bash
# Start in Embedded mode
cargo run --release -p velocity-dev-server -- --embedded-mode --port 7233
```

---

## 21. Troubleshooting and FAQ

### Common Issues

**Server won't start — port already in use:**
```bash
# Check what's using the port
lsof -i :7233  # Linux/macOS
netstat -ano | findstr :7233  # Windows

# Use a different port
cargo run --release -p velocity-dev-server -- --port 8080
```

**Worker cannot connect:**
1. Verify the server is running: `curl http://localhost:7233/health`
2. Check the gRPC port is correct (default: 7234)
3. Verify no firewall is blocking the port
4. If using Docker, ensure port mapping is correct

**Workflow stuck in Running state:**
- Verify a worker is polling the correct task queue
- Check that the workflow type name matches exactly
- Check `total_steps` matches actual workflow steps

**Signal not received:**
- Verify signal name matches exactly (case-sensitive)
- Confirm workflow is still running: `client.describe_workflow(key)`
- Check signal handler is registered in the workflow

### FAQ

**Q: Do I need a database?**
A: No. VELOCITY uses slab files and WAL for persistence. PostgreSQL is optional (only for the Embedded flavor or SQL visibility queries).

**Q: What happens on crash?**
A: On restart, the server mmaps slab files and replays unflushed WAL entries. Recovery is < 0.001 ms regardless of history size.

**Q: How many concurrent workflows can one node handle?**
A: 100,000+ concurrent workflows. Each uses only 128 bytes of slab memory.

**Q: Can I embed VELOCITY in my application?**
A: Yes. The Rust core can be linked directly via FFI. See the Rust SDK.

**Q: Is VELOCITY compatible with Temporal SDKs?**
A: The Classic flavor provides gRPC API compatibility. Many Temporal SDKs can connect directly with minimal or no code changes.

---

## Next Steps

- [Architecture Deep Dive](architecture.md) — Slab model, WAL, replication, security
- [API Reference](api_reference.md) — Complete gRPC API documentation
- [SDK Guides](sdk_guide.md) — Language-specific SDK documentation
- [Deployment Guide](deployment_guide.md) — Production deployment patterns
- [Troubleshooting](troubleshooting.md) — Extended debugging guide
