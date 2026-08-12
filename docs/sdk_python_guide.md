# VELOCITY-WorkFlow Python SDK Guide

> Complete reference for building durable workflows with the Python SDK.

---

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Connecting to the Server](#connecting-to-the-server)
4. [Defining Workflows](#defining-workflows)
5. [Defining Activities](#defining-activities)
6. [Starting and Managing Workflows](#starting-and-managing-workflows)
7. [Workers](#workers)
8. [Signals and Queries](#signals-and-queries)
9. [Child Workflows](#child-workflows)
10. [Schedules and Cron](#schedules-and-cron)
11. [Error Handling and Retries](#error-handling-and-retries)
12. [Advanced Features](#advanced-features)
13. [Testing](#testing)
14. [Complete Example](#complete-example)

---

## Overview

The VELOCITY Python SDK provides an idiomatic Python API for building durable workflows. It uses dataclasses, type hints, and async/await patterns familiar to Python developers.

**Package location:** `velocity-sdk-python/`

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Client** | Connection to the VELOCITY server for starting/managing workflows |
| **Worker** | Long-running process that polls for tasks and executes workflows |
| **Workflow** | A durable function registered with `@register_workflow` |
| **Activity** | A non-deterministic function registered with `@register_activity` |
| **WorkflowHandle** | Handle to a running workflow for signaling, querying, waiting |
| **WorkflowExecution** | Result of starting a workflow (workflow_id + run_id) |

---

## Installation

```bash
cd velocity-sdk-python
pip install -r requirements.txt

# Or add to your project
pip install velocity-sdk-python
```

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.10+ | Runtime |
| VELOCITY server | Running | Workflow engine |

---

## Connecting to the Server

```python
from velocity import Client, ClientOptions

# Default connection (localhost:7233)
client = Client(ClientOptions())

# Custom connection
client = Client(ClientOptions(
    host_port="my-server:7233",
    namespace="default",
    tls=False,
))

# Close when done
client.close()
```

### ClientOptions

| Option | Default | Description |
|--------|---------|-------------|
| `host_port` | `localhost:7233` | Server address |
| `namespace` | `default` | Default namespace |
| `tls` | `False` | Enable TLS |

---

## Defining Workflows

Workflows are durable functions. They must be deterministic — use activities for I/O.

```python
from velocity import WorkflowContext, register_workflow

@register_workflow("greeting_workflow")
async def greeting_workflow(ctx: WorkflowContext, input_data: dict) -> str:
    """A simple greeting workflow."""
    name = input_data.get("name", "World")
    # Workflow logic here — must be deterministic
    return f"Hello, {name}! Welcome to VELOCITY."
```

### Workflow Context

```python
@dataclass
class WorkflowContext:
    workflow_id: str
    run_id: str
    task_queue: str
    memo: dict | None = None
    search_attributes: dict | None = None
```

---

## Defining Activities

Activities perform non-deterministic operations (I/O, HTTP, database):

```python
from velocity import ActivityContext, register_activity

@register_activity("fetch_order")
async def fetch_order(ctx: ActivityContext, input_data: dict) -> dict:
    """Fetch order data from database."""
    order_id = input_data.get("order_id")
    # Non-deterministic I/O is OK in activities
    return {"order_id": order_id, "amount": 99.99, "status": "pending"}
```

### Activity Context

```python
@dataclass
class ActivityContext:
    task_token: str
    workflow_execution: WorkflowExecution
    activity_id: str
    activity_type: str
    attempt: int
    scheduled_time: int
    started_time: int
```

---

## Starting and Managing Workflows

### Start a Workflow

```python
from velocity import WorkflowOptions

execution = client.start_workflow(WorkflowOptions(
    workflow_id="order-12345",
    workflow_type="order_workflow",
    task_queue="orders",
    input_data={"order_id": "ORD-12345"},
))

print(f"Started: {execution.workflow_id}, run: {execution.run_id}")
```

### Start and Wait for Result

```python
result = client.execute_workflow(
    WorkflowOptions(
        workflow_id="order-12345",
        workflow_type="order_workflow",
        task_queue="orders",
        input_data={"order_id": "ORD-12345"},
    ),
    timeout=300,  # 5 minute timeout
)
print(f"Result: {result}")
```

### Get a Workflow Handle

```python
handle = client.get_workflow("order-12345")

# Describe
info = handle.describe()

# Wait for result
result = handle.result(timeout=300)

# Get history
history = handle.get_history()
```

### Terminate / Cancel

```python
# Terminate with reason
client.terminate_workflow("order-12345", "User requested cancellation")

# Cancel (graceful)
client.cancel_workflow("order-12345")
```

---

## Workers

Workers poll for tasks and execute workflows/activities:

```python
from velocity import Worker, WorkerOptions

worker = Worker(WorkerOptions(
    task_queue="orders",
    workflows={"order_workflow": order_workflow},
    activities={"fetch_order": fetch_order},
))

# Start the worker (blocks until stopped)
await worker.start()

# Stop gracefully
await worker.stop()
```

### Worker Lifecycle

```
1. Worker connects to VELOCITY server
2. Polls for workflow tasks on the task queue
3. Polls for activity tasks on the task queue
4. On receiving a task:
   a. Looks up the registered function
   b. Creates a context
   c. Executes the function
   d. Reports completion or failure
5. On stop(): finishes current tasks, disconnects
```

---

## Signals and Queries

### Signals (External Events)

```python
# Signal a running workflow
client.signal_workflow(
    workflow_id="order-12345",
    signal_name="payment-confirmed",
    input={"amount": 99.99},
)

# Using a handle
handle = client.get_workflow("order-12345")
handle.signal("payment-confirmed", {"amount": 99.99})
```

### Queries (Read-Only State)

```python
# Query a workflow
status = client.query_workflow(
    workflow_id="order-12345",
    query_type="get-status",
)

# Using a handle
handle = client.get_workflow("order-12345")
status = handle.query("get-status")
```

---

## Schedules and Cron

```python
schedule_client = client.get_schedule_client()

# Create a recurring schedule
schedule_client.create(
    schedule_id="daily-report",
    workflow_type="generate_report",
    cron_schedule="0 9 * * *",
    task_queue="reports",
)

# List schedules
schedules = schedule_client.list()

# Delete a schedule
schedule_client.delete("daily-report")
```

---

## Error Handling and Retries

### Workflow Errors

```python
try:
    result = client.execute_workflow(options)
except Exception as e:
    if "failed" in str(e):
        print(f"Workflow failed: {e}")
    elif "cancelled" in str(e):
        print("Workflow was cancelled")
```

### Retry Policy

```python
from velocity import RetryPolicy

options = WorkflowOptions(
    workflow_id="order-12345",
    workflow_type="order_workflow",
    task_queue="orders",
    retry_policy=RetryPolicy(
        initial_interval=1.0,
        backoff_coefficient=2.0,
        maximum_interval=30.0,
        maximum_attempts=5,
        non_retryable_error_types=["WorkflowAlreadyCompletedError"],
    ),
)
```

---

## Advanced Features

### Workflow Updates

```python
result = client.update_workflow("order-12345", UpdateOptions(
    update_id="update-1",
    update_name="change_address",
    args={"new_address": "123 Main St"},
))
print(f"Update status: {result.status}")  # ACCEPTED
```

### Workflow Reset

```python
new_run_id = client.reset_workflow("order-12345", ResetOptions(
    workflow_task_finish_event_id=10,
))
```

### Search Attributes

```python
search_client = client.get_search_attributes_client()
```

### Batch Operations

```python
batch_client = client.get_batch_operation_client()
# Terminate, cancel, or signal multiple workflows at once
```

### Saga Pattern

```python
from velocity import Saga

saga = Saga()

saga.add_step(
    action=lambda: charge_credit_card(),
    compensate=lambda: refund_credit_card(),
)

saga.add_step(
    action=lambda: reserve_hotel(),
    compensate=lambda: cancel_hotel(),
)

saga.execute()
```

---

## Testing

### Mock Client

```python
from velocity import Client, ClientOptions

# Create a client for testing
client = Client(ClientOptions(host_port="localhost:7233"))

# Test workflow execution
execution = client.start_workflow(WorkflowOptions(
    workflow_id="test-1",
    workflow_type="greeting_workflow",
    task_queue="test-queue",
    input_data={"name": "Test"},
))

assert execution.workflow_id == "test-1"
```

---

## Complete Example

```python
"""Complete VELOCITY Python SDK example: order processing workflow."""

import asyncio
from velocity import (
    Client, ClientOptions, Worker, WorkerOptions,
    WorkflowContext, ActivityContext,
    WorkflowOptions, register_workflow, register_activity,
)

# ─── Activities ──────────────────────────────────────────────────────────────

@register_activity("fetch_order_data")
async def fetch_order_data(ctx: ActivityContext, input_data: dict) -> dict:
    """Fetch order data from database."""
    order_id = input_data.get("order_id", "unknown")
    print(f"[Activity] Fetching order {order_id}")
    return {"order_id": order_id, "amount": 99.99, "status": "pending"}

@register_activity("charge_payment")
async def charge_payment(ctx: ActivityContext, input_data: dict) -> dict:
    """Charge payment for the order."""
    amount = input_data.get("amount", 0)
    print(f"[Activity] Charging ${amount}")
    return {"transaction_id": f"txn-{ctx.attempt}", "status": "charged"}

@register_activity("send_confirmation")
async def send_confirmation(ctx: ActivityContext, input_data: dict) -> dict:
    """Send order confirmation."""
    txn_id = input_data.get("transaction_id", "unknown")
    print(f"[Activity] Sending confirmation for {txn_id}")
    return {"sent": True}

# ─── Workflow ────────────────────────────────────────────────────────────────

@register_workflow("order_workflow")
async def order_workflow(ctx: WorkflowContext, input_data: dict) -> dict:
    """Process an order: fetch → charge → confirm."""
    order_id = input_data.get("order_id", "unknown")
    print(f"[Workflow] Processing order {order_id}")

    # Steps would execute activities via the worker
    return {"order_id": order_id, "status": "completed"}

# ─── Main ────────────────────────────────────────────────────────────────────

async def main():
    # Start worker
    worker = Worker(WorkerOptions(
        task_queue="orders",
        workflows={"order_workflow": order_workflow},
        activities={
            "fetch_order_data": fetch_order_data,
            "charge_payment": charge_payment,
            "send_confirmation": send_confirmation,
        },
    ))

    # Start client and submit workflow
    client = Client(ClientOptions(host_port="localhost:7233"))

    execution = client.start_workflow(WorkflowOptions(
        workflow_id="order-12345",
        workflow_type="order_workflow",
        task_queue="orders",
        input_data={"order_id": "ORD-12345"},
    ))

    print(f"Workflow started: {execution.workflow_id}")

    # Wait for result
    handle = client.get_workflow(execution.workflow_id)
    result = handle.result(timeout=60)
    print(f"Result: {result}")

    client.close()

if __name__ == "__main__":
    asyncio.run(main())
```
