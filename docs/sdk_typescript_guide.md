# VELOCITY-WorkFlow TypeScript SDK Guide

> Complete reference for building durable workflows with the TypeScript SDK.

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

The VELOCITY TypeScript SDK provides a natural, async/await-first API for defining and executing durable workflows. It connects to the VELOCITY-WorkFlow server via gRPC (Velocity Classic) or HTTP (Velocity Runtime).

**Package location:** `velocity-sdk-typescript/`

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Workflow** | A durable function that survives crashes. Defined with `defineWorkflow()`. |
| **Activity** | A non-deterministic function (I/O, HTTP calls). Defined with `defineActivity()`. |
| **Worker** | A long-running process that polls for tasks and executes workflows/activities. |
| **Client** | A connection to the VELOCITY server for starting and managing workflows. |
| **Task Queue** | A named queue that workers poll for work. |
| **Signal** | An external event injected into a running workflow. |
| **Query** | A read-only inspection of workflow state. |

---

## Installation

```bash
# From the SDK directory
cd velocity-sdk-typescript
npm install

# Or add to your project
npm install velocity-sdk-typescript
```

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Node.js | 18+ | Runtime |
| TypeScript | 5.0+ | Type checking |
| VELOCITY server | Running | Workflow engine |

---

## Connecting to the Server

```typescript
import { Client, Connection } from 'velocity-sdk-typescript';

// Default connection (localhost:7233)
const client = new Client();

// Custom connection
const client = new Client({
  connection: { address: 'my-server:7233' },
  namespace: 'default',
});

// Close when done
client.close();
```

### Connection Options

| Option | Default | Description |
|--------|---------|-------------|
| `address` | `localhost:7233` | Server address (host:port) |
| `namespace` | `default` | Default namespace for workflows |

---

## Defining Workflows

Workflows are durable functions that survive crashes. They must be deterministic — no direct I/O, random numbers, or time calls. Use activities for non-deterministic operations.

```typescript
import { WorkflowContext, WorkflowHelpers } from 'velocity-sdk-typescript';

// Define a workflow function
async function greetingWorkflow(ctx: WorkflowContext, input: { name: string }): Promise<string> {
  // Execute an activity (durable — survives crashes)
  const greeting = await WorkflowHelpers.executeActivity<string, string>({
    taskQueue: 'greetings',
    activityType: 'generateGreeting',
    input: input.name,
  });

  // Sleep is durable (engine-managed timer)
  await WorkflowHelpers.sleep(5000); // 5 seconds

  return greeting;
}
```

### Workflow Rules

1. **Deterministic**: Same input always produces same output
2. **No direct I/O**: Use activities for HTTP calls, database queries, file operations
3. **No random/time**: Use `WorkflowHelpers.sleep()` instead of `setTimeout()`
4. **Idempotent**: May be re-executed from any step after a crash

### Workflow Context

The `WorkflowContext` provides metadata about the current execution:

```typescript
interface WorkflowContext {
  workflowId: string;      // Unique workflow identifier
  runId: string;           // Current run ID
  taskQueue: string;       // Queue this workflow is running on
  memo?: Record<string, any>;           // Workflow memo
  searchAttributes?: Record<string, any>; // Search attributes
}
```

Access it from within a workflow:

```typescript
const info = WorkflowHelpers.getInfo();
console.log(`Running workflow: ${info.workflowId}`);
```

---

## Defining Activities

Activities are non-deterministic functions that perform I/O. They run on workers and are retried on failure.

```typescript
import { ActivityContext } from 'velocity-sdk-typescript';

// Define an activity
async function generateGreeting(ctx: ActivityContext, name: string): Promise<string> {
  // This is non-deterministic — that's OK in an activity
  const response = await fetch(`https://api.example.com/greet?name=${name}`);
  return response.text();
}
```

### Activity Context

```typescript
interface ActivityContext {
  taskToken: string;
  workflowExecution: { workflowId: string; runId: string };
  activityId: string;
  activityType: string;
  attempt: number;           // Retry attempt (1-based)
  scheduledTime: number;
  startedTime: number;
}
```

---

## Starting and Managing Workflows

### Start a Workflow

```typescript
const result = await client.start({
  workflowId: 'order-12345',
  workflowType: 'greetingWorkflow',
  taskQueue: 'greetings',
  input: { name: 'World' },
});

console.log(`Started: ${result.workflowExecution.workflowId}`);
```

### Start and Wait for Result

```typescript
const output = await client.execute<string>({
  workflowId: 'order-12345',
  workflowType: 'greetingWorkflow',
  taskQueue: 'greetings',
  input: { name: 'World' },
});

console.log(`Result: ${output}`);
```

### Get a Workflow Handle

```typescript
const handle = client.getWorkflow('order-12345');

// Describe
const info = await handle.describe();

// Get result (waits for completion)
const result = await handle.result<string>();

// Get history
const history = await handle.history();
```

### Terminate / Cancel

```typescript
// Terminate with a reason
await client.terminate('order-12345', 'User requested cancellation');

// Cancel (graceful)
await client.cancel('order-12345');
```

---

## Workers

Workers poll the server for tasks and execute workflow/activity logic.

```typescript
import { Worker } from 'velocity-sdk-typescript';

const worker = new Worker({
  taskQueue: 'greetings',
  workflows: new Map([
    ['greetingWorkflow', greetingWorkflow],
  ]),
  activities: new Map([
    ['generateGreeting', generateGreeting],
  ]),
});

// Start the worker (blocks until stopped)
await worker.start();

// Stop gracefully
await worker.stop();
```

### Worker Options

| Option | Required | Description |
|--------|----------|-------------|
| `taskQueue` | Yes | Queue to poll for tasks |
| `workflows` | No | Map of workflow name → function |
| `activities` | No | Map of activity name → function |
| `connection` | No | Connection options (defaults to localhost:7233) |
| `namespace` | No | Namespace (defaults to `default`) |

### Worker Lifecycle

```
1. Worker connects to VELOCITY server
2. Starts polling for workflow tasks on the task queue
3. Starts polling for activity tasks on the task queue
4. On receiving a task:
   a. Looks up the registered workflow/activity function
   b. Creates a context (WorkflowContext or ActivityContext)
   c. Executes the function
   d. Reports completion or failure
5. On stop(): finishes current tasks, then disconnects
```

---

## Signals and Queries

### Signals (External Events)

Signals inject external events into a running workflow:

```typescript
// Send a signal
await client.signal('order-12345', {
  signalName: 'payment-confirmed',
  args: [{ amount: 99.99 }],
});

// Using a handle
const handle = client.getWorkflow('order-12345');
await handle.signal('payment-confirmed', { amount: 99.99 });
```

### Queries (Read-Only State)

Queries read workflow state without modifying it:

```typescript
// Query a workflow
const status = await client.query('order-12345', {
  queryType: 'get-status',
});

// Using a handle
const handle = client.getWorkflow('order-12345');
const status = await handle.query('get-status');
```

---

## Child Workflows

Start child workflows from within a parent workflow:

```typescript
async function parentWorkflow(ctx: WorkflowContext, input: any) {
  // Start a child workflow
  const childResult = await WorkflowHelpers.executeChildWorkflow({
    workflowType: 'childWorkflow',
    workflowId: `child-${ctx.workflowId}`,
    taskQueue: 'greetings',
    input: { parentId: ctx.workflowId },
  });

  return childResult;
}
```

---

## Schedules and Cron

Create recurring workflow schedules:

```typescript
const scheduleClient = client.getScheduleClient();

// Create a schedule
await scheduleClient.create({
  scheduleId: 'daily-report',
  workflowType: 'generateReport',
  cronSchedule: '0 9 * * *', // Every day at 9 AM
  taskQueue: 'reports',
  input: { date: 'today' },
});

// List schedules
const schedules = await scheduleClient.list();

// Delete a schedule
await scheduleClient.delete('daily-report');
```

---

## Error Handling and Retries

### Workflow Errors

```typescript
try {
  const result = await client.execute({
    workflowId: 'order-12345',
    workflowType: 'greetingWorkflow',
    taskQueue: 'greetings',
    input: { name: 'World' },
  });
} catch (error) {
  if (error.message.includes('failed')) {
    console.error('Workflow failed:', error.message);
  } else if (error.message.includes('cancelled')) {
    console.error('Workflow was cancelled');
  } else if (error.message.includes('terminated')) {
    console.error('Workflow was terminated');
  }
}
```

### Retry Policy

Configure retry behavior when starting workflows:

```typescript
const result = await client.start({
  workflowId: 'order-12345',
  workflowType: 'greetingWorkflow',
  taskQueue: 'greetings',
  input: { name: 'World' },
  retryPolicy: {
    initialInterval: 1000,       // 1 second
    backoffCoefficient: 2.0,     // Double each retry
    maximumInterval: 30000,      // Max 30 seconds
    maximumAttempts: 5,          // Give up after 5 attempts
    nonRetryableErrorTypes: ['WorkflowAlreadyCompletedError'],
  },
});
```

### Activity Error Handling

Activities that throw will be retried according to the retry policy:

```typescript
async function unreliableActivity(ctx: ActivityContext, input: any) {
  if (ctx.attempt < 3) {
    throw new Error('Transient failure — will retry');
  }
  return 'Success on attempt ' + ctx.attempt;
}
```

---

## Advanced Features

### Workflow Updates

Send synchronous updates to running workflows:

```typescript
const result = await client.update('order-12345', {
  updateId: 'update-1',
  updateName: 'changeAddress',
  args: [{ newAddress: '123 Main St' }],
});

console.log(`Update status: ${result.status}`); // ACCEPTED
```

### Workflow Reset

Reset a workflow to a previous event for replay:

```typescript
const newRunId = await client.reset('order-12345', {
  workflowTaskFinishEventId: 10,
});
```

### Search Attributes

Set custom search attributes for visibility queries:

```typescript
const searchClient = client.getSearchAttributesClient();
// Use with the visibility query API
```

### Batch Operations

Perform operations on multiple workflows at once:

```typescript
const batchClient = client.getBatchOperationClient();
// Terminate, cancel, or signal multiple workflows
```

### Saga Pattern

Implement distributed transactions with compensating actions:

```typescript
import { Saga } from 'velocity-sdk-typescript';

const saga = new Saga();

saga.addStep(
  async () => { /* charge credit card */ },
  async () => { /* refund credit card */ },  // compensation
);

saga.addStep(
  async () => { /* reserve hotel */ },
  async () => { /* cancel hotel reservation */ },
);

await saga.execute();
```

---

## Testing

### Local Execution (No Server Required)

```typescript
import { Worker } from 'velocity-sdk-typescript';

const worker = new Worker({
  taskQueue: 'test-queue',
  workflows: new Map([['greetingWorkflow', greetingWorkflow]]),
  activities: new Map([['generateGreeting', generateGreeting]]),
});

// Execute locally without polling
const result = await worker.executeWorkflow(
  'test-workflow-1',
  'greetingWorkflow',
  { name: 'Test' }
);

console.log(`Test result: ${result}`);
```

---

## Complete Example

A complete working example with worker and client:

```typescript
import { Client, Worker, WorkflowContext, WorkflowHelpers, ActivityContext } from 'velocity-sdk-typescript';

// ─── Activities ──────────────────────────────────────────────────────────────

async function fetchOrderData(ctx: ActivityContext, orderId: string) {
  return { orderId, amount: 99.99, status: 'pending' };
}

async function chargePayment(ctx: ActivityContext, data: any) {
  console.log(`Charging $${data.amount} for order ${data.orderId}`);
  return { transactionId: `txn-${Date.now()}`, status: 'charged' };
}

async function sendConfirmation(ctx: ActivityContext, data: any) {
  console.log(`Sending confirmation for transaction ${data.transactionId}`);
  return { sent: true };
}

// ─── Workflow ────────────────────────────────────────────────────────────────

async function orderWorkflow(ctx: WorkflowContext, input: { orderId: string }) {
  // Step 1: Fetch order data
  const order = await WorkflowHelpers.executeActivity({
    taskQueue: 'orders',
    activityType: 'fetchOrderData',
    input: input.orderId,
  });

  // Step 2: Charge payment
  const payment = await WorkflowHelpers.executeActivity({
    taskQueue: 'orders',
    activityType: 'chargePayment',
    input: order,
  });

  // Step 3: Send confirmation
  await WorkflowHelpers.executeActivity({
    taskQueue: 'orders',
    activityType: 'sendConfirmation',
    input: payment,
  });

  return { orderId: input.orderId, status: 'completed' };
}

// ─── Worker ──────────────────────────────────────────────────────────────────

async function startWorker() {
  const worker = new Worker({
    taskQueue: 'orders',
    workflows: new Map([['orderWorkflow', orderWorkflow]]),
    activities: new Map([
      ['fetchOrderData', fetchOrderData],
      ['chargePayment', chargePayment],
      ['sendConfirmation', sendConfirmation],
    ]),
  });

  console.log('Worker started, polling for tasks...');
  await worker.start();
}

// ─── Client ──────────────────────────────────────────────────────────────────

async function startOrder() {
  const client = new Client({ connection: { address: 'localhost:7233' } });

  const result = await client.execute({
    workflowId: `order-${Date.now()}`,
    workflowType: 'orderWorkflow',
    taskQueue: 'orders',
    input: { orderId: 'ORD-12345' },
  });

  console.log('Order completed:', result);
  client.close();
}

// Run
startWorker().catch(console.error);
// In another process: startOrder().catch(console.error);
```
