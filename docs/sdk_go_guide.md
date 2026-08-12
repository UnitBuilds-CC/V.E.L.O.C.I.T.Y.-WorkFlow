# VELOCITY-WorkFlow Go SDK Guide

> Complete reference for building durable workflows with the Go SDK.

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

The VELOCITY Go SDK provides an idiomatic Go API with context support, typed errors, and goroutine-friendly workers. It follows Go conventions: explicit error returns, context propagation, and interface-based design.

**Package location:** `velocity-sdk-go/`

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Client** | High-level API for workflow management with `context.Context` support |
| **Worker** | Polls for tasks and executes workflows/activities |
| **WorkflowFunction** | `func(WorkflowContext, interface{}) (interface{}, error)` |
| **WorkflowContext** | Metadata about the current workflow execution |
| **WorkflowHandle** | Handle to a running workflow |
| **WorkflowExecution** | Result of starting a workflow (WorkflowID + RunID) |

---

## Installation

```bash
cd velocity-sdk-go
go mod download

# Or add to your project
import velocity "github.com/velocity-workflow/sdk/go/velocity_sdk"
```

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Go | 1.21+ | Runtime |
| VELOCITY server | Running | Workflow engine |

---

## Connecting to the Server

```go
import velocity "github.com/velocity-workflow/sdk/go/velocity_sdk"

// Default connection (localhost:7233)
client, err := velocity.NewClient(velocity.ClientOptions{})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Custom connection
client, err := velocity.NewClient(velocity.ClientOptions{
    HostPort:  "my-server:7233",
    Namespace: "default",
    TLS:       false,
})
```

### ClientOptions

| Field | Default | Description |
|-------|---------|-------------|
| `HostPort` | `localhost:7233` | Server address |
| `Namespace` | `default` | Default namespace |
| `TLS` | `false` | Enable TLS |

---

## Defining Workflows

Workflows are functions with the signature `func(WorkflowContext, interface{}) (interface{}, error)`:

```go
import velocity "github.com/velocity-workflow/sdk/go/velocity_sdk"

func GreetingWorkflow(ctx velocity.WorkflowContext, input interface{}) (interface{}, error) {
    args := input.(map[string]interface{})
    name := args["name"].(string)

    // Execute an activity
    result, err := velocity.ExecuteActivity(ctx, "generateGreeting", map[string]interface{}{
        "name": name,
    })
    if err != nil {
        return nil, err
    }

    return map[string]interface{}{
        "greeting": result,
    }, nil
}

// Register the workflow
func init() {
    velocity.RegisterWorkflow("greetingWorkflow", GreetingWorkflow)
}
```

### Workflow Context

```go
type WorkflowContext struct {
    WorkflowID       string
    RunID            string
    TaskQueue        string
    Memo             map[string]interface{}
    SearchAttributes map[string]interface{}
}
```

### In-Workflow Operations

```go
// Execute an activity
result, err := velocity.ExecuteActivity(ctx, "activityType", input)

// Sleep (durable timer)
err := velocity.Sleep(ctx, 5*time.Second)

// Start a child workflow
childResult, err := velocity.ExecuteChildWorkflow(ctx, "childWorkflow", "child-id-1", input)

// Get workflow info
info := velocity.GetWorkflowInfo(ctx)
fmt.Printf("Running: %s\n", info.WorkflowID)

// Signal another workflow
err = velocity.SignalExternal(ctx, "other-workflow", "event-name", data)
```

---

## Defining Activities

Activities are regular Go functions that perform I/O:

```go
func FetchOrderData(ctx context.Context, input interface{}) (interface{}, error) {
    args := input.(map[string]interface{})
    orderID := args["order_id"].(string)

    // Non-deterministic I/O is OK in activities
    resp, err := http.Get(fmt.Sprintf("https://api.example.com/orders/%s", orderID))
    if err != nil {
        return nil, fmt.Errorf("failed to fetch order: %w", err)
    }
    defer resp.Body.Close()

    var order map[string]interface{}
    json.NewDecoder(resp.Body).Decode(&order)
    return order, nil
}
```

---

## Starting and Managing Workflows

### Start a Workflow

```go
exec, err := client.Start(ctx, velocity.WorkflowOptions{
    WorkflowID:   "order-12345",
    WorkflowType: "orderWorkflow",
    TaskQueue:    "orders",
    Input:        map[string]interface{}{"order_id": "ORD-12345"},
})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Started: %s (run: %s)\n", exec.WorkflowID, exec.RunID)
```

### Start and Wait for Result

```go
result, err := client.Execute(ctx, velocity.WorkflowOptions{
    WorkflowID:   "order-12345",
    WorkflowType: "orderWorkflow",
    TaskQueue:    "orders",
    Input:        map[string]interface{}{"order_id": "ORD-12345"},
})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Result: %v\n", result)
```

### Workflow Handle

```go
handle := client.GetWorkflow("order-12345")

// Describe
desc, err := handle.Describe(ctx)
fmt.Printf("Status: %v\n", desc.Status)

// Wait for result
result, err := handle.Result(ctx)

// Get history
history, err := handle.GetHistory(ctx)
```

### Terminate / Cancel

```go
// Terminate with reason
err := client.Terminate(ctx, "order-12345", "User requested cancellation")

// Cancel (graceful)
err := client.Cancel(ctx, "order-12345")
```

---

## Workers

Workers poll for tasks and execute workflows/activities:

```go
import velocity "github.com/velocity-workflow/sdk/go/velocity_sdk"

worker := velocity.NewWorker(velocity.WorkerOptions{
    TaskQueue: "orders",
})

// Register workflows
worker.RegisterWorkflow("orderWorkflow", OrderWorkflow)

// Register activities
worker.RegisterActivity("fetchOrderData", FetchOrderData)
worker.RegisterActivity("chargePayment", ChargePayment)

// Start (blocks until stopped)
if err := worker.Start(); err != nil {
    log.Fatal(err)
}
```

### Worker Lifecycle

```
1. Worker connects to VELOCITY server
2. Polls for workflow tasks on the task queue
3. Polls for activity tasks on the task queue
4. On receiving a task:
   a. Looks up the registered function
   b. Creates a WorkflowContext
   c. Executes the function
   d. Reports completion or failure
5. On Stop(): finishes current tasks, disconnects
```

---

## Signals and Queries

### Signals

```go
// Signal a running workflow
err := client.Signal(ctx, "order-12345", velocity.SignalOptions{
    SignalName: "payment-confirmed",
    Args:       map[string]interface{}{"amount": 99.99},
})

// Using a handle
handle := client.GetWorkflow("order-12345")
err = handle.Signal(ctx, "payment-confirmed", map[string]interface{}{"amount": 99.99})

// Signal with start (atomic)
exec, err := client.SignalWithStart(ctx, "orderWorkflow", "payment-confirmed",
    map[string]interface{}{"amount": 99.99},
    velocity.WorkflowOptions{
        WorkflowID: "order-12345",
        TaskQueue:  "orders",
    },
)
```

### Queries

```go
// Query a workflow
result, err := client.Query(ctx, "order-12345", velocity.QueryOptions{
    QueryType: "get-status",
})

// Using a handle
handle := client.GetWorkflow("order-12345")
result, err = handle.Query(ctx, "get-status")
```

---

## Child Workflows

```go
func ParentWorkflow(ctx velocity.WorkflowContext, input interface{}) (interface{}, error) {
    childResult, err := velocity.ExecuteChildWorkflow(
        ctx,
        "childWorkflow",
        fmt.Sprintf("child-%s", ctx.WorkflowID),
        map[string]interface{}{"parent_id": ctx.WorkflowID},
    )
    if err != nil {
        return nil, err
    }
    return childResult, nil
}
```

---

## Schedules and Cron

```go
// Create a schedule
err := client.CreateSchedule(ctx, "daily-report", "0 9 * * *", "generateReport",
    velocity.WorkflowOptions{
        TaskQueue: "reports",
        Input:     map[string]interface{}{"date": "today"},
    },
)

// List schedules
schedules, err := client.ListSchedules(ctx)

// Delete a schedule
err = client.DeleteSchedule(ctx, "daily-report")
```

---

## Error Handling and Retries

### Typed Errors

```go
result, err := client.Execute(ctx, options)
if err != nil {
    switch {
    case strings.Contains(err.Error(), "failed"):
        log.Printf("Workflow failed: %v", err)
    case strings.Contains(err.Error(), "cancelled"):
        log.Printf("Workflow cancelled")
    case strings.Contains(err.Error(), "terminated"):
        log.Printf("Workflow terminated")
    }
}
```

### Retry Policy

```go
options := velocity.WorkflowOptions{
    WorkflowID:   "order-12345",
    WorkflowType: "orderWorkflow",
    TaskQueue:    "orders",
    RetryPolicy: &velocity.RetryPolicy{
        InitialInterval:    time.Second,
        BackoffCoefficient: 2.0,
        MaximumInterval:    30 * time.Second,
        MaximumAttempts:    5,
    },
}
```

---

## Advanced Features

### Search Workflows

```go
results, err := client.SearchWorkflows(ctx, "WorkflowType = 'orderWorkflow' AND Status = 'RUNNING'")
```

### List Workflows

```go
workflows, err := client.ListWorkflows(ctx)
```

### Reset Workflow

```go
err = client.ResetWorkflow(ctx, "order-12345", 10) // Reset to event ID 10
```

### Update Workflow

```go
result, err := client.UpdateWorkflow(ctx, "order-12345", "changeAddress",
    map[string]interface{}{"new_address": "123 Main St"},
)
```

### Continue As New

```go
newExec, err := client.ContinueAsNew(ctx, "order-12345",
    "orderWorkflowV2", "orders-v2", newInput)
```

### Batch Operations

```go
// Batch terminate
jobID, err := client.BatchTerminate(ctx, []string{"wf-1", "wf-2", "wf-3"}, "cleanup")

// Batch cancel
jobID, err := client.BatchCancel(ctx, []string{"wf-1", "wf-2"})

// Batch signal
jobID, err := client.BatchSignal(ctx, []string{"wf-1", "wf-2"}, "event", data)

// Check batch status
status, err := client.DescribeBatchOperation(ctx, jobID)
```

### Memo and Search Attributes

```go
// Set memo
err = client.SetMemo(ctx, "order-12345", map[string]interface{}{
    "customer": "acme-corp",
})

// Set search attributes
err = client.SetSearchAttributes(ctx, "order-12345", map[string]interface{}{
    "priority": "high",
    "region":   "us-east-1",
})
```

---

## Testing

```go
func TestGreetingWorkflow(t *testing.T) {
    // Register the workflow
    velocity.RegisterWorkflow("greetingWorkflow", GreetingWorkflow)
    velocity.RegisterActivity("generateGreeting", MockGenerateGreeting)

    // Create a worker for local execution
    worker := velocity.NewWorker(velocity.WorkerOptions{
        TaskQueue: "test-queue",
    })
    worker.RegisterWorkflow("greetingWorkflow", GreetingWorkflow)

    // Execute locally
    ctx := velocity.WorkflowContext{
        WorkflowID: "test-1",
        RunID:      "run-test-1",
        TaskQueue:  "test-queue",
        _worker:    worker,
    }

    result, err := GreetingWorkflow(ctx, map[string]interface{}{"name": "Test"})
    assert.NoError(t, err)
    assert.NotNil(t, result)
}
```

---

## Complete Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    velocity "github.com/velocity-workflow/sdk/go/velocity_sdk"
)

// ─── Activities ──────────────────────────────────────────────────────────────

func FetchOrderData(ctx context.Context, input interface{}) (interface{}, error) {
    args := input.(map[string]interface{})
    orderID := args["order_id"].(string)
    fmt.Printf("[Activity] Fetching order %s\n", orderID)
    return map[string]interface{}{
        "order_id": orderID,
        "amount":   99.99,
        "status":   "pending",
    }, nil
}

func ChargePayment(ctx context.Context, input interface{}) (interface{}, error) {
    args := input.(map[string]interface{})
    amount := args["amount"].(float64)
    fmt.Printf("[Activity] Charging $%.2f\n", amount)
    return map[string]interface{}{
        "transaction_id": fmt.Sprintf("txn-%d", time.Now().UnixNano()),
        "status":         "charged",
    }, nil
}

func SendConfirmation(ctx context.Context, input interface{}) (interface{}, error) {
    args := input.(map[string]interface{})
    txnID := args["transaction_id"].(string)
    fmt.Printf("[Activity] Sending confirmation for %s\n", txnID)
    return map[string]interface{}{"sent": true}, nil
}

// ─── Workflow ────────────────────────────────────────────────────────────────

func OrderWorkflow(ctx velocity.WorkflowContext, input interface{}) (interface{}, error) {
    args := input.(map[string]interface{})
    orderID := args["order_id"].(string)
    fmt.Printf("[Workflow] Processing order %s\n", orderID)

    // Step 1: Fetch order data
    order, err := velocity.ExecuteActivity(ctx, "fetchOrderData", args)
    if err != nil {
        return nil, err
    }

    // Step 2: Charge payment
    payment, err := velocity.ExecuteActivity(ctx, "chargePayment", order)
    if err != nil {
        return nil, err
    }

    // Step 3: Send confirmation
    _, err = velocity.ExecuteActivity(ctx, "sendConfirmation", payment)
    if err != nil {
        return nil, err
    }

    return map[string]interface{}{
        "order_id": orderID,
        "status":   "completed",
    }, nil
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
    ctx := context.Background()

    // Create client
    client, err := velocity.NewClient(velocity.ClientOptions{
        HostPort: "localhost:7233",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Start a workflow
    exec, err := client.Start(ctx, velocity.WorkflowOptions{
        WorkflowID:   fmt.Sprintf("order-%d", time.Now().UnixNano()),
        WorkflowType: "orderWorkflow",
        TaskQueue:    "orders",
        Input:        map[string]interface{}{"order_id": "ORD-12345"},
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Workflow started: %s (run: %s)\n", exec.WorkflowID, exec.RunID)

    // Wait for result
    handle := client.GetWorkflow(exec.WorkflowID)
    result, err := handle.Result(ctx)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Result: %v\n", result)
}
```
