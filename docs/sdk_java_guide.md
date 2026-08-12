# VELOCITY-WorkFlow Java SDK Guide

> Complete reference for building durable workflows with the Java SDK.

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
9. [Error Handling and Retries](#error-handling-and-retries)
10. [Advanced Features](#advanced-features)
11. [Complete Example](#complete-example)

---

## Overview

The VELOCITY Java SDK provides an idiomatic Java API with annotation-based workflow definitions, typed exceptions, and a high-level client. It uses gRPC for communication with the VELOCITY server.

**Package location:** `velocity-sdk-java/`

---

## Installation

```bash
cd velocity-sdk-java
./gradlew build

# Or use as a dependency in your project
# Add to build.gradle:
# implementation project(':velocity-sdk-java')
```

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Java | 17+ | Runtime |
| Gradle | 8.0+ | Build system |
| VELOCITY server | Running | Workflow engine |

---

## Connecting to the Server

```java
import io.velocity.*;

// Default connection (localhost:7233)
Client client = new Client(new ClientOptions());

// Custom connection
ClientOptions options = new ClientOptions();
options.setHostPort("my-server:7233");
options.setNamespace("default");
options.setUseTls(false);

Client client = new Client(options);

// Close when done
client.close();
```

### ClientOptions

| Option | Default | Description |
|--------|---------|-------------|
| `hostPort` | `localhost:7233` | Server address |
| `namespace` | `default` | Default namespace |
| `useTls` | `false` | Enable TLS |

---

## Defining Workflows

Workflows are durable functions. Register them with the workflow registry:

```java
import io.velocity.*;

// Define a workflow
public class GreetingWorkflow {
    public static Object execute(WorkflowContext ctx, Object input) {
        @SuppressWarnings("unchecked")
        var args = (java.util.Map<String, Object>) input;
        String name = (String) args.getOrDefault("name", "World");

        // Execute an activity
        Object greeting = WorkflowHelpers.executeActivity(
            ctx, "generateGreeting",
            java.util.Map.of("name", name)
        );

        return java.util.Map.of("greeting", greeting);
    }
}

// Register the workflow
WorkflowRegistry.register("greetingWorkflow", GreetingWorkflow::execute);
```

### Workflow Context

```java
public class WorkflowContext {
    String workflowId;
    String runId;
    String taskQueue;
    Map<String, Object> memo;
    Map<String, Object> searchAttributes;
}
```

---

## Defining Activities

Activities perform non-deterministic operations:

```java
import io.velocity.*;

public class GreetingActivity {
    public static Object generateGreeting(ActivityContext ctx, Object input) {
        @SuppressWarnings("unchecked")
        var args = (java.util.Map<String, Object>) input;
        String name = (String) args.get("name");
        return "Hello, " + name + "! Welcome to VELOCITY.";
    }
}

// Register the activity
ActivityRegistry.register("generateGreeting", GreetingActivity::generateGreeting);
```

---

## Starting and Managing Workflows

### Start a Workflow

```java
WorkflowOptions options = new WorkflowOptions();
options.setWorkflowId("order-12345");
options.setWorkflowType("orderWorkflow");
options.setTaskQueue("orders");
options.setInput(Map.of("order_id", "ORD-12345"));

WorkflowExecution execution = client.startWorkflow(options);
System.out.println("Started: " + execution.getWorkflowId());
```

### Start and Wait for Result

```java
Object result = client.executeWorkflow(options);
System.out.println("Result: " + result);
```

### Workflow Handle

```java
WorkflowHandle handle = client.getWorkflow("order-12345");

// Describe
Optional<WorkflowExecution> desc = handle.describe();

// Wait for result
Object result = handle.getResult();

// Get history
List<HistoryEvent> history = handle.getHistory();
```

### Terminate / Cancel

```java
client.terminateWorkflow("order-12345", "User requested cancellation");
client.cancelWorkflow("order-12345");
```

---

## Workers

Workers poll for tasks and execute workflows/activities:

```java
import io.velocity.*;

WorkerOptions workerOptions = new WorkerOptions();
workerOptions.setTaskQueue("orders");
workerOptions.setWorkflows(Map.of("orderWorkflow", OrderWorkflow::execute));
workerOptions.setActivities(Map.of(
    "fetchOrderData", FetchOrderActivity::execute,
    "chargePayment", ChargePaymentActivity::execute
));

Worker worker = new Worker(workerOptions);

// Start (blocks until stopped)
worker.start();

// Stop gracefully
worker.stop();
```

---

## Signals and Queries

### Signals

```java
// Signal a running workflow
client.signalWorkflow("order-12345", "payment-confirmed",
    Map.of("amount", 99.99));

// Using a handle
WorkflowHandle handle = client.getWorkflow("order-12345");
handle.signal("payment-confirmed", Map.of("amount", 99.99));
```

### Queries

```java
Object status = client.queryWorkflow("order-12345", "get-status", null);
```

---

## Error Handling and Retries

### Retry Policy

```java
RetryPolicy policy = new RetryPolicy();
policy.setInitialInterval(1000);       // 1 second
policy.setBackoffCoefficient(2.0);
policy.setMaximumInterval(30000);      // 30 seconds
policy.setMaximumAttempts(5);

WorkflowOptions options = new WorkflowOptions();
options.setWorkflowId("order-12345");
options.setWorkflowType("orderWorkflow");
options.setTaskQueue("orders");
options.setRetryPolicy(policy);
```

---

## Advanced Features

### Workflow Updates

```java
UpdateResult result = client.updateWorkflow("order-12345",
    new UpdateOptions("changeAddress", Map.of("newAddress", "123 Main St")));
System.out.println("Update status: " + result.status());
```

### Schedules

```java
ScheduleClient scheduleClient = client.getScheduleClient();
```

### Batch Operations

```java
BatchOperationClient batchClient = client.getBatchOperationClient();
```

### Search Attributes

```java
SearchAttributesClient searchClient = client.getSearchAttributesClient();
```

---

## Complete Example

```java
package io.velocity.examples;

import io.velocity.*;
import java.util.Map;

public class OrderProcessingExample {

    // Activity: Fetch order data
    public static Object fetchOrderData(ActivityContext ctx, Object input) {
        var args = (Map<String, Object>) input;
        String orderId = (String) args.get("order_id");
        System.out.println("[Activity] Fetching order " + orderId);
        return Map.of("order_id", orderId, "amount", 99.99, "status", "pending");
    }

    // Activity: Charge payment
    public static Object chargePayment(ActivityContext ctx, Object input) {
        var args = (Map<String, Object>) input;
        double amount = (double) args.get("amount");
        System.out.println("[Activity] Charging $" + amount);
        return Map.of("transaction_id", "txn-" + System.currentTimeMillis(), "status", "charged");
    }

    // Workflow: Process order
    public static Object orderWorkflow(WorkflowContext ctx, Object input) {
        var args = (Map<String, Object>) input;
        String orderId = (String) args.get("order_id");
        System.out.println("[Workflow] Processing order " + orderId);

        Object order = WorkflowHelpers.executeActivity(ctx, "fetchOrderData", args);
        Object payment = WorkflowHelpers.executeActivity(ctx, "chargePayment", order);

        return Map.of("order_id", orderId, "status", "completed");
    }

    public static void main(String[] args) throws InterruptedException {
        // Register workflows and activities
        WorkflowRegistry.register("orderWorkflow", OrderProcessingExample::orderWorkflow);
        ActivityRegistry.register("fetchOrderData", OrderProcessingExample::fetchOrderData);
        ActivityRegistry.register("chargePayment", OrderProcessingExample::chargePayment);

        // Create client
        Client client = new Client(new ClientOptions());

        // Start a workflow
        WorkflowOptions options = new WorkflowOptions();
        options.setWorkflowId("order-" + System.currentTimeMillis());
        options.setWorkflowType("orderWorkflow");
        options.setTaskQueue("orders");
        options.setInput(Map.of("order_id", "ORD-12345"));

        WorkflowExecution exec = client.startWorkflow(options);
        System.out.println("Started: " + exec.getWorkflowId());

        // Wait for result
        WorkflowHandle handle = client.getWorkflow(exec.getWorkflowId());
        Object result = handle.getResult();
        System.out.println("Result: " + result);

        client.close();
    }
}
```
