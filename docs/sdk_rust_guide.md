# VELOCITY-WorkFlow Rust SDK Guide

> Complete reference for building durable workflows with the native Rust SDK.

---

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Client Basics](#client-basics)
4. [Workflow Lifecycle](#workflow-lifecycle)
5. [Signals and Queries](#signals-and-queries)
6. [Interceptors](#interceptors)
7. [Error Handling](#error-handling)
8. [Retry Policies](#retry-policies)
9. [Codec Chain](#codec-chain)
10. [Testing](#testing)
11. [Complete Example](#complete-example)

---

## Overview

The VELOCITY Rust SDK provides a zero-allocation, native client that wraps `Arc<WorkflowEngine>` directly. Unlike other SDKs that use gRPC, the Rust SDK communicates via FFI (C-ABI) for maximum performance — no serialization overhead, no network stack.

**Package location:** `sdk/rust/`

### Key Advantages

| Feature | Benefit |
|---------|---------|
| Zero-copy FFI | Direct memory access to slab state |
| `Arc<WorkflowEngine>` | Shared ownership, thread-safe |
| `#![no_std]` compatible core | Works in embedded/bare-metal |
| Interceptor chain | Composable cross-cutting concerns |
| Codec chain | JSON, Binary, Protobuf encoding |

---

## Installation

```toml
# Cargo.toml
[dependencies]
velocity-sdk = { path = "../sdk/rust" }
velocity-workflow-engine = { path = "../velocity-workflow-engine" }
```

```bash
cd sdk/rust
cargo build
```

---

## Client Basics

```rust
use velocity_sdk::VelocityClient;

// Create a new client (owns a WorkflowEngine)
let client = VelocityClient::new();

// Or wrap an existing engine
use std::sync::Arc;
use velocity_workflow_engine::engine::WorkflowEngine;

let engine = Arc::new(WorkflowEngine::new());
let client = VelocityClient::with_engine(engine);
```

### Access the Engine

```rust
let engine = client.engine();
// Use engine directly for low-level operations
```

---

## Workflow Lifecycle

### Start a Workflow

```rust
// Start with explicit parameters
let workflow_key = client.start_workflow(
    1,    // workflow_type_id (hashed name)
    1,    // namespace_id
    42,   // task_queue_hash
    3,    // total_steps
);

assert!(workflow_key > 0);
```

### Start with Input

```rust
let input = b"{\"order_id\": \"ORD-12345\"}".to_vec();
let workflow_key = client.start_workflow_with_input(
    1,    // workflow_type_id
    1,    // namespace_id
    42,   // task_queue_hash
    3,    // total_steps
    input,
);
```

### Complete a Step

```rust
client.complete_step(workflow_key, 0, b"step 0 result".to_vec())?;
client.complete_step(workflow_key, 1, b"step 1 result".to_vec())?;
client.complete_step(workflow_key, 2, b"step 2 result".to_vec())?;
```

### Check Status

```rust
use velocity_workflow_engine::engine::WorkflowStatus;

let status = client.get_status(workflow_key);
assert_eq!(status, WorkflowStatus::Running);

// After completing all steps
let status = client.get_status(workflow_key);
assert_eq!(status, WorkflowStatus::Completed);
```

### Describe a Workflow

```rust
let desc = client.describe_workflow(workflow_key)?;
println!("Status: {:?}", desc.status);
println!("Step: {}/{}", desc.current_step, desc.total_steps);
```

### List All Workflows

```rust
let keys = client.list_workflows();
println!("Active workflows: {:?}", keys);
```

### Cancel a Workflow

```rust
client.cancel_workflow(workflow_key);
```

### Shutdown

```rust
client.destroy(); // Flushes WAL, stops timers
```

---

## Signals and Queries

### Signal a Workflow

```rust
let signal_id = 1001;
let payload = b"{\"event\": \"payment-confirmed\"}".to_vec();
client.signal_workflow(workflow_key, signal_id, payload);
```

### Query a Workflow

```rust
let query_id = 2001;
let result = client.query_workflow(workflow_key, query_id)?;
println!("Query result: {:?}", result);
```

---

## Interceptors

The interceptor chain provides composable cross-cutting concerns:

```rust
use velocity_sdk::interceptors::{InterceptorChain, Interceptor};

// Add a logging interceptor
client.interceptors_mut().add(Box::new(LoggingInterceptor));

// Add a metrics interceptor
client.interceptors_mut().add(Box::new(MetricsInterceptor::new(registry)));
```

### Built-in Interceptors

| Interceptor | Purpose |
|-------------|---------|
| `LoggingInterceptor` | Logs workflow start, signal, complete events |
| `MetricsInterceptor` | Records Prometheus-compatible metrics |
| `TracingInterceptor` | OpenTelemetry-compatible distributed tracing |

### Custom Interceptor

```rust
struct MyInterceptor;

impl Interceptor for MyInterceptor {
    fn on_workflow_start(&self, workflow_type_id: u64, key: u64) {
        println!("Workflow started: type={}, key={}", workflow_type_id, key);
    }

    fn on_workflow_signal(&self, key: u64, signal_id: u64) {
        println!("Signal received: key={}, signal={}", key, signal_id);
    }

    fn on_workflow_complete(&self, key: u64) {
        println!("Workflow completed: key={}", key);
    }
}
```

---

## Error Handling

All fallible operations return `Result<T, VelocityError>`:

```rust
use velocity_sdk::errors::VelocityError;

match client.describe_workflow(invalid_key) {
    Ok(desc) => println!("Status: {:?}", desc.status),
    Err(VelocityError::WorkflowNotFound(key)) => {
        eprintln!("Workflow {} not found", key);
    }
    Err(VelocityError::InvalidStep { key, step }) => {
        eprintln!("Invalid step {} for workflow {}", step, key);
    }
    Err(e) => eprintln!("Error: {}", e),
}
```

### Error Types

| Error | Description |
|-------|-------------|
| `WorkflowNotFound(key)` | Invalid workflow key |
| `InvalidStep { key, step }` | Step index out of range |
| `WorkflowAlreadyCompleted(key)` | Workflow in terminal state |
| `EngineShutdown` | Engine has been shut down |

---

## Retry Policies

```rust
use velocity_sdk::retry::{RetryPolicy, RetryConfig};

let policy = RetryPolicy::new(RetryConfig {
    initial_interval: Duration::from_secs(1),
    backoff_coefficient: 2.0,
    max_interval: Duration::from_secs(30),
    max_attempts: 5,
    non_retryable_errors: vec!["WorkflowAlreadyCompleted".to_string()],
});

// Use with operations
let result = policy.execute(|| {
    client.describe_workflow(workflow_key)
});
```

---

## Codec Chain

The codec chain handles payload encoding/decoding:

```rust
use velocity_sdk::codec::{JsonCodec, BinaryCodec, CodecChain};

let mut chain = CodecChain::new();
chain.register(Box::new(JsonCodec));
chain.register(Box::new(BinaryCodec));

// Encode
let encoded = chain.encode(&my_data)?;

// Decode
let decoded: MyType = chain.decode(&encoded)?;
```

---

## Testing

The SDK includes a test environment for unit testing workflows:

```rust
use velocity_sdk::testing::TestEnvironment;

#[test]
fn test_workflow_completion() {
    let env = TestEnvironment::new();
    let client = env.client();

    let key = client.start_workflow(1, 1, 42, 2);
    assert!(key > 0);

    client.complete_step(key, 0, b"done".to_vec()).unwrap();
    client.complete_step(key, 1, b"done".to_vec()).unwrap();

    let status = client.get_status(key);
    assert_eq!(status, WorkflowStatus::Completed);
}

#[test]
fn test_signal_delivery() {
    let env = TestEnvironment::new();
    let client = env.client();

    let key = client.start_workflow(1, 1, 42, 3);
    client.signal_workflow(key, 1001, b"event".to_vec());

    // Verify signal was delivered
    let desc = client.describe_workflow(key).unwrap();
    assert_eq!(desc.status, WorkflowStatus::Running);
}
```

---

## Complete Example

```rust
use velocity_sdk::VelocityClient;
use velocity_workflow_engine::engine::WorkflowStatus;

fn main() {
    // Create client
    let client = VelocityClient::new();

    // Start a 3-step workflow
    let input = br#"{"order_id": "ORD-12345"}"#.to_vec();
    let key = client.start_workflow_with_input(
        1,    // workflow type: "order-processing"
        1,    // namespace: "default"
        42,   // task queue: "orders"
        3,    // 3 steps
        input,
    );

    println!("Started workflow: key={}", key);

    // Step 1: Fetch order data
    client.complete_step(key, 0, br#"{"status": "fetched"}"#.to_vec()).unwrap();
    println!("Step 0 complete");

    // Step 2: Charge payment
    client.complete_step(key, 1, br#"{"status": "charged"}"#.to_vec()).unwrap();
    println!("Step 1 complete");

    // Step 3: Send confirmation
    client.complete_step(key, 2, br#"{"status": "confirmed"}"#.to_vec()).unwrap();
    println!("Step 2 complete");

    // Check final status
    let status = client.get_status(key);
    assert_eq!(status, WorkflowStatus::Completed);
    println!("Workflow completed!");

    // Describe the workflow
    let desc = client.describe_workflow(key).unwrap();
    println!("Final: step={}/{}", desc.current_step, desc.total_steps);

    // Cleanup
    client.destroy();
}
```
