# VELOCITY-WorkFlow SDK Guide

> Comprehensive reference for all seven language SDKs.

---

## Table of Contents

1. [SDK Overview](#sdk-overview)
2. [Common Patterns](#common-patterns)
3. [Python SDK](#python-sdk)
4. [TypeScript SDK](#typescript-sdk)
5. [Go SDK](#go-sdk)
6. [Java SDK](#java-sdk)
7. [Rust SDK](#rust-sdk)
8. [PHP SDK](#php-sdk)
9. [Ruby SDK](#ruby-sdk)
10. [Error Handling](#error-handling)
11. [Best Practices](#best-practices)

---

## SDK Overview

VELOCITY-WorkFlow provides SDKs for seven languages. All SDKs communicate with the same Rust/C# engine via gRPC (or FFI for C#), ensuring language-agnostic workflow execution.

| SDK | Language | Transport | Package Manager | Status |
|-----|----------|-----------|-----------------|--------|
| `sdk/python` | Python 3.10+ | gRPC | pip | Stable |
| `sdk/typescript` | TypeScript / Node 18+ | gRPC | npm | Stable |
| `sdk/go` | Go 1.21+ | gRPC | go modules | Stable |
| `sdk/java` | Java 17+ | gRPC | Gradle | Stable |
| `sdk/rust` | Rust 1.82+ | FFI (C-ABI) | Cargo | Stable |
| `sdk/php` | PHP 8.2+ | gRPC | Composer | Stable |
| `sdk/ruby` | Ruby 3.2+ | gRPC | Bundler | Stable |

### Architecture

Every SDK follows the same three-layer architecture:

```
┌───────────────────────────────────────────────────────┐
│  Developer Code (Workflow / Activity definitions)      │
├───────────────────────────────────────────────────────┤
│  SDK Client                                            │
│  ┌────────────┐ ┌────────────┐ ┌──────────────────┐   │
│  │ gRPC Stub  │ │Interceptors│ │  Error Mapping   │   │
│  └─────┬──────┘ └────────────┘ └──────────────────┘   │
├────────┼──────────────────────────────────────────────┤
│  Transport: gRPC (HTTP/2) or FFI (C-ABI)              │
├────────┼──────────────────────────────────────────────┤
│  velocity-workflow-engine (Rust core)                  │
└───────────────────────────────────────────────────────┘
```

---

## Common Patterns

All SDKs share these core concepts:

### Client Connection

```
client = new VelocityClient(address, [auth_token])
```

### Workflow Lifecycle

```
1. start_workflow(type, queue, steps, input) → workflow_key
2. poll_task(queue, timeout) → task | null
3. complete_step(key, step_index, result)
4. signal_workflow(key, signal_name, payload)
5. query_workflow(key, query_name) → state
6. complete_workflow(key, result)
7. fail_workflow(key, error_message)
```

### Interceptor Chain

Every SDK supports an interceptor chain for cross-cutting concerns:

```
Request → LoggingInterceptor → MetricsInterceptor → TracingInterceptor → gRPC
```

### Retry Policy

```
RetryPolicy:
  initial_interval: 1s
  backoff_coefficient: 2.0
  max_interval: 30s
  max_attempts: 5
  non_retryable_errors: [WorkflowAlreadyCompletedError]
```

---

## Python SDK

**Location:** `sdk/python/`

### Installation

```bash
pip install -r requirements.txt
```

### Quick Start

```python
from velocity_sdk import VelocityClient, WorkflowStatus

client = VelocityClient("localhost:50051")

# Start a workflow
handle = client.start_workflow(
    workflow_type="order-processing",
    namespace="default",
    task_queue="orders",
    total_steps=3,
    input_data=b'{"order_id": 12345}',
)

# Check status
desc = client.describe_workflow(handle.workflow_key)
print(f"Status: {desc.status.name}")

# Send a signal
client.signal_workflow(handle.workflow_key, "payment-confirmed", b'{"amount": 99.99}')

# Complete
client.complete_workflow(handle.workflow_key, b'{"result": "shipped"}')
```

### Features

- `VelocityClient` — gRPC client with connection management
- `WorkflowStub` — Type-safe workflow stubs
- `RetryPolicy` — Configurable retry with exponential backoff
- `InterceptorChain` — Logging, metrics, tracing interceptors
- `PayloadCodec` — JSON, Binary, Protobuf codec chain
- `WorkflowTestEnvironment` — Mock client for unit testing
- `transpile_python` — Temporal-to-VELOCITY transpiler

### Examples

| File | Description |
|------|-------------|
| `examples/basic_workflow.py` | Start, signal, query, complete |
| `examples/simple_worker.py` | Worker polling and task execution |
| `examples/saga_pattern.py` | Distributed transaction with compensation |
| `examples/child_workflow.py` | Parent-child workflow orchestration |
| `examples/cron_schedule.py` | Recurring workflow execution |

---

## TypeScript SDK

**Location:** `sdk/typescript/`

### Installation

```bash
npm install
```

### Quick Start

```typescript
import { VelocityClient, WorkflowStatus } from '@velocity/core';

const client = new VelocityClient('localhost:50051');
await client.connect();

const handle = await client.startWorkflow({
  workflowType: 'order-processing',
  taskQueue: 'orders',
  totalSteps: 3,
  input: { order_id: 12345 },
});

console.log(`Status: ${VelocityClient.statusName(handle.status)}`);
await client.close();
```

### Features

- `VelocityClient` — gRPC client with async/await
- `WorkflowStub` — Type-safe workflow interfaces
- `retryWithBackoff` — Generic retry utility
- `InterceptorChain` — Composable interceptor pipeline
- `JsonCodec` / `BinaryCodec` / `NullCodec` — Payload encoding
- `TestWorkflowEnvironment` — In-memory test environment
- `transpileTypeScript` — Temporal-to-VELOCITY transpiler

### Examples

| File | Description |
|------|-------------|
| `examples/basic-workflow.ts` | Start, signal, query, complete |
| `examples/simple-worker.ts` | Worker polling and task execution |
| `examples/saga-pattern.ts` | Distributed transaction with compensation |
| `examples/child-workflow.ts` | Parent-child workflow orchestration |
| `examples/cron-schedule.ts` | Recurring workflow execution |

---

## Go SDK

**Location:** `sdk/go/`

### Installation

```bash
go mod download
```

### Quick Start

```go
import velocity_sdk "github.com/velocity-workflow/sdk/go/velocity_sdk"

client, err := velocity_sdk.NewClient("localhost:50051", "")
if err != nil {
    log.Fatal(err)
}
defer client.Close()

ctx := context.Background()
key, err := client.StartWorkflow(ctx, &velocity_sdk.StartWorkflowRequest{
    WorkflowType: "order-processing",
    TaskQueue:    "orders",
    TotalSteps:   3,
    Input:        []byte(`{"order_id": 12345}`),
})
```

### Features

- `velocity_sdk.Client` — gRPC client with context support
- `retry` package — Configurable retry with jitter
- `interceptors` package — gRPC unary interceptors
- `errors` package — Typed error mapping from gRPC status codes
- `codec` package — Payload encoding/decoding
- `testing` package — Mock client for unit tests
- `stub` package — Type-safe workflow stubs

### Examples

| File | Description |
|------|-------------|
| `examples/basic_workflow.go` | Start, signal, query, complete |
| `examples/simple_worker.go` | Worker polling and task execution |
| `examples/saga_pattern.go` | Distributed transaction with compensation |
| `examples/child_workflow.go` | Parent-child workflow orchestration |
| `examples/cron_schedule.go` | Recurring workflow execution |

---

## Java SDK

**Location:** `sdk/java/`

### Installation

```bash
./gradlew build
```

### Quick Start

```java
VelocityClient client = new VelocityClient("localhost:50051");

WorkflowHandle handle = client.startWorkflow(
    "order-processing", "default", "orders", 3,
    "{\"order_id\": 12345}".getBytes()
);

WorkflowDescription desc = client.describeWorkflow(handle.getWorkflowKey());
System.out.println("Status: " + desc.getStatus());
```

### Features

- `VelocityClient` — gRPC client
- `VelocityWorker` — Worker with annotation-based registration
- `@DurableWorkflow`, `@WorkflowMethod`, `@SignalMethod`, `@QueryMethod` — Annotations
- `VelocityException` hierarchy — Typed exceptions
- `LoggingInterceptor` / `MetricsInterceptor` — Interceptor chain
- `JsonPayloadCodec` — Payload encoding
- `VelocityJniBridge` — JNI bridge for native Rust FFI

### Examples

| File | Description |
|------|-------------|
| `src/.../examples/BasicWorkflow.java` | Start, signal, query, complete |
| `src/.../examples/SimpleWorker.java` | Worker polling and task execution |
| `src/.../examples/SagaPattern.java` | Distributed transaction with compensation |
| `src/.../examples/ChildWorkflow.java` | Parent-child workflow orchestration |
| `src/.../examples/CronSchedule.java` | Recurring workflow execution |

---

## Rust SDK

**Location:** `sdk/rust/`

### Installation

```bash
cargo build
```

### Quick Start

```rust
use velocity_sdk::{VelocityClient, WorkflowStatus};

let client = VelocityClient::new();
let key = client.start_workflow(1, 1, 42, 3);
let status = client.get_status(key);
assert_eq!(status, WorkflowStatus::Running);
client.complete_step(key, 0, b"done".to_vec()).unwrap();
```

### Features

- `VelocityClient` — FFI client (direct Rust, no gRPC overhead)
- `WorkflowStatus` — Enum with Display/Debug
- Zero-copy slab access via FFI
- `#![no_std]` compatible core types
- Examples run as Cargo examples

### Examples

| File | Description |
|------|-------------|
| `examples/basic_workflow.rs` | Start, signal, query, complete |
| `examples/simple_worker.rs` | Worker polling and task execution |
| `examples/saga_pattern.rs` | Distributed transaction with compensation |

---

## PHP SDK

**Location:** `sdk/php/`

### Installation

```bash
composer install
```

### Quick Start

```php
use Velocity\SDK\VelocityClient;

$client = new VelocityClient('localhost:50051');
$key = $client->startWorkflow(
    workflowType: 'order-processing',
    namespace: 'default',
    taskQueue: 'orders',
    totalSteps: 3,
    input: '{"order_id": 12345}',
);
$status = $client->getWorkflowStatus($key);
```

### Features

- `VelocityClient` — gRPC client with named arguments
- Typed exceptions (`VelocityException`, `WorkflowNotFoundException`)
- Interceptor support
- Payload codec chain

### Examples

| File | Description |
|------|-------------|
| `examples/basic_workflow.php` | Start, signal, query, complete |
| `examples/simple_worker.php` | Worker polling and task execution |
| `examples/saga_pattern.php` | Distributed transaction with compensation |

---

## Ruby SDK

**Location:** `sdk/ruby/`

### Installation

```bash
bundle install
```

### Quick Start

```ruby
require_relative 'lib/velocity_sdk'

client = VelocitySdk::VelocityClient.new(target: 'localhost:50051')
key = client.start_workflow('order-processing',
  namespace: 'default', task_queue: 'orders', total_steps: 3,
  input: '{"order_id": 12345}')
status = client.get_status(key)
```

### Features

- `VelocitySdk::VelocityClient` — gRPC client
- Keyword arguments for readability
- Signal handling for graceful shutdown
- RSpec test utilities

### Examples

| File | Description |
|------|-------------|
| `examples/basic_workflow.rb` | Start, signal, query, complete |
| `examples/simple_worker.rb` | Worker polling and task execution |
| `examples/saga_pattern.rb` | Distributed transaction with compensation |

---

## Error Handling

All SDKs map gRPC status codes to typed exceptions:

| gRPC Code | SDK Exception | Meaning |
|-----------|---------------|---------|
| `NOT_FOUND` | `WorkflowNotFoundError` | Workflow key does not exist |
| `ALREADY_EXISTS` | `WorkflowAlreadyCompletedError` | Workflow already in terminal state |
| `UNAVAILABLE` | `ConnectionError` | Server unreachable |
| `DEADLINE_EXCEEDED` | `TimeoutError` | Operation timed out |
| `RESOURCE_EXHAUSTED` | `RateLimitError` | Rate limit exceeded |
| `UNAUTHENTICATED` | `AuthenticationError` | Invalid or missing JWT |
| `INTERNAL` | `InternalError` | Server-side error |

### Retry Pattern

```python
from velocity_sdk import RetryPolicy, retry_with_policy

policy = RetryPolicy(
    initial_interval=1.0,
    backoff_coefficient=2.0,
    max_interval=30.0,
    max_attempts=5,
)

result = retry_with_policy(policy, lambda: client.describe_workflow(key))
```

---

## Best Practices

### 1. Connection Management

- Create one client per process — reuse across workflows
- Use connection pooling for high-throughput scenarios
- Always close the client on shutdown

### 2. Error Handling

- Catch typed exceptions, not generic errors
- Implement retry with exponential backoff for transient failures
- Do not retry `WorkflowAlreadyCompletedError`

### 3. Task Queue Design

- Use descriptive queue names (`orders`, `payments`, `notifications`)
- Separate long-running and short-running tasks onto different queues
- Monitor queue depth — alert on backlog growth

### 4. Workflow Design

- Keep workflows deterministic — no random, no time calls, no I/O
- Use activities for non-deterministic operations
- Set `total_steps` accurately for slab allocation

### 5. Signal and Query

- Use signals for external events (payment confirmed, user action)
- Use queries for read-only state inspection
- Signal handlers should be idempotent

### 6. Testing

- Use the mock client / test environment for unit tests
- Test happy path, error path, and timeout scenarios
- Verify workflow completion with assertions

### 7. Production Deployment

- Enable structured logging (JSON) for observability
- Configure health checks and readiness probes
- Set up Prometheus metrics and Grafana dashboards
- Use TLS in production environments
- Implement graceful shutdown handlers
