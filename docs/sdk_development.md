# SDK Developer Guide

> How to build a VELOCITY-WorkFlow SDK for any programming language.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Required Components](#required-components)
3. [Client Implementation](#client-implementation)
4. [Error Code Conventions](#error-code-conventions)
5. [Interceptor Chain Pattern](#interceptor-chain-pattern)
6. [Testing Utilities](#testing-utilities)
7. [Code Generation from Proto Files](#code-generation-from-proto-files)
8. [SDK Directory Structure](#sdk-directory-structure)
9. [Checklist](#checklist)

---

## Architecture Overview

Every VELOCITY-WorkFlow SDK follows the same three-layer architecture:

```
┌─────────────────────────────────────────────────────────────┐
│  Developer Code (Workflow / Activity definitions)           │
├─────────────────────────────────────────────────────────────┤
│  SDK Client                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │  gRPC Client │  │  Interceptors│  │  Error Mapping   │  │
│  │  (stub)      │  │  (chain)     │  │  (typed errors)  │  │
│  └──────┬───────┘  └──────────────┘  └──────────────────┘  │
├─────────┼───────────────────────────────────────────────────┤
│  Transport: gRPC (HTTP/2) or FFI (C-ABI)                    │
├─────────┼───────────────────────────────────────────────────┤
│  ┌──────▼───────────────────────────────────────────────┐   │
│  │  velocity-workflow-engine (Rust)                      │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │   │
│  │  │ Workflow │  │ Task     │  │ WAL / Timer /    │   │   │
│  │  │ Engine   │  │ Queue    │  │ Visibility       │   │   │
│  │  └──────────┘  └──────────┘  └──────────────────┘   │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  velocity-workflow-core (Rust)                        │   │
│  │  Slab allocator │ Bitmask256 │ Merkle verification   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**Two connection modes:**

1. **gRPC** — Language-agnostic; SDK generates a gRPC stub from `proto/velocity/v1/*.proto`. Used by Go, TypeScript, Python, Java, Ruby, PHP.
2. **FFI** — Direct C-ABI calls into the Rust engine. Used by C# (.NET) for zero-network-overhead scenarios. The C# `NativeBridge` loads `velocity_workflow_engine.so`/`.dll` via P/Invoke.

---

## Required Components

Every SDK must provide these components:

### 1. Client

The main entry point for developers. Wraps the gRPC stub or FFI bridge.

**Required methods:**

| Method | Description |
|--------|-------------|
| `NewClient(target, options)` | Create a connection to the server |
| `StartWorkflow(opts)` | Start a new workflow execution |
| `SignalWorkflow(workflowID, signalName, input)` | Deliver a signal |
| `SignalWithStart(opts)` | Atomically signal or start |
| `QueryWorkflow(workflowID, queryType, args)` | Query workflow state |
| `CancelWorkflow(workflowID)` | Request cancellation |
| `TerminateWorkflow(workflowID, reason)` | Forcefully terminate |
| `DescribeWorkflow(workflowID)` | Get workflow details |
| `ListWorkflows(filter)` | List workflows with pagination |
| `GetWorkflowHistory(workflowID)` | Get event history |
| `Close()` | Close the connection |

### 2. Error Types

Typed errors that map to the engine's error codes. See [Error Code Conventions](#error-code-conventions).

### 3. Interceptors

Middleware hooks for workflow and activity lifecycle events. See [Interceptor Chain Pattern](#interceptor-chain-pattern).

### 4. Testing Utilities

In-process test server for unit testing workflows without a running engine. See [Testing Utilities](#testing-utilities).

---

## Client Implementation

### Connection

```
Client ──gRPC──► WorkflowService (port 7234)
```

Configure the gRPC connection with:

- **Target address** — `host:port`
- **TLS** — optional TLS credentials
- **Auth metadata** — API key or JWT in the `authorization` header
- **Timeouts** — per-RPC deadlines

### StartWorkflow Example

```go
handle, err := client.StartWorkflow(ctx, &StartWorkflowOptions{
    WorkflowType: "OrderProcessingWorkflow",
    Namespace:    "default",
    TaskQueue:    "order-queue",
    TotalSteps:   5,
    Input:        []byte(`{"orderId": "12345"}`),
})
// handle.WorkflowKey  → engine slab key (uint64)
// handle.WorkflowID   → workflow identifier
// handle.Status       → current status
```

### Worker Polling

Workers poll for tasks in a loop:

```go
for {
    task, err := worker.PollWorkflowTask(ctx, taskQueue)
    if err != nil { break }

    // Execute workflow logic
    result := executeWorkflow(task)

    // Report completion
    worker.RespondTaskCompleted(ctx, task.TaskToken, result)
}
```

---

## Error Code Conventions

All SDKs must use consistent error codes. The engine defines these categories:

| SDK Error Code | Engine Code | gRPC Status | Retryable |
|---------------|-------------|-------------|-----------|
| `CodeUnknown (0)` | `Unknown` | `UNKNOWN (2)` | No |
| `CodeNotFound (1)` | `NotFound` | `NOT_FOUND (5)` | No |
| `CodeAlreadyCompleted (2)` | `AlreadyExists` | `ALREADY_EXISTS (6)` | No |
| `CodeConnection (3)` | `Unavailable` | `UNAVAILABLE (14)` | Yes |
| `CodeTimeout (4)` | `DeadlineExceeded` | `DEADLINE_EXCEEDED (4)` | Yes |
| `CodeRateLimit (5)` | `ResourceExhausted` | `RESOURCE_EXHAUSTED (8)` | Yes |
| `CodeAuthentication (6)` | `Unauthenticated` | `UNAUTHENTICATED (16)` | No |
| `CodeInternal (7)` | `InternalError` | `INTERNAL (13)` | No |

### Error Structure

Every error must include:

1. **Error code** — numeric, from the table above
2. **Message** — human-readable description
3. **Retryable flag** — whether the operation can be retried
4. **Details** — structured context (workflow key, namespace, etc.)

```go
type VelocityError struct {
    Message   string
    ErrorCode ErrorCode
    Retryable bool
    Details   map[string]interface{}
}
```

### Specialized Errors

| Error Type | When |
|-----------|------|
| `WorkflowNotFoundError` | Workflow does not exist |
| `WorkflowAlreadyCompletedError` | Workflow in terminal state |
| `NamespaceNotFoundError` | Namespace not registered |
| `RateLimitExceededError` | Rate limit hit |
| `AuthenticationError` | Invalid or missing credentials |
| `ConnectionError` | Server unreachable |
| `TimeoutError` | RPC deadline exceeded |

---

## Interceptor Chain Pattern

Interceptors provide cross-cutting concerns (logging, metrics, tracing) without polluting workflow logic.

### Interface

```go
// WorkflowInterceptor defines hooks for workflow lifecycle events.
type WorkflowInterceptor interface {
    OnStart(workflowType string, workflowID uint64)
    OnComplete(workflowID uint64, result []byte)
    OnFail(workflowID uint64, err error)
    OnSignal(workflowID uint64, signalName string)
}

// ActivityInterceptor defines hooks for activity lifecycle events.
type ActivityInterceptor interface {
    OnExecute(activityType string, activityID string)
    OnActivityComplete(activityID string, result []byte)
    OnActivityFail(activityID string, err error)
}
```

### Chain Execution

Interceptors are composed into a chain and executed in order:

```
Request → [Logging] → [Metrics] → [Tracing] → [Auth] → gRPC call
Response ← [Logging] ← [Metrics] ← [Tracing] ← [Auth] ← gRPC response
```

```go
chain := interceptors.NewInterceptorChain(
    interceptors.NewLoggingInterceptor("[WF]"),
    interceptors.NewMetricsInterceptor(registry),
    interceptors.NewTracingInterceptor(tracer),
)

client := velocity_sdk.NewClientWithInterceptors("localhost:7234", chain)
```

### Built-in Interceptors

| Interceptor | Purpose |
|-------------|---------|
| `LoggingInterceptor` | Logs lifecycle events |
| `MetricsInterceptor` | Records Prometheus metrics |
| `TracingInterceptor` | Creates OpenTelemetry spans |
| `RetryInterceptor` | Automatic retry with backoff |

---

## Testing Utilities

Every SDK must provide an in-process test environment:

### Test Server

A lightweight, in-memory engine that runs in the same process:

```go
func TestOrderWorkflow(t *testing.T) {
    ts := testing.NewTestServer()
    
    handle, err := ts.StartWorkflow(&StartWorkflowOptions{
        WorkflowType: "OrderProcessingWorkflow",
        TotalSteps:   3,
    })
    
    // Complete steps
    ts.CompleteStep(handle.WorkflowKey, 0, []byte("validated"))
    ts.CompleteStep(handle.WorkflowKey, 1, []byte("paid"))
    ts.CompleteStep(handle.WorkflowKey, 2, []byte("shipped"))
    
    // Verify
    desc := ts.DescribeWorkflow(handle.WorkflowKey)
    assert.Equal(t, StatusCompleted, desc.Status)
}
```

### Test Requirements

| Feature | Description |
|---------|-------------|
| In-memory engine | No external server needed |
| Deterministic time | Control timers and timeouts |
| Signal injection | Inject signals during execution |
| Activity mocking | Mock activity results |
| Error injection | Simulate failures |
| History verification | Assert on event history |

---

## Code Generation from Proto Files

### Generate gRPC Stubs

The proto files are in `proto/velocity/v1/`:

| File | Contents |
|------|----------|
| `workflow_service.proto` | Service definition + all request/response messages |
| `messages.proto` | Shared messages |
| `common.proto` | Enums, Payload, RetryPolicy |
| `errordetails.proto` | Error detail messages |

### Language-Specific Generation

**Go:**
```bash
protoc --go_out=. --go-grpc_out=. \
  proto/velocity/v1/*.proto
```

**TypeScript:**
```bash
protoc --plugin=protoc-gen-ts=./node_modules/.bin/protoc-gen-ts \
  --ts_out=. proto/velocity/v1/*.proto
```

**Python:**
```bash
python -m grpc_tools.protoc -I. \
  --python_out=. --grpc_python_out=. \
  proto/velocity/v1/*.proto
```

**Java:**
```bash
protoc --java_out=src/main/java \
  --grpc-java_out=src/main/java \
  proto/velocity/v1/*.proto
```

**Rust:**
```bash
# The engine uses tonic-build in build.rs
# Proto compilation is automatic with the `grpc` feature
cargo build --features grpc
```

---

## SDK Directory Structure

Each SDK follows this layout:

```
sdk/<language>/
├── <sdk_package>/          # Main client package
│   ├── client              # gRPC/FFI client
│   └── client_test         # Client tests
├── errors/                 # Error types and codes
│   └── errors
├── interceptors/           # Interceptor framework
│   └── interceptors
├── testing/                # Test utilities
│   └── testing
├── examples/               # Example workflows
│   └── hello_world
├── go.mod / package.json   # Package manifest
└── README.md
```

### Existing SDKs

| Language | Location | Status |
|----------|----------|--------|
| Go | `sdk/go/` | Complete |
| TypeScript | `sdk/typescript/` | Complete |
| Python | `sdk/python/` | Complete |
| Java | `sdk/java/` | Complete |
| Ruby | `sdk/ruby/` | Complete |
| PHP | `sdk/php/` | Complete |
| Rust | `sdk/rust/` | Complete |

---

## Checklist

When building a new SDK, verify:

- [ ] Client connects via gRPC (or FFI for .NET)
- [ ] All 21 RPCs are exposed as typed methods
- [ ] Error types map to engine error codes
- [ ] Retryable flag is set correctly per error type
- [ ] Interceptor chain executes in correct order
- [ ] Built-in logging interceptor provided
- [ ] Test server runs workflows in-process
- [ ] Activity mocking is supported in tests
- [ ] Proto stubs generated for the target language
- [ ] Examples demonstrate start/signal/query/complete flow
- [ ] Authentication (API key + JWT) is supported
- [ ] Pagination is handled transparently
- [ ] Context/cancellation propagation works correctly
- [ ] Connection pooling and keepalive configured
