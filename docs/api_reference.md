# VELOCITY-WorkFlow gRPC API Reference

> **Version:** 1.0.0 | **Package:** `velocity.v1` | **Transport:** gRPC (HTTP/2)

Complete reference for the `WorkflowService` gRPC API — the primary external surface for the VELOCITY-WorkFlow engine. All SDKs (Go, TypeScript, Python, Java, Ruby, PHP, Rust) connect to this service.

---

## Table of Contents

1. [Service Overview](#service-overview)
2. [Workflow Lifecycle RPCs](#workflow-lifecycle-rpcs)
3. [Workflow Visibility RPCs](#workflow-visibility-rpcs)
4. [Task Dispatch RPCs](#task-dispatch-rpcs)
5. [Namespace Management RPCs](#namespace-management-rpcs)
6. [System RPCs](#system-rpcs)
7. [Common Messages](#common-messages)
8. [Error Codes and Details](#error-codes-and-details)
9. [Authentication](#authentication)
10. [Rate Limiting](#rate-limiting)
11. [Pagination Patterns](#pagination-patterns)
12. [Example Requests (grpcurl)](#example-requests)

---

## Service Overview

```
service WorkflowService {
  // Lifecycle (6 RPCs)
  rpc StartWorkflowExecution(StartWorkflowExecutionRequest) returns (StartWorkflowExecutionResponse);
  rpc SignalWorkflowExecution(SignalWorkflowExecutionRequest) returns (SignalWorkflowExecutionResponse);
  rpc SignalWithStartWorkflowExecution(SignalWithStartWorkflowExecutionRequest) returns (SignalWithStartWorkflowExecutionResponse);
  rpc QueryWorkflow(QueryWorkflowRequest) returns (QueryWorkflowResponse);
  rpc CancelWorkflowExecution(CancelWorkflowExecutionRequest) returns (CancelWorkflowExecutionResponse);
  rpc TerminateWorkflowExecution(TerminateWorkflowExecutionRequest) returns (TerminateWorkflowExecutionResponse);

  // Visibility (3 RPCs)
  rpc DescribeWorkflowExecution(DescribeWorkflowExecutionRequest) returns (DescribeWorkflowExecutionResponse);
  rpc ListWorkflowExecutions(ListWorkflowExecutionsRequest) returns (ListWorkflowExecutionsResponse);
  rpc GetWorkflowExecutionHistory(GetWorkflowExecutionHistoryRequest) returns (GetWorkflowExecutionHistoryResponse);

  // Task Dispatch (6 RPCs)
  rpc PollWorkflowTaskQueue(PollWorkflowTaskQueueRequest) returns (PollWorkflowTaskQueueResponse);
  rpc PollActivityTaskQueue(PollActivityTaskQueueRequest) returns (PollActivityTaskQueueResponse);
  rpc RespondWorkflowTaskCompleted(RespondWorkflowTaskCompletedRequest) returns (RespondWorkflowTaskCompletedResponse);
  rpc RespondActivityTaskCompleted(RespondActivityTaskCompletedRequest) returns (RespondActivityTaskCompletedResponse);
  rpc RespondActivityTaskFailed(RespondActivityTaskFailedRequest) returns (RespondActivityTaskFailedResponse);
  rpc RespondQueryTaskCompleted(RespondQueryTaskCompletedRequest) returns (RespondQueryTaskCompletedResponse);

  // Namespace (4 RPCs)
  rpc RegisterNamespace(RegisterNamespaceRequest) returns (RegisterNamespaceResponse);
  rpc DescribeNamespace(DescribeNamespaceRequest) returns (DescribeNamespaceResponse);
  rpc ListNamespaces(ListNamespacesRequest) returns (ListNamespacesResponse);
  rpc UpdateNamespace(UpdateNamespaceRequest) returns (UpdateNamespaceResponse);

  // System (1 RPC)
  rpc GetSystemInfo(GetSystemInfoRequest) returns (GetSystemInfoResponse);

  // Advanced Visibility (4 RPCs)
  rpc CountWorkflowExecutions(CountWorkflowExecutionsRequest) returns (CountWorkflowExecutionsResponse);
  rpc ScanWorkflowExecutions(ScanWorkflowExecutionsRequest) returns (ScanWorkflowExecutionsResponse);
  rpc ResetWorkflowExecution(ResetWorkflowExecutionRequest) returns (ResetWorkflowExecutionResponse);
  rpc UpdateWorkflowExecution(UpdateWorkflowExecutionRequest) returns (UpdateWorkflowExecutionResponse);

  // Schedules (5 RPCs)
  rpc CreateSchedule(CreateScheduleRequest) returns (CreateScheduleResponse);
  rpc DescribeSchedule(DescribeScheduleRequest) returns (DescribeScheduleResponse);
  rpc ListSchedules(ListSchedulesRequest) returns (ListSchedulesResponse);
  rpc DeleteSchedule(DeleteScheduleRequest) returns (DeleteScheduleResponse);
  rpc UpdateSchedule(UpdateScheduleRequest) returns (UpdateScheduleResponse);

  // Batch Operations (3 RPCs)
  rpc StartBatchOperation(StartBatchOperationRequest) returns (StartBatchOperationResponse);
  rpc DescribeBatchOperation(DescribeBatchOperationRequest) returns (DescribeBatchOperationResponse);
  rpc ListBatchOperations(ListBatchOperationsRequest) returns (ListBatchOperationsResponse);
}
```

**Total: 32 RPCs** across 7 functional groups (Lifecycle, Visibility, Task Dispatch, Namespace, System, Advanced Visibility, Schedules, Batch Operations).

---

## Workflow Lifecycle RPCs

### StartWorkflowExecution

Starts a new workflow execution. Returns the workflow execution identifiers and the engine-assigned workflow key.

**Request: `StartWorkflowExecutionRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Target namespace name |
| `workflow_execution` | `WorkflowExecution` | Client-supplied `workflow_id`; `run_id` is server-generated |
| `workflow_type` | `WorkflowType` | Workflow definition name and optional `type_id` |
| `task_queue` | `TaskQueue` | Named queue workers poll for tasks |
| `input` | `Payload` | Opaque input bytes (JSON, protobuf, etc.) |
| `workflow_execution_timeout` | `Duration` | Total wall-clock timeout for the execution |
| `workflow_run_timeout` | `Duration` | Timeout for a single run |
| `workflow_task_timeout` | `Duration` | Timeout for a single workflow task |
| `identity` | `string` | Caller identity (for audit) |
| `request_id` | `string` | Idempotency key |
| `retry_policy` | `RetryPolicy` | Retry configuration |
| `cron_schedule` | `string` | Cron expression (e.g. `"0 * * * *"`) |
| `memo` | `Memo` | User-defined key-value metadata |
| `search_attributes` | `SearchAttributes` | Indexed fields for visibility queries |
| `header` | `Header` | Propagated key-value pairs (auth, tracing) |
| `parent_close_policy` | `ParentClosePolicy` | Behavior when parent closes |
| `total_steps` | `uint32` | Total steps for the slab engine |

**Response: `StartWorkflowExecutionResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `workflow_execution` | `WorkflowExecution` | `workflow_id` + server-generated `run_id` |
| `workflow_key` | `uint64` | Engine-internal slab key |
| `started` | `bool` | `true` if newly started; `false` if already running |

---

### SignalWorkflowExecution

Delivers a named signal to a running workflow execution.

**Request: `SignalWorkflowExecutionRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Target namespace |
| `workflow_execution` | `WorkflowExecution` | Target workflow |
| `signal_name` | `string` | Signal name |
| `signal_name_id` | `uint64` | Numeric signal identifier |
| `input` | `Payload` | Signal payload |
| `identity` | `string` | Caller identity |
| `request_id` | `string` | Idempotency key |
| `header` | `Header` | Propagated headers |

**Response: `SignalWorkflowExecutionResponse`** — empty message.

---

### SignalWithStartWorkflowExecution

Atomically signals a workflow or starts it if not already running. Combines start and signal into a single idempotent operation.

**Request: `SignalWithStartWorkflowExecutionRequest`**

Contains all fields from `StartWorkflowExecutionRequest` plus:

| Field | Type | Description |
|-------|------|-------------|
| `signal_name` | `string` | Signal name |
| `signal_name_id` | `uint64` | Numeric signal identifier |
| `signal_input` | `Payload` | Signal payload |

**Response: `SignalWithStartWorkflowExecutionResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `workflow_execution` | `WorkflowExecution` | Workflow identifiers |
| `workflow_key` | `uint64` | Engine slab key |
| `started` | `bool` | `true` if newly started |

---

### QueryWorkflow

Queries a running workflow's current state without mutating it.

**Request: `QueryWorkflowRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Target namespace |
| `workflow_execution` | `WorkflowExecution` | Target workflow |
| `query` | `Query` | Query type, name ID, args, and header |

**`Query` message:**

| Field | Type | Description |
|-------|------|-------------|
| `query_type` | `string` | Query handler name |
| `query_name_id` | `uint64` | Numeric query identifier |
| `query_args` | `Payload` | Arguments |
| `header` | `Header` | Propagated headers |

**Response: `QueryWorkflowResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `query_result` | `Payload` | Result payload from the query handler |

---

### CancelWorkflowExecution

Requests graceful cancellation of a running workflow. The workflow receives a cancellation signal and can handle it.

**Request: `CancelWorkflowExecutionRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Target namespace |
| `workflow_execution` | `WorkflowExecution` | Target workflow |
| `identity` | `string` | Caller identity |
| `request_id` | `string` | Idempotency key |
| `details` | `Payload` | Cancellation details |

**Response: `CancelWorkflowExecutionResponse`** — empty message.

---

### TerminateWorkflowExecution

Forcefully terminates a workflow. The workflow does not receive a cancellation signal.

**Request: `TerminateWorkflowExecutionRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Target namespace |
| `workflow_execution` | `WorkflowExecution` | Target workflow |
| `reason` | `string` | Termination reason |
| `identity` | `string` | Caller identity |
| `details` | `Payload` | Termination details |

**Response: `TerminateWorkflowExecutionResponse`** — empty message.

---

## Workflow Visibility RPCs

### DescribeWorkflowExecution

Returns detailed information about a workflow execution including pending activities and tasks.

**Request: `DescribeWorkflowExecutionRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Target namespace |
| `workflow_execution` | `WorkflowExecution` | Target workflow |

**Response: `DescribeWorkflowExecutionResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `execution_info` | `WorkflowExecutionInfo` | Full execution summary |
| `pending_activities` | `PendingActivityInfo[]` | List of in-flight activities |
| `pending_workflow_task` | `PendingWorkflowTaskInfo` | Current workflow task (if any) |

---

### ListWorkflowExecutions

Lists workflow executions with optional filtering and pagination.

**Request: `ListWorkflowExecutionsRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Target namespace |
| `page_size` | `int32` | Maximum results per page |
| `next_page_token` | `bytes` | Opaque pagination token |
| `status_filter` | `WorkflowExecutionStatus` | Filter by status |
| `namespace_id_filter` | `uint64` | Filter by namespace ID |
| `type_filter` | `WorkflowType` | Filter by workflow type |
| `start_time_min` | `Timestamp` | Earliest start time |
| `start_time_max` | `Timestamp` | Latest start time |

**Response: `ListWorkflowExecutionsResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `executions` | `WorkflowExecutionInfo[]` | Matching executions |
| `next_page_token` | `bytes` | Token for next page (empty if last page) |

---

### GetWorkflowExecutionHistory

Returns the event history for a workflow execution.

**Request: `GetWorkflowExecutionHistoryRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Target namespace |
| `workflow_execution` | `WorkflowExecution` | Target workflow |
| `maximum_page_size` | `int32` | Max events per page |
| `next_page_token` | `bytes` | Pagination token |
| `wait_new_event` | `bool` | Long-poll for new events |
| `history_event_filter_type` | `HistoryEventFilterType` | `ALL_EVENT` or `CLOSE_EVENT` |

**Response: `GetWorkflowExecutionHistoryResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `history` | `History` | List of `HistoryEvent` |
| `next_page_token` | `bytes` | Pagination token |
| `archived` | `bool` | Whether history was served from archive |

---

## Task Dispatch RPCs

### PollWorkflowTaskQueue

Called by workflow workers to receive workflow tasks. Blocks until a task is available or the deadline expires.

**Request: `PollWorkflowTaskQueueRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Target namespace |
| `task_queue` | `TaskQueue` | Queue to poll |
| `identity` | `string` | Worker identity |
| `build_id` | `string` | Worker build ID for versioning |

**Response: `PollWorkflowTaskQueueResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `task_token` | `uint64` | Opaque task token |
| `workflow_execution` | `WorkflowExecution` | Workflow identifiers |
| `workflow_type` | `WorkflowType` | Workflow type |
| `history` | `History` | Event history for replay |
| `workflow_key` | `uint64` | Engine slab key |
| `step_index` | `uint32` | Current step index |
| `attempt` | `int32` | Attempt number |

---

### PollActivityTaskQueue

Called by activity workers to receive activity tasks.

**Request: `PollActivityTaskQueueRequest`** — same structure as `PollWorkflowTaskQueueRequest`.

**Response: `PollActivityTaskQueueResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `task_token` | `uint64` | Opaque task token |
| `workflow_execution` | `WorkflowExecution` | Workflow identifiers |
| `activity_type` | `ActivityType` | Activity type name and ID |
| `input` | `Payload` | Activity input |
| `workflow_key` | `uint64` | Engine slab key |
| `step_index` | `uint32` | Step index |
| `attempt` | `int32` | Attempt number |
| `scheduled_time` | `Timestamp` | When the activity was scheduled |
| `started_time` | `Timestamp` | When the activity started |
| `retry_policy` | `RetryPolicy` | Retry configuration |

---

### RespondWorkflowTaskCompleted

Marks a workflow task as completed and submits commands.

**Request: `RespondWorkflowTaskCompletedRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `task_token` | `uint64` | Task token from poll |
| `identity` | `string` | Worker identity |
| `commands` | `Command[]` | Commands to execute |
| `query_results` | `map<string, Payload>` | Query results |
| `namespace` | `string` | Namespace |

**`Command` message** — oneof:

| Variant | Attributes Message |
|---------|-------------------|
| `complete_workflow` | `CompleteWorkflowCommandAttributes` |
| `fail_workflow` | `FailWorkflowCommandAttributes` |
| `schedule_activity` | `ScheduleActivityCommandAttributes` |
| `start_timer` | `StartTimerCommandAttributes` |
| `signal_external` | `SignalExternalCommandAttributes` |
| `start_child_workflow` | `StartChildWorkflowCommandAttributes` |
| `cancel_workflow` | `CancelWorkflowCommandAttributes` |
| `continue_as_new` | `ContinueAsNewCommandAttributes` |

**Response: `RespondWorkflowTaskCompletedResponse`** — empty message.

---

### RespondActivityTaskCompleted

Marks an activity task as completed with a result payload.

**Request: `RespondActivityTaskCompletedRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `task_token` | `uint64` | Task token from poll |
| `result` | `Payload` | Activity result |
| `identity` | `string` | Worker identity |
| `namespace` | `string` | Namespace |
| `workflow_key` | `uint64` | Engine slab key |
| `step_index` | `uint32` | Step index |

**Response: `RespondActivityTaskCompletedResponse`** — empty message.

---

### RespondActivityTaskFailed

Marks an activity task as failed.

**Request: `RespondActivityTaskFailedRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `task_token` | `uint64` | Task token from poll |
| `failure` | `Payload` | Failure details |
| `identity` | `string` | Worker identity |
| `namespace` | `string` | Namespace |
| `workflow_key` | `uint64` | Engine slab key |
| `step_index` | `uint32` | Step index |

**Response: `RespondActivityTaskFailedResponse`** — empty message.

---

### RespondQueryTaskCompleted

Returns the result of a query task.

**Request: `RespondQueryTaskCompletedRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `task_token` | `uint64` | Task token from poll |
| `result` | `Payload` | Query result |
| `identity` | `string` | Worker identity |
| `namespace` | `string` | Namespace |

**Response: `RespondQueryTaskCompletedResponse`** — empty message.

---

## Namespace Management RPCs

### RegisterNamespace

Creates a new namespace.

**Request: `RegisterNamespaceRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Namespace name |
| `description` | `string` | Human-readable description |
| `workflow_execution_retention_period` | `Duration` | History retention period |
| `is_global_namespace` | `bool` | Whether this is a global (multi-region) namespace |
| `metadata` | `map<string, string>` | Arbitrary metadata |
| `max_concurrent_workflows` | `uint64` | Concurrency limit |

**Response: `RegisterNamespaceResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace_id` | `uint64` | Server-assigned namespace ID |

---

### DescribeNamespace

Returns information about a namespace.

**Request: `DescribeNamespaceRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Namespace name (or `namespace_id`) |
| `namespace_id` | `uint64` | Namespace ID (or `namespace`) |

**Response: `DescribeNamespaceResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace_info` | `NamespaceInfo` | Namespace metadata |
| `config` | `NamespaceConfig` | Namespace configuration |

---

### ListNamespaces

Lists all registered namespaces with pagination.

**Request: `ListNamespacesRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `page_size` | `int32` | Maximum results per page |
| `next_page_token` | `bytes` | Pagination token |

**Response: `ListNamespacesResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `namespaces` | `NamespaceInfo[]` | List of namespaces |
| `next_page_token` | `bytes` | Pagination token |

---

### UpdateNamespace

Updates namespace configuration.

**Request: `UpdateNamespaceRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace` | `string` | Namespace name |
| `update` | `NamespaceConfig` | Configuration to update |

**Response: `UpdateNamespaceResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `namespace_info` | `NamespaceInfo` | Updated namespace info |

---

## System RPCs

### GetSystemInfo

Returns server capabilities and version information.

**Request: `GetSystemInfoRequest`** — empty message.

**Response: `GetSystemInfoResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `system_info` | `SystemInfo` | Server info and capabilities |

**`SystemInfo`** contains:

| Field | Type | Description |
|-------|------|-------------|
| `server` | `ServerInfo` | Version and supported features |
| `capabilities` | `Capabilities` | Feature flags |

**`Capabilities`:**

| Field | Type | Description |
|-------|------|-------------|
| `signal_and_query_header` | `bool` | Header propagation support |
| `internal_error_differentiation` | `bool` | Error type differentiation |
| `signal_with_start_as_new` | `bool` | SignalWithStart support |
| `upsert_memo` | `bool` | Memo upsert support |
| `eager_workflow_start` | `bool` | Eager start support |
| `nexus` | `bool` | Nexus operations support |

---

## Common Messages

### WorkflowExecution

| Field | Type | Description |
|-------|------|-------------|
| `workflow_id` | `string` | User-supplied logical identifier |
| `run_id` | `string` | System-generated unique run identifier |

### WorkflowType

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Workflow type name (e.g. `"OrderProcessingWorkflow"`) |
| `type_id` | `uint64` | Internal numeric type ID |

### TaskQueue

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Human-readable queue name |
| `hash` | `uint64` | Computed hash (engine uses `u64`) |
| `kind` | `TaskKind` | `WORKFLOW_TASK`, `ACTIVITY_TASK`, `TIMER_TASK`, `SIGNAL_TASK` |

### Payload

| Field | Type | Description |
|-------|------|-------------|
| `data` | `bytes` | Raw byte payload |
| `encoding` | `Encoding` | `PROTO3` or `JSON` |
| `metadata` | `map<string, bytes>` | Content-type, compression, etc. |

### RetryPolicy

| Field | Type | Description |
|-------|------|-------------|
| `maximum_attempts` | `int32` | Max attempts (0 = unlimited) |
| `initial_interval` | `Duration` | Initial backoff |
| `backoff_coefficient` | `double` | Multiplier (e.g. 2.0 for exponential) |
| `maximum_interval` | `Duration` | Backoff cap |

### SearchAttributes

| Field | Type | Description |
|-------|------|-------------|
| `indexed_fields` | `map<string, SearchAttributeValue>` | Indexed key-value pairs |

`SearchAttributeValue` is a oneof: `string_value`, `integer_value`, `double_value`, `bool_value`, `datetime_value`, `keyword_value`.

### WorkflowExecutionStatus (enum)

| Value | Name |
|-------|------|
| 0 | `UNSPECIFIED` |
| 1 | `RUNNING` |
| 2 | `COMPLETED` |
| 3 | `FAILED` |
| 4 | `CANCELED` |
| 5 | `TERMINATED` |
| 6 | `CONTINUED_AS_NEW` |
| 7 | `TIMED_OUT` |

### ParentClosePolicy (enum)

| Value | Name |
|-------|------|
| 0 | `UNSPECIFIED` |
| 1 | `TERMINATE` |
| 2 | `CANCEL` |
| 3 | `ABANDON` |

---

## Error Codes and Details

Errors are returned as gRPC status details (`google.rpc.Status.details`). Each error type has a dedicated protobuf message.

| gRPC Code | Error Detail Message | Condition |
|-----------|---------------------|-----------|
| `NOT_FOUND (5)` | `WorkflowNotFoundFailure` | Workflow execution does not exist |
| `ALREADY_EXISTS (6)` | `WorkflowExecutionAlreadyStartedFailure` | Workflow ID already running |
| `NOT_FOUND (5)` | `NamespaceNotFoundFailure` | Namespace does not exist |
| `ALREADY_EXISTS (6)` | `NamespaceAlreadyExistsFailure` | Namespace name already registered |
| `FAILED_PRECONDITION (9)` | `NamespaceInvalidStateFailure` | Operation on inactive namespace |
| `FAILED_PRECONDITION (9)` | `WorkflowNotReadyFailure` | Operation requires running workflow |
| `INVALID_ARGUMENT (3)` | `QueryFailedFailure` | Query handler not registered or failed |
| `UNIMPLEMENTED (12)` | `ClientVersionNotSupportedFailure` | SDK version incompatible |
| `UNIMPLEMENTED (12)` | `FeatureVersionNotSupportedFailure` | Feature version unsupported |
| `DATA_LOSS (15)` | `ShardLostFailure` | Shard owning workflow key unavailable |
| `RESOURCE_EXHAUSTED (8)` | `RateLimitExceededFailure` | Namespace rate limit exceeded |
| `RESOURCE_EXHAUSTED (8)` | `ConcurrencyLimitExceededFailure` | Max concurrent workflows reached |
| `ABORTED (10)` | `MultiOperationExecutionAbortedFailure` | SignalWithStart conflict |
| `ABORTED (10)` | `CancellationAlreadyRequestedFailure` | Cancel already requested |
| `NOT_FOUND (5)` | `TaskQueueNotFoundFailure` | Task queue does not exist |

### Engine FFI Error Codes

The Rust engine returns FFI error codes to the C# bridge layer:

| Code | Name | Description |
|------|------|-------------|
| 0 | `Success` | Operation succeeded |
| -1 | `GenericError` | Unknown error |
| -100 | `WorkflowNotFound` | Workflow not found |
| -101 | `WorkflowAlreadyCompleted` | Workflow already in terminal state |
| -102 | `InvalidWorkflowState` | Invalid state transition |
| -103 | `StepOutOfRange` | Step index exceeds total steps |

---

## Authentication

VELOCITY-WorkFlow supports two authentication mechanisms:

### API Key Authentication

Pass the API key in the `authorization` gRPC metadata header:

```
authorization: Bearer <api-key>
```

API keys are managed per-namespace via the `ApiKeyManager`. Each key has scoped permissions:

- `StartWorkflow`, `SignalWorkflow`, `QueryWorkflow`
- `TerminateWorkflow`, `CancelWorkflow`
- `DescribeWorkflow`, `ListWorkflows`
- `AdminAccess` (full access)

### OAuth2 / JWT Authentication

Pass a JWT token in the `authorization` header:

```
authorization: Bearer <jwt-token>
```

The token must contain:
- `sub` — subject identifier
- `namespace_id` — target namespace
- `roles` — list of role names (`admin`, `operator`, `reader`)

Built-in roles:

| Role | Permissions |
|------|-------------|
| `admin` | All permissions including namespace management |
| `operator` | Start, signal, query, describe, list, poll, respond |
| `reader` | Query, describe, list only |

---

## Rate Limiting

The engine uses a **token bucket** algorithm with two tiers:

1. **Global rate limit** — applies to all operations across all namespaces.
2. **Per-namespace rate limit** — applies to each namespace independently.

Both limits must pass for an operation to proceed. Configure via:

- `RateLimiter::new(global_rate, global_capacity, default_namespace_rate)`
- `RateLimiter::set_namespace_limit(namespace_id, rate, capacity)`

When a limit is exceeded, the server returns `RESOURCE_EXHAUSTED` with a `RateLimitExceededFailure` detail.

---

## Pagination Patterns

List operations (`ListWorkflowExecutions`, `ListNamespaces`) use opaque token-based pagination:

1. Send the initial request with `page_size` set and `next_page_token` empty.
2. The response includes `next_page_token` — pass it in the next request.
3. An empty `next_page_token` in the response indicates the last page.

```
Request 1: page_size=100, next_page_token=""
Response 1: 100 results, next_page_token="abc123"

Request 2: page_size=100, next_page_token="abc123"
Response 2: 50 results, next_page_token=""  ← last page
```

---

## Example Requests

### Start a Workflow (grpcurl)

```bash
grpcurl -plaintext -d '{
  "namespace": "default",
  "workflow_execution": { "workflow_id": "order-12345" },
  "workflow_type": { "name": "OrderProcessingWorkflow" },
  "task_queue": { "name": "order-queue" },
  "input": { "data": "eyJpdGVtSWQiOiAiS1MtMTIzNCJ9", "encoding": "ENCODING_JSON" },
  "totalSteps": 5,
  "workflowExecutionTimeout": "3600s",
  "identity": "cli-admin"
}' localhost:7234 velocity.v1.WorkflowService/StartWorkflowExecution
```

### Signal a Workflow

```bash
grpcurl -plaintext -d '{
  "namespace": "default",
  "workflow_execution": { "workflow_id": "order-12345" },
  "signal_name": "payment_received",
  "signal_name_id": 1,
  "input": { "data": "eyJhbW91bnQiOiA5OS45OX0=", "encoding": "ENCODING_JSON" },
  "identity": "payment-service"
}' localhost:7234 velocity.v1.WorkflowService/SignalWorkflowExecution
```

### Query a Workflow

```bash
grpcurl -plaintext -d '{
  "namespace": "default",
  "workflow_execution": { "workflow_id": "order-12345" },
  "query": { "query_type": "get_status", "query_name_id": 1 }
}' localhost:7234 velocity.v1.WorkflowService/QueryWorkflow
```

### List Workflows with Pagination

```bash
grpcurl -plaintext -d '{
  "namespace": "default",
  "page_size": 50,
  "status_filter": "WORKFLOW_EXECUTION_STATUS_RUNNING"
}' localhost:7234 velocity.v1.WorkflowService/ListWorkflowExecutions
```

### Register a Namespace

```bash
grpcurl -plaintext -d '{
  "namespace": "production",
  "description": "Production workloads",
  "workflowExecutionRetentionPeriod": "7200s",
  "maxConcurrentWorkflows": 10000
}' localhost:7234 velocity.v1.WorkflowService/RegisterNamespace
```

### Get System Info

```bash
grpcurl -plaintext localhost:7234 velocity.v1.WorkflowService/GetSystemInfo
```

---

## Proto Files

The proto definitions are located in `proto/velocity/v1/`:

| File | Contents |
|------|----------|
| `workflow_service.proto` | Service definition and all request/response messages |
| `messages.proto` | Shared messages (WorkflowExecution, History, NamespaceInfo, etc.) |
| `common.proto` | Enums (Status, TaskKind), Payload, RetryPolicy, SearchAttributes |
| `errordetails.proto` | gRPC error detail messages |

Language-specific packages:
- **Go:** `go.velocity.dev/api/velocity/v1`
- **Java:** `dev.velocity.api.v1`
