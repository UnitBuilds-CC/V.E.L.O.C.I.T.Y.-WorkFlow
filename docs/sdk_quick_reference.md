# VELOCITY-WorkFlow SDK Quick Reference

> One-page cheat sheet for all 7 language SDKs. Print this or keep it handy while coding.

---

## Connection

| Language | Code |
|----------|------|
| **TypeScript** | `const client = new Client({ connection: { address: 'localhost:7233' } });` |
| **Python** | `client = Client(ClientOptions(host_port="localhost:7233"))` |
| **Go** | `client, _ := velocity.NewClient(velocity.ClientOptions{HostPort: "localhost:7233"})` |
| **Java** | `Client client = new Client(new ClientOptions()); // setHostPort("localhost:7233")` |
| **Rust** | `let client = VelocityClient::new();` |
| **PHP** | `$client = new VelocityClient("localhost:7233");` |
| **Ruby** | `client = Velocity::Client.new("localhost:7233")` |

---

## Start a Workflow

| Language | Code |
|----------|------|
| **TypeScript** | `await client.execute({ workflowId: 'wf-1', workflowType: 'myWorkflow', taskQueue: 'default', input: {...} });` |
| **Python** | `client.execute_workflow(WorkflowOptions(workflow_id="wf-1", workflow_type="my_workflow", task_queue="default", input_data={...}))` |
| **Go** | `result, err := client.Execute(ctx, velocity.WorkflowOptions{WorkflowID: "wf-1", WorkflowType: "myWorkflow", TaskQueue: "default", Input: data})` |
| **Java** | `Object result = client.executeWorkflow(options);` |
| **Rust** | `let key = client.start_workflow_with_input(1, 1, 42, 3, input);` |

---

## Define a Workflow

| Language | Code |
|----------|------|
| **TypeScript** | `async function myWorkflow(ctx: WorkflowContext, input: any) { ... }` |
| **Python** | `@register_workflow("my_workflow")`<br>`async def my_workflow(ctx: WorkflowContext, input_data: dict): ...` |
| **Go** | `func MyWorkflow(ctx velocity.WorkflowContext, input interface{}) (interface{}, error) { ... }` |
| **Java** | `public static Object execute(WorkflowContext ctx, Object input) { ... }` |
| **Rust** | `client.start_workflow(type_id, ns_id, tq_hash, total_steps);` |

---

## Define an Activity

| Language | Code |
|----------|------|
| **TypeScript** | `async function myActivity(ctx: ActivityContext, input: any) { ... }` |
| **Python** | `@register_activity("my_activity")`<br>`async def my_activity(ctx: ActivityContext, input_data: dict): ...` |
| **Go** | `func MyActivity(ctx context.Context, input interface{}) (interface{}, error) { ... }` |
| **Java** | `public static Object myActivity(ActivityContext ctx, Object input) { ... }` |
| **Rust** | `client.complete_step(key, step_index, result_bytes)?;` |

---

## Execute Activity (in Workflow)

| Language | Code |
|----------|------|
| **TypeScript** | `await WorkflowHelpers.executeActivity({ taskQueue: 'q', activityType: 'act', input: data });` |
| **Python** | Activities called via worker dispatch |
| **Go** | `result, err := velocity.ExecuteActivity(ctx, "activityType", input)` |
| **Java** | `WorkflowHelpers.executeActivity(ctx, "activityType", input)` |
| **Rust** | `client.complete_step(key, step, result)?;` |

---

## Start a Worker

| Language | Code |
|----------|------|
| **TypeScript** | `const worker = new Worker({ taskQueue: 'q', workflows: new Map([...]), activities: new Map([...]) });`<br>`await worker.start();` |
| **Python** | `worker = Worker(WorkerOptions(task_queue="q", workflows={...}, activities={...}))`<br>`await worker.start()` |
| **Go** | `worker := velocity.NewWorker(velocity.WorkerOptions{TaskQueue: "q"})`<br>`worker.RegisterWorkflow("name", Fn)`<br>`worker.Start()` |
| **Java** | `Worker worker = new Worker(workerOptions);`<br>`worker.start();` |
| **Rust** | Engine runs in-process — no separate worker needed |

---

## Signal a Workflow

| Language | Code |
|----------|------|
| **TypeScript** | `await client.signal('wf-1', { signalName: 'event', args: [data] });` |
| **Python** | `client.signal_workflow("wf-1", "event", input=data)` |
| **Go** | `client.Signal(ctx, "wf-1", velocity.SignalOptions{SignalName: "event", Args: data})` |
| **Java** | `client.signalWorkflow("wf-1", "event", data);` |
| **Rust** | `client.signal_workflow(key, signal_id, payload);` |

---

## Query a Workflow

| Language | Code |
|----------|------|
| **TypeScript** | `await client.query('wf-1', { queryType: 'get-status' });` |
| **Python** | `client.query_workflow("wf-1", "get-status")` |
| **Go** | `client.Query(ctx, "wf-1", velocity.QueryOptions{QueryType: "get-status"})` |
| **Java** | `client.queryWorkflow("wf-1", "get-status", null);` |
| **Rust** | `client.query_workflow(key, query_id)?;` |

---

## Terminate / Cancel

| Language | Terminate | Cancel |
|----------|-----------|--------|
| **TypeScript** | `client.terminate('wf-1', 'reason')` | `client.cancel('wf-1')` |
| **Python** | `client.terminate_workflow("wf-1", "reason")` | `client.cancel_workflow("wf-1")` |
| **Go** | `client.Terminate(ctx, "wf-1", "reason")` | `client.Cancel(ctx, "wf-1")` |
| **Java** | `client.terminateWorkflow("wf-1", "reason")` | `client.cancelWorkflow("wf-1")` |
| **Rust** | `client.cancel_workflow(key)` | — |

---

## Retry Policy

| Language | Code |
|----------|------|
| **TypeScript** | `retryPolicy: { initialInterval: 1000, backoffCoefficient: 2.0, maximumInterval: 30000, maximumAttempts: 5 }` |
| **Python** | `RetryPolicy(initial_interval=1.0, backoff_coefficient=2.0, maximum_interval=30.0, maximum_attempts=5)` |
| **Go** | `&velocity.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2.0, MaximumInterval: 30*time.Second, MaximumAttempts: 5}` |
| **Java** | `RetryPolicy policy = new RetryPolicy(); policy.setInitialInterval(1000); ...` |
| **Rust** | `RetryPolicy::new(RetryConfig { initial_interval: Duration::from_secs(1), ... })` |

---

## HTTP API (Velocity Runtime / Embedded)

```bash
# Start workflow
curl -X POST http://localhost:7233/api/v1/namespaces/default/workflows \
  -H "Content-Type: application/json" \
  -d '{"workflow_type": "myWorkflow", "task_queue": "default", "input": {...}}'

# List workflows
curl http://localhost:7233/api/v1/namespaces/default/workflows

# Signal workflow
curl -X POST http://localhost:7233/api/v1/namespaces/default/workflows/WF_ID/signal \
  -H "Content-Type: application/json" \
  -d '{"signal_name": "event", "input": {...}}'

# Health check
curl http://localhost:7233/health

# Prometheus metrics
curl http://localhost:7233/metrics
```

---

## Server Startup

```bash
# Flavor 1: Classic (gRPC)
cargo run --release -p velocity-dev-server -- --grpc-port 7234

# Flavor 2: Runtime (HTTP)
cargo run --release -p velocity-dev-server -- --port 7233

# Flavor 3: Embedded (Postgres)
docker run -d --name velocity-pg -p 5432:5432 -e POSTGRES_PASSWORD=velocity postgres:16
cargo run --release -p velocity-dev-server -- --embedded-mode --port 7233
```

---

## Key Rules

1. **Workflows must be deterministic** — no direct I/O, random, or time calls
2. **Use activities for I/O** — HTTP calls, database queries, file operations
3. **Use engine sleep** — `WorkflowHelpers.sleep()`, not `setTimeout()`
4. **Declare total steps** (Rust) — slab allocator needs bitmask size
5. **Register before start** — workflows and activities must be registered before worker starts
6. **Idempotent activities** — activities may be retried on failure
