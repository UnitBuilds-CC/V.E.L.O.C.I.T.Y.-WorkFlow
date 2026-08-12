# Getting Started with VELOCITY-WorkFlow

> Your first durable workflow in under five minutes.

---

## Table of Contents

1. [What is VELOCITY-WorkFlow?](#what-is-velocity-workflow)
2. [Three Flavors](#three-flavors)
3. [Prerequisites](#prerequisites)
4. [Installation](#installation)
5. [Quick Start: Your First Workflow](#quick-start-your-first-workflow)
6. [Running a Worker](#running-a-worker)
7. [Sending Signals and Queries](#sending-signals-and-queries)
8. [Next Steps](#next-steps)

---

## What is VELOCITY-WorkFlow?

VELOCITY-WorkFlow is a hardware-native, zero-allocation durable execution engine. It provides the same programming model as Temporal — workflows, activities, signals, queries, task queues — but replaces the event-sourcing replay architecture with an O(1) slab pointer-cast model built on `#![no_std]` Rust, memory-mapped slabs, and Merkle-verified state.

**Key benefits over traditional workflow engines:**

| Feature | Traditional Engines | VELOCITY-WorkFlow |
|---------|-------------------|-------------------|
| Crash recovery | O(N) event replay | O(1) pointer cast (< 0.001 ms) |
| Memory allocation | Managed heap + GC | Zero-allocation slab allocator |
| State verification | Trust database admin | SHA-256 Merkle root per slab |
| Infrastructure | 4+ services + database | Single binary or embedded |
| Transport | gRPC/HTTP2 | VCTP zero-copy UDP + shared memory |
| Encryption | External KMS | AES-256-GCM with key rotation |

---

## Three Flavors

VELOCITY-WorkFlow ships in three flavors. Choose the one that matches your use case:

| Flavor | Protocol | Best For | Quick Start |
|--------|----------|----------|-------------|
| **Velocity Classic** | gRPC | Temporal migration; full-featured | `cargo run -p velocity-dev-server -- --grpc-port 7234` |
| **Velocity Runtime** | HTTP/JSON | Lightweight; serverless; Restate migration | `cargo run -p velocity-dev-server -- --port 7233` |
| **Velocity Embedded** | HTTP + Postgres | DBOS migration; embedded durability | `cargo run -p velocity-dev-server -- --embedded-mode` |

### Flavor 1: Velocity Classic (gRPC)

The gRPC flavor provides a full Temporal-compatible API. Connect any Temporal SDK directly.

```bash
# Start the server
cargo run --release -p velocity-dev-server -- --grpc-port 7234

# In another terminal, start a workflow via gRPC
# (Use any Temporal SDK pointing to localhost:7234)
```

### Flavor 2: Velocity Runtime (HTTP)

The HTTP flavor provides a REST API for lightweight workflow management.

```bash
# Start the server
cargo run --release -p velocity-dev-server -- --port 7233

# Start a workflow via HTTP
curl -X POST http://localhost:7233/api/v1/namespaces/default/workflows \
  -H "Content-Type: application/json" \
  -d '{"workflow_type": "greeting", "task_queue": "greetings", "input": {"name": "World"}}'

# Check health
curl http://localhost:7233/health
```

### Flavor 3: Velocity Embedded (Postgres)

The Embedded flavor uses PostgreSQL for persistence, compatible with DBOS.

```bash
# Start PostgreSQL
docker run -d --name velocity-pg -p 5432:5432 -e POSTGRES_PASSWORD=velocity postgres:16

# Start the server in embedded mode
cargo run --release -p velocity-dev-server -- --embedded-mode --port 7233

# Start a workflow
curl -X POST http://localhost:7233/api/v1/namespaces/default/workflows \
  -H "Content-Type: application/json" \
  -d '{"workflow_type": "greeting", "task_queue": "greetings"}'
```

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| **Rust** | 1.82+ (stable) | Build the core engine |
| **.NET SDK** | 10.0-preview | Build the server |
| **Your language SDK** | See below | Write workflows |
| **Docker** (optional) | 24+ | Containerized deployment |

### Supported SDK Languages

- **Python** 3.10+
- **TypeScript** / Node.js 18+
- **Go** 1.21+
- **Java** 17+
- **Rust** 1.82+
- **PHP** 8.2+
- **Ruby** 3.2+

---

## Installation

### 1. Build the Server

```bash
git clone https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow.git
cd VELOCITY-WorkFlow

# Build the Rust core engine
cd velocity-workflow-core && cargo build --release && cd ..

# Build and start the server
cd src/Velocity.Workflow.Server
dotnet run
```

The server starts on `localhost:50051` (gRPC) and `localhost:5182` (HTTP API).

### 2. Install Your Language SDK

**Python:**
```bash
cd sdk/python
pip install -r requirements.txt
```

**TypeScript:**
```bash
cd sdk/typescript
npm install
```

**Go:**
```bash
cd sdk/go
go mod download
```

**Java:**
```bash
cd sdk/java
./gradlew build
```

**Rust:**
```bash
cd sdk/rust
cargo build
```

**PHP:**
```bash
cd sdk/php
composer install
```

**Ruby:**
```bash
cd sdk/ruby
bundle install
```

### 3. Docker (Alternative)

```bash
# Start the full stack with Docker Compose
docker compose up -d

# Verify the server is running
curl http://localhost:5182/health
```

---

## Quick Start: Your First Workflow

### Step 1: Define a Workflow (TypeScript)

```typescript
import { Durable } from '@velocity/core';

@Durable()
export async function greetingWorkflow(name: string): Promise<string> {
  // This code is durable — it survives crashes and restarts
  const greeting = await generateGreeting(name);
  await sendNotification(greeting);
  return greeting;
}

async function generateGreeting(name: string): Promise<string> {
  return `Hello, ${name}! Welcome to VELOCITY-WorkFlow.`;
}

async function sendNotification(message: string): Promise<void> {
  console.log(`[notification] ${message}`);
}
```

### Step 2: Start a Worker

```typescript
import { VelocityClient } from '@velocity/core';

const client = new VelocityClient('localhost:50051');

// Register the worker on a task queue
const worker = await client.createWorker({
  taskQueue: 'greetings',
  workflows: [greetingWorkflow],
});

await worker.start();
console.log('Worker is polling for tasks...');
```

### Step 3: Start a Workflow Execution

```typescript
// From any client process
const handle = await client.startWorkflow('greetingWorkflow', {
  taskQueue: 'greetings',
  args: ['World'],
});

const result = await handle.result();
console.log(`Result: ${result}`);
// Output: Result: Hello, World! Welcome to VELOCITY-WorkFlow.
```

### Python Equivalent

```python
from velocity_sdk import VelocityClient

client = VelocityClient("localhost:50051")

# Start a workflow
handle = client.start_workflow(
    workflow_type="greeting-workflow",
    namespace="default",
    task_queue="greetings",
    total_steps=2,
    input_data=b'{"name": "World"}',
)

print(f"Workflow started: key={handle.workflow_key}")

# Poll until completion
result = client.wait_for_completion(handle.workflow_key)
print(f"Result: {result}")
```

---

## Running a Worker

Workers poll the server for tasks and execute workflow / activity logic. Every SDK follows the same pattern:

1. **Connect** to the VELOCITY-WorkFlow server
2. **Register** workflow and activity handlers on a task queue
3. **Poll** for tasks in a loop
4. **Execute** the task handler
5. **Complete** or **fail** the task

See the `examples/simple_worker` file in your SDK directory for a complete working example.

```bash
# Python
python sdk/python/examples/simple_worker.py

# TypeScript
npx ts-node sdk/typescript/examples/simple-worker.ts

# Go
go run sdk/go/examples/simple_worker.go

# Java
cd sdk/java && ./gradlew run -PmainClass=io.velocity.examples.SimpleWorker

# Rust
cd sdk/rust && cargo run --example simple_worker

# PHP
php sdk/php/examples/simple_worker.php

# Ruby
ruby sdk/ruby/examples/simple_worker.rb
```

---

## Sending Signals and Queries

### Signals (External Events)

Signals inject external events into a running workflow:

```python
# Send a signal to a running workflow
client.signal_workflow(
    workflow_key,
    "payment-confirmed",
    b'{"amount": 99.99}',
)
```

### Queries (Read-Only State)

Queries read workflow state without modifying it:

```python
# Query the current state of a workflow
state = client.query_workflow(workflow_key, "get-status")
print(f"Current state: {state}")
```

---

## Next Steps

Now that you have a running workflow, explore the deeper topics:

| Topic | Description |
|-------|-------------|
| [Architecture Guide](architecture.md) | Deep dive into the slab engine, WAL, and replication |
| [SDK Guide](sdk_guide.md) | Language-specific SDK documentation for all 7 languages |
| [Deployment Guide](deployment.md) | Docker, Kubernetes, and production deployment |
| [Migration Guide](migration_from_temporal.md) | Migrate from Temporal to VELOCITY-WorkFlow |
| [API Reference](api_reference.md) | Full gRPC API and SDK method reference |
| [Troubleshooting](troubleshooting.md) | Common issues, debugging, and FAQ |

### Example Gallery

Each SDK ships with ready-to-run examples:

- `basic_workflow` — Start, signal, query, and complete a workflow
- `simple_worker` — Worker registration, polling, and task execution
- `saga_pattern` — Distributed transaction with compensating actions
- `child_workflow` — Parent-child workflow orchestration
- `cron_schedule` — Recurring workflow execution
