# Velocity Classic (Rust Server)

<cite>
**Referenced Files in This Document**
- [velocity-classic-server/src/main.rs](file://velocity-classic-server/src/main.rs)
- [velocity-classic-server/Cargo.toml](file://velocity-classic-server/Cargo.toml)
- [velocity-server-bootstrap/src/lib.rs](file://velocity-server-bootstrap/src/lib.rs)
- [velocity-nmcp-protocol/src/lib.rs](file://velocity-nmcp-protocol/src/lib.rs)
- [velocity-classic-server/Dockerfile](file://velocity-classic-server/Dockerfile)
</cite>

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Worker System](#worker-system)
4. [Workflow Classes](#workflow-classes)
5. [Activity Classes](#activity-classes)
6. [HTTP API](#http-api)
7. [Temporal Compatibility](#temporal-compatibility)
8. [Configuration](#configuration)
9. [Performance Characteristics](#performance-characteristics)
10. [Deployment](#deployment)
11. [Monitoring and Debugging](#monitoring-and-debugging)
12. [Use Cases](#use-cases)
13. [Migration from Temporal](#migration-from-temporal)
14. [Limitations and Trade-offs](#limitations-and-trade-offs)

## Overview

Velocity Classic is the **Rust-native** flavor of Velocity with NMCP transport, designed for Temporal compatibility patterns and low-latency execution. The original TypeScript engine was replaced with a Rust server (commit 2bae043) for better performance and unified toolchain.

**Key Characteristics:**
- **Protocol:** NMCP (shmem + WebSocket)
- **Persistence:** WAL + optional PostgreSQL
- **Port:** 8083 (WebSocket default)
- **Memory:** ~9.23 MiB
- **Throughput:** 61.54 ops/s (simple workflow) — **tied for highest**
- **Latency:** p50=14.51ms, p99=18.1ms — **lowest latency**
- **Security:** API auth, rate limiting, audit logging, mTLS, security headers (via velocity-server-bootstrap)
- **Tracing:** OpenTelemetry with optional OTLP export
- **Allocator:** jemalloc (tikv-jemallocator)
- **VCTP Gateways:** Hosts WS-to-VCTP (WSS, 692 lines), HTTP-to-VCTP (HTTPS, 871 lines) gateways with TLS termination and rate limiting
- **VCTP RPC:** Shared VCTP RPC server (UDP :9090) with HMAC-SHA256 auth encryption, replay protection, 9,052 ops/s dispatch

**When to Use:**
- Migrating from Temporal workflows (API patterns)
- Need lowest latency with NMCP shmem IPC
- Want WAL durability with optional PostgreSQL
- Rust-native deployment without Node.js dependency

**When NOT to Use:**
- Need ACID transactions (use Embedded)
- Require SQL queries on workflow state (use Embedded)
- Need multi-node clustering (not yet supported)
- Need TypeScript-native development (use SDK instead)

## Architecture

### Component Overview

```mermaid
graph TB
    subgraph "Velocity Classic Server"
        A[Local Workers] -->|NMCP Shmem| B[NMCP Server]
        C[Remote Clients] -->|NMCP WebSocket| B
        B --> D[NmcpFrameRouter]
        D --> E[Workflow Engine]
        E --> F[WAL Writer]
        F --> G[WAL Files]
        E --> H[Optional PostgreSQL]
    end
    
    B --> I[Metrics Exporter]
```

### Request Flow

```mermaid
sequenceDiagram
    participant Client
    participant HTTP
    participant Server
    participant Worker
    participant Workflow
    participant Activity
    
    Client->>HTTP: POST /workflows
    HTTP->>Server: Route to VelocityServer
    Server->>Worker: Queue workflow
    Worker->>Workflow: Execute workflow
    Workflow->>Worker: Schedule activity
    Worker->>Activity: Execute activity
    Activity-->>Worker: Activity result
    Worker->>Workflow: Continue workflow
    Workflow-->>Worker: Workflow complete
    Worker-->>Server: Result
    Server-->>HTTP: Response
    HTTP-->>Client: 200 OK
```

### Worker Architecture

```mermaid
graph TB
    subgraph "Worker"
        A[Task Queue] --> B[Workflow Executor]
        A --> C[Activity Executor]
        B --> D[Workflow Instances]
        C --> E[Activity Instances]
        D --> F[State Management]
        E --> G[Result Handling]
    end
    
    H[HTTP Requests] --> A
    B --> I[Workflow Registry]
    C --> J[Activity Registry]
```

## Worker System

### Worker Configuration

```typescript
import { Worker } from './index';

const worker = await Worker.create({
  // Task queue configuration
  taskQueue: 'benchmark',
  
  // Concurrency limits
  maxConcurrentWorkflows: 100,
  maxConcurrentActivities: 200,
  
  // Timeouts
  workflowTimeout: 3600,  // 1 hour
  activityTimeout: 300,   // 5 minutes
  
  // Logging
  logLevel: 'info',  // trace, debug, info, warn, error
  
  // Advanced
  enableHeartbeats: true,
  heartbeatInterval: 30,  // seconds
});
```

### Worker Lifecycle

```typescript
// Create worker
const worker = await Worker.create({
  taskQueue: 'my-queue',
  maxConcurrentWorkflows: 50,
});

// Register workflows and activities
worker.registerWorkflow(MyWorkflow);
worker.registerActivity(MyActivity);

// Start worker
await worker.run();

// Graceful shutdown
process.on('SIGTERM', async () => {
  await worker.shutdown();
});
```

### Task Queue

```typescript
class TaskQueue {
  private queue: Task[] = [];
  private processing: Set<string> = new Set();
  
  async enqueue(task: Task): Promise<void> {
    this.queue.push(task);
    this.processNext();
  }
  
  private async processNext(): Promise<void> {
    if (this.queue.length === 0) return;
    if (this.processing.size >= this.maxConcurrent) return;
    
    const task = this.queue.shift()!;
    this.processing.add(task.id);
    
    try {
      await this.executeTask(task);
    } finally {
      this.processing.delete(task.id);
      this.processNext();
    }
  }
}
```

## Workflow Classes

### Defining Workflows

```typescript
import { Workflow } from './index';

class OrderWorkflow extends Workflow {
  static typeName = 'orderWorkflow';
  
  async execute(input: OrderInput): Promise<OrderResult> {
    // Step 1: Validate order
    const validated = await this.executeActivity<ValidatedOrder>(
      'validateOrder',
      input
    );
    
    // Step 2: Reserve inventory
    await this.executeActivity<void>(
      'reserveInventory',
      validated.items
    );
    
    // Step 3: Process payment
    const payment = await this.executeActivity<PaymentResult>(
      'processPayment',
      validated.payment
    );
    
    // Step 4: Create shipment
    const shipment = await this.executeActivity<ShipmentResult>(
      'createShipment',
      validated.address
    );
    
    // Step 5: Send confirmation
    await this.executeActivity<void>(
      'sendConfirmation',
      { email: validated.email, shipment }
    );
    
    return {
      orderId: validated.id,
      paymentId: payment.id,
      shipmentId: shipment.id,
    };
  }
}
```

### Workflow Features

**Timers:**
```typescript
class DelayedWorkflow extends Workflow {
  async execute(input: { delayMs: number }): Promise<void> {
    // Wait for specified duration
    await this.sleep(input.delayMs);
    
    // Continue after delay
    await this.executeActivity('processAfterDelay', {});
  }
}
```

**Signals:**
```typescript
class ApprovalWorkflow extends Workflow {
  private approved: boolean = false;
  
  async execute(input: ApprovalInput): Promise<ApprovalResult> {
    // Wait for approval signal
    await this.waitForSignal('approval', (signal) => {
      this.approved = signal.payload.approved;
    });
    
    if (!this.approved) {
      throw new Error('Workflow rejected');
    }
    
    // Continue with approved workflow
    return await this.executeActivity('processApproved', input);
  }
}
```

**Queries:**
```typescript
class StatusWorkflow extends Workflow {
  private progress: number = 0;
  
  async execute(input: WorkflowInput): Promise<WorkflowResult> {
    // Register query handler
    this.setQueryHandler('getProgress', () => {
      return { progress: this.progress };
    });
    
    // Execute steps with progress updates
    this.progress = 0.25;
    await this.executeActivity('step1', input);
    
    this.progress = 0.5;
    await this.executeActivity('step2', input);
    
    this.progress = 0.75;
    await this.executeActivity('step3', input);
    
    this.progress = 1.0;
    return await this.executeActivity('step4', input);
  }
}
```

### Workflow State

```typescript
class StatefulWorkflow extends Workflow {
  // Workflow state (persisted across activities)
  private state: WorkflowState = {
    step: 'init',
    retries: 0,
    data: {},
  };
  
  async execute(input: WorkflowInput): Promise<WorkflowResult> {
    // State is automatically saved after each activity
    this.state.step = 'processing';
    const result = await this.executeActivity('process', input);
    
    this.state.step = 'validating';
    this.state.data = result;
    await this.executeActivity('validate', result);
    
    this.state.step = 'complete';
    return result;
  }
}
```

## Activity Classes

### Defining Activities

```typescript
import { Activity } from './index';

class ValidateOrderActivity extends Activity {
  static typeName = 'validateOrder';
  
  async execute(input: OrderInput): Promise<ValidatedOrder> {
    // Validate order fields
    if (!input.items || input.items.length === 0) {
      throw new Error('Order must have items');
    }
    
    if (!input.payment || !input.payment.cardNumber) {
      throw new Error('Payment information required');
    }
    
    // Validate items
    for (const item of input.items) {
      if (item.quantity <= 0) {
        throw new Error(`Invalid quantity for item ${item.productId}`);
      }
    }
    
    // Return validated order
    return {
      id: generateOrderId(),
      items: input.items,
      payment: input.payment,
      address: input.address,
      email: input.email,
      validatedAt: new Date(),
    };
  }
}
```

### Activity Features

**Heartbeats:**
```typescript
class LongRunningActivity extends Activity {
  async execute(input: LongRunningInput): Promise<LongRunningResult> {
    const totalSteps = 100;
    
    for (let i = 0; i < totalSteps; i++) {
      // Perform work
      await this.doWork(i);
      
      // Send heartbeat
      this.heartbeat({
        progress: i / totalSteps,
        currentStep: i,
      });
    }
    
    return { success: true };
  }
}
```

**Retry Logic:**
```typescript
class RetryableActivity extends Activity {
  static retryPolicy = {
    maxAttempts: 3,
    initialInterval: 1000,  // 1 second
    backoffCoefficient: 2.0,
    maximumInterval: 10000,  // 10 seconds
  };
  
  async execute(input: RetryableInput): Promise<RetryableResult> {
    // Activity will be retried up to 3 times on failure
    const result = await this.callExternalService(input);
    return result;
  }
}
```

**Timeouts:**
```typescript
class TimeoutActivity extends Activity {
  static timeout = 30000;  // 30 seconds
  
  async execute(input: TimeoutInput): Promise<TimeoutResult> {
    // Must complete within 30 seconds or will be cancelled
    const result = await this.processWithTimeout(input);
    return result;
  }
}
```

### Activity Registration

```typescript
// Register activities
worker.registerActivity(ValidateOrderActivity);
worker.registerActivity(ReserveInventoryActivity);
worker.registerActivity(ProcessPaymentActivity);
worker.registerActivity(CreateShipmentActivity);
worker.registerActivity(SendConfirmationActivity);
```

## HTTP API

### Endpoints (via HTTP health endpoint)

```
POST   /workflows              - Start workflow
GET    /workflows/:id          - Get workflow
GET    /workflows              - List workflows
DELETE /workflows/:id          - Cancel workflow

POST   /workflows/:id/signal   - Send signal
GET    /workflows/:id/query    - Query workflow
POST   /workflows/:id/complete - Complete step
GET    /workflows/:id/wait     - Wait for completion

GET    /health                 - Health check
GET    /ready                  - Readiness probe
GET    /live                   - Liveness probe
GET    /metrics                - Prometheus metrics
```

### NMCP Transport

Primary communication uses NMCP protocol:
- **Local workers** connect via shared memory at `/tmp/velocity-classic.nmcp`
- **Remote clients** connect via WebSocket at `ws://0.0.0.0:8083`
- Both transports use the same binary frame format (16-byte header + JSON payload)
- TLS/mTLS supported on WebSocket endpoint
- 50-100x faster local IPC than HTTP

### Request/Response Examples

**Start Workflow:**
```http
POST /workflows HTTP/1.1
Content-Type: application/json

{
  "workflow_type": "orderWorkflow",
  "input": {
    "items": [
      { "productId": "prod-123", "quantity": 2 }
    ],
    "payment": {
      "cardNumber": "4111111111111111",
      "amount": 99.99
    },
    "address": {
      "street": "123 Main St",
      "city": "San Francisco",
      "state": "CA",
      "zip": "94105"
    },
    "email": "customer@example.com"
  },
  "task_queue": "orders"
}
```

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "workflow_id": "wf-abc123",
  "run_id": "run-def456",
  "status": "running",
  "created_at": "2026-08-14T19:28:01Z"
}
```

**Send Signal:**
```http
POST /workflows/wf-abc123/signal HTTP/1.1
Content-Type: application/json

{
  "signal_name": "approval",
  "payload": {
    "approved": true,
    "approver": "manager@example.com"
  }
}
```

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "signal_id": "sig-ghi789",
  "delivered": true
}
```

**Query Workflow:**
```http
GET /workflows/wf-abc123/query?type=getProgress HTTP/1.1
```

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "result": {
    "progress": 0.75,
    "currentStep": "validation"
  }
}
```

**Wait for Completion:**
```http
GET /workflows/wf-abc123/wait?timeout=30s HTTP/1.1
```

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "workflow_id": "wf-abc123",
  "status": "completed",
  "result": {
    "orderId": "order-123",
    "paymentId": "payment-456",
    "shipmentId": "shipment-789"
  }
}
```

## Temporal Compatibility

### API Mapping

| Temporal API | Velocity Classic | Notes |
|--------------|------------------|-------|
| `Workflow.executeActivity` | `this.executeActivity` | Same signature |
| `Workflow.sleep` | `this.sleep` | Same signature |
| `Workflow.sideEffect` | `this.sideEffect` | Same signature |
| `Workflow.signal` | `this.waitForSignal` | Slightly different name |
| `Workflow.query` | `this.setQueryHandler` | Same concept |
| `Activity.heartbeat` | `this.heartbeat` | Same signature |

### Migration Example

**Temporal Code:**
```typescript
import { proxyActivities } from '@temporalio/workflow';

const activities = proxyActivities<ReturnType<typeof createActivities>>({
  startToCloseTimeout: '10 minutes',
});

export async function orderWorkflow(order: Order): Promise<OrderResult> {
  const validated = await activities.validateOrder(order);
  await activities.reserveInventory(validated.items);
  const payment = await activities.processPayment(validated.payment);
  const shipment = await activities.createShipment(validated.address);
  await activities.sendConfirmation(validated.email, shipment);
  
  return {
    orderId: validated.id,
    paymentId: payment.id,
    shipmentId: shipment.id,
  };
}
```

**Velocity Classic Code:**
```typescript
import { Workflow, Activity } from './index';

class OrderWorkflow extends Workflow {
  static typeName = 'orderWorkflow';
  
  async execute(order: Order): Promise<OrderResult> {
    const validated = await this.executeActivity('validateOrder', order);
    await this.executeActivity('reserveInventory', validated.items);
    const payment = await this.executeActivity('processPayment', validated.payment);
    const shipment = await this.executeActivity('createShipment', validated.address);
    await this.executeActivity('sendConfirmation', { email: validated.email, shipment });
    
    return {
      orderId: validated.id,
      paymentId: payment.id,
      shipmentId: shipment.id,
    };
  }
}
```

### Key Differences

1. **Class-based vs Function-based**
   - Temporal: Functions with `proxyActivities`
   - Velocity: Classes extending `Workflow` and `Activity`

2. **Activity Invocation**
   - Temporal: `activities.myActivity()`
   - Velocity: `this.executeActivity('myActivity', input)`

3. **Type Safety**
   - Temporal: Inferred from proxy
   - Velocity: Explicit generic types

4. **Signal Handling**
   - Temporal: `setHandler(signal, callback)`
   - Velocity: `waitForSignal(signal, callback)`

## Configuration

### Server Configuration (CLI)

```bash
# Basic usage
velocity-classic-server

# With PostgreSQL persistence
velocity-classic-server --postgres "host=localhost port=5432 dbname=velocity user=vel password=vel"

# Custom ports and paths
velocity-classic-server \
  --shmem-path /tmp/velocity-classic.nmcp \
  --ws-bind 0.0.0.0:8083 \
  --wal-path velocity-classic.wal \
  --wal-max-size 67108864 \
  --log-level info \
  --log-format json

# With authentication
VELOCITY_API_KEYS=key1,key2 velocity-classic-server

# With distributed tracing
VELOCITY_OTLP_ENDPOINT=http://localhost:4317 velocity-classic-server

# With TLS/mTLS
velocity-classic-server --tls-cert cert.pem --tls-key key.pem
```

### Environment Variables

```bash
# Database
DATABASE_URL=postgres://velocity:velocity@localhost:5432/velocity

# Logging
VELOCITY_LOG_FORMAT=pretty  # pretty or json

# Metrics
VELOCITY_METRICS_TOKEN=my-token  # Auth for /metrics endpoint

# Authentication
VELOCITY_API_KEYS=key1,key2,key3
VELOCITY_JWT_SECRET=my-jwt-secret
VELOCITY_JWT_ISSUER=velocity
VELOCITY_JWT_AUDIENCE=workflow-clients

# Rate limiting
VELOCITY_RATE_LIMIT_BURST=100
VELOCITY_RATE_LIMIT_RATE=10.0

# Tracing
VELOCITY_OTLP_ENDPOINT=http://localhost:4317
```

## Performance Characteristics

### Throughput

| Workload | ops/s | p50 Latency | p99 Latency | Memory |
|----------|-------|-------------|-------------|--------|
| simple_workflow | 61.54 | 14.51ms | 18.1ms | 9.23 MiB |
| signal_storm | 9.8 | 102ms | 150ms | 10.1 MiB |
| query_burst | 52.3 | 19ms | 28ms | 9.5 MiB |
| high_step | 60.2 | 16ms | 22ms | 9.8 MiB |
| concurrent_100 | 92.1 | 10ms | 15ms | 12.3 MiB |
| throughput_ceiling | 98.7 | 9ms | 14ms | 15.2 MiB |

### Memory Breakdown

```
Base Memory:        5.0 MiB
Worker Overhead:    2.0 MiB
Workflow Instances: 1.5 MiB (100 workflows)
Activity Instances: 0.5 MiB (200 activities)
Task Queue:         0.2 MiB
Total:              9.23 MiB
```

### Optimization Strategies

**Increase Throughput:**
```bash
# Increase WAL batch size and sync interval
velocity-classic-server --wal-batch-size 10 --wal-sync-interval 100
```

**Reduce Latency:**
```bash
# Use shmem IPC for local workers (50-100x faster)
velocity-classic-server --shmem-path /tmp/velocity-classic.nmcp
```

**Reduce Memory:**
```bash
# Limit concurrent workflows
VELOCITY_MAX_WORKFLOWS=50 velocity-classic-server
```

## Deployment

### Docker

```dockerfile
# velocity-classic-ts/Dockerfile
FROM rust:1.88-slim-bookworm AS builder
WORKDIR /build

# Create minimal workspace with only needed crates
RUN echo '[workspace]\nmembers = ["velocity-workflow-core", "velocity-workflow-engine", "velocity-classic", "velocity-classic-server"]\nresolver = "2"\n\n[profile.release]\nopt-level = 2\nlto = false\ncodegen-units = 16\n' > Cargo.toml

COPY velocity-workflow-core ./velocity-workflow-core
COPY velocity-workflow-engine ./velocity-workflow-engine
COPY velocity-classic ./velocity-classic
COPY velocity-classic-server ./velocity-classic-server
COPY migrations ./migrations

RUN cargo build --release -p velocity-classic-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/velocity-classic-server /usr/local/bin/
EXPOSE 8083
CMD ["velocity-classic-server", "--bind", "0.0.0.0:8083"]
```

**Docker Compose:**
```yaml
version: '3.8'
services:
  velocity-classic:
    build:
      context: .
      dockerfile: velocity-classic-ts/Dockerfile
    ports:
      - "18083:8083"
    environment:
      - RUST_LOG=info
      - VELOCITY_LOG_FORMAT=json
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 512M
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: velocity-classic
spec:
  replicas: 1
  selector:
    matchLabels:
      app: velocity-classic
  template:
    metadata:
      labels:
        app: velocity-classic
    spec:
      containers:
      - name: velocity-classic
        image: velocity-classic:latest
        ports:
        - containerPort: 8083
        env:
        - name: RUST_LOG
          value: "info"
        - name: VELOCITY_LOG_FORMAT
          value: "json"
        resources:
          requests:
            cpu: "500m"
            memory: "128Mi"
          limits:
            cpu: "1"
            memory: "256Mi"
```

## Monitoring and Debugging

### Metrics

**Prometheus Metrics:**
```
# NMCP metrics
velocity_nmcp_frames_received_total{transport="shmem"} 12345
velocity_nmcp_frames_received_total{transport="websocket"} 6789
velocity_nmcp_frame_duration_seconds{transport="shmem",quantile="0.5"} 0.00001
velocity_nmcp_frame_duration_seconds{transport="websocket",quantile="0.5"} 0.014
velocity_nmcp_frame_duration_seconds{transport="websocket",quantile="0.99"} 0.018

# Worker metrics
velocity_worker_workflows_active 42
velocity_worker_activities_active 87
velocity_worker_workflows_completed_total 9876
velocity_worker_workflows_failed_total 23

# WAL metrics
velocity_wal_entries_written_total 54321
velocity_wal_bytes_written_total 123456789
```

### Logging

```bash
# Set log level
RUST_LOG=debug velocity-classic-server

# JSON log format
VELOCITY_LOG_FORMAT=json velocity-classic-server

# Component-specific logging
RUST_LOG=velocity_classic_server=debug,velocity_engine=info velocity-classic-server
```

### Debugging

**Enable Debug Mode:**
```bash
RUST_LOG=debug RUST_BACKTRACE=1 velocity-classic-server

# Enable trace logging (very verbose)
RUST_LOG=trace velocity-classic-server
```

**Inspect WAL:**
```bash
# List WAL files
ls -lh velocity-classic.wal*

# Check WAL integrity
velocity-wal-check velocity-classic.wal
```

**Performance Profiling:**
```bash
# CPU profiling
cargo flamegraph --bin velocity-classic-server

# Memory profiling
valgrind --tool=massif target/release/velocity-classic-server
```

## Use Cases

### Ideal Use Cases

1. **Temporal Migration**
   - Existing Temporal workflows
   - Team familiar with Temporal API
   - Gradual migration path

2. **TypeScript-Native Development**
   - TypeScript codebase
   - Want type safety
   - Prefer class-based architecture

3. **Low-Latency Requirements**
   - Real-time workflows
   - Sub-20ms latency needed
   - In-memory performance acceptable

4. **Development and Testing**
   - Fast iteration
   - No external dependencies
   - Easy local development

5. **Edge Cases**
   - Prototyping new workflows
   - Proof of concepts
   - Temporary workflows

### Example: User Registration Workflow

```typescript
class UserRegistrationWorkflow extends Workflow {
  static typeName = 'userRegistration';
  
  async execute(input: RegistrationInput): Promise<RegistrationResult> {
    // Step 1: Validate input
    const validated = await this.executeActivity<ValidatedInput>(
      'validateInput',
      input
    );
    
    // Step 2: Check if user exists
    const exists = await this.executeActivity<boolean>(
      'checkUserExists',
      validated.email
    );
    
    if (exists) {
      throw new Error('User already exists');
    }
    
    // Step 3: Create user
    const user = await this.executeActivity<User>(
      'createUser',
      validated
    );
    
    // Step 4: Send verification email
    await this.executeActivity<void>(
      'sendVerificationEmail',
      { email: user.email, token: user.verificationToken }
    );
    
    // Step 5: Wait for email verification (with timeout)
    const verified = await Promise.race([
      this.waitForSignal('emailVerified'),
      this.sleep(24 * 60 * 60 * 1000),  // 24 hours
    ]);
    
    if (!verified) {
      await this.executeActivity('cleanupUnverifiedUser', user.id);
      throw new Error('Email verification timeout');
    }
    
    // Step 6: Activate user
    await this.executeActivity('activateUser', user.id);
    
    return { userId: user.id, status: 'active' };
  }
}
```

## Migration from Temporal

### Step-by-Step Migration

1. **Install Velocity Classic SDK**
   ```bash
   npm install @velocity/classic-sdk
   ```

2. **Convert Workflow Functions to Classes**
   ```typescript
   // Before (Temporal)
   export async function myWorkflow(input: Input): Promise<Output> {
     const result = await activities.step1(input);
     return result;
   }
   
   // After (Velocity)
   class MyWorkflow extends Workflow {
     static typeName = 'myWorkflow';
     
     async execute(input: Input): Promise<Output> {
       const result = await this.executeActivity('step1', input);
       return result;
     }
   }
   ```

3. **Convert Activity Functions to Classes**
   ```typescript
   // Before (Temporal)
   export async function step1(input: Input): Promise<Output> {
     return { result: 'done' };
   }
   
   // After (Velocity)
   class Step1Activity extends Activity {
     static typeName = 'step1';
     
     async execute(input: Input): Promise<Output> {
       return { result: 'done' };
     }
   }
   ```

4. **Update Worker Setup**
   ```typescript
   // Before (Temporal)
   const worker = await Worker.create({
     taskQueue: 'my-queue',
     workflowsPath: path.join(__dirname, 'workflows'),
     activities: createActivities(),
   });
   
   // After (Velocity)
   const worker = await Worker.create({
     taskQueue: 'my-queue',
   });
   worker.registerWorkflow(MyWorkflow);
   worker.registerActivity(Step1Activity);
   ```

5. **Update Client Code**
   ```typescript
   // Before (Temporal)
   const handle = await client.start('myWorkflow', {
     taskQueue: 'my-queue',
     args: [input],
   });
   
   // After (Velocity)
   const response = await fetch('http://localhost:8083/workflows', {
     method: 'POST',
     body: JSON.stringify({
       workflow_type: 'myWorkflow',
       input,
     }),
   });
   ```

### Migration Checklist

- [ ] Install @velocity/classic-sdk
- [ ] Convert workflow functions to classes
- [ ] Convert activity functions to classes
- [ ] Update worker setup code
- [ ] Update client code to use HTTP API
- [ ] Test workflows locally
- [ ] Deploy to staging
- [ ] Monitor for issues
- [ ] Cut over production traffic

## Limitations and Trade-offs

### Limitations

1. **WAL-Based Durability**
   - Group-commit window (small data loss risk on crash)
   - No ACID transactions (use Embedded for that)

2. **Single Node**
   - No horizontal scaling (yet)
   - Limited by single machine resources
   - Multi-instance possible with PG advisory locks

3. **No SQL Queries**
   - Cannot query workflow state with SQL
   - Limited search/filtering
   - Use Embedded flavor for SQL queries

4. **NMCP Protocol**
   - Newer protocol, less tooling than HTTP/gRPC
   - Shmem paths differ across OS (Linux vs macOS vs Windows)

### Trade-offs

| Aspect | Velocity Classic | Velocity Embedded | Trade-off |
|--------|------------------|-------------------|----------|
| Throughput | **61.54 ops/s** | 61.25 ops/s | WAL vs PostgreSQL overhead |
| Latency | **14.51ms p50** | 14.65ms p50 | No DB roundtrip |
| Memory | 9.23 MiB | **1.25 MiB** | Rust runtime vs minimal |
| Durability | WAL (group-commit) | **ACID (per-step)** | WAL vs PostgreSQL |
| Queryability | None | **Full SQL** | No DB vs PostgreSQL |
| Temporal Compat | **Yes (patterns)** | No | API compatibility |
| Transport | **NMCP shmem** | NMCP shmem | Both use NMCP |

## Conclusion

Velocity Classic is the **best choice for Temporal migration patterns** and low-latency workflows with NMCP transport. The Rust replacement (commit 2bae043) provides better performance, unified toolchain, and WAL durability with optional PostgreSQL.

**Key Strengths:**
- Lowest latency (14.51ms p50)
- Highest throughput (61.54 ops/s, tied with Embedded)
- NMCP shmem IPC (50-100x faster than HTTP)
- Temporal API pattern compatibility
- WAL durability with optional PostgreSQL
- jemalloc allocator for performance

**Key Weaknesses:**
- No ACID transactions (use Embedded)
- No SQL queries (use Embedded)
- NMCP protocol is newer (less tooling)
- Higher memory than Embedded

**Section sources**
- [velocity-classic-server/src/main.rs](file://velocity-classic-server/src/main.rs)
- [velocity-classic-server/Cargo.toml](file://velocity-classic-server/Cargo.toml)
- [velocity-server-bootstrap/src/lib.rs](file://velocity-server-bootstrap/src/lib.rs)
- [velocity-nmcp-protocol/src/lib.rs](file://velocity-nmcp-protocol/src/lib.rs)
