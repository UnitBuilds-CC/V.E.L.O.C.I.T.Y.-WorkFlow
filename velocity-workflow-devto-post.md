# We Built a Workflow Engine in Rust That Runs on 90 MB of RAM

## Why another workflow engine?

If you've worked with distributed systems, you've probably dealt with Temporal, Restate, or DBOS. They're great at what they do — but they all share a common problem: they're heavy. Java runtimes, external databases, event-sourcing overhead. They work, but they cost you in compute, memory, and operational complexity.

We asked a different question: **what if a workflow engine was built from the ground up like a network protocol — where every byte of allocation matters, every microsecond of latency counts, and crash recovery is a cryptographic guarantee rather than a database transaction?**

That's **Velocity Workflow** — and we just shipped v1.0.0.

---

## The elevator pitch

Velocity is a durable execution engine written in Rust with a zero-allocation hot path. It gives you:

- **Sub-4ms p99 per-step latency** on production hardware (end-to-end HTTP + WAL durability: ~550 workflows/sec, 10-step workflows)
- **~90 MB memory footprint** — fixed-capacity slab pools with zero unbounded growth under sustained load
- **7 language SDKs** — TypeScript, Python, Go, Java, Rust, PHP, Ruby
- **No external database required** — persistence lives in repr(C) slab files with chained SHA-256 Merkle proofs
- **Three deployment flavors** — each designed to directly replace an existing tool

| Flavor | Replaces | Protocol | Use Case |
|--------|----------|----------|----------|
| **Velocity Server** | Temporal | VCTP (zero-copy UDP) | Full production distributed workflows |
| **Velocity Classic** | Temporal (HTTP users) | NMCP (shmem + WebSocket) + HTTP | Teams migrating from Temporal's API |
| **Velocity Embedded** | DBOS | HTTP + PostgreSQL | Single-binary durable execution |

All three share the same zero-allocation engine core.

---

## What makes it different

### 1. Zero-allocation slab allocator

The core engine doesn't use `Vec`, `String`, or `Box` on the hot path. Instead, it uses a `repr(C)` slab allocator with O(1) bitmask delta tracking. Step completion is tracked with a 256-bit vector — two cache lines. Crash recovery is an O(1) pointer read, not a database scan.

```rust
// This is what step completion looks like internally
#[repr(C)]
pub struct SlabHeader {
    pub magic: u32,                 // 4B: "VLCT" magic bytes
    pub schema_version: u32,        // 4B: Schema version ID
    pub workflow_id: u64,           // 8B: Unique workflow instance
    pub run_id: u64,                // 8B: Unique run ID
    pub current_step: u32,          // 4B: Current step index
    pub total_steps: u32,           // 4B: Total planned steps
    pub merkle_root: [u8; 32],      // 32B: SHA-256 integrity proof (chained)
    pub step_bitmask: Bitmask256,   // 32B: O(1) step completion flags
    pub prev_merkle_root: [u8; 32], // 32B: Previous step's Merkle root (chain link)
    // 128 bytes total — fits in two cache lines
}
```

### 2. VCTP — our custom transport protocol

For the production server, we built **VCTP** (Velocity Compact Transport Protocol) — an io_uring zero-copy UDP transport with:

- **HMAC-SHA256 authenticated encryption** — every packet is cryptographically signed
- **AES-256-GCM encryption-in-transit** — authenticated encryption for production traffic (alongside XOR stream cipher for cluster-internal use)
- **64-depth sliding window replay detection** — handles 10M+ replay checks per second
- **Circuit breaker overload protection** — automatic backpressure when the server is saturated
- **AES-256-GCM WAL encryption** with key rotation — your workflow state is encrypted at rest

This isn't gRPC wrapped in TLS. This is a purpose-built protocol designed for workflow traffic patterns.

### 3. Cryptographic state integrity

Every step in a Velocity workflow produces a SHA-256 Merkle root that chains to the previous step's root, forming a tamper-evident hash chain. This means:

- You can **prove** a workflow executed correctly without re-running it
- Tampering with persisted state is **cryptographically detectable**
- Crash recovery doesn't require trusting your storage layer

### 4. Compile-time determinism (C#)

For C# developers, Velocity includes Roslyn source generators that transpile standard `async` methods into deterministic state machines at compile time:

```csharp
[DurableWorkflow]
public class OrderWorkflow
{
    [DurableActivity]
    public async Task<string> ProcessPayment(string orderId)
    {
        // This compiles into a deterministic state machine
        // Every await becomes a durable checkpoint
        var charge = await ChargeCard(orderId);
        var receipt = await GenerateReceipt(charge);
        return receipt;
    }
}
```

The `[DurableWorkflow]` attribute analyzes your code at build time and flags non-deterministic operations before they ever reach production.

---

## Migration is a non-event

We know switching workflow engines is a big deal. That's why we built migration tools:

- **`velocity-migrate --from temporal`** — CLI that transpiles Temporal TypeScript and C# workflows to Velocity SDK calls
- **`velocity-migrate`** — converts between Velocity SDK flavors (e.g., TypeScript gRPC to Python HTTP)

You don't need to rewrite your workflows from scratch.

---

## What v1.0.0 includes

We just tagged v1.0.0. Here's what shipped:

### Distribution
- **Docker images** — multi-arch (amd64 + arm64) on GHCR
- **Native binaries** — Linux, macOS, Windows (5 platforms x 3 server variants = 15 binaries)
- **SDK packages** — npm, PyPI, Maven Central, Go modules
- **Helm chart** — production-ready with autoscaling, network policies, RBAC, pod security

### Production hardening
- 17 Prometheus alert rules (HTTP, VCTP, data integrity, replication lag)
- OpenTelemetry/OTLP distributed tracing (Jaeger, Tempo, Grafana backends)
- Pre-built Grafana dashboards
- 65-point security audit checklist
- 15 operations runbooks for incident response
- Encrypted WAL backup with SHA-256 integrity checksums and S3 upload
- CI-gated benchmark regression thresholds (100K–5M ops/s micro-benchmarks, <30µs p99)

### CI/CD gates
Every PR must pass:
- Benchmark regression gate (15 micro-benchmarks at 100K–5M ops/s minimum)
- Fault injection tests
- Chaos/failure injection tests
- Property-based tests (proptest)
- API hardening tests
- TLS/mTLS E2E tests
- Trivy container scanning

---

## Getting started

**Docker (fastest):**
```bash
docker pull ghcr.io/unitbuilds-cc/velocity-workflow:latest
docker run -p 7234:7234 ghcr.io/unitbuilds-cc/velocity-workflow:latest
```

**TypeScript SDK:**
```bash
npm install @velocity-workflow/sdk
```

```typescript
import { Client } from '@velocity-workflow/sdk';

const client = new Client({ connection: { address: 'localhost:7233' } });
const handle = await client.start({
  workflowId: 'order-123',
  workflowType: 'processOrder',
  taskQueue: 'default',
  input: { orderId: 'abc', amount: 99.99 },
});
// handle.workflowExecution contains { workflowId, runId }
```

**Python SDK:**
```bash
pip install velocity-workflow
```

```python
from velocity import Client, ClientOptions, WorkflowOptions

client = Client(ClientOptions(host_port="localhost:7233"))
execution = client.start_workflow(WorkflowOptions(
    workflow_id="order-123",
    workflow_type="process_order",
    task_queue="default",
    input={"order_id": "abc", "amount": 99.99},
))
# execution.workflow_id contains the started workflow ID
```

**Kubernetes (Helm):**
```bash
helm repo add velocity https://charts.velocity-workflow.io
helm install velocity velocity/velocity --set replicas=3
```

---

## Who is this for?

- **Teams running Temporal** who want lower latency and smaller infrastructure bills
- **Teams evaluating DBOS** who want durable execution without the PostgreSQL lock-in (Embedded flavor uses Postgres, but Server and Classic don't need it)
- **Teams building on Restate** who want authenticated transport and cryptographic state integrity
- **Edge/IoT deployments** where a small memory footprint matters (fixed ~90 MB regardless of total workflow volume)
- **Anyone** who wants a workflow engine that treats performance and security as first-class concerns, not afterthoughts

---

## What's next

We're just getting started. Post-1.0 priorities:

- **Multi-region CRDT convergence** — AWORSet and PNCounter support is in the core; we're building the cross-region replication layer
- **WebAssembly targets** — run workflow logic in the browser or at the edge
- **Visual workflow designer** — web UI for building and monitoring workflows

---

## Links

- **GitHub**: [github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow](https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow)
- **Releases**: [v1.0.0](https://github.com/UnitBuilds-CC/V.E.L.O.C.I.T.Y.-WorkFlow/releases/tag/v1.0.0)
- **License**: Apache 2.0

Velocity is open source and we'd love your feedback. If you're running a workflow engine today, try swapping it out and see what happens. We think you'll be surprised.

---

*Built with Rust. Zero allocations. Zero compromises.*
