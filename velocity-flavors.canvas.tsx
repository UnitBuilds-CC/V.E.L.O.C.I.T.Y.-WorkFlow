import {
  Divider, Grid, H1, H2, H3, Stack, Stat, Table, Text,
  Callout, Tag,
} from 'qoder/canvas';

export default function VelocityFlavors() {
  return (
    <Stack gap={20}>
      <H1>Velocity: Three Flavors, One Engine</H1>
      <Text tone="secondary">Same 136K-line Rust engine. Three distinct developer experiences, each native to its niche.</Text>

      <Grid columns={3} gap={16}>
        <Stat value="Velocity Classic" label="Temporal-compatible" tone="info" />
        <Stat value="Velocity Runtime" label="Restate-compatible" tone="success" />
        <Stat value="Velocity Embedded" label="DBOS-compatible" tone="warning" />
      </Grid>

      <Divider />

      {/* ─── FLAVOR 1: VELOCITY CLASSIC ─── */}
      <H2>Flavor 1: Velocity Classic</H2>
      <Callout tone="info">
        <Text><strong>For:</strong> Teams who already know Temporal, have existing workflows, or need the full orchestration surface. "I want Temporal's power without the operational headache."</Text>
      </Callout>

      <Table
        headers={['Aspect', 'Detail']}
        rows={[
          ['Target user', 'Platform teams, workflow engineers, enterprises migrating from Temporal'],
          ['Mental model', 'Workflows + Activities. Long-running orchestrations driven by signals and queries.'],
          ['Programming model', 'Define workflows as durable functions. Define activities as side-effecting steps. Register both with a worker. Worker polls for tasks.'],
          ['Deployment', 'Single Velocity binary (replaces Temporal cluster + Cassandra + Elasticsearch). Workers are long-running processes.'],
          ['API surface', 'All 148 gRPC RPCs. Full Temporal-compatible proto. Drop-in replacement.'],
          ['SDK languages', 'Go, Python, TypeScript, Java (already built)'],
          ['Key features', 'Signals, Queries, Updates, Child Workflows, Continue-as-New, Schedules, Batch Operations, Search Attributes, Saga, Nexus, Versioning'],
          ['Why choose this over Temporal', 'Single binary. No Cassandra. No Elasticsearch. Zero-allocation engine. 10x lower latency. Same API.'],
          ['Code example', 'See below'],
        ]}
      />

      <H3>Velocity Classic — Code Example (Go)</H3>
      <Table
        headers={['File', 'Code']}
        rows={[
          ['workflow.go', `import "velocity-sdk-go"

func OrderWorkflow(ctx velocity.WorkflowContext, orderID string) error {
    // Activity: charge payment
    err := velocity.ExecuteActivity(ctx, ChargePayment, orderID).Get(ctx, nil)
    if err != nil { return err }

    // Activity: reserve inventory
    err = velocity.ExecuteActivity(ctx, ReserveInventory, orderID).Get(ctx, nil)
    if err != nil { return err }

    // Activity: ship order
    return velocity.ExecuteActivity(ctx, ShipOrder, orderID).Get(ctx, nil)
}`],
          ['worker.go', `func main() {
    w := velocity.NewWorker(velocity.WorkerOptions{
        TaskQueue: "orders",
    })
    w.RegisterWorkflow(OrderWorkflow)
    w.RegisterActivity(ChargePayment)
    w.RegisterActivity(ReserveInventory)
    w.RegisterActivity(ShipOrder)
    w.Run()
}`],
          ['client.go', `func main() {
    c, _ := velocity.NewClient(velocity.ClientOptions{HostPort: "localhost:7233"})
    exec, _ := c.Start(ctx, velocity.WorkflowOptions{
        WorkflowID:   "order-123",
        WorkflowType: "OrderWorkflow",
        TaskQueue:    "orders",
        Input:        "order-123",
    })
    fmt.Println("Started:", exec.WorkflowID)
}`],
        ]}
      />

      <Divider />

      {/* ─── FLAVOR 2: VELOCITY RUNTIME ─── */}
      <H2>Flavor 2: Velocity Runtime</H2>
      <Callout tone="success">
        <Text><strong>For:</strong> Teams building microservices, agent platforms, or event-driven systems who want durability without the workflow ceremony. "I want Restate's simplicity and push model."</Text>
      </Callout>

      <Table
        headers={['Aspect', 'Detail']}
        rows={[
          ['Target user', 'Backend developers, AI/agent platform builders, microservice teams'],
          ['Mental model', 'Durable services. Virtual Objects (actors). Durable Promises. Push-based. Handlers suspend when waiting.'],
          ['Programming model', 'Define services as plain async HTTP handlers. Wrap durable steps in ctx.run(). Virtual Objects hold keyed state. No workflow/activity split.'],
          ['Deployment', 'Single Velocity binary. Services are plain HTTP endpoints (or serverless functions). Runtime pushes work to them.'],
          ['API surface', 'HTTP-based. Service registration endpoint. Invocation via HTTP POST. Virtual Object routing by key.'],
          ['SDK languages', 'TypeScript, Python, Go, Rust, Java, Kotlin (Restate-compatible SDKs)'],
          ['Key features', 'Virtual Objects (keyed state), Durable Promises, Awakeables, ctx.run() durable steps, handler suspension, idempotency keys, push dispatch'],
          ['Why choose this over Restate', 'Same single-binary simplicity. Faster engine (zero-alloc). Full Temporal surface available if needed. Built-in Service Mesh.'],
          ['Code example', 'See below'],
        ]}
      />

      <H3>Velocity Runtime — Code Example (Python)</H3>
      <Table
        headers={['File', 'Code']}
        rows={[
          ['app.py', `from velocity_runtime import VirtualObject, ObjectContext

chat = VirtualObject("ChatAgent")

@chat.handler()
async def message(ctx: ObjectContext, query: str):
    history = await ctx.get("history") or []
    history.append({"role": "user", "content": query})

    # Durable step — survives crashes, replayed on recovery
    result = await ctx.run(lambda: call_llm(history))

    history.append({"role": "assistant", "content": result})
    await ctx.set("history", history)
    return result

app = velocity_runtime.app(services=[chat])`],
          ['Send a message', `curl localhost:8080/ChatAgent/session-42/message \\
    -d '"What is new in AI?"'

# State persists per session-42. Concurrent messages serialize automatically.
# No worker setup. No signals. No event passing.`],
          ['Durable Promise', `# Create a promise (e.g., for webhook callback)
promise = await ctx.promise("approval-order-123")

# Hand the URL to an external system
await notify_approver(promise.id)

# Suspend until someone resolves it
result = await promise.resolve()  # parks here, no compute cost`],
        ]}
      />

      <Divider />

      {/* ─── FLAVOR 3: VELOCITY EMBEDDED ─── */}
      <H2>Flavor 3: Velocity Embedded</H2>
      <Callout tone="warning">
        <Text><strong>For:</strong> Teams who want durable execution embedded directly in their application, backed by Postgres. "I want DBOS's simplicity — just add a library to my existing app."</Text>
      </Callout>

      <Table
        headers={['Aspect', 'Detail']}
        rows={[
          ['Target user', 'Application developers, full-stack engineers, teams with existing Postgres'],
          ['Mental model', 'Durable functions embedded in your app. Transactable procedures. Postgres is the source of truth.'],
          ['Programming model', 'Import velocity-embedded. Wrap functions with @durable. Call them normally. They survive crashes. Postgres stores everything.'],
          ['Deployment', 'Library in your app. Postgres as the durability backend. No separate server needed (optional: Velocity server for multi-node).'],
          ['API surface', 'Library API. @durable decorator. ctx.run() for side effects. Direct Postgres access for state.'],
          ['SDK languages', 'TypeScript (primary, like DBOS), Python, Go'],
          ['Key features', 'Postgres-backed durability, transactable procedures, embedded K/V, direct SQL access, zero-config (just point at Postgres)'],
          ['Why choose this over DBOS', 'Same embedded simplicity. But the Velocity engine is available when you need it (upgrade to full server). Better performance (Rust core).'],
          ['Code example', 'See below'],
        ]}
      />

      <H3>Velocity Embedded — Code Example (TypeScript)</H3>
      <Table
        headers={['File', 'Code']}
        rows={[
          ['app.ts', `import { Velocity, durable } from 'velocity-embedded';

const velocity = new Velocity({ postgresUrl: process.env.DATABASE_URL });

@durable
async function processOrder(orderId: string) {
    // This function is durable — survives crashes, retries automatically
    const payment = await chargePayment(orderId);
    const inventory = await reserveInventory(orderId);
    await shipOrder(orderId);
    return { payment, inventory };
}

// Just call it like a normal function
const result = await velocity.run(processOrder, "order-123");
console.log(result);`],
          ['Upgrade path', `// When you need more power, switch to Velocity Classic or Runtime:
// - Same durable functions work as workflows
// - Same activities work as ctx.run() steps
// - Postgres data migrates to the Velocity engine seamlessly`],
        ]}
      />

      <Divider />

      <H2>Shared Engine, Different Skins</H2>
      <Table
        headers={['Capability', 'Classic (Temporal)', 'Runtime (Restate)', 'Embedded (DBOS)']}
        rows={[
          ['Workflow orchestration', 'Primary', 'Via handlers', 'Via @durable'],
          ['Activity / side-effect execution', 'ExecuteActivity()', 'ctx.run()', '@durable steps'],
          ['State management', 'Workflow variables', 'Virtual Object K/V', 'Postgres tables'],
          ['Inter-service calls', 'Nexus operations', 'Durable RPC', 'Direct function calls'],
          ['Crash recovery', 'Event history replay', 'Journal replay', 'Postgres WAL'],
          ['Push vs Pull', 'Pull (worker polls)', 'Push (HTTP dispatch)', 'Embedded (in-process)'],
          ['External DB needed', 'No (built-in WAL)', 'No (built-in WAL)', 'Yes (Postgres)'],
          ['Serverless support', 'No (long-running workers)', 'Yes (handler suspension)', 'N/A (embedded)'],
          ['Actor model', 'Via signals', 'Virtual Objects (native)', 'Via Postgres rows'],
          ['Upgrade path', 'Can add Runtime features', 'Can add Classic features', 'Can upgrade to Classic/Runtime'],
        ]}
      />

      <Divider />

      <H2>Implementation Effort</H2>
      <Table
        headers={['Flavor', 'What Exists', 'What to Build', 'Est. Effort']}
        rows={[
          ['Classic (Temporal)', '100% — All 148 RPCs, 4 SDKs, 2,378 tests', 'Packaging + docs + branding', '1 week'],
          ['Runtime (Restate)', 'Engine core (WAL, Raft, VCTP, partitioning, DurableServiceMesh)', 'Virtual Objects, Durable Promises, Push dispatcher, Handler suspension, HTTP API, Restate-compatible SDKs', '3-4 weeks'],
          ['Embedded (DBOS)', 'Engine core + Postgres adapter potential', 'Library crate, @durable decorator, Postgres WAL backend, TypeScript SDK', '2-3 weeks'],
        ]}
      />

      <Divider />

      <H2>Why This Works</H2>
      <Callout tone="success">
        <Stack gap={8}>
          <Text><strong>One engine, three markets.</strong> The 136K-line Rust engine is the shared core. Each flavor is a thin API layer targeting a specific developer audience with a specific mental model.</Text>
          <Text><strong>No compromised middle ground.</strong> Instead of building one API that tries to be everything (and satisfies nobody), each flavor is opinionated and excellent at its niche. Classic users get Temporal's full power. Runtime users get Restate's simplicity. Embedded users get DBOS's ease of integration.</Text>
          <Text><strong>Upgrade paths create lock-in.</strong> An Embedded user who outgrows Postgres upgrades to Classic. A Classic user who wants serverless switches to Runtime. All on the same engine. No migration needed.</Text>
          <Text><strong>Velocity Classic is ready today.</strong> The Temporal-compatible flavor is 100% complete with 4 SDKs and 2,378 tests. It can ship now. Runtime and Embedded are engine-feature additions on top of the existing core.</Text>
        </Stack>
      </Callout>

      <Divider />

      <Text tone="secondary" size="small">
        Product strategy — August 10, 2026 | V.E.L.O.C.I.T.Y.-WorkFlow
      </Text>
    </Stack>
  );
}
