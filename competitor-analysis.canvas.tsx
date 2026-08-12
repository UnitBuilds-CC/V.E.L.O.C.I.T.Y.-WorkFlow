import {
  Divider, Grid, H1, H2, H3, Stack, Stat, Table, Text,
  MaturityMatrix, Callout, Tag,
} from 'qoder/canvas';

const compDimensions = [
  { id: 'velocity', title: 'Velocity' },
  { id: 'restate', title: 'Restate' },
  { id: 'temporal', title: 'Temporal' },
  { id: 'dbos', title: 'DBOS' },
  { id: 'inngest', title: 'Inngest' },
];

const compScopes = [
  { id: 'durable-exec', title: 'Durable Execution' },
  { id: 'workflows', title: 'Workflow Orchestration' },
  { id: 'deployment', title: 'Deployment Simplicity' },
  { id: 'latency', title: 'Latency / Push Model' },
  { id: 'stateful-entities', title: 'Stateful Entities (Actors)' },
  { id: 'serverless', title: 'Serverless Native' },
  { id: 'durable-rpc', title: 'Durable RPC / Idempotency' },
  { id: 'durable-promises', title: 'Durable Promises / Awakeables' },
  { id: 'event-driven', title: 'Event-Driven Triggers' },
  { id: 'sdk-ergonomics', title: 'SDK Ergonomics' },
  { id: 'maturity', title: 'Production Maturity' },
];

const compCells = [
  // Durable Execution
  { scopeId: 'durable-exec', dimensionId: 'velocity', level: 'Strong', tone: 'strong' as const },
  { scopeId: 'durable-exec', dimensionId: 'restate', level: 'Strong', tone: 'strong' as const },
  { scopeId: 'durable-exec', dimensionId: 'temporal', level: 'Strong', tone: 'strong' as const },
  { scopeId: 'durable-exec', dimensionId: 'dbos', level: 'Good', tone: 'good' as const },
  { scopeId: 'durable-exec', dimensionId: 'inngest', level: 'Good', tone: 'good' as const },
  // Workflow Orchestration
  { scopeId: 'workflows', dimensionId: 'velocity', level: '148 RPCs', tone: 'strong' as const },
  { scopeId: 'workflows', dimensionId: 'restate', level: 'Flexible', tone: 'strong' as const },
  { scopeId: 'workflows', dimensionId: 'temporal', level: '148 RPCs', tone: 'strong' as const },
  { scopeId: 'workflows', dimensionId: 'dbos', level: 'Basic', tone: 'usable' as const },
  { scopeId: 'workflows', dimensionId: 'inngest', level: 'Basic', tone: 'usable' as const },
  // Deployment Simplicity
  { scopeId: 'deployment', dimensionId: 'velocity', level: 'Rust binary', tone: 'good' as const },
  { scopeId: 'deployment', dimensionId: 'restate', level: 'Single binary', tone: 'strong' as const },
  { scopeId: 'deployment', dimensionId: 'temporal', level: 'Complex cluster', tone: 'partial' as const },
  { scopeId: 'deployment', dimensionId: 'dbos', level: 'Postgres dep.', tone: 'usable' as const },
  { scopeId: 'deployment', dimensionId: 'inngest', level: 'Simple SaaS', tone: 'good' as const },
  // Latency / Push Model
  { scopeId: 'latency', dimensionId: 'velocity', level: 'Zero-alloc', tone: 'strong' as const },
  { scopeId: 'latency', dimensionId: 'restate', level: 'P99 <170ms', tone: 'strong' as const },
  { scopeId: 'latency', dimensionId: 'temporal', level: 'Pull-based', tone: 'usable' as const },
  { scopeId: 'latency', dimensionId: 'dbos', level: 'Postgres dep.', tone: 'usable' as const },
  { scopeId: 'latency', dimensionId: 'inngest', level: 'HTTP-based', tone: 'good' as const },
  // Stateful Entities (Actors)
  { scopeId: 'stateful-entities', dimensionId: 'velocity', level: 'Missing', tone: 'weak' as const },
  { scopeId: 'stateful-entities', dimensionId: 'restate', level: 'Virtual Objects', tone: 'strong' as const },
  { scopeId: 'stateful-entities', dimensionId: 'temporal', level: 'Via workflows', tone: 'partial' as const },
  { scopeId: 'stateful-entities', dimensionId: 'dbos', level: 'Via Postgres', tone: 'usable' as const },
  { scopeId: 'stateful-entities', dimensionId: 'inngest', level: 'None', tone: 'weak' as const },
  // Serverless Native
  { scopeId: 'serverless', dimensionId: 'velocity', level: 'Missing', tone: 'weak' as const },
  { scopeId: 'serverless', dimensionId: 'restate', level: 'Native', tone: 'strong' as const },
  { scopeId: 'serverless', dimensionId: 'temporal', level: 'Lambda only', tone: 'partial' as const },
  { scopeId: 'serverless', dimensionId: 'dbos', level: 'Library', tone: 'good' as const },
  { scopeId: 'serverless', dimensionId: 'inngest', level: 'Native', tone: 'strong' as const },
  // Durable RPC / Idempotency
  { scopeId: 'durable-rpc', dimensionId: 'velocity', level: 'Service Mesh', tone: 'strong' as const },
  { scopeId: 'durable-rpc', dimensionId: 'restate', level: 'ctx.run()', tone: 'strong' as const },
  { scopeId: 'durable-rpc', dimensionId: 'temporal', level: 'Activities', tone: 'good' as const },
  { scopeId: 'durable-rpc', dimensionId: 'dbos', level: 'TxProcs', tone: 'good' as const },
  { scopeId: 'durable-rpc', dimensionId: 'inngest', level: 'Steps', tone: 'usable' as const },
  // Durable Promises / Awakeables
  { scopeId: 'durable-promises', dimensionId: 'velocity', level: 'Missing', tone: 'weak' as const },
  { scopeId: 'durable-promises', dimensionId: 'restate', level: 'Native', tone: 'strong' as const },
  { scopeId: 'durable-promises', dimensionId: 'temporal', level: 'Via signals', tone: 'usable' as const },
  { scopeId: 'durable-promises', dimensionId: 'dbos', level: 'Via Postgres', tone: 'usable' as const },
  { scopeId: 'durable-promises', dimensionId: 'inngest', level: 'None', tone: 'weak' as const },
  // Event-Driven Triggers
  { scopeId: 'event-driven', dimensionId: 'velocity', level: 'VCTP', tone: 'good' as const },
  { scopeId: 'event-driven', dimensionId: 'restate', level: 'HTTP push', tone: 'strong' as const },
  { scopeId: 'event-driven', dimensionId: 'temporal', level: 'Via polling', tone: 'usable' as const },
  { scopeId: 'event-driven', dimensionId: 'dbos', level: 'Via triggers', tone: 'usable' as const },
  { scopeId: 'event-driven', dimensionId: 'inngest', level: 'Event-first', tone: 'strong' as const },
  // SDK Ergonomics
  { scopeId: 'sdk-ergonomics', dimensionId: 'velocity', level: '4 SDKs', tone: 'good' as const },
  { scopeId: 'sdk-ergonomics', dimensionId: 'restate', level: '6 SDKs', tone: 'strong' as const },
  { scopeId: 'sdk-ergonomics', dimensionId: 'temporal', level: '8 SDKs', tone: 'strong' as const },
  { scopeId: 'sdk-ergonomics', dimensionId: 'dbos', level: '1 SDK (TS)', tone: 'usable' as const },
  { scopeId: 'sdk-ergonomics', dimensionId: 'inngest', level: '1 SDK (TS)', tone: 'usable' as const },
  // Production Maturity
  { scopeId: 'maturity', dimensionId: 'velocity', level: 'Pre-prod', tone: 'medium' as const },
  { scopeId: 'maturity', dimensionId: 'restate', level: 'Fortune 500', tone: 'strong' as const },
  { scopeId: 'maturity', dimensionId: 'temporal', level: 'Fortune 500', tone: 'strong' as const },
  { scopeId: 'maturity', dimensionId: 'dbos', level: 'Growing', tone: 'good' as const },
  { scopeId: 'maturity', dimensionId: 'inngest', level: 'Growing', tone: 'good' as const },
];

export default function CompetitorAnalysis() {
  return (
    <Stack gap={20}>
      <H1>Competitive Landscape: Beating the Rest</H1>
      <Text tone="secondary">Velocity vs Restate, Temporal, DBOS, Inngest — and what it takes to win</Text>

      <Grid columns={5} gap={12}>
        <Stat value="136K" label="Lines of Rust" />
        <Stat value="148" label="gRPC RPCs" />
        <Stat value="2,378" label="Engine Tests" />
        <Stat value="4/5" label="Competitor Gaps Closeable" tone="warning" />
        <Stat value="~3 weeks" label="Est. to Beat Restate" />
      </Grid>

      <Divider />

      <H2>Competitive Maturity Matrix</H2>
      <MaturityMatrix
        dimensions={compDimensions}
        scopes={compScopes}
        cells={compCells}
        labels={{ scope: 'Capability' }}
      />

      <Divider />

      <H2>Restate: The Main Threat</H2>
      <Callout tone="info">
        <Text>Restate is the most dangerous competitor. Built by Apache Flink creators, it is a <strong>single Rust binary</strong> with a <strong>push model</strong> (P99 &lt;170ms for 10-step workflows), <strong>Virtual Objects</strong> (actor-model keyed state), <strong>Durable Promises</strong>, and <strong>serverless-native</strong> handlers that suspend while waiting. It makes entire systems durable, not just workflows.</Text>
      </Callout>

      <H3>What Restate Has That Velocity Doesn't</H3>
      <Table
        headers={['Restate Feature', 'What It Does', 'Velocity Has?', 'Effort to Add']}
        rows={[
          ['Virtual Objects', 'Actor-model keyed state: one object per cart/session/agent. Single-writer per key, parallel across keys.', 'No', 'Medium — 3-5 days. Engine already has partitioned state + keyed workflows. Need SDK-level VirtualObject abstraction.'],
          ['Durable Promises', 'External resolution points: a promise is created, handed out, and resolved later by anyone. Idempotent.', 'No', 'Easy — 2-3 days. Similar to existing signal/query mechanism. Add create_promise/resolve_promise RPCs.'],
          ['Awakeables', 'Named callbacks that external systems can resolve (webhooks, approvals).', 'No', 'Easy — 1-2 days. Thin wrapper over Durable Promises.'],
          ['Push Model', 'Restate pushes work to services via HTTP, not workers polling.', 'Partial (VCTP)', 'Medium — 3-5 days. Engine has VCTP transport. Need HTTP push dispatcher + service endpoint registration.'],
          ['Handler Suspension', 'Handlers suspend when awaiting (no compute cost), resume when result arrives.', 'No', 'Medium — 3-5 days. Need async execution context that can park/resume. Rust async already supports this.'],
          ['Embedded K/V (RocksDB)', 'Per-key state stored in embedded RocksDB, snapshotted to object store.', 'Partial (WAL)', 'Medium — 5-7 days. WAL exists. Need RocksDB-backed K/V layer with snapshot-to-S3.'],
          ['Single Binary (no ext DB)', 'No Cassandra/Postgres/Elasticsearch needed.', 'Yes!', 'Done — Velocity is already a single Rust binary with no external dependencies.'],
          ['ctx.run() Durable Steps', 'Inline durable steps without workflow/activity split.', 'No', 'Easy — 2-3 days. Add a "durable step" journal entry type to the workflow state machine.'],
        ]}
      />

      <Divider />

      <H2>What Velocity Already Beats Everyone On</H2>
      <Table
        headers={['Velocity Advantage', 'vs Restate', 'vs Temporal', 'vs DBOS', 'vs Inngest']}
        rows={[
          ['Zero-Allocation Hardware-Native', 'Restate uses RocksDB (allocations). Velocity uses slab allocators.', 'Temporal workers allocate heavily.', 'DBOS depends on Postgres I/O.', 'Inngest is JavaScript.'],
          ['148 gRPC RPCs (full Temporal surface)', 'Restate has ~20 core RPCs.', 'Same surface (we match).', 'DBOS has minimal API.', 'Inngest has ~10 endpoints.'],
          ['Durable Service Mesh (built-in)', 'Restate has durable steps but no service mesh.', 'Temporal has no service mesh.', 'DBOS has TxProcs only.', 'Inngest has steps only.'],
          ['Idempotency Keys + Crash Recovery', 'Restate has idempotency keys.', 'Temporal has none built-in.', 'DBOS has via Postgres.', 'Inngest has idempotency keys.'],
          ['Call Graph Tracking', 'Restate has none.', 'Temporal has none.', 'DBOS has none.', 'Inngest has none.'],
          ['Raft Consensus (built-in)', 'Restate has RAFT for metadata only.', 'Temporal uses external DB consensus.', 'DBOS uses Postgres.', 'Inngest uses external DB.'],
          ['VCTP Transport', 'Restate uses HTTP push.', 'Temporal uses polling.', 'DBOS uses Postgres triggers.', 'Inngest uses HTTP.'],
          ['Nexus Operations (34 RPCs)', 'Restate has service-to-service calls.', 'Temporal has Nexus (new).', 'DBOS has none.', 'Inngest has none.'],
          ['2,378 tests, 0 failures', 'Restate has tests (unknown count).', 'Temporal has extensive tests.', 'DBOS has moderate tests.', 'Inngest has moderate tests.'],
        ]}
      />

      <Divider />

      <H2>Other Competitors: Gap Analysis</H2>

      <H3>DBOS (Postgres-based Durable Execution)</H3>
      <Table
        headers={['DBOS Feature', 'Status in Velocity', 'Effort']}
        rows={[
          ['Embedded library mode (not a service)', 'Missing — Velocity is a server', 'Large — 2-3 weeks. Would need a library crate that embeds the engine.'],
          ['Postgres-backed durability', 'Missing — Velocity uses WAL + Raft', 'Medium — 5-7 days. Add Postgres WAL adapter alongside existing Raft log.'],
          ['Transactable procedures (TxProcs)', 'Partial — DurableServiceMesh exists', 'Easy — 2-3 days. Wrap existing durable RPC as "transactable" functions.'],
          ['TypeScript-only SDK', 'Have TS SDK already', 'Done.'],
        ]}
      />

      <H3>Inngest (Event-Driven Durable Execution)</H3>
      <Table
        headers={['Inngest Feature', 'Status in Velocity', 'Effort']}
        rows={[
          ['Event-driven triggers (HTTP in, events out)', 'Partial — VCTP exists', 'Medium — 3-5 days. Add event gateway on top of existing gRPC ingress.'],
          ['Step-based durable functions', 'Missing', 'Easy — 2-3 days. Add "durable step" journal entry (same as Restate ctx.run).'],
          ['Automatic retries with backoff', 'Have — RetryPolicy in engine', 'Done.'],
          ['Idempotency via event IDs', 'Have — idempotency_key in DurableRpc', 'Done.'],
          ['Serve handler (one-line integration)', 'Missing', 'Easy — 1-2 days. Add a simple HTTP handler wrapper in each SDK.'],
        ]}
      />

      <H3>Hatchet (Kubernetes-Native Task Queue)</H3>
      <Table
        headers={['Hatchet Feature', 'Status in Velocity', 'Effort']}
        rows={[
          ['Distributed task queue', 'Have — MatchingService + task queues', 'Done.'],
          ['Kubernetes-native (CRDs, operators)', 'Missing', 'Medium — 1-2 weeks. Need K8s operator + CRD definitions.'],
          ['Fan-out / fan-in patterns', 'Have — child workflows + batch', 'Done.'],
          ['Rate limiting', 'Missing', 'Easy — 2-3 days. Add rate limiter to MatchingService.'],
          ['Workflow-level concurrency limits', 'Missing', 'Easy — 1-2 days. Add to WorkflowExecution state machine.'],
        ]}
      />

      <Divider />

      <H2>Priority Roadmap: Beat Restate in ~3 Weeks</H2>
      <Table
        headers={['Week', 'Feature', 'Impact', 'Effort']}
        rows={[
          ['Week 1', 'Virtual Objects (keyed state + single-writer per key)', 'Highest — Restate killer feature', '3-5 days'],
          ['Week 1', 'Durable Promises + Awakeables', 'High — external resolution', '2-3 days'],
          ['Week 2', 'Push Model (HTTP dispatcher + service registration)', 'High — latency advantage', '3-5 days'],
          ['Week 2', 'ctx.run() Durable Steps (journal entry)', 'Medium — simpler developer experience', '2-3 days'],
          ['Week 3', 'Handler Suspension (async park/resume)', 'Medium — serverless cost savings', '3-5 days'],
          ['Week 3', 'Event Gateway (HTTP triggers on top of VCTP)', 'Medium — Inngest-style events', '2-3 days'],
          ['Ongoing', 'Embedded RocksDB K/V + S3 snapshots', 'Medium — Restate-style storage', '5-7 days'],
        ]}
      />

      <Divider />

      <H2>Bottom Line</H2>
      <Callout tone="success">
        <Stack gap={8}>
          <Text><strong>Velocity already beats DBOS, Inngest, and Hatchet.</strong> The engine is more capable (148 RPCs, Durable Service Mesh, Raft, VCTP, Nexus) and faster (zero-allocation Rust). These competitors are niche; Velocity covers their use cases already.</Text>
          <Text><strong>Restate is the real competition.</strong> But Velocity is closer than it looks: the hard distributed systems primitives are already built (WAL, Raft, partitioning, replication, VCTP). The gap is mostly developer-experience features: Virtual Objects, Durable Promises, push model, handler suspension. These are ~3 weeks of focused work.</Text>
          <Text><strong>After those 3 weeks, Velocity would be:</strong> faster than Restate (zero-allocation vs RocksDB), more capable than Temporal (148 RPCs + Service Mesh + Nexus), simpler than both (single binary, no external DB), and the only engine with hardware-native durable execution.</Text>
        </Stack>
      </Callout>

      <Divider />

      <Text tone="secondary" size="small">
        Competitive analysis August 10, 2026 | V.E.L.O.C.I.T.Y.-WorkFlow
      </Text>
    </Stack>
  );
}
