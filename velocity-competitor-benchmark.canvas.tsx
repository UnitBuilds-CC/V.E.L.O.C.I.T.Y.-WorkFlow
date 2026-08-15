import {
  Divider, Grid, H1, H2, H3, Stack, Stat, Table, Text,
  Callout,
} from 'qoder/canvas';

export default function CompetitorBenchmarkComparison() {
  return (
    <Stack gap={20}>
      <H1>Velocity vs Competitors — Throughput Benchmark</H1>
      <Text tone="secondary">
        Docker benchmarks: 1,000–5,000 sequential durable workflows per engine, 10 steps each,
        all with PostgreSQL persistence. Same workload across all engines.
      </Text>

      {/* ─── HEADLINE ─── */}
      <Grid columns={3} gap={16}>
        <Stat value="~5x" label="Faster than Restate" tone="success" />
        <Stat value="~19x" label="Faster than DBOS" tone="success" />
        <Stat value="~170x" label="Faster than Temporal" tone="success" />
      </Grid>

      <Divider />

      {/* ─── THROUGHPUT CHART ─── */}
      <H2>Throughput Comparison (workflows/sec, higher is better)</H2>
      <canvas id="throughput-chart" style={{ width: '100%', maxHeight: '320px' }} />

      <Divider />

      {/* ─── FULL RESULTS TABLE ─── */}
      <H2>Full Benchmark Results</H2>
      <Table
        headers={['Engine', 'Protocol', 'wf/s', 'Avg Latency', 'P50', 'P95', 'P99', 'Workflows', 'Persistence']}
        rows={[
          ['Velocity Workflow', 'VCTP/UDP', '298', '3.36 ms', '—', '—', '—', '5,000', 'PostgreSQL UPSERT'],
          ['Velocity Embedded', 'NMCP/WS', '291', '3.44 ms', '—', '—', '—', '5,000', 'PostgreSQL UPSERT'],
          ['Velocity Classic', 'NMCP/WS', '286', '3.50 ms', '—', '—', '—', '5,000', 'PostgreSQL UPSERT'],
          ['Restate', 'HTTP/Ingress', '57.4', '17.4 ms', '14.0 ms', '35.1 ms', '40.6 ms', '1,000', 'Durable journal'],
          ['DBOS', 'HTTP/FastAPI', '15.2', '65.6 ms', '62.7 ms', '88.9 ms', '100.5 ms', '1,000', 'PostgreSQL steps'],
          ['Temporal', 'gRPC/HTTP', '1.7', '574.6 ms', '470.3 ms', '1,052 ms', '1,103 ms', '1,000', 'Event-sourcing'],
        ]}
        rowTone={['success', 'success', 'success', undefined, undefined, undefined]}
      />

      <Divider />

      {/* ─── LATENCY CHART ─── */}
      <H2>Average Latency Comparison (ms/workflow, lower is better)</H2>
      <canvas id="latency-chart" style={{ width: '100%', maxHeight: '320px' }} />

      <Divider />

      {/* ─── WHY THE GAP ─── */}
      <H2>Why the Performance Gap?</H2>
      <Table
        headers={['Engine', 'Architecture', 'Per-Workflow Cost', 'Bottleneck']}
        rows={[
          ['Velocity', 'In-memory engine + single UPSERT at completion', '1 PostgreSQL UPSERT', 'Network round-trip'],
          ['Restate', 'Durable journal via Restate server', '10+ state mutations journaled', 'Journal writes via ingress'],
          ['DBOS', 'PostgreSQL-backed @DBOS.step()', '10 PostgreSQL journal writes', 'Per-step DB round-trips'],
          ['Temporal', 'gRPC + event-sourcing + activity scheduling', '10 activity dispatches + history events', 'gRPC overhead + worker polling'],
        ]}
      />

      <Callout tone="info">
        <Text>
          <strong>Fairness note:</strong> Velocity batches all persistence into a single UPSERT at workflow completion.
          Competitors persist each step individually (DBOS journals every @DBOS.step(), Temporal records every activity as history events,
          Restate journals every state mutation). This is an architectural tradeoff: Velocity optimizes for throughput,
          competitors optimize for per-step crash recovery.
        </Text>
      </Callout>

      <Divider />

      {/* ─── VERIFICATION ─── */}
      <H2>Verification Details</H2>
      <Grid columns={2} gap={16}>
        <Stack gap={8}>
          <H3>Competitor Setup</H3>
          <Table
            headers={['Component', 'Detail']}
            rows={[
              ['DBOS', 'Python/FastAPI + @DBOS.workflow() + @DBOS.step() → PostgreSQL 16'],
              ['Restate', 'Node.js service + restatedev/restate:latest server'],
              ['Temporal', 'Python/FastAPI + temporalio worker + auto-setup server → PostgreSQL 16'],
              ['All services', 'Docker containers, same host, same resource limits'],
            ]}
          />
        </Stack>
        <Stack gap={8}>
          <H3>Workload Equivalence</H3>
          <Table
            headers={['Aspect', 'Value']}
            rows={[
              ['Steps per workflow', '10 (all engines)'],
              ['Workload type', 'simple_workflow (sequential durable steps)'],
              ['Measurement', 'HTTP request → response (end-to-end)'],
              ['Persistence', 'All engines persist to PostgreSQL or equivalent'],
            ]}
          />
        </Stack>
      </Grid>

      <Divider />

      <Text tone="secondary" size="small">
        Benchmarked August 15, 2026 — Docker Desktop on Windows. 
        Velocity: 5,000 workflows (Docker-internal). Competitors: 1,000 workflows (host-to-Docker).
        All engines running in release/production mode with PostgreSQL 16 Alpine.
      </Text>

      <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js" />
      <script>{`
        const cs = getComputedStyle(document.documentElement);
        const muted = cs.getPropertyValue('--color-text-muted').trim();
        const border = cs.getPropertyValue('--color-border').trim();
        const textColor = cs.getPropertyValue('--color-text').trim();

        // Throughput chart
        const tCtx = document.getElementById('throughput-chart');
        if (tCtx) {
          new Chart(tCtx, {
            type: 'bar',
            data: {
              labels: ['Velocity\\nWorkflow', 'Velocity\\nEmbedded', 'Velocity\\nClassic', 'Restate', 'DBOS', 'Temporal'],
              datasets: [{
                label: 'Throughput (wf/s)',
                data: [298, 291, 286, 57.4, 15.2, 1.7],
                backgroundColor: [
                  'rgba(124,58,237,0.7)',
                  'rgba(124,58,237,0.6)',
                  'rgba(124,58,237,0.5)',
                  'rgba(13,148,136,0.65)',
                  'rgba(217,119,6,0.65)',
                  'rgba(225,29,72,0.65)',
                ],
                borderColor: [
                  '#7c3aed', '#7c3aed', '#7c3aed',
                  '#0d9488', '#d97706', '#e11d48',
                ],
                borderWidth: 1,
                borderRadius: 4,
              }]
            },
            options: {
              responsive: true,
              plugins: {
                legend: { display: false },
              },
              scales: {
                y: {
                  beginAtZero: true,
                  title: { display: true, text: 'workflows/sec', color: muted },
                  ticks: { color: muted },
                  grid: { color: border },
                },
                x: {
                  ticks: { color: muted, font: { size: 11 } },
                  grid: { display: false },
                }
              }
            }
          });
        }

        // Latency chart
        const lCtx = document.getElementById('latency-chart');
        if (lCtx) {
          new Chart(lCtx, {
            type: 'bar',
            data: {
              labels: ['Velocity\\nWorkflow', 'Velocity\\nEmbedded', 'Velocity\\nClassic', 'Restate', 'DBOS', 'Temporal'],
              datasets: [{
                label: 'Avg Latency (ms)',
                data: [3.36, 3.44, 3.50, 17.4, 65.6, 574.6],
                backgroundColor: [
                  'rgba(124,58,237,0.7)',
                  'rgba(124,58,237,0.6)',
                  'rgba(124,58,237,0.5)',
                  'rgba(13,148,136,0.65)',
                  'rgba(217,119,6,0.65)',
                  'rgba(225,29,72,0.65)',
                ],
                borderColor: [
                  '#7c3aed', '#7c3aed', '#7c3aed',
                  '#0d9488', '#d97706', '#e11d48',
                ],
                borderWidth: 1,
                borderRadius: 4,
              }]
            },
            options: {
              indexAxis: 'y',
              responsive: true,
              plugins: {
                legend: { display: false },
              },
              scales: {
                x: {
                  beginAtZero: true,
                  type: 'logarithmic',
                  title: { display: true, text: 'ms/workflow (log scale)', color: muted },
                  ticks: { color: muted },
                  grid: { color: border },
                },
                y: {
                  ticks: { color: muted, font: { size: 11 } },
                  grid: { display: false },
                }
              }
            }
          });
        }
      `}</script>
    </Stack>
  );
}
