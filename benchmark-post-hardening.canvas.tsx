import { Divider, Grid, H1, H2, Stack, Stat, Table, Text } from 'qoder/canvas';

const BEFORE = {
  velocity_wins: 0, temporal_wins: 0, comparable: 18,
  avg_throughput_delta_pct: 0.73, avg_p99_latency_delta_pct: 102.35,
  verdict: "VELOCITY and Temporal are roughly comparable",
};

const AFTER = {
  velocity_wins: 4, temporal_wins: 2, comparable: 12,
  avg_throughput_delta_pct: 7.39, avg_p99_latency_delta_pct: 6.98,
  verdict: "VELOCITY is competitive — faster in some areas",
};

const WORKLOADS = [
  { name: "simple_workflow", v_ops: 6514, t_ops: 5379, v_p99: 970, t_p99: 888, delta: 21.1, verdict: "VELOCITY faster", before_delta: 13.2 },
  { name: "signal_storm", v_ops: 34, t_ops: 29, v_p99: 242, t_p99: 302, delta: 15.1, verdict: "Comparable", before_delta: 0.0 },
  { name: "query_burst", v_ops: 34, t_ops: 36, v_p99: 235, t_p99: 207, delta: -5.4, verdict: "Comparable", before_delta: 0.0 },
  { name: "high_step", v_ops: 755, t_ops: 667, v_p99: 868, t_p99: 892, delta: 13.3, verdict: "Comparable", before_delta: 0.0 },
  { name: "concurrent_1k", v_ops: 9556, t_ops: 8752, v_p99: 5939, t_p99: 6434, delta: 9.2, verdict: "Comparable", before_delta: 0.0 },
  { name: "child_workflows", v_ops: 3493, t_ops: 3142, v_p99: 957, t_p99: 876, delta: 11.2, verdict: "Comparable", before_delta: 0.0 },
  { name: "saga_pattern", v_ops: 3715, t_ops: 2031, v_p99: 838, t_p99: 863, delta: 82.9, verdict: "VELOCITY dominates", before_delta: 0.0 },
  { name: "timer_workflow", v_ops: 3459, t_ops: 3611, v_p99: 903, t_p99: 914, delta: -4.2, verdict: "Comparable", before_delta: 0.0 },
  { name: "search_attributes", v_ops: 4783, t_ops: 6545, v_p99: 927, t_p99: 937, delta: -26.9, verdict: "Temporal faster", before_delta: 0.0 },
  { name: "signal_query_mix", v_ops: 2636, t_ops: 3825, v_p99: 718, t_p99: 891, delta: -31.1, verdict: "Temporal faster", before_delta: 0.0 },
  { name: "batch_operations", v_ops: 6602, t_ops: 5246, v_p99: 937, t_p99: 1007, delta: 25.8, verdict: "VELOCITY faster", before_delta: 0.0 },
  { name: "payload_1kb", v_ops: 5092, t_ops: 5661, v_p99: 952, t_p99: 868, delta: -10.0, verdict: "Comparable", before_delta: 0.0 },
  { name: "payload_1mb", v_ops: 3451, t_ops: 3432, v_p99: 933, t_p99: 938, delta: 0.6, verdict: "Comparable", before_delta: 0.0 },
  { name: "namespace_isolation", v_ops: 5470, t_ops: 4422, v_p99: 979, t_p99: 952, delta: 23.7, verdict: "VELOCITY faster", before_delta: 0.0 },
  { name: "throughput_ceiling", v_ops: 13205, t_ops: 11812, v_p99: 50967, t_p99: 56140, delta: 11.8, verdict: "Comparable", before_delta: 0.0 },
  { name: "memory_scaling", v_ops: 7171, t_ops: 6986, v_p99: 838, t_p99: 823, delta: 2.6, verdict: "Comparable", before_delta: 0.0 },
  { name: "cold_start", v_ops: 74, t_ops: 88, v_p99: 484, t_p99: 171, delta: -15.7, verdict: "Comparable", before_delta: 0.0 },
  { name: "crash_recovery", v_ops: 4437, t_ops: 4068, v_p99: 691, t_p99: 1020, delta: 9.1, verdict: "Comparable", before_delta: 0.0 },
];

function verdictColor(v: string) {
  if (v.includes("dominates") || v.includes("VELOCITY faster")) return "success";
  if (v.includes("Temporal") || v.includes("slower")) return "danger";
  return undefined;
}

export default function BenchmarkReport() {
  const vWins = WORKLOADS.filter(w => w.delta > 15).length;
  const tWins = WORKLOADS.filter(w => w.delta < -15).length;
  const comparable = WORKLOADS.length - vWins - tWins;
  const avgDelta = (WORKLOADS.reduce((s, w) => s + w.delta, 0) / WORKLOADS.length).toFixed(1);
  const signalStormFix = "23,593µs → 242µs";

  return (
    <Stack gap={20}>
      <H1>Benchmark: Post-Hardening vs Baseline</H1>
      <Text tone="secondary" size="small">VELOCITY-WorkFlow vs Temporal — 18 workloads, identical gRPC paths, Aug 11 2026</Text>

      <Divider />

      <Grid columns={4} gap={16}>
        <Stat value={`${AFTER.velocity_wins}`} label="Velocity Wins" tone="success" />
        <Stat value={`${AFTER.temporal_wins}`} label="Temporal Wins" />
        <Stat value={`+${avgDelta}%`} label="Avg Throughput Delta" tone="success" />
        <Stat value={`${AFTER.comparable}`} label="Comparable" />
      </Grid>

      <Divider />

      <H2>Before vs After Summary</H2>
      <Grid columns={3} gap={16}>
        <Stat value={`${BEFORE.velocity_wins} → ${AFTER.velocity_wins}`} label="Velocity Wins" tone="success" />
        <Stat value={`+${BEFORE.avg_throughput_delta_pct}% → +${AFTER.avg_throughput_delta_pct}%`} label="Avg Throughput Δ" tone="success" />
        <Stat value={`${BEFORE.avg_p99_latency_delta_pct}% → ${AFTER.avg_p99_latency_delta_pct}%`} label="Avg p99 Latency Δ" tone="success" />
      </Grid>

      <Divider />

      <H2>Key Improvements</H2>
      <Grid columns={2} gap={16}>
        <Stat value={signalStormFix} label="Signal Storm p99 Fix" tone="success" />
        <Stat value="+82.9%" label="Saga Pattern Throughput" tone="success" />
      </Grid>

      <Divider />

      <H2>All Workloads — Throughput Comparison</H2>
      <Table
        headers={['Workload', 'VELOCITY ops/s', 'Temporal ops/s', 'Δ Throughput', 'Verdict']}
        rows={WORKLOADS.map(w => [
          w.name,
          w.v_ops.toLocaleString(),
          w.t_ops.toLocaleString(),
          `${w.delta > 0 ? '+' : ''}${w.delta.toFixed(1)}%`,
          w.verdict,
        ])}
        rowTone={WORKLOADS.map(w => verdictColor(w.verdict))}
      />

      <Divider />

      <H2>p99 Latency (µs)</H2>
      <Table
        headers={['Workload', 'VELOCITY p99', 'Temporal p99', 'Δ Latency']}
        rows={WORKLOADS.map(w => [
          w.name,
          w.v_p99.toLocaleString() + 'µs',
          w.t_p99.toLocaleString() + 'µs',
          `${((w.v_p99 / w.t_p99 - 1) * 100).toFixed(1)}%`,
        ])}
        rowTone={WORKLOADS.map(w => {
          const delta = (w.v_p99 / w.t_p99 - 1) * 100;
          if (delta < -5) return "success";
          if (delta > 15) return "danger";
          return undefined;
        })}
      />

      <Divider />

      <H2>Verdict Distribution</H2>
      <Grid columns={3} gap={16}>
        <Stat value={`${vWins}`} label="Velocity Wins (Δ > 15%)" tone="success" />
        <Stat value={`${tWins}`} label="Temporal Wins (Δ < -15%)" tone="danger" />
        <Stat value={`${comparable}`} label="Comparable" />
      </Grid>

      <Divider />

      <Text tone="secondary" size="small">
        Benchmark harness: velocity-bench v0.1.0 — 18 workloads, Standard profile, both engines via identical gRPC BenchmarkService proto.
        VELOCITY dev-server v0.1.0 (in-memory, 4 shards) vs Temporal Bridge v0.2.0 (event-sourcing simulation, O(N) replay).
        Signal storm p99 regression from prior session fixed via key rotation restructure.
      </Text>
    </Stack>
  );
}
