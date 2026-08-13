#!/usr/bin/env node
/**
 * Restate Production Benchmark Client — measures real throughput via HTTP.
 *
 * Sends requests through Restate ingress to benchmark services and measures:
 *   - ops/sec (throughput)
 *   - p50, p99, p999 latency (microseconds)
 *   - error rate
 *
 * Usage:
 *   node client.js [profile]
 *   profile: smoke, standard, stress (default: standard)
 */

const RESTATE_INGRESS = process.env.RESTATE_INGRESS || "http://localhost:8080";

// ─── HTTP Client ─────────────────────────────────────────────────────────────

async function postJSON(url, body, timeoutMs = 120000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: typeof body === "string" ? body : JSON.stringify(body),
      signal: controller.signal,
    });
    const data = await resp.json();
    return { ok: resp.status >= 200 && resp.status < 300, data, status: resp.status };
  } catch (e) {
    return { ok: false, data: null, status: 0, error: e.message };
  } finally {
    clearTimeout(timer);
  }
}

async function getJSON(url) {
  try {
    const resp = await fetch(url);
    return await resp.json();
  } catch {
    return null;
  }
}

// ─── Workload Runner ─────────────────────────────────────────────────────────

async function runWorkload(name, urlFn, payload, count, concurrency = 1) {
  const latencies = [];
  let success = 0;
  let fail = 0;

  const wallStart = performance.now();

  if (concurrency <= 1) {
    for (let i = 0; i < count; i++) {
      const start = performance.now();
      const url = typeof urlFn === "function" ? urlFn(i) : urlFn;
      const result = await postJSON(url, payload);
      const elapsed = (performance.now() - start) * 1000; // ms → µs
      latencies.push(elapsed);
      if (result.ok) success++;
      else fail++;
    }
  } else {
    const sem = { value: concurrency };
    const tasks = [];
    for (let i = 0; i < count; i++) {
      tasks.push(
        (async (idx) => {
          // Simple semaphore
          while (sem.value <= 0) await new Promise((r) => setTimeout(r, 0));
          sem.value--;
          const start = performance.now();
          const url = typeof urlFn === "function" ? urlFn(idx) : urlFn;
          const result = await postJSON(url, payload);
          const elapsed = (performance.now() - start) * 1000;
          latencies.push(elapsed);
          if (result.ok) success++;
          else fail++;
          sem.value++;
        })(i)
      );
    }
    await Promise.all(tasks);
  }

  const wallClock = (performance.now() - wallStart) / 1000; // s
  const opsPerSec = success / wallClock;

  latencies.sort((a, b) => a - b);
  const n = latencies.length;
  const p50 = latencies[Math.floor(n * 0.50)] || 0;
  const p99 = latencies[Math.floor(n * 0.99)] || 0;
  const p999 = latencies[Math.floor(n * 0.999)] || 0;
  const mean = latencies.reduce((a, b) => a + b, 0) / n || 0;

  return {
    name,
    operations: count,
    success_count: success,
    fail_count: fail,
    ops_per_second: Math.round(opsPerSec * 10) / 10,
    latency_p50_us: Math.round(p50 * 10) / 10,
    latency_p99_us: Math.round(p99 * 10) / 10,
    latency_p999_us: Math.round(p999 * 10) / 10,
    latency_mean_us: Math.round(mean * 10) / 10,
  };
}

// ─── Benchmark Suite ─────────────────────────────────────────────────────────

async function runAllBenchmarks(profile = "standard") {
  const mult = { smoke: 0.1, stress: 10.0 }[profile] || 1.0;

  const workloads = [
    // simple_workflow: 10 durable state mutations
    {
      name: "simple_workflow",
      urlFn: (i) => `${RESTATE_INGRESS}/bench/sw_${i}/simple`,
      payload: {},
      count: Math.round(50 * mult),
      concurrency: 1,
    },
    // signal_storm: 50 signal-like state mutations
    {
      name: "signal_storm",
      urlFn: (i) => `${RESTATE_INGRESS}/bench/ss_${i}/signalStorm`,
      payload: { numSignals: 50 },
      count: Math.round(20 * mult),
      concurrency: 1,
    },
    // cold_start: single durable operation
    {
      name: "cold_start",
      urlFn: (i) => `${RESTATE_INGRESS}/bench/cs_${i}/coldStart`,
      payload: {},
      count: Math.round(10 * mult),
      concurrency: 1,
    },
    // multi_step: 100 durable state mutations
    {
      name: "multi_step",
      urlFn: (i) => `${RESTATE_INGRESS}/bench/ms_${i}/multiStep`,
      payload: { steps: 100 },
      count: Math.round(10 * mult),
      concurrency: 1,
    },
    // echo: payload roundtrip (no durable state)
    {
      name: "echo",
      urlFn: (i) => `${RESTATE_INGRESS}/bench/e_${i}/echo`,
      payload: { data: "x".repeat(256) },
      count: Math.round(100 * mult),
      concurrency: 1,
    },
    // payload_1kb: 1KB payload roundtrip
    {
      name: "payload_1kb",
      urlFn: (i) => `${RESTATE_INGRESS}/bench/p_${i}/payload`,
      payload: { data: "x".repeat(1024) },
      count: Math.round(100 * mult),
      concurrency: 1,
    },
    // durable_promise: set + read durable state
    {
      name: "durable_promise",
      urlFn: (i) => `${RESTATE_INGRESS}/bench/dp_${i}/durablePromise`,
      payload: {},
      count: Math.round(50 * mult),
      concurrency: 1,
    },
    // stateful (keyed): per-key counter with durable state
    {
      name: "stateful_keyed",
      urlFn: (i) =>
        `${RESTATE_INGRESS}/keyed_bench/key_${i % 10}/stateful`,
      payload: {},
      count: Math.round(50 * mult),
      concurrency: 1,
    },
    // concurrent_20: 20 parallel keyed workflows
    {
      name: "concurrent_20",
      urlFn: (i) =>
        `${RESTATE_INGRESS}/concurrent_bench/wf_${i}/execute`,
      payload: {},
      count: Math.round(50 * mult),
      concurrency: 20,
    },
  ];

  const results = [];
  for (const w of workloads) {
    process.stdout.write(`  Running ${w.name} (${w.count} ops, concurrency=${w.concurrency})...`);
    const result = await runWorkload(
      w.name,
      w.urlFn || w.url,
      w.payload,
      w.count,
      w.concurrency
    );
    console.log(
      ` -> ${result.ops_per_second} ops/sec, ` +
        `p99=${result.latency_p99_us}µs, ` +
        `success=${result.success_count}/${result.operations}`
    );
    results.push(result);
  }

  return {
    generated_at: new Date().toISOString(),
    engine: "Restate",
    profile,
    workloads: results,
    total_ops: results.reduce((s, r) => s + r.operations, 0),
    total_success: results.reduce((s, r) => s + r.success_count, 0),
    total_fail: results.reduce((s, r) => s + r.fail_count, 0),
  };
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  const profile = process.argv[2] || "standard";
  console.log(`=== Restate Production Benchmark (profile: ${profile}) ===`);
  console.log(`Target: ${RESTATE_INGRESS}`);
  console.log();

  // Health check — verify Restate ingress is reachable
  const health = await getJSON(`${RESTATE_INGRESS}/health`);
  if (!health) {
    // Restate may not have /health — try a test invocation instead
    const test = await postJSON(`${RESTATE_INGRESS}/bench/health_0/invoke`, {});
    if (!test.ok) {
      console.error("ERROR: Cannot connect to Restate ingress");
      console.error("Make sure Restate is running and service is registered.");
      process.exit(1);
    }
    console.log("Restate ingress: OK (verified via test invocation)");
  } else {
    console.log("Restate health:", health);
  }
  console.log();

  const report = await runAllBenchmarks(profile);

  // Write results
  const fs = require("fs");
  const outputPath = "/tmp/restate_bench_results.json";
  fs.writeFileSync(outputPath, JSON.stringify(report, null, 2));

  console.log();
  console.log(`Results written to ${outputPath}`);
  console.log(
    `Summary: ${report.workloads.length} workloads, ` +
      `${report.total_success}/${report.total_ops} success, ` +
      `${report.total_fail} failures`
  );
  console.log();
  for (const w of report.workloads) {
    const errRate =
      w.operations > 0 ? ((w.fail_count / w.operations) * 100).toFixed(1) : "0.0";
    console.log(
      `  ${w.name}: ${w.ops_per_second} ops/sec, ` +
        `p99=${w.latency_p99_us}µs, ` +
        `errors=${errRate}%`
    );
  }
}

main().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
