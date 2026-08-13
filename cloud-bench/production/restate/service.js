/**
 * Restate Production Benchmark Service — Real durable execution.
 *
 * Exposes the same workload types as velocity-bench via Restate services.
 * Uses both stateless services and keyed virtual objects for stateful workloads.
 *
 * Architecture:
 *   [benchmark client] ──HTTP──► [Restate Ingress :8080] ──► [Service :9080]
 *
 * Restate journals every state mutation to its durable log, providing
 * the same guarantees as Velocity's WAL and DBOS's PostgreSQL commits.
 */

const restate = require("@restatedev/restate-sdk");

// ─── Stateless Benchmark Service ─────────────────────────────────────────────
// Mirrors velocity-bench workloads: simple_workflow, signal_storm, cold_start, etc.

const benchService = restate.object({
  name: "bench",
  handlers: {
    /**
     * simple_workflow: 10 durable state mutations → complete.
     * Each ctx.set() is journaled to Restate's durable log.
     */
    async simple(ctx) {
      for (let i = 0; i < 10; i++) {
        const prev = (await ctx.get(`step_${i}`)) || 0;
        ctx.set(`step_${i}`, prev + 1);
      }
      return { status: "completed", steps: 10 };
    },

    /**
     * signal_storm: simulate N signal processing steps.
     * Each signal is a durable state mutation.
     */
    async signalStorm(ctx, input) {
      const numSignals = (input && input.numSignals) || 100;
      let received = 0;
      for (let i = 0; i < numSignals; i++) {
        const prev = (await ctx.get(`signal_${i}`)) || 0;
        ctx.set(`signal_${i}`, prev + 1);
        received++;
      }
      return { status: "completed", signals_received: received };
    },

    /**
     * cold_start: single durable operation after startup.
     */
    async coldStart(ctx) {
      ctx.set("cold_start_ts", Date.now());
      return { status: "ok", ts: Date.now() };
    },

    /**
     * multi_step: 100 durable state mutations in sequence.
     */
    async multiStep(ctx, input) {
      const steps = (input && input.steps) || 100;
      let result = 0;
      for (let i = 0; i < steps; i++) {
        const prev = (await ctx.get("step_result")) || 0;
        ctx.set("step_result", prev + 1);
        result = prev + 1;
      }
      return { status: "completed", steps_completed: result };
    },

    /**
     * echo: return input as-is (no durable state).
     */
    async echo(ctx, input) {
      return { status: "ok", data: input };
    },

    /**
     * payload: payload roundtrip (no durable state).
     */
    async payload(ctx, input) {
      const size = input
        ? typeof input === "string"
          ? input.length
          : JSON.stringify(input).length
        : 0;
      return { status: "ok", size };
    },

    /**
     * durable_promise: set + read durable state (simulates promise).
     */
    async durablePromise(ctx) {
      ctx.set("promise_result", { resolved: true, ts: Date.now() });
      const result = await ctx.get("promise_result");
      return result || { resolved: false };
    },

    /**
     * invoke: minimal handler invocation (no durable state).
     */
    async invoke(ctx, input) {
      return { status: "ok", ts: Date.now() };
    },
  },
});

// ─── Keyed Virtual Object (Stateful Service) ─────────────────────────────────
// Uses Restate's keyed state for per-key isolation.
// This is Restate's equivalent of Velocity's per-workflow state.

const keyedBenchService = restate.object({
  name: "keyed_bench",
  handlers: {
    /**
     * stateful: per-key counter with durable state.
     * Each key gets its own state, isolated from other keys.
     */
    async stateful(ctx) {
      const count = ((await ctx.get("counter")) || 0) + 1;
      ctx.set("counter", count);
      return { status: "ok", key: ctx.key, count };
    },

    /**
     * invoke: per-key invoke with key tracking.
     */
    async invoke(ctx) {
      const count = ((await ctx.get("invoke_count")) || 0) + 1;
      ctx.set("invoke_count", count);
      return { status: "ok", key: ctx.key, invoke_count: count };
    },

    /**
     * multiStep: per-key multi-step with durable state.
     */
    async multiStep(ctx, input) {
      const steps = (input && input.steps) || 10;
      let result = 0;
      for (let i = 0; i < steps; i++) {
        const prev = (await ctx.get(`step_${i}`)) || 0;
        ctx.set(`step_${i}`, prev + 1);
        result = prev + 1;
      }
      return { status: "completed", key: ctx.key, steps_completed: result };
    },
  },
});

// ─── Concurrent Workflow Service ─────────────────────────────────────────────
// Each invocation gets a unique key for state isolation.

const concurrentService = restate.object({
  name: "concurrent_bench",
  handlers: {
    async execute(ctx) {
      const count = ((await ctx.get("exec_count")) || 0) + 1;
      ctx.set("exec_count", count);
      return { status: "ok", key: ctx.key, result: count * 2 };
    },
  },
});

// ─── Serve ───────────────────────────────────────────────────────────────────
restate.serve({
  services: [benchService, keyedBenchService, concurrentService],
  port: 9080,
});
console.log("Restate production bench service listening on port 9080");
