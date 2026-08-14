/**
 * Restate Benchmark Service — durable execution with log-structured storage.
 *
 * Service name: BenchmarkService (matches prod-bench restate_client.rs)
 *
 * Handlers:
 *   handler_invocation  — basic durable invocation
 *   mixed_operations    — mixed state reads/writes
 *   payload_roundtrip   — payload echo through durable execution
 */
const restate = require("@restatedev/restate-sdk");

const benchmarkService = restate.service({
  name: "BenchmarkService",
  handlers: {
    handler_invocation: async (ctx, input) => {
      return {
        status: "ok",
        handler: "handler_invocation",
        input_size: input
          ? typeof input === "string"
            ? input.length
            : JSON.stringify(input).length
          : 0,
      };
    },

    mixed_operations: async (ctx, input) => {
      // Perform a mix of state reads and writes (like mixed_operations workload)
      const count = ((await ctx.get("op_count")) || 0) + 1;
      ctx.set("op_count", count);

      for (let i = 0; i < 10; i++) {
        const prev = (await ctx.get("step_result")) || 0;
        ctx.set("step_result", prev + 1);
      }

      return {
        status: "ok",
        handler: "mixed_operations",
        ops: count,
        steps: 10,
      };
    },

    payload_roundtrip: async (ctx, input) => {
      const size = input
        ? typeof input === "string"
          ? input.length
          : JSON.stringify(input).length
        : 0;

      // Store and retrieve through durable state
      await ctx.set("last_payload_size", size);
      const stored = await ctx.get("last_payload_size");

      return {
        status: "ok",
        handler: "payload_roundtrip",
        size: size,
        stored_size: stored,
      };
    },
  },
});

// Keyed (virtual object) service for stateful workloads
const keyedService = restate.object({
  name: "KeyedBenchmarkService",
  handlers: {
    stateful: async (ctx, input) => {
      const count = ((await ctx.get("counter")) || 0) + 1;
      ctx.set("counter", count);
      return {
        status: "ok",
        handler: "stateful",
        key: ctx.key,
        count,
      };
    },
    invoke: async (ctx, input) => {
      return {
        status: "ok",
        handler: "invoke",
        key: ctx.key,
      };
    },
  },
});

restate.serve({
  services: [benchmarkService, keyedService],
  port: 9080,
});

console.log("Restate bench service listening on port 9080");
