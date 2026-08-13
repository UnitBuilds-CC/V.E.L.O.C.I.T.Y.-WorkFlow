// Quick Restate benchmark via the local Docker container
const INGRESS = "http://localhost:8082";

async function post(url, body) {
  const start = performance.now();
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const elapsed = performance.now() - start;
  const data = await r.json();
  return { status: r.status, data, elapsed };
}

async function main() {
  // Health
  const h = await fetch(`${INGRESS}/health`).catch(() => null);
  console.log("Restate health:", h ? await h.json() : "via test invocation");

  // simple_workflow
  console.log("\nRunning simple_workflow smoke test...");
  let r = await post(`${INGRESS}/bench/smoke_0/simple`, {});
  console.log(`  HTTP ${r.status}: ${JSON.stringify(r.data)} (${r.elapsed.toFixed(0)}ms)`);

  // cold_start
  console.log("Running cold_start smoke test...");
  r = await post(`${INGRESS}/bench/smoke_1/coldStart`, {});
  console.log(`  HTTP ${r.status}: ${JSON.stringify(r.data)} (${r.elapsed.toFixed(0)}ms)`);

  // echo
  console.log("Running echo smoke test...");
  r = await post(`${INGRESS}/bench/smoke_2/echo`, { data: "x".repeat(256) });
  console.log(`  HTTP ${r.status}: ${JSON.stringify(r.data)} (${r.elapsed.toFixed(0)}ms)`);

  console.log("\nRestate smoke tests complete!");
}

main().catch(console.error);
