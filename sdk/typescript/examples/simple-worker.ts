/**
 * Example: Simple task worker using the VELOCITY-WorkFlow TypeScript SDK.
 *
 * Demonstrates:
 *   - Connecting to the VELOCITY-WorkFlow server
 *   - Registering for a task queue
 *   - Polling for tasks in a loop
 *   - Executing task logic
 *   - Error handling
 *   - Graceful shutdown on SIGINT / SIGTERM
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *
 *   2. Install dependencies:
 *      cd VELOCITY-WorkFlow/sdk/typescript && npm install
 *
 *   3. Run this worker:
 *      npx ts-node examples/simple-worker.ts
 */

import { VelocityClient, VelocityError } from '../src';

// ── Configuration ────────────────────────────────────────────────────────
const SERVER_ADDR = 'localhost:50051';
const TASK_QUEUE = 'orders';
const POLL_INTERVAL_MS = 1000;

// ── Graceful shutdown ────────────────────────────────────────────────────
let shutdownRequested = false;

process.on('SIGINT', () => {
  console.log('[worker] Received SIGINT — shutting down gracefully...');
  shutdownRequested = true;
});

process.on('SIGTERM', () => {
  console.log('[worker] Received SIGTERM — shutting down gracefully...');
  shutdownRequested = true;
});

// ── Task handlers ────────────────────────────────────────────────────────
interface Task {
  workflow_key: string;
  workflow_type: string;
  input: string;
}

type TaskHandler = (task: Task) => Promise<Record<string, unknown>>;

async function processOrder(task: Task): Promise<Record<string, unknown>> {
  const payload = JSON.parse(task.input || '{}');
  const orderId = payload.order_id ?? 'unknown';
  console.log(`[worker] Processing order ${orderId}`);
  // Simulate async work
  await new Promise((resolve) => setTimeout(resolve, 50));
  return { status: 'shipped', order_id: orderId };
}

const TASK_HANDLERS: Record<string, TaskHandler> = {
  'order-processing': processOrder,
};

// ── Worker loop ──────────────────────────────────────────────────────────
async function runWorker(): Promise<void> {
  console.log('[worker] Starting VELOCITY-WorkFlow TypeScript worker');
  console.log(`[worker] Server: ${SERVER_ADDR} | Queue: ${TASK_QUEUE}`);

  const client = new VelocityClient(SERVER_ADDR);

  try {
    const connected = await client.connect();
    if (!connected) throw new Error('Failed to connect to server');
    console.log(`[worker] Registered on task queue '${TASK_QUEUE}'`);

    while (!shutdownRequested) {
      try {
        const task: Task | null = await client.pollTask(TASK_QUEUE, 2000);

        if (task === null) {
          await sleep(POLL_INTERVAL_MS);
          continue;
        }

        const handler = TASK_HANDLERS[task.workflow_type];
        if (!handler) {
          console.warn(`[worker] No handler for task type '${task.workflow_type}' — skipping`);
          await client.failTask(task.workflow_key, `No handler for ${task.workflow_type}`);
          continue;
        }

        const result = await handler(task);
        await client.completeWorkflow(task.workflow_key, JSON.stringify(result));
        console.log(`[worker] Task '${task.workflow_type}' completed successfully`);
      } catch (err) {
        if (err instanceof VelocityError) {
          console.error(`[worker] Velocity error: ${err.message}`);
        } else {
          console.error(`[worker] Unexpected error:`, err);
        }
        await sleep(POLL_INTERVAL_MS);
      }
    }
  } catch (err) {
    console.error('[worker] Fatal error:', err);
  } finally {
    await client.close();
    console.log('[worker] Shut down cleanly');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

runWorker().catch(console.error);
