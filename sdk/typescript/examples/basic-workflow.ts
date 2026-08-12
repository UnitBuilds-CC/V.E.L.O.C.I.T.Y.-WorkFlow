/**
 * Example: Basic workflow with signal and query using the VELOCITY-WorkFlow TypeScript SDK.
 *
 * Demonstrates:
 *   - Starting a workflow
 *   - Sending signals
 *   - Querying workflow state
 *   - Completing the workflow
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *   2. Install dependencies:
 *      cd VELOCITY-WorkFlow/sdk/typescript && npm install
 *   3. Run this example:
 *      npx ts-node examples/basic-workflow.ts
 */

import { VelocityClient, WorkflowStatus, WorkflowStub } from '../src';

async function main(): Promise<void> {
  console.log('=== VELOCITY-WorkFlow TypeScript SDK — Basic Workflow ===\n');

  const client = new VelocityClient('localhost:50051');

  // 1. Connect and verify
  const connected = await client.connect();
  console.log(`1. Connected: ${connected}`);

  // 2. Start a workflow using the typed stub
  const stub = new WorkflowStub(client, {
    workflowType: 'order-processing',
    namespace: 'default',
    taskQueue: 'orders',
  });

  const handle = await stub.start({ orderId: 12345 });
  console.log(`2. Workflow started: key=${handle.workflowKey}`);

  // 3. Send a signal (e.g. payment confirmed)
  await stub.signal('payment-confirmed', { amount: 99.99 });
  console.log('3. Signal sent: payment-confirmed');

  // 4. Query the workflow state
  const state = await stub.query<{ status: string }>('current-state');
  console.log(`4. Queried state: ${JSON.stringify(state)}`);

  // 5. Wait for completion
  const result = await stub.result<{ shipped: boolean }>();
  console.log(`5. Workflow result: ${JSON.stringify(result)}`);

  await client.close();
  console.log('\n=== Basic workflow example finished! ===');
}

main().catch(console.error);
