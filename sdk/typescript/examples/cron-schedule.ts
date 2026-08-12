/**
 * Example: Scheduled (cron) workflow using the VELOCITY-WorkFlow TypeScript SDK.
 *
 * Demonstrates:
 *   - Registering a cron schedule
 *   - Starting a workflow tied to a cron expression
 *   - Monitoring scheduled executions
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *   2. npm install
 *   3. npx ts-node examples/cron-schedule.ts
 */

import { VelocityClient, WorkflowStub } from '../src';

const CRON_EXPRESSION = '*/5 * * * *'; // Every 5 minutes

async function main(): Promise<void> {
  console.log('=== VELOCITY-WorkFlow TypeScript SDK — Cron Schedule ===\n');

  const client = new VelocityClient('localhost:50051');
  await client.connect();

  // 1. Start a workflow with a cron schedule
  const stub = new WorkflowStub(client, {
    workflowType: 'periodic-report',
    namespace: 'default',
    taskQueue: 'reports',
  });

  const handle = await stub.start({ cron: CRON_EXPRESSION });
  console.log(`1. Scheduled workflow started: key=${handle.workflowKey}`);

  // 2. Send a trigger signal (simulating a cron fire)
  await stub.signal('cron-fire', { fireNumber: 1 });
  console.log('2. Cron fire signal sent');

  // 3. Wait for the scheduled execution to complete
  const result = await stub.result<{ report: string }>();
  console.log(`3. Execution result: ${JSON.stringify(result)}`);

  await client.close();
  console.log('\n=== Cron schedule example finished! ===');
}

main().catch(console.error);
