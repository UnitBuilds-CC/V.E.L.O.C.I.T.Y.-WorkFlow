/**
 * Example: Parent-child workflow orchestration using the VELOCITY-WorkFlow TypeScript SDK.
 *
 * Demonstrates:
 *   - Starting a parent workflow
 *   - Spawning child workflows from the parent
 *   - Waiting for children to complete
 *   - Aggregating child results in the parent
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *   2. npm install
 *   3. npx ts-node examples/child-workflow.ts
 */

import { VelocityClient, WorkflowStub, WorkflowStatus } from '../src';

async function runChildWorkflow(
  client: VelocityClient,
  childType: string,
  orderId: number,
): Promise<bigint> {
  const stub = new WorkflowStub(client, {
    workflowType: childType,
    namespace: 'default',
    taskQueue: 'children',
  });

  const handle = await stub.start({ orderId });
  console.log(`   Child '${childType}' started: key=${handle.workflowKey}`);

  // Simulate child processing
  await stub.signal('process', {});
  const result = await stub.result<{ childResult: string }>();
  console.log(`   Child '${childType}' completed: ${JSON.stringify(result)}`);

  return handle.workflowKey;
}

async function main(): Promise<void> {
  console.log('=== VELOCITY-WorkFlow TypeScript SDK — Child Workflows ===\n');

  const client = new VelocityClient('localhost:50051');
  await client.connect();

  // 1. Start the parent workflow
  const parentStub = new WorkflowStub(client, {
    workflowType: 'order-orchestrator',
    namespace: 'default',
    taskQueue: 'orchestration',
  });
  const parentHandle = await parentStub.start({ orderId: 1001 });
  console.log(`1. Parent workflow started: key=${parentHandle.workflowKey}`);

  // 2. Spawn child workflows
  console.log('\n2. Spawning child workflows...');
  const childTypes = ['validate-order', 'process-payment', 'arrange-shipping'];
  const childKeys: bigint[] = [];

  for (let i = 0; i < childTypes.length; i++) {
    const key = await runChildWorkflow(client, childTypes[i], 1001 + i);
    childKeys.push(key);
  }

  // 3. Signal parent that all children are done
  console.log('\n3. All children completed — signaling parent...');
  await parentStub.signal('children-complete', { children: childKeys });

  // 4. Wait for parent to complete
  const parentResult = await parentStub.result<{ result: string }>();
  console.log(`4. Parent result: ${JSON.stringify(parentResult)}`);

  await client.close();
  console.log('\n=== Child workflow example finished! ===');
}

main().catch(console.error);
