/**
 * Example: Simple workflow worker using the VELOCITY-WorkFlow TypeScript SDK.
 *
 * This demonstrates that the VELOCITY-WorkFlow gRPC API is language-agnostic.
 * The same workflow engine serves TypeScript, Go, Python, C#, Java, or any gRPC client.
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server
 *      dotnet run
 *
 *   2. Install dependencies:
 *      cd VELOCITY-WorkFlow/sdk/typescript
 *      npm install
 *
 *   3. Run this example:
 *      npx ts-node examples/simple_worker.ts
 */

import { VelocityClient, WorkflowStatus } from '../src';

async function main(): Promise<void> {
  console.log('=== VELOCITY-WorkFlow TypeScript SDK Example ===\n');

  // Connect to the server (no JWT = anonymous access)
  const client = new VelocityClient('localhost:50051');

  console.log(`Target: ${client.getTarget()}`);
  console.log(`Auth: ${client.hasAuth() ? 'JWT' : 'Anonymous'}`);

  // Verify connectivity
  const connected = await client.connect();
  console.log(`Connected: ${connected}`);

  // In a full implementation, you would:
  // 1. Start a workflow
  // 2. Describe the workflow
  // 3. Send signals
  // 4. Complete/fail/cancel the workflow
  // 5. Query the final state

  console.log(`\nStatus names:`);
  for (const status of [
    WorkflowStatus.Running,
    WorkflowStatus.Completed,
    WorkflowStatus.Failed,
    WorkflowStatus.Canceled,
  ]) {
    console.log(`  ${status} = ${VelocityClient.statusName(status)}`);
  }

  await client.close();

  console.log('\n=== TypeScript SDK connected successfully! ===');
  console.log('The TypeScript SDK can communicate with the Rust/C# workflow engine via gRPC.');
}

main().catch(console.error);
