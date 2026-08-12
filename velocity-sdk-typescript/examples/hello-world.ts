/**
 * Hello World Example
 * 
 * Demonstrates a simple workflow that greets a user.
 */

import { Client, Worker, defineWorkflow, defineActivity, WorkflowContext, ActivityContext } from '../src';

// Define an activity that generates a greeting
defineActivity('greet-activity', async (ctx: ActivityContext, name: string) => {
  console.log(`Activity executing: greeting ${name}`);
  return `Hello, ${name}! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow.`;
});

// Define a workflow that uses the activity
defineWorkflow('greeting-workflow', async (ctx: WorkflowContext, input: { name: string }) => {
  console.log(`Workflow started: ${ctx.workflowId}`);
  
  // In a real implementation, this would execute the activity
  // For now, we'll just return a greeting directly
  const greeting = `Hello, ${input.name}! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow.`;
  
  console.log(`Workflow completed: ${ctx.workflowId}`);
  return greeting;
});

// Worker process
async function runWorker() {
  const worker = new Worker({
    namespace: 'default',
    taskQueue: 'greeting-queue',
  });

  console.log('Starting worker...');
  await worker.start();
}

// Client process
async function runClient() {
  const client = new Client({
    namespace: 'default',
  });

  console.log('Starting workflow...');
  
  const handle = await client.start({
    workflowId: `greeting-${Date.now()}`,
    workflowType: 'greeting-workflow',
    taskQueue: 'greeting-queue',
    input: { name: 'World' },
  });

  console.log(`Workflow started: ${handle.workflowId}`);
  console.log(`Run ID: ${handle.runId}`);

  // Wait for result
  const result = await handle.result();
  console.log(`Workflow result: ${result}`);

  client.close();
}

// Run both worker and client
async function main() {
  // In production, these would be separate processes
  // For this example, we'll run them sequentially
  
  // Start worker in background
  const workerPromise = runWorker();
  
  // Give worker time to start
  await new Promise(resolve => setTimeout(resolve, 1000));
  
  // Run client
  await runClient();
  
  // Stop worker (in production, this would be handled by process signals)
  process.exit(0);
}

main().catch(console.error);
