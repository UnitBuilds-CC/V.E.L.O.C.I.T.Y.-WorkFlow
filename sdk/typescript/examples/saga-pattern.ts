/**
 * Example: Multi-step saga with compensation using the VELOCITY-WorkFlow TypeScript SDK.
 *
 * Demonstrates:
 *   - Defining a saga with compensable steps
 *   - Executing steps in order
 *   - Triggering compensation on failure
 *   - Rolling back completed steps in reverse order
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *   2. npm install
 *   3. npx ts-node examples/saga-pattern.ts
 */

import { VelocityClient, WorkflowStub, WorkflowStatus } from '../src';

// ── Saga step definitions ────────────────────────────────────────────────────

interface SagaStep {
  name: string;
  compensate: string;
}

const STEPS: SagaStep[] = [
  { name: 'reserve_inventory', compensate: 'release_inventory' },
  { name: 'charge_payment',    compensate: 'refund_payment' },
  { name: 'book_shipping',     compensate: 'cancel_shipping' },
  { name: 'send_confirmation', compensate: 'send_cancellation_notice' },
];

async function executeStep(
  stub: WorkflowStub,
  step: SagaStep,
): Promise<boolean> {
  console.log(`   Executing: ${step.name}`);
  await stub.signal(step.name, {});
  return true;
}

async function compensateStep(stub: WorkflowStub, step: SagaStep): Promise<void> {
  console.log(`   Compensating: ${step.compensate}`);
  await stub.signal(step.compensate, {});
}

async function runSaga(
  client: VelocityClient,
  simulateFailureAt?: number,
): Promise<boolean> {
  const stub = new WorkflowStub(client, {
    workflowType: 'order-saga',
    namespace: 'default',
    taskQueue: 'orders',
  });

  const handle = await stub.start({ sagaId: Date.now() });
  console.log(`  Saga started: key=${handle.workflowKey}`);

  const completedSteps: SagaStep[] = [];

  for (let i = 0; i < STEPS.length; i++) {
    if (simulateFailureAt !== undefined && i === simulateFailureAt) {
      console.log(`\n   ✗ Step '${STEPS[i].name}' FAILED — triggering compensation`);
      for (const prev of [...completedSteps].reverse()) {
        await compensateStep(stub, prev);
      }
      await stub.terminate(`Step ${STEPS[i].name} failed`);
      return false;
    }

    const success = await executeStep(stub, STEPS[i]);
    if (success) completedSteps.push(STEPS[i]);
  }

  console.log('   ✓ All saga steps completed successfully');
  return true;
}

async function main(): Promise<void> {
  console.log('=== VELOCITY-WorkFlow TypeScript SDK — Saga Pattern ===\n');

  const client = new VelocityClient('localhost:50051');
  await client.connect();

  // Scenario 1: Happy path
  console.log('Scenario 1: Happy path');
  await runSaga(client);

  // Scenario 2: Payment step fails (index=1)
  console.log('\nScenario 2: Payment step fails (index=1)');
  await runSaga(client, 1);

  await client.close();
  console.log('\n=== Saga examples finished! ===');
}

main().catch(console.error);
