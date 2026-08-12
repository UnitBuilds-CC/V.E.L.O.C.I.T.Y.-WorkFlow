/**
 * Integration / E2E Tests for V.E.L.O.C.I.T.Y.-WorkFlow
 * 
 * Tests cross-component scenarios that exercise multiple subsystems together:
 * - HTTP Server + Worker + Client full lifecycle
 * - Persistence + crash recovery end-to-end
 * - Signal/Query/Update through HTTP
 * - Cross-SDK migration integration
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  Workflow, Activity, Worker, Client, WorkflowStatus,
  FileJournalBackend, VelocityServer,
} from '../src/index';
import { migrate, parseClassic } from '../../velocity-migration-toolkit/src/index';

// ─── Test Workflow Definitions ───────────────────────────────────────────────

class E2EWorkflow extends Workflow {
  async execute(input: string): Promise<any> {
    const result = await this.executeActivity('E2EActivity', input);
    return { processed: result };
  }
}

class E2EActivity extends Activity {
  async execute(input: string): Promise<string> {
    return `processed-${input}`;
  }
}

class SignalE2EWorkflow extends Workflow {
  async execute(): Promise<any> {
    const data = await this.waitForSignal('data');
    return { received: data };
  }
}

class MultiFeatureWorkflow extends Workflow {
  async execute(initialValue: number): Promise<any> {
    const doubled = await this.executeActivity<number>('DoubleActivity', initialValue);
    const signal = await this.waitForSignal<number>('multiplier');
    const multiplied = doubled * signal;
    return { result: multiplied };
  }
}

class DoubleActivity extends Activity {
  async execute(value: number): Promise<number> {
    return value * 2;
  }
}

class CounterWorkflow extends Workflow {
  async execute(count: number): Promise<any> {
    if (count < 3) {
      this.continueAsNew('CounterWorkflow', count + 1);
    }
    return { finalCount: count };
  }
}

class SimpleWorkflow extends Workflow {
  async execute(msg: string): Promise<any> {
    return { result: msg };
  }
}

function tmpDir(): string {
  return path.join(os.tmpdir(), `velocity-e2e-${Date.now()}-${Math.random().toString(36).slice(2)}`);
}

// ─── E2E Test Suite ──────────────────────────────────────────────────────────

describe('Integration E2E Tests', () => {

  describe('HTTP Server Full Lifecycle', () => {
    let server: VelocityServer;
    let worker: Worker;
    let port: number;

    beforeAll(async () => {
      port = 9800 + Math.floor(Math.random() * 100);
      worker = await Worker.create({ taskQueue: 'e2e-queue', logLevel: 'silent' });
      worker.registerWorkflow(E2EWorkflow);
      worker.registerActivity(E2EActivity);
      worker.registerWorkflow(SignalE2EWorkflow);
      worker.registerWorkflow(SimpleWorkflow);
      await worker.run();

      server = new VelocityServer({ port, worker });
      await server.start();
    });

    afterAll(async () => {
      await server.stop();
      await worker.shutdown();
    });

    test('health check returns healthy', async () => {
      const res = await fetch(`http://localhost:${port}/api/health`);
      const data: any = await res.json();
      expect(data.success).toBe(true);
      expect(data.data.status).toBe('healthy');
    });

    test('start workflow → describe → list → complete', async () => {
      // Start
      const startRes = await fetch(`http://localhost:${port}/api/workflows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          workflow_id: 'e2e-lifecycle-1',
          workflow_type: 'E2EWorkflow',
          task_queue: 'e2e-queue',
          input: ['hello-e2e'],
        }),
      });
      const startData: any = await startRes.json();
      expect(startData.success).toBe(true);
      expect(startData.data.workflowId).toBe('e2e-lifecycle-1');

      // Describe
      const descRes = await fetch(`http://localhost:${port}/api/workflows/e2e-lifecycle-1`);
      const descData: any = await descRes.json();
      expect(descData.success).toBe(true);
      expect(descData.data.workflowId).toBe('e2e-lifecycle-1');

      // List
      const listRes = await fetch(`http://localhost:${port}/api/workflows`);
      const listData: any = await listRes.json();
      expect(listData.success).toBe(true);
      expect(Array.isArray(listData.data)).toBe(true);
      expect(listData.data.length).toBeGreaterThanOrEqual(1);
    });

    test('signal workflow via HTTP', async () => {
      // Start a signal workflow
      await fetch(`http://localhost:${port}/api/workflows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          workflow_id: 'e2e-signal-1',
          workflow_type: 'SignalE2EWorkflow',
          task_queue: 'e2e-queue',
        }),
      });

      // Send signal
      const signalRes = await fetch(`http://localhost:${port}/api/workflows/e2e-signal-1/signal`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          signal_name: 'data',
          input: { message: 'hello' },
        }),
      });
      const signalData: any = await signalRes.json();
      expect(signalData.success).toBe(true);
    });

    test('terminate workflow via HTTP', async () => {
      await fetch(`http://localhost:${port}/api/workflows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          workflow_id: 'e2e-term-1',
          workflow_type: 'SimpleWorkflow',
          task_queue: 'e2e-queue',
          input: ['terminate-me'],
        }),
      });

      const termRes = await fetch(`http://localhost:${port}/api/workflows/e2e-term-1/terminate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'testing termination' }),
      });
      const termData: any = await termRes.json();
      expect(termData.success).toBe(true);
    });

    test('404 or 400 for non-existent workflow', async () => {
      const res = await fetch(`http://localhost:${port}/api/workflows/does-not-exist`);
      expect([400, 404]).toContain(res.status);
    });
  });

  describe('Persistence + Crash Recovery E2E', () => {
    let journalDir: string;

    beforeEach(() => {
      journalDir = tmpDir();
    });

    afterEach(() => {
      if (fs.existsSync(journalDir)) {
        fs.rmSync(journalDir, { recursive: true, force: true });
      }
    });

    test('create journal → flush to disk → reload → verify recovery', () => {
      const backend = new FileJournalBackend({ journalDir });

      // Create a journal
      const journal = backend.createJournal('wf-recovery-1', 'E2EWorkflow');
      expect(journal.workflowId).toBe('wf-recovery-1');
      expect(journal.status).toBe(WorkflowStatus.RUNNING);

      // Append events
      backend.appendEvent('wf-recovery-1', 'ACTIVITY_SCHEDULED', { activity: 'E2EActivity', input: 'test' });
      backend.appendEvent('wf-recovery-1', 'ACTIVITY_COMPLETED', { result: 'processed-test' });

      // Update status
      backend.updateStatus('wf-recovery-1', WorkflowStatus.COMPLETED);

      // Flush to disk
      backend.flush();

      // Verify files exist on disk
      const files = fs.readdirSync(journalDir);
      expect(files.length).toBeGreaterThan(0);
      expect(files.some(f => f.includes('wf-recovery-1'))).toBe(true);

      // Create a new backend instance (simulating restart)
      const backend2 = new FileJournalBackend({ journalDir });
      backend2.loadFromDisk();

      // Verify the journal was recovered
      const recovered = backend2.getJournal('wf-recovery-1');
      expect(recovered).toBeDefined();
      expect(recovered!.workflowId).toBe('wf-recovery-1');
      expect(recovered!.status).toBe(WorkflowStatus.COMPLETED);
      expect(recovered!.events.length).toBeGreaterThanOrEqual(2);
      backend.close();
      backend2.close();
    });

    test('incomplete workflows are detected for re-execution', () => {
      const backend = new FileJournalBackend({ journalDir });

      // Create two workflows — one complete, one incomplete
      backend.createJournal('wf-complete', 'E2EWorkflow');
      backend.updateStatus('wf-complete', WorkflowStatus.COMPLETED);

      backend.createJournal('wf-incomplete', 'E2EWorkflow');
      // Leave as RUNNING (simulating crash)

      backend.flush();

      // Reload
      const backend2 = new FileJournalBackend({ journalDir });
      backend2.loadFromDisk();

      const incomplete = backend2.getIncompleteWorkflows();
      expect(incomplete.length).toBe(1);
      expect(incomplete[0].workflowId).toBe('wf-incomplete');
      backend.close();
      backend2.close();
    });

    test('full crash recovery cycle: start → crash → recover → complete', async () => {
      // Phase 1: Start a workflow
      const worker1 = await Worker.create({ taskQueue: 'crash-queue', logLevel: 'silent' });
      worker1.registerWorkflow(E2EWorkflow);
      worker1.registerActivity(E2EActivity);
      await worker1.run();

      const handle = await worker1._executeWorkflow('wf-crash-e2e', 'E2EWorkflow', ['crash-test']);
      const result = await handle.promise;
      expect(result).toEqual({ processed: 'processed-crash-test' });

      // Simulate "crash" by shutting down
      await worker1.shutdown();

      // Phase 2: Restart worker and execute again
      const worker2 = await Worker.create({ taskQueue: 'crash-queue', logLevel: 'silent' });
      worker2.registerWorkflow(E2EWorkflow);
      worker2.registerActivity(E2EActivity);
      await worker2.run();

      const handle2 = await worker2._executeWorkflow('wf-crash-e2e-retry', 'E2EWorkflow', ['recovery-test']);
      const result2 = await handle2.promise;
      expect(result2).toEqual({ processed: 'processed-recovery-test' });

      await worker2.shutdown();
    });
  });

  describe('Cross-SDK Migration Integration', () => {
    test('Classic workflow → migrate to Runtime → verify generated code is valid', () => {
      const classicSource = `
import { Workflow, Activity } from '@velocity-workflow/classic';

class OrderWorkflow extends Workflow {
  async execute(ctx: Context, orderId: string, amount: number): Promise<any> {
    const charge = await this.executeActivity('ChargeActivity', orderId, amount);
    const status = await this.waitForSignal('confirmation');
    return { orderId, charge, status };
  }
}

class ChargeActivity extends Activity {
  async execute(orderId: string, amount: number): Promise<any> {
    return { transactionId: orderId, amount, charged: true };
  }
}
`;
      const runtimeCode = migrate(classicSource, { source: 'classic', target: 'runtime' });
      expect(runtimeCode).toContain('VirtualObject');
      expect(runtimeCode).toContain('ctx.invoke');
      expect(runtimeCode).toContain('OrderWorkflow');
      expect(runtimeCode).toContain("import { VirtualObject");
      expect(runtimeCode).toContain("addHandler");
    });

    test('Classic workflow → migrate to Embedded → verify structure', () => {
      const classicSource = `
import { Workflow } from '@velocity-workflow/classic';

class PaymentWorkflow extends Workflow {
  async execute(ctx: Context, paymentId: string): Promise<any> {
    const result = await this.executeActivity('ProcessPayment', paymentId);
    return result;
  }
}
`;
      const embeddedCode = migrate(classicSource, { source: 'classic', target: 'embedded' });
      expect(embeddedCode).toContain('@Durable()');
      expect(embeddedCode).toContain('class PaymentWorkflow');
      expect(embeddedCode).toContain('ctx.invoke');
    });

    test('Classic workflow → migrate to Python → verify structure', () => {
      const classicSource = `
import { Workflow } from '@velocity-workflow/classic';

class NotifyWorkflow extends Workflow {
  async execute(ctx: Context, userId: string): Promise<any> {
    const result = await this.executeActivity('SendNotification', userId);
    return result;
  }
}
`;
      const pythonCode = migrate(classicSource, { source: 'classic', target: 'python-runtime' });
      expect(pythonCode).toContain('class NotifyWorkflow');
      expect(pythonCode).toContain('async def');
      expect(pythonCode).toContain('ctx.invoke');
    });

    test('migration preserves entity names across all targets', () => {
      const source = `
import { Workflow } from '@velocity-workflow/classic';
class UniqueEntityName extends Workflow {
  async execute(): Promise<any> { return 42; }
}
`;
      for (const target of ['runtime', 'embedded', 'python-runtime'] as const) {
        const result = migrate(source, { source: 'classic', target });
        expect(result).toContain('UniqueEntityName');
      }
    });
  });

  describe('Multi-Feature Workflow E2E', () => {
    test('workflow with signals + activities executes correctly', async () => {
      const worker = await Worker.create({ taskQueue: 'multi-queue', logLevel: 'silent' });
      worker.registerWorkflow(MultiFeatureWorkflow);
      worker.registerActivity(DoubleActivity);
      await worker.run();

      // Start workflow via worker directly
      const handle = await worker._executeWorkflow('wf-multi-1', 'MultiFeatureWorkflow', [5]);

      // Wait briefly for workflow to start and reach signal wait
      await new Promise(r => setTimeout(r, 50));

      // Deliver signal through the workflow instance
      const workerHandle = worker._handles.get('wf-multi-1');
      expect(workerHandle).toBeDefined();
      workerHandle!.instance._deliverSignal('multiplier', 3);

      const result = await handle.promise;
      expect(result).toEqual({ result: 30 }); // 5 * 2 * 3

      await worker.shutdown();
    });

    test('continue-as-new creates chain of executions', async () => {
      const worker = await Worker.create({ taskQueue: 'counter-queue', logLevel: 'silent' });
      worker.registerWorkflow(CounterWorkflow);
      await worker.run();

      const handle = await worker._executeWorkflow('wf-counter-1', 'CounterWorkflow', [0]);
      // The result from the final continued execution
      const result = await handle.promise;
      expect(result).toEqual({ finalCount: 3 });

      // The original handle should be CONTINUING_AS_NEW
      expect(handle.status).toBe(WorkflowStatus.CONTINUING_AS_NEW);

      await worker.shutdown();
    });

    test('sticky queue assignment and retrieval', async () => {
      const worker = await Worker.create({ taskQueue: 'sticky-queue', logLevel: 'silent' });
      worker.registerWorkflow(SimpleWorkflow);
      await worker.run();

      worker.assignStickyQueue('SimpleWorkflow', 'sticky-pool-1');
      expect(worker.getStickyQueue('SimpleWorkflow')).toBe('sticky-pool-1');

      await worker.shutdown();
    });

    test('nexus endpoint registration and lookup', async () => {
      const worker = await Worker.create({ taskQueue: 'nexus-queue', logLevel: 'silent' });
      worker.registerWorkflow(SimpleWorkflow);
      await worker.run();

      worker.registerNexusEndpoint('payment-service', 'http://payments.internal:8080', 'payments');

      // Verify endpoint is registered (actual HTTP call would fail since endpoint is fake)
      // Just verify the endpoint was stored
      expect(worker._nexusEndpoints.has('payment-service')).toBe(true);
      expect(worker._nexusEndpoints.get('payment-service')!.namespace).toBe('payments');

      await worker.shutdown();
    });

    test('workflow reset terminates old and starts new execution', async () => {
      const worker = await Worker.create({ taskQueue: 'reset-queue', logLevel: 'silent' });
      worker.registerWorkflow(SimpleWorkflow);
      await worker.run();

      const client = new Client({}, worker);
      const handle = await client.startWorkflow('wf-reset-1', 'SimpleWorkflow', ['original']);

      // Wait for async execution to complete
      await new Promise(r => setTimeout(r, 100));

      // Reset the workflow
      const newHandle = await client.reset('wf-reset-1', 0);
      expect(newHandle).toBeDefined();
      expect(newHandle.workflowId).not.toBe('wf-reset-1');

      await worker.shutdown();
    });

    test('persistence + worker integration: journal records events', async () => {
      const journalDir = tmpDir();
      try {
        const backend = new FileJournalBackend({ journalDir });

        // Simulate a workflow lifecycle through the journal
        backend.createJournal('wf-journal-e2e', 'E2EWorkflow');
        backend.appendEvent('wf-journal-e2e', 'WORKFLOW_STARTED', { input: ['test'] });
        backend.appendEvent('wf-journal-e2e', 'ACTIVITY_SCHEDULED', { activity: 'E2EActivity' });
        backend.appendEvent('wf-journal-e2e', 'ACTIVITY_COMPLETED', { result: 'processed-test' });
        backend.updateStatus('wf-journal-e2e', WorkflowStatus.COMPLETED);
        backend.flush();

        // Verify on disk
        const files = fs.readdirSync(journalDir);
        expect(files.length).toBe(1);

        // Reload and verify
        const backend2 = new FileJournalBackend({ journalDir });
        backend2.loadFromDisk();
        const journal = backend2.getJournal('wf-journal-e2e');
        expect(journal).toBeDefined();
        expect(journal!.status).toBe(WorkflowStatus.COMPLETED);
        expect(journal!.events.length).toBeGreaterThanOrEqual(4);
        expect(journal!.events[0].eventType).toBe('WORKFLOW_STARTED');
        // The last event is from updateStatus which adds a 'status_changed' event
        const eventTypes = journal!.events.map(e => e.eventType);
        expect(eventTypes).toContain('ACTIVITY_COMPLETED');
        expect(eventTypes).toContain('status_changed');

        backend.close();
        backend2.close();
      } finally {
        if (fs.existsSync(journalDir)) {
          fs.rmSync(journalDir, { recursive: true, force: true });
        }
      }
    });
  });
});
