import {
  Workflow, Activity, Worker, Client, featureMatrix,
  ContinueAsNewError, NexusOperationError, WorkflowStatus,
} from '../src/index';

// ─── Test Workflows & Activities ─────────────────────────────────────────────

class CounterWorkflow extends Workflow {
  async execute(count: number): Promise<any> {
    if (count <= 0) return { final: 0 };
    if (count >= 3) {
      this.continueAsNew('CounterWorkflow', 0);
    }
    return { count };
  }
}

class UpdatableWorkflow extends Workflow {
  async execute(): Promise<any> {
    this.registerUpdate('setStatus', async (input: any) => {
      return { updated: true, status: input.status };
    });
    await this.sleep(5000);
    return { done: true };
  }
}

class SimpleActivity extends Activity {
  async execute(msg: string): Promise<any> {
    return { msg };
  }
}

// ─── Continue-As-New ─────────────────────────────────────────────────────────

describe('Continue-As-New', () => {
  test('continueAsNew throws ContinueAsNewError', () => {
    const wf = new CounterWorkflow();
    expect(() => wf.continueAsNew('CounterWorkflow', 0)).toThrow(ContinueAsNewError);
  });

  test('worker handles continue-as-new and re-executes', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(CounterWorkflow);
    await worker.run();

    // Workflow with count=5 triggers continueAsNew → re-executes with count=0
    const handle = await worker._executeWorkflow('can-1', 'CounterWorkflow', [5]);
    const result = await handle.promise;
    expect(result).toEqual({ final: 0 });
    // Original handle transitions to CONTINUING_AS_NEW (the continued execution is a new handle)
    expect(handle.status).toBe(WorkflowStatus.CONTINUING_AS_NEW);

    await worker.shutdown();
  });

  test('workflow without continue-as-new completes normally', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(CounterWorkflow);
    await worker.run();

    const handle = await worker._executeWorkflow('can-2', 'CounterWorkflow', [1]);
    const result = await handle.promise;
    expect(result).toEqual({ count: 1 });

    await worker.shutdown();
  });
});

// ─── Signal-With-Start ───────────────────────────────────────────────────────

describe('Signal-With-Start', () => {
  test('signalWithStart starts workflow and delivers signal', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(UpdatableWorkflow);
    await worker.run();

    const client = new Client({}, worker);
    const execution = await client.signalWithStart(
      'sws-1', 'UpdatableWorkflow', 'approval', { approved: true }, []
    );
    expect(execution.workflowId).toBe('sws-1');
    expect(execution.status).toBe(WorkflowStatus.RUNNING);

    await worker.shutdown();
  });

  test('signalWithStart on running workflow just sends signal', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(UpdatableWorkflow);
    await worker.run();

    const client = new Client({}, worker);
    // Start first
    await client.startWorkflow('sws-2', 'UpdatableWorkflow', []);
    await new Promise(r => setTimeout(r, 20));
    // signalWithStart should just signal the running workflow
    const execution = await client.signalWithStart(
      'sws-2', 'UpdatableWorkflow', 'data', { value: 42 }, []
    );
    expect(execution.workflowId).toBe('sws-2');

    await worker.shutdown();
  });
});

// ─── Updates ─────────────────────────────────────────────────────────────────

describe('Updates', () => {
  test('update delivers to handler and returns result', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(UpdatableWorkflow);
    await worker.run();

    const client = new Client({}, worker);
    await client.startWorkflow('upd-1', 'UpdatableWorkflow', []);
    await new Promise(r => setTimeout(r, 20));

    const result = await client.update('upd-1', 'setStatus', { status: 'active' });
    expect(result).toEqual({ updated: true, status: 'active' });

    await worker.shutdown();
  });

  test('update on unknown type throws', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(UpdatableWorkflow);
    await worker.run();

    const client = new Client({}, worker);
    await client.startWorkflow('upd-2', 'UpdatableWorkflow', []);
    await new Promise(r => setTimeout(r, 20));

    await expect(client.update('upd-2', 'unknownType', {})).rejects.toThrow('Unknown update type');

    await worker.shutdown();
  });
});

// ─── Reset ───────────────────────────────────────────────────────────────────

describe('Reset', () => {
  test('reset terminates old workflow and starts new one', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(UpdatableWorkflow);
    await worker.run();

    const client = new Client({}, worker);
    await client.startWorkflow('rst-1', 'UpdatableWorkflow', []);
    await new Promise(r => setTimeout(r, 20));

    const resetExec = await client.reset('rst-1');
    expect(resetExec.workflowId).toContain('rst-1-reset');
    expect(resetExec.status).toBe(WorkflowStatus.RUNNING);

    // Old workflow should be terminated
    const oldExec = await client.describe('rst-1');
    expect(oldExec!.status).toBe(WorkflowStatus.TERMINATED);

    await worker.shutdown();
  });

  test('reset non-existent workflow throws', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    await worker.run();
    const client = new Client({}, worker);

    await expect(client.reset('non-existent')).rejects.toThrow();

    await worker.shutdown();
  });
});

// ─── Sticky Queues ──────────────────────────────────────────────────────────

describe('Sticky Queues', () => {
  test('assign and retrieve sticky queue', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(UpdatableWorkflow);
    await worker.run();

    worker.assignStickyQueue('wf-1', 'worker-abc');
    expect(worker.getStickyQueue('wf-1')).toBe('worker-abc');
    expect(worker.getStickyQueue('wf-unknown')).toBeUndefined();

    await worker.shutdown();
  });

  test('client assigns sticky queue through worker', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    await worker.run();
    const client = new Client({}, worker);

    client.assignStickyQueue('wf-2', 'worker-xyz');
    expect(worker.getStickyQueue('wf-2')).toBe('worker-xyz');

    await worker.shutdown();
  });
});

// ─── Nexus Operations ────────────────────────────────────────────────────────

describe('Nexus Operations', () => {
  test('register nexus endpoint', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    await worker.run();

    worker.registerNexusEndpoint('payments', 'http://payments.ns.svc', 'payments-ns');
    // No error means success
    await worker.shutdown();
  });

  test('execute nexus operation on unregistered endpoint throws', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    await worker.run();

    await expect(
      worker.executeNexusOperation('unknown', 'charge')
    ).rejects.toThrow(NexusOperationError);

    await worker.shutdown();
  });

  test('client registers nexus endpoint through worker', async () => {
    const worker = await Worker.create({ taskQueue: 'test' });
    await worker.run();
    const client = new Client({}, worker);

    client.registerNexusEndpoint('shipping', 'http://shipping.ns.svc', 'shipping-ns');
    // No error means success

    await worker.shutdown();
  });
});

// ─── Feature Matrix ──────────────────────────────────────────────────────────

describe('Feature Matrix — 21/21', () => {
  test('all 21 features are true', () => {
    const matrix = featureMatrix();
    const entries = Object.entries(matrix);
    expect(entries.length).toBe(21);
    for (const [key, value] of entries) {
      expect(value).toBe(true);
    }
  });
});
