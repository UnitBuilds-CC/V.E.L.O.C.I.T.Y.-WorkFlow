/**
 * Tests for real workflow execution, activity dispatch, signals, and child workflows.
 */

import {
  Workflow, Activity, Worker, Client,
  WorkflowStatus, WorkflowHandle,
} from '../src/index';

// ─── Test Workflows and Activities ───────────────────────────────────────────

class GreetActivity extends Activity {
  static typeName = 'GreetActivity';
  async execute(name: string): Promise<string> {
    return `Hello, ${name}!`;
  }
}

class DoubleActivity extends Activity {
  static typeName = 'DoubleActivity';
  async execute(n: number): Promise<number> {
    return n * 2;
  }
}

class GreetWorkflow extends Workflow {
  static typeName = 'GreetWorkflow';
  async execute(name: string): Promise<string> {
    const greeting = await this.executeActivity<string>('GreetActivity', name);
    return greeting;
  }
}

class MathWorkflow extends Workflow {
  static typeName = 'MathWorkflow';
  async execute(start: number): Promise<number> {
    const doubled = await this.executeActivity<number>('DoubleActivity', start);
    const quadrupled = await this.executeActivity<number>('DoubleActivity', doubled);
    return quadrupled;
  }
}

class ChildWorkflow extends Workflow {
  static typeName = 'ChildWorkflow';
  async execute(msg: string): Promise<string> {
    return `child:${msg}`;
  }
}

class ParentWorkflow extends Workflow {
  static typeName = 'ParentWorkflow';
  async execute(msg: string): Promise<string> {
    const childResult = await this.executeChildWorkflow<string>('ChildWorkflow', msg);
    return `parent:${childResult}`;
  }
}

class SignalWorkflow extends Workflow {
  static typeName = 'SignalWorkflow';
  async execute(): Promise<string> {
    const approval = await this.waitForSignal<string>('approval');
    return `approved:${approval}`;
  }
}

class BufferedSignalWorkflow extends Workflow {
  static typeName = 'BufferedSignalWorkflow';
  async execute(): Promise<string[]> {
    const items: string[] = [];
    const first = await this.waitForSignal<string>('addItem');
    items.push(first);
    const second = await this.waitForSignal<string>('addItem');
    items.push(second);
    return items;
  }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe('Real Workflow Execution', () => {
  let worker: Worker;

  beforeEach(async () => {
    worker = await Worker.create({ taskQueue: 'test', logLevel: 'silent' });
    await worker.run();
  });

  afterEach(async () => {
    await worker.shutdown();
  });

  test('workflow executes and returns result', async () => {
    worker.registerWorkflow(GreetWorkflow);
    worker.registerActivity(GreetActivity);

    const handle = await worker._executeWorkflow('wf-1', 'GreetWorkflow', ['World']);
    const result = await handle.promise;
    expect(result).toBe('Hello, World!');
    expect(handle.status).toBe(WorkflowStatus.COMPLETED);
  });

  test('workflow executes multiple activities in sequence', async () => {
    worker.registerWorkflow(MathWorkflow);
    worker.registerActivity(DoubleActivity);

    const handle = await worker._executeWorkflow('wf-2', 'MathWorkflow', [5]);
    const result = await handle.promise;
    expect(result).toBe(20); // 5 * 2 = 10, 10 * 2 = 20
  });

  test('workflow failure is tracked', async () => {
    class FailWorkflow extends Workflow {
      static typeName = 'FailWorkflow';
      async execute(): Promise<void> {
        throw new Error('intentional failure');
      }
    }
    worker.registerWorkflow(FailWorkflow);

    const handle = await worker._executeWorkflow('wf-3', 'FailWorkflow', []);
    await expect(handle.promise).rejects.toThrow('intentional failure');
    expect(handle.status).toBe(WorkflowStatus.FAILED);
    expect(handle.error).toBe('intentional failure');
  });

  test('child workflow execution', async () => {
    worker.registerWorkflow(ParentWorkflow);
    worker.registerWorkflow(ChildWorkflow);

    const handle = await worker._executeWorkflow('wf-4', 'ParentWorkflow', ['test']);
    const result = await handle.promise;
    expect(result).toBe('parent:child:test');
  });

  test('unknown activity throws', async () => {
    class BadWorkflow extends Workflow {
      static typeName = 'BadWorkflow';
      async execute(): Promise<void> {
        await this.executeActivity('NonExistent');
      }
    }
    worker.registerWorkflow(BadWorkflow);

    const handle = await worker._executeWorkflow('wf-5', 'BadWorkflow', []);
    await expect(handle.promise).rejects.toThrow();
    expect(handle.status).toBe(WorkflowStatus.FAILED);
  });

  test('unknown workflow type throws', async () => {
    await expect(worker._executeWorkflow('wf-6', 'NonExistent', [])).rejects.toThrow();
  });
});

describe('Real Signal Delivery', () => {
  let worker: Worker;

  beforeEach(async () => {
    worker = await Worker.create({ taskQueue: 'test', logLevel: 'silent' });
    await worker.run();
  });

  afterEach(async () => {
    await worker.shutdown();
  });

  test('signal resumes suspended workflow', async () => {
    worker.registerWorkflow(SignalWorkflow);

    const handle = await worker._executeWorkflow('wf-sig-1', 'SignalWorkflow', []);

    // Workflow is suspended waiting for signal — deliver it
    worker._signalWorkflow('wf-sig-1', 'approval', 'yes');

    const result = await handle.promise;
    expect(result).toBe('approved:yes');
    expect(handle.status).toBe(WorkflowStatus.COMPLETED);
  });

  test('buffered signal is consumed immediately', async () => {
    worker.registerWorkflow(BufferedSignalWorkflow);

    const handle = await worker._executeWorkflow('wf-sig-2', 'BufferedSignalWorkflow', []);

    // Deliver two signals
    worker._signalWorkflow('wf-sig-2', 'addItem', 'first');
    worker._signalWorkflow('wf-sig-2', 'addItem', 'second');

    const result = await handle.promise;
    expect(result).toEqual(['first', 'second']);
  });

  test('signal before waitForSignal is buffered', async () => {
    worker.registerWorkflow(SignalWorkflow);

    const handle = await worker._executeWorkflow('wf-sig-3', 'SignalWorkflow', []);

    // Small delay to let workflow start and reach waitForSignal
    await new Promise(r => setTimeout(r, 50));

    worker._signalWorkflow('wf-sig-3', 'approval', 'delayed');
    const result = await handle.promise;
    expect(result).toBe('approved:delayed');
  });
});

describe('Client-Worker Integration', () => {
  let worker: Worker;
  let client: Client;

  beforeEach(async () => {
    worker = await Worker.create({ taskQueue: 'test', logLevel: 'silent' });
    await worker.run();
    client = new Client({}, worker);
  });

  afterEach(async () => {
    await worker.shutdown();
  });

  test('client starts workflow and gets result', async () => {
    worker.registerWorkflow(GreetWorkflow);
    worker.registerActivity(GreetActivity);

    const execution = await client.startWorkflow('wf-c-1', 'GreetWorkflow', ['Alice']);

    // Wait for completion
    await new Promise(r => setTimeout(r, 100));

    const desc = await client.describe('wf-c-1');
    expect(desc!.status).toBe(WorkflowStatus.COMPLETED);
    expect(desc!.result).toBe('Hello, Alice!');
  });

  test('client signals workflow through worker', async () => {
    worker.registerWorkflow(SignalWorkflow);

    const execution = await client.startWorkflow('wf-c-2', 'SignalWorkflow', []);

    // Signal through client
    await client.signal('wf-c-2', 'approval', 'client-approved');

    // Wait for completion
    await new Promise(r => setTimeout(r, 100));

    const desc = await client.describe('wf-c-2');
    expect(desc!.status).toBe(WorkflowStatus.COMPLETED);
    expect(desc!.result).toBe('approved:client-approved');
  });

  test('client queries workflow through worker', async () => {
    class QueryableWorkflow extends Workflow {
      static typeName = 'QueryableWorkflow';
      private _status = 'processing';
      async execute(): Promise<string> {
        await this.sleep(200);
        this._status = 'done';
        return 'result';
      }
      handleQuery(queryType: string): any {
        if (queryType === 'status') return this._status;
        return null;
      }
    }
    worker.registerWorkflow(QueryableWorkflow);

    await client.startWorkflow('wf-c-3', 'QueryableWorkflow', []);
    await new Promise(r => setTimeout(r, 50));

    const status = await client.query('wf-c-3', 'status');
    expect(status).toBe('processing');
  });

  test('client tracks failed workflow', async () => {
    class FailWorkflow extends Workflow {
      static typeName = 'FailWorkflow';
      async execute(): Promise<void> { throw new Error('boom'); }
    }
    worker.registerWorkflow(FailWorkflow);

    await client.startWorkflow('wf-c-4', 'FailWorkflow', []);
    await new Promise(r => setTimeout(r, 100));

    const desc = await client.describe('wf-c-4');
    expect(desc!.status).toBe(WorkflowStatus.FAILED);
    expect(desc!.error).toBe('boom');
  });

  test('client without worker still works (backward compatible)', async () => {
    const standaloneClient = new Client({ logLevel: 'silent' });
    const execution = await standaloneClient.startWorkflow('wf-standalone', 'AnyWorkflow', []);
    expect(execution.status).toBe(WorkflowStatus.RUNNING);

    // Manual completion (old behavior)
    standaloneClient.completeWorkflow('wf-standalone', 'manual-result');
    const desc = await standaloneClient.describe('wf-standalone');
    expect(desc!.status).toBe(WorkflowStatus.COMPLETED);
    expect(desc!.result).toBe('manual-result');
  });

  test('connectWorker after construction', async () => {
    worker.registerWorkflow(GreetWorkflow);
    worker.registerActivity(GreetActivity);

    const lateClient = new Client({ logLevel: 'silent' });
    lateClient.connectWorker(worker);

    await lateClient.startWorkflow('wf-late', 'GreetWorkflow', ['Late']);
    await new Promise(r => setTimeout(r, 100));

    const desc = await lateClient.describe('wf-late');
    expect(desc!.status).toBe(WorkflowStatus.COMPLETED);
    expect(desc!.result).toBe('Hello, Late!');
  });
});

describe('Worker Activity Metrics', () => {
  test('tracks activity execution count', async () => {
    const worker = await Worker.create({ taskQueue: 'test', logLevel: 'silent' });
    await worker.run();
    worker.registerWorkflow(MathWorkflow);
    worker.registerActivity(DoubleActivity);

    const handle = await worker._executeWorkflow('wf-m', 'MathWorkflow', [3]);
    await handle.promise;

    expect(worker.metrics.activitiesExecuted).toBe(2);
    await worker.shutdown();
  });
});
