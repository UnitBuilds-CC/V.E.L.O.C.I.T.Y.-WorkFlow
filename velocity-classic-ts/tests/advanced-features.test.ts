import { Worker, Workflow, Activity, Client, WorkflowStatus } from '../src/index';

// ─── Test Workflows and Activities ──────────────────────────────────────────

class HeartbeatActivity extends Activity {
  static typeName = 'HeartbeatActivity';
  async execute(data: string): Promise<string> {
    this.startHeartbeat(100);
    this.heartbeat({ progress: 50 });
    await new Promise(resolve => setTimeout(resolve, 50));
    this.heartbeat({ progress: 100 });
    this.stopHeartbeat();
    return `processed: ${data}`;
  }
}

class VersionedWorkflow extends Workflow {
  static typeName = 'VersionedWorkflow';
  static version = '2.0';
  async execute(): Promise<string> {
    const version = this.getVersion();
    const result = await this.patched('2.0', async () => 'new logic', async () => 'old logic');
    return `${version}: ${result}`;
  }
}

class OldVersionedWorkflow extends Workflow {
  static typeName = 'OldVersionedWorkflow';
  static version = '1.0';
  async execute(): Promise<string> {
    const result = await this.patched('2.0', async () => 'new logic', async () => 'old logic');
    return result;
  }
}

class CancellableWorkflow extends Workflow {
  static typeName = 'CancellableWorkflow';
  async execute(): Promise<string> {
    try {
      await this.sleep(10000);
      return 'completed';
    } catch (err: any) {
      return `cancelled: ${err.message}`;
    }
  }
}

class SagaWorkflow extends Workflow {
  static typeName = 'SagaWorkflow';
  async execute(shouldFail: boolean): Promise<string> {
    this.addCompensation(async () => { /* compensate step 1 */ });
    await this.executeActivity('LogActivity', 'step1');
    this.addCompensation(async () => { /* compensate step 2 */ });
    if (shouldFail) throw new Error('Saga failed');
    return 'success';
  }
}

class LogActivity extends Activity {
  static typeName = 'LogActivity';
  async execute(msg: string): Promise<string> {
    return `logged: ${msg}`;
  }
}

class ScheduledWorkflow extends Workflow {
  static typeName = 'ScheduledWorkflow';
  async execute(): Promise<string> {
    return `scheduled at ${Date.now()}`;
  }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

describe('Heartbeating', () => {
  let worker: Worker;

  beforeEach(async () => {
    worker = await Worker.create({ taskQueue: 'test' });
    worker.registerActivity(HeartbeatActivity);
    worker.registerActivity(LogActivity);
    await worker.run();
  });

  afterEach(async () => {
    await worker.shutdown();
  });

  test('activity can report heartbeat progress', async () => {
    const result = await worker._executeActivity<string>('HeartbeatActivity', ['test-data']);
    expect(result).toBe('processed: test-data');
  });

  test('heartbeat monitor tracks activity', async () => {
    const stats = worker.getStats();
    expect(stats.activeHeartbeatMonitors).toBeDefined();
  });
});

describe('Versioning', () => {
  let worker: Worker;

  beforeEach(async () => {
    worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(VersionedWorkflow);
    worker.registerWorkflow(OldVersionedWorkflow);
    await worker.run();
  });

  afterEach(async () => {
    await worker.shutdown();
  });

  test('workflow reports its version', async () => {
    const handle = await worker._executeWorkflow('wf-v1', 'VersionedWorkflow', []);
    const result = await handle.promise;
    expect(result).toBe('2.0: new logic');
  });

  test('patched() uses old logic on old version', async () => {
    const handle = await worker._executeWorkflow('wf-v2', 'OldVersionedWorkflow', []);
    const result = await handle.promise;
    expect(result).toBe('old logic');
  });
});

describe('Cancellation', () => {
  let worker: Worker;

  beforeEach(async () => {
    worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(CancellableWorkflow);
    await worker.run();
  });

  afterEach(async () => {
    await worker.shutdown();
  });

  test('workflow can be cancelled', async () => {
    const handle = await worker._executeWorkflow('wf-cancel', 'CancellableWorkflow', []);
    await new Promise(resolve => setTimeout(resolve, 50));
    await handle.instance.cancel();
    const result = await handle.promise;
    expect(result).toBe('cancelled: Workflow cancelled');
    expect(handle.status).toBe(WorkflowStatus.COMPLETED);
  });

  test('isCancelled returns correct state', async () => {
    const handle = await worker._executeWorkflow('wf-cancel2', 'CancellableWorkflow', []);
    expect(handle.instance.isCancelled()).toBe(false);
    await handle.instance.cancel();
    expect(handle.instance.isCancelled()).toBe(true);
    await handle.promise;
  });
});

describe('Saga Pattern', () => {
  let worker: Worker;

  beforeEach(async () => {
    worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(SagaWorkflow);
    worker.registerActivity(LogActivity);
    await worker.run();
  });

  afterEach(async () => {
    await worker.shutdown();
  });

  test('saga compensations run on failure', async () => {
    const handle = await worker._executeWorkflow('wf-saga', 'SagaWorkflow', [true]);
    await handle.promise.catch(() => {});
    expect(handle.status).toBe(WorkflowStatus.FAILED);
    // Compensations should have run (no error thrown)
  });

  test('saga succeeds when no failure', async () => {
    const handle = await worker._executeWorkflow('wf-saga-ok', 'SagaWorkflow', [false]);
    const result = await handle.promise;
    expect(result).toBe('success');
    expect(handle.status).toBe(WorkflowStatus.COMPLETED);
  });
});

describe('Schedules', () => {
  let worker: Worker;

  beforeEach(async () => {
    worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(ScheduledWorkflow);
    await worker.run();
  });

  afterEach(async () => {
    await worker.shutdown();
  });

  test('create and list schedule', async () => {
    await worker.createSchedule('sched-1', 'every 10s', 'ScheduledWorkflow', []);
    const schedules = worker.listSchedules();
    expect(schedules.length).toBe(1);
    expect(schedules[0].scheduleId).toBe('sched-1');
    expect(schedules[0].cron).toBe('every 10s');
    expect(schedules[0].workflowType).toBe('ScheduledWorkflow');
  });

  test('delete schedule', async () => {
    await worker.createSchedule('sched-2', 'every 5s', 'ScheduledWorkflow', []);
    expect(worker.listSchedules().length).toBe(1);
    await worker.deleteSchedule('sched-2');
    expect(worker.listSchedules().length).toBe(0);
  });

  test('duplicate schedule throws', async () => {
    await worker.createSchedule('sched-3', 'every 5s', 'ScheduledWorkflow', []);
    await expect(worker.createSchedule('sched-3', 'every 5s', 'ScheduledWorkflow', [])).rejects.toThrow();
  });
});

describe('Batch Operations', () => {
  let worker: Worker;
  let client: Client;

  beforeEach(async () => {
    worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(ScheduledWorkflow);
    worker.registerWorkflow(CancellableWorkflow);
    await worker.run();
    client = new Client({}, worker);
  });

  afterEach(async () => {
    await worker.shutdown();
  });

  test('batch start workflows', async () => {
    const handles = await worker.batchStartWorkflows([
      { workflowId: 'batch-1', workflowType: 'ScheduledWorkflow', args: [] },
      { workflowId: 'batch-2', workflowType: 'ScheduledWorkflow', args: [] },
      { workflowId: 'batch-3', workflowType: 'ScheduledWorkflow', args: [] },
    ]);
    expect(handles.length).toBe(3);
    expect(handles[0].executionId).toBe('batch-1');
    expect(handles[1].executionId).toBe('batch-2');
    expect(handles[2].executionId).toBe('batch-3');
  });

  test('batch cancel workflows', async () => {
    const h1 = await worker._executeWorkflow('batch-c1', 'CancellableWorkflow', []);
    const h2 = await worker._executeWorkflow('batch-c2', 'CancellableWorkflow', []);
    await new Promise(resolve => setTimeout(resolve, 50));
    await worker.batchCancel(['batch-c1', 'batch-c2']);
    expect(h1.instance.isCancelled()).toBe(true);
    expect(h2.instance.isCancelled()).toBe(true);
  });

  test('batch terminate workflows', async () => {
    await worker._executeWorkflow('batch-t1', 'CancellableWorkflow', []);
    await worker._executeWorkflow('batch-t2', 'CancellableWorkflow', []);
    await worker.batchTerminate(['batch-t1', 'batch-t2']);
    const h1 = worker._getHandle('batch-t1');
    const h2 = worker._getHandle('batch-t2');
    expect(h1?.status).toBe(WorkflowStatus.TERMINATED);
    expect(h2?.status).toBe(WorkflowStatus.TERMINATED);
  });
});

describe('Search Attributes', () => {
  let worker: Worker;

  beforeEach(async () => {
    worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(ScheduledWorkflow);
    await worker.run();
  });

  afterEach(async () => {
    await worker.shutdown();
  });

  test('set and get workflow attributes', async () => {
    await worker._executeWorkflow('wf-attr', 'ScheduledWorkflow', []);
    worker.setWorkflowAttributes('wf-attr', { customer: 'acme', priority: 'high' });
    const attrs = worker.getWorkflowAttributes('wf-attr');
    expect(attrs).toEqual({ customer: 'acme', priority: 'high' });
  });

  test('query workflows by attributes', async () => {
    await worker._executeWorkflow('wf-q1', 'ScheduledWorkflow', []);
    await worker._executeWorkflow('wf-q2', 'ScheduledWorkflow', []);
    await worker._executeWorkflow('wf-q3', 'ScheduledWorkflow', []);
    worker.setWorkflowAttributes('wf-q1', { env: 'prod' });
    worker.setWorkflowAttributes('wf-q2', { env: 'staging' });
    worker.setWorkflowAttributes('wf-q3', { env: 'prod' });
    const prodWorkflows = worker.queryWorkflows(a => a.env === 'prod');
    expect(prodWorkflows.length).toBe(2);
  });
});

describe('Client Integration', () => {
  let worker: Worker;
  let client: Client;

  beforeEach(async () => {
    worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(ScheduledWorkflow);
    worker.registerWorkflow(CancellableWorkflow);
    await worker.run();
    client = new Client({}, worker);
  });

  afterEach(async () => {
    await worker.shutdown();
  });

  test('client cancel through worker', async () => {
    await client.startWorkflow('wf-cc1', 'CancellableWorkflow', []);
    await new Promise(resolve => setTimeout(resolve, 50));
    await client.cancel('wf-cc1');
    // Wait for workflow to actually complete
    await new Promise(resolve => setTimeout(resolve, 100));
    const exec = await client.describe('wf-cc1');
    // The workflow caught the cancellation and returned a value, so it's COMPLETED
    // but the client's execution status is CANCELLED
    expect(exec?.status).toBe(WorkflowStatus.CANCELLED);
  });

  test('client terminate through worker', async () => {
    await client.startWorkflow('wf-ct1', 'CancellableWorkflow', []);
    await client.terminate('wf-ct1', 'timeout');
    const exec = await client.describe('wf-ct1');
    expect(exec?.status).toBe(WorkflowStatus.TERMINATED);
  });

  test('client schedule management', async () => {
    await client.createSchedule('client-sched', 'every 30s', 'ScheduledWorkflow', []);
    const schedules = client.listSchedules();
    expect(schedules.length).toBe(1);
    expect(schedules[0].scheduleId).toBe('client-sched');
    await client.deleteSchedule('client-sched');
    expect(client.listSchedules().length).toBe(0);
  });

  test('client search attributes', async () => {
    await client.startWorkflow('wf-sa1', 'ScheduledWorkflow', []);
    client.setWorkflowAttributes('wf-sa1', { region: 'us-east' });
    const attrs = client.getWorkflowAttributes('wf-sa1');
    expect(attrs).toEqual({ region: 'us-east' });
    const results = client.queryWorkflows(a => a.region === 'us-east');
    expect(results.length).toBe(1);
  });
});
