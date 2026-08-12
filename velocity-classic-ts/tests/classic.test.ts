import {
  Workflow, Activity, Worker, Client,
  WorkflowStatus, ClassicConfig,
  defaultConfig, featureMatrix,
} from '../src/index';

// ─── Test Workflow/Activity Classes ──────────────────────────────────────────

class OrderWorkflow extends Workflow {
  static typeName = 'OrderWorkflow';

  async execute(orderId: string) {
    const charge = await this.executeActivity('chargeActivity', orderId);
    const ship = await this.executeActivity('shipActivity', orderId);
    return { charge, ship };
  }
}

class ChargeActivity extends Activity {
  static typeName = 'ChargeActivity';

  async execute(orderId: string) {
    return { status: 'charged', orderId };
  }
}

// ─── Worker Tests ────────────────────────────────────────────────────────────

describe('Worker', () => {
  test('creation with default config', async () => {
    const worker = await Worker.create();
    expect(worker.taskQueue).toBe('default');
    expect(worker.config.namespace).toBe('default');
  });

  test('creation with custom config', async () => {
    const worker = await Worker.create({ taskQueue: 'orders' });
    expect(worker.taskQueue).toBe('orders');
  });

  test('register workflow', async () => {
    const worker = await Worker.create();
    worker.registerWorkflow(OrderWorkflow);
    expect(worker.workflowTypes).toContain('OrderWorkflow');
  });

  test('register activity', async () => {
    const worker = await Worker.create();
    worker.registerActivity(ChargeActivity);
    expect(worker.activityTypes).toContain('ChargeActivity');
  });

  test('run and shutdown', async () => {
    const worker = await Worker.create();
    expect(worker.isRunning).toBe(false);
    await worker.run();
    expect(worker.isRunning).toBe(true);
    worker.shutdown();
    expect(worker.isRunning).toBe(false);
  });
});

// ─── Client Tests ────────────────────────────────────────────────────────────

describe('Client', () => {
  test('creation with default config', () => {
    const client = new Client();
    expect(client.config.serverAddress).toBe('localhost:7233');
    expect(client.config.namespace).toBe('default');
  });

  test('start workflow', async () => {
    const client = new Client();
    const exec = await client.startWorkflow('wf-1', 'OrderWorkflow', ['order-123']);

    expect(exec.workflowId).toBe('wf-1');
    expect(exec.workflowType).toBe('OrderWorkflow');
    expect(exec.status).toBe(WorkflowStatus.RUNNING);
  });

  test('describe workflow', async () => {
    const client = new Client();
    await client.startWorkflow('wf-1', 'OrderWorkflow', ['order-123']);

    const desc = await client.describe('wf-1');
    expect(desc).toBeDefined();
    expect(desc!.status).toBe(WorkflowStatus.RUNNING);
  });

  test('cancel workflow', async () => {
    const client = new Client();
    await client.startWorkflow('wf-1', 'OrderWorkflow', ['order-123']);

    await client.cancel('wf-1');
    const desc = await client.describe('wf-1');
    expect(desc!.status).toBe(WorkflowStatus.CANCELLED);
    expect(desc!.closeTime).toBeDefined();
  });

  test('terminate workflow', async () => {
    const client = new Client();
    await client.startWorkflow('wf-1', 'OrderWorkflow', ['order-123']);

    await client.terminate('wf-1', 'timeout');
    const desc = await client.describe('wf-1');
    expect(desc!.status).toBe(WorkflowStatus.TERMINATED);
  });

  test('signal workflow', async () => {
    const client = new Client();
    await client.startWorkflow('wf-1', 'OrderWorkflow', ['order-123']);

    await expect(client.signal('wf-1', 'approve', true)).resolves.toBeUndefined();
  });

  test('signal nonexistent workflow', async () => {
    const client = new Client();
    await expect(client.signal('wf-999', 'approve', true)).rejects.toThrow();
  });

  test('query workflow', async () => {
    const client = new Client();
    await client.startWorkflow('wf-1', 'OrderWorkflow', ['order-123']);

    const result = await client.query('wf-1', 'getStatus');
    expect(result).toEqual({ status: 'ok' });
  });

  test('list workflows', async () => {
    const client = new Client();
    await client.startWorkflow('wf-1', 'OrderWorkflow', ['a']);
    await client.startWorkflow('wf-2', 'OrderWorkflow', ['b']);

    const list = await client.list();
    expect(list.length).toBe(2);
  });
});

// ─── Config Tests ────────────────────────────────────────────────────────────

describe('defaultConfig', () => {
  test('returns valid config', () => {
    const config = defaultConfig();
    expect(config.serverAddress).toBe('localhost:7233');
    expect(config.namespace).toBe('default');
    expect(config.taskQueue).toBe('default');
    expect(config.maxConcurrentWorkflows).toBe(100);
    expect(config.maxConcurrentActivities).toBe(200);
    expect(config.stickyQueues).toBe(true);
  });
});

// ─── Feature Matrix Tests ────────────────────────────────────────────────────

describe('featureMatrix', () => {
  test('all core features enabled', () => {
    const matrix = featureMatrix();
    expect(matrix.workflows).toBe(true);
    expect(matrix.activities).toBe(true);
    expect(matrix.signals).toBe(true);
    expect(matrix.queries).toBe(true);
    expect(matrix.childWorkflows).toBe(true);
    expect(matrix.continueAsNew).toBe(true);
    expect(matrix.retries).toBe(true);
    expect(matrix.heartbeats).toBe(true);
  });

  test('advanced features enabled', () => {
    const matrix = featureMatrix();
    expect(matrix.signalWithStart).toBe(true);
    expect(matrix.searchAttributes).toBe(true);
    expect(matrix.memo).toBe(true);
    expect(matrix.batchOperations).toBe(true);
    expect(matrix.schedules).toBe(true);
    expect(matrix.updates).toBe(true);
    expect(matrix.reset).toBe(true);
    expect(matrix.stickyQueues).toBe(true);
  });

  test('velocity extensions enabled', () => {
    const matrix = featureMatrix();
    expect(matrix.nexusOperations).toBe(true);
    expect(matrix.sagaPattern).toBe(true);
  });

  test('all 21 features are true', () => {
    const matrix = featureMatrix();
    const trueFeatures = Object.values(matrix).filter(v => v === true);
    expect(trueFeatures.length).toBe(21);
    expect(Object.keys(matrix).length).toBe(21);
  });
});

// ─── WorkflowStatus Tests ────────────────────────────────────────────────────

describe('WorkflowStatus', () => {
  test('all variants exist', () => {
    expect(WorkflowStatus.RUNNING).toBe('running');
    expect(WorkflowStatus.COMPLETED).toBe('completed');
    expect(WorkflowStatus.FAILED).toBe('failed');
    expect(WorkflowStatus.CANCELLED).toBe('cancelled');
    expect(WorkflowStatus.TERMINATED).toBe('terminated');
    expect(WorkflowStatus.CONTINUING_AS_NEW).toBe('continuingAsNew');
  });
});
