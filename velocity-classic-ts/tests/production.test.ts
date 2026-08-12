import {
  // Core
  Workflow, Activity, Worker, Client, WorkflowStatus,
  featureMatrix, createClassicConfig, defaultConfig,
  // Errors
  VelocityClassicError, WorkflowNotFoundError, WorkflowTypeError,
  ActivityTypeError, WorkerNotRunningError, DuplicateRegistrationError,
  // Logger
  createClassicLogger, ClassicLogger,
  // Metrics
  ClassicMetrics,
} from '../src/index';

// ─── Test helpers ────────────────────────────────────────────────────────────

class TestWorkflow extends Workflow {
  static typeName = 'TestWorkflow';
  async execute(orderId: string): Promise<string> {
    return `completed-${orderId}`;
  }
}

class TestActivity extends Activity {
  static typeName = 'TestActivity';
  async execute(data: string): Promise<string> {
    return `processed-${data}`;
  }
}

// ─── Error Tests ────────────────────────────────────────────────────────────

describe('Error hierarchy', () => {
  test('WorkflowNotFoundError', () => {
    const err = new WorkflowNotFoundError('wf-1');
    expect(err).toBeInstanceOf(VelocityClassicError);
    expect(err.code).toBe('WORKFLOW_NOT_FOUND');
    expect(err.workflowId).toBe('wf-1');
  });

  test('WorkflowTypeError', () => {
    const err = new WorkflowTypeError('OrderWorkflow');
    expect(err.code).toBe('WORKFLOW_TYPE_NOT_FOUND');
  });

  test('ActivityTypeError', () => {
    const err = new ActivityTypeError('chargeActivity');
    expect(err.code).toBe('ACTIVITY_TYPE_NOT_FOUND');
  });

  test('WorkerNotRunningError', () => {
    const err = new WorkerNotRunningError();
    expect(err.code).toBe('WORKER_NOT_RUNNING');
  });

  test('DuplicateRegistrationError', () => {
    const err = new DuplicateRegistrationError('MyWorkflow');
    expect(err.code).toBe('DUPLICATE_REGISTRATION');
  });
});

// ─── Config Tests ───────────────────────────────────────────────────────────

describe('Config', () => {
  test('defaults', () => {
    const cfg = defaultConfig();
    expect(cfg.serverAddress).toBe('localhost:7233');
    expect(cfg.namespace).toBe('default');
    expect(cfg.maxConcurrentWorkflows).toBe(100);
  });

  test('createClassicConfig with overrides', () => {
    const cfg = createClassicConfig({ taskQueue: 'orders', logLevel: 'debug' });
    expect(cfg.taskQueue).toBe('orders');
    expect(cfg.logLevel).toBe('debug');
  });

  test('validation rejects bad concurrency', () => {
    expect(() => createClassicConfig({ maxConcurrentWorkflows: 0 })).toThrow();
    expect(() => createClassicConfig({ maxConcurrentActivities: 0 })).toThrow();
  });
});

// ─── Logger Tests ───────────────────────────────────────────────────────────

describe('Logger', () => {
  test('silent logger', () => {
    const spy = jest.spyOn(console, 'info').mockImplementation();
    const logger = createClassicLogger('silent');
    logger.info('test');
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  test('info logger', () => {
    const spy = jest.spyOn(console, 'info').mockImplementation();
    const logger = createClassicLogger('info');
    logger.info('test');
    expect(spy).toHaveBeenCalled();
    spy.mockRestore();
  });
});

// ─── Metrics Tests ──────────────────────────────────────────────────────────

describe('ClassicMetrics', () => {
  test('tracks all counters', () => {
    const m = new ClassicMetrics();
    m.recordWorkflowStarted();
    m.recordWorkflowCompleted();
    m.recordWorkflowStarted();
    m.recordWorkflowFailed();
    m.recordActivityExecuted();
    m.recordSignalSent();
    m.recordQueryHandled();

    const snap = m.snapshot();
    expect(snap.workflowsStarted).toBe(2);
    expect(snap.workflowsCompleted).toBe(1);
    expect(snap.workflowsFailed).toBe(1);
    expect(snap.activitiesExecuted).toBe(1);
    expect(snap.signalsSent).toBe(1);
    expect(snap.queriesHandled).toBe(1);
  });

  test('reset', () => {
    const m = new ClassicMetrics();
    m.recordWorkflowStarted();
    m.reset();
    expect(m.snapshot().workflowsStarted).toBe(0);
  });
});

// ─── Worker Production Tests ────────────────────────────────────────────────

describe('Worker production features', () => {
  test('duplicate workflow registration throws', async () => {
    const worker = await Worker.create({ logLevel: 'silent' });
    worker.registerWorkflow(TestWorkflow);
    expect(() => worker.registerWorkflow(TestWorkflow)).toThrow(DuplicateRegistrationError);
  });

  test('duplicate activity registration throws', async () => {
    const worker = await Worker.create({ logLevel: 'silent' });
    worker.registerActivity(TestActivity);
    expect(() => worker.registerActivity(TestActivity)).toThrow(DuplicateRegistrationError);
  });

  test('health check when running', async () => {
    const worker = await Worker.create({ logLevel: 'silent' });
    worker.registerWorkflow(TestWorkflow);
    await worker.run();
    const status = await worker.healthCheck();
    expect(status.status).toBe('healthy');
    expect(status.checks.some(c => c.name === 'readiness')).toBe(true);
    await worker.shutdown();
  });

  test('health check when not running', async () => {
    const worker = await Worker.create({ logLevel: 'silent' });
    worker.registerWorkflow(TestWorkflow);
    const status = await worker.healthCheck();
    expect(status.status).toBe('degraded');
  });

  test('health check degraded when no registrations', async () => {
    const worker = await Worker.create({ logLevel: 'silent' });
    await worker.run();
    const status = await worker.healthCheck();
    expect(status.status).toBe('degraded');
    await worker.shutdown();
  });

  test('stats with metrics', async () => {
    const worker = await Worker.create({ logLevel: 'silent' });
    worker.registerWorkflow(TestWorkflow);
    worker.registerActivity(TestActivity);
    await worker.run();

    const stats = worker.getStats();
    expect(stats.metrics).toBeDefined();
    expect(stats.registeredWorkflows).toBe(1);
    expect(stats.registeredActivities).toBe(1);
    expect(stats.running).toBe(true);
    await worker.shutdown();
  });

  test('graceful shutdown', async () => {
    const worker = await Worker.create({ logLevel: 'silent' });
    worker.registerWorkflow(TestWorkflow);
    await worker.run();
    expect(worker.isRunning).toBe(true);
    await worker.shutdown();
    expect(worker.isRunning).toBe(false);
  });
});

// ─── Client Production Tests ────────────────────────────────────────────────

describe('Client production features', () => {
  test('WorkflowNotFoundError on signal', async () => {
    const client = new Client({ logLevel: 'silent' });
    await expect(client.signal('nonexistent', 'sig', {})).rejects.toThrow(WorkflowNotFoundError);
  });

  test('WorkflowNotFoundError on query', async () => {
    const client = new Client({ logLevel: 'silent' });
    await expect(client.query('nonexistent', 'q')).rejects.toThrow(WorkflowNotFoundError);
  });

  test('WorkflowNotFoundError on cancel', async () => {
    const client = new Client({ logLevel: 'silent' });
    await expect(client.cancel('nonexistent')).rejects.toThrow(WorkflowNotFoundError);
  });

  test('WorkflowNotFoundError on terminate', async () => {
    const client = new Client({ logLevel: 'silent' });
    await expect(client.terminate('nonexistent')).rejects.toThrow(WorkflowNotFoundError);
  });

  test('list with filters', async () => {
    const client = new Client({ logLevel: 'silent' });
    await client.startWorkflow('wf-1', 'OrderWorkflow', ['order-1']);
    await client.startWorkflow('wf-2', 'PaymentWorkflow', ['pay-1']);
    client.completeWorkflow('wf-1', 'done');

    const all = await client.list();
    expect(all).toHaveLength(2);

    const completed = await client.list({ status: WorkflowStatus.COMPLETED });
    expect(completed).toHaveLength(1);

    const orders = await client.list({ workflowType: 'OrderWorkflow' });
    expect(orders).toHaveLength(1);
  });

  test('completeWorkflow updates status', async () => {
    const client = new Client({ logLevel: 'silent' });
    await client.startWorkflow('wf-1', 'OrderWorkflow', ['order-1']);
    client.completeWorkflow('wf-1', { result: 'done' });

    const exec = await client.describe('wf-1');
    expect(exec!.status).toBe(WorkflowStatus.COMPLETED);
    expect(exec!.result).toEqual({ result: 'done' });
    expect(exec!.closeTime).toBeGreaterThan(0);
  });

  test('failWorkflow updates status', async () => {
    const client = new Client({ logLevel: 'silent' });
    await client.startWorkflow('wf-1', 'OrderWorkflow', ['order-1']);
    client.failWorkflow('wf-1', 'something broke');

    const exec = await client.describe('wf-1');
    expect(exec!.status).toBe(WorkflowStatus.FAILED);
    expect(exec!.error).toBe('something broke');
  });

  test('metrics tracked on client operations', async () => {
    const client = new Client({ logLevel: 'silent' });
    await client.startWorkflow('wf-1', 'OrderWorkflow', ['order-1']);
    await client.signal('wf-1', 'approval', { approved: true });
    await client.query('wf-1', 'status');
    client.completeWorkflow('wf-1', 'done');

    const stats = client.getStats();
    expect(stats.metrics.workflowsStarted).toBe(1);
    expect(stats.metrics.workflowsCompleted).toBe(1);
    expect(stats.metrics.signalsSent).toBe(1);
    expect(stats.metrics.queriesHandled).toBe(1);
  });

  test('health check', async () => {
    const client = new Client({ logLevel: 'silent' });
    const status = await client.healthCheck();
    expect(status.status).toBe('healthy');
    expect(status.checks.some(c => c.name === 'connectivity')).toBe(true);
  });

  test('search attributes and memo stored', async () => {
    const client = new Client({ logLevel: 'silent' });
    await client.startWorkflow('wf-1', 'OrderWorkflow', [], {
      searchAttributes: { customerId: 'c-1' },
      memo: { note: 'test' },
    });

    const exec = await client.describe('wf-1');
    expect(exec!.searchAttributes).toEqual({ customerId: 'c-1' });
    expect(exec!.memo).toEqual({ note: 'test' });
  });
});

// ─── Feature Matrix Tests ───────────────────────────────────────────────────

describe('Feature matrix', () => {
  test('core features enabled', () => {
    const fm = featureMatrix();
    expect(fm.workflows).toBe(true);
    expect(fm.activities).toBe(true);
    expect(fm.signals).toBe(true);
    expect(fm.sagaPattern).toBe(true);
    expect(fm.heartbeats).toBe(true);
    expect(fm.cancellation).toBe(true);
    expect(fm.schedules).toBe(true);
    expect(fm.batchOperations).toBe(true);
    expect(fm.versioning).toBe(true);
    // Count how many features are enabled
    const enabledCount = Object.values(fm).filter(v => v === true).length;
    expect(enabledCount).toBe(21);
  });
});
