/**
 * VELOCITY-WorkFlow TypeScript SDK — Worker & Workflow Context tests.
 *
 * Verifies the full worker lifecycle: creation, workflow execution,
 * activity execution, signal/query/update handling, interceptors,
 * and graceful shutdown.
 */

import {
  Worker,
  WorkflowContext,
  ActivityScheduledMessage,
  TimerScheduledMessage,
  SignalWaitMessage,
  ChildWorkflowScheduledMessage,
  ContinueAsNewMessage,
} from './worker';

// ─── Worker Creation Tests ──────────────────────────────────────────────────

describe('Worker', () => {
  test('create requires taskQueue', async () => {
    await expect(Worker.create({ taskQueue: '' })).rejects.toThrow('taskQueue is required');
  });

  test('create with valid options', async () => {
    const worker = await Worker.create({
      taskQueue: 'test-queue',
      serverAddress: 'localhost:7234',
      namespace: 'default',
    });
    expect(worker).toBeDefined();
    expect(worker.getTaskQueue()).toBe('test-queue');
    expect(worker.isRunning()).toBe(false);
    worker.shutdown();
  });

  test('stats tracking', async () => {
    const worker = await Worker.create({ taskQueue: 'stats-queue' });
    const stats = worker.getStats();
    expect(stats.workflowsStarted).toBe(0);
    expect(stats.workflowsCompleted).toBe(0);
    expect(stats.uptimeMs).toBeGreaterThanOrEqual(0);
    worker.shutdown();
  });
});

// ─── Workflow Context Tests ─────────────────────────────────────────────────

describe('WorkflowContext', () => {
  function makeCtx(overrides?: Partial<ConstructorParameters<typeof WorkflowContext>[0]>) {
    return new WorkflowContext({
      workflowKey: BigInt(42),
      workflowId: 'wf-1',
      runId: 'run-1',
      workflowType: 'test-workflow',
      taskQueue: 'test-queue',
      ...overrides,
    });
  }

  test('constructor sets properties', () => {
    const ctx = makeCtx();
    expect(ctx.workflowKey).toBe(BigInt(42));
    expect(ctx.workflowId).toBe('wf-1');
    expect(ctx.runId).toBe('run-1');
    expect(ctx.workflowType).toBe('test-workflow');
    expect(ctx.taskQueue).toBe('test-queue');
    expect(ctx.currentStep).toBe(0);
    expect(ctx.isCanceled).toBe(false);
  });

  test('executeActivity throws ActivityScheduledMessage', async () => {
    const ctx = makeCtx();
    try {
      await ctx.executeActivity('sendEmail', 'user@test.com', 'Hello');
      fail('Should have thrown');
    } catch (e) {
      expect(e).toBeInstanceOf(ActivityScheduledMessage);
      const msg = e as ActivityScheduledMessage;
      expect(msg.activityName).toBe('sendEmail');
      expect(msg.args).toEqual(['user@test.com', 'Hello']);
      expect(msg.step).toBe(1);
    }
  });

  test('sleep throws TimerScheduledMessage', async () => {
    const ctx = makeCtx();
    try {
      await ctx.sleep(5000);
      fail('Should have thrown');
    } catch (e) {
      expect(e).toBeInstanceOf(TimerScheduledMessage);
      const msg = e as TimerScheduledMessage;
      expect(msg.durationMs).toBe(5000);
    }
  });

  test('waitForSignal throws SignalWaitMessage when no buffered signal', async () => {
    const ctx = makeCtx();
    try {
      await ctx.waitForSignal('approval');
      fail('Should have thrown');
    } catch (e) {
      expect(e).toBeInstanceOf(SignalWaitMessage);
      const msg = e as SignalWaitMessage;
      expect(msg.signalName).toBe('approval');
    }
  });

  test('signal handler registration and delivery', () => {
    const ctx = makeCtx();
    let received: any = null;
    ctx.onSignal('approval', (payload) => {
      received = payload;
    });
    ctx._deliverSignal('approval', { approved: true });
    expect(received).toEqual({ approved: true });
  });

  test('signal buffering when no handler', () => {
    const ctx = makeCtx();
    ctx._deliverSignal('approval', { approved: true });
    ctx._deliverSignal('approval', { approved: false });
    // Now register handler — buffered signals should be available
    // (but only via waitForSignal, which we test separately)
  });

  test('query handler registration and execution', () => {
    const ctx = makeCtx();
    ctx.onQuery('getStatus', () => 'running');
    expect(ctx._handleQuery('getStatus')).toBe('running');
  });

  test('query handler not found throws', () => {
    const ctx = makeCtx();
    expect(() => ctx._handleQuery('unknown')).toThrow("No query handler registered for 'unknown'");
  });

  test('update handler registration and execution', () => {
    const ctx = makeCtx();
    ctx.onUpdate('setPriority', (payload) => {
      return { priority: payload.priority };
    });
    const result = ctx._handleUpdate('setPriority', { priority: 'high' });
    expect(result).toEqual({ priority: 'high' });
  });

  test('cancel marks workflow as canceled', () => {
    const ctx = makeCtx();
    expect(ctx.isCanceled).toBe(false);
    ctx._markCanceled();
    expect(ctx.isCanceled).toBe(true);
  });

  test('upsertSearchAttributes', () => {
    const ctx = makeCtx();
    ctx.upsertSearchAttributes({ customerId: 'C123', priority: 'high' });
    // Search attributes are stored internally
  });

  test('continueAsNew throws ContinueAsNewMessage', () => {
    const ctx = makeCtx();
    try {
      ctx.continueAsNew('new-arg');
      fail('Should have thrown');
    } catch (e) {
      expect(e).toBeInstanceOf(ContinueAsNewMessage);
      const msg = e as ContinueAsNewMessage;
      expect(msg.args).toEqual(['new-arg']);
    }
  });

  test('startChildWorkflow throws ChildWorkflowScheduledMessage', async () => {
    const ctx = makeCtx();
    try {
      await ctx.startChildWorkflow('child-wf', { taskQueue: 'child-queue' }, 'arg1');
      fail('Should have thrown');
    } catch (e) {
      expect(e).toBeInstanceOf(ChildWorkflowScheduledMessage);
      const msg = e as ChildWorkflowScheduledMessage;
      expect(msg.workflowType).toBe('child-wf');
      expect(msg.options.taskQueue).toBe('child-queue');
    }
  });
});

// ─── Workflow Execution Tests ───────────────────────────────────────────────

describe('Worker workflow execution', () => {
  test('executeWorkflow with registered implementation', async () => {
    const worker = await Worker.create({
      taskQueue: 'exec-queue',
      workflows: {
        'greeting': async (ctx: WorkflowContext, name: string) => {
          return `Hello, ${name}!`;
        },
      },
    });

    const result = await worker.executeWorkflow(
      'greeting',
      BigInt(1),
      'wf-1',
      ['World'],
    );
    expect(result).toBe('Hello, World!');

    const stats = worker.getStats();
    expect(stats.workflowsStarted).toBe(1);
    expect(stats.workflowsCompleted).toBe(1);
    expect(stats.workflowsFailed).toBe(0);
    worker.shutdown();
  });

  test('executeWorkflow with unregistered type throws', async () => {
    const worker = await Worker.create({ taskQueue: 'exec-queue' });
    await expect(
      worker.executeWorkflow('unknown', BigInt(1), 'wf-1', []),
    ).rejects.toThrow("No workflow implementation registered for 'unknown'");
    worker.shutdown();
  });

  test('executeWorkflow tracks failures', async () => {
    const worker = await Worker.create({
      taskQueue: 'fail-queue',
      workflows: {
        'failing': async () => {
          throw new Error('intentional failure');
        },
      },
    });

    await expect(
      worker.executeWorkflow('failing', BigInt(1), 'wf-1', []),
    ).rejects.toThrow('intentional failure');

    const stats = worker.getStats();
    expect(stats.workflowsStarted).toBe(1);
    expect(stats.workflowsFailed).toBe(1);
    worker.shutdown();
  });
});

// ─── Activity Execution Tests ───────────────────────────────────────────────

describe('Worker activity execution', () => {
  test('executeActivity with registered implementation', async () => {
    const worker = await Worker.create({
      taskQueue: 'act-queue',
      activities: {
        'add': async (a: number, b: number) => a + b,
      },
    });

    const result = await worker.executeActivity('add', [3, 4]);
    expect(result).toBe(7);

    const stats = worker.getStats();
    expect(stats.activitiesScheduled).toBe(1);
    expect(stats.activitiesCompleted).toBe(1);
    worker.shutdown();
  });

  test('executeActivity with unregistered type throws', async () => {
    const worker = await Worker.create({ taskQueue: 'act-queue' });
    await expect(
      worker.executeActivity('unknown', []),
    ).rejects.toThrow("No activity implementation registered for 'unknown'");
    worker.shutdown();
  });

  test('registerWorkflows and registerActivities', async () => {
    const worker = await Worker.create({ taskQueue: 'dyn-queue' });

    worker.registerWorkflows({
      'dynamic-wf': async (ctx: WorkflowContext) => 'dynamic result',
    });
    worker.registerActivities({
      'dynamic-act': async (x: string) => x.toUpperCase(),
    });

    const wfResult = await worker.executeWorkflow('dynamic-wf', BigInt(1), 'wf-1', []);
    expect(wfResult).toBe('dynamic result');

    const actResult = await worker.executeActivity('dynamic-act', ['hello']);
    expect(actResult).toBe('HELLO');

    worker.shutdown();
  });
});

// ─── Interceptor Tests ──────────────────────────────────────────────────────

describe('Worker interceptors', () => {
  test('interceptor wraps workflow execution', async () => {
    const log: string[] = [];

    const worker = await Worker.create({
      taskQueue: 'int-queue',
      workflows: {
        'test-wf': async (ctx: WorkflowContext) => 'result',
      },
      interceptors: [
        {
          interceptWorkflow: async (input, next) => {
            log.push(`before:${input.workflowType}`);
            const result = await next();
            log.push(`after:${input.workflowType}`);
            return result;
          },
        },
      ],
    });

    const result = await worker.executeWorkflow('test-wf', BigInt(1), 'wf-1', []);
    expect(result).toBe('result');
    expect(log).toEqual(['before:test-wf', 'after:test-wf']);
    worker.shutdown();
  });
});

// ─── Shutdown Tests ─────────────────────────────────────────────────────────

describe('Worker shutdown', () => {
  test('shutdown stops the worker', async () => {
    const worker = await Worker.create({ taskQueue: 'sd-queue' });
    worker.shutdown();
    expect(worker.isRunning()).toBe(false);
  });

  test('shutdown emits shutdown event', async () => {
    const worker = await Worker.create({ taskQueue: 'sd-queue' });
    let emitted = false;
    worker.on('shutdown', () => { emitted = true; });
    worker.shutdown();
    expect(emitted).toBe(true);
  });
});
