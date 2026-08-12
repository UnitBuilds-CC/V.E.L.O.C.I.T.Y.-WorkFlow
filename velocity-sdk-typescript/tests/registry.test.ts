import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { Workflow, WorkflowHelpers, defineWorkflow } from '../src/workflow';
import { Activity, defineActivity } from '../src/activity';
import { Worker } from '../src/worker';
import { Connection } from '../src/connection';
import {
  ContinueAsNewError,
  ScheduleClient,
  SearchAttributesClient,
  BatchOperationClient,
  Saga,
} from '../src/advanced';

beforeEach(() => {
  Workflow.clear();
  Activity.clear();
  WorkflowHelpers.setCurrentContext(null);
});

// ─── Registration Tests ───────────────────────────────────────────────────────

describe('Workflow Registration', () => {
  it('should register and retrieve a workflow', () => {
    const workflow = async () => 'test';
    Workflow.register('test-workflow', workflow);

    expect(Workflow.has('test-workflow')).toBe(true);
    expect(Workflow.get('test-workflow')).toBe(workflow);
  });

  it('should return false for non-existent workflow', () => {
    expect(Workflow.has('non-existent')).toBe(false);
    expect(Workflow.get('non-existent')).toBeUndefined();
  });

  it('should support defineWorkflow helper', () => {
    const workflow = async () => 'test';
    defineWorkflow('helper-workflow', workflow);

    expect(Workflow.has('helper-workflow')).toBe(true);
  });

  it('should clear all workflows', () => {
    Workflow.register('wf1', async () => 'a');
    Workflow.register('wf2', async () => 'b');
    expect(Workflow.has('wf1')).toBe(true);
    Workflow.clear();
    expect(Workflow.has('wf1')).toBe(false);
    expect(Workflow.has('wf2')).toBe(false);
  });
});

describe('Activity Registration', () => {
  it('should register and retrieve an activity', () => {
    const activity = async () => 'test';
    Activity.register('test-activity', activity);

    expect(Activity.has('test-activity')).toBe(true);
    expect(Activity.get('test-activity')).toBe(activity);
  });

  it('should return false for non-existent activity', () => {
    expect(Activity.has('non-existent')).toBe(false);
  });

  it('should support defineActivity helper', () => {
    const activity = async () => 'test';
    defineActivity('helper-activity', activity);

    expect(Activity.has('helper-activity')).toBe(true);
  });

  it('should clear all activities', () => {
    Activity.register('act1', async () => 'a');
    Activity.clear();
    expect(Activity.has('act1')).toBe(false);
  });
});

// ─── Worker Local Execution Tests ─────────────────────────────────────────────

describe('Worker Local Execution', () => {
  it('should execute a simple workflow', async () => {
    Workflow.register('simple-wf', async (_ctx, input) => {
      return { result: `hello ${input}` };
    });

    const worker = new Worker({ taskQueue: 'test-queue' });
    const result = await worker.executeWorkflow('wf-1', 'simple-wf', 'world');

    expect(result).toEqual({ result: 'hello world' });
  });

  it('should throw for unregistered workflow', async () => {
    const worker = new Worker({ taskQueue: 'test-queue' });
    await expect(worker.executeWorkflow('wf-1', 'nonexistent')).rejects.toThrow('not registered');
  });

  it('should propagate workflow errors', async () => {
    Workflow.register('failing-wf', async () => {
      throw new Error('intentional failure');
    });

    const worker = new Worker({ taskQueue: 'test-queue' });
    await expect(worker.executeWorkflow('wf-1', 'failing-wf')).rejects.toThrow('intentional failure');
  });

  it('should execute workflow with activity', async () => {
    Activity.register('greet', async (_ctx, input: any) => {
      return `Hello, ${input.name}!`;
    });

    Workflow.register('activity-wf', async (ctx, input) => {
      const result = await WorkflowHelpers.executeActivity({
        taskQueue: 'test-queue',
        activityType: 'greet',
        input: { name: 'World' },
      });
      return result;
    });

    const worker = new Worker({ taskQueue: 'test-queue' });
    const result = await worker.executeWorkflow('wf-2', 'activity-wf');
    expect(result).toBe('Hello, World!');
  });

  it('should execute workflow with child workflow', async () => {
    Workflow.register('child-wf', async (_ctx, input: any) => {
      return input.value * 2;
    });

    Workflow.register('parent-wf', async (ctx) => {
      const result = await WorkflowHelpers.executeChildWorkflow({
        workflowId: 'child-1',
        workflowType: 'child-wf',
        taskQueue: 'test-queue',
        input: { value: 21 },
      });
      return result;
    });

    const worker = new Worker({ taskQueue: 'test-queue' });
    const result = await worker.executeWorkflow('parent-1', 'parent-wf');
    expect(result).toBe(42);
  });

  it('should execute multi-step workflow with multiple activities', async () => {
    Activity.register('multiply', async (_ctx, input: any) => {
      return input.a * input.b;
    });

    Activity.register('add', async (_ctx, input: any) => {
      return input.a + input.b;
    });

    Workflow.register('calculator-wf', async (ctx) => {
      const product = await WorkflowHelpers.executeActivity({
        taskQueue: 'test-queue',
        activityType: 'multiply',
        input: { a: 6, b: 7 },
      });

      const sum = await WorkflowHelpers.executeActivity({
        taskQueue: 'test-queue',
        activityType: 'add',
        input: { a: product, b: 8 },
      });

      return sum;
    });

    const worker = new Worker({ taskQueue: 'test-queue' });
    const result = await worker.executeWorkflow('calc-1', 'calculator-wf');
    expect(result).toBe(50);
  });

  it('should throw when executing activity without worker context', async () => {
    WorkflowHelpers.setCurrentContext(null);
    await expect(
      WorkflowHelpers.executeActivity({
        taskQueue: 'test-queue',
        activityType: 'some-activity',
      })
    ).rejects.toThrow('No worker bound');
  });

  it('should throw when getting info without active context', () => {
    WorkflowHelpers.setCurrentContext(null);
    expect(() => WorkflowHelpers.getInfo()).toThrow('No active workflow context');
  });

  it('should provide workflow info via getInfo', async () => {
    Workflow.register('info-wf', async (ctx) => {
      const info = WorkflowHelpers.getInfo();
      return { workflowId: info.workflowId, taskQueue: info.taskQueue };
    });

    const worker = new Worker({ taskQueue: 'my-queue' });
    const result = await worker.executeWorkflow('wf-info-1', 'info-wf');
    expect(result.workflowId).toBe('wf-info-1');
    expect(result.taskQueue).toBe('my-queue');
  });
});

// ─── WorkflowHelpers.sleep ────────────────────────────────────────────────────

describe('WorkflowHelpers.sleep', () => {
  it('should sleep for the specified duration', async () => {
    const start = Date.now();
    await WorkflowHelpers.sleep(50);
    const elapsed = Date.now() - start;
    expect(elapsed).toBeGreaterThanOrEqual(40);
  });
});

// ─── Worker Properties ────────────────────────────────────────────────────────

describe('Worker Properties', () => {
  it('should return task queue', () => {
    const worker = new Worker({ taskQueue: 'my-queue' });
    expect(worker.getTaskQueue()).toBe('my-queue');
  });

  it('should not be running initially', () => {
    const worker = new Worker({ taskQueue: 'test' });
    expect(worker.isRunning()).toBe(false);
  });
});

// ─── Connection Tests ─────────────────────────────────────────────────────────

describe('Connection', () => {
  it('should create with default scheme', () => {
    const conn = new Connection({ address: 'localhost:5000' });
    expect(conn).toBeDefined();
    conn.close();
  });

  it('should preserve explicit scheme', () => {
    const conn = new Connection({ address: 'https://velocity.example.com:8443' });
    expect(conn).toBeDefined();
    conn.close();
  });

  it('should close without error', () => {
    const conn = new Connection({ address: 'localhost:5000' });
    expect(() => conn.close()).not.toThrow();
  });
});

// ─── Advanced Features ────────────────────────────────────────────────────────

describe('ContinueAsNewError', () => {
  it('should create a continue-as-new error', () => {
    const err = new ContinueAsNewError({
      workflowType: 'LongRunningWorkflow',
      taskQueue: 'main',
      input: { iteration: 42 },
    });
    expect(err.workflowType).toBe('LongRunningWorkflow');
    expect(err.input).toEqual({ iteration: 42 });
    expect(err.message).toContain('continue-as-new');
    expect(err instanceof Error).toBe(true);
  });
});

describe('ScheduleClient', () => {
  it('should create, describe, list, and delete schedules', async () => {
    const sc = new ScheduleClient('default');

    const id = await sc.create({
      scheduleId: 'daily-report',
      workflowType: 'GenerateReport',
      taskQueue: 'reports',
      cronSchedule: '0 9 * * *',
    });
    expect(id).toBe('daily-report');

    const desc = await sc.describe('daily-report');
    expect(desc.scheduleId).toBe('daily-report');

    const list = await sc.list();
    expect(Array.isArray(list)).toBe(true);

    await sc.delete('daily-report');
  });
});

describe('SearchAttributesClient', () => {
  it('should upsert and search', async () => {
    const sac = new SearchAttributesClient('default');

    await sac.upsert('wf-1', { CustomField: 'value1' });
    const workflows = await sac.listWorkflows("CustomField = 'value1'");
    expect(Array.isArray(workflows)).toBe(true);

    const count = await sac.countWorkflows("CustomField = 'value1'");
    expect(count).toBeGreaterThanOrEqual(0);
  });
});

describe('BatchOperationClient', () => {
  it('should start, describe, and list batch operations', async () => {
    const bc = new BatchOperationClient('default');

    const jobId = await bc.start({
      operation: 'terminate',
      query: "WorkflowType = 'TestWorkflow'",
      reason: 'cleanup',
    });
    expect(jobId).toBeTruthy();

    const desc = await bc.describe(jobId);
    expect(desc.jobId).toBe(jobId);

    const list = await bc.list();
    expect(Array.isArray(list)).toBe(true);
  });
});

describe('Saga', () => {
  it('should execute all steps successfully', async () => {
    const saga = new Saga();
    const order: string[] = [];

    saga.addStep('step1', () => { order.push('exec-1'); return 'r1'; }, () => { order.push('comp-1'); });
    saga.addStep('step2', () => { order.push('exec-2'); return 'r2'; }, () => { order.push('comp-2'); });

    const { results, error } = await saga.execute();
    expect(error).toBeNull();
    expect(results).toEqual(['r1', 'r2']);
    expect(order).toEqual(['exec-1', 'exec-2']);
  });

  it('should compensate on failure', async () => {
    const saga = new Saga();
    const compensated: string[] = [];

    saga.addStep('step1', () => 'ok', () => { compensated.push('step1'); });
    saga.addStep('step2-fails', () => { throw new Error('step2 failed'); }, () => { compensated.push('step2'); });

    const { results, error } = await saga.execute();
    expect(error).not.toBeNull();
    expect(error!.message).toContain('step2 failed');
    expect(compensated).toEqual(['step1']);
  });
});
