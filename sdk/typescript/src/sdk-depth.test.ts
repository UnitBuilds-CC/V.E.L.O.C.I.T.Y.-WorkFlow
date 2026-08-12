/**
 * VELOCITY-WorkFlow TypeScript SDK - Depth tests.
 *
 * Comprehensive tests for error hierarchy, interceptors, mock client,
 * and testing utilities. These tests verify real behavior, not just mocking.
 */

import {
  VelocityError,
  WorkflowNotFoundError,
  WorkflowAlreadyCompletedError,
  ConnectionError,
  TimeoutError,
  RateLimitError,
  AuthenticationError,
  InternalError,
} from './errors';
import {
  WorkflowInterceptor,
  ActivityInterceptor,
  LoggingInterceptor,
  MetricsInterceptor,
  InterceptorChain,
} from './interceptors';
import { MockVelocityClient, TestWorkflowEnvironment } from './testing';
import { WorkflowStatus } from './client';

// ─── Error Hierarchy Tests ───────────────────────────────────────────────────

describe('Error Hierarchy', () => {
  test('VelocityError base with all parameters', () => {
    const error = new VelocityError('Test error', 99, true, { key: 'value' });
    expect(error.toString()).toBe('VelocityError[99]: Test error (retryable)');
    expect(error.errorCode).toBe(99);
    expect(error.retryable).toBe(true);
    expect(error.details).toEqual({ key: 'value' });
    expect(error.message).toBe('Test error');
  });

  test('WorkflowNotFoundError with error code 1', () => {
    const error = new WorkflowNotFoundError(42n);
    expect(error.errorCode).toBe(1);
    expect(error.retryable).toBe(false);
    expect(error.workflowKey).toBe(42n);
    expect(error.message).toContain('42');
  });

  test('WorkflowAlreadyCompletedError with error code 2', () => {
    const error = new WorkflowAlreadyCompletedError(100n);
    expect(error.errorCode).toBe(2);
    expect(error.retryable).toBe(false);
    expect(error.workflowKey).toBe(100n);
    expect(error.message).toContain('100');
  });

  test('ConnectionError with error code 3', () => {
    const error = new ConnectionError('localhost:8080');
    expect(error.errorCode).toBe(3);
    expect(error.retryable).toBe(true);
    expect(error.target).toBe('localhost:8080');
    expect(error.message).toContain('localhost:8080');
  });

  test('TimeoutError with error code 4', () => {
    const error = new TimeoutError('start_workflow', 5000);
    expect(error.errorCode).toBe(4);
    expect(error.retryable).toBe(true);
    expect(error.operation).toBe('start_workflow');
    expect(error.timeoutMs).toBe(5000);
    expect(error.message).toContain('5000');
  });

  test('RateLimitError with error code 5', () => {
    const error = new RateLimitError(1000);
    expect(error.errorCode).toBe(5);
    expect(error.retryable).toBe(true);
    expect(error.retryAfterMs).toBe(1000);
  });

  test('AuthenticationError with error code 6', () => {
    const error = new AuthenticationError();
    expect(error.errorCode).toBe(6);
    expect(error.retryable).toBe(false);
    expect(error.message).toContain('Authentication');
  });

  test('InternalError with error code 7', () => {
    const error = new InternalError();
    expect(error.errorCode).toBe(7);
    expect(error.retryable).toBe(true);
    expect(error.message).toContain('Internal');
  });

  test('All exceptions inherit from VelocityError', () => {
    expect(new WorkflowNotFoundError(1n)).toBeInstanceOf(VelocityError);
    expect(new WorkflowAlreadyCompletedError(1n)).toBeInstanceOf(VelocityError);
    expect(new ConnectionError('test')).toBeInstanceOf(VelocityError);
    expect(new TimeoutError('test', 1000)).toBeInstanceOf(VelocityError);
    expect(new RateLimitError()).toBeInstanceOf(VelocityError);
    expect(new AuthenticationError()).toBeInstanceOf(VelocityError);
    expect(new InternalError()).toBeInstanceOf(VelocityError);
  });
});

// ─── Interceptor Tests ───────────────────────────────────────────────────────

describe('Interceptors', () => {
  test('InterceptorChain execution order', async () => {
    const executionOrder: string[] = [];

    class TrackingInterceptor implements WorkflowInterceptor {
      constructor(private name: string) {}
      onStart() {
        executionOrder.push(this.name);
      }
    }

    const chain = new InterceptorChain();
    chain.add(new TrackingInterceptor('first'));
    chain.add(new TrackingInterceptor('second'));
    chain.add(new TrackingInterceptor('third'));

    await chain.invokeWorkflowStart('test_workflow', 1n);

    expect(executionOrder).toEqual(['first', 'second', 'third']);
  });

  test('LoggingInterceptor produces correct output', () => {
    const logs: string[] = [];
    const originalLog = console.log;
    console.log = (...args: any[]) => logs.push(args.join(' '));

    const interceptor = new LoggingInterceptor('[TEST]');
    interceptor.onStart('test_workflow', 42n);
    interceptor.onSignal(42n, 'test_signal');

    console.log = originalLog;

    expect(logs.some(log => log.includes('Workflow started') && log.includes('42'))).toBe(true);
    expect(logs.some(log => log.includes('Workflow signal') && log.includes('test_signal'))).toBe(true);
  });

  test('MetricsInterceptor tracks workflow metrics', () => {
    const interceptor = new MetricsInterceptor();

    interceptor.onStart();
    interceptor.onStart();

    expect(interceptor.workflowStarts).toBe(2);
  });

  test('MetricsInterceptor tracks activity metrics', () => {
    const interceptor = new MetricsInterceptor();

    interceptor.onExecute();
    interceptor.onExecute();

    expect(interceptor.activityExecutions).toBe(2);
  });

  test('InterceptorChain with multiple interceptor types', async () => {
    const chain = new InterceptorChain();
    const metrics = new MetricsInterceptor();
    chain.add(metrics);

    await chain.invokeWorkflowStart('workflow', 1n);
    await chain.invokeActivityExecute('activity', 'act_1');

    expect(metrics.workflowStarts).toBe(1);
    expect(metrics.activityExecutions).toBe(1);
  });
});

// ─── Mock Client Tests ───────────────────────────────────────────────────────

describe('MockVelocityClient', () => {
  test('startWorkflow returns valid handle', async () => {
    const client = new MockVelocityClient();
    const handle = await client.startWorkflow({ workflowType: 'test_workflow', totalSteps: 3 });

    expect(handle.workflowKey).toBeDefined();
    expect(handle.workflowId).toBeDefined();
    expect(handle.status).toBe(WorkflowStatus.Running);
  });

  test('describeWorkflow returns correct state', async () => {
    const client = new MockVelocityClient();
    const handle = await client.startWorkflow({ workflowType: 'test_workflow', totalSteps: 5 });

    const description = await client.describeWorkflow(handle.workflowKey);
    expect(description.workflowKey).toBe(handle.workflowKey);
    expect(description.status).toBe(WorkflowStatus.Running);
    expect(description.totalSteps).toBe(5);
    expect(description.currentStep).toBe(0);
  });

  test('signalWorkflow succeeds', async () => {
    const client = new MockVelocityClient();
    const handle = await client.startWorkflow({ workflowType: 'test_workflow' });

    const result = await client.signalWorkflow(handle.workflowKey, 'test_signal', Buffer.from('data'));
    expect(result).toBe(true);
  });

  test('completeWorkflow changes status to Completed', async () => {
    const client = new MockVelocityClient();
    const handle = await client.startWorkflow({ workflowType: 'test_workflow' });

    const result = await client.completeWorkflow(handle.workflowKey, Buffer.from('success'));
    expect(result).toBe(true);

    const description = await client.describeWorkflow(handle.workflowKey);
    expect(description.status).toBe(WorkflowStatus.Completed);
  });

  test('failWorkflow changes status to Failed', async () => {
    const client = new MockVelocityClient();
    const handle = await client.startWorkflow({ workflowType: 'test_workflow' });

    const result = await client.failWorkflow(handle.workflowKey, 'error occurred');
    expect(result).toBe(true);

    const description = await client.describeWorkflow(handle.workflowKey);
    expect(description.status).toBe(WorkflowStatus.Failed);
  });

  test('cancelWorkflow changes status to Canceled', async () => {
    const client = new MockVelocityClient();
    const handle = await client.startWorkflow({ workflowType: 'test_workflow' });

    const result = await client.cancelWorkflow(handle.workflowKey);
    expect(result).toBe(true);

    const description = await client.describeWorkflow(handle.workflowKey);
    expect(description.status).toBe(WorkflowStatus.Canceled);
  });

  test('describeWorkflow throws WorkflowNotFoundError for non-existent workflow', async () => {
    const client = new MockVelocityClient();

    await expect(client.describeWorkflow(999n)).rejects.toThrow(WorkflowNotFoundError);
  });

  test('completeWorkflow throws WorkflowAlreadyCompletedError when completing twice', async () => {
    const client = new MockVelocityClient();
    const handle = await client.startWorkflow({ workflowType: 'test_workflow' });

    await client.completeWorkflow(handle.workflowKey);

    await expect(client.completeWorkflow(handle.workflowKey)).rejects.toThrow(WorkflowAlreadyCompletedError);
  });
});

// ─── Test Workflow Environment Tests ─────────────────────────────────────────

describe('TestWorkflowEnvironment', () => {
  test('environment creation', () => {
    const env = new TestWorkflowEnvironment();
    expect(env.client).toBeDefined();
  });

  test('startWorkflow in test environment', async () => {
    const env = new TestWorkflowEnvironment();
    const handle = await env.client.startWorkflow({ workflowType: 'test_workflow', totalSteps: 2 });

    expect(handle.workflowKey).toBeDefined();
  });

  test('assert workflow is running', async () => {
    const env = new TestWorkflowEnvironment();
    const handle = await env.client.startWorkflow({ workflowType: 'test_workflow' });

    const description = await env.client.describeWorkflow(handle.workflowKey);
    expect(description.status).toBe(WorkflowStatus.Running);
  });

  test('assert workflow is completed', async () => {
    const env = new TestWorkflowEnvironment();
    const handle = await env.client.startWorkflow({ workflowType: 'test_workflow' });
    await env.client.completeWorkflow(handle.workflowKey);

    const description = await env.client.describeWorkflow(handle.workflowKey);
    expect(description.status).toBe(WorkflowStatus.Completed);
  });

  test('manage multiple workflows', async () => {
    const env = new TestWorkflowEnvironment();

    const handles = [];
    for (let i = 0; i < 5; i++) {
      const handle = await env.client.startWorkflow({ workflowType: `workflow_${i}` });
      handles.push(handle);
    }

    await env.client.completeWorkflow(handles[0].workflowKey);
    await env.client.completeWorkflow(handles[1].workflowKey);
    await env.client.failWorkflow(handles[2].workflowKey);

    expect((await env.client.describeWorkflow(handles[0].workflowKey)).status).toBe(WorkflowStatus.Completed);
    expect((await env.client.describeWorkflow(handles[1].workflowKey)).status).toBe(WorkflowStatus.Completed);
    expect((await env.client.describeWorkflow(handles[2].workflowKey)).status).toBe(WorkflowStatus.Failed);
    expect((await env.client.describeWorkflow(handles[3].workflowKey)).status).toBe(WorkflowStatus.Running);
    expect((await env.client.describeWorkflow(handles[4].workflowKey)).status).toBe(WorkflowStatus.Running);
  });
});
