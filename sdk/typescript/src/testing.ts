/**
 * VELOCITY-WorkFlow TypeScript SDK - Testing utilities.
 *
 * Provides test environment and mock client for unit testing workflows
 * without requiring a running VELOCITY-WorkFlow server.
 *
 * @packageDocumentation
 */

import { VelocityClient, WorkflowStatus, WorkflowHandle, WorkflowDescription } from './client';
import { WorkflowNotFoundError, WorkflowAlreadyCompletedError } from './errors';

/** Mock client for testing workflows without a server. */
export class MockVelocityClient {
  private workflows = new Map<bigint, any>();
  private signals = new Map<bigint, Array<{ signalName: string; payload: Buffer }>>();
  private nextKey = 1n;

  /** Start a mock workflow. */
  async startWorkflow(options: {
    workflowType: string;
    namespace?: string;
    taskQueue?: string;
    totalSteps?: number;
    input?: Buffer;
  }): Promise<WorkflowHandle> {
    const key = this.nextKey++;
    this.workflows.set(key, {
      workflowType: options.workflowType,
      namespace: options.namespace || 'default',
      taskQueue: options.taskQueue || 'default',
      totalSteps: options.totalSteps || 1,
      currentStep: 0,
      status: WorkflowStatus.Running,
      result: null,
    });
    this.signals.set(key, []);

    return {
      workflowKey: key,
      workflowId: key.toString(),
      status: WorkflowStatus.Running,
    };
  }

  /** Describe a mock workflow. */
  async describeWorkflow(workflowKey: bigint): Promise<WorkflowDescription> {
    const wf = this.workflows.get(workflowKey);
    if (!wf) {
      throw new WorkflowNotFoundError(workflowKey);
    }

    return {
      workflowKey,
      workflowId: workflowKey.toString(),
      status: wf.status,
      currentStep: wf.currentStep,
      totalSteps: wf.totalSteps,
      namespace: wf.namespace,
      result: wf.result,
      startTimeMs: 0,
      closeTimeMs: null,
      taskQueue: '',
    };
  }

  /** Send a signal to a mock workflow. */
  async signalWorkflow(
    workflowKey: bigint,
    signalName: string,
    payload: Buffer = Buffer.alloc(0)
  ): Promise<boolean> {
    const wf = this.workflows.get(workflowKey);
    if (!wf) {
      throw new WorkflowNotFoundError(workflowKey);
    }

    const signalList = this.signals.get(workflowKey) || [];
    signalList.push({ signalName, payload });
    this.signals.set(workflowKey, signalList);
    return true;
  }

  /** Complete a mock workflow. */
  async completeWorkflow(workflowKey: bigint, result: Buffer = Buffer.alloc(0)): Promise<boolean> {
    const wf = this.workflows.get(workflowKey);
    if (!wf) {
      throw new WorkflowNotFoundError(workflowKey);
    }
    if (wf.status !== WorkflowStatus.Running) {
      throw new WorkflowAlreadyCompletedError(workflowKey);
    }

    wf.status = WorkflowStatus.Completed;
    wf.result = result;
    return true;
  }

  /** Fail a mock workflow. */
  async failWorkflow(workflowKey: bigint, reason: string = ''): Promise<boolean> {
    const wf = this.workflows.get(workflowKey);
    if (!wf) {
      throw new WorkflowNotFoundError(workflowKey);
    }
    if (wf.status !== WorkflowStatus.Running) {
      throw new WorkflowAlreadyCompletedError(workflowKey);
    }

    wf.status = WorkflowStatus.Failed;
    return true;
  }

  /** Cancel a mock workflow. */
  async cancelWorkflow(workflowKey: bigint): Promise<boolean> {
    const wf = this.workflows.get(workflowKey);
    if (!wf) {
      throw new WorkflowNotFoundError(workflowKey);
    }

    wf.status = WorkflowStatus.Canceled;
    return true;
  }

  /** Get all signals received by a workflow. */
  getSignals(workflowKey: bigint): Array<{ signalName: string; payload: Buffer }> {
    return this.signals.get(workflowKey) || [];
  }

  /** Close the mock client (no-op). */
  async close(): Promise<void> {
    // No-op for mock
  }
}

/** Test environment for running workflows in isolation. */
export class TestWorkflowEnvironment {
  client: MockVelocityClient;
  private timeOffset = 0;

  constructor() {
    this.client = new MockVelocityClient();
  }

  /** Start a workflow in the test environment. */
  async startWorkflow(options: {
    workflowType: string;
    namespace?: string;
    taskQueue?: string;
    totalSteps?: number;
    input?: Buffer;
  }): Promise<WorkflowHandle> {
    return this.client.startWorkflow(options);
  }

  /** Complete a workflow in the test environment. */
  async completeWorkflow(workflowKey: bigint, result?: Buffer): Promise<boolean> {
    return this.client.completeWorkflow(workflowKey, result);
  }

  /** Signal a workflow in the test environment. */
  async signalWorkflow(
    workflowKey: bigint,
    signalName: string,
    payload?: Buffer
  ): Promise<boolean> {
    return this.client.signalWorkflow(workflowKey, signalName, payload);
  }

  /** Advance the test environment's clock. */
  timeSkip(seconds: number): void {
    this.timeOffset += seconds;
  }

  /** Get the current test time (real time + offset). */
  getCurrentTime(): number {
    return Date.now() / 1000 + this.timeOffset;
  }

  /** Assert that a workflow has completed. */
  async assertWorkflowCompleted(workflowKey: bigint): Promise<void> {
    const desc = await this.client.describeWorkflow(workflowKey);
    if (desc.status !== WorkflowStatus.Completed) {
      throw new Error(
        `Expected workflow ${workflowKey} to be completed, but status is ${desc.status}`
      );
    }
  }

  /** Assert that a workflow received a specific signal. */
  assertSignalReceived(workflowKey: bigint, signalName: string): void {
    const signals = this.client.getSignals(workflowKey);
    const signalNames = signals.map((s) => s.signalName);
    if (!signalNames.includes(signalName)) {
      throw new Error(
        `Expected signal '${signalName}' not found. Received: ${signalNames.join(', ')}`
      );
    }
  }

  /** Reset the test environment. */
  reset(): void {
    this.client = new MockVelocityClient();
    this.timeOffset = 0;
  }
}

/** Assert that a workflow has completed. */
export async function assertWorkflowCompleted(
  client: MockVelocityClient,
  workflowKey: bigint
): Promise<void> {
  const desc = await client.describeWorkflow(workflowKey);
  if (desc.status !== WorkflowStatus.Completed) {
    throw new Error(
      `Expected workflow ${workflowKey} to be completed, but status is ${desc.status}`
    );
  }
}

/** Assert that a workflow received a specific signal. */
export function assertSignalReceived(
  client: MockVelocityClient,
  workflowKey: bigint,
  signalName: string
): void {
  const signals = client.getSignals(workflowKey);
  const signalNames = signals.map((s) => s.signalName);
  if (!signalNames.includes(signalName)) {
    throw new Error(
      `Expected signal '${signalName}' not found. Received: ${signalNames.join(', ')}`
    );
  }
}
