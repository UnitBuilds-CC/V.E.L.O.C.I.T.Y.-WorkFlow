/**
 * VELOCITY-WorkFlow TypeScript SDK - Typed workflow stub.
 *
 * Provides a high-level interface for workflow execution with type safety.
 *
 * @packageDocumentation
 */

import { VelocityClient, WorkflowHandle, WorkflowStatus } from './client';
import { PayloadCodec, JsonCodec } from './payload-codec';

/** Configuration for WorkflowStub. */
export interface WorkflowStubOptions {
  /** Workflow type name. */
  workflowType: string;
  /** Namespace (default: "default"). */
  namespace?: string;
  /** Task queue name (default: "default"). */
  taskQueue?: string;
  /** Execution timeout in milliseconds. */
  executionTimeoutMs?: number;
  /** Payload codec (default: JsonCodec). */
  codec?: PayloadCodec;
}

/**
 * Typed workflow execution stub.
 *
 * Provides a convenient interface for starting, signaling, querying,
 * and waiting for workflow results.
 *
 * @example
 * ```typescript
 * const stub = new WorkflowStub(client, {
 *   workflowType: 'order-processing',
 *   namespace: 'default',
 *   taskQueue: 'orders',
 * });
 *
 * await stub.start({ orderId: '12345' });
 * await stub.signal('approve', { approved: true });
 * const result = await stub.result<{ status: string }>();
 * ```
 */
export class WorkflowStub<TInput = unknown, TResult = unknown> {
  private readonly client: VelocityClient;
  private readonly options: Required<WorkflowStubOptions>;
  private readonly codec: PayloadCodec;
  private handle: WorkflowHandle | null = null;

  constructor(client: VelocityClient, options: WorkflowStubOptions) {
    this.client = client;
    this.options = {
      workflowType: options.workflowType,
      namespace: options.namespace ?? 'default',
      taskQueue: options.taskQueue ?? 'default',
      executionTimeoutMs: options.executionTimeoutMs ?? 60_000,
      codec: options.codec ?? new JsonCodec(),
    };
    this.codec = this.options.codec;
  }

  /**
   * Start workflow execution.
   *
   * @param input - Input data for the workflow (will be encoded).
   * @returns WorkflowHandle for the started workflow.
   */
  async start(input?: TInput): Promise<WorkflowHandle> {
    const payload = input !== undefined ? this.codec.encode(input) : new Uint8Array(0);

    this.handle = await this.client.startWorkflow({
      workflowType: this.options.workflowType,
      namespace: this.options.namespace,
      taskQueue: this.options.taskQueue,
      input: payload,
    });

    return this.handle;
  }

  /**
   * Send a signal to the workflow.
   *
   * @param signalName - Name of the signal.
   * @param data - Signal payload (will be encoded).
   */
  async signal(signalName: string, data?: unknown): Promise<void> {
    this.ensureStarted();
    const payload = data !== undefined ? this.codec.encode(data) : new Uint8Array(0);
    await this.client.signalWorkflow(this.handle!.workflowKey, signalName, payload);
  }

  /**
   * Query the workflow state.
   *
   * @param queryType - Type of query.
   * @param args - Query arguments (will be encoded).
   * @returns Decoded query result.
   */
  async query<T = unknown>(queryType: string, args?: unknown): Promise<T> {
    this.ensureStarted();
    const payload = args !== undefined ? this.codec.encode(args) : new Uint8Array(0);
    const result = await this.client.queryWorkflow(this.handle!.workflowKey, queryType, payload);
    return (result ? this.codec.decode(result) : null) as T;
  }

  /**
   * Wait for workflow completion and return the result.
   *
   * @returns Decoded workflow result.
   */
  async result(): Promise<TResult> {
    this.ensureStarted();
    const description = await this.client.waitForCompletion(this.handle!.workflowKey);
    if (description.result) {
      return this.codec.decode(description.result) as TResult;
    }
    return null as unknown as TResult;
  }

  /**
   * Cancel the workflow.
   */
  async cancel(): Promise<void> {
    this.ensureStarted();
    await this.client.cancelWorkflow(this.handle!.workflowKey);
  }

  /**
   * Terminate the workflow.
   *
   * @param reason - Termination reason.
   */
  async terminate(reason?: string): Promise<void> {
    this.ensureStarted();
    await this.client.terminateWorkflow(this.handle!.workflowKey, reason ?? '');
  }

  /**
   * Get the underlying workflow handle.
   */
  getHandle(): WorkflowHandle | null {
    return this.handle;
  }

  /**
   * Get the workflow key.
   */
  get workflowKey(): bigint | null {
    return this.handle?.workflowKey ?? null;
  }

  /**
   * Ensure the workflow has been started.
   * @throws Error if the workflow has not been started.
   */
  private ensureStarted(): void {
    if (!this.handle) {
      throw new Error('Workflow not started. Call start() first.');
    }
  }
}
