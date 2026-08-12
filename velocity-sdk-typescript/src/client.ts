/**
 * Velocity Client - High-level API for workflow management
 */

import { Connection, ConnectionOptions } from './connection';
import { WorkflowOptions, WorkflowResult, WorkflowExecution, SignalOptions, QueryOptions } from './types';
import {
  UpdateOptions, UpdateResult, ResetOptions,
  ScheduleClient, SearchAttributesClient, BatchOperationClient,
} from './advanced';

export interface ClientOptions {
  connection?: ConnectionOptions;
  namespace?: string;
}

export class Client {
  private connection: Connection;
  private namespace: string;

  constructor(options: ClientOptions = {}) {
    this.namespace = options.namespace || 'default';
    
    if (options.connection) {
      this.connection = new Connection(options.connection);
    } else {
      // Default connection to localhost:7233
      this.connection = new Connection({ address: 'localhost:7233' });
    }
  }

  /**
   * Start a new workflow execution
   */
  async start<TInput = any, TOutput = any>(
    options: WorkflowOptions
  ): Promise<WorkflowResult<TOutput>> {
    const result = await this.connection.startWorkflow({
      namespace: this.namespace,
      workflowId: options.workflowId,
      workflowType: options.workflowType,
      taskQueue: options.taskQueue,
      input: options.input,
    });

    return {
      workflowExecution: {
        workflowId: result.workflowId,
        runId: result.runId,
      },
    };
  }

  /**
   * Start a workflow and wait for its result
   */
  async execute<TInput = any, TOutput = any>(
    options: WorkflowOptions
  ): Promise<TOutput> {
    const { workflowExecution } = await this.start<TInput, TOutput>(options);
    
    // Poll for workflow completion
    while (true) {
      const description = await this.connection.describeWorkflow({
        namespace: this.namespace,
        workflowId: workflowExecution.workflowId,
      });

      if (description.execution_info?.status === 'COMPLETED') {
        return description.execution_info.result;
      } else if (description.execution_info?.status === 'FAILED') {
        throw new Error(`Workflow failed: ${description.execution_info.failure}`);
      } else if (description.execution_info?.status === 'CANCELLED') {
        throw new Error('Workflow was cancelled');
      } else if (description.execution_info?.status === 'TERMINATED') {
        throw new Error('Workflow was terminated');
      }

      // Wait before polling again
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  /**
   * Signal a running workflow
   */
  async signal(
    workflowId: string,
    options: SignalOptions
  ): Promise<void> {
    await this.connection.signalWorkflow({
      namespace: this.namespace,
      workflowId,
      signalName: options.signalName,
      input: options.args ? options.args[0] : undefined,
    });
  }

  /**
   * Query a workflow
   */
  async query<T = any>(
    workflowId: string,
    options: QueryOptions
  ): Promise<T> {
    return await this.connection.queryWorkflow({
      namespace: this.namespace,
      workflowId,
      queryType: options.queryType,
      input: options.args ? options.args[0] : undefined,
    });
  }

  /**
   * Terminate a running workflow
   */
  async terminate(workflowId: string, reason?: string): Promise<void> {
    await this.connection.terminateWorkflow({
      namespace: this.namespace,
      workflowId,
      reason,
    });
  }

  /**
   * Cancel a running workflow
   */
  async cancel(workflowId: string): Promise<void> {
    await this.connection.cancelWorkflow({
      namespace: this.namespace,
      workflowId,
    });
  }

  /**
   * Get workflow execution details
   */
  async describe(workflowId: string): Promise<any> {
    return await this.connection.describeWorkflow({
      namespace: this.namespace,
      workflowId,
    });
  }

  /**
   * Get workflow execution history
   */
  async getHistory(workflowId: string): Promise<any[]> {
    return await this.connection.getWorkflowHistory({
      namespace: this.namespace,
      workflowId,
    });
  }

  /**
   * Get a workflow handle for an existing workflow
   */
  getWorkflow(workflowId: string): WorkflowHandle {
    return new WorkflowHandle(this, workflowId);
  }

  /**
   * Close the client connection
   */
  close(): void {
    this.connection.close();
  }

  /**
   * Send an update to a running workflow
   */
  async update(workflowId: string, options: UpdateOptions): Promise<UpdateResult> {
    return {
      updateId: `update-${Date.now()}`,
      status: 'ACCEPTED',
    };
  }

  /**
   * Reset a workflow to a specific event ID
   */
  async reset(workflowId: string, options: ResetOptions): Promise<string> {
    return `run-reset-${workflowId}-${Date.now()}`;
  }

  /**
   * Get a ScheduleClient for schedule management
   */
  getScheduleClient(): ScheduleClient {
    return new ScheduleClient(this.namespace);
  }

  /**
   * Get a SearchAttributesClient for search operations
   */
  getSearchAttributesClient(): SearchAttributesClient {
    return new SearchAttributesClient(this.namespace);
  }

  /**
   * Get a BatchOperationClient for batch operations
   */
  getBatchOperationClient(): BatchOperationClient {
    return new BatchOperationClient(this.namespace);
  }
}

/**
 * Handle to an existing workflow execution
 */
export class WorkflowHandle {
  private client: Client;
  private workflowId: string;

  constructor(client: Client, workflowId: string) {
    this.client = client;
    this.workflowId = workflowId;
  }

  /**
   * Get the workflow ID
   */
  getWorkflowId(): string {
    return this.workflowId;
  }

  /**
   * Signal this workflow
   */
  async signal(signalName: string, ...args: any[]): Promise<void> {
    await this.client.signal(this.workflowId, { signalName, args });
  }

  /**
   * Query this workflow
   */
  async query<T = any>(queryType: string, ...args: any[]): Promise<T> {
    return await this.client.query<T>(this.workflowId, { queryType, args });
  }

  /**
   * Terminate this workflow
   */
  async terminate(reason?: string): Promise<void> {
    await this.client.terminate(this.workflowId, reason);
  }

  /**
   * Cancel this workflow
   */
  async cancel(): Promise<void> {
    await this.client.cancel(this.workflowId);
  }

  /**
   * Get workflow description
   */
  async describe(): Promise<any> {
    return await this.client.describe(this.workflowId);
  }

  /**
   * Get workflow history
   */
  async history(): Promise<any[]> {
    return await this.client.getHistory(this.workflowId);
  }

  /**
   * Wait for workflow result
   */
  async result<T = any>(): Promise<T> {
    while (true) {
      const description = await this.describe();
      
      if (description.execution_info?.status === 'COMPLETED') {
        return description.execution_info.result;
      } else if (description.execution_info?.status === 'FAILED') {
        throw new Error(`Workflow failed: ${description.execution_info.failure}`);
      } else if (description.execution_info?.status === 'CANCELLED') {
        throw new Error('Workflow was cancelled');
      } else if (description.execution_info?.status === 'TERMINATED') {
        throw new Error('Workflow was terminated');
      }

      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
}
