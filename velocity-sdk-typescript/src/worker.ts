/**
 * Velocity Worker - Executes workflows and activities
 */

import { Connection, ConnectionOptions } from './connection';
import { Workflow, WorkflowFunction, WorkflowHelpers } from './workflow';
import { Activity, ActivityFunction } from './activity';
import { WorkflowContext } from './workflow';
import { ActivityContext } from './types';

export interface WorkerOptions {
  connection?: ConnectionOptions;
  namespace?: string;
  taskQueue: string;
  workflows?: Map<string, WorkflowFunction>;
  activities?: Map<string, ActivityFunction>;
}

export class Worker {
  private connection: Connection;
  private namespace: string;
  private taskQueue: string;
  private running: boolean = false;
  private workflowPoller?: Promise<void>;
  private activityPoller?: Promise<void>;

  constructor(options: WorkerOptions) {
    this.namespace = options.namespace || 'default';
    this.taskQueue = options.taskQueue;
    
    if (options.connection) {
      this.connection = new Connection(options.connection);
    } else {
      this.connection = new Connection({ address: 'localhost:7233' });
    }

    // Register workflows and activities if provided
    if (options.workflows) {
      options.workflows.forEach((fn, name) => Workflow.register(name, fn));
    }
    if (options.activities) {
      options.activities.forEach((fn, name) => Activity.register(name, fn));
    }
  }

  /**
   * Start the worker
   */
  async start(): Promise<void> {
    if (this.running) {
      throw new Error('Worker is already running');
    }

    this.running = true;
    console.log(`Worker started for task queue: ${this.taskQueue}`);

    // Start polling for workflow tasks
    this.workflowPoller = this.pollWorkflowTasks();

    // Start polling for activity tasks
    this.activityPoller = this.pollActivityTasks();

    // Wait for both pollers
    await Promise.all([this.workflowPoller, this.activityPoller]);
  }

  /**
   * Stop the worker
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.running = false;
    console.log('Worker stopping...');

    // Wait for pollers to finish
    if (this.workflowPoller) {
      await this.workflowPoller;
    }
    if (this.activityPoller) {
      await this.activityPoller;
    }

    this.connection.close();
    console.log('Worker stopped');
  }

  /**
   * Poll for workflow tasks
   */
  private async pollWorkflowTasks(): Promise<void> {
    while (this.running) {
      try {
        const task = await this.connection.pollWorkflowTaskQueue({
          namespace: this.namespace,
          taskQueue: this.taskQueue,
        });

        if (task && task.task_token) {
          await this.handleWorkflowTask(task);
        }
      } catch (error) {
        console.error('Error polling workflow task:', error);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }

  /**
   * Poll for activity tasks
   */
  private async pollActivityTasks(): Promise<void> {
    while (this.running) {
      try {
        const task = await this.connection.pollActivityTaskQueue({
          namespace: this.namespace,
          taskQueue: this.taskQueue,
        });

        if (task && task.task_token) {
          await this.handleActivityTask(task);
        }
      } catch (error) {
        console.error('Error polling activity task:', error);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }

  /**
   * Handle a workflow task
   */
  private async handleWorkflowTask(task: any): Promise<void> {
    try {
      const workflowType = task.workflow_type?.name || task.workflow_type;
      const workflowFn = Workflow.get(workflowType);

      if (!workflowFn) {
        throw new Error(`Workflow ${workflowType} not registered`);
      }

      // Create workflow context
      const ctx: WorkflowContext = {
        workflowId: task.workflow_execution?.workflow_id || '',
        runId: task.workflow_execution?.run_id || '',
        taskQueue: this.taskQueue,
      };

      // Parse input
      const input = task.input ? JSON.parse(task.input.data.toString()) : undefined;

      // Execute workflow
      const result = await workflowFn(ctx, input);

      // Complete workflow task with result
      await this.connection.respondWorkflowTaskCompleted({
        taskToken: task.task_token,
        commands: [
          {
            attributes: {
              completeWorkflow: {
                result: result ? { data: Buffer.from(JSON.stringify(result)), encoding: 0, metadata: {} } : undefined,
              },
            },
          },
        ],
      });

      console.log(`Workflow ${workflowType} completed`);
    } catch (error) {
      console.error('Error handling workflow task:', error);
      
      // Fail workflow
      await this.connection.respondWorkflowTaskCompleted({
        taskToken: task.task_token,
        commands: [
          {
            attributes: {
              failWorkflow: {
                failure: {
                  data: Buffer.from(error instanceof Error ? error.message : String(error)),
                  encoding: 0,
                  metadata: {},
                },
              },
            },
          },
        ],
      });
    }
  }

  /**
   * Handle an activity task
   */
  private async handleActivityTask(task: any): Promise<void> {
    try {
      const activityType = task.activity_type?.name || task.activity_type;
      const activityFn = Activity.get(activityType);

      if (!activityFn) {
        throw new Error(`Activity ${activityType} not registered`);
      }

      // Create activity context
      const ctx: ActivityContext = {
        taskToken: task.task_token,
        workflowExecution: {
          workflowId: task.workflow_execution?.workflow_id || '',
          runId: task.workflow_execution?.run_id || '',
        },
        activityId: task.activity_id || '',
        activityType: activityType,
        scheduledTime: task.scheduled_time?.seconds ? task.scheduled_time.seconds * 1000 : 0,
        startedTime: task.started_time?.seconds ? task.started_time.seconds * 1000 : 0,
        attempt: task.attempt || 1,
      };

      // Parse input
      const input = task.input ? JSON.parse(task.input.data.toString()) : undefined;

      // Execute activity
      const result = await activityFn(ctx, input);

      // Complete activity task
      await this.connection.respondActivityTaskCompleted({
        taskToken: task.task_token,
        result,
      });

      console.log(`Activity ${activityType} completed`);
    } catch (error) {
      console.error('Error handling activity task:', error);
      
      // Fail activity
      await this.connection.respondActivityTaskFailed({
        taskToken: task.task_token,
        failure: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Check if worker is running
   */
  isRunning(): boolean {
    return this.running;
  }

  /**
   * Get task queue name
   */
  getTaskQueue(): string {
    return this.taskQueue;
  }

  // ─── Local Execution (for testing or embedded mode) ───────────────────────

  /**
   * Execute a workflow locally without polling.
   */
  async executeWorkflow(workflowId: string, workflowType: string, input?: any): Promise<any> {
    const workflowFn = Workflow.get(workflowType);
    if (!workflowFn) {
      throw new Error(`Workflow "${workflowType}" not registered`);
    }

    const ctx: WorkflowContext = {
      workflowId,
      runId: `run-${workflowId}-${Date.now()}`,
      taskQueue: this.taskQueue,
      _worker: this,
    };

    // Set the context for WorkflowHelpers
    const prevCtx = WorkflowHelpers.getCurrentContext();
    WorkflowHelpers.setCurrentContext(ctx);
    try {
      return await workflowFn(ctx, input);
    } finally {
      WorkflowHelpers.setCurrentContext(prevCtx);
    }
  }

  /**
   * Execute an activity locally (called by WorkflowHelpers.executeActivity).
   */
  async executeActivityLocal(activityType: string, input?: any): Promise<any> {
    const activityFn = Activity.get(activityType);
    if (!activityFn) {
      throw new Error(`Activity "${activityType}" not registered`);
    }

    const actCtx: ActivityContext = {
      taskToken: `local-${Date.now()}`,
      workflowExecution: { workflowId: '', runId: '' },
      activityId: `act-${Date.now()}`,
      activityType,
      scheduledTime: Date.now(),
      startedTime: Date.now(),
      attempt: 1,
    };

    return activityFn(actCtx, input);
  }

  /**
   * Execute a child workflow locally (called by WorkflowHelpers.executeChildWorkflow).
   */
  async executeChildWorkflowLocal(workflowType: string, workflowId: string, input?: any): Promise<any> {
    return this.executeWorkflow(workflowId, workflowType, input);
  }
}
