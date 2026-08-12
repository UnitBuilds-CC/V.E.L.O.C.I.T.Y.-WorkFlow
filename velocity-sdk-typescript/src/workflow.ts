/**
 * Workflow definition API
 */

import { ActivityOptions, ChildWorkflowOptions, TimerOptions } from './types';
import { Activity } from './activity';

export interface WorkflowContext {
  workflowId: string;
  runId: string;
  taskQueue: string;
  memo?: Record<string, any>;
  searchAttributes?: Record<string, any>;
  /** @internal Worker reference for in-workflow operations */
  _worker?: any;
}

export type WorkflowFunction<TInput = any, TOutput = any> = (
  ctx: WorkflowContext,
  input: TInput
) => Promise<TOutput>;

export class Workflow {
  private static workflows = new Map<string, WorkflowFunction>();

  /**
   * Register a workflow function
   */
  static register<TInput = any, TOutput = any>(
    name: string,
    fn: WorkflowFunction<TInput, TOutput>
  ): void {
    Workflow.workflows.set(name, fn as WorkflowFunction);
  }

  /**
   * Get a registered workflow function
   */
  static get(name: string): WorkflowFunction | undefined {
    return Workflow.workflows.get(name);
  }

  /**
   * Check if a workflow is registered
   */
  static has(name: string): boolean {
    return Workflow.workflows.has(name);
  }

  /**
   * Clear all registered workflows (for testing)
   */
  static clear(): void {
    Workflow.workflows.clear();
  }
}

/**
 * Define a workflow
 */
export function defineWorkflow<TInput = any, TOutput = any>(
  name: string,
  fn: WorkflowFunction<TInput, TOutput>
): void {
  Workflow.register(name, fn);
}

/**
 * Workflow context helpers — these delegate to the worker bound to the context.
 */
export class WorkflowHelpers {
  private static _currentContext: WorkflowContext | null = null;

  /** @internal Set the current workflow context for helpers */
  static setCurrentContext(ctx: WorkflowContext | null): void {
    WorkflowHelpers._currentContext = ctx;
  }

  /** @internal Get the current workflow context */
  static getCurrentContext(): WorkflowContext | null {
    return WorkflowHelpers._currentContext;
  }

  /**
   * Schedule an activity — executes the activity locally on the same worker.
   */
  static async executeActivity<TInput = any, TOutput = any>(
    options: ActivityOptions
  ): Promise<TOutput> {
    const ctx = WorkflowHelpers._currentContext;
    if (ctx?._worker) {
      return ctx._worker.executeActivityLocal(options.activityType, options.input) as Promise<TOutput>;
    }
    throw new Error('No worker bound to workflow context — use worker.executeWorkflow() for local execution');
  }

  /**
   * Sleep for a duration (milliseconds).
   */
  static async sleep(durationMs: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, durationMs));
  }

  /**
   * Start a child workflow — executes locally on the same worker.
   */
  static async executeChildWorkflow<TInput = any, TOutput = any>(
    options: ChildWorkflowOptions
  ): Promise<TOutput> {
    const ctx = WorkflowHelpers._currentContext;
    if (ctx?._worker) {
      return ctx._worker.executeChildWorkflowLocal(
        options.workflowType,
        options.workflowId || `child-${ctx.workflowId}-${options.workflowType}`,
        options.input
      ) as Promise<TOutput>;
    }
    throw new Error('No worker bound to workflow context — use worker.executeWorkflow() for local execution');
  }

  /**
   * Get current workflow info.
   */
  static getInfo(): WorkflowContext {
    if (!WorkflowHelpers._currentContext) {
      throw new Error('No active workflow context — must be called from within a workflow');
    }
    return WorkflowHelpers._currentContext;
  }
}
