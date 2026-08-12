/**
 * VELOCITY-WorkFlow TypeScript SDK - Interceptor framework.
 *
 * Provides middleware pattern for workflow and activity lifecycle hooks.
 * Interceptors can be chained to compose logging, metrics, and custom logic.
 *
 * @packageDocumentation
 */

/** Base interface for workflow interceptors. */
export interface WorkflowInterceptor {
  /** Called before workflow starts. */
  onStart?(workflowType: string, workflowId: bigint, context?: Record<string, any>): void | Promise<void>;

  /** Called after workflow completes successfully. */
  onWorkflowComplete?(workflowId: bigint, result: any, context?: Record<string, any>): void | Promise<void>;

  /** Called when workflow fails. */
  onWorkflowFail?(workflowId: bigint, error: Error, context?: Record<string, any>): void | Promise<void>;

  /** Called when workflow receives a signal. */
  onSignal?(workflowId: bigint, signalName: string, context?: Record<string, any>): void | Promise<void>;
}

/** Base interface for activity interceptors. */
export interface ActivityInterceptor {
  /** Called before activity executes. */
  onExecute?(activityType: string, activityId: string, context?: Record<string, any>): void | Promise<void>;

  /** Called after activity completes. */
  onActivityComplete?(activityId: string, result: any, context?: Record<string, any>): void | Promise<void>;

  /** Called when activity fails. */
  onActivityFail?(activityId: string, error: Error, context?: Record<string, any>): void | Promise<void>;
}

/** Logs workflow and activity lifecycle events. */
export class LoggingInterceptor implements WorkflowInterceptor, ActivityInterceptor {
  private prefix: string;

  constructor(prefix: string = '[VELOCITY]') {
    this.prefix = prefix;
  }

  onStart(workflowType: string, workflowId: bigint): void {
    console.log(`${this.prefix} Workflow started: type=${workflowType}, id=${workflowId}`);
  }

  onWorkflowComplete(workflowId: bigint): void {
    console.log(`${this.prefix} Workflow completed: id=${workflowId}`);
  }

  onWorkflowFail(workflowId: bigint, error: Error): void {
    console.error(`${this.prefix} Workflow failed: id=${workflowId}, error=${error.message}`);
  }

  onSignal(workflowId: bigint, signalName: string): void {
    console.log(`${this.prefix} Workflow signal: id=${workflowId}, signal=${signalName}`);
  }

  onExecute(activityType: string, activityId: string): void {
    console.log(`${this.prefix} Activity executing: type=${activityType}, id=${activityId}`);
  }

  onActivityComplete(activityId: string): void {
    console.log(`${this.prefix} Activity completed: id=${activityId}`);
  }

  onActivityFail(activityId: string, error: Error): void {
    console.error(`${this.prefix} Activity failed: id=${activityId}, error=${error.message}`);
  }
}

/** Tracks workflow and activity metrics. */
export class MetricsInterceptor implements WorkflowInterceptor, ActivityInterceptor {
  workflowStarts = 0;
  workflowCompletions = 0;
  workflowFailures = 0;
  activityExecutions = 0;
  activityCompletions = 0;
  activityFailures = 0;

  onStart(): void {
    this.workflowStarts++;
  }

  onWorkflowComplete(): void {
    this.workflowCompletions++;
  }

  onWorkflowFail(): void {
    this.workflowFailures++;
  }

  onExecute(): void {
    this.activityExecutions++;
  }

  onActivityComplete(): void {
    this.activityCompletions++;
  }

  onActivityFail(): void {
    this.activityFailures++;
  }

  /** Return current metrics snapshot. */
  getMetrics(): Record<string, number> {
    return {
      workflowStarts: this.workflowStarts,
      workflowCompletions: this.workflowCompletions,
      workflowFailures: this.workflowFailures,
      activityExecutions: this.activityExecutions,
      activityCompletions: this.activityCompletions,
      activityFailures: this.activityFailures,
    };
  }
}

/** Chain of interceptors that are invoked in order. */
export class InterceptorChain {
  private interceptors: Array<WorkflowInterceptor | ActivityInterceptor>;

  constructor(interceptors: Array<WorkflowInterceptor | ActivityInterceptor> = []) {
    this.interceptors = interceptors;
  }

  /** Add an interceptor to the chain. */
  add(interceptor: WorkflowInterceptor | ActivityInterceptor): void {
    this.interceptors.push(interceptor);
  }

  /** Invoke all workflow interceptors for start event. */
  async invokeWorkflowStart(workflowType: string, workflowId: bigint, context?: Record<string, any>): Promise<void> {
    for (const interceptor of this.interceptors) {
      if ('onStart' in interceptor && interceptor.onStart) {
        await interceptor.onStart(workflowType, workflowId, context);
      }
    }
  }

  /** Invoke all workflow interceptors for complete event. */
  async invokeWorkflowComplete(workflowId: bigint, result: any, context?: Record<string, any>): Promise<void> {
    for (const interceptor of this.interceptors) {
      if ('onWorkflowComplete' in interceptor && interceptor.onWorkflowComplete) {
        await interceptor.onWorkflowComplete(workflowId, result, context);
      }
    }
  }

  /** Invoke all workflow interceptors for fail event. */
  async invokeWorkflowFail(workflowId: bigint, error: Error, context?: Record<string, any>): Promise<void> {
    for (const interceptor of this.interceptors) {
      if ('onWorkflowFail' in interceptor && interceptor.onWorkflowFail) {
        await interceptor.onWorkflowFail(workflowId, error, context);
      }
    }
  }

  /** Invoke all activity interceptors for execute event. */
  async invokeActivityExecute(activityType: string, activityId: string, context?: Record<string, any>): Promise<void> {
    for (const interceptor of this.interceptors) {
      if ('onExecute' in interceptor && interceptor.onExecute) {
        await interceptor.onExecute(activityType, activityId, context);
      }
    }
  }
}

/** Compose multiple interceptors into a single chain. */
export function composeInterceptors(
  ...interceptors: Array<WorkflowInterceptor | ActivityInterceptor>
): InterceptorChain {
  return new InterceptorChain(interceptors);
}
