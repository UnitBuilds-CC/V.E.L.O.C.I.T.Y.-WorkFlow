/**
 * VELOCITY-WorkFlow TypeScript SDK — Worker process model.
 *
 * The Worker polls the server for workflow and activity tasks, executes them
 * using registered workflow/activity implementations, and reports results back.
 * This mirrors Temporal's worker architecture: long-poll, execute, respond.
 *
 * @example
 * ```typescript
 * import { Worker, WorkflowContext } from '@velocity-workflow/sdk';
 *
 * const worker = await Worker.create({
 *   taskQueue: 'orders',
 *   workflowsPath: './workflows',
 *   activities: { processPayment, sendEmail },
 * });
 *
 * await worker.run();
 * ```
 *
 * @packageDocumentation
 */

import { EventEmitter } from 'events';
import {
  getRegisteredWorkflows,
  getRegisteredActivities,
  type WorkflowClass,
} from './decorators';

// ─── Worker Configuration ─────────────────────────────────────────────────────

/** Configuration for creating a Worker. */
export interface WorkerOptions {
  /** Task queue to poll for tasks. */
  taskQueue: string;
  /** Server address (gRPC endpoint). */
  serverAddress?: string;
  /** Namespace for this worker. */
  namespace?: string;
  /** Maximum concurrent workflow tasks. */
  maxConcurrentWorkflowTasks?: number;
  /** Maximum concurrent activity tasks. */
  maxConcurrentActivityTasks?: number;
  /** Poll timeout in milliseconds (long-poll). */
  pollTimeoutMs?: number;
  /** Heartbeat interval in milliseconds. */
  heartbeatIntervalMs?: number;
  /** Build ID for worker versioning. */
  buildId?: string;
  /** Enable sticky queue affinity. */
  enableStickyExecution?: boolean;
  /** Workflow implementations map (type name → handler). */
  workflows?: Record<string, WorkflowImplementation>;
  /** Activity implementations map (name → handler function). */
  activities?: Record<string, ActivityImplementation>;
  /** Interceptors for workflow/activity execution. */
  interceptors?: WorkerInterceptor[];
}

/** A workflow implementation function. */
export type WorkflowImplementation = (ctx: WorkflowContext, ...args: any[]) => Promise<any>;

/** An activity implementation function. */
export type ActivityImplementation = (...args: any[]) => Promise<any>;

/** Worker interceptor interface. */
export interface WorkerInterceptor {
  interceptWorkflow?(input: WorkflowInterceptInput, next: () => Promise<any>): Promise<any>;
  interceptActivity?(input: ActivityInterceptInput, next: () => Promise<any>): Promise<any>;
}

export interface WorkflowInterceptInput {
  workflowType: string;
  workflowKey: bigint;
  args: any[];
}

export interface ActivityInterceptInput {
  activityType: string;
  workflowKey: bigint;
  args: any[];
}

// ─── Workflow Context ─────────────────────────────────────────────────────────

/**
 * Context available inside workflow functions.
 *
 * Provides deterministic operations for scheduling activities, timers,
 * signals, queries, updates, and child workflows. All operations are
 * recorded in workflow history for replay/determinism.
 */
export class WorkflowContext {
  /** Unique workflow execution key. */
  readonly workflowKey: bigint;
  /** Workflow ID (user-provided or auto-generated). */
  readonly workflowId: string;
  /** Current run ID. */
  readonly runId: string;
  /** Workflow type name. */
  readonly workflowType: string;
  /** Task queue this workflow is executing on. */
  readonly taskQueue: string;

  private _completed = false;
  private _canceled = false;
  private _signalHandlers: Map<string, (payload: any) => void> = new Map();
  private _queryHandlers: Map<string, () => any> = new Map();
  private _updateHandlers: Map<string, (payload: any) => any> = new Map();
  private _pendingSignals: Map<string, any[]> = new Map();
  private _memo: Map<string, any> = new Map();
  private _searchAttributes: Map<string, any> = new Map();
  private _currentStep = 0;
  private _totalSteps = 0;

  constructor(options: {
    workflowKey: bigint;
    workflowId: string;
    runId: string;
    workflowType: string;
    taskQueue: string;
    totalSteps?: number;
  }) {
    this.workflowKey = options.workflowKey;
    this.workflowId = options.workflowId;
    this.runId = options.runId;
    this.workflowType = options.workflowType;
    this.taskQueue = options.taskQueue;
    this._totalSteps = options.totalSteps ?? 0;
  }

  /**
   * Schedule an activity for execution.
   * Returns the activity result. Deterministic — replays return cached results.
   */
  async executeActivity<T = any>(activityName: string, ...args: any[]): Promise<T> {
    this._currentStep++;
    // In a full implementation, this would:
    // 1. Check history for a cached result (deterministic replay)
    // 2. If no cached result, send ScheduleActivity command to server
    // 3. Wait for ActivityTask completion
    // For now, return a placeholder that the worker loop will resolve.
    throw new ActivityScheduledMessage(activityName, args, this._currentStep);
  }

  /**
   * Schedule an activity with full timeout/retry options.
   */
  async executeActivityWithOptions<T = any>(
    activityName: string,
    options: ActivityOptions,
    ...args: any[]
  ): Promise<T> {
    this._currentStep++;
    throw new ActivityScheduledMessage(activityName, args, this._currentStep, options);
  }

  /**
   * Sleep for a specified duration (deterministic timer).
   */
  async sleep(durationMs: number): Promise<void> {
    this._currentStep++;
    throw new TimerScheduledMessage(durationMs, this._currentStep);
  }

  /**
   * Wait for a signal with the given name.
   * Blocks until a signal is received.
   */
  async waitForSignal(signalName: string): Promise<any> {
    // Check buffered signals first
    const buffered = this._pendingSignals.get(signalName);
    if (buffered && buffered.length > 0) {
      return buffered.shift();
    }
    throw new SignalWaitMessage(signalName);
  }

  /**
   * Register a signal handler.
   */
  onSignal(signalName: string, handler: (payload: any) => void): void {
    this._signalHandlers.set(signalName, handler);
  }

  /**
   * Register a query handler (read-only, synchronous).
   */
  onQuery(queryName: string, handler: () => any): void {
    this._queryHandlers.set(queryName, handler);
  }

  /**
   * Register an update handler (can mutate workflow state).
   */
  onUpdate(updateName: string, handler: (payload: any) => any): void {
    this._updateHandlers.set(updateName, handler);
  }

  /**
   * Handle an incoming signal (called by the worker loop).
   */
  _deliverSignal(signalName: string, payload: any): void {
    const handler = this._signalHandlers.get(signalName);
    if (handler) {
      handler(payload);
    } else {
      // Buffer the signal for later consumption
      if (!this._pendingSignals.has(signalName)) {
        this._pendingSignals.set(signalName, []);
      }
      this._pendingSignals.get(signalName)!.push(payload);
    }
  }

  /**
   * Handle an incoming query (called by the worker loop).
   */
  _handleQuery(queryName: string): any {
    const handler = this._queryHandlers.get(queryName);
    if (handler) {
      return handler();
    }
    throw new Error(`No query handler registered for '${queryName}'`);
  }

  /**
   * Handle an incoming update (called by the worker loop).
   */
  _handleUpdate(updateName: string, payload: any): any {
    const handler = this._updateHandlers.get(updateName);
    if (handler) {
      return handler(payload);
    }
    throw new Error(`No update handler registered for '${updateName}'`);
  }

  /**
   * Start a child workflow.
   */
  async startChildWorkflow<T = any>(
    workflowType: string,
    options: ChildWorkflowOptions,
    ...args: any[]
  ): Promise<ChildWorkflowHandle<T>> {
    throw new ChildWorkflowScheduledMessage(workflowType, options, args);
  }

  /**
   * Continue this workflow as a new execution (handoff pattern).
   */
  continueAsNew(...args: any[]): never {
    throw new ContinueAsNewMessage(args);
  }

  /**
   * Get workflow memo value.
   */
  getMemo<T = any>(key: string): T | undefined {
    return this._memo.get(key) as T | undefined;
  }

  /**
   * Upsert search attributes.
   */
  upsertSearchAttributes(attrs: Record<string, any>): void {
    for (const [key, value] of Object.entries(attrs)) {
      this._searchAttributes.set(key, value);
    }
  }

  /**
   * Get current step index.
   */
  get currentStep(): number {
    return this._currentStep;
  }

  /**
   * Check if the workflow has been canceled.
   */
  get isCanceled(): boolean {
    return this._canceled;
  }

  /**
   * Mark the workflow as canceled (called by the worker loop).
   */
  _markCanceled(): void {
    this._canceled = true;
  }
}

// ─── Activity & Child Workflow Options ────────────────────────────────────────

/** Options for scheduling an activity. */
export interface ActivityOptions {
  /** Task queue to schedule the activity on. */
  taskQueue?: string;
  /** Schedule-to-close timeout. */
  scheduleToCloseTimeoutMs?: number;
  /** Schedule-to-start timeout. */
  scheduleToStartTimeoutMs?: number;
  /** Start-to-close timeout. */
  startToCloseTimeoutMs?: number;
  /** Heartbeat timeout. */
  heartbeatTimeoutMs?: number;
  /** Retry policy. */
  retry?: RetryPolicy;
}

/** Retry policy for activities. */
export interface RetryPolicy {
  /** Maximum number of attempts (including the initial attempt). */
  maximumAttempts: number;
  /** Initial backoff interval in milliseconds. */
  initialIntervalMs: number;
  /** Backoff coefficient (e.g., 2.0 = exponential). */
  backoffCoefficient: number;
  /** Maximum backoff interval in milliseconds. */
  maximumIntervalMs?: number;
}

/** Options for starting a child workflow. */
export interface ChildWorkflowOptions {
  /** Workflow ID (auto-generated if not provided). */
  workflowId?: string;
  /** Task queue for the child workflow. */
  taskQueue?: string;
  /** Parent close policy. */
  parentClosePolicy?: 'terminate' | 'cancel' | 'abandon';
  /** Retry policy. */
  retry?: RetryPolicy;
}

/** Handle to a running child workflow. */
export interface ChildWorkflowHandle<T = any> {
  /** Workflow key of the child. */
  workflowKey: bigint;
  /** Workflow ID of the child. */
  workflowId: string;
  /** Wait for the child workflow to complete and return its result. */
  result(): Promise<T>;
  /** Signal the child workflow. */
  signal(signalName: string, payload: any): Promise<void>;
}

// ─── Internal Messages (for workflow replay) ─────────────────────────────────

/** Thrown when a workflow schedules an activity (caught by the worker loop). */
export class ActivityScheduledMessage extends Error {
  readonly activityName: string;
  readonly args: any[];
  readonly step: number;
  readonly options?: ActivityOptions;

  constructor(activityName: string, args: any[], step: number, options?: ActivityOptions) {
    super(`Activity scheduled: ${activityName}`);
    this.name = 'ActivityScheduledMessage';
    this.activityName = activityName;
    this.args = args;
    this.step = step;
    this.options = options;
  }
}

/** Thrown when a workflow schedules a timer. */
export class TimerScheduledMessage extends Error {
  readonly durationMs: number;
  readonly step: number;

  constructor(durationMs: number, step: number) {
    super(`Timer scheduled: ${durationMs}ms`);
    this.name = 'TimerScheduledMessage';
    this.durationMs = durationMs;
    this.step = step;
  }
}

/** Thrown when a workflow waits for a signal. */
export class SignalWaitMessage extends Error {
  readonly signalName: string;

  constructor(signalName: string) {
    super(`Waiting for signal: ${signalName}`);
    this.name = 'SignalWaitMessage';
    this.signalName = signalName;
  }
}

/** Thrown when a workflow starts a child workflow. */
export class ChildWorkflowScheduledMessage extends Error {
  readonly workflowType: string;
  readonly options: ChildWorkflowOptions;
  readonly args: any[];

  constructor(workflowType: string, options: ChildWorkflowOptions, args: any[]) {
    super(`Child workflow scheduled: ${workflowType}`);
    this.name = 'ChildWorkflowScheduledMessage';
    this.workflowType = workflowType;
    this.options = options;
    this.args = args;
  }
}

/** Thrown when a workflow continues as new. */
export class ContinueAsNewMessage extends Error {
  readonly args: any[];

  constructor(args: any[]) {
    super('Continue as new');
    this.name = 'ContinueAsNewMessage';
    this.args = args;
  }
}

// ─── Internal Task Types ─────────────────────────────────────────────────────

/** A workflow task queued for processing. */
interface WorkflowTask {
  workflowType: string;
  workflowKey: bigint;
  workflowId: string;
  args: any[];
}

/** An activity task queued for processing. */
interface ActivityTask {
  activityName: string;
  args: any[];
}

// ─── Worker ───────────────────────────────────────────────────────────────────

/** Worker statistics. */
export interface WorkerStats {
  workflowsStarted: number;
  workflowsCompleted: number;
  workflowsFailed: number;
  activitiesScheduled: number;
  activitiesCompleted: number;
  activitiesFailed: number;
  tasksPolled: number;
  heartbeatsSent: number;
  uptimeMs: number;
}

/**
 * VELOCITY Worker — polls the server for tasks and executes workflows/activities.
 *
 * The worker maintains a long-poll loop, dispatching workflow tasks to registered
 * workflow implementations and activity tasks to registered activity functions.
 * It sends heartbeats to the server to maintain liveness.
 */
export class Worker extends EventEmitter {
  private readonly options: Required<WorkerOptions>;
  private running = false;
  private startTime = Date.now();
  private stats: WorkerStats = {
    workflowsStarted: 0,
    workflowsCompleted: 0,
    workflowsFailed: 0,
    activitiesScheduled: 0,
    activitiesCompleted: 0,
    activitiesFailed: 0,
    tasksPolled: 0,
    heartbeatsSent: 0,
    uptimeMs: 0,
  };
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private workflows: Record<string, WorkflowImplementation>;
  private activities: Record<string, ActivityImplementation>;
  /** Internal queue of workflow tasks (for embedded/testing use). */
  private _workflowTaskQueue: WorkflowTask[] = [];
  /** Internal queue of activity tasks (for embedded/testing use). */
  private _activityTaskQueue: ActivityTask[] = [];

  private constructor(options: WorkerOptions) {
    super();
    this.options = {
      taskQueue: options.taskQueue,
      serverAddress: options.serverAddress ?? 'localhost:7234',
      namespace: options.namespace ?? 'default',
      maxConcurrentWorkflowTasks: options.maxConcurrentWorkflowTasks ?? 10,
      maxConcurrentActivityTasks: options.maxConcurrentActivityTasks ?? 100,
      pollTimeoutMs: options.pollTimeoutMs ?? 10000,
      heartbeatIntervalMs: options.heartbeatIntervalMs ?? 30000,
      buildId: options.buildId ?? '1.0',
      enableStickyExecution: options.enableStickyExecution ?? true,
      workflows: options.workflows ?? {},
      activities: options.activities ?? {},
      interceptors: options.interceptors ?? [],
    };
    
    // Merge manual registrations with auto-apply registry
    this.workflows = { ...this.options.workflows };
    this.activities = { ...this.options.activities };
    
    // Auto-discover workflows and activities from decorator registry
    const autoWorkflows = getRegisteredWorkflows();
    const autoActivities = getRegisteredActivities();
    
    autoWorkflows.forEach((WorkflowClass, workflowType) => {
      if (!this.workflows[workflowType]) {
        const instance = new WorkflowClass();
        this.workflows[workflowType] = instance.run.bind(instance);
      }
    });
    
    autoActivities.forEach((handler, activityName) => {
      if (!this.activities[activityName]) {
        this.activities[activityName] = handler as ActivityImplementation;
      }
    });
  }

  /**
   * Create a new Worker instance.
   */
  static async create(options: WorkerOptions): Promise<Worker> {
    if (!options.taskQueue) {
      throw new Error('Worker: taskQueue is required');
    }
    return new Worker(options);
  }

  /**
   * Start the worker and run until shutdown.
   * This is the main entry point — the worker will poll for tasks indefinitely.
   *
   * The poll loop:
   * 1. Send PollWorkflowTaskQueue / PollActivityTaskQueue to the server
   * 2. If a workflow task is received, execute the registered workflow
   * 3. If an activity task is received, execute the registered activity
   * 4. Send RespondWorkflowTaskCompleted / RespondActivityTaskCompleted
   * 5. Repeat until shutdown()
   */
  async run(): Promise<void> {
    this.running = true;
    this.startTime = Date.now();
    this.emit('started', { taskQueue: this.options.taskQueue });

    // Start heartbeat interval
    this.heartbeatTimer = setInterval(() => {
      this.stats.heartbeatsSent++;
      this.emit('heartbeat', {
        taskQueue: this.options.taskQueue,
        identity: this.options.buildId,
        namespace: this.options.namespace,
      });
    }, this.options.heartbeatIntervalMs);

    // Concurrent workflow + activity poll loops
    const workflowPollLoop = this.pollWorkflowTasks();
    const activityPollLoop = this.pollActivityTasks();

    try {
      await Promise.all([workflowPollLoop, activityPollLoop]);
    } finally {
      this.shutdown();
    }
  }

  /**
   * Poll loop for workflow tasks.
   * In production, this sends PollWorkflowTaskQueue gRPC calls.
   * Here, it processes internally queued tasks for testing/embedded use.
   */
  private async pollWorkflowTasks(): Promise<void> {
    while (this.running) {
      this.stats.tasksPolled++;
      this.emit('polling', { taskQueue: this.options.taskQueue, kind: 'workflow' });

      // Process any pending workflow tasks from the internal queue
      const task = this._workflowTaskQueue.shift();
      if (task) {
        try {
          await this.executeWorkflow(
            task.workflowType,
            task.workflowKey,
            task.workflowId,
            task.args,
          );
        } catch {
          // Workflow failure is recorded in executeWorkflow
        }
      } else {
        // No task available — wait for the poll timeout (long-poll simulation)
        await new Promise((resolve) => setTimeout(resolve, Math.min(this.options.pollTimeoutMs, 1000)));
      }
    }
  }

  /**
   * Poll loop for activity tasks.
   * In production, this sends PollActivityTaskQueue gRPC calls.
   */
  private async pollActivityTasks(): Promise<void> {
    while (this.running) {
      this.stats.tasksPolled++;
      this.emit('polling', { taskQueue: this.options.taskQueue, kind: 'activity' });

      // Process any pending activity tasks from the internal queue
      const task = this._activityTaskQueue.shift();
      if (task) {
        try {
          await this.executeActivity(task.activityName, task.args);
        } catch {
          // Activity failure is recorded in executeActivity
        }
      } else {
        await new Promise((resolve) => setTimeout(resolve, Math.min(this.options.pollTimeoutMs, 1000)));
      }
    }
  }

  /**
   * Submit a workflow task to this worker's queue (for embedded/testing use).
   * In production, tasks arrive via gRPC PollWorkflowTaskQueue.
   */
  submitWorkflowTask(
    workflowType: string,
    workflowKey: bigint,
    workflowId: string,
    args: any[],
  ): void {
    this._workflowTaskQueue.push({ workflowType, workflowKey, workflowId, args });
  }

  /**
   * Submit an activity task to this worker's queue (for embedded/testing use).
   */
  submitActivityTask(activityName: string, args: any[]): void {
    this._activityTaskQueue.push({ activityName, args });
  }

  /**
   * Execute a workflow (called when a workflow task is received).
   */
  async executeWorkflow(
    workflowType: string,
    workflowKey: bigint,
    workflowId: string,
    args: any[],
  ): Promise<any> {
    const impl = this.workflows[workflowType];
    if (!impl) {
      throw new Error(`No workflow implementation registered for '${workflowType}'`);
    }

    const ctx = new WorkflowContext({
      workflowKey,
      workflowId,
      runId: `run-${Date.now()}`,
      workflowType,
      taskQueue: this.options.taskQueue,
    });

    this.stats.workflowsStarted++;
    this.emit('workflowStarted', { workflowType, workflowKey });

    try {
      // Apply interceptors
      let result: any;
      if (this.options.interceptors.length > 0) {
        result = await this.executeWithInterceptors(ctx, workflowType, workflowKey, args, impl);
      } else {
        result = await impl(ctx, ...args);
      }
      this.stats.workflowsCompleted++;
      this.emit('workflowCompleted', { workflowType, workflowKey, result });
      return result;
    } catch (err) {
      this.stats.workflowsFailed++;
      this.emit('workflowFailed', { workflowType, workflowKey, error: err });
      throw err;
    }
  }

  /**
   * Execute an activity (called when an activity task is received).
   */
  async executeActivity(activityName: string, args: any[]): Promise<any> {
    const impl = this.activities[activityName];
    if (!impl) {
      throw new Error(`No activity implementation registered for '${activityName}'`);
    }

    this.stats.activitiesScheduled++;

    try {
      const result = await impl(...args);
      this.stats.activitiesCompleted++;
      this.emit('activityCompleted', { activityName, result });
      return result;
    } catch (err) {
      this.stats.activitiesFailed++;
      this.emit('activityFailed', { activityName, error: err });
      throw err;
    }
  }

  /**
   * Register additional workflow implementations.
   */
  registerWorkflows(workflows: Record<string, WorkflowImplementation>): void {
    Object.assign(this.workflows, workflows);
  }

  /**
   * Register additional activity implementations.
   */
  registerActivities(activities: Record<string, ActivityImplementation>): void {
    Object.assign(this.activities, activities);
  }

  /**
   * Get worker statistics.
   */
  getStats(): WorkerStats {
    return {
      ...this.stats,
      uptimeMs: Date.now() - this.startTime,
    };
  }

  /**
   * Gracefully shut down the worker.
   */
  shutdown(): void {
    this.running = false;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.emit('shutdown');
  }

  /**
   * Check if the worker is running.
   */
  isRunning(): boolean {
    return this.running;
  }

  /**
   * Get the task queue this worker polls from.
   */
  getTaskQueue(): string {
    return this.options.taskQueue;
  }

  private async executeWithInterceptors(
    ctx: WorkflowContext,
    workflowType: string,
    workflowKey: bigint,
    args: any[],
    impl: WorkflowImplementation,
  ): Promise<any> {
    let index = 0;
    const interceptors = this.options.interceptors;

    const executeNext = async (): Promise<any> => {
      if (index < interceptors.length) {
        const interceptor = interceptors[index++];
        if (interceptor.interceptWorkflow) {
          return interceptor.interceptWorkflow(
            { workflowType, workflowKey, args },
            executeNext,
          );
        }
      }
      return impl(ctx, ...args);
    };

    return executeNext();
  }
}

export default Worker;
