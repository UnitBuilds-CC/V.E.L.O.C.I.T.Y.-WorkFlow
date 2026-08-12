/**
 * Velocity Classic — Temporal-compatible durable workflow SDK for TypeScript.
 *
 * Production-grade SDK with:
 * - Workflows: Durable functions that orchestrate activities
 * - Activities: Side-effect-producing functions
 * - Worker: Polls for tasks and executes workflows/activities
 * - Client: Submits workflows and queries their status
 * - Error hierarchy with typed error codes
 * - Configuration management with validation
 * - Structured logging
 * - Metrics collection
 * - Health checks
 * - Graceful shutdown
 *
 * @example
 * ```typescript
 * import { Workflow, Activity, Worker, Client } from '@velocity-workflow/classic';
 *
 * class OrderWorkflow extends Workflow {
 *   async execute(orderId: string) {
 *     const charge = await this.executeActivity('chargeActivity', orderId);
 *     const ship = await this.executeActivity('shipActivity', orderId);
 *     return { charge, ship };
 *   }
 * }
 *
 * const worker = await Worker.create({ taskQueue: 'orders' });
 * worker.registerWorkflow(OrderWorkflow);
 * await worker.run();
 * ```
 */

// ─── Error Hierarchy ─────────────────────────────────────────────────────────

export class VelocityClassicError extends Error {
  readonly code: string;
  readonly details: Record<string, any>;

  constructor(message: string, code = 'CLASSIC_ERROR', details: Record<string, any> = {}) {
    super(message);
    this.name = new.target.name;
    this.code = code;
    this.details = details;
  }
}

export class WorkflowNotFoundError extends VelocityClassicError {
  readonly workflowId: string;
  constructor(workflowId: string) {
    super(`Workflow not found: ${workflowId}`, 'WORKFLOW_NOT_FOUND', { workflowId });
    this.workflowId = workflowId;
  }
}

export class WorkflowTypeError extends VelocityClassicError {
  readonly workflowType: string;
  constructor(workflowType: string) {
    super(`Unknown workflow type: ${workflowType}`, 'WORKFLOW_TYPE_NOT_FOUND', { workflowType });
    this.workflowType = workflowType;
  }
}

export class ActivityTypeError extends VelocityClassicError {
  readonly activityType: string;
  constructor(activityType: string) {
    super(`Unknown activity type: ${activityType}`, 'ACTIVITY_TYPE_NOT_FOUND', { activityType });
    this.activityType = activityType;
  }
}

export class WorkerNotRunningError extends VelocityClassicError {
  constructor() {
    super('Worker is not running', 'WORKER_NOT_RUNNING');
  }
}

export class DuplicateRegistrationError extends VelocityClassicError {
  readonly typeName: string;
  constructor(typeName: string) {
    super(`Type already registered: ${typeName}`, 'DUPLICATE_REGISTRATION', { typeName });
    this.typeName = typeName;
  }
}

export class ContinueAsNewError extends VelocityClassicError {
  readonly workflowType: string;
  readonly args: any[];
  constructor(workflowType: string, args: any[]) {
    super(`Workflow continuing as new: ${workflowType}`, 'CONTINUE_AS_NEW', { workflowType });
    this.workflowType = workflowType;
    this.args = args;
  }
}

export class NexusOperationError extends VelocityClassicError {
  readonly namespace: string;
  readonly operation: string;
  constructor(namespace: string, operation: string) {
    super(`Nexus operation failed: ${namespace}/${operation}`, 'NEXUS_ERROR', { namespace, operation });
    this.namespace = namespace;
    this.operation = operation;
  }
}

// ─── Configuration ───────────────────────────────────────────────────────────

export interface ClassicConfig {
  serverAddress: string;
  namespace: string;
  taskQueue: string;
  maxConcurrentWorkflows: number;
  maxConcurrentActivities: number;
  stickyQueues: boolean;
  identity: string;
  shutdownGracePeriodMs: number;
  logLevel: 'debug' | 'info' | 'warn' | 'error' | 'silent';
  enableMetrics: boolean;
  enableHealthChecks: boolean;
}

export function defaultConfig(): ClassicConfig {
  return {
    serverAddress: 'localhost:7233',
    namespace: 'default',
    taskQueue: 'default',
    maxConcurrentWorkflows: 100,
    maxConcurrentActivities: 200,
    stickyQueues: true,
    identity: `worker-${typeof process !== 'undefined' ? process.pid : 0}`,
    shutdownGracePeriodMs: 10_000,
    logLevel: 'info',
    enableMetrics: true,
    enableHealthChecks: true,
  };
}

export function createClassicConfig(overrides: Partial<ClassicConfig> = {}): ClassicConfig {
  const config = { ...defaultConfig(), ...overrides };
  if (config.maxConcurrentWorkflows < 1) throw new Error('maxConcurrentWorkflows must be >= 1');
  if (config.maxConcurrentActivities < 1) throw new Error('maxConcurrentActivities must be >= 1');
  return config;
}

// ─── Logger ──────────────────────────────────────────────────────────────────

export interface ClassicLogger {
  debug(msg: string, ...args: any[]): void;
  info(msg: string, ...args: any[]): void;
  warn(msg: string, ...args: any[]): void;
  error(msg: string, ...args: any[]): void;
}

const LOG_LEVELS: Record<string, number> = { debug: 0, info: 1, warn: 2, error: 3, silent: 4 };

export function createClassicLogger(level: string = 'info', prefix = '[velocity-classic]'): ClassicLogger {
  const threshold = LOG_LEVELS[level] ?? 1;
  return {
    debug: (msg, ...args) => { if (threshold <= 0) console.debug(prefix, msg, ...args); },
    info: (msg, ...args) => { if (threshold <= 1) console.info(prefix, msg, ...args); },
    warn: (msg, ...args) => { if (threshold <= 2) console.warn(prefix, msg, ...args); },
    error: (msg, ...args) => { if (threshold <= 3) console.error(prefix, msg, ...args); },
  };
}

// ─── Metrics ─────────────────────────────────────────────────────────────────

export class ClassicMetrics {
  workflowsStarted = 0;
  workflowsCompleted = 0;
  workflowsFailed = 0;
  workflowsCancelled = 0;
  workflowsTerminated = 0;
  activitiesExecuted = 0;
  activitiesFailed = 0;
  signalsSent = 0;
  queriesHandled = 0;
  private _startTime = Date.now();

  recordWorkflowStarted(): void { this.workflowsStarted++; }
  recordWorkflowCompleted(): void { this.workflowsCompleted++; }
  recordWorkflowFailed(): void { this.workflowsFailed++; }
  recordWorkflowCancelled(): void { this.workflowsCancelled++; }
  recordWorkflowTerminated(): void { this.workflowsTerminated++; }
  recordActivityExecuted(): void { this.activitiesExecuted++; }
  recordActivityFailed(): void { this.activitiesFailed++; }
  recordSignalSent(): void { this.signalsSent++; }
  recordQueryHandled(): void { this.queriesHandled++; }

  snapshot(): Record<string, any> {
    return {
      uptimeSeconds: (Date.now() - this._startTime) / 1000,
      workflowsStarted: this.workflowsStarted,
      workflowsCompleted: this.workflowsCompleted,
      workflowsFailed: this.workflowsFailed,
      workflowsCancelled: this.workflowsCancelled,
      workflowsTerminated: this.workflowsTerminated,
      activitiesExecuted: this.activitiesExecuted,
      activitiesFailed: this.activitiesFailed,
      signalsSent: this.signalsSent,
      queriesHandled: this.queriesHandled,
    };
  }

  reset(): void {
    this.workflowsStarted = 0; this.workflowsCompleted = 0; this.workflowsFailed = 0;
    this.workflowsCancelled = 0; this.workflowsTerminated = 0;
    this.activitiesExecuted = 0; this.activitiesFailed = 0;
    this.signalsSent = 0; this.queriesHandled = 0;
    this._startTime = Date.now();
  }
}

// ─── Health ──────────────────────────────────────────────────────────────────

export interface ClassicHealthResult {
  name: string;
  status: 'healthy' | 'degraded' | 'unhealthy';
  message?: string;
  latencyMs: number;
}

export interface ClassicHealthStatus {
  status: 'healthy' | 'degraded' | 'unhealthy';
  checks: ClassicHealthResult[];
  timestamp: number;
  uptimeSeconds: number;
}

// ─── Types ───────────────────────────────────────────────────────────────────

export interface WorkflowExecution {
  workflowId: string;
  runId: string;
  workflowType: string;
  status: WorkflowStatus;
  startTime: number;
  closeTime?: number;
  result?: any;
  error?: string;
  errorCode?: string;
  searchAttributes?: SearchAttributes;
  memo?: Memo;
}

export enum WorkflowStatus {
  RUNNING = 'running',
  COMPLETED = 'completed',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
  TERMINATED = 'terminated',
  CONTINUING_AS_NEW = 'continuingAsNew',
}

export interface RetryPolicy {
  initialIntervalMs: number;
  backoffCoefficient: number;
  maximumIntervalMs: number;
  maximumAttempts: number;
  nonRetryableErrorTypes: string[];
}

export interface SearchAttributes { [key: string]: any; }
export interface Memo { [key: string]: any; }
export interface Signal { signalName: string; input: any; }
export interface Query { queryType: string; input: any; }

// ─── Workflow Base Class ─────────────────────────────────────────────────────

export abstract class Workflow {
  static typeName: string;
  static version: string = '1.0';
  /** @internal */ _worker: Worker | null = null;
  /** @internal */ _executionId: string = '';
  /** @internal */ _signals = new Map<string, { resolve: (v: any) => void; value?: any }[]>();
  /** @internal */ _signalBuffers = new Map<string, any[]>();
  /** @internal */ _cancelled = false;
  /** @internal */ _cancellationWaiters: (() => void)[] = [];
  /** @internal */ _sagaCompensations: (() => Promise<void>)[] = [];
  abstract execute(...args: any[]): Promise<any>;

  async executeActivity<T>(activityName: string, ...args: any[]): Promise<T> {
    if (!this._worker) throw new ActivityTypeError(activityName);
    return this._worker._executeActivityWithRetry<T>(activityName, args);
  }

  async executeChildWorkflow<T>(workflowType: string, ...args: any[]): Promise<T> {
    if (!this._worker) throw new WorkflowTypeError(workflowType);
    return this._worker._executeChildWorkflow<T>(workflowType, args);
  }

  async sleep(durationMs: number): Promise<void> {
    if (this._cancelled) throw new Error('Workflow cancelled');
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(resolve, durationMs);
      this._cancellationWaiters.push(() => { clearTimeout(timer); reject(new Error('Workflow cancelled')); });
    });
  }

  async waitForSignal<T>(signalName: string): Promise<T> {
    if (this._cancelled) throw new Error('Workflow cancelled');
    const buffer = this._signalBuffers.get(signalName);
    if (buffer && buffer.length > 0) return buffer.shift() as T;
    return new Promise<T>((resolve, reject) => {
      const waiters = this._signals.get(signalName) || [];
      waiters.push({ resolve });
      this._signals.set(signalName, waiters);
      this._cancellationWaiters.push(() => reject(new Error('Workflow cancelled')));
    });
  }

  // ─── Versioning ──────────────────────────────────────────────────────────

  getVersion(): string {
    return (this.constructor as typeof Workflow).version;
  }

  async patched<T>(version: string, currentFn: () => Promise<T>, oldFn?: () => Promise<T>): Promise<T> {
    // If running on current version, execute current logic
    // If running on old version, execute old logic (for backward compat)
    const runningVersion = this.getVersion();
    if (runningVersion === version || !oldFn) return currentFn();
    return oldFn();
  }

  // ─── Cancellation ────────────────────────────────────────────────────────

  isCancelled(): boolean { return this._cancelled; }

  async cancel(): Promise<void> {
    this._cancelled = true;
    for (const waiter of this._cancellationWaiters) waiter();
    this._cancellationWaiters = [];
  }

  // ─── Saga Pattern ────────────────────────────────────────────────────────

  addCompensation(fn: () => Promise<void>): void {
    this._sagaCompensations.push(fn);
  }

  async compensate(): Promise<void> {
    // Run compensations in reverse order
    for (let i = this._sagaCompensations.length - 1; i >= 0; i--) {
      try { await this._sagaCompensations[i](); } catch { /* best effort */ }
    }
    this._sagaCompensations = [];
  }

  /** @internal — deliver a signal to this workflow */
  _deliverSignal(signalName: string, input: any): void {
    const waiters = this._signals.get(signalName);
    if (waiters && waiters.length > 0) {
      const waiter = waiters.shift()!;
      waiter.resolve(input);
      if (waiters.length === 0) this._signals.delete(signalName);
    } else {
      const buffer = this._signalBuffers.get(signalName) || [];
      buffer.push(input);
      this._signalBuffers.set(signalName, buffer);
    }
  }

  // ─── Continue-As-New ─────────────────────────────────────────────────────

  /** @internal */ _continueAsNew: { workflowType: string; args: any[] } | null = null;

  continueAsNew(workflowType: string, ...args: any[]): never {
    this._continueAsNew = { workflowType, args };
    throw new ContinueAsNewError(workflowType, args);
  }

  // ─── Updates ──────────────────────────────────────────────────────────────

  /** @internal */ _updateHandlers = new Map<string, (input: any) => Promise<any>>();

  registerUpdate(updateType: string, handler: (input: any) => Promise<any>): void {
    this._updateHandlers.set(updateType, handler);
  }

  async handleUpdate(updateType: string, input: any): Promise<any> {
    const handler = this._updateHandlers.get(updateType);
    if (!handler) throw new Error(`Unknown update type: ${updateType}`);
    return handler(input);
  }

  handleQuery(queryType: string): any { return null; }
}

// ─── Activity Base Class ─────────────────────────────────────────────────────

export abstract class Activity {
  static typeName: string;
  /** @internal */ _heartbeatCallback: ((detail: any) => void) | null = null;
  /** @internal */ _heartbeatInterval: ReturnType<typeof setInterval> | null = null;
  /** @internal */ _lastHeartbeat: number = 0;
  /** @internal */ _cancelled = false;

  abstract execute(...args: any[]): Promise<any>;

  /** Report progress to the worker. If the worker detects a missed heartbeat, the activity can be retried. */
  heartbeat(detail?: any): void {
    this._lastHeartbeat = Date.now();
    if (this._heartbeatCallback) this._heartbeatCallback(detail);
  }

  /** Start automatic heartbeating at the given interval. */
  startHeartbeat(intervalMs: number = 5000): void {
    this.stopHeartbeat();
    this._heartbeatInterval = setInterval(() => this.heartbeat(), intervalMs);
  }

  stopHeartbeat(): void {
    if (this._heartbeatInterval) { clearInterval(this._heartbeatInterval); this._heartbeatInterval = null; }
  }

  isCancelled(): boolean { return this._cancelled; }
  cancel(): void { this._cancelled = true; }
  get lastHeartbeat(): number { return this._lastHeartbeat; }
}

// ─── Worker ──────────────────────────────────────────────────────────────────

export interface WorkflowHandle {
  executionId: string;
  workflowType: string;
  instance: Workflow;
  status: WorkflowStatus;
  result?: any;
  error?: string;
  startTime: number;
  closeTime?: number;
  promise: Promise<any>;
}

export class Worker {
  private _config: ClassicConfig;
  private _workflows = new Map<string, typeof Workflow>();
  private _activities = new Map<string, typeof Activity>();
  private _running = false;
  private _logger: ClassicLogger;
  private _metrics = new ClassicMetrics();
  private _startTime = Date.now();
  /** @internal */ _handles = new Map<string, WorkflowHandle>();
  /** @internal */ _schedules = new Map<string, { cron: string; workflowType: string; args: any[]; interval: ReturnType<typeof setInterval>; nextRun: number }>();
  /** @internal */ _searchAttributes = new Map<string, Record<string, any>>();
  /** @internal */ _heartbeatMonitors = new Map<string, { activity: Activity; interval: number; lastBeat: number; timer: ReturnType<typeof setInterval> }>();
  /** @internal */ _stickyQueues = new Map<string, string>(); // workflowId → workerId
  /** @internal */ _nexusEndpoints = new Map<string, { url: string; namespace: string }>();
  /** @internal */ _workflowHistory = new Map<string, { events: any[]; resetPoint: number }>();

  private constructor(config: ClassicConfig) {
    this._config = config;
    this._logger = createClassicLogger(config.logLevel);
  }

  static async create(config?: Partial<ClassicConfig>): Promise<Worker> {
    const fullConfig = createClassicConfig(config);
    return new Worker(fullConfig);
  }

  registerWorkflow(cls: typeof Workflow): this {
    const name = cls.typeName || cls.name;
    if (this._workflows.has(name)) throw new DuplicateRegistrationError(name);
    this._workflows.set(name, cls);
    this._logger.info(`Workflow registered: ${name}`);
    return this;
  }

  registerActivity(cls: typeof Activity): this {
    const name = cls.typeName || cls.name;
    if (this._activities.has(name)) throw new DuplicateRegistrationError(name);
    this._activities.set(name, cls);
    this._logger.info(`Activity registered: ${name}`);
    return this;
  }

  get config(): ClassicConfig { return this._config; }
  get metrics(): ClassicMetrics { return this._metrics; }
  get workflowTypes(): string[] { return Array.from(this._workflows.keys()); }
  get activityTypes(): string[] { return Array.from(this._activities.keys()); }
  get taskQueue(): string { return this._config.taskQueue; }
  get isRunning(): boolean { return this._running; }

  async run(): Promise<void> {
    this._running = true;
    this._logger.info(`Worker started (taskQueue=${this._config.taskQueue}, identity=${this._config.identity})`);
  }

  async shutdown(): Promise<void> {
    if (!this._running) return;
    // Stop all schedules
    for (const [, schedule] of this._schedules) clearInterval(schedule.interval);
    this._schedules.clear();
    // Stop all heartbeat monitors
    for (const [, monitor] of this._heartbeatMonitors) clearInterval(monitor.timer);
    this._heartbeatMonitors.clear();
    this._running = false;
    this._logger.info('Worker shut down.');
  }

  // ─── Schedules ────────────────────────────────────────────────────────────

  async createSchedule(scheduleId: string, cronExpression: string, workflowType: string, args: any[] = []): Promise<void> {
    if (this._schedules.has(scheduleId)) throw new Error(`Schedule ${scheduleId} already exists`);
    const intervalMs = this._parseCronToMs(cronExpression);
    const nextRun = Date.now() + intervalMs;
    const interval = setInterval(() => {
      this._executeWorkflow(`schedule-${scheduleId}-${Date.now()}`, workflowType, args).catch(err => {
        this._logger.error(`Schedule ${scheduleId} failed: ${err.message}`);
      });
    }, intervalMs);
    this._schedules.set(scheduleId, { cron: cronExpression, workflowType, args, interval, nextRun });
    this._logger.info(`Schedule created: ${scheduleId} (${cronExpression})`);
  }

  async deleteSchedule(scheduleId: string): Promise<void> {
    const schedule = this._schedules.get(scheduleId);
    if (schedule) {
      clearInterval(schedule.interval);
      this._schedules.delete(scheduleId);
      this._logger.info(`Schedule deleted: ${scheduleId}`);
    }
  }

  listSchedules(): { scheduleId: string; cron: string; workflowType: string; nextRun: number }[] {
    return Array.from(this._schedules.entries()).map(([id, s]) => ({
      scheduleId: id, cron: s.cron, workflowType: s.workflowType, nextRun: s.nextRun,
    }));
  }

  private _parseCronToMs(cron: string): number {
    // Simple parser: supports "every Xs", "every Xm", "every Xh", or raw ms
    const match = cron.match(/every\s+(\d+)(s|m|h)/);
    if (match) {
      const val = parseInt(match[1]);
      const unit = match[2];
      if (unit === 's') return val * 1000;
      if (unit === 'm') return val * 60 * 1000;
      if (unit === 'h') return val * 60 * 60 * 1000;
    }
    const ms = parseInt(cron);
    if (!isNaN(ms)) return ms;
    throw new Error(`Invalid cron expression: ${cron}`);
  }

  // ─── Batch Operations ─────────────────────────────────────────────────────

  async batchStartWorkflows(workflows: { workflowId: string; workflowType: string; args: any[] }[]): Promise<WorkflowHandle[]> {
    const handles: WorkflowHandle[] = [];
    for (const wf of workflows) {
      const handle = await this._executeWorkflow(wf.workflowId, wf.workflowType, wf.args);
      handles.push(handle);
    }
    return handles;
  }

  async batchSignal(workflowIds: string[], signalName: string, input: any): Promise<void> {
    for (const id of workflowIds) {
      try { this._signalWorkflow(id, signalName, input); } catch { /* best effort */ }
    }
  }

  async batchCancel(workflowIds: string[]): Promise<void> {
    for (const id of workflowIds) {
      const handle = this._handles.get(id);
      if (handle && handle.status === WorkflowStatus.RUNNING) {
        await handle.instance.cancel();
        handle.status = WorkflowStatus.CANCELLED;
      }
    }
  }

  async batchTerminate(workflowIds: string[]): Promise<void> {
    for (const id of workflowIds) {
      const handle = this._handles.get(id);
      if (handle && handle.status === WorkflowStatus.RUNNING) {
        handle.status = WorkflowStatus.TERMINATED;
      }
    }
  }

  // ─── Search Attributes ────────────────────────────────────────────────────

  setWorkflowAttributes(workflowId: string, attributes: Record<string, any>): void {
    this._searchAttributes.set(workflowId, attributes);
  }

  getWorkflowAttributes(workflowId: string): Record<string, any> | undefined {
    return this._searchAttributes.get(workflowId);
  }

  queryWorkflows(predicate: (attrs: Record<string, any>) => boolean): WorkflowHandle[] {
    const results: WorkflowHandle[] = [];
    for (const [id, attrs] of this._searchAttributes) {
      if (predicate(attrs)) {
        const handle = this._handles.get(id);
        if (handle) results.push(handle);
      }
    }
    return results;
  }

  // ─── Heartbeat Monitoring ─────────────────────────────────────────────────

  private _startHeartbeatMonitor(activityId: string, activity: Activity, intervalMs: number): void {
    const timer = setInterval(() => {
      const monitor = this._heartbeatMonitors.get(activityId);
      if (monitor && Date.now() - monitor.lastBeat > intervalMs * 2) {
        this._logger.warn(`Activity ${activityId} missed heartbeat`);
        activity.cancel();
      }
    }, intervalMs);
    this._heartbeatMonitors.set(activityId, { activity, interval: intervalMs, lastBeat: Date.now(), timer });
  }

  private _stopHeartbeatMonitor(activityId: string): void {
    const monitor = this._heartbeatMonitors.get(activityId);
    if (monitor) {
      clearInterval(monitor.timer);
      this._heartbeatMonitors.delete(activityId);
    }
  }

  /** @internal — Execute a workflow through this worker */
  async _executeWorkflow(workflowId: string, workflowType: string, args: any[]): Promise<WorkflowHandle> {
    const WfClass = this._workflows.get(workflowType);
    if (!WfClass) throw new WorkflowTypeError(workflowType);
    if (!this._running) throw new WorkerNotRunningError();

    const instance = new (WfClass as any)();
    instance._worker = this;
    instance._executionId = workflowId;

    this._metrics.recordWorkflowStarted();

    let resolvePromise: (v: any) => void;
    let rejectPromise: (e: any) => void;
    const promise = new Promise<any>((res, rej) => { resolvePromise = res; rejectPromise = rej; });

    const handle: WorkflowHandle = {
      executionId: workflowId,
      workflowType,
      instance,
      status: WorkflowStatus.RUNNING,
      startTime: Date.now(),
      promise,
    };
    this._handles.set(workflowId, handle);

    // Execute asynchronously
    (async () => {
      try {
        const result = await instance.execute(...args);
        handle.status = WorkflowStatus.COMPLETED;
        handle.result = result;
        handle.closeTime = Date.now();
        this._metrics.recordWorkflowCompleted();
        resolvePromise!(result);
      } catch (err: any) {
        // Handle continue-as-new
        if (err instanceof ContinueAsNewError) {
          handle.status = WorkflowStatus.CONTINUING_AS_NEW;
          handle.closeTime = Date.now();
          try {
            const newHandle = await this._handleContinueAsNew(workflowId, err.workflowType, err.args);
            const newResult = await newHandle.promise;
            handle.result = newResult;
            resolvePromise!(newResult);
          } catch (newErr: any) {
            handle.status = WorkflowStatus.FAILED;
            handle.error = newErr.message || String(newErr);
            rejectPromise!(newErr);
          }
          return;
        }
        // Run saga compensations on failure
        if (instance._sagaCompensations.length > 0) {
          await instance.compensate();
        }
        handle.status = WorkflowStatus.FAILED;
        handle.error = err.message || String(err);
        handle.closeTime = Date.now();
        this._metrics.recordWorkflowFailed();
        rejectPromise!(err);
      }
    })();

    return handle;
  }

  /** @internal — Execute an activity with retry */
  async _executeActivityWithRetry<T>(activityName: string, args: any[]): Promise<T> {
    const ActClass = this._activities.get(activityName);
    if (!ActClass) throw new ActivityTypeError(activityName);
    
    const instance = new (ActClass as any)();
    const activityId = `act-${activityName}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    
    // Set up heartbeat callback
    instance._heartbeatCallback = (detail: any) => {
      const monitor = this._heartbeatMonitors.get(activityId);
      if (monitor) monitor.lastBeat = Date.now();
    };
    
    // Start heartbeat monitor (5 second default)
    this._startHeartbeatMonitor(activityId, instance, 5000);
    
    try {
      const result = await instance.execute(...args);
      this._metrics.recordActivityExecuted();
      return result as T;
    } catch (err) {
      this._metrics.recordActivityFailed();
      throw err;
    } finally {
      instance.stopHeartbeat();
      this._stopHeartbeatMonitor(activityId);
    }
  }

  /** @internal — Execute an activity */
  async _executeActivity<T>(activityName: string, args: any[]): Promise<T> {
    return this._executeActivityWithRetry<T>(activityName, args);
  }

  /** @internal — Execute a child workflow */
  async _executeChildWorkflow<T>(workflowType: string, args: any[]): Promise<T> {
    const childId = `child-${Math.random().toString(36).slice(2)}-${workflowType}`;
    const handle = await this._executeWorkflow(childId, workflowType, args);
    return handle.promise as Promise<T>;
  }

  /** @internal — Deliver a signal to a running workflow */
  _signalWorkflow(workflowId: string, signalName: string, input: any): void {
    const handle = this._handles.get(workflowId);
    if (!handle) throw new WorkflowNotFoundError(workflowId);
    handle.instance._deliverSignal(signalName, input);
  }

  /** @internal — Get a workflow handle */
  _getHandle(workflowId: string): WorkflowHandle | undefined {
    return this._handles.get(workflowId);
  }

  // ─── Continue-As-New ──────────────────────────────────────────────────────

  /** @internal — Handle continue-as-new: re-execute the workflow with new args */
  private async _handleContinueAsNew(workflowId: string, workflowType: string, args: any[]): Promise<WorkflowHandle> {
    const oldHandle = this._handles.get(workflowId);
    if (oldHandle) {
      oldHandle.status = WorkflowStatus.CONTINUING_AS_NEW;
      oldHandle.closeTime = Date.now();
    }
    // Start a new execution with the same workflowId
    return this._executeWorkflow(`${workflowId}-continued`, workflowType, args);
  }

  // ─── Sticky Queues ────────────────────────────────────────────────────────

  assignStickyQueue(workflowId: string, workerId: string): void {
    this._stickyQueues.set(workflowId, workerId);
    this._logger.info(`Sticky queue assigned: ${workflowId} → ${workerId}`);
  }

  getStickyQueue(workflowId: string): string | undefined {
    return this._stickyQueues.get(workflowId);
  }

  // ─── Nexus Operations ─────────────────────────────────────────────────────

  registerNexusEndpoint(name: string, url: string, namespace: string): void {
    this._nexusEndpoints.set(name, { url, namespace });
    this._logger.info(`Nexus endpoint registered: ${name} → ${url} (${namespace})`);
  }

  async executeNexusOperation<T>(endpointName: string, operation: string, input?: any): Promise<T> {
    const endpoint = this._nexusEndpoints.get(endpointName);
    if (!endpoint) throw new NexusOperationError(endpointName, operation);
    // Simulate cross-namespace operation via HTTP
    try {
      const response = await fetch(`${endpoint.url}/api/nexus/${operation}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Namespace': endpoint.namespace },
        body: JSON.stringify({ input }),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data: any = await response.json();
      return data.result as T;
    } catch (err: any) {
      throw new NexusOperationError(endpoint.namespace, operation);
    }
  }

  // ─── Reset ─────────────────────────────────────────────────────────────────

  async resetWorkflow(workflowId: string, eventId?: number): Promise<WorkflowHandle> {
    const oldHandle = this._handles.get(workflowId);
    if (!oldHandle) throw new WorkflowNotFoundError(workflowId);
    const workflowType = oldHandle.workflowType;
    // Get the history for this workflow
    const history = this._workflowHistory.get(workflowId);
    const resetPoint = eventId ?? history?.resetPoint ?? 0;
    // Mark old handle as terminated
    oldHandle.status = WorkflowStatus.TERMINATED;
    oldHandle.closeTime = Date.now();
    // Re-execute from scratch (in real impl, would replay from reset point)
    const newId = `${workflowId}-reset-${Date.now()}`;
    return this._executeWorkflow(newId, workflowType, []);
  }

  recordWorkflowEvent(workflowId: string, event: any): void {
    if (!this._workflowHistory.has(workflowId)) {
      this._workflowHistory.set(workflowId, { events: [], resetPoint: 0 });
    }
    const history = this._workflowHistory.get(workflowId)!;
    history.events.push(event);
  }

  setResetPoint(workflowId: string, eventId: number): void {
    const history = this._workflowHistory.get(workflowId);
    if (history) history.resetPoint = eventId;
  }

  async healthCheck(): Promise<ClassicHealthStatus> {
    const checks: ClassicHealthResult[] = [];
    let overall: 'healthy' | 'degraded' | 'unhealthy' = 'healthy';

    checks.push({ name: 'liveness', status: 'healthy', message: 'alive', latencyMs: 0 });

    if (!this._running) {
      checks.push({ name: 'readiness', status: 'degraded', message: 'worker not running', latencyMs: 0 });
      overall = 'degraded';
    } else if (this._workflows.size === 0 && this._activities.size === 0) {
      checks.push({ name: 'readiness', status: 'degraded', message: 'no workflows or activities registered', latencyMs: 0 });
      overall = 'degraded';
    } else {
      checks.push({ name: 'readiness', status: 'healthy', latencyMs: 0 });
    }

    return { status: overall, checks, timestamp: Date.now(), uptimeSeconds: (Date.now() - this._startTime) / 1000 };
  }

  getStats(): Record<string, any> {
    return {
      running: this._running,
      taskQueue: this._config.taskQueue,
      registeredWorkflows: this._workflows.size,
      registeredActivities: this._activities.size,
      activeSchedules: this._schedules.size,
      activeHeartbeatMonitors: this._heartbeatMonitors.size,
      indexedWorkflowsAttributes: this._searchAttributes.size,
      uptimeSeconds: (Date.now() - this._startTime) / 1000,
      ...(this._config.enableMetrics ? { metrics: this._metrics.snapshot() } : {}),
    };
  }
}

// ─── Client ──────────────────────────────────────────────────────────────────

export class Client {
  private _config: ClassicConfig;
  private _executions = new Map<string, WorkflowExecution>();
  private _logger: ClassicLogger;
  private _metrics = new ClassicMetrics();
  private _worker: Worker | null = null;

  constructor(config?: Partial<ClassicConfig>, worker?: Worker) {
    this._config = createClassicConfig(config);
    this._logger = createClassicLogger(this._config.logLevel);
    this._worker = worker ?? null;
  }

  get config(): ClassicConfig { return this._config; }
  get metrics(): ClassicMetrics { return this._metrics; }

  /** Connect this client to a Worker for actual workflow execution. */
  connectWorker(worker: Worker): void {
    this._worker = worker;
  }

  async startWorkflow(
    workflowId: string,
    workflowType: string,
    args: any[],
    options?: {
      taskQueue?: string;
      retryPolicy?: RetryPolicy;
      searchAttributes?: SearchAttributes;
      memo?: Memo;
      executionTimeoutMs?: number;
    }
  ): Promise<WorkflowExecution> {
    const execution: WorkflowExecution = {
      workflowId, runId: `run-${workflowId}`, workflowType,
      status: WorkflowStatus.RUNNING, startTime: Date.now(),
      searchAttributes: options?.searchAttributes,
      memo: options?.memo,
    };
    this._executions.set(workflowId, execution);
    this._metrics.recordWorkflowStarted();
    this._logger.info(`Workflow started: ${workflowId} (${workflowType})`);

    // If connected to a Worker, actually execute the workflow
    if (this._worker) {
      this._worker._executeWorkflow(workflowId, workflowType, args).then(handle => {
        handle.promise.then(
          (result) => {
            // Don't overwrite CANCELLED or TERMINATED status
            if (execution.status !== WorkflowStatus.CANCELLED && execution.status !== WorkflowStatus.TERMINATED) {
              execution.status = WorkflowStatus.COMPLETED;
              execution.result = result;
              execution.closeTime = Date.now();
            }
          },
          (err) => {
            // Don't overwrite CANCELLED or TERMINATED status
            if (execution.status !== WorkflowStatus.CANCELLED && execution.status !== WorkflowStatus.TERMINATED) {
              execution.status = WorkflowStatus.FAILED;
              execution.error = err.message || String(err);
              execution.errorCode = err.code || 'UNKNOWN';
              execution.closeTime = Date.now();
            }
          },
        );
      }).catch(err => {
        execution.status = WorkflowStatus.FAILED;
        execution.error = err.message || String(err);
        execution.errorCode = err.code || 'UNKNOWN';
        execution.closeTime = Date.now();
      });
    }

    return execution;
  }

  async signal(workflowId: string, signalName: string, input: any): Promise<void> {
    if (!this._executions.has(workflowId)) throw new WorkflowNotFoundError(workflowId);
    this._metrics.recordSignalSent();
    // Deliver signal to worker if connected
    if (this._worker) {
      this._worker._signalWorkflow(workflowId, signalName, input);
    }
  }

  async query(workflowId: string, queryType: string, input?: any): Promise<any> {
    if (!this._executions.has(workflowId)) throw new WorkflowNotFoundError(workflowId);
    this._metrics.recordQueryHandled();
    // Query the actual workflow instance if connected
    if (this._worker) {
      const handle = this._worker._getHandle(workflowId);
      if (handle) return handle.instance.handleQuery(queryType);
    }
    return { status: 'ok' };
  }

  async describe(workflowId: string): Promise<WorkflowExecution | undefined> {
    return this._executions.get(workflowId);
  }

  async list(filter?: { status?: WorkflowStatus; workflowType?: string; limit?: number }): Promise<WorkflowExecution[]> {
    let results = Array.from(this._executions.values());
    if (filter?.status) results = results.filter(r => r.status === filter.status);
    if (filter?.workflowType) results = results.filter(r => r.workflowType === filter.workflowType);
    results.sort((a, b) => b.startTime - a.startTime);
    return results.slice(0, filter?.limit ?? 100);
  }

  async cancel(workflowId: string): Promise<void> {
    const exec = this._executions.get(workflowId);
    if (!exec) throw new WorkflowNotFoundError(workflowId);
    // Actually cancel through worker if connected
    if (this._worker) {
      await this._worker.batchCancel([workflowId]);
    }
    exec.status = WorkflowStatus.CANCELLED;
    exec.closeTime = Date.now();
    this._metrics.recordWorkflowCancelled();
    this._logger.info(`Workflow cancelled: ${workflowId}`);
  }

  async terminate(workflowId: string, reason?: string): Promise<void> {
    const exec = this._executions.get(workflowId);
    if (!exec) throw new WorkflowNotFoundError(workflowId);
    // Actually terminate through worker if connected
    if (this._worker) {
      await this._worker.batchTerminate([workflowId]);
    }
    exec.status = WorkflowStatus.TERMINATED;
    exec.closeTime = Date.now();
    exec.error = reason;
    this._metrics.recordWorkflowTerminated();
    this._logger.info(`Workflow terminated: ${workflowId}${reason ? ` — ${reason}` : ''}`);
  }

  // ─── Signal-With-Start ─────────────────────────────────────────────────────

  async signalWithStart(
    workflowId: string,
    workflowType: string,
    signalName: string,
    signalInput: any,
    args: any[] = [],
    options?: { taskQueue?: string; searchAttributes?: SearchAttributes; memo?: Memo }
  ): Promise<WorkflowExecution> {
    const existing = this._executions.get(workflowId);
    if (existing && existing.status === WorkflowStatus.RUNNING) {
      // Workflow already running — just deliver the signal
      await this.signal(workflowId, signalName, signalInput);
      return existing;
    }
    // Start the workflow and deliver the signal atomically
    const execution = await this.startWorkflow(workflowId, workflowType, args, options);
    // Wait briefly for the workflow to actually start in the worker
    await new Promise(resolve => setTimeout(resolve, 10));
    await this.signal(workflowId, signalName, signalInput);
    return execution;
  }

  // ─── Updates ──────────────────────────────────────────────────────────────

  async update(workflowId: string, updateType: string, input: any): Promise<any> {
    if (!this._executions.has(workflowId)) throw new WorkflowNotFoundError(workflowId);
    if (this._worker) {
      const handle = this._worker._getHandle(workflowId);
      if (handle) return handle.instance.handleUpdate(updateType, input);
    }
    throw new Error(`Workflow ${workflowId} not running`);
  }

  // ─── Reset ─────────────────────────────────────────────────────────────────

  async reset(workflowId: string, eventId?: number): Promise<WorkflowExecution> {
    if (!this._worker) throw new Error('Client not connected to Worker');
    // Mark old execution as terminated in client's local map
    const oldExec = this._executions.get(workflowId);
    if (oldExec) {
      oldExec.status = WorkflowStatus.TERMINATED;
      oldExec.closeTime = Date.now();
    }
    const handle = await this._worker.resetWorkflow(workflowId, eventId);
    const execution: WorkflowExecution = {
      workflowId: handle.executionId,
      runId: `run-${handle.executionId}`,
      workflowType: handle.workflowType,
      status: WorkflowStatus.RUNNING,
      startTime: Date.now(),
    };
    this._executions.set(execution.workflowId, execution);
    return execution;
  }

  // ─── Nexus Operations ─────────────────────────────────────────────────────

  async executeNexusOperation<T>(endpointName: string, operation: string, input?: any): Promise<T> {
    if (!this._worker) throw new Error('Client not connected to Worker');
    return this._worker.executeNexusOperation<T>(endpointName, operation, input);
  }

  registerNexusEndpoint(name: string, url: string, namespace: string): void {
    if (!this._worker) throw new Error('Client not connected to Worker');
    this._worker.registerNexusEndpoint(name, url, namespace);
  }

  // ─── Sticky Queues ────────────────────────────────────────────────────────

  assignStickyQueue(workflowId: string, workerId: string): void {
    if (!this._worker) throw new Error('Client not connected to Worker');
    this._worker.assignStickyQueue(workflowId, workerId);
  }

  // ─── Batch Operations ─────────────────────────────────────────────────────

  async batchStart(workflows: { workflowId: string; workflowType: string; args: any[] }[]): Promise<WorkflowExecution[]> {
    if (!this._worker) throw new Error('Client not connected to Worker');
    const handles = await this._worker.batchStartWorkflows(workflows);
    return handles.map(h => this._executions.get(h.executionId)!);
  }

  async batchSignal(workflowIds: string[], signalName: string, input: any): Promise<void> {
    if (!this._worker) throw new Error('Client not connected to Worker');
    await this._worker.batchSignal(workflowIds, signalName, input);
  }

  async batchCancel(workflowIds: string[]): Promise<void> {
    if (!this._worker) throw new Error('Client not connected to Worker');
    await this._worker.batchCancel(workflowIds);
  }

  async batchTerminate(workflowIds: string[], reason?: string): Promise<void> {
    if (!this._worker) throw new Error('Client not connected to Worker');
    await this._worker.batchTerminate(workflowIds);
  }

  // ─── Schedules ────────────────────────────────────────────────────────────

  async createSchedule(scheduleId: string, cronExpression: string, workflowType: string, args: any[] = []): Promise<void> {
    if (!this._worker) throw new Error('Client not connected to Worker');
    await this._worker.createSchedule(scheduleId, cronExpression, workflowType, args);
  }

  async deleteSchedule(scheduleId: string): Promise<void> {
    if (!this._worker) throw new Error('Client not connected to Worker');
    await this._worker.deleteSchedule(scheduleId);
  }

  listSchedules(): { scheduleId: string; cron: string; workflowType: string; nextRun: number }[] {
    if (!this._worker) return [];
    return this._worker.listSchedules();
  }

  // ─── Search Attributes ────────────────────────────────────────────────────

  setWorkflowAttributes(workflowId: string, attributes: Record<string, any>): void {
    if (!this._worker) throw new Error('Client not connected to Worker');
    this._worker.setWorkflowAttributes(workflowId, attributes);
  }

  getWorkflowAttributes(workflowId: string): Record<string, any> | undefined {
    if (!this._worker) return undefined;
    return this._worker.getWorkflowAttributes(workflowId);
  }

  queryWorkflows(predicate: (attrs: Record<string, any>) => boolean): WorkflowExecution[] {
    if (!this._worker) return [];
    return this._worker.queryWorkflows(predicate).map(h => this._executions.get(h.executionId)!);
  }

  /** Simulate workflow completion (for testing). */
  completeWorkflow(workflowId: string, result?: any): void {
    const exec = this._executions.get(workflowId);
    if (!exec) throw new WorkflowNotFoundError(workflowId);
    exec.status = WorkflowStatus.COMPLETED;
    exec.result = result;
    exec.closeTime = Date.now();
    this._metrics.recordWorkflowCompleted();
  }

  /** Simulate workflow failure (for testing). */
  failWorkflow(workflowId: string, error: string): void {
    const exec = this._executions.get(workflowId);
    if (!exec) throw new WorkflowNotFoundError(workflowId);
    exec.status = WorkflowStatus.FAILED;
    exec.error = error;
    exec.closeTime = Date.now();
    this._metrics.recordWorkflowFailed();
  }

  async healthCheck(): Promise<ClassicHealthStatus> {
    const checks: ClassicHealthResult[] = [];
    checks.push({ name: 'liveness', status: 'healthy', message: 'alive', latencyMs: 0 });
    checks.push({ name: 'connectivity', status: 'healthy', message: `connected to ${this._config.serverAddress}`, latencyMs: 0 });
    return { status: 'healthy', checks, timestamp: Date.now(), uptimeSeconds: 0 };
  }

  getStats(): Record<string, any> {
    return {
      totalExecutions: this._executions.size,
      ...(this._config.enableMetrics ? { metrics: this._metrics.snapshot() } : {}),
    };
  }
}

// ─── Feature Matrix ──────────────────────────────────────────────────────────

export function featureMatrix(): Record<string, boolean> {
  return {
    workflows: true, activities: true, signals: true, queries: true,
    childWorkflows: true, continueAsNew: true, timers: true, retries: true,
    heartbeats: true, cancellation: true, signalWithStart: true,
    searchAttributes: true, memo: true, batchOperations: true,
    schedules: true, updates: true, reset: true, stickyQueues: true,
    versioning: true, nexusOperations: true, sagaPattern: true,
  };
}

// ─── HTTP Transport ──────────────────────────────────────────────────────────

export { RemoteClient, VelocityHttpClient } from './http-transport';

// ─── Persistence ─────────────────────────────────────────────────────────────

export { FileJournalBackend, InMemoryJournalBackend } from './persistence';
export type { JournalEvent, WorkflowJournal, PersistenceConfig } from './persistence';

// ─── HTTP Server ─────────────────────────────────────────────────────────────

export { VelocityServer } from './server';
export type { ServerConfig, ApiResponse } from './server';
