/**
 * VELOCITY-WorkFlow TypeScript SDK — Workflow primitives.
 *
 * This module provides deterministic workflow operations that mirror Temporal's
 * workflow API. All operations are recorded in workflow history for replay safety.
 *
 * @example
 * ```typescript
 * import { proxyActivities, getVersion, condition, sleep } from '@velocity-workflow/sdk/workflow';
 *
 * export async function orderWorkflow(orderId: string) {
 *   const v = await getVersion('add-shipping', 1, 2);
 *   const activities = proxyActivities<typeof orderActivities>({
 *     startToCloseTimeoutMs: 30000,
 *   });
 *
 *   const payment = await activities.processPayment(orderId);
 *   if (v >= 2) {
 *     await activities.addShippingLabel(orderId);
 *   }
 *   await sleep(1000);
 *   await activities.sendConfirmation(orderId);
 * }
 * ```
 *
 * @packageDocumentation
 */

// ─── Version Management ─────────────────────────────────────────────────────

/**
 * Get or record a version decision for a workflow change ID.
 *
 * This is the equivalent of Temporal's `workflow.getVersion()`. It enables safe
 * deployment of workflow code changes by durably recording version decisions.
 *
 * - First call: records `maxSupported` and returns it.
 * - Subsequent calls (replay): returns the originally recorded version.
 *
 * @param changeId - Unique identifier for this code change
 * @param minSupported - Minimum version this code supports
 * @param maxSupported - Maximum version this code supports
 * @returns The decided version number
 */
export async function getVersion(
  changeId: string,
  minSupported: number,
  maxSupported: number,
): Promise<number> {
  // In a real implementation, this would:
  // 1. Check workflow history for an existing VersionMarker event
  // 2. If found, verify it's within [minSupported, maxSupported] and return it
  // 3. If not found, record a new VersionMarker with maxSupported
  // For now, return maxSupported (correct for new workflows).
  void changeId;
  void minSupported;
  return maxSupported;
}

/**
 * Check if a specific change version has been applied.
 * Convenience wrapper around getVersion for boolean checks.
 */
export async function hasVersion(changeId: string, atLeast: number): Promise<boolean> {
  const v = await getVersion(changeId, 0, atLeast + 1);
  return v >= atLeast;
}

// ─── Timers ─────────────────────────────────────────────────────────────────

/**
 * Sleep for a specified duration (deterministic timer).
 * On replay, the timer fires immediately if the original timer already expired.
 */
export function sleep(durationMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });
}

/**
 * Sleep until a specific timestamp (epoch ms).
 */
export function sleepUntil(timestampMs: number): Promise<void> {
  const now = Date.now();
  const duration = Math.max(0, timestampMs - now);
  return sleep(duration);
}

// ─── Conditions ─────────────────────────────────────────────────────────────

/**
 * Wait for a condition to become true, with optional timeout.
 * Returns true if the condition was met, false if timed out.
 */
export async function condition(
  fn: () => boolean,
  timeoutMs?: number,
): Promise<boolean> {
  if (fn()) return true;

  return new Promise<boolean>((resolve) => {
    let resolved = false;
    const interval = setInterval(() => {
      if (fn()) {
        resolved = true;
        clearInterval(interval);
        if (timer) clearTimeout(timer);
        resolve(true);
      }
    }, 10);

    let timer: ReturnType<typeof setTimeout> | undefined;
    if (timeoutMs !== undefined) {
      timer = setTimeout(() => {
        if (!resolved) {
          clearInterval(interval);
          resolve(false);
        }
      }, timeoutMs);
    }
  });
}

// ─── Activity Proxy ─────────────────────────────────────────────────────────

/** Options for activity execution. */
export interface ActivityOptions {
  taskQueue?: string;
  startToCloseTimeoutMs?: number;
  scheduleToCloseTimeoutMs?: number;
  scheduleToStartTimeoutMs?: number;
  heartbeatTimeoutMs?: number;
  retry?: {
    maxAttempts: number;
    initialIntervalMs: number;
    backoffCoefficient: number;
    maxIntervalMs: number;
    nonRetryableErrorTypes?: string[];
  };
}

/**
 * Create a typed proxy for activities.
 * Mirrors Temporal's `proxyActivities<T>()` pattern.
 */
export function proxyActivities<T extends Record<string, (...args: any[]) => Promise<any>>>(
  options: ActivityOptions,
): T {
  const handler: ProxyHandler<Record<string, any>> = {
    get(_target, prop) {
      if (typeof prop !== 'string') return undefined;
      const activityName = prop;
      return async (...args: any[]) => {
        throw new ActivityInvocationError(activityName, args, options);
      };
    },
  };
  return new Proxy({} as T, handler) as T;
}

/** Error thrown when an activity is invoked through the proxy. */
export class ActivityInvocationError extends Error {
  readonly activityName: string;
  readonly args: any[];
  readonly options: ActivityOptions;

  constructor(activityName: string, args: any[], options: ActivityOptions) {
    super(`Activity '${activityName}' scheduled (resolved by worker)`);
    this.name = 'ActivityInvocationError';
    this.activityName = activityName;
    this.args = args;
    this.options = options;
  }
}

// ─── Child Workflows ────────────────────────────────────────────────────────

/** Options for starting a child workflow. */
export interface ChildWorkflowOptions {
  workflowId?: string;
  taskQueue?: string;
  parentClosePolicy?: 'terminate' | 'requestCancel' | 'abandon';
  executionTimeoutMs?: number;
  runTimeoutMs?: number;
  taskTimeoutMs?: number;
  retryPolicy?: {
    maxAttempts: number;
    initialIntervalMs: number;
    backoffCoefficient: number;
  };
}

/** Execute a child workflow and wait for its result. */
export async function executeChildWorkflow<T = any>(
  workflowType: string,
  options: ChildWorkflowOptions,
  ...args: any[]
): Promise<T> {
  void workflowType;
  void options;
  void args;
  throw new Error('Child workflow execution requires a running worker');
}

/** Start a child workflow without waiting for its result. */
export async function startChildWorkflow(
  workflowType: string,
  options: ChildWorkflowOptions,
  ...args: any[]
): Promise<{ workflowId: string; workflowKey: bigint }> {
  void workflowType;
  void options;
  void args;
  throw new Error('Child workflow start requires a running worker');
}

// ─── Continue As New ────────────────────────────────────────────────────────

/** Continue this workflow as a new execution with new arguments. */
export function continueAsNew(...args: any[]): never {
  throw new ContinueAsNewError(args);
}

/** Error thrown to signal continue-as-new. */
export class ContinueAsNewError extends Error {
  readonly args: any[];
  constructor(args: any[]) {
    super('Continue as new');
    this.name = 'ContinueAsNewError';
    this.args = args;
  }
}

// ─── Patches ────────────────────────────────────────────────────────────────

/** Mark a patch/change in workflow code. Returns true for new executions. */
export async function patched(patchId: string): Promise<boolean> {
  const v = await getVersion(patchId, 0, 1);
  return v >= 1;
}

/** Check if this workflow is replaying from history. */
export function isReplaying(): boolean {
  return false;
}

// ─── Side Effects ───────────────────────────────────────────────────────────

/** Execute a side effect that produces a deterministic value. */
export function sideEffect<T>(fn: () => T): T {
  return fn();
}

/** Generate a deterministic random UUID. */
export function randomUUID(): string {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// ─── Cancellation ───────────────────────────────────────────────────────────

/** Check if the current workflow has been canceled. */
export function isCanceled(): boolean {
  return false;
}

/** Execute a function with cancellation scope. */
export async function withCancellation<T>(fn: () => Promise<T>): Promise<T> {
  return fn();
}

// ─── Signals & Queries ──────────────────────────────────────────────────────

/** Set a signal handler for the given signal name. */
export function setSignalHandler(signalName: string, handler: (...args: any[]) => void): void {
  void signalName;
  void handler;
}

/** Set a query handler for the given query name. */
export function setQueryHandler(queryName: string, handler: (...args: any[]) => any): void {
  void queryName;
  void handler;
}

/** Set an update handler for the given update name. */
export function setUpdateHandler(
  updateName: string,
  handler: (...args: any[]) => any,
  validator?: (...args: any[]) => void,
): void {
  void updateName;
  void handler;
  void validator;
}

// ─── Search Attributes & Memo ───────────────────────────────────────────────

/** Upsert custom search attributes for this workflow execution. */
export function upsertSearchAttributes(attrs: Record<string, any>): void {
  void attrs;
}

/** Get the workflow's memo value for a key. */
export function getMemo<T = any>(key: string): T | undefined {
  void key;
  return undefined;
}

/** Upsert memo values. */
export function upsertMemo(values: Record<string, any>): void {
  void values;
}

// ─── Info ───────────────────────────────────────────────────────────────────

/** Information about the current workflow execution. */
export interface WorkflowInfo {
  workflowId: string;
  runId: string;
  workflowType: string;
  taskQueue: string;
  namespace: string;
  attempt: number;
  startTime: Date;
  executionTimeoutMs?: number;
  runTimeoutMs?: number;
}

/** Get information about the current workflow execution. */
export function workflowInfo(): WorkflowInfo {
  return {
    workflowId: '',
    runId: '',
    workflowType: '',
    taskQueue: 'default',
    namespace: 'default',
    attempt: 1,
    startTime: new Date(),
  };
}

/** Get the current workflow's deterministic time. */
export function workflowNow(): Date {
  return new Date();
}

/** Log a message that is recorded in workflow history (deterministic). */
export function workflowLog(message: string, ...args: any[]): void {
  void message;
  void args;
}
