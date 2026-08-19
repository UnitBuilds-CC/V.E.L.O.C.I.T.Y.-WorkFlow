/**
 * Auto-apply decorators for the VELOCITY-WorkFlow TypeScript SDK.
 *
 * These decorators enable annotation-driven workflow and activity registration.
 * When a class or function is decorated with @workflow or @activity, it is
 * automatically registered in a global registry. The Worker class scans this
 * registry at startup — no manual registration needed.
 *
 * @example
 * ```typescript
 * import { workflow, activity, WorkflowContext } from 'velocity-sdk-typescript';
 *
 * @activity
 * function processPayment(orderId: string): { status: string } {
 *   return { status: 'charged', orderId };
 * }
 *
 * @workflow
 * class OrderWorkflow {
 *   async run(ctx: WorkflowContext, orderId: string) {
 *     const result = await ctx.executeActivity('processPayment', orderId);
 *     return result;
 *   }
 * }
 * ```
 */

// ─── Global Registries ────────────────────────────────────────────────────────

const workflowRegistry = new Map<string, new () => WorkflowClass>();
const activityRegistry = new Map<string, Function>();

// ─── Type Definitions ─────────────────────────────────────────────────────────

export interface WorkflowClass {
  run(ctx: any, ...args: any[]): Promise<any> | any;
}

export interface ActivityOptions {
  name?: string;
  startToCloseTimeoutMs?: number;
  scheduleToCloseTimeoutMs?: number;
  retryMaxAttempts?: number;
}

export interface WorkflowOptions {
  name?: string;
  taskQueue?: string;
}

// ─── Workflow Decorator ───────────────────────────────────────────────────────

/**
 * Decorator that marks a class as a durable workflow.
 *
 * The decorated class is automatically registered in the workflow registry.
 * The Worker scans this registry at startup and dispatches tasks to the
 * matching class based on the workflow type name.
 *
 * Can be used with or without arguments:
 *   @workflow
 *   class MyWorkflow { ... }
 *
 *   @workflow({ name: 'custom_name', taskQueue: 'orders' })
 *   class MyWorkflow { ... }
 */
export function workflow(
  target?: new () => WorkflowClass,
  options?: WorkflowOptions
): any {
  function decorator(target: new () => WorkflowClass): void {
    const workflowName = options?.name || target.name;
    (target as any)._velocityWorkflowType = workflowName;
    (target as any)._velocityTaskQueue = options?.taskQueue;
    (target as any)._velocityIsWorkflow = true;
    workflowRegistry.set(workflowName, target);
  }

  if (target !== undefined) {
    // Used as @workflow without arguments
    return decorator(target);
  }
  // Used as @workflow(...) with arguments
  return decorator;
}

// ─── Activity Decorator ───────────────────────────────────────────────────────

/**
 * Decorator that marks a function as a durable activity.
 *
 * The decorated function is automatically registered in the activity registry.
 * The Worker scans this registry at startup and dispatches activity tasks to
 * the matching function based on the activity type name.
 *
 * Can be used with or without arguments:
 *   @activity
 *   function myActivity(...): ... { ... }
 *
 *   @activity({ name: 'custom_name', retryMaxAttempts: 3 })
 *   function myActivity(...): ... { ... }
 */
export function activity(
  target?: Function | PropertyDescriptor,
  options?: ActivityOptions
): any {
  function decorator(target: Function): void {
    const activityName = options?.name || target.name;
    (target as any)._velocityActivityType = activityName;
    (target as any)._velocityActivityOptions = options;
    (target as any)._velocityIsActivity = true;
    activityRegistry.set(activityName, target);
  }

  if (target !== undefined && typeof target === 'function') {
    // Used as @activity without arguments (function decorator)
    return decorator(target);
  }
  // Used as @activity(...) with arguments
  return decorator;
}

// ─── Signal Decorator ─────────────────────────────────────────────────────────

/**
 * Decorator that marks a method as a signal handler within a workflow class.
 *
 * @signal('cancel_order')
 * handleCancel(payload: any) { ... }
 */
export function signal(name?: string) {
  return function (
    target: any,
    propertyKey: string,
    descriptor: PropertyDescriptor
  ): void {
    const signalName = name || propertyKey;
    target._velocitySignals = target._velocitySignals || {};
    target._velocitySignals[signalName] = propertyKey;
  };
}

// ─── Query Decorator ──────────────────────────────────────────────────────────

/**
 * Decorator that marks a method as a query handler within a workflow class.
 *
 * @query('get_status')
 * handleStatusQuery(): string { ... }
 */
export function query(name?: string) {
  return function (
    target: any,
    propertyKey: string,
    descriptor: PropertyDescriptor
  ): void {
    const queryName = name || propertyKey;
    target._velocityQueries = target._velocityQueries || {};
    target._velocityQueries[queryName] = propertyKey;
  };
}

// ─── Update Decorator ─────────────────────────────────────────────────────────

/**
 * Decorator that marks a method as an update handler within a workflow class.
 *
 * @update('change_address')
 * handleAddressUpdate(payload: any) { ... }
 */
export function update(name?: string) {
  return function (
    target: any,
    propertyKey: string,
    descriptor: PropertyDescriptor
  ): void {
    const updateName = name || propertyKey;
    target._velocityUpdates = target._velocityUpdates || {};
    target._velocityUpdates[updateName] = propertyKey;
  };
}

// ─── Registry Access ──────────────────────────────────────────────────────────

/**
 * Return a copy of the global workflow registry.
 */
export function getRegisteredWorkflows(): Map<string, new () => WorkflowClass> {
  return new Map(workflowRegistry);
}

/**
 * Return a copy of the global activity registry.
 */
export function getRegisteredActivities(): Map<string, Function> {
  return new Map(activityRegistry);
}

/**
 * Clear both registries (useful for testing).
 */
export function clearRegistries(): void {
  workflowRegistry.clear();
  activityRegistry.clear();
}

/**
 * Count of registered workflows.
 */
export function workflowCount(): number {
  return workflowRegistry.size;
}

/**
 * Count of registered activities.
 */
export function activityCount(): number {
  return activityRegistry.size;
}
