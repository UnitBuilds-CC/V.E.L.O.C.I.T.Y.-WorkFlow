/**
 * VELOCITY-WorkFlow TypeScript SDK - Error hierarchy.
 *
 * Defines error types that map to gRPC status codes and server error codes.
 * All exceptions include an error code, message, and retryable flag.
 *
 * @packageDocumentation
 */

/** Base error class for all VELOCITY-WorkFlow errors. */
export class VelocityError extends Error {
  readonly errorCode: number;
  readonly retryable: boolean;
  readonly details?: Record<string, any>;

  constructor(
    message: string,
    errorCode: number = 0,
    retryable: boolean = false,
    details?: Record<string, any>
  ) {
    super(message);
    this.name = 'VelocityError';
    this.errorCode = errorCode;
    this.retryable = retryable;
    this.details = details;
  }

  toString(): string {
    const retry = this.retryable ? ' (retryable)' : '';
    return `VelocityError[${this.errorCode}]: ${this.message}${retry}`;
  }
}

/** Raised when a workflow does not exist. */
export class WorkflowNotFoundError extends VelocityError {
  readonly workflowKey: bigint;

  constructor(workflowKey: bigint, message?: string) {
    super(message || `Workflow not found: ${workflowKey}`, 1, false);
    this.name = 'WorkflowNotFoundError';
    this.workflowKey = workflowKey;
  }
}

/** Raised when attempting to modify a completed workflow. */
export class WorkflowAlreadyCompletedError extends VelocityError {
  readonly workflowKey: bigint;

  constructor(workflowKey: bigint, message?: string) {
    super(message || `Workflow already completed: ${workflowKey}`, 2, false);
    this.name = 'WorkflowAlreadyCompletedError';
    this.workflowKey = workflowKey;
  }
}

/** Raised when connection to the server fails. */
export class ConnectionError extends VelocityError {
  readonly target: string;

  constructor(target: string, message?: string) {
    super(message || `Failed to connect to ${target}`, 3, true);
    this.name = 'ConnectionError';
    this.target = target;
  }
}

/** Raised when an operation times out. */
export class TimeoutError extends VelocityError {
  readonly operation: string;
  readonly timeoutMs: number;

  constructor(operation: string, timeoutMs: number, message?: string) {
    super(message || `Operation '${operation}' timed out after ${timeoutMs}ms`, 4, true);
    this.name = 'TimeoutError';
    this.operation = operation;
    this.timeoutMs = timeoutMs;
  }
}

/** Raised when rate limit is exceeded. */
export class RateLimitError extends VelocityError {
  readonly retryAfterMs: number;

  constructor(retryAfterMs: number = 0, message?: string) {
    super(message || 'Rate limit exceeded', 5, true);
    this.name = 'RateLimitError';
    this.retryAfterMs = retryAfterMs;
  }
}

/** Raised when authentication fails. */
export class AuthenticationError extends VelocityError {
  constructor(message?: string) {
    super(message || 'Authentication failed', 6, false);
    this.name = 'AuthenticationError';
  }
}

/** Raised for internal server errors. */
export class InternalError extends VelocityError {
  constructor(message?: string) {
    super(message || 'Internal server error', 7, true);
    this.name = 'InternalError';
  }
}
