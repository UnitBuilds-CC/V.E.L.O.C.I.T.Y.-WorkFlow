/**
 * VELOCITY-WorkFlow TypeScript SDK
 * @packageDocumentation
 */

export { VelocityClient, WorkflowStatus } from './client';
export type { StartWorkflowOptions, WorkflowHandle, WorkflowDescription, ListWorkflowOptions, ListWorkflowsResult } from './client';
export { transpileTypeScript, isTemporalWorkflow } from './transpiler';
export type { TranspilerConfig, TranspileStats, TranspileResult } from './transpiler';

// Workflow primitives (Temporal-compatible API)
export {
  getVersion, hasVersion, sleep, sleepUntil, condition,
  proxyActivities, ActivityInvocationError,
  executeChildWorkflow, startChildWorkflow,
  continueAsNew, ContinueAsNewError,
  patched, isReplaying, sideEffect, randomUUID,
  isCanceled, withCancellation,
  setSignalHandler, setQueryHandler, setUpdateHandler,
  upsertSearchAttributes, getMemo, upsertMemo,
  workflowInfo, workflowNow, workflowLog,
} from './workflow';
export type { ActivityOptions, ChildWorkflowOptions, WorkflowInfo } from './workflow';

// Worker & Workflow Context
export {
  Worker,
  WorkflowContext,
  ActivityScheduledMessage,
  TimerScheduledMessage,
  SignalWaitMessage,
  ChildWorkflowScheduledMessage,
  ContinueAsNewMessage,
} from './worker';
export type {
  WorkerOptions,
  WorkerStats,
  WorkerInterceptor,
  WorkflowImplementation,
  ActivityImplementation,
  ActivityOptions as WorkerActivityOptions,
  RetryPolicy,
  ChildWorkflowOptions as WorkerChildWorkflowOptions,
  ChildWorkflowHandle,
  WorkflowInterceptInput,
  ActivityInterceptInput,
} from './worker';

// Errors
export {
  VelocityError,
  WorkflowNotFoundError,
  WorkflowAlreadyCompletedError,
  ConnectionError,
  TimeoutError,
  RateLimitError,
  AuthenticationError,
  InternalError,
} from './errors';

// Interceptors
export {
  LoggingInterceptor,
  MetricsInterceptor,
  InterceptorChain,
  composeInterceptors,
} from './interceptors';
export type { WorkflowInterceptor, ActivityInterceptor } from './interceptors';

// Testing
export {
  TestWorkflowEnvironment,
  MockVelocityClient,
  assertWorkflowCompleted,
  assertSignalReceived,
} from './testing';

// Retry
export {
  retryWithBackoff,
  calculateBackoff,
  validateRetryOptions,
  createRetry,
  DEFAULT_RETRY_OPTIONS,
} from './retry';
export type { RetryOptions } from './retry';

// Payload Codec
export {
  JsonCodec,
  BinaryCodec,
  NullCodec,
  CodecChain,
} from './payload-codec';
export type { PayloadCodec } from './payload-codec';

// Workflow Stub
export { WorkflowStub } from './workflow-stub';
export type { WorkflowStubOptions } from './workflow-stub';

// Update
export { UpdateClient } from './update';
export type { UpdateOptions, UpdateResult } from './update';
