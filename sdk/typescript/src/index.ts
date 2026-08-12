/**
 * VELOCITY-WorkFlow TypeScript SDK
 * @packageDocumentation
 */

export { VelocityClient, WorkflowStatus } from './client';
export type { StartWorkflowOptions, WorkflowHandle, WorkflowDescription } from './client';
export { transpileTypeScript, isTemporalWorkflow } from './transpiler';
export type { TranspilerConfig, TranspileStats, TranspileResult } from './transpiler';

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
