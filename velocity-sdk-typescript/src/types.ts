/**
 * Core type definitions for the Velocity SDK
 */

export interface WorkflowExecution {
  workflowId: string;
  runId: string;
}

export interface WorkflowOptions {
  workflowId: string;
  taskQueue: string;
  workflowType: string;
  input?: any;
  executionTimeout?: number;
  runTimeout?: number;
  taskTimeout?: number;
  retryPolicy?: RetryPolicy;
  memo?: Record<string, any>;
  searchAttributes?: Record<string, any>;
}

export interface RetryPolicy {
  initialInterval?: number;
  backoffCoefficient?: number;
  maximumInterval?: number;
  maximumAttempts?: number;
  nonRetryableErrorTypes?: string[];
}

export interface SignalOptions {
  signalName: string;
  args?: any[];
}

export interface QueryOptions {
  queryType: string;
  args?: any[];
}

export interface WorkflowResult<T = any> {
  workflowExecution: WorkflowExecution;
  result?: T;
}

export interface HistoryEvent {
  eventId: number;
  eventType: string;
  eventTime: number;
  taskId: number;
  attributes?: any;
}

export interface TaskQueue {
  name: string;
  taskType: 'workflow' | 'activity';
  backlogCount: number;
  pollers: number;
}

export interface Schedule {
  scheduleId: string;
  workflowType: string;
  state: 'ACTIVE' | 'PAUSED' | 'COMPLETED';
  cronSchedule: string;
  lastActionTime: number;
}

export interface BatchOperation {
  jobId: string;
  operation: 'terminate' | 'cancel' | 'signal' | 'query';
  status: 'RUNNING' | 'COMPLETED' | 'FAILED';
  totalWorkflows: number;
  succeeded: number;
  failed: number;
}

export enum WorkflowStatus {
  RUNNING = 'RUNNING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  CANCELLED = 'CANCELLED',
  TERMINATED = 'TERMINATED',
  CONTINUED_AS_NEW = 'CONTINUED_AS_NEW',
  TIMED_OUT = 'TIMED_OUT',
}

export interface ActivityContext {
  taskToken: string;
  workflowExecution: WorkflowExecution;
  activityId: string;
  activityType: string;
  input?: any;
  heartbeatDetails?: any;
  heartbeatTimeout?: number;
  scheduledTime: number;
  startedTime: number;
  attempt: number;
}

export interface ActivityOptions {
  taskQueue: string;
  activityType: string;
  input?: any;
  scheduleToCloseTimeout?: number;
  scheduleToStartTimeout?: number;
  startToCloseTimeout?: number;
  heartbeatTimeout?: number;
  retryPolicy?: RetryPolicy;
}

export interface TimerOptions {
  duration: number;
}

export interface ChildWorkflowOptions {
  workflowId: string;
  workflowType: string;
  taskQueue: string;
  input?: any;
  executionTimeout?: number;
  runTimeout?: number;
  taskTimeout?: number;
  retryPolicy?: RetryPolicy;
}
