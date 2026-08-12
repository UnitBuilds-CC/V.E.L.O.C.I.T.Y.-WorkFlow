/**
 * V.E.L.O.C.I.T.Y.-WorkFlow TypeScript SDK
 * 
 * Hardware-native zero-allocation durable execution engine
 * Temporal alternative with superior performance
 */

export { Client, ClientOptions } from './client';
export { Worker, WorkerOptions } from './worker';
export { Workflow, WorkflowContext, defineWorkflow } from './workflow';
export { Activity, defineActivity } from './activity';
export { Connection, ConnectionOptions } from './connection';
export * from './types';
export {
  UpdateOptions,
  UpdateResult,
  ResetOptions,
  ContinueAsNewError,
  ScheduleClient,
  ScheduleOptions,
  ScheduleDescription,
  SearchAttributesClient,
  BatchOperationClient,
  BatchOperationOptions,
  BatchOperationDescription,
  Saga,
  SagaStep,
} from './advanced';
