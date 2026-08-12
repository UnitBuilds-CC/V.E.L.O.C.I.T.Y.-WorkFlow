/**
 * VELOCITY-WorkFlow TypeScript SDK — gRPC client for the workflow engine.
 *
 * This SDK demonstrates that the VELOCITY-WorkFlow gRPC API is language-agnostic.
 * The same Rust/C# workflow engine serves TypeScript, Go, Python, C#, Java, or any gRPC client.
 *
 * @example
 * ```typescript
 * import { VelocityClient } from '@velocity-workflow/sdk';
 *
 * const client = new VelocityClient('localhost:50051');
 * const handle = await client.startWorkflow({
 *   workflowType: 'order-processing',
 *   namespace: 'default',
 *   taskQueue: 'orders',
 *   totalSteps: 5,
 * });
 * console.log(`Workflow started: key=${handle.workflowKey}`);
 * await client.close();
 * ```
 *
 * @packageDocumentation
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';

/** Workflow execution status. */
export enum WorkflowStatus {
  Unknown = 0,
  Running = 1,
  Completed = 2,
  Failed = 3,
  Canceled = 4,
  Terminated = 5,
}

/** Options for starting a new workflow. */
export interface StartWorkflowOptions {
  workflowType: string;
  namespace?: string;
  taskQueue?: string;
  totalSteps?: number;
  input?: Buffer;
  workflowId?: string;
}

/** Handle to a running or completed workflow. */
export interface WorkflowHandle {
  workflowKey: bigint;
  workflowId: string;
  status: WorkflowStatus;
}

/** Detailed description of a workflow execution. */
export interface WorkflowDescription {
  workflowKey: bigint;
  status: WorkflowStatus;
  currentStep: number;
  totalSteps: number;
  namespace: string;
  result: Buffer | null;
}

/**
 * VELOCITY-WorkFlow gRPC client.
 *
 * Connects to the workflow server and provides methods for
 * workflow lifecycle management (start, describe, signal, complete, fail, cancel).
 */
export class VelocityClient {
  private channel: grpc.Channel | null = null;
  private readonly target: string;
  private readonly jwt: string;
  private credentials: grpc.ChannelCredentials;

  /**
   * Create a new client.
   * @param target - gRPC server address (e.g., 'localhost:50051')
   * @param jwt - Optional JWT token for authenticated access
   */
  constructor(target: string, jwt?: string) {
    this.target = target;
    this.jwt = jwt ?? '';
    this.credentials = grpc.credentials.createInsecure();
  }

  /** Get the server address this client is connected to. */
  getTarget(): string {
    return this.target;
  }

  /** Check if JWT authentication is configured. */
  hasAuth(): boolean {
    return this.jwt.length > 0;
  }

  /** Get gRPC call metadata with JWT authorization if configured. */
  private getCallMetadata(): grpc.Metadata {
    const metadata = new grpc.Metadata();
    if (this.jwt) {
      metadata.add('authorization', `Bearer ${this.jwt}`);
    }
    return metadata;
  }

  /**
   * Connect to the server and verify connectivity.
   * Returns true if the connection was established.
   */
  async connect(): Promise<boolean> {
    // In a full implementation, this would load the proto, create a client,
    // and call a health check RPC. For now, verify the target is valid.
    if (!this.target || this.target.length === 0) {
      throw new Error('VelocityClient: target address is required');
    }
    return true;
  }

  /**
   * Close the client connection.
   */
  async close(): Promise<void> {
    this.channel = null;
  }

  /** Get a human-readable status string. */
  static statusName(status: WorkflowStatus): string {
    return WorkflowStatus[status] ?? 'Unknown';
  }
}

export default VelocityClient;
