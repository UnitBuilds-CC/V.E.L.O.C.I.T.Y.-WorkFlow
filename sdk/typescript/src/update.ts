/**
 * Workflow Update API — synchronous workflow mutation.
 *
 * Unlike signals (fire-and-forget), updates provide:
 * - Synchronous request/response semantics
 * - Wait policies (Accepted, Completed, Admitted)
 * - Validation before execution
 * - Named update handlers registered by workflows
 *
 * Usage:
 *   import { UpdateClient, UpdateWaitPolicy } from './update';
 *
 *   const client = new UpdateClient('localhost:7234');
 *   const result = await client.executeUpdate({
 *     workflowKey: 42,
 *     updateName: 'setAmount',
 *     args: { amount: 100 },
 *     waitPolicy: UpdateWaitPolicy.Completed,
 *   });
 */

export enum UpdateStatus {
  Admitted = 0,
  Accepted = 1,
  Completed = 2,
  Rejected = 3,
}

export enum UpdateWaitPolicy {
  Admitted = 0,
  Accepted = 1,
  Completed = 2,
}

export interface UpdateRequest {
  workflowKey: number;
  updateId: string;
  updateName: string;
  args?: unknown;
  waitPolicy: UpdateWaitPolicy;
}

/** Options for executing an update. */
export interface UpdateOptions {
  workflowKey: number;
  updateName: string;
  args?: unknown;
  waitPolicy?: UpdateWaitPolicy;
  updateId?: string;
}

export interface UpdateResult {
  updateId: string;
  status: UpdateStatus;
  result?: unknown;
  error?: string;
  durationMs: number;
}

export interface UpdateHandler {
  name: string;
  handler: (args: unknown) => unknown;
  validator?: (args: unknown) => boolean;
}

export class UpdateClient {
  private serverAddress: string;
  private handlers: Map<string, UpdateHandler> = new Map();
  private pending: Map<string, UpdateResult> = new Map();

  constructor(serverAddress: string = 'localhost:7234') {
    this.serverAddress = serverAddress;
  }

  registerHandler(
    name: string,
    handler: (args: unknown) => unknown,
    validator?: (args: unknown) => boolean,
  ): void {
    this.handlers.set(name, { name, handler, validator });
  }

  async executeUpdate(opts: {
    workflowKey: number;
    updateName: string;
    args?: unknown;
    waitPolicy?: UpdateWaitPolicy;
    updateId?: string;
  }): Promise<UpdateResult> {
    const uid = opts.updateId || `update-${opts.workflowKey}-${Date.now()}`;
    const start = Date.now();
    const waitPolicy = opts.waitPolicy ?? UpdateWaitPolicy.Completed;

    const handler = this.handlers.get(opts.updateName);
    if (!handler) {
      const result: UpdateResult = {
        updateId: uid,
        status: UpdateStatus.Rejected,
        error: `No handler registered for update '${opts.updateName}'`,
        durationMs: Date.now() - start,
      };
      this.pending.set(uid, result);
      return result;
    }

    if (handler.validator && !handler.validator(opts.args)) {
      const result: UpdateResult = {
        updateId: uid,
        status: UpdateStatus.Rejected,
        error: 'Update validation failed',
        durationMs: Date.now() - start,
      };
      this.pending.set(uid, result);
      return result;
    }

    try {
      const value = handler.handler(opts.args);
      const result: UpdateResult = {
        updateId: uid,
        status: UpdateStatus.Completed,
        result: value,
        durationMs: Date.now() - start,
      };
      this.pending.set(uid, result);
      return result;
    } catch (e: unknown) {
      const result: UpdateResult = {
        updateId: uid,
        status: UpdateStatus.Rejected,
        error: e instanceof Error ? e.message : String(e),
        durationMs: Date.now() - start,
      };
      this.pending.set(uid, result);
      return result;
    }
  }

  getUpdateResult(updateId: string): UpdateResult | undefined {
    return this.pending.get(updateId);
  }

  listHandlers(): string[] {
    return Array.from(this.handlers.keys());
  }

  listPending(): string[] {
    return Array.from(this.pending.keys());
  }
}
