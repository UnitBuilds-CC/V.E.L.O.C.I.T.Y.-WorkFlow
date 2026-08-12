/**
 * Velocity Client - High-level API for workflow management
 */
import { ConnectionOptions } from './connection';
import { WorkflowOptions, WorkflowResult, SignalOptions, QueryOptions } from './types';
export interface ClientOptions {
    connection?: ConnectionOptions;
    namespace?: string;
}
export declare class Client {
    private connection;
    private namespace;
    constructor(options?: ClientOptions);
    /**
     * Start a new workflow execution
     */
    start<TInput = any, TOutput = any>(options: WorkflowOptions): Promise<WorkflowResult<TOutput>>;
    /**
     * Start a workflow and wait for its result
     */
    execute<TInput = any, TOutput = any>(options: WorkflowOptions): Promise<TOutput>;
    /**
     * Signal a running workflow
     */
    signal(workflowId: string, options: SignalOptions): Promise<void>;
    /**
     * Query a workflow
     */
    query<T = any>(workflowId: string, options: QueryOptions): Promise<T>;
    /**
     * Terminate a running workflow
     */
    terminate(workflowId: string, reason?: string): Promise<void>;
    /**
     * Cancel a running workflow
     */
    cancel(workflowId: string): Promise<void>;
    /**
     * Get workflow execution details
     */
    describe(workflowId: string): Promise<any>;
    /**
     * Get workflow execution history
     */
    getHistory(workflowId: string): Promise<any[]>;
    /**
     * Get a workflow handle for an existing workflow
     */
    getWorkflow(workflowId: string): WorkflowHandle;
    /**
     * Close the client connection
     */
    close(): void;
}
/**
 * Handle to an existing workflow execution
 */
export declare class WorkflowHandle {
    private client;
    private workflowId;
    constructor(client: Client, workflowId: string);
    /**
     * Get the workflow ID
     */
    getWorkflowId(): string;
    /**
     * Signal this workflow
     */
    signal(signalName: string, ...args: any[]): Promise<void>;
    /**
     * Query this workflow
     */
    query<T = any>(queryType: string, ...args: any[]): Promise<T>;
    /**
     * Terminate this workflow
     */
    terminate(reason?: string): Promise<void>;
    /**
     * Cancel this workflow
     */
    cancel(): Promise<void>;
    /**
     * Get workflow description
     */
    describe(): Promise<any>;
    /**
     * Get workflow history
     */
    history(): Promise<any[]>;
    /**
     * Wait for workflow result
     */
    result<T = any>(): Promise<T>;
}
