/**
 * Workflow definition API
 */
import { ActivityOptions, ChildWorkflowOptions } from './types';
export interface WorkflowContext {
    workflowId: string;
    runId: string;
    taskQueue: string;
    memo?: Record<string, any>;
    searchAttributes?: Record<string, any>;
}
export type WorkflowFunction<TInput = any, TOutput = any> = (ctx: WorkflowContext, input: TInput) => Promise<TOutput>;
export declare class Workflow {
    private static workflows;
    /**
     * Register a workflow function
     */
    static register<TInput = any, TOutput = any>(name: string, fn: WorkflowFunction<TInput, TOutput>): void;
    /**
     * Get a registered workflow function
     */
    static get(name: string): WorkflowFunction | undefined;
    /**
     * Check if a workflow is registered
     */
    static has(name: string): boolean;
}
/**
 * Define a workflow
 */
export declare function defineWorkflow<TInput = any, TOutput = any>(name: string, fn: WorkflowFunction<TInput, TOutput>): void;
/**
 * Workflow context helpers
 */
export declare class WorkflowHelpers {
    /**
     * Schedule an activity
     */
    static executeActivity<TInput = any, TOutput = any>(options: ActivityOptions): Promise<TOutput>;
    /**
     * Sleep for a duration
     */
    static sleep(duration: number): Promise<void>;
    /**
     * Start a child workflow
     */
    static executeChildWorkflow<TInput = any, TOutput = any>(options: ChildWorkflowOptions): Promise<TOutput>;
    /**
     * Get current workflow info
     */
    static getInfo(): WorkflowContext;
}
