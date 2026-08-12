/**
 * Velocity Worker - Executes workflows and activities
 */
import { ConnectionOptions } from './connection';
import { WorkflowFunction } from './workflow';
import { ActivityFunction } from './activity';
export interface WorkerOptions {
    connection?: ConnectionOptions;
    namespace?: string;
    taskQueue: string;
    workflows?: Map<string, WorkflowFunction>;
    activities?: Map<string, ActivityFunction>;
}
export declare class Worker {
    private connection;
    private namespace;
    private taskQueue;
    private running;
    private workflowPoller?;
    private activityPoller?;
    constructor(options: WorkerOptions);
    /**
     * Start the worker
     */
    start(): Promise<void>;
    /**
     * Stop the worker
     */
    stop(): Promise<void>;
    /**
     * Poll for workflow tasks
     */
    private pollWorkflowTasks;
    /**
     * Poll for activity tasks
     */
    private pollActivityTasks;
    /**
     * Handle a workflow task
     */
    private handleWorkflowTask;
    /**
     * Handle an activity task
     */
    private handleActivityTask;
    /**
     * Check if worker is running
     */
    isRunning(): boolean;
    /**
     * Get task queue name
     */
    getTaskQueue(): string;
}
