"use strict";
/**
 * Velocity Worker - Executes workflows and activities
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.Worker = void 0;
const connection_1 = require("./connection");
const workflow_1 = require("./workflow");
const activity_1 = require("./activity");
class Worker {
    constructor(options) {
        this.running = false;
        this.namespace = options.namespace || 'default';
        this.taskQueue = options.taskQueue;
        if (options.connection) {
            this.connection = new connection_1.Connection(options.connection);
        }
        else {
            this.connection = new connection_1.Connection({ address: 'localhost:7233' });
        }
        // Register workflows and activities if provided
        if (options.workflows) {
            options.workflows.forEach((fn, name) => workflow_1.Workflow.register(name, fn));
        }
        if (options.activities) {
            options.activities.forEach((fn, name) => activity_1.Activity.register(name, fn));
        }
    }
    /**
     * Start the worker
     */
    async start() {
        if (this.running) {
            throw new Error('Worker is already running');
        }
        this.running = true;
        console.log(`Worker started for task queue: ${this.taskQueue}`);
        // Start polling for workflow tasks
        this.workflowPoller = this.pollWorkflowTasks();
        // Start polling for activity tasks
        this.activityPoller = this.pollActivityTasks();
        // Wait for both pollers
        await Promise.all([this.workflowPoller, this.activityPoller]);
    }
    /**
     * Stop the worker
     */
    async stop() {
        if (!this.running) {
            return;
        }
        this.running = false;
        console.log('Worker stopping...');
        // Wait for pollers to finish
        if (this.workflowPoller) {
            await this.workflowPoller;
        }
        if (this.activityPoller) {
            await this.activityPoller;
        }
        this.connection.close();
        console.log('Worker stopped');
    }
    /**
     * Poll for workflow tasks
     */
    async pollWorkflowTasks() {
        while (this.running) {
            try {
                const task = await this.connection.pollWorkflowTaskQueue({
                    namespace: this.namespace,
                    taskQueue: this.taskQueue,
                });
                if (task && task.task_token) {
                    await this.handleWorkflowTask(task);
                }
            }
            catch (error) {
                console.error('Error polling workflow task:', error);
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }
    /**
     * Poll for activity tasks
     */
    async pollActivityTasks() {
        while (this.running) {
            try {
                const task = await this.connection.pollActivityTaskQueue({
                    namespace: this.namespace,
                    taskQueue: this.taskQueue,
                });
                if (task && task.task_token) {
                    await this.handleActivityTask(task);
                }
            }
            catch (error) {
                console.error('Error polling activity task:', error);
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }
    /**
     * Handle a workflow task
     */
    async handleWorkflowTask(task) {
        try {
            const workflowType = task.workflow_type?.name || task.workflow_type;
            const workflowFn = workflow_1.Workflow.get(workflowType);
            if (!workflowFn) {
                throw new Error(`Workflow ${workflowType} not registered`);
            }
            // Create workflow context
            const ctx = {
                workflowId: task.workflow_execution?.workflow_id || '',
                runId: task.workflow_execution?.run_id || '',
                taskQueue: this.taskQueue,
            };
            // Parse input
            const input = task.input ? JSON.parse(task.input.data.toString()) : undefined;
            // Execute workflow
            const result = await workflowFn(ctx, input);
            // Complete workflow task with result
            await this.connection.respondWorkflowTaskCompleted({
                taskToken: task.task_token,
                commands: [
                    {
                        attributes: {
                            completeWorkflow: {
                                result: result ? { data: Buffer.from(JSON.stringify(result)), encoding: 0, metadata: {} } : undefined,
                            },
                        },
                    },
                ],
            });
            console.log(`Workflow ${workflowType} completed`);
        }
        catch (error) {
            console.error('Error handling workflow task:', error);
            // Fail workflow
            await this.connection.respondWorkflowTaskCompleted({
                taskToken: task.task_token,
                commands: [
                    {
                        attributes: {
                            failWorkflow: {
                                failure: {
                                    data: Buffer.from(error instanceof Error ? error.message : String(error)),
                                    encoding: 0,
                                    metadata: {},
                                },
                            },
                        },
                    },
                ],
            });
        }
    }
    /**
     * Handle an activity task
     */
    async handleActivityTask(task) {
        try {
            const activityType = task.activity_type?.name || task.activity_type;
            const activityFn = activity_1.Activity.get(activityType);
            if (!activityFn) {
                throw new Error(`Activity ${activityType} not registered`);
            }
            // Create activity context
            const ctx = {
                taskToken: task.task_token,
                workflowExecution: {
                    workflowId: task.workflow_execution?.workflow_id || '',
                    runId: task.workflow_execution?.run_id || '',
                },
                activityId: task.activity_id || '',
                activityType: activityType,
                scheduledTime: task.scheduled_time?.seconds ? task.scheduled_time.seconds * 1000 : 0,
                startedTime: task.started_time?.seconds ? task.started_time.seconds * 1000 : 0,
                attempt: task.attempt || 1,
            };
            // Parse input
            const input = task.input ? JSON.parse(task.input.data.toString()) : undefined;
            // Execute activity
            const result = await activityFn(ctx, input);
            // Complete activity task
            await this.connection.respondActivityTaskCompleted({
                taskToken: task.task_token,
                result,
            });
            console.log(`Activity ${activityType} completed`);
        }
        catch (error) {
            console.error('Error handling activity task:', error);
            // Fail activity
            await this.connection.respondActivityTaskFailed({
                taskToken: task.task_token,
                failure: error instanceof Error ? error.message : String(error),
            });
        }
    }
    /**
     * Check if worker is running
     */
    isRunning() {
        return this.running;
    }
    /**
     * Get task queue name
     */
    getTaskQueue() {
        return this.taskQueue;
    }
}
exports.Worker = Worker;
//# sourceMappingURL=worker.js.map