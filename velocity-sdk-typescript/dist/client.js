"use strict";
/**
 * Velocity Client - High-level API for workflow management
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.WorkflowHandle = exports.Client = void 0;
const connection_1 = require("./connection");
class Client {
    constructor(options = {}) {
        this.namespace = options.namespace || 'default';
        if (options.connection) {
            this.connection = new connection_1.Connection(options.connection);
        }
        else {
            // Default connection to localhost:7233
            this.connection = new connection_1.Connection({ address: 'localhost:7233' });
        }
    }
    /**
     * Start a new workflow execution
     */
    async start(options) {
        const result = await this.connection.startWorkflow({
            namespace: this.namespace,
            workflowId: options.workflowId,
            workflowType: options.workflowType,
            taskQueue: options.taskQueue,
            input: options.input,
        });
        return {
            workflowExecution: {
                workflowId: result.workflowId,
                runId: result.runId,
            },
        };
    }
    /**
     * Start a workflow and wait for its result
     */
    async execute(options) {
        const { workflowExecution } = await this.start(options);
        // Poll for workflow completion
        while (true) {
            const description = await this.connection.describeWorkflow({
                namespace: this.namespace,
                workflowId: workflowExecution.workflowId,
            });
            if (description.execution_info?.status === 'COMPLETED') {
                return description.execution_info.result;
            }
            else if (description.execution_info?.status === 'FAILED') {
                throw new Error(`Workflow failed: ${description.execution_info.failure}`);
            }
            else if (description.execution_info?.status === 'CANCELLED') {
                throw new Error('Workflow was cancelled');
            }
            else if (description.execution_info?.status === 'TERMINATED') {
                throw new Error('Workflow was terminated');
            }
            // Wait before polling again
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }
    /**
     * Signal a running workflow
     */
    async signal(workflowId, options) {
        await this.connection.signalWorkflow({
            namespace: this.namespace,
            workflowId,
            signalName: options.signalName,
            input: options.args ? options.args[0] : undefined,
        });
    }
    /**
     * Query a workflow
     */
    async query(workflowId, options) {
        return await this.connection.queryWorkflow({
            namespace: this.namespace,
            workflowId,
            queryType: options.queryType,
            input: options.args ? options.args[0] : undefined,
        });
    }
    /**
     * Terminate a running workflow
     */
    async terminate(workflowId, reason) {
        await this.connection.terminateWorkflow({
            namespace: this.namespace,
            workflowId,
            reason,
        });
    }
    /**
     * Cancel a running workflow
     */
    async cancel(workflowId) {
        await this.connection.cancelWorkflow({
            namespace: this.namespace,
            workflowId,
        });
    }
    /**
     * Get workflow execution details
     */
    async describe(workflowId) {
        return await this.connection.describeWorkflow({
            namespace: this.namespace,
            workflowId,
        });
    }
    /**
     * Get workflow execution history
     */
    async getHistory(workflowId) {
        return await this.connection.getWorkflowHistory({
            namespace: this.namespace,
            workflowId,
        });
    }
    /**
     * Get a workflow handle for an existing workflow
     */
    getWorkflow(workflowId) {
        return new WorkflowHandle(this, workflowId);
    }
    /**
     * Close the client connection
     */
    close() {
        this.connection.close();
    }
}
exports.Client = Client;
/**
 * Handle to an existing workflow execution
 */
class WorkflowHandle {
    constructor(client, workflowId) {
        this.client = client;
        this.workflowId = workflowId;
    }
    /**
     * Get the workflow ID
     */
    getWorkflowId() {
        return this.workflowId;
    }
    /**
     * Signal this workflow
     */
    async signal(signalName, ...args) {
        await this.client.signal(this.workflowId, { signalName, args });
    }
    /**
     * Query this workflow
     */
    async query(queryType, ...args) {
        return await this.client.query(this.workflowId, { queryType, args });
    }
    /**
     * Terminate this workflow
     */
    async terminate(reason) {
        await this.client.terminate(this.workflowId, reason);
    }
    /**
     * Cancel this workflow
     */
    async cancel() {
        await this.client.cancel(this.workflowId);
    }
    /**
     * Get workflow description
     */
    async describe() {
        return await this.client.describe(this.workflowId);
    }
    /**
     * Get workflow history
     */
    async history() {
        return await this.client.getHistory(this.workflowId);
    }
    /**
     * Wait for workflow result
     */
    async result() {
        while (true) {
            const description = await this.describe();
            if (description.execution_info?.status === 'COMPLETED') {
                return description.execution_info.result;
            }
            else if (description.execution_info?.status === 'FAILED') {
                throw new Error(`Workflow failed: ${description.execution_info.failure}`);
            }
            else if (description.execution_info?.status === 'CANCELLED') {
                throw new Error('Workflow was cancelled');
            }
            else if (description.execution_info?.status === 'TERMINATED') {
                throw new Error('Workflow was terminated');
            }
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }
}
exports.WorkflowHandle = WorkflowHandle;
//# sourceMappingURL=client.js.map