"use strict";
/**
 * Workflow definition API
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.WorkflowHelpers = exports.Workflow = void 0;
exports.defineWorkflow = defineWorkflow;
class Workflow {
    /**
     * Register a workflow function
     */
    static register(name, fn) {
        Workflow.workflows.set(name, fn);
    }
    /**
     * Get a registered workflow function
     */
    static get(name) {
        return Workflow.workflows.get(name);
    }
    /**
     * Check if a workflow is registered
     */
    static has(name) {
        return Workflow.workflows.has(name);
    }
}
exports.Workflow = Workflow;
Workflow.workflows = new Map();
/**
 * Define a workflow
 */
function defineWorkflow(name, fn) {
    Workflow.register(name, fn);
}
/**
 * Workflow context helpers
 */
class WorkflowHelpers {
    /**
     * Schedule an activity
     */
    static async executeActivity(options) {
        // In a real implementation, this would create a ScheduleActivity command
        // and wait for the activity to complete via workflow task completion
        throw new Error('Activity execution not yet implemented in worker');
    }
    /**
     * Sleep for a duration
     */
    static async sleep(duration) {
        // In a real implementation, this would create a StartTimer command
        throw new Error('Timer not yet implemented in worker');
    }
    /**
     * Start a child workflow
     */
    static async executeChildWorkflow(options) {
        // In a real implementation, this would create a StartChildWorkflowExecution command
        throw new Error('Child workflow not yet implemented in worker');
    }
    /**
     * Get current workflow info
     */
    static getInfo() {
        // In a real implementation, this would return the current workflow context
        throw new Error('Workflow context not available');
    }
}
exports.WorkflowHelpers = WorkflowHelpers;
//# sourceMappingURL=workflow.js.map