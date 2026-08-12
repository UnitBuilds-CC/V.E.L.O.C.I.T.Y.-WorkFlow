package io.velocity;

import java.util.Map;
import java.util.HashMap;

/**
 * Helper functions for use within workflows.
 */
public class WorkflowHelpers {
    /**
     * Execute an activity from within a workflow.
     */
    public static Object executeActivity(WorkflowContext context, String activityType, Object input) {
        // In a real implementation, this would schedule the activity
        return null;
    }

    /**
     * Sleep for a specified duration.
     */
    public static void sleep(WorkflowContext context, long durationMs) {
        // In a real implementation, this would create a timer
        try {
            Thread.sleep(durationMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Execute a child workflow from within a workflow.
     */
    public static Object executeChildWorkflow(WorkflowContext context, String workflowType, Object input) {
        // In a real implementation, this would start a child workflow
        return null;
    }

    /**
     * Get workflow execution info.
     */
    public static Map<String, Object> getInfo(WorkflowContext context) {
        Map<String, Object> info = new HashMap<>();
        info.put("workflowId", context.getWorkflowId());
        info.put("runId", context.getRunId());
        info.put("workflowType", context.getWorkflowType());
        info.put("taskQueue", context.getTaskQueue());
        info.put("attempt", context.getAttempt());
        return info;
    }
}
