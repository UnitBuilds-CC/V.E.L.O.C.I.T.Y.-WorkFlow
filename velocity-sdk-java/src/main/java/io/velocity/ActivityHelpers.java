package io.velocity;

import java.util.Map;
import java.util.HashMap;

/**
 * Helper functions for use within activities.
 */
public class ActivityHelpers {
    /**
     * Record activity heartbeat.
     */
    public static void heartbeat(ActivityContext context, Object details) {
        // In a real implementation, this would send heartbeat to server
    }

    /**
     * Get activity execution info.
     */
    public static Map<String, Object> getInfo(ActivityContext context) {
        Map<String, Object> info = new HashMap<>();
        info.put("activityId", context.getActivityId());
        info.put("activityType", context.getActivityType());
        info.put("taskQueue", context.getTaskQueue());
        info.put("workflowId", context.getWorkflowId());
        info.put("runId", context.getRunId());
        info.put("attempt", context.getAttempt());
        return info;
    }
}
