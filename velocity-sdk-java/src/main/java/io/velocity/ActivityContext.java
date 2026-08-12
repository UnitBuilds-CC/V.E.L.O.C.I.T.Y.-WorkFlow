package io.velocity;

import java.util.Map;
import java.util.HashMap;

/**
 * Context for activity execution.
 */
public class ActivityContext {
    private final String activityId;
    private final String activityType;
    private final String taskQueue;
    private final String workflowId;
    private final String runId;
    private int attempt;
    private Long heartbeatTimeout;

    public ActivityContext(String activityId, String activityType, String taskQueue, 
                          String workflowId, String runId) {
        this.activityId = activityId;
        this.activityType = activityType;
        this.taskQueue = taskQueue;
        this.workflowId = workflowId;
        this.runId = runId;
        this.attempt = 1;
    }

    // Getters
    public String getActivityId() { return activityId; }
    public String getActivityType() { return activityType; }
    public String getTaskQueue() { return taskQueue; }
    public String getWorkflowId() { return workflowId; }
    public String getRunId() { return runId; }
    public int getAttempt() { return attempt; }
    public Long getHeartbeatTimeout() { return heartbeatTimeout; }

    // Setters
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public void setHeartbeatTimeout(Long heartbeatTimeout) { this.heartbeatTimeout = heartbeatTimeout; }
}
