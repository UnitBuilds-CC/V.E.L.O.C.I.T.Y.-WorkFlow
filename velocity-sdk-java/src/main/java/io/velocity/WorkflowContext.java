package io.velocity;

import java.util.Map;
import java.util.HashMap;

/**
 * Context for workflow execution.
 */
public class WorkflowContext {
    private final String workflowId;
    private final String runId;
    private final String workflowType;
    private final String taskQueue;
    private int attempt;
    private Map<String, Object> memo;
    private Map<String, Object> searchAttributes;

    public WorkflowContext(String workflowId, String runId, String workflowType, String taskQueue) {
        this.workflowId = workflowId;
        this.runId = runId;
        this.workflowType = workflowType;
        this.taskQueue = taskQueue;
        this.attempt = 1;
        this.memo = new HashMap<>();
        this.searchAttributes = new HashMap<>();
    }

    // Getters
    public String getWorkflowId() { return workflowId; }
    public String getRunId() { return runId; }
    public String getWorkflowType() { return workflowType; }
    public String getTaskQueue() { return taskQueue; }
    public int getAttempt() { return attempt; }
    public Map<String, Object> getMemo() { return memo; }
    public Map<String, Object> getSearchAttributes() { return searchAttributes; }

    // Setters
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public void setMemo(Map<String, Object> memo) { this.memo = memo; }
    public void setSearchAttributes(Map<String, Object> searchAttributes) { this.searchAttributes = searchAttributes; }
}
