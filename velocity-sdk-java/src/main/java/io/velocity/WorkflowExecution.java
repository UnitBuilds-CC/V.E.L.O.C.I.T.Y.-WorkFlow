package io.velocity;

import java.util.Map;
import java.util.HashMap;

/**
 * Represents a workflow execution.
 */
public class WorkflowExecution {
    private final String workflowId;
    private final String runId;
    private final String workflowType;
    private final String taskQueue;
    private final WorkflowStatus status;
    private final long startedAt;
    private Long closedAt;
    private int historyLength;
    private Map<String, Object> memo;
    private Map<String, Object> searchAttributes;

    public WorkflowExecution(String workflowId, String runId, String workflowType, 
                            String taskQueue, WorkflowStatus status, long startedAt) {
        this.workflowId = workflowId;
        this.runId = runId;
        this.workflowType = workflowType;
        this.taskQueue = taskQueue;
        this.status = status;
        this.startedAt = startedAt;
        this.historyLength = 0;
        this.memo = new HashMap<>();
        this.searchAttributes = new HashMap<>();
    }

    // Getters
    public String getWorkflowId() { return workflowId; }
    public String getRunId() { return runId; }
    public String getWorkflowType() { return workflowType; }
    public String getTaskQueue() { return taskQueue; }
    public WorkflowStatus getStatus() { return status; }
    public long getStartedAt() { return startedAt; }
    public Long getClosedAt() { return closedAt; }
    public int getHistoryLength() { return historyLength; }
    public Map<String, Object> getMemo() { return memo; }
    public Map<String, Object> getSearchAttributes() { return searchAttributes; }

    // Setters
    public void setClosedAt(Long closedAt) { this.closedAt = closedAt; }
    public void setHistoryLength(int historyLength) { this.historyLength = historyLength; }
    public void setMemo(Map<String, Object> memo) { this.memo = memo; }
    public void setSearchAttributes(Map<String, Object> searchAttributes) { this.searchAttributes = searchAttributes; }
}
