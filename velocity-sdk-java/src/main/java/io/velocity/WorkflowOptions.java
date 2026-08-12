package io.velocity;

import java.util.Map;
import java.util.HashMap;

/**
 * Options for starting a workflow.
 */
public class WorkflowOptions {
    private String workflowId;
    private String workflowType;
    private String taskQueue;
    private Object input;
    private Long executionTimeout;
    private Long runTimeout;
    private Long taskTimeout;
    private Map<String, Object> memo;
    private Map<String, Object> searchAttributes;
    private RetryPolicy retryPolicy;

    public WorkflowOptions() {
        this.memo = new HashMap<>();
        this.searchAttributes = new HashMap<>();
    }

    // Builder pattern
    public WorkflowOptions setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
        return this;
    }

    public WorkflowOptions setWorkflowType(String workflowType) {
        this.workflowType = workflowType;
        return this;
    }

    public WorkflowOptions setTaskQueue(String taskQueue) {
        this.taskQueue = taskQueue;
        return this;
    }

    public WorkflowOptions setInput(Object input) {
        this.input = input;
        return this;
    }

    public WorkflowOptions setExecutionTimeout(Long executionTimeout) {
        this.executionTimeout = executionTimeout;
        return this;
    }

    public WorkflowOptions setRunTimeout(Long runTimeout) {
        this.runTimeout = runTimeout;
        return this;
    }

    public WorkflowOptions setTaskTimeout(Long taskTimeout) {
        this.taskTimeout = taskTimeout;
        return this;
    }

    public WorkflowOptions setMemo(Map<String, Object> memo) {
        this.memo = memo;
        return this;
    }

    public WorkflowOptions setSearchAttributes(Map<String, Object> searchAttributes) {
        this.searchAttributes = searchAttributes;
        return this;
    }

    public WorkflowOptions setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    // Getters
    public String getWorkflowId() { return workflowId; }
    public String getWorkflowType() { return workflowType; }
    public String getTaskQueue() { return taskQueue; }
    public Object getInput() { return input; }
    public Long getExecutionTimeout() { return executionTimeout; }
    public Long getRunTimeout() { return runTimeout; }
    public Long getTaskTimeout() { return taskTimeout; }
    public Map<String, Object> getMemo() { return memo; }
    public Map<String, Object> getSearchAttributes() { return searchAttributes; }
    public RetryPolicy getRetryPolicy() { return retryPolicy; }
}
