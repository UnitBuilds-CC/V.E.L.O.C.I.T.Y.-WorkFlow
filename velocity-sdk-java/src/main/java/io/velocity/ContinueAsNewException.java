package io.velocity;

import java.util.*;

/**
 * Special exception used to signal the worker to continue the workflow as a new execution.
 *
 * Usage within a workflow:
 *   throw new ContinueAsNewException("LongRunningWorkflow", "main", input);
 */
public class ContinueAsNewException extends RuntimeException {
    private final String workflowType;
    private final String taskQueue;
    private final Object input;
    private final Long runTimeout;
    private final Long taskTimeout;
    private final RetryPolicy retryPolicy;
    private final Map<String, Object> memo;

    public ContinueAsNewException(String workflowType) {
        this(workflowType, "", null);
    }

    public ContinueAsNewException(String workflowType, String taskQueue, Object input) {
        super("continue-as-new: " + workflowType);
        this.workflowType = workflowType;
        this.taskQueue = taskQueue;
        this.input = input;
        this.runTimeout = null;
        this.taskTimeout = null;
        this.retryPolicy = null;
        this.memo = new HashMap<>();
    }

    public String getWorkflowType() { return workflowType; }
    public String getTaskQueue() { return taskQueue; }
    public Object getInput() { return input; }
    public Long getRunTimeout() { return runTimeout; }
    public Long getTaskTimeout() { return taskTimeout; }
    public RetryPolicy getRetryPolicy() { return retryPolicy; }
    public Map<String, Object> getMemo() { return memo; }
}
