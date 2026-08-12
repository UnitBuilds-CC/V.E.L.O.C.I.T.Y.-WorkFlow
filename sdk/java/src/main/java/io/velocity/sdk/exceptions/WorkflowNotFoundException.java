package io.velocity.sdk.exceptions;

/**
 * Raised when a workflow does not exist.
 */
public class WorkflowNotFoundException extends VelocityException {

    private final long workflowKey;

    /**
     * Create a new WorkflowNotFoundException.
     *
     * @param workflowKey the workflow key that was not found
     */
    public WorkflowNotFoundException(long workflowKey) {
        super("Workflow not found: " + workflowKey, 1, false);
        this.workflowKey = workflowKey;
    }

    /**
     * Create a new WorkflowNotFoundException with a custom message.
     *
     * @param workflowKey the workflow key that was not found
     * @param message     custom error message
     */
    public WorkflowNotFoundException(long workflowKey, String message) {
        super(message, 1, false);
        this.workflowKey = workflowKey;
    }

    /** Get the workflow key that was not found. */
    public long getWorkflowKey() {
        return workflowKey;
    }
}
