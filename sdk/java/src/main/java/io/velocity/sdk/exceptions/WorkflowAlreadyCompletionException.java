package io.velocity.sdk.exceptions;

/**
 * Raised when attempting to modify a completed workflow.
 */
public class WorkflowAlreadyCompletionException extends VelocityException {

    private final long workflowKey;

    /**
     * Create a new WorkflowAlreadyCompletionException.
     *
     * @param workflowKey the workflow key that is already completed
     */
    public WorkflowAlreadyCompletionException(long workflowKey) {
        super("Workflow already completed: " + workflowKey, 2, false);
        this.workflowKey = workflowKey;
    }

    /**
     * Create a new WorkflowAlreadyCompletionException with a custom message.
     *
     * @param workflowKey the workflow key that is already completed
     * @param message     custom error message
     */
    public WorkflowAlreadyCompletionException(long workflowKey, String message) {
        super(message, 2, false);
        this.workflowKey = workflowKey;
    }

    /** Get the workflow key that is already completed. */
    public long getWorkflowKey() {
        return workflowKey;
    }
}
