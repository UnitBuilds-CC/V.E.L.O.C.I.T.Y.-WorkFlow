package io.velocity.sdk.interceptors;

/**
 * Interface for workflow interceptors.
 * <p>
 * Interceptors can be used to add cross-cutting concerns like logging,
 * metrics, tracing, and custom logic to workflow lifecycle events.
 */
public interface WorkflowInterceptor {

    /**
     * Called before a workflow starts.
     *
     * @param workflowType the workflow type name
     * @param workflowId   the workflow ID
     */
    default void onStart(String workflowType, long workflowId) {
        // No-op by default
    }

    /**
     * Called after a workflow completes successfully.
     *
     * @param workflowId the workflow ID
     * @param result     the workflow result
     */
    default void onComplete(long workflowId, byte[] result) {
        // No-op by default
    }

    /**
     * Called when a workflow fails.
     *
     * @param workflowId the workflow ID
     * @param error      the error that caused the failure
     */
    default void onFail(long workflowId, Throwable error) {
        // No-op by default
    }

    /**
     * Called when a workflow receives a signal.
     *
     * @param workflowId the workflow ID
     * @param signalName the signal name
     */
    default void onSignal(long workflowId, String signalName) {
        // No-op by default
    }
}
