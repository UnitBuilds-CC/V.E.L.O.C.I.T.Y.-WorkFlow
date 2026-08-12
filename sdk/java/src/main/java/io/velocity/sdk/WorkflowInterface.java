package io.velocity.sdk;

/**
 * Interface that all workflow classes should implement.
 * Provides lifecycle hooks for the VELOCITY-WorkFlow engine.
 */
public interface WorkflowInterface {
    /**
     * Called when the workflow is first started.
     * Override to perform initialization.
     */
    default void onInit() {}

    /**
     * Called when the workflow completes successfully.
     */
    default void onComplete() {}

    /**
     * Called when the workflow fails with an error.
     *
     * @param error the failure cause
     */
    default void onFailure(Throwable error) {}
}
