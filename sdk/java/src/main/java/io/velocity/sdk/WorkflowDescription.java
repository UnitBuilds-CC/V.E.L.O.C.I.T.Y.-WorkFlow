package io.velocity.sdk;

/**
 * Description of a workflow's current state.
 *
 * @param workflowKey unique server-assigned key
 * @param status      current execution status
 * @param currentStep current step index
 * @param totalSteps  total number of steps
 */
public record WorkflowDescription(
        long workflowKey,
        WorkflowStatus status,
        int currentStep,
        int totalSteps
) {
}
