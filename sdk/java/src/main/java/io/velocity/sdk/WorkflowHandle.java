package io.velocity.sdk;

/**
 * Handle to a running or completed workflow execution.
 *
 * @param workflowKey unique server-assigned key
 * @param workflowId  client-provided or derived workflow ID
 * @param runId       unique run identifier
 */
public record WorkflowHandle(long workflowKey, long workflowId, long runId) {
}
