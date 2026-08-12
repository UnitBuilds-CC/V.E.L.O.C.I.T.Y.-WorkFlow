<?php

declare(strict_types=1);

namespace Velocity\SDK\Client;

use Velocity\SDK\Workflow\WorkflowExecution;
use Velocity\SDK\Workflow\WorkflowOptions;

/**
 * Contract for VELOCITY-WorkFlow client implementations.
 *
 * All client variants (gRPC, FFI) must implement this interface
 * to ensure a consistent API surface across transport layers.
 */
interface VelocityClientInterface
{
    /**
     * Start a new workflow execution.
     *
     * @param string $workflowType Workflow type name.
     * @param WorkflowOptions|null $options Optional workflow options.
     * @param string $input Optional input payload.
     *
     * @return WorkflowExecution Handle to the started workflow.
     * @throws \Velocity\SDK\Exceptions\VelocityException On failure.
     */
    public function startWorkflow(
        string $workflowType,
        ?WorkflowOptions $options = null,
        string $input = '',
    ): WorkflowExecution;

    /**
     * Get the current status of a workflow by key.
     *
     * @param int $workflowKey Workflow key.
     * @return string Status string (running, completed, failed, canceled, terminated).
     */
    public function getWorkflowStatus(int $workflowKey): string;

    /**
     * Signal a running workflow.
     *
     * @param int $workflowKey Workflow key.
     * @param string $signalName Signal name.
     * @param string $payload Signal payload.
     * @return bool True on success.
     */
    public function signalWorkflow(int $workflowKey, string $signalName, string $payload = ''): bool;

    /**
     * Cancel a running workflow.
     *
     * @param int $workflowKey Workflow key.
     * @return bool True on success.
     */
    public function cancelWorkflow(int $workflowKey): bool;

    /**
     * Query a workflow for its current state.
     *
     * @param int $workflowKey Workflow key.
     * @param string $queryType Query type name.
     * @return string Query result payload.
     */
    public function queryWorkflow(int $workflowKey, string $queryType): string;

    /**
     * Get the server target address.
     */
    public function getTarget(): string;

    /**
     * Close the client and release all resources.
     */
    public function close(): void;
}
