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

    /**
     * Poll the server for a workflow task (long-poll).
     *
     * @param string $taskQueue Task queue to poll.
     * @param string $namespace Namespace scope.
     * @param string $identity Worker identity string.
     * @param string $buildId Worker build ID for versioning.
     * @param int $timeoutMs Long-poll timeout in milliseconds.
     * @return array|null Task data array with keys: task_token, workflow_key, workflow_type,
     *                    step_index, attempt, history, workflow_id — or null if no task.
     */
    public function pollWorkflowTaskQueue(
        string $taskQueue,
        string $namespace = 'default',
        string $identity = '',
        string $buildId = '1.0',
        int $timeoutMs = 10000,
    ): ?array;

    /**
     * Report a workflow task as completed with commands.
     *
     * @param int $taskToken Opaque task token from poll.
     * @param array $commands List of Command objects (complete_workflow, fail_workflow, schedule_activity, etc.).
     * @param string $identity Worker identity string.
     * @param string $namespace Namespace scope.
     * @return bool True on success.
     */
    public function respondWorkflowTaskCompleted(
        int $taskToken,
        array $commands = [],
        string $identity = '',
        string $namespace = 'default',
    ): bool;

    /**
     * Poll the server for an activity task (long-poll).
     *
     * @param string $taskQueue Task queue to poll.
     * @param string $namespace Namespace scope.
     * @param string $identity Worker identity string.
     * @param string $buildId Worker build ID for versioning.
     * @param int $timeoutMs Long-poll timeout in milliseconds.
     * @return array|null Task data with keys: task_token, workflow_key, activity_type,
     *                    input, step_index, attempt — or null if no task.
     */
    public function pollActivityTaskQueue(
        string $taskQueue,
        string $namespace = 'default',
        string $identity = '',
        string $buildId = '1.0',
        int $timeoutMs = 10000,
    ): ?array;

    /**
     * Report an activity task as completed.
     *
     * @param int $taskToken Opaque task token from poll.
     * @param string $result Result payload bytes.
     * @param string $identity Worker identity string.
     * @param string $namespace Namespace scope.
     * @return bool True on success.
     */
    public function respondActivityTaskCompleted(
        int $taskToken,
        string $result = '',
        string $identity = '',
        string $namespace = 'default',
    ): bool;

    /**
     * Report an activity task as failed.
     *
     * @param int $taskToken Opaque task token from poll.
     * @param string $failure Failure reason/message.
     * @param string $identity Worker identity string.
     * @param string $namespace Namespace scope.
     * @return bool True on success.
     */
    public function respondActivityTaskFailed(
        int $taskToken,
        string $failure = '',
        string $identity = '',
        string $namespace = 'default',
    ): bool;
}
