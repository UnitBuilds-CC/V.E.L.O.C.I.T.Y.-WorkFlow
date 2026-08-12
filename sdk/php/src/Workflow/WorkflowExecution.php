<?php

declare(strict_types=1);

namespace Velocity\SDK\Workflow;

/**
 * Immutable handle to a running or completed workflow execution.
 *
 * Returned by {@see \Velocity\SDK\Client\VelocityClientInterface::startWorkflow()}
 * and provides access to the workflow's key, type, namespace, and current status.
 *
 * This class is intentionally read-only; workflow state transitions are
 * performed through the client that created the execution.
 */
class WorkflowExecution
{
    /**
     * @param int $key Unique workflow key assigned by the engine.
     * @param string $workflowType Workflow type name.
     * @param string $namespace Namespace the workflow runs in.
     * @param string $status Current status (running, completed, failed, etc.).
     * @param string|null $result Optional result payload (set when completed).
     * @param int $startedAt Unix timestamp (ms) when the workflow was started.
     */
    public function __construct(
        private readonly int $key,
        private readonly string $workflowType,
        private readonly string $namespace = 'default',
        private string $status = 'running',
        private readonly ?string $result = null,
        private readonly int $startedAt = 0,
    ) {
        if ($this->startedAt === 0) {
            $this->startedAt = (int)(microtime(true) * 1000);
        }
    }

    /** Get the unique workflow key. */
    public function getKey(): int
    {
        return $this->key;
    }

    /** Get the workflow type name. */
    public function getWorkflowType(): string
    {
        return $this->workflowType;
    }

    /** Get the namespace. */
    public function getNamespace(): string
    {
        return $this->namespace;
    }

    /** Get the current status. */
    public function getStatus(): string
    {
        return $this->status;
    }

    /** Update the status (used internally by the client). */
    public function setStatus(string $status): void
    {
        $this->status = $status;
    }

    /** Get the result payload, if completed. */
    public function getResult(): ?string
    {
        return $this->result;
    }

    /** Get the start timestamp in milliseconds. */
    public function getStartedAt(): int
    {
        return $this->startedAt;
    }

    /** Whether the workflow is still running. */
    public function isRunning(): bool
    {
        return $this->status === 'running';
    }

    /** Whether the workflow has reached a terminal state. */
    public function isTerminal(): bool
    {
        return in_array($this->status, ['completed', 'failed', 'canceled', 'terminated'], true);
    }

    /**
     * Create a serialisable array representation.
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'key' => $this->key,
            'workflow_type' => $this->workflowType,
            'namespace' => $this->namespace,
            'status' => $this->status,
            'result' => $this->result,
            'started_at' => $this->startedAt,
        ];
    }
}
