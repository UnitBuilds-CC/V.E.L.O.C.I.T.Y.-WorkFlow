<?php

declare(strict_types=1);

namespace Velocity\SDK\Workflow;

/**
 * Builder for workflow execution options.
 *
 * Provides a fluent interface for configuring workflow parameters such as
 * namespace, task queue, timeouts, retry policy, and execution steps.
 *
 * Usage:
 *     $options = WorkflowOptions::new()
 *         ->withNamespace('production')
 *         ->withTaskQueue('high-priority')
 *         ->withTotalSteps(10)
 *         ->withExecutionTimeoutMs(30_000)
 *         ->withRetryPolicy(['max_attempts' => 3]);
 */
class WorkflowOptions
{
    private string $namespace = 'default';
    private string $taskQueue = 'default';
    private int $totalSteps = 1;
    private int $executionTimeoutMs = 60_000;
    private array $retryPolicy = [];
    private string $workflowId = '';
    private array $searchAttributes = [];
    private ?string $memo = null;

    /** Use the named constructor {@see defaults()} or {@see new()}. */
    private function __construct()
    {
    }

    /** Create a new options builder. */
    public static function new(): self
    {
        return new self();
    }

    /** Create options with all defaults. */
    public static function defaults(): self
    {
        return new self();
    }

    /** Set the namespace. */
    public function withNamespace(string $namespace): self
    {
        $this->namespace = $namespace;
        return $this;
    }

    /** Set the task queue. */
    public function withTaskQueue(string $taskQueue): self
    {
        $this->taskQueue = $taskQueue;
        return $this;
    }

    /** Set the total number of execution steps. */
    public function withTotalSteps(int $totalSteps): self
    {
        $this->totalSteps = max(1, $totalSteps);
        return $this;
    }

    /** Set the execution timeout in milliseconds. */
    public function withExecutionTimeoutMs(int $timeoutMs): self
    {
        $this->executionTimeoutMs = max(0, $timeoutMs);
        return $this;
    }

    /** Set the retry policy as an associative array. */
    public function withRetryPolicy(array $policy): self
    {
        $this->retryPolicy = $policy;
        return $this;
    }

    /** Set an explicit workflow ID (server assigns one if empty). */
    public function withWorkflowId(string $workflowId): self
    {
        $this->workflowId = $workflowId;
        return $this;
    }

    /** Set search attributes for visibility queries. */
    public function withSearchAttributes(array $attributes): self
    {
        $this->searchAttributes = $attributes;
        return $this;
    }

    /** Set a memo attached to the workflow. */
    public function withMemo(string $memo): self
    {
        $this->memo = $memo;
        return $this;
    }

    // ─── Accessors ────────────────────────────────────────────────────────

    public function getNamespace(): string { return $this->namespace; }
    public function getTaskQueue(): string { return $this->taskQueue; }
    public function getTotalSteps(): int { return $this->totalSteps; }
    public function getExecutionTimeoutMs(): int { return $this->executionTimeoutMs; }
    public function getRetryPolicy(): array { return $this->retryPolicy; }
    public function getWorkflowId(): string { return $this->workflowId; }
    public function getSearchAttributes(): array { return $this->searchAttributes; }
    public function getMemo(): ?string { return $this->memo; }

    /**
     * Convert to an associative array for serialisation.
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'namespace' => $this->namespace,
            'task_queue' => $this->taskQueue,
            'total_steps' => $this->totalSteps,
            'execution_timeout_ms' => $this->executionTimeoutMs,
            'retry_policy' => $this->retryPolicy,
            'workflow_id' => $this->workflowId,
            'search_attributes' => $this->searchAttributes,
            'memo' => $this->memo,
        ];
    }
}
