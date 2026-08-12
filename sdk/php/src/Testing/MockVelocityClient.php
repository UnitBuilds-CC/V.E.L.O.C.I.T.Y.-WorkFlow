<?php

declare(strict_types=1);

namespace Velocity\SDK\Testing;

use Velocity\SDK\Exceptions\WorkflowNotFoundException;
use Velocity\SDK\Exceptions\WorkflowAlreadyCompletedException;

/**
 * In-memory mock client that mirrors the VelocityClient API surface.
 *
 * Useful for unit tests that need to verify workflow interactions without
 * depending on the real engine or gRPC server.
 */
class MockVelocityClient
{
    /** @var array<int, array<string, mixed>> */
    private array $workflows = [];

    /** @var array<int, array<int, array{signal_name: string, payload: string}>> */
    private array $signals = [];

    private int $nextKey = 1;

    /**
     * Start a mock workflow.
     *
     * @return int Workflow key.
     */
    public function startWorkflow(
        string $workflowType,
        string $namespace = 'default',
        string $taskQueue = 'default',
        int $totalSteps = 1,
    ): int {
        $key = $this->nextKey++;
        $this->workflows[$key] = [
            'workflow_type' => $workflowType,
            'namespace' => $namespace,
            'task_queue' => $taskQueue,
            'total_steps' => $totalSteps,
            'current_step' => 0,
            'status' => 'running',
            'result' => null,
        ];
        $this->signals[$key] = [];
        return $key;
    }

    /**
     * Describe a mock workflow.
     *
     * @return array<string, mixed>
     * @throws WorkflowNotFoundException
     */
    public function describeWorkflow(int $workflowKey): array
    {
        if (!isset($this->workflows[$workflowKey])) {
            throw new WorkflowNotFoundException($workflowKey);
        }
        $wf = $this->workflows[$workflowKey];
        return [
            'workflow_key' => $workflowKey,
            'status' => $wf['status'],
            'current_step' => $wf['current_step'],
            'total_steps' => $wf['total_steps'],
            'namespace' => $wf['namespace'],
            'result' => $wf['result'],
        ];
    }

    /**
     * Signal a mock workflow.
     *
     * @throws WorkflowNotFoundException
     */
    public function signalWorkflow(int $workflowKey, string $signalName, string $payload = ''): bool
    {
        if (!isset($this->workflows[$workflowKey])) {
            throw new WorkflowNotFoundException($workflowKey);
        }
        $this->signals[$workflowKey][] = [
            'signal_name' => $signalName,
            'payload' => $payload,
        ];
        return true;
    }

    /**
     * Complete a mock workflow.
     *
     * @throws WorkflowNotFoundException
     * @throws WorkflowAlreadyCompletedException
     */
    public function completeWorkflow(int $workflowKey, string $result = ''): bool
    {
        if (!isset($this->workflows[$workflowKey])) {
            throw new WorkflowNotFoundException($workflowKey);
        }
        if ($this->workflows[$workflowKey]['status'] !== 'running') {
            throw new WorkflowAlreadyCompletedException($workflowKey);
        }
        $this->workflows[$workflowKey]['status'] = 'completed';
        $this->workflows[$workflowKey]['result'] = $result;
        return true;
    }

    /**
     * Fail a mock workflow.
     *
     * @throws WorkflowNotFoundException
     * @throws WorkflowAlreadyCompletedException
     */
    public function failWorkflow(int $workflowKey, string $reason = ''): bool
    {
        if (!isset($this->workflows[$workflowKey])) {
            throw new WorkflowNotFoundException($workflowKey);
        }
        if ($this->workflows[$workflowKey]['status'] !== 'running') {
            throw new WorkflowAlreadyCompletedException($workflowKey);
        }
        $this->workflows[$workflowKey]['status'] = 'failed';
        return true;
    }

    /**
     * Cancel a mock workflow.
     *
     * @throws WorkflowNotFoundException
     */
    public function cancelWorkflow(int $workflowKey): bool
    {
        if (!isset($this->workflows[$workflowKey])) {
            throw new WorkflowNotFoundException($workflowKey);
        }
        $this->workflows[$workflowKey]['status'] = 'canceled';
        return true;
    }

    /**
     * Get all signals received by a workflow.
     *
     * @return array<int, array{signal_name: string, payload: string}>
     */
    public function getSignals(int $workflowKey): array
    {
        return $this->signals[$workflowKey] ?? [];
    }

    /** Close the mock client (no-op). */
    public function close(): void
    {
        // No-op for mock client.
    }
}
