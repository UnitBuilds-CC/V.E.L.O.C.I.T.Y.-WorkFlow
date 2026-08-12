<?php

declare(strict_types=1);

namespace Velocity\SDK\Testing;

use Velocity\SDK\Exceptions\WorkflowNotFoundException;
use Velocity\SDK\Exceptions\WorkflowAlreadyCompletedException;

/**
 * Mock workflow environment for unit testing without a running server.
 *
 * Provides assertion helpers and time-skip support for deterministic tests.
 */
class TestWorkflowEnvironment
{
    private MockVelocityClient $client;
    private int $timeOffsetSecs = 0;

    public function __construct()
    {
        $this->client = new MockVelocityClient();
    }

    /** Get the underlying mock client. */
    public function getClient(): MockVelocityClient
    {
        return $this->client;
    }

    /**
     * Start a workflow in the test environment.
     *
     * @return int Workflow key.
     */
    public function startWorkflow(
        string $workflowType,
        string $namespace = 'default',
        string $taskQueue = 'default',
        int $totalSteps = 1,
    ): int {
        return $this->client->startWorkflow($workflowType, $namespace, $taskQueue, $totalSteps);
    }

    /** Complete a workflow in the test environment. */
    public function completeWorkflow(int $workflowKey, string $result = ''): bool
    {
        return $this->client->completeWorkflow($workflowKey, $result);
    }

    /** Signal a workflow in the test environment. */
    public function signalWorkflow(int $workflowKey, string $signalName, string $payload = ''): bool
    {
        return $this->client->signalWorkflow($workflowKey, $signalName, $payload);
    }

    /** Advance the simulated clock. */
    public function timeSkip(int $seconds): void
    {
        $this->timeOffsetSecs += $seconds;
    }

    /** Get the current simulated UNIX timestamp. */
    public function getCurrentTime(): int
    {
        return time() + $this->timeOffsetSecs;
    }

    /**
     * Assert that a workflow has completed.
     *
     * @throws \RuntimeException If the workflow is not completed.
     */
    public function assertWorkflowCompleted(int $workflowKey): void
    {
        $desc = $this->client->describeWorkflow($workflowKey);
        if ($desc['status'] !== 'completed') {
            throw new \RuntimeException(
                "Expected workflow {$workflowKey} to be completed, but status is {$desc['status']}"
            );
        }
    }

    /**
     * Assert that a workflow received a specific signal.
     *
     * @throws \RuntimeException If the signal was not received.
     */
    public function assertSignalReceived(int $workflowKey, string $signalName): void
    {
        $signals = $this->client->getSignals($workflowKey);
        $names = array_column($signals, 'signal_name');
        if (!in_array($signalName, $names, true)) {
            throw new \RuntimeException(
                "Expected signal '{$signalName}' not found for workflow {$workflowKey}. Received: " . implode(', ', $names)
            );
        }
    }

    /** Reset the test environment to a clean state. */
    public function reset(): void
    {
        $this->client = new MockVelocityClient();
        $this->timeOffsetSecs = 0;
    }
}
