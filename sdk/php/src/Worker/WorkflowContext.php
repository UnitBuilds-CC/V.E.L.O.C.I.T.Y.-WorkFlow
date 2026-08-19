<?php
/**
 * Context available inside workflow functions.
 *
 * Provides deterministic operations for scheduling activities, timers,
 * signals, queries, updates, and child workflows.
 */

declare(strict_types=1);

namespace Velocity\SDK\Worker;

use Velocity\SDK\Client\VelocityClientInterface;
use Velocity\SDK\AutoApply\Registry;

class WorkflowContext
{
    private int $workflowKey;
    private string $workflowId;
    private string $runId;
    private string $workflowType;
    private string $taskQueue;
    private ?VelocityClientInterface $client;
    private int $currentStep = 0;
    private array $signalHandlers = [];
    private array $queryHandlers = [];
    private array $updateHandlers = [];
    private array $pendingSignals = [];

    public function __construct(
        int $workflowKey,
        string $workflowId,
        string $runId,
        string $workflowType,
        string $taskQueue,
        ?VelocityClientInterface $client = null,
    ) {
        $this->workflowKey = $workflowKey;
        $this->workflowId = $workflowId;
        $this->runId = $runId;
        $this->workflowType = $workflowType;
        $this->taskQueue = $taskQueue;
        $this->client = $client;
    }

    /**
     * Schedule an activity for execution.
     */
    public function executeActivity(string $activityName, mixed ...$args): mixed
    {
        $this->currentStep++;

        // In a full implementation, this would send a command to the server.
        // For embedded/local mode, call the registered activity directly.
        $activities = Registry::getRegisteredActivities();
        $handler = $activities[$activityName] ?? null;

        if ($handler === null) {
            throw new \RuntimeException("No activity registered for '$activityName'");
        }

        return is_callable($handler) ? $handler(...$args) : call_user_func($handler, ...$args);
    }

    /**
     * Deterministic timer.
     */
    public function sleep(int $durationMs): void
    {
        $this->currentStep++;
        usleep($durationMs * 1000);
    }

    /**
     * Register a signal handler.
     */
    public function onSignal(string $signalName, callable $handler): void
    {
        $this->signalHandlers[$signalName] = $handler;
    }

    /**
     * Register a query handler.
     */
    public function onQuery(string $queryName, callable $handler): void
    {
        $this->queryHandlers[$queryName] = $handler;
    }

    /**
     * Register an update handler.
     */
    public function onUpdate(string $updateName, callable $handler): void
    {
        $this->updateHandlers[$updateName] = $handler;
    }

    /**
     * Block until a signal is received.
     */
    public function waitForSignal(string $signalName): mixed
    {
        if (isset($this->pendingSignals[$signalName])) {
            return array_shift($this->pendingSignals[$signalName]);
        }

        // In production, this suspends the workflow until the signal arrives.
        throw new \RuntimeException("Waiting for signal '$signalName' — not yet buffered");
    }

    /**
     * Start a child workflow.
     */
    public function startChildWorkflow(string $workflowType, mixed ...$args): mixed
    {
        $this->currentStep++;
        throw new \RuntimeException("Child workflows require server-side support");
    }

    /**
     * Get the current step number.
     */
    public function getCurrentStep(): int
    {
        return $this->currentStep;
    }

    /**
     * Get the workflow key.
     */
    public function getWorkflowKey(): int
    {
        return $this->workflowKey;
    }

    /**
     * Get the workflow ID.
     */
    public function getWorkflowId(): string
    {
        return $this->workflowId;
    }

    /**
     * Get the run ID.
     */
    public function getRunId(): string
    {
        return $this->runId;
    }

    /**
     * Get the workflow type.
     */
    public function getWorkflowType(): string
    {
        return $this->workflowType;
    }

    /**
     * Get the task queue.
     */
    public function getTaskQueue(): string
    {
        return $this->taskQueue;
    }
}
