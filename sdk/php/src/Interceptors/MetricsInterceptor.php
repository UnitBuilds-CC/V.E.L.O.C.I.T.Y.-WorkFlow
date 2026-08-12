<?php

declare(strict_types=1);

namespace Velocity\SDK\Interceptors;

/**
 * Tracks workflow and activity metrics (counts and timing).
 */
class MetricsInterceptor implements WorkflowInterceptorInterface
{
    private int $workflowStarts = 0;
    private int $workflowCompletions = 0;
    private int $workflowFailures = 0;
    private int $activityExecutions = 0;
    private int $activityCompletions = 0;
    private int $activityFailures = 0;

    /** @var array<int, float> Workflow key => start microtime. */
    private array $startTimes = [];

    /** @inheritDoc */
    public function onStart(string $workflowType, int $workflowKey): void
    {
        $this->workflowStarts++;
        $this->startTimes[$workflowKey] = microtime(true);
    }

    /** @inheritDoc */
    public function onComplete(int $workflowKey, string $result): void
    {
        $this->workflowCompletions++;
        unset($this->startTimes[$workflowKey]);
    }

    /** @inheritDoc */
    public function onFail(int $workflowKey, \Throwable $error): void
    {
        $this->workflowFailures++;
        unset($this->startTimes[$workflowKey]);
    }

    /** @inheritDoc */
    public function onSignal(int $workflowKey, string $signalName): void
    {
        // Signals don't affect metrics counters.
    }

    /** Track activity execution. */
    public function onActivityExecute(string $activityType, string $activityId): void
    {
        $this->activityExecutions++;
    }

    /** Track activity completion. */
    public function onActivityComplete(string $activityId, string $result): void
    {
        $this->activityCompletions++;
    }

    /** Track activity failure. */
    public function onActivityFail(string $activityId, \Throwable $error): void
    {
        $this->activityFailures++;
    }

    /**
     * Return a snapshot of current metrics.
     *
     * @return array<string, int>
     */
    public function getMetrics(): array
    {
        return [
            'workflow_starts' => $this->workflowStarts,
            'workflow_completions' => $this->workflowCompletions,
            'workflow_failures' => $this->workflowFailures,
            'activity_executions' => $this->activityExecutions,
            'activity_completions' => $this->activityCompletions,
            'activity_failures' => $this->activityFailures,
        ];
    }
}
