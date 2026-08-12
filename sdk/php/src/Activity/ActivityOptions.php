<?php

declare(strict_types=1);

namespace Velocity\SDK\Activity;

/**
 * Configuration options for activity execution.
 *
 * Controls timeouts, retry behaviour, and task queue routing for
 * individual activity invocations within a workflow.
 *
 * Usage:
 *     $opts = ActivityOptions::new()
 *         ->withStartToCloseTimeoutMs(5_000)
 *         ->withRetryMaxAttempts(3)
 *         ->withTaskQueue('activity-workers');
 */
class ActivityOptions
{
    private int $startToCloseTimeoutMs = 10_000;
    private int $scheduleToCloseTimeoutMs = 60_000;
    private int $heartbeatTimeoutMs = 0;
    private string $taskQueue = 'default';
    private int $retryMaxAttempts = 1;
    private float $retryBackoffCoefficient = 2.0;
    private int $retryInitialIntervalMs = 100;

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

    /** Set the start-to-close timeout in milliseconds. */
    public function withStartToCloseTimeoutMs(int $ms): self
    {
        $this->startToCloseTimeoutMs = max(0, $ms);
        return $this;
    }

    /** Set the schedule-to-close timeout in milliseconds. */
    public function withScheduleToCloseTimeoutMs(int $ms): self
    {
        $this->scheduleToCloseTimeoutMs = max(0, $ms);
        return $this;
    }

    /** Set the heartbeat timeout in milliseconds (0 = disabled). */
    public function withHeartbeatTimeoutMs(int $ms): self
    {
        $this->heartbeatTimeoutMs = max(0, $ms);
        return $this;
    }

    /** Set the task queue for activity dispatch. */
    public function withTaskQueue(string $taskQueue): self
    {
        $this->taskQueue = $taskQueue;
        return $this;
    }

    /** Set the maximum number of retry attempts. */
    public function withRetryMaxAttempts(int $attempts): self
    {
        $this->retryMaxAttempts = max(1, $attempts);
        return $this;
    }

    /** Set the retry backoff coefficient. */
    public function withRetryBackoffCoefficient(float $coefficient): self
    {
        $this->retryBackoffCoefficient = max(1.0, $coefficient);
        return $this;
    }

    /** Set the initial retry interval in milliseconds. */
    public function withRetryInitialIntervalMs(int $ms): self
    {
        $this->retryInitialIntervalMs = max(0, $ms);
        return $this;
    }

    // ─── Accessors ────────────────────────────────────────────────────────

    public function getStartToCloseTimeoutMs(): int { return $this->startToCloseTimeoutMs; }
    public function getScheduleToCloseTimeoutMs(): int { return $this->scheduleToCloseTimeoutMs; }
    public function getHeartbeatTimeoutMs(): int { return $this->heartbeatTimeoutMs; }
    public function getTaskQueue(): string { return $this->taskQueue; }
    public function getRetryMaxAttempts(): int { return $this->retryMaxAttempts; }
    public function getRetryBackoffCoefficient(): float { return $this->retryBackoffCoefficient; }
    public function getRetryInitialIntervalMs(): int { return $this->retryInitialIntervalMs; }

    /**
     * Convert to an associative array for serialisation.
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'start_to_close_timeout_ms' => $this->startToCloseTimeoutMs,
            'schedule_to_close_timeout_ms' => $this->scheduleToCloseTimeoutMs,
            'heartbeat_timeout_ms' => $this->heartbeatTimeoutMs,
            'task_queue' => $this->taskQueue,
            'retry_max_attempts' => $this->retryMaxAttempts,
            'retry_backoff_coefficient' => $this->retryBackoffCoefficient,
            'retry_initial_interval_ms' => $this->retryInitialIntervalMs,
        ];
    }
}
