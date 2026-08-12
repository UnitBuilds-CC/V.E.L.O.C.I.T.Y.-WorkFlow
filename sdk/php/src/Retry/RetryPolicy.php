<?php

declare(strict_types=1);

namespace Velocity\SDK\Retry;

use InvalidArgumentException;

/**
 * Retry policy with exponential backoff and jitter.
 *
 * Usage:
 *     $policy = RetryPolicy::builder()
 *         ->maxAttempts(5)
 *         ->initialIntervalMs(100)
 *         ->backoffCoefficient(2.0)
 *         ->maxIntervalMs(10000)
 *         ->jitter(true)
 *         ->build();
 *
 *     $result = $policy->execute(function() {
 *         return fetchRemoteData();
 *     });
 */
class RetryPolicy
{
    private int $maxAttempts;
    private int $initialIntervalMs;
    private float $backoffCoefficient;
    private int $maxIntervalMs;
    private bool $jitter;
    /** @var array<class-string<\Throwable>> */
    private array $retryableExceptions;

    /**
     * @param array<class-string<\Throwable>> $retryableExceptions
     */
    public function __construct(
        int $maxAttempts = 3,
        int $initialIntervalMs = 100,
        float $backoffCoefficient = 2.0,
        int $maxIntervalMs = 60000,
        bool $jitter = true,
        array $retryableExceptions = []
    ) {
        $this->maxAttempts = $maxAttempts;
        $this->initialIntervalMs = $initialIntervalMs;
        $this->backoffCoefficient = $backoffCoefficient;
        $this->maxIntervalMs = $maxIntervalMs;
        $this->jitter = $jitter;
        $this->retryableExceptions = $retryableExceptions;
        $this->validate();
    }

    /** Create a default retry policy. */
    public static function defaults(): self
    {
        return new self();
    }

    /** Create a new builder. */
    public static function builder(): RetryPolicyBuilder
    {
        return new RetryPolicyBuilder();
    }

    /**
     * Execute a callable with retry logic.
     *
     * @template T
     * @param callable(): T $callable
     * @return T
     * @throws \Throwable
     */
    public function execute(callable $callable): mixed
    {
        $this->validate();

        $lastException = null;

        for ($attempt = 0; $attempt < $this->maxAttempts; $attempt++) {
            try {
                return $callable();
            } catch (\Throwable $e) {
                $lastException = $e;

                if (!$this->isRetryable($e)) {
                    throw $e;
                }

                if ($attempt < $this->maxAttempts - 1) {
                    $backoff = $this->calculateBackoff($attempt);
                    usleep((int) ($backoff * 1000));
                }
            }
        }

        throw $lastException;
    }

    /** Calculate backoff duration in milliseconds for a given attempt. */
    public function calculateBackoff(int $attempt): float
    {
        $interval = $this->initialIntervalMs * ($this->backoffCoefficient ** $attempt);
        $interval = min($interval, $this->maxIntervalMs);

        if ($this->jitter) {
            $interval = mt_rand(0, (int) $interval);
        }

        return $interval;
    }

    /** Check if an exception is retryable. */
    public function isRetryable(\Throwable $e): bool
    {
        if (empty($this->retryableExceptions)) {
            return true; // retry all by default
        }

        foreach ($this->retryableExceptions as $class) {
            if ($e instanceof $class) {
                return true;
            }
        }

        return false;
    }

    private function validate(): void
    {
        if ($this->maxAttempts < 1) {
            throw new InvalidArgumentException('maxAttempts must be >= 1');
        }
        if ($this->initialIntervalMs <= 0) {
            throw new InvalidArgumentException('initialIntervalMs must be > 0');
        }
        if ($this->backoffCoefficient < 1.0) {
            throw new InvalidArgumentException('backoffCoefficient must be >= 1.0');
        }
        if ($this->maxIntervalMs < $this->initialIntervalMs) {
            throw new InvalidArgumentException('maxIntervalMs must be >= initialIntervalMs');
        }
    }

    // ─── Getters ───────────────────────────────────────────────────────────────

    public function getMaxAttempts(): int { return $this->maxAttempts; }
    public function getInitialIntervalMs(): int { return $this->initialIntervalMs; }
    public function getBackoffCoefficient(): float { return $this->backoffCoefficient; }
    public function getMaxIntervalMs(): int { return $this->maxIntervalMs; }
    public function isJitterEnabled(): bool { return $this->jitter; }
}

/**
 * Builder for RetryPolicy.
 */
class RetryPolicyBuilder
{
    private int $maxAttempts = 3;
    private int $initialIntervalMs = 100;
    private float $backoffCoefficient = 2.0;
    private int $maxIntervalMs = 60000;
    private bool $jitter = true;
    /** @var array<class-string<\Throwable>> */
    private array $retryableExceptions = [];

    public function maxAttempts(int $maxAttempts): self
    {
        $this->maxAttempts = $maxAttempts;
        return $this;
    }

    public function initialIntervalMs(int $ms): self
    {
        $this->initialIntervalMs = $ms;
        return $this;
    }

    public function backoffCoefficient(float $coeff): self
    {
        $this->backoffCoefficient = $coeff;
        return $this;
    }

    public function maxIntervalMs(int $ms): self
    {
        $this->maxIntervalMs = $ms;
        return $this;
    }

    public function jitter(bool $enabled): self
    {
        $this->jitter = $enabled;
        return $this;
    }

    /**
     * @param class-string<\Throwable> $exceptionClass
     */
    public function addRetryableException(string $exceptionClass): self
    {
        $this->retryableExceptions[] = $exceptionClass;
        return $this;
    }

    public function build(): RetryPolicy
    {
        return new RetryPolicy(
            $this->maxAttempts,
            $this->initialIntervalMs,
            $this->backoffCoefficient,
            $this->maxIntervalMs,
            $this->jitter,
            $this->retryableExceptions,
        );
    }
}
