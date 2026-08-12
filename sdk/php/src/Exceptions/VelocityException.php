<?php

declare(strict_types=1);

namespace Velocity\SDK\Exceptions;

use RuntimeException;

/**
 * Base exception for all VELOCITY-WorkFlow SDK errors.
 *
 * Error codes are consistent across all SDKs (Python, Go, TypeScript, Java, Rust, Ruby).
 */
class VelocityException extends RuntimeException
{
    /** @var int Numeric error code matching other SDKs. */
    protected int $errorCode;

    /** @var bool Whether the operation can be retried. */
    protected bool $retryable;

    /** @var array<string, mixed> Optional structured details. */
    protected array $details;

    /**
     * @param string $message Human-readable error message.
     * @param int $errorCode Numeric error code.
     * @param bool $retryable Whether the operation is retryable.
     * @param array<string, mixed> $details Additional context.
     */
    public function __construct(
        string $message,
        int $errorCode = 0,
        bool $retryable = false,
        array $details = [],
    ) {
        parent::__construct($message);
        $this->errorCode = $errorCode;
        $this->retryable = $retryable;
        $this->details = $details;
    }

    /** Get the numeric error code. */
    public function getErrorCode(): int
    {
        return $this->errorCode;
    }

    /** Whether this error is retryable. */
    public function isRetryable(): bool
    {
        return $this->retryable;
    }

    /** Get structured detail values. */
    public function getDetails(): array
    {
        return $this->details;
    }

    /** @return string */
    public function __toString(): string
    {
        $retry = $this->retryable ? ' (retryable)' : '';
        return "VelocityException[{$this->errorCode}]: {$this->message}{$retry}";
    }
}
