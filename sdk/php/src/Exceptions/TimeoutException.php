<?php

declare(strict_types=1);

namespace Velocity\SDK\Exceptions;

/**
 * Raised when an operation times out (error code 4).
 */
class TimeoutException extends VelocityException
{
    public function __construct(string $operation, int $timeoutMs, ?string $message = null)
    {
        $msg = $message ?? "Operation '{$operation}' timed out after {$timeoutMs}ms";
        parent::__construct($msg, errorCode: 4, retryable: true, details: [
            'operation' => $operation,
            'timeout_ms' => $timeoutMs,
        ]);
    }
}
