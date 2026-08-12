<?php

declare(strict_types=1);

namespace Velocity\SDK\Exceptions;

/**
 * Raised when connection to the server fails (error code 3).
 */
class ConnectionException extends VelocityException
{
    public function __construct(string $target, ?string $message = null)
    {
        $msg = $message ?? "Failed to connect to {$target}";
        parent::__construct($msg, errorCode: 3, retryable: true, details: ['target' => $target]);
    }
}
