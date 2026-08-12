<?php

declare(strict_types=1);

namespace Velocity\SDK\Exceptions;

/**
 * Raised when attempting to modify a completed workflow (error code 2).
 */
class WorkflowAlreadyCompletedException extends VelocityException
{
    public function __construct(int $workflowKey, ?string $message = null)
    {
        $msg = $message ?? "Workflow already completed: {$workflowKey}";
        parent::__construct($msg, errorCode: 2, retryable: false, details: ['workflow_key' => $workflowKey]);
    }
}
