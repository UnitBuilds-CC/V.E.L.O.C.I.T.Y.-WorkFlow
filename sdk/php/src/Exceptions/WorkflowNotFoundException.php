<?php

declare(strict_types=1);

namespace Velocity\SDK\Exceptions;

/**
 * Raised when a workflow does not exist (error code 1).
 */
class WorkflowNotFoundException extends VelocityException
{
    public function __construct(int $workflowKey, ?string $message = null)
    {
        $msg = $message ?? "Workflow not found: {$workflowKey}";
        parent::__construct($msg, errorCode: 1, retryable: false, details: ['workflow_key' => $workflowKey]);
    }
}
