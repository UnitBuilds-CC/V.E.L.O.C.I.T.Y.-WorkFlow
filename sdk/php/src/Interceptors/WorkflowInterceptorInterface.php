<?php

declare(strict_types=1);

namespace Velocity\SDK\Interceptors;

/**
 * Interface for workflow lifecycle interceptors.
 *
 * Implement this interface to hook into workflow start, complete, fail, and signal events.
 */
interface WorkflowInterceptorInterface
{
    /**
     * Called before a workflow starts.
     *
     * @param string $workflowType Workflow type name.
     * @param int $workflowKey Engine-assigned workflow key.
     */
    public function onStart(string $workflowType, int $workflowKey): void;

    /**
     * Called after a workflow completes successfully.
     *
     * @param int $workflowKey Workflow key.
     * @param string $result Result payload.
     */
    public function onComplete(int $workflowKey, string $result): void;

    /**
     * Called when a workflow fails.
     *
     * @param int $workflowKey Workflow key.
     * @param \Throwable $error The error that caused the failure.
     */
    public function onFail(int $workflowKey, \Throwable $error): void;

    /**
     * Called when a workflow receives a signal.
     *
     * @param int $workflowKey Workflow key.
     * @param string $signalName Name of the signal.
     */
    public function onSignal(int $workflowKey, string $signalName): void;
}
