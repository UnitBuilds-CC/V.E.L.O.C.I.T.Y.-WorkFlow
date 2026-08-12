<?php

declare(strict_types=1);

namespace Velocity\SDK\Interceptors;

/**
 * Chain of responsibility for workflow interceptors.
 *
 * Interceptors are invoked in the order they were added.
 */
class InterceptorChain
{
    /** @var WorkflowInterceptorInterface[] */
    private array $interceptors = [];

    /**
     * Add an interceptor to the chain.
     *
     * @param WorkflowInterceptorInterface $interceptor
     * @return self Fluent interface.
     */
    public function add(WorkflowInterceptorInterface $interceptor): self
    {
        $this->interceptors[] = $interceptor;
        return $this;
    }

    /** Get the number of interceptors in the chain. */
    public function count(): int
    {
        return count($this->interceptors);
    }

    /** Invoke onStart for all interceptors. */
    public function invokeStart(string $workflowType, int $workflowKey): void
    {
        foreach ($this->interceptors as $interceptor) {
            $interceptor->onStart($workflowType, $workflowKey);
        }
    }

    /** Invoke onComplete for all interceptors. */
    public function invokeComplete(int $workflowKey, string $result): void
    {
        foreach ($this->interceptors as $interceptor) {
            $interceptor->onComplete($workflowKey, $result);
        }
    }

    /** Invoke onFail for all interceptors. */
    public function invokeFail(int $workflowKey, \Throwable $error): void
    {
        foreach ($this->interceptors as $interceptor) {
            $interceptor->onFail($workflowKey, $error);
        }
    }

    /** Invoke onSignal for all interceptors. */
    public function invokeSignal(int $workflowKey, string $signalName): void
    {
        foreach ($this->interceptors as $interceptor) {
            $interceptor->onSignal($workflowKey, $signalName);
        }
    }
}
