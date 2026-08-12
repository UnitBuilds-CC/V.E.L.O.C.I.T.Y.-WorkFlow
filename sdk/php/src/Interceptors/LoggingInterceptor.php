<?php

declare(strict_types=1);

namespace Velocity\SDK\Interceptors;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Logs workflow lifecycle events using a PSR-3 logger.
 */
class LoggingInterceptor implements WorkflowInterceptorInterface
{
    private LoggerInterface $logger;
    private string $prefix;

    /**
     * @param LoggerInterface|null $logger PSR-3 logger; defaults to NullLogger.
     * @param string $prefix Log message prefix.
     */
    public function __construct(?LoggerInterface $logger = null, string $prefix = '[VELOCITY]')
    {
        $this->logger = $logger ?? new NullLogger();
        $this->prefix = $prefix;
    }

    /** @inheritDoc */
    public function onStart(string $workflowType, int $workflowKey): void
    {
        $this->logger->info("{$this->prefix} Workflow started: type={$workflowType}, key={$workflowKey}");
    }

    /** @inheritDoc */
    public function onComplete(int $workflowKey, string $result): void
    {
        $this->logger->info("{$this->prefix} Workflow completed: key={$workflowKey}");
    }

    /** @inheritDoc */
    public function onFail(int $workflowKey, \Throwable $error): void
    {
        $this->logger->error("{$this->prefix} Workflow failed: key={$workflowKey}, error={$error->getMessage()}");
    }

    /** @inheritDoc */
    public function onSignal(int $workflowKey, string $signalName): void
    {
        $this->logger->info("{$this->prefix} Workflow signal: key={$workflowKey}, signal={$signalName}");
    }
}
