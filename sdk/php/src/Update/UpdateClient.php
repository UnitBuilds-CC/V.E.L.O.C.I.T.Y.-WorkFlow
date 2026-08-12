<?php
/**
 * Workflow Update API — synchronous workflow mutation.
 *
 * Unlike signals (fire-and-forget), updates provide:
 * - Synchronous request/response semantics
 * - Wait policies (Accepted, Completed, Admitted)
 * - Validation before execution
 * - Named update handlers registered by workflows
 *
 * Usage:
 *   $client = new \Velocity\SDK\Update\UpdateClient('localhost:50051');
 *   $client->registerHandler('setAmount', fn($args) => $args);
 *   $result = $client->executeUpdate(42, 'setAmount', ['amount' => 100]);
 */

namespace Velocity\SDK\Update;

enum UpdateStatus: int
{
    case Admitted = 0;
    case Accepted = 1;
    case Completed = 2;
    case Rejected = 3;
}

enum UpdateWaitPolicy: int
{
    case Admitted = 0;
    case Accepted = 1;
    case Completed = 2;
}

class UpdateRequest
{
    public function __construct(
        public readonly int $workflowKey,
        public readonly string $updateId,
        public readonly string $updateName,
        public readonly mixed $args = null,
        public readonly UpdateWaitPolicy $waitPolicy = UpdateWaitPolicy::Completed,
    ) {}
}

class UpdateResult
{
    public function __construct(
        public readonly string $updateId,
        public readonly UpdateStatus $status,
        public readonly mixed $result = null,
        public readonly ?string $error = null,
        public readonly float $durationMs = 0.0,
    ) {}
}

class UpdateHandler
{
    public function __construct(
        public readonly string $name,
        public readonly \Closure $handler,
        public readonly ?\Closure $validator = null,
    ) {}
}

class UpdateClient
{
    private string $serverAddress;
    /** @var array<string, UpdateHandler> */
    private array $handlers = [];
    /** @var array<string, UpdateResult> */
    private array $pending = [];

    public function __construct(string $serverAddress = 'localhost:50051')
    {
        $this->serverAddress = $serverAddress;
    }

    /**
     * Register a named update handler.
     */
    public function registerHandler(
        string $name,
        \Closure $handler,
        ?\Closure $validator = null,
    ): void {
        $this->handlers[$name] = new UpdateHandler($name, $handler, $validator);
    }

    /**
     * Execute a workflow update.
     */
    public function executeUpdate(
        int $workflowKey,
        string $updateName,
        mixed $args = null,
        UpdateWaitPolicy $waitPolicy = UpdateWaitPolicy::Completed,
        ?string $updateId = null,
    ): UpdateResult {
        $uid = $updateId ?? "update-{$workflowKey}-" . (int)(microtime(true) * 1000);
        $start = microtime(true);

        $handler = $this->handlers[$updateName] ?? null;
        if ($handler === null) {
            $result = new UpdateResult(
                updateId: $uid,
                status: UpdateStatus::Rejected,
                error: "No handler registered for update '{$updateName}'",
                durationMs: (microtime(true) - $start) * 1000,
            );
            $this->pending[$uid] = $result;
            return $result;
        }

        if ($handler->validator !== null && !($handler->validator)($args)) {
            $result = new UpdateResult(
                updateId: $uid,
                status: UpdateStatus::Rejected,
                error: 'Update validation failed',
                durationMs: (microtime(true) - $start) * 1000,
            );
            $this->pending[$uid] = $result;
            return $result;
        }

        try {
            $value = ($handler->handler)($args);
            $result = new UpdateResult(
                updateId: $uid,
                status: UpdateStatus::Completed,
                result: $value,
                durationMs: (microtime(true) - $start) * 1000,
            );
        } catch (\Throwable $e) {
            $result = new UpdateResult(
                updateId: $uid,
                status: UpdateStatus::Rejected,
                error: $e->getMessage(),
                durationMs: (microtime(true) - $start) * 1000,
            );
        }

        $this->pending[$uid] = $result;
        return $result;
    }

    /** Get the result of a previously executed update. */
    public function getUpdateResult(string $updateId): ?UpdateResult
    {
        return $this->pending[$updateId] ?? null;
    }

    /** List registered update handler names. */
    public function listHandlers(): array
    {
        return array_keys($this->handlers);
    }

    /** List pending update IDs. */
    public function listPending(): array
    {
        return array_keys($this->pending);
    }
}
