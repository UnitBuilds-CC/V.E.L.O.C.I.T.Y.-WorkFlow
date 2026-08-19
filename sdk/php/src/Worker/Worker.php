<?php
/**
 * Worker process model for the VELOCITY-WorkFlow PHP SDK.
 *
 * The Worker polls the server for workflow and activity tasks, executes them
 * using auto-registered (or manually registered) implementations, and reports
 * results back. Supports the auto-apply attribute system for zero-config
 * workflow discovery.
 *
 * @example
 * ```php
 * // Auto-apply mode — attributes register workflows automatically
 * use Velocity\SDK\Worker\Worker;
 * use Velocity\SDK\Attributes\Workflow;
 * use Velocity\SDK\Attributes\Activity;
 *
 * #[Activity]
 * function process_payment(string $orderId): array {
 *     return ['status' => 'charged', 'order_id' => $orderId];
 * }
 *
 * #[Workflow]
 * class OrderWorkflow {
 *     public function run(WorkflowContext $ctx, string $orderId): array {
 *         return $ctx->executeActivity('process_payment', $orderId);
 *     }
 * }
 *
 * // Worker auto-discovers all #[Workflow] and #[Activity] in the directory
 * $worker = new Worker('orders', workflowsPath: __DIR__ . '/workflows');
 * $worker->run();
 * ```
 *
 * @example
 * ```php
 * // Manual registration mode
 * $worker = new Worker('orders');
 * $worker->registerWorkflow('OrderWorkflow', OrderWorkflow::class);
 * $worker->registerActivity('process_payment', 'process_payment');
 * $worker->run();
 * ```
 */

declare(strict_types=1);

namespace Velocity\SDK\Worker;

use Velocity\SDK\Client\VelocityClientInterface;
use Velocity\SDK\Client\GrpcVelocityClient;
use Velocity\SDK\AutoApply\Registry;

class Worker
{
    private string $taskQueue;
    private string $serverAddress;
    private string $namespace;
    private int $maxConcurrentWorkflowTasks;
    private int $maxConcurrentActivityTasks;
    private int $pollTimeoutMs;
    private int $heartbeatIntervalMs;
    private string $buildId;
    private ?string $workflowsPath;

    private VelocityClientInterface $client;
    private array $stats;
    private bool $running = false;
    private array $workflows = [];
    private array $activities = [];

    public function __construct(
        string $taskQueue,
        string $serverAddress = 'localhost:7234',
        string $namespace = 'default',
        ?string $workflowsPath = null,
        int $maxConcurrentWorkflowTasks = 10,
        int $maxConcurrentActivityTasks = 100,
        int $pollTimeoutMs = 10000,
        int $heartbeatIntervalMs = 30000,
        string $buildId = '1.0',
        ?VelocityClientInterface $client = null,
    ) {
        $this->taskQueue = $taskQueue;
        $this->serverAddress = $serverAddress;
        $this->namespace = $namespace;
        $this->workflowsPath = $workflowsPath;
        $this->maxConcurrentWorkflowTasks = $maxConcurrentWorkflowTasks;
        $this->maxConcurrentActivityTasks = $maxConcurrentActivityTasks;
        $this->pollTimeoutMs = $pollTimeoutMs;
        $this->heartbeatIntervalMs = $heartbeatIntervalMs;
        $this->buildId = $buildId;
        $this->client = $client ?? new GrpcVelocityClient($serverAddress);

        $this->stats = [
            'workflows_started' => 0,
            'workflows_completed' => 0,
            'workflows_failed' => 0,
            'activities_scheduled' => 0,
            'activities_completed' => 0,
            'activities_failed' => 0,
            'tasks_polled' => 0,
            'heartbeats_sent' => 0,
            'start_time' => microtime(true),
        ];
    }

    /**
     * Manually register a workflow class.
     */
    public function registerWorkflow(string $workflowType, string $className): void
    {
        $this->workflows[$workflowType] = $className;
    }

    /**
     * Manually register an activity function.
     */
    public function registerActivity(string $activityName, callable $handler): void
    {
        $this->activities[$activityName] = $handler;
    }

    /**
     * Auto-discover workflows and activities from the registry and workflows path.
     */
    private function autoDiscover(): void
    {
        // Scan directory if path provided
        if ($this->workflowsPath !== null) {
            if (is_dir($this->workflowsPath)) {
                Registry::scanDirectory($this->workflowsPath);
            } elseif (is_file($this->workflowsPath)) {
                Registry::scanFile($this->workflowsPath);
            }
        }

        // Merge auto-apply registry with manual registrations
        $autoWorkflows = Registry::getRegisteredWorkflows();
        $autoActivities = Registry::getRegisteredActivities();

        foreach ($autoWorkflows as $workflowType => $className) {
            if (!isset($this->workflows[$workflowType])) {
                $this->workflows[$workflowType] = $className;
            }
        }

        foreach ($autoActivities as $activityName => $handler) {
            if (!isset($this->activities[$activityName])) {
                $this->activities[$activityName] = $handler;
            }
        }
    }

    /**
     * Start the worker and block until shutdown.
     */
    public function run(): void
    {
        $this->autoDiscover();
        $this->running = true;

        // Install signal handlers for graceful shutdown
        if (function_exists('pcntl_signal')) {
            pcntl_signal(SIGINT, function () {
                $this->shutdown();
            });
            pcntl_signal(SIGTERM, function () {
                $this->shutdown();
            });
        }

        while ($this->running) {
            $this->stats['tasks_polled']++;

            // Poll for workflow tasks
            try {
                $task = $this->pollForTask();
                if ($task !== null) {
                    $this->executeTask($task);
                } else {
                    usleep(100000); // 100ms
                }
            } catch (\Throwable $e) {
                error_log("[velocity-worker] Poll error: " . $e->getMessage());
                sleep(1);
            }

            // Dispatch signals if pcntl is available
            if (function_exists('pcntl_signal_dispatch')) {
                pcntl_signal_dispatch();
            }
        }

        $this->client->close();
    }

    /**
     * Request graceful shutdown.
     */
    public function shutdown(): void
    {
        $this->running = false;
    }

    /**
     * Get current worker statistics.
     */
    public function getStats(): array
    {
        return array_merge($this->stats, [
            'uptime_ms' => (microtime(true) - $this->stats['start_time']) * 1000,
            'registered_workflows' => count($this->workflows),
            'registered_activities' => count($this->activities),
        ]);
    }

    /**
     * Check if the worker is running.
     */
    public function isRunning(): bool
    {
        return $this->running;
    }

    /**
     * Get the task queue name.
     */
    public function getTaskQueue(): string
    {
        return $this->taskQueue;
    }

    /**
     * Poll for a task from the server.
     */
    private function pollForTask(): ?array
    {
        // In a full implementation, this would call the server via gRPC/HTTP.
        // For now, return null (no task available).
        return null;
    }

    /**
     * Execute a workflow or activity task.
     */
    private function executeTask(array $task): void
    {
        $taskType = $task['type'] ?? 'unknown';
        $workflowType = $task['workflow_type'] ?? '';
        $activityType = $task['activity_type'] ?? '';

        if ($taskType === 'workflow' && isset($this->workflows[$workflowType])) {
            $this->executeWorkflowTask($task);
        } elseif ($taskType === 'activity' && isset($this->activities[$activityType])) {
            $this->executeActivityTask($task);
        } else {
            error_log("[velocity-worker] No handler for task type: $taskType");
        }
    }

    /**
     * Execute a workflow task.
     */
    private function executeWorkflowTask(array $task): void
    {
        $workflowType = $task['workflow_type'];
        $workflowKey = $task['workflow_key'] ?? 0;
        $workflowId = $task['workflow_id'] ?? "wf-$workflowKey";
        $input = $task['input'] ?? '{}';

        $className = $this->workflows[$workflowType];
        $this->stats['workflows_started']++;

        try {
            $instance = new $className();
            $context = new WorkflowContext(
                workflowKey: $workflowKey,
                workflowId: $workflowId,
                runId: 'run-' . intval(microtime(true) * 1000),
                workflowType: $workflowType,
                taskQueue: $this->taskQueue,
                client: $this->client,
            );

            $args = json_decode($input, true) ?? [];
            $result = method_exists($instance, 'run')
                ? $instance->run($context, ...$args)
                : $instance(...$args);

            $resultBytes = json_encode($result);
            // In production, call $this->client->completeWorkflow($workflowKey, $resultBytes);
            $this->stats['workflows_completed']++;

        } catch (\Throwable $e) {
            $this->stats['workflows_failed']++;
            error_log("[velocity-worker] Workflow '$workflowType' failed: " . $e->getMessage());
            // In production, call $this->client->failTask($workflowKey, $e->getMessage());
        }
    }

    /**
     * Execute an activity task.
     */
    private function executeActivityTask(array $task): void
    {
        $activityType = $task['activity_type'];
        $activityId = $task['activity_id'] ?? 'act-0';
        $input = $task['input'] ?? '{}';

        $handler = $this->activities[$activityType];
        $this->stats['activities_scheduled']++;

        try {
            $args = json_decode($input, true) ?? [];
            $result = is_callable($handler) ? $handler(...$args) : call_user_func($handler, ...$args);

            $resultBytes = json_encode($result);
            // In production, call $this->client->completeActivity($activityId, $resultBytes);
            $this->stats['activities_completed']++;

        } catch (\Throwable $e) {
            $this->stats['activities_failed']++;
            error_log("[velocity-worker] Activity '$activityType' failed: " . $e->getMessage());
            // In production, call $this->client->failActivity($activityId, $e->getMessage());
        }
    }
}
