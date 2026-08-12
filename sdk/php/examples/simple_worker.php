<?php
/**
 * Example: Simple task worker using the VELOCITY-WorkFlow PHP SDK.
 *
 * Demonstrates:
 *   - Worker registration with a task queue
 *   - Polling for tasks in a loop
 *   - Executing task logic via registered handlers
 *   - Error handling
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *
 *   2. Install dependencies:
 *      cd VELOCITY-WorkFlow/sdk/php && composer install
 *
 *   3. Run this worker:
 *      php examples/simple_worker.php
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Velocity\SDK\VelocityClient;
use Velocity\SDK\Exceptions\VelocityException;

// ── Configuration ────────────────────────────────────────────────────────

const SERVER_ADDR  = 'localhost:50051';
const TASK_QUEUE   = 'orders';
const POLL_INTERVAL_US = 1_000_000; // 1 second in microseconds

// ── Graceful shutdown ────────────────────────────────────────────────────

$shutdownRequested = false;

if (function_exists('pcntl_signal')) {
    pcntl_signal(SIGINT, function () use (&$shutdownRequested) {
        echo "[worker] Received SIGINT — shutting down gracefully...\n";
        $shutdownRequested = true;
    });
    pcntl_signal(SIGTERM, function () use (&$shutdownRequested) {
        echo "[worker] Received SIGTERM — shutting down gracefully...\n";
        $shutdownRequested = true;
    });
}

// ── Task handlers ────────────────────────────────────────────────────────

function processOrder(array $task): array
{
    $input = json_decode($task['input'] ?? '{}', true);
    $orderId = $input['order_id'] ?? 'unknown';
    echo "[worker] Processing order {$orderId}\n";
    // Simulate work
    usleep(50_000);
    return ['status' => 'shipped', 'order_id' => $orderId];
}

$handlers = [
    'order-processing' => 'processOrder',
];

// ── Worker loop ──────────────────────────────────────────────────────────

echo "[worker] Starting VELOCITY-WorkFlow PHP worker\n";
echo "[worker] Server: " . SERVER_ADDR . " | Queue: " . TASK_QUEUE . "\n";

$client = new VelocityClient(SERVER_ADDR);

try {
    echo "[worker] Registered on task queue '" . TASK_QUEUE . "'\n";
    echo "[worker] Polling for tasks... (Ctrl+C to stop)\n";

    while (!$shutdownRequested) {
        // Dispatch pending signals
        if (function_exists('pcntl_signal_dispatch')) {
            pcntl_signal_dispatch();
        }

        try {
            // Poll for a task from the server
            $task = $client->pollTask(TASK_QUEUE, timeoutMs: 2000);

            if ($task === null) {
                usleep(POLL_INTERVAL_US);
                continue;
            }

            $taskType = $task['workflow_type'] ?? 'unknown';
            $handler = $handlers[$taskType] ?? null;

            if ($handler === null) {
                echo "[worker] No handler for task type '{$taskType}' — skipping\n";
                $client->failTask($task['workflow_key'], "No handler for {$taskType}");
                continue;
            }

            // Execute the task
            $result = $handler($task);
            $client->completeWorkflow(
                $task['workflow_key'],
                json_encode($result)
            );
            echo "[worker] Task '{$taskType}' completed successfully\n";

        } catch (VelocityException $e) {
            echo "[worker] Velocity error: " . $e->getMessage() . "\n";
            usleep(POLL_INTERVAL_US);

        } catch (\Throwable $e) {
            echo "[worker] Unexpected error: " . $e->getMessage() . "\n";
            usleep(POLL_INTERVAL_US);
        }
    }
} finally {
    $client->close();
    echo "[worker] Shut down cleanly\n";
}
