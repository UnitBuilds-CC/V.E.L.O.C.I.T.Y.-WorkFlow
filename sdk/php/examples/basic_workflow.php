<?php
/**
 * Example: Basic workflow with signal and query using the VELOCITY-WorkFlow PHP SDK.
 *
 * Demonstrates:
 *   - Starting a workflow
 *   - Sending signals
 *   - Querying workflow state
 *   - Completing the workflow
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *   2. Install dependencies:
 *      cd VELOCITY-WorkFlow/sdk/php && composer install
 *   3. Run this example:
 *      php examples/basic_workflow.php
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Velocity\SDK\VelocityClient;

echo "=== VELOCITY-WorkFlow PHP SDK — Basic Workflow ===\n\n";

$client = new VelocityClient('localhost:7234');

try {
    // 1. Start a workflow
    $key = $client->startWorkflow(
        workflowType: 'order-processing',
        namespace: 'default',
        taskQueue: 'orders',
        totalSteps: 3,
        input: '{"order_id": 12345}',
    );
    echo "1. Workflow started: key={$key}\n";

    // 2. Get the workflow status
    $status = $client->getWorkflowStatus($key);
    echo "2. Status: {$status}\n";

    // 3. Send a signal (payment confirmed)
    $signaled = $client->signalWorkflow($key, 'payment-confirmed', '{"amount": 99.99}');
    echo "3. Signal sent: " . ($signaled ? 'true' : 'false') . "\n";

    // 4. Query the workflow state
    echo "4. Querying workflow state...\n";
    $currentStatus = $client->getWorkflowStatus($key);
    echo "   Current status: {$currentStatus}\n";

    // 5. Complete the workflow
    echo "5. Completing workflow...\n";
    // $client->completeWorkflow($key, '{"result": "order shipped"}');

    echo "\n=== Basic workflow example finished! ===\n";
} finally {
    $client->close();
}
