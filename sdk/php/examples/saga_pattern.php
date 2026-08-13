<?php
/**
 * Example: Multi-step saga with compensation using the VELOCITY-WorkFlow PHP SDK.
 *
 * Demonstrates:
 *   - Defining a saga with compensable steps
 *   - Executing steps in order
 *   - Triggering compensation on failure
 *   - Rolling back completed steps in reverse order
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *   2. composer install
 *   3. php examples/saga_pattern.php
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Velocity\SDK\VelocityClient;

/** Saga step definition. */
class SagaStep {
    public function __construct(
        public readonly string $name,
        public readonly string $compensate,
    ) {}
}

const STEPS = [
    new SagaStep('reserve_inventory', 'release_inventory'),
    new SagaStep('charge_payment',    'refund_payment'),
    new SagaStep('book_shipping',     'cancel_shipping'),
    new SagaStep('send_confirmation', 'send_cancellation_notice'),
];

/**
 * Execute the saga. If $simulateFailureAt is set, the step at that index fails.
 */
function runSaga(VelocityClient $client, ?int $simulateFailureAt): bool
{
    $key = $client->startWorkflow(
        workflowType: 'order-saga',
        namespace: 'default',
        taskQueue: 'orders',
        totalSteps: count(STEPS),
    );
    echo "  Saga started: key={$key}\n";

    $completedSteps = [];

    foreach (STEPS as $i => $step) {
        // Simulate failure
        if ($simulateFailureAt !== null && $i === $simulateFailureAt) {
            echo "\n   ✗ Step '{$step->name}' FAILED — triggering compensation\n";
            // Compensate in reverse order
            for ($j = count($completedSteps) - 1; $j >= 0; $j--) {
                $prev = $completedSteps[$j];
                echo "   Compensating: {$prev->compensate}\n";
                $client->signalWorkflow($key, $prev->compensate, '');
            }
            return false;
        }

        echo "   Executing: {$step->name}\n";
        $client->signalWorkflow($key, $step->name, '');
        $completedSteps[] = $step;
    }

    echo "   ✓ All saga steps completed successfully\n";
    return true;
}

echo "=== VELOCITY-WorkFlow PHP SDK — Saga Pattern ===\n\n";

$client = new VelocityClient('localhost:7234');

try {
    // Scenario 1: Happy path
    echo "Scenario 1: Happy path\n";
    runSaga($client, null);

    // Scenario 2: Payment step fails (index=1)
    echo "\nScenario 2: Payment step fails (index=1)\n";
    runSaga($client, 1);
} finally {
    $client->close();
}

echo "\n=== Saga examples finished! ===\n";
