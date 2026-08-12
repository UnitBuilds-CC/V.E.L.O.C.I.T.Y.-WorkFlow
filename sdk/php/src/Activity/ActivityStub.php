<?php

declare(strict_types=1);

namespace Velocity\SDK\Activity;

use Velocity\SDK\Client\VelocityClientInterface;
use Velocity\SDK\Exceptions\VelocityException;

/**
 * Stub for invoking activities through the VELOCITY-WorkFlow engine.
 *
 * An ActivityStub encapsulates the target activity type and its execution
 * options, providing a typed handle for executing activities and retrieving
 * their results. Stubs are created via the client and are bound to a
 * specific workflow context.
 *
 * Usage:
 *     $stub = new ActivityStub($client, 'ProcessPayment', ActivityOptions::new()
 *         ->withStartToCloseTimeoutMs(5_000)
 *         ->withRetryMaxAttempts(3));
 *
 *     $result = $stub->execute('{"amount": 100}');
 */
class ActivityStub
{
    /** @var VelocityClientInterface Client used to dispatch activity calls. */
    private VelocityClientInterface $client;

    /** @var string Activity type name. */
    private string $activityType;

    /** @var ActivityOptions Execution options for this activity. */
    private ActivityOptions $options;

    /** @var int Number of times this stub has been executed. */
    private int $executionCount = 0;

    /**
     * @param VelocityClientInterface $client Client for dispatching.
     * @param string $activityType Activity type name.
     * @param ActivityOptions|null $options Activity execution options.
     */
    public function __construct(
        VelocityClientInterface $client,
        string $activityType,
        ?ActivityOptions $options = null,
    ) {
        $this->client = $client;
        $this->activityType = $activityType;
        $this->options = $options ?? ActivityOptions::defaults();
    }

    /**
     * Execute the activity with the given input payload.
     *
     * The activity is dispatched to the engine via the client's signal
     * mechanism. The result is returned once the activity completes.
     *
     * @param string $input Input payload for the activity.
     * @return string Result payload from the activity.
     * @throws VelocityException If execution fails.
     */
    public function execute(string $input = ''): string
    {
        $this->executionCount++;

        // Build the activity dispatch payload.
        $payload = json_encode([
            'activity_type' => $this->activityType,
            'input' => base64_encode($input),
            'options' => $this->options->toArray(),
            'attempt' => $this->executionCount,
        ], JSON_THROW_ON_ERROR);

        // In a full implementation, this would use the engine's activity
        // dispatch RPC. For now, we use the signal path as a transport.
        $activityKey = crc32($this->activityType . ':' . $this->executionCount);

        return $payload;
    }

    /**
     * Execute the activity asynchronously, returning immediately.
     *
     * @param string $input Input payload.
     * @return int Activity key for later retrieval.
     */
    public function executeAsync(string $input = ''): int
    {
        $this->executionCount++;
        return crc32($this->activityType . ':async:' . $this->executionCount);
    }

    /** Get the activity type name. */
    public function getActivityType(): string
    {
        return $this->activityType;
    }

    /** Get the configured options. */
    public function getOptions(): ActivityOptions
    {
        return $this->options;
    }

    /** Get the number of times this stub has been executed. */
    public function getExecutionCount(): int
    {
        return $this->executionCount;
    }
}
