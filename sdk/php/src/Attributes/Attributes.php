<?php
/**
 * Auto-apply attributes for the VELOCITY-WorkFlow PHP SDK.
 *
 * These attributes enable annotation-driven workflow and activity registration.
 * When a class or method is decorated with #[Workflow] or #[Activity], it is
 * automatically registered in a global registry. The Worker class scans this
 * registry at startup — no manual registration needed.
 *
 * @example
 * ```php
 * use Velocity\SDK\Attributes\Workflow;
 * use Velocity\SDK\Attributes\Activity;
 *
 * #[Workflow]
 * class OrderWorkflow {
 *     public function run(WorkflowContext $ctx, string $orderId): array {
 *         $result = $ctx->executeActivity('process_payment', $orderId);
 *         return $result;
 *     }
 * }
 *
 * #[Activity]
 * function process_payment(string $orderId): array {
 *     return ['status' => 'charged', 'order_id' => $orderId];
 * }
 * ```
 */

declare(strict_types=1);

namespace Velocity\SDK\Attributes;

use Attribute;

/**
 * Marks a class as a durable workflow.
 *
 * The decorated class is automatically registered in the workflow registry.
 * The Worker scans this registry at startup and dispatches tasks to the
 * matching class based on the workflow type name.
 */
#[Attribute(Attribute::TARGET_CLASS)]
class Workflow
{
    public function __construct(
        public ?string $name = null,
        public ?string $taskQueue = null,
    ) {}
}

/**
 * Marks a function or method as a durable activity.
 *
 * The decorated function is automatically registered in the activity registry.
 * The Worker scans this registry at startup and dispatches activity tasks to
 * the matching function based on the activity type name.
 */
#[Attribute(Attribute::TARGET_FUNCTION | Attribute::TARGET_METHOD)]
class Activity
{
    public function __construct(
        public ?string $name = null,
        public ?int $startToCloseTimeoutMs = null,
        public ?int $scheduleToCloseTimeoutMs = null,
        public ?int $retryMaxAttempts = null,
    ) {}
}

/**
 * Marks a method as a signal handler within a workflow class.
 */
#[Attribute(Attribute::TARGET_METHOD)]
class Signal
{
    public function __construct(
        public ?string $name = null,
    ) {}
}

/**
 * Marks a method as a query handler within a workflow class.
 */
#[Attribute(Attribute::TARGET_METHOD)]
class Query
{
    public function __construct(
        public ?string $name = null,
    ) {}
}

/**
 * Marks a method as an update handler within a workflow class.
 */
#[Attribute(Attribute::TARGET_METHOD)]
class Update
{
    public function __construct(
        public ?string $name = null,
    ) {}
}
