package io.velocity.sdk;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a durable workflow definition.
 * <p>
 * The VELOCITY-WorkFlow engine uses this annotation to register workflow types
 * at compile time, enabling type-safe workflow dispatch and versioning.
 * <p>
 * Example:
 * <pre>{@code
 * @DurableWorkflow(taskQueue = "orders", version = 1)
 * public class OrderProcessingWorkflow implements WorkflowInterface {
 *     @WorkflowMethod
 *     public void execute(OrderContext ctx) { ... }
 *
 *     @SignalMethod
 *     public void onApproval(ApprovalSignal signal) { ... }
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DurableWorkflow {
    /** Task queue for worker dispatch. */
    String taskQueue() default "default";

    /** Workflow version for versioning and replay compatibility. */
    int version() default 1;

    /** Namespace to register in. */
    String namespace() default "default";

    /** Maximum execution time in seconds (0 = unlimited). */
    long timeoutSeconds() default 0;

    /** Retry policy: maximum attempts (0 = no retry). */
    int maxAttempts() default 3;
}
