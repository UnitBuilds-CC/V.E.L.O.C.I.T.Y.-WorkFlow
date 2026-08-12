package io.velocity.sdk;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a durable activity.
 * Activities are the unit of retryable, fault-tolerant work within a workflow.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ActivityMethod {
    /** Activity type name. Defaults to the method name if empty. */
    String value() default "";

    /** Maximum retry attempts for this activity. */
    int maxAttempts() default 3;

    /** Initial retry interval in milliseconds. */
    long retryIntervalMs() default 1000;
}
