package io.velocity.sdk;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Marks a method as a signal handler for incoming workflow signals. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SignalMethod {
    /** Signal name. Defaults to the method name if empty. */
    String value() default "";
}
