package io.velocity;

import java.util.function.Supplier;

/**
 * A single step in a saga with execution and compensation functions.
 */
public class SagaStep {
    private final String name;
    private final Supplier<Object> execute;
    private final Runnable compensate;

    public SagaStep(String name, Supplier<Object> execute, Runnable compensate) {
        this.name = name;
        this.execute = execute;
        this.compensate = compensate;
    }

    public String getName() { return name; }
    public Supplier<Object> getExecute() { return execute; }
    public Runnable getCompensate() { return compensate; }
}
