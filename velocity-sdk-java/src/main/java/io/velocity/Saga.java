package io.velocity;

import java.util.*;
import java.util.function.Supplier;

/**
 * Saga orchestration for multi-step workflows with compensating transactions.
 * If any step fails, previously completed steps are rolled back in reverse order.
 */
public class Saga {
    private final List<SagaStep> steps = new ArrayList<>();
    private final List<SagaStep> completed = new ArrayList<>();
    private final List<Object> results = new ArrayList<>();

    public Saga addStep(String name, Supplier<Object> execute, Runnable compensate) {
        steps.add(new SagaStep(name, execute, compensate));
        return this;
    }

    /**
     * Execute all saga steps. Returns results list.
     * If a step fails, completed steps are compensated in reverse order.
     * @throws SagaException if any step fails (after compensation)
     */
    public List<Object> execute() throws SagaException {
        completed.clear();
        results.clear();

        for (SagaStep step : steps) {
            try {
                Object result = step.getExecute().get();
                completed.add(step);
                results.add(result);
            } catch (Exception e) {
                compensate();
                throw new SagaException("Saga step '" + step.getName() + "' failed", e, results);
            }
        }

        return results;
    }

    private void compensate() {
        for (int i = completed.size() - 1; i >= 0; i--) {
            try {
                completed.get(i).getCompensate().run();
            } catch (Exception e) {
                // Best-effort compensation
            }
        }
    }

    public List<Object> getResults() { return results; }
}
