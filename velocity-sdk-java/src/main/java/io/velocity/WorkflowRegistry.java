package io.velocity;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Workflow registration and execution.
 */
public class WorkflowRegistry {
    private static final Map<String, BiFunction<WorkflowContext, Object, Object>> registry = new ConcurrentHashMap<>();

    /**
     * Register a workflow function.
     */
    public static void registerWorkflow(String name, BiFunction<WorkflowContext, Object, Object> func) {
        registry.put(name, func);
    }

    /**
     * Get a registered workflow function.
     */
    public static BiFunction<WorkflowContext, Object, Object> getWorkflow(String name) {
        return registry.get(name);
    }

    /**
     * Check if a workflow is registered.
     */
    public static boolean hasWorkflow(String name) {
        return registry.containsKey(name);
    }

    /**
     * Execute a workflow.
     */
    public static Object executeWorkflow(String name, WorkflowContext context, Object input) {
        BiFunction<WorkflowContext, Object, Object> func = registry.get(name);
        if (func == null) {
            throw new IllegalArgumentException("Workflow not registered: " + name);
        }
        return func.apply(context, input);
    }
}
