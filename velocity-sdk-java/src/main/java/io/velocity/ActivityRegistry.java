package io.velocity;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Activity registration and execution.
 */
public class ActivityRegistry {
    private static final Map<String, BiFunction<ActivityContext, Object, Object>> registry = new ConcurrentHashMap<>();

    /**
     * Register an activity function.
     */
    public static void registerActivity(String name, BiFunction<ActivityContext, Object, Object> func) {
        registry.put(name, func);
    }

    /**
     * Get a registered activity function.
     */
    public static BiFunction<ActivityContext, Object, Object> getActivity(String name) {
        return registry.get(name);
    }

    /**
     * Check if an activity is registered.
     */
    public static boolean hasActivity(String name) {
        return registry.containsKey(name);
    }

    /**
     * Execute an activity.
     */
    public static Object executeActivity(String name, ActivityContext context, Object input) {
        BiFunction<ActivityContext, Object, Object> func = registry.get(name);
        if (func == null) {
            throw new IllegalArgumentException("Activity not registered: " + name);
        }
        return func.apply(context, input);
    }
}
