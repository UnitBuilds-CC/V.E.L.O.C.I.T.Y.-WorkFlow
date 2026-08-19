package io.velocity.sdk;

import java.lang.reflect.Method;
import java.util.*;

/**
 * Worker that processes workflow and activity tasks from the VELOCITY-WorkFlow server.
 * <p>
 * The worker connects to the server, polls for tasks, and dispatches them to
 * registered workflow and activity implementations.
 * <p>
 * Usage:
 * <pre>{@code
 * // Manual registration
 * VelocityWorker worker = VelocityWorker.create("localhost:7234", "orders");
 * worker.registerWorkflow(OrderProcessingWorkflow.class);
 * worker.start();
 * 
 * // Auto-apply registration (scans packages for @DurableWorkflow annotations)
 * VelocityWorker worker = VelocityWorker.create("localhost:7234", "orders");
 * worker.autoDiscoverWorkflows("com.example.workflows");
 * worker.start();
 * }</pre>
 */
public class VelocityWorker {

    private final String target;
    private final String taskQueue;
    private final Map<String, Class<?>> workflowTypes = new LinkedHashMap<>();
    private final Map<String, Method> signalHandlers = new LinkedHashMap<>();
    private final Map<String, Method> queryHandlers = new LinkedHashMap<>();
    private final Map<String, Method> activityHandlers = new LinkedHashMap<>();
    private volatile boolean running = false;

    private VelocityWorker(String target, String taskQueue) {
        this.target = target;
        this.taskQueue = taskQueue;
    }

    /**
     * Create a new worker.
     *
     * @param target    gRPC server address
     * @param taskQueue task queue to poll from
     * @return a new VelocityWorker
     */
    public static VelocityWorker create(String target, String taskQueue) {
        return new VelocityWorker(target, taskQueue);
    }

    /**
     * Register a workflow class annotated with @DurableWorkflow.
     * Scans for @WorkflowMethod, @SignalMethod, @QueryMethod, and @ActivityMethod annotations.
     *
     * @param workflowClass the workflow class to register
     */
    public void registerWorkflow(Class<?> workflowClass) {
        DurableWorkflow annotation = workflowClass.getAnnotation(DurableWorkflow.class);
        String typeName = workflowClass.getSimpleName();
        if (annotation != null) {
            typeName = annotation.taskQueue() + "/" + typeName;
        }
        workflowTypes.put(typeName, workflowClass);

        // Scan for annotated methods
        for (Method method : workflowClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(WorkflowMethod.class)) {
                // Main workflow entry point — registered under the type name
            }
            if (method.isAnnotationPresent(SignalMethod.class)) {
                SignalMethod sig = method.getAnnotation(SignalMethod.class);
                String signalName = sig.value().isEmpty() ? method.getName() : sig.value();
                signalHandlers.put(signalName, method);
            }
            if (method.isAnnotationPresent(QueryMethod.class)) {
                QueryMethod qry = method.getAnnotation(QueryMethod.class);
                String queryName = qry.value().isEmpty() ? method.getName() : qry.value();
                queryHandlers.put(queryName, method);
            }
            if (method.isAnnotationPresent(ActivityMethod.class)) {
                ActivityMethod act = method.getAnnotation(ActivityMethod.class);
                String actName = act.value().isEmpty() ? method.getName() : act.value();
                activityHandlers.put(actName, method);
            }
        }
    }

    /**
     * Auto-discover and register all workflows in the specified packages.
     * Scans for classes with @DurableWorkflow annotation and registers them automatically.
     *
     * @param packageNames packages to scan (e.g., "com.example.workflows")
     */
    public void autoDiscoverWorkflows(String... packageNames) {
        WorkflowScanner scanner = new WorkflowScanner();
        List<Class<?>> workflows = scanner.scanPackages(packageNames);
        
        for (Class<?> workflow : workflows) {
            registerWorkflow(workflow);
        }
        
        System.out.printf("[VelocityWorker] Auto-discovered %d workflows from packages: %s%n",
                workflows.size(), String.join(", ", packageNames));
    }

    /**
     * Start the worker polling loop.
     * In a full implementation, this would poll the gRPC server for tasks.
     */
    public void start() {
        running = true;
        System.out.printf("[VelocityWorker] Started on %s, queue=%s, workflows=%d, signals=%d, queries=%d, activities=%d%n",
                target, taskQueue, workflowTypes.size(), signalHandlers.size(),
                queryHandlers.size(), activityHandlers.size());
    }

    /**
     * Stop the worker.
     */
    public void stop() {
        running = false;
    }

    /**
     * Check if the worker is running.
     */
    public boolean isRunning() {
        return running;
    }

    /** Get registered workflow type names. */
    public Set<String> getWorkflowTypes() {
        return Collections.unmodifiableSet(workflowTypes.keySet());
    }

    /** Get registered signal handler names. */
    public Set<String> getSignalHandlers() {
        return Collections.unmodifiableSet(signalHandlers.keySet());
    }

    /** Get registered query handler names. */
    public Set<String> getQueryHandlers() {
        return Collections.unmodifiableSet(queryHandlers.keySet());
    }

    /** Get registered activity handler names. */
    public Set<String> getActivityHandlers() {
        return Collections.unmodifiableSet(activityHandlers.keySet());
    }
}
