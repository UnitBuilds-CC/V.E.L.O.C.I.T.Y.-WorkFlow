package io.velocity.sdk;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Worker that processes workflow and activity tasks from the VELOCITY-WorkFlow server.
 * <p>
 * The worker connects to the server, polls for tasks, and dispatches them to
 * registered workflow and activity implementations. Results and failures are
 * reported back to the server via RespondWorkflowTaskCompleted / RespondActivityTaskCompleted.
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
    private final String namespace;
    private final String identity;
    private final Map<String, Class<?>> workflowTypes = new LinkedHashMap<>();
    private final Map<String, Method> signalHandlers = new LinkedHashMap<>();
    private final Map<String, Method> queryHandlers = new LinkedHashMap<>();
    private final Map<String, Method> activityHandlers = new LinkedHashMap<>();
    private volatile boolean running = false;
    private ExecutorService executor;

    // Stats
    private final AtomicLong workflowsStarted = new AtomicLong();
    private final AtomicLong workflowsCompleted = new AtomicLong();
    private final AtomicLong workflowsFailed = new AtomicLong();
    private final AtomicLong activitiesStarted = new AtomicLong();
    private final AtomicLong activitiesCompleted = new AtomicLong();
    private final AtomicLong activitiesFailed = new AtomicLong();
    private final AtomicLong tasksPolled = new AtomicLong();

    private VelocityWorker(String target, String taskQueue) {
        this(target, taskQueue, "default");
    }

    private VelocityWorker(String target, String taskQueue, String namespace) {
        this.target = target;
        this.taskQueue = taskQueue;
        this.namespace = namespace;
        this.identity = String.format("java-worker-1.0@%s", getHostname());
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
     * Create a new worker with namespace.
     *
     * @param target    gRPC server address
     * @param taskQueue task queue to poll from
     * @param namespace namespace scope
     * @return a new VelocityWorker
     */
    public static VelocityWorker create(String target, String taskQueue, String namespace) {
        return new VelocityWorker(target, taskQueue, namespace);
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
     * Polls the server for workflow and activity tasks, dispatches them to
     * registered handlers, and reports results back.
     */
    public void start() {
        running = true;
        executor = Executors.newFixedThreadPool(4);

        System.out.printf("[VelocityWorker] Started on %s, queue=%s, ns=%s, identity=%s, " +
                "workflows=%d, signals=%d, queries=%d, activities=%d%n",
                target, taskQueue, namespace, identity,
                workflowTypes.size(), signalHandlers.size(),
                queryHandlers.size(), activityHandlers.size());

        // Install shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

        // Start workflow poll loop
        executor.submit(this::pollWorkflowTasks);

        // Start activity poll loop
        executor.submit(this::pollActivityTasks);
    }

    /**
     * Poll loop for workflow tasks.
     * Sends PollWorkflowTaskQueue gRPC calls and dispatches results.
     */
    private void pollWorkflowTasks() {
        while (running) {
            tasksPolled.incrementAndGet();
            try {
                // Poll the server for a workflow task (long-poll)
                Map<String, Object> task = pollWorkflowTaskQueue();
                if (task != null) {
                    executor.submit(() -> executeWorkflowTask(task));
                } else {
                    Thread.sleep(100); // Brief backoff
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[VelocityWorker] Workflow poll error: " + e.getMessage());
                try { Thread.sleep(1000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Poll loop for activity tasks.
     * Sends PollActivityTaskQueue gRPC calls and dispatches results.
     */
    private void pollActivityTasks() {
        while (running) {
            tasksPolled.incrementAndGet();
            try {
                Map<String, Object> task = pollActivityTaskQueue();
                if (task != null) {
                    executor.submit(() -> executeActivityTask(task));
                } else {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[VelocityWorker] Activity poll error: " + e.getMessage());
                try { Thread.sleep(1000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Execute a workflow task by dispatching to the registered workflow class.
     */
    private void executeWorkflowTask(Map<String, Object> task) {
        long taskToken = ((Number) task.getOrDefault("task_token", 0)).longValue();
        String workflowType = (String) task.getOrDefault("workflow_type", "");
        long workflowKey = ((Number) task.getOrDefault("workflow_key", 0)).longValue();

        Class<?> workflowClass = workflowTypes.get(workflowType);
        if (workflowClass == null) {
            System.err.println("[VelocityWorker] No workflow registered for type: " + workflowType);
            List<Map<String, Object>> commands = List.of(
                Map.of("fail_workflow", Map.of("reason", "No workflow registered for type: " + workflowType))
            );
            respondWorkflowTaskCompleted(taskToken, commands);
            return;
        }

        workflowsStarted.incrementAndGet();
        try {
            Object instance = workflowClass.getDeclaredConstructor().newInstance();
            Method runMethod = findWorkflowMethod(workflowClass);

            Object result;
            if (runMethod != null) {
                result = runMethod.invoke(instance);
            } else {
                throw new NoSuchMethodException("No @WorkflowMethod found on " + workflowClass.getSimpleName());
            }

            workflowsCompleted.incrementAndGet();
            List<Map<String, Object>> commands = List.of(
                Map.of("complete_workflow", Map.of("result", result != null ? result.toString().getBytes() : new byte[0]))
            );
            respondWorkflowTaskCompleted(taskToken, commands);

        } catch (Exception e) {
            workflowsFailed.incrementAndGet();
            System.err.println("[VelocityWorker] Workflow '" + workflowType + "' failed: " + e.getMessage());
            List<Map<String, Object>> commands = List.of(
                Map.of("fail_workflow", Map.of("reason", e.getMessage() != null ? e.getMessage() : e.getClass().getName()))
            );
            respondWorkflowTaskCompleted(taskToken, commands);
        }
    }

    /**
     * Execute an activity task by dispatching to the registered handler method.
     */
    private void executeActivityTask(Map<String, Object> task) {
        long taskToken = ((Number) task.getOrDefault("task_token", 0)).longValue();
        String activityType = (String) task.getOrDefault("activity_type", "");

        Method handler = activityHandlers.get(activityType);
        if (handler == null) {
            System.err.println("[VelocityWorker] No activity registered for type: " + activityType);
            respondActivityTaskFailed(taskToken, "No activity registered for type: " + activityType);
            return;
        }

        activitiesStarted.incrementAndGet();
        try {
            Object result = handler.invoke(null); // Static method invocation
            activitiesCompleted.incrementAndGet();
            respondActivityTaskCompleted(taskToken, result != null ? result.toString() : "");

        } catch (Exception e) {
            activitiesFailed.incrementAndGet();
            System.err.println("[VelocityWorker] Activity '" + activityType + "' failed: " + e.getMessage());
            respondActivityTaskFailed(taskToken, e.getMessage() != null ? e.getMessage() : e.getClass().getName());
        }
    }

    // ─── Server communication methods (gRPC stubs) ─────────────────────────────

    /**
     * Poll the server for a workflow task (long-poll).
     * In production, this calls PollWorkflowTaskQueue gRPC RPC.
     */
    private Map<String, Object> pollWorkflowTaskQueue() {
        // gRPC: PollWorkflowTaskQueue(namespace, task_queue, identity, build_id, long_poll_timeout_ms)
        // Returns task with: task_token, workflow_key, workflow_type, step_index, attempt, history
        return null; // Server returns null when no task is available within timeout
    }

    /**
     * Poll the server for an activity task (long-poll).
     * In production, this calls PollActivityTaskQueue gRPC RPC.
     */
    private Map<String, Object> pollActivityTaskQueue() {
        // gRPC: PollActivityTaskQueue(namespace, task_queue, identity, build_id, long_poll_timeout_ms)
        // Returns task with: task_token, workflow_key, activity_type, input, step_index, attempt
        return null;
    }

    /**
     * Report a workflow task as completed with commands.
     * In production, this calls RespondWorkflowTaskCompleted gRPC RPC.
     */
    private void respondWorkflowTaskCompleted(long taskToken, List<Map<String, Object>> commands) {
        // gRPC: RespondWorkflowTaskCompleted(task_token, commands, identity, namespace)
    }

    /**
     * Report an activity task as completed.
     * In production, this calls RespondActivityTaskCompleted gRPC RPC.
     */
    private void respondActivityTaskCompleted(long taskToken, String result) {
        // gRPC: RespondActivityTaskCompleted(task_token, result, identity, namespace)
    }

    /**
     * Report an activity task as failed.
     * In production, this calls RespondActivityTaskFailed gRPC RPC.
     */
    private void respondActivityTaskFailed(long taskToken, String failure) {
        // gRPC: RespondActivityTaskFailed(task_token, failure, identity, namespace)
    }

    // ─── Helper methods ─────────────────────────────────────────────────────────

    private Method findWorkflowMethod(Class<?> clazz) {
        for (Method m : clazz.getDeclaredMethods()) {
            if (m.isAnnotationPresent(WorkflowMethod.class)) {
                m.setAccessible(true);
                return m;
            }
        }
        return null;
    }

    private static String getHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    /**
     * Stop the worker.
     */
    public void stop() {
        running = false;
        if (executor != null) {
            executor.shutdownNow();
        }
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

    /** Get worker statistics as a map. */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("workflowsStarted", workflowsStarted.get());
        stats.put("workflowsCompleted", workflowsCompleted.get());
        stats.put("workflowsFailed", workflowsFailed.get());
        stats.put("activitiesStarted", activitiesStarted.get());
        stats.put("activitiesCompleted", activitiesCompleted.get());
        stats.put("activitiesFailed", activitiesFailed.get());
        stats.put("tasksPolled", tasksPolled.get());
        stats.put("running", running);
        return stats;
    }
}
