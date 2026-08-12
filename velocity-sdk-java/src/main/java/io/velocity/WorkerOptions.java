package io.velocity;

import java.util.Map;
import java.util.HashMap;
import java.util.function.BiFunction;

/**
 * Options for creating a Worker.
 */
public class WorkerOptions {
    private String hostPort;
    private String namespace;
    private String taskQueue;
    private Map<String, BiFunction<WorkflowContext, Object, Object>> workflows;
    private Map<String, BiFunction<ActivityContext, Object, Object>> activities;
    private int maxConcurrentWorkflowTasks;
    private int maxConcurrentActivityTasks;

    public WorkerOptions() {
        this.hostPort = "localhost:7233";
        this.namespace = "default";
        this.workflows = new HashMap<>();
        this.activities = new HashMap<>();
        this.maxConcurrentWorkflowTasks = 10;
        this.maxConcurrentActivityTasks = 10;
    }

    // Builder pattern
    public WorkerOptions setHostPort(String hostPort) {
        this.hostPort = hostPort;
        return this;
    }

    public WorkerOptions setNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public WorkerOptions setTaskQueue(String taskQueue) {
        this.taskQueue = taskQueue;
        return this;
    }

    public WorkerOptions setWorkflows(Map<String, BiFunction<WorkflowContext, Object, Object>> workflows) {
        this.workflows = workflows;
        return this;
    }

    public WorkerOptions setActivities(Map<String, BiFunction<ActivityContext, Object, Object>> activities) {
        this.activities = activities;
        return this;
    }

    public WorkerOptions setMaxConcurrentWorkflowTasks(int maxConcurrentWorkflowTasks) {
        this.maxConcurrentWorkflowTasks = maxConcurrentWorkflowTasks;
        return this;
    }

    public WorkerOptions setMaxConcurrentActivityTasks(int maxConcurrentActivityTasks) {
        this.maxConcurrentActivityTasks = maxConcurrentActivityTasks;
        return this;
    }

    // Getters
    public String getHostPort() { return hostPort; }
    public String getNamespace() { return namespace; }
    public String getTaskQueue() { return taskQueue; }
    public Map<String, BiFunction<WorkflowContext, Object, Object>> getWorkflows() { return workflows; }
    public Map<String, BiFunction<ActivityContext, Object, Object>> getActivities() { return activities; }
    public int getMaxConcurrentWorkflowTasks() { return maxConcurrentWorkflowTasks; }
    public int getMaxConcurrentActivityTasks() { return maxConcurrentActivityTasks; }
}
