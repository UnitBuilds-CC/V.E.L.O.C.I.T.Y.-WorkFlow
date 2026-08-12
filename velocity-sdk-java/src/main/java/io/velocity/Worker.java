package io.velocity;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Worker that polls for and executes workflow and activity tasks.
 */
public class Worker {
    private final WorkerOptions options;
    private final Connection connection;
    private final AtomicBoolean running;
    private final ExecutorService executor;

    public Worker(WorkerOptions options) {
        if (options.getTaskQueue() == null || options.getTaskQueue().isEmpty()) {
            throw new IllegalArgumentException("task_queue is required");
        }

        this.options = options;
        this.connection = new Connection(options.getHostPort(), false);
        this.connection.connect();
        this.running = new AtomicBoolean(false);
        this.executor = Executors.newCachedThreadPool();

        // Register workflows and activities
        if (options.getWorkflows() != null) {
            options.getWorkflows().forEach(WorkflowRegistry::registerWorkflow);
        }
        if (options.getActivities() != null) {
            options.getActivities().forEach(ActivityRegistry::registerActivity);
        }
    }

    /**
     * Start the worker and block until stopped.
     */
    public void run() throws InterruptedException {
        if (running.getAndSet(true)) {
            throw new RuntimeException("Worker is already running");
        }

        System.out.println("Worker started for task queue: " + options.getTaskQueue());

        // Start polling threads
        executor.submit(this::pollWorkflowTasks);
        executor.submit(this::pollActivityTasks);

        // Wait for shutdown signal
        try {
            while (running.get()) {
                Thread.sleep(1000);
            }
        } finally {
            shutdown();
        }
    }

    /**
     * Stop the worker.
     */
    public void stop() {
        if (running.getAndSet(false)) {
            System.out.println("Worker stopping...");
        }
    }

    /**
     * Check if worker is running.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Get the task queue name.
     */
    public String getTaskQueue() {
        return options.getTaskQueue();
    }

    private void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            connection.close();
            System.out.println("Worker stopped");
        }
    }

    private void pollWorkflowTasks() {
        while (running.get()) {
            try {
                // In a real implementation, this would call the gRPC client
                // to poll for workflow tasks
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                if (running.get()) {
                    System.err.println("Error polling workflow task: " + e.getMessage());
                }
            }
        }
    }

    private void pollActivityTasks() {
        while (running.get()) {
            try {
                // In a real implementation, this would call the gRPC client
                // to poll for activity tasks
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                if (running.get()) {
                    System.err.println("Error polling activity task: " + e.getMessage());
                }
            }
        }
    }
}
