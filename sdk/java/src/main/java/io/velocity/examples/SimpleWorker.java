package io.velocity.examples;

import io.velocity.sdk.VelocityWorker;
import io.velocity.sdk.VelocityClient;
import io.velocity.sdk.DurableWorkflow;
import io.velocity.sdk.WorkflowMethod;
import io.velocity.sdk.SignalMethod;

/**
 * Example: Simple task worker using the VELOCITY-WorkFlow Java SDK.
 *
 * Demonstrates:
 *   - Worker registration with options
 *   - Task polling from a task queue
 *   - Task execution with registered handlers
 *   - Error handling
 *   - Shutdown hook for graceful termination
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *
 *   2. Build the SDK:
 *      cd VELOCITY-WorkFlow/sdk/java && ./gradlew build
 *
 *   3. Run this example:
 *      ./gradlew run -PmainClass=io.velocity.examples.SimpleWorker
 */
public class SimpleWorker {

    private static final String SERVER_ADDR = "localhost:50051";
    private static final String TASK_QUEUE = "orders";

    // ── Workflow definition ──────────────────────────────────────────────

    @DurableWorkflow(taskQueue = "orders")
    public static class OrderProcessingWorkflow {

        @WorkflowMethod
        public String execute(String input) {
            System.out.printf("[worker] Processing order: %s%n", input);
            // Simulate work
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
            return "{\"status\": \"shipped\", \"order_id\": " + input + "}";
        }

        @SignalMethod("payment-confirmed")
        public void onPaymentConfirmed(String payload) {
            System.out.printf("[worker] Payment confirmed: %s%n", payload);
        }
    }

    // ── Main entry point ─────────────────────────────────────────────────

    public static void main(String[] args) {
        System.out.println("[worker] Starting VELOCITY-WorkFlow Java worker");
        System.out.printf("[worker] Server: %s | Queue: %s%n", SERVER_ADDR, TASK_QUEUE);

        // Create and configure the worker
        VelocityWorker worker = VelocityWorker.create(SERVER_ADDR, TASK_QUEUE);
        worker.registerWorkflow(OrderProcessingWorkflow.class);

        // Register JVM shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[worker] Shutdown signal received — stopping worker...");
            worker.stop();
            System.out.println("[worker] Worker shut down cleanly");
        }));

        // Start the worker (begins polling)
        worker.start();

        System.out.printf("[worker] Registered workflows: %s%n", worker.getWorkflowTypes());
        System.out.printf("[worker] Registered signals: %s%n", worker.getSignalHandlers());
        System.out.println("[worker] Polling for tasks... (Ctrl+C to stop)");

        // Poll loop — in production this runs until worker.stop() is called
        try {
            while (worker.isRunning()) {
                try {
                    // Poll for next task (blocking with timeout)
                    // In a full implementation, VelocityWorker.pollAndExecute() handles
                    // task dispatch to registered handlers automatically.
                    Thread.sleep(1000); // Poll interval
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("[worker] Poll loop interrupted");
                    break;
                } catch (Exception e) {
                    System.err.printf("[worker] Error processing task: %s%n", e.getMessage());
                }
            }
        } finally {
            worker.stop();
        }
    }
}
