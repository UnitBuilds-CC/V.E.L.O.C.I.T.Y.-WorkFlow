package io.velocity.examples;

import io.velocity.sdk.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Example: Multi-step saga with compensation using the VELOCITY-WorkFlow Java SDK.
 *
 * Demonstrates:
 *   - Defining a saga with compensable steps
 *   - Executing steps in order
 *   - Triggering compensation on failure
 *   - Rolling back completed steps in reverse order
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *   2. Build the SDK:
 *      cd VELOCITY-WorkFlow/sdk/java && ./gradlew build
 *   3. Run this example:
 *      ./gradlew run -PmainClass=io.velocity.examples.SagaPattern
 */
public class SagaPattern {

    /** A single saga step with a forward action and a compensation action. */
    record SagaStep(String name, String compensate) {}

    private static final List<SagaStep> STEPS = List.of(
        new SagaStep("reserve_inventory", "release_inventory"),
        new SagaStep("charge_payment",    "refund_payment"),
        new SagaStep("book_shipping",     "cancel_shipping"),
        new SagaStep("send_confirmation", "send_cancellation_notice")
    );

    /**
     * Execute the saga. If simulateFailureAt is non-null, the step at that
     * index will fail, triggering compensation for all previously completed steps.
     */
    static boolean runSaga(VelocityClient client, Integer simulateFailureAt) {
        WorkflowHandle handle = client.startWorkflow(
            StartWorkflowOptions.builder()
                .workflowType("order-saga")
                .namespace("default")
                .taskQueue("orders")
                .totalSteps(STEPS.size())
                .build()
        );
        System.out.println("  Saga started: key=" + handle.workflowKey());

        List<SagaStep> completedSteps = new ArrayList<>();

        for (int i = 0; i < STEPS.size(); i++) {
            SagaStep step = STEPS.get(i);

            // Simulate failure at the specified step
            if (simulateFailureAt != null && i == simulateFailureAt) {
                System.out.println("\n   ✗ Step '" + step.name() + "' FAILED — triggering compensation");
                // Compensate in reverse order
                for (int j = completedSteps.size() - 1; j >= 0; j--) {
                    SagaStep prev = completedSteps.get(j);
                    System.out.println("   Compensating: " + prev.compensate());
                    client.signalWorkflow(handle.workflowKey(), prev.compensate(), new byte[0]);
                }
                client.failWorkflow(handle.workflowKey(), "Step " + step.name() + " failed");
                return false;
            }

            System.out.println("   Executing: " + step.name());
            client.signalWorkflow(handle.workflowKey(), step.name(), new byte[0]);
            completedSteps.add(step);
        }

        client.completeWorkflow(handle.workflowKey(), "{\"status\": \"saga_complete\"}".getBytes());
        System.out.println("   ✓ All saga steps completed successfully");
        return true;
    }

    public static void main(String[] args) {
        System.out.println("=== VELOCITY-WorkFlow Java SDK — Saga Pattern ===\n");

        try (VelocityClient client = VelocityClient.create("localhost:7234")) {
            // Scenario 1: Happy path
            System.out.println("Scenario 1: Happy path");
            runSaga(client, null);

            // Scenario 2: Payment step fails (index=1)
            System.out.println("\nScenario 2: Payment step fails (index=1)");
            runSaga(client, 1);
        }

        System.out.println("\n=== Saga examples finished! ===");
    }
}
