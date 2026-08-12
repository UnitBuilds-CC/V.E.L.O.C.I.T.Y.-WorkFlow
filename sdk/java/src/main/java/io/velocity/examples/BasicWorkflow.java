package io.velocity.examples;

import io.velocity.sdk.*;

/**
 * Example: Basic workflow with signal and query using the VELOCITY-WorkFlow Java SDK.
 *
 * Demonstrates:
 *   - Starting a workflow
 *   - Sending signals
 *   - Querying workflow state
 *   - Completing the workflow
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *   2. Build the SDK:
 *      cd VELOCITY-WorkFlow/sdk/java && ./gradlew build
 *   3. Run this example:
 *      ./gradlew run -PmainClass=io.velocity.examples.BasicWorkflow
 */
public class BasicWorkflow {

    public static void main(String[] args) {
        System.out.println("=== VELOCITY-WorkFlow Java SDK — Basic Workflow ===\n");

        try (VelocityClient client = VelocityClient.create("localhost:50051")) {
            // 1. Verify connectivity
            boolean connected = client.ping();
            System.out.println("1. Connected: " + connected);

            // 2. Start a workflow
            WorkflowHandle handle = client.startWorkflow(
                StartWorkflowOptions.builder()
                    .workflowType("order-processing")
                    .namespace("default")
                    .taskQueue("orders")
                    .totalSteps(3)
                    .build()
            );
            System.out.println("2. Workflow started: key=" + handle.workflowKey());

            // 3. Describe the workflow
            WorkflowDescription desc = client.describeWorkflow(handle.workflowKey());
            System.out.println("3. Status: " + desc.status() + ", Step: " + desc.currentStep() + "/" + desc.totalSteps());

            // 4. Send a signal (payment confirmed)
            boolean signaled = client.signalWorkflow(
                handle.workflowKey(),
                "payment-confirmed",
                "{\"amount\": 99.99}".getBytes()
            );
            System.out.println("4. Signal sent: " + signaled);

            // 5. Query the workflow state
            byte[] queryResult = client.queryWorkflow(handle.workflowKey(), "current-state");
            System.out.println("5. Query result: " + new String(queryResult));

            // 6. Complete the workflow
            boolean completed = client.completeWorkflow(
                handle.workflowKey(),
                "{\"result\": \"order shipped\"}".getBytes()
            );
            System.out.println("6. Completed: " + completed);
        }

        System.out.println("\n=== Basic workflow example finished! ===");
    }
}
