package io.velocity.examples;

import io.velocity.sdk.*;

/**
 * Example: Scheduled (cron) workflow using the VELOCITY-WorkFlow Java SDK.
 *
 * Demonstrates:
 *   - Starting a workflow tied to a cron expression
 *   - Simulating a cron fire signal
 *   - Completing the scheduled execution
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *   2. Build the SDK:
 *      cd VELOCITY-WorkFlow/sdk/java && ./gradlew build
 *   3. Run this example:
 *      ./gradlew run -PmainClass=io.velocity.examples.CronSchedule
 */
public class CronSchedule {

    private static final String CRON_EXPRESSION = "*/5 * * * *"; // Every 5 minutes

    public static void main(String[] args) {
        System.out.println("=== VELOCITY-WorkFlow Java SDK — Cron Schedule ===\n");

        try (VelocityClient client = VelocityClient.create("localhost:50051")) {
            // 1. Start a workflow with a cron schedule
            WorkflowHandle handle = client.startWorkflow(
                StartWorkflowOptions.builder()
                    .workflowType("periodic-report")
                    .namespace("default")
                    .taskQueue("reports")
                    .totalSteps(1)
                    .build()
            );
            System.out.println("1. Scheduled workflow started: key=" + handle.workflowKey());
            System.out.println("   Cron expression: " + CRON_EXPRESSION);

            // 2. Describe the workflow
            WorkflowDescription desc = client.describeWorkflow(handle.workflowKey());
            System.out.println("2. Status: " + desc.status());

            // 3. Send a cron-fire signal
            boolean signaled = client.signalWorkflow(
                handle.workflowKey(),
                "cron-fire",
                "{\"fire_number\": 1}".getBytes()
            );
            System.out.println("3. Cron fire signal sent: " + signaled);

            // 4. Complete the scheduled execution
            boolean completed = client.completeWorkflow(
                handle.workflowKey(),
                "{\"report\": \"generated\"}".getBytes()
            );
            System.out.println("4. Execution completed: " + completed);
        }

        System.out.println("\n=== Cron schedule example finished! ===");
    }
}
