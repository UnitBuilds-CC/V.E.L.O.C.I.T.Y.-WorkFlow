package io.velocity.examples;

import io.velocity.sdk.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Example: Parent-child workflow orchestration using the VELOCITY-WorkFlow Java SDK.
 *
 * Demonstrates:
 *   - Starting a parent workflow
 *   - Spawning child workflows from the parent
 *   - Waiting for children to complete
 *   - Aggregating child results in the parent
 *
 * Prerequisites:
 *   1. Start the VELOCITY-WorkFlow server:
 *      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
 *   2. Build the SDK:
 *      cd VELOCITY-WorkFlow/sdk/java && ./gradlew build
 *   3. Run this example:
 *      ./gradlew run -PmainClass=io.velocity.examples.ChildWorkflow
 */
public class ChildWorkflow {

    /**
     * Start and complete a child workflow, returning its workflow key.
     */
    static long runChildWorkflow(VelocityClient client, String childType, int orderId) {
        WorkflowHandle childHandle = client.startWorkflow(
            StartWorkflowOptions.builder()
                .workflowType(childType)
                .namespace("default")
                .taskQueue("children")
                .totalSteps(2)
                .build()
        );
        System.out.println("   Child '" + childType + "' started: key=" + childHandle.workflowKey());

        // Simulate child processing
        client.signalWorkflow(childHandle.workflowKey(), "process", new byte[0]);
        client.completeWorkflow(childHandle.workflowKey(), "{\"child_result\": \"ok\"}".getBytes());

        WorkflowDescription desc = client.describeWorkflow(childHandle.workflowKey());
        System.out.println("   Child '" + childType + "' completed: status=" + desc.status());
        return childHandle.workflowKey();
    }

    public static void main(String[] args) {
        System.out.println("=== VELOCITY-WorkFlow Java SDK — Child Workflows ===\n");

        try (VelocityClient client = VelocityClient.create("localhost:7234")) {
            // 1. Start the parent workflow
            WorkflowHandle parent = client.startWorkflow(
                StartWorkflowOptions.builder()
                    .workflowType("order-orchestrator")
                    .namespace("default")
                    .taskQueue("orchestration")
                    .totalSteps(4)
                    .build()
            );
            System.out.println("1. Parent workflow started: key=" + parent.workflowKey());

            // 2. Spawn child workflows
            System.out.println("\n2. Spawning child workflows...");
            String[] childTypes = {"validate-order", "process-payment", "arrange-shipping"};
            List<Long> childKeys = new ArrayList<>();

            for (int i = 0; i < childTypes.length; i++) {
                long key = runChildWorkflow(client, childTypes[i], 1001 + i);
                childKeys.add(key);
            }

            // 3. Signal parent that all children are done
            System.out.println("\n3. All children completed — signaling parent...");
            client.signalWorkflow(parent.workflowKey(), "children-complete", new byte[0]);

            // 4. Complete the parent workflow
            client.completeWorkflow(parent.workflowKey(), "{\"result\": \"all_children_done\"}".getBytes());

            // 5. Verify parent is completed
            WorkflowDescription desc = client.describeWorkflow(parent.workflowKey());
            System.out.println("4. Parent final status: " + desc.status());
        }

        System.out.println("\n=== Child workflow example finished! ===");
    }
}
