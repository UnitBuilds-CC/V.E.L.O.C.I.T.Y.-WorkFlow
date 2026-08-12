package io.velocity.sdk;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the VELOCITY-WorkFlow Java SDK.
 */
class VelocityClientTest {

    @Test
    void testCreateClient() {
        try (VelocityClient client = VelocityClient.create("localhost:50051")) {
            assertEquals("localhost:50051", client.target());
            assertTrue(client.ping());
        }
    }

    @Test
    void testStartWorkflow() {
        try (VelocityClient client = VelocityClient.create("localhost:50051")) {
            WorkflowHandle handle = client.startWorkflow(
                    StartWorkflowOptions.builder()
                            .workflowType("order-processing")
                            .namespace("default")
                            .taskQueue("orders")
                            .totalSteps(5)
                            .build()
            );
            assertTrue(handle.workflowKey() > 0);
            assertTrue(handle.runId() > 0);
        }
    }

    @Test
    void testDescribeWorkflow() {
        try (VelocityClient client = VelocityClient.create("localhost:50051")) {
            WorkflowHandle handle = client.startWorkflow(
                    StartWorkflowOptions.builder()
                            .workflowType("test-workflow")
                            .totalSteps(3)
                            .build()
            );
            WorkflowDescription desc = client.describeWorkflow(handle.workflowKey());
            assertEquals(handle.workflowKey(), desc.workflowKey());
            assertEquals(WorkflowStatus.RUNNING, desc.status());
        }
    }

    @Test
    void testSignalWorkflow() {
        try (VelocityClient client = VelocityClient.create("localhost:50051")) {
            WorkflowHandle handle = client.startWorkflow(
                    StartWorkflowOptions.builder()
                            .workflowType("signal-test")
                            .build()
            );
            assertTrue(client.signalWorkflow(handle.workflowKey(), "approval", new byte[]{1, 2, 3}));
        }
    }

    @Test
    void testWorkflowLifecycle() {
        try (VelocityClient client = VelocityClient.create("localhost:50051")) {
            WorkflowHandle handle = client.startWorkflow(
                    StartWorkflowOptions.builder()
                            .workflowType("lifecycle-test")
                            .build()
            );
            assertTrue(client.completeWorkflow(handle.workflowKey(), new byte[]{42}));
        }
    }

    @Test
    void testFnv1aHashDeterministic() {
        long h1 = VelocityClient.fnv1aHash("order-processing");
        long h2 = VelocityClient.fnv1aHash("order-processing");
        assertEquals(h1, h2);

        long h3 = VelocityClient.fnv1aHash("different-type");
        assertNotEquals(h1, h3);
    }

    @Test
    void testWorkflowStatus() {
        assertEquals(0, WorkflowStatus.RUNNING.getValue());
        assertEquals(1, WorkflowStatus.COMPLETED.getValue());
        assertEquals(WorkflowStatus.FAILED, WorkflowStatus.fromValue(2));
        assertEquals(WorkflowStatus.RUNNING, WorkflowStatus.fromValue(99)); // unknown → RUNNING
    }

    @Test
    void testStartWorkflowOptionsBuilder() {
        StartWorkflowOptions opts = StartWorkflowOptions.builder()
                .workflowType("my-workflow")
                .namespace("production")
                .taskQueue("high-priority")
                .totalSteps(10)
                .input(new byte[]{1, 2, 3})
                .build();

        assertEquals("my-workflow", opts.workflowType());
        assertEquals("production", opts.namespace());
        assertEquals("high-priority", opts.taskQueue());
        assertEquals(10, opts.totalSteps());
        assertArrayEquals(new byte[]{1, 2, 3}, opts.input());
    }

    @Test
    void testWorkerRegistration() {
        VelocityWorker worker = VelocityWorker.create("localhost:50051", "orders");

        worker.registerWorkflow(SampleWorkflow.class);

        assertTrue(worker.getWorkflowTypes().stream().anyMatch(t -> t.contains("SampleWorkflow")));
        assertTrue(worker.getSignalHandlers().contains("onApproval"));
        assertTrue(worker.getQueryHandlers().contains("getStatus"));
        assertTrue(worker.getActivityHandlers().contains("processOrder"));
    }

    @Test
    void testWorkerStartStop() {
        VelocityWorker worker = VelocityWorker.create("localhost:50051", "default");
        assertFalse(worker.isRunning());

        worker.start();
        assertTrue(worker.isRunning());

        worker.stop();
        assertFalse(worker.isRunning());
    }

    // ─── Test Fixtures ──────────────────────────────────────────────────────

    @DurableWorkflow(taskQueue = "orders", version = 1)
    static class SampleWorkflow implements WorkflowInterface {
        @WorkflowMethod
        public void execute() {}

        @SignalMethod("onApproval")
        public void handleApproval() {}

        @QueryMethod("getStatus")
        public String getStatus() { return "running"; }

        @ActivityMethod(value = "processOrder", maxAttempts = 3)
        public void processOrder() {}
    }
}
