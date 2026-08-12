package io.velocity.sdk.testing;

import io.velocity.sdk.*;
import io.velocity.sdk.exceptions.WorkflowNotFoundException;
import io.velocity.sdk.exceptions.WorkflowAlreadyCompletionException;

import java.util.*;

/**
 * Test environment for running workflows in isolation.
 * <p>
 * Provides a mock client and utilities for unit testing workflows
 * without requiring a running VELOCITY-WorkFlow server.
 */
public class TestWorkflowEnvironment {

    private final MockVelocityClient client;
    private long timeOffset = 0;

    /**
     * Create a new test environment.
     */
    public TestWorkflowEnvironment() {
        this.client = new MockVelocityClient();
    }

    /**
     * Get the mock client.
     *
     * @return the mock client
     */
    public MockVelocityClient getClient() {
        return client;
    }

    /**
     * Start a workflow in the test environment.
     *
     * @param options workflow options
     * @return handle to the workflow
     */
    public WorkflowHandle startWorkflow(StartWorkflowOptions options) {
        return client.startWorkflow(options);
    }

    /**
     * Complete a workflow in the test environment.
     *
     * @param workflowKey the workflow key
     * @param result      the result payload
     * @return true if completed
     */
    public boolean completeWorkflow(long workflowKey, byte[] result) {
        return client.completeWorkflow(workflowKey, result);
    }

    /**
     * Signal a workflow in the test environment.
     *
     * @param workflowKey the workflow key
     * @param signalName  the signal name
     * @param payload     the signal payload
     * @return true if signaled
     */
    public boolean signalWorkflow(long workflowKey, String signalName, byte[] payload) {
        return client.signalWorkflow(workflowKey, signalName, payload);
    }

    /**
     * Advance the test environment's clock.
     *
     * @param seconds number of seconds to advance
     */
    public void timeSkip(long seconds) {
        this.timeOffset += seconds;
    }

    /**
     * Get the current test time (real time + offset).
     *
     * @return current test time in milliseconds
     */
    public long getCurrentTime() {
        return System.currentTimeMillis() + (timeOffset * 1000);
    }

    /**
     * Assert that a workflow has completed.
     *
     * @param workflowKey the workflow key
     * @throws AssertionError if the workflow is not completed
     */
    public void assertWorkflowCompleted(long workflowKey) {
        WorkflowDescription desc = client.describeWorkflow(workflowKey);
        if (desc.status() != WorkflowStatus.COMPLETED) {
            throw new AssertionError(
                String.format("Expected workflow %d to be completed, but status is %s",
                    workflowKey, desc.status())
            );
        }
    }

    /**
     * Assert that a workflow received a specific signal.
     *
     * @param workflowKey the workflow key
     * @param signalName  the signal name
     * @throws AssertionError if the signal was not received
     */
    public void assertSignalReceived(long workflowKey, String signalName) {
        List<String> signals = client.getSignals(workflowKey);
        if (!signals.contains(signalName)) {
            throw new AssertionError(
                String.format("Expected signal '%s' not found. Received: %s",
                    signalName, signals)
            );
        }
    }

    /**
     * Reset the test environment.
     */
    public void reset() {
        // Note: In a real implementation, you'd want to replace the client
        // For now, this is a no-op placeholder
        this.timeOffset = 0;
    }

    /**
     * Mock client for testing workflows without a server.
     */
    public static class MockVelocityClient {

        private final Map<Long, WorkflowState> workflows = new HashMap<>();
        private final Map<Long, List<String>> signals = new HashMap<>();
        private long nextKey = 1;

        /**
         * Start a mock workflow.
         *
         * @param options workflow options
         * @return handle to the workflow
         */
        public WorkflowHandle startWorkflow(StartWorkflowOptions options) {
            long key = nextKey++;
            workflows.put(key, new WorkflowState(
                options.workflowType(),
                options.namespace(),
                options.taskQueue(),
                options.totalSteps()
            ));
            signals.put(key, new ArrayList<>());
            return new WorkflowHandle(key, key, key + 1000);
        }

        /**
         * Describe a mock workflow.
         *
         * @param workflowKey the workflow key
         * @return workflow description
         * @throws WorkflowNotFoundException if workflow not found
         */
        public WorkflowDescription describeWorkflow(long workflowKey) {
            WorkflowState state = workflows.get(workflowKey);
            if (state == null) {
                throw new WorkflowNotFoundException(workflowKey);
            }
            return new WorkflowDescription(workflowKey, state.status, 0, state.totalSteps);
        }

        /**
         * Send a signal to a mock workflow.
         *
         * @param workflowKey the workflow key
         * @param signalName  the signal name
         * @param payload     the signal payload
         * @return true if signaled
         * @throws WorkflowNotFoundException if workflow not found
         */
        public boolean signalWorkflow(long workflowKey, String signalName, byte[] payload) {
            WorkflowState state = workflows.get(workflowKey);
            if (state == null) {
                throw new WorkflowNotFoundException(workflowKey);
            }
            signals.get(workflowKey).add(signalName);
            return true;
        }

        /**
         * Complete a mock workflow.
         *
         * @param workflowKey the workflow key
         * @param result      the result payload
         * @return true if completed
         * @throws WorkflowNotFoundException if workflow not found
         * @throws WorkflowAlreadyCompletionException if already completed
         */
        public boolean completeWorkflow(long workflowKey, byte[] result) {
            WorkflowState state = workflows.get(workflowKey);
            if (state == null) {
                throw new WorkflowNotFoundException(workflowKey);
            }
            if (state.status != WorkflowStatus.RUNNING) {
                throw new WorkflowAlreadyCompletionException(workflowKey);
            }
            state.status = WorkflowStatus.COMPLETED;
            return true;
        }

        /**
         * Fail a mock workflow.
         *
         * @param workflowKey the workflow key
         * @param reason      the failure reason
         * @return true if failed
         * @throws WorkflowNotFoundException if workflow not found
         * @throws WorkflowAlreadyCompletionException if already completed
         */
        public boolean failWorkflow(long workflowKey, String reason) {
            WorkflowState state = workflows.get(workflowKey);
            if (state == null) {
                throw new WorkflowNotFoundException(workflowKey);
            }
            if (state.status != WorkflowStatus.RUNNING) {
                throw new WorkflowAlreadyCompletionException(workflowKey);
            }
            state.status = WorkflowStatus.FAILED;
            return true;
        }

        /**
         * Cancel a mock workflow.
         *
         * @param workflowKey the workflow key
         * @return true if cancelled
         * @throws WorkflowNotFoundException if workflow not found
         */
        public boolean cancelWorkflow(long workflowKey) {
            WorkflowState state = workflows.get(workflowKey);
            if (state == null) {
                throw new WorkflowNotFoundException(workflowKey);
            }
            state.status = WorkflowStatus.CANCELED;
            return true;
        }

        /**
         * Get all signals received by a workflow.
         *
         * @param workflowKey the workflow key
         * @return list of signal names
         */
        public List<String> getSignals(long workflowKey) {
            return signals.getOrDefault(workflowKey, new ArrayList<>());
        }

        /**
         * Internal workflow state.
         */
        private static class WorkflowState {
            String workflowType;
            String namespace;
            String taskQueue;
            int totalSteps;
            WorkflowStatus status;

            WorkflowState(String workflowType, String namespace, String taskQueue, int totalSteps) {
                this.workflowType = workflowType;
                this.namespace = namespace;
                this.taskQueue = taskQueue;
                this.totalSteps = totalSteps;
                this.status = WorkflowStatus.RUNNING;
            }
        }
    }
}
