package io.velocity.sdk;

/**
 * Configuration options for starting a new workflow.
 *
 * @param workflowType type name of the workflow
 * @param namespace    namespace to run in
 * @param taskQueue    task queue for worker dispatch
 * @param totalSteps   number of execution steps
 * @param input        optional input payload
 */
public record StartWorkflowOptions(
        String workflowType,
        String namespace,
        String taskQueue,
        int totalSteps,
        byte[] input
) {
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String workflowType = "";
        private String namespace = "default";
        private String taskQueue = "default";
        private int totalSteps = 1;
        private byte[] input = new byte[0];

        public Builder workflowType(String workflowType) {
            this.workflowType = workflowType;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder taskQueue(String taskQueue) {
            this.taskQueue = taskQueue;
            return this;
        }

        public Builder totalSteps(int totalSteps) {
            this.totalSteps = totalSteps;
            return this;
        }

        public Builder input(byte[] input) {
            this.input = input != null ? input.clone() : new byte[0];
            return this;
        }

        public StartWorkflowOptions build() {
            return new StartWorkflowOptions(workflowType, namespace, taskQueue, totalSteps, input);
        }
    }
}
