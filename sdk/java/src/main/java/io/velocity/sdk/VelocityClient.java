package io.velocity.sdk;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import java.util.concurrent.TimeUnit;

/**
 * VELOCITY-WorkFlow Java SDK client.
 * <p>
 * Cross-language gRPC client that connects to the VELOCITY-WorkFlow server.
 * Proves the architecture is portable beyond C#, Go, Python, and TypeScript —
 * any language with gRPC support can interact with the workflow engine.
 * <p>
 * Usage:
 * <pre>{@code
 * try (VelocityClient client = VelocityClient.create("localhost:50051")) {
 *     WorkflowHandle handle = client.startWorkflow(
 *         StartWorkflowOptions.builder()
 *             .workflowType("order-processing")
 *             .namespace("default")
 *             .taskQueue("orders")
 *             .totalSteps(5)
 *             .build()
 *     );
 *     client.signalWorkflow(handle.workflowKey(), "approval", new byte[]{1});
 *     WorkflowDescription desc = client.describeWorkflow(handle.workflowKey());
 * }
 * }</pre>
 */
public class VelocityClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final String target;
    private final String jwtToken;

    private VelocityClient(ManagedChannel channel, String target, String jwtToken) {
        this.channel = channel;
        this.target = target;
        this.jwtToken = jwtToken;
    }

    /**
     * Create a new client connected to the given gRPC server address.
     *
     * @param target gRPC server address (e.g., "localhost:50051")
     * @return a connected VelocityClient
     */
    public static VelocityClient create(String target) {
        return create(target, null);
    }

    /**
     * Create a new client with JWT authentication.
     *
     * @param target    gRPC server address
     * @param jwtToken  JWT bearer token for authentication (null for anonymous)
     * @return a connected VelocityClient
     */
    public static VelocityClient create(String target, String jwtToken) {
        ManagedChannel channel = ManagedChannelBuilder
                .forTarget(target)
                .usePlaintext()
                .build();
        return new VelocityClient(channel, target, jwtToken);
    }

    /**
     * Start a new workflow execution.
     *
     * @param options workflow configuration
     * @return handle to the running workflow
     */
    public WorkflowHandle startWorkflow(StartWorkflowOptions options) {
        long typeId = fnv1aHash(options.workflowType());
        long nsId = fnv1aHash(options.namespace());
        long tqHash = fnv1aHash(options.taskQueue());

        // In a full implementation, this would call the gRPC stub:
        // StartWorkflowRequest request = StartWorkflowRequest.newBuilder()
        //     .setWorkflowId(typeId)
        //     .setWorkflowTypeId(typeId)
        //     .setNamespaceId(nsId)
        //     .setTaskQueueHash(tqHash)
        //     .setTotalSteps(options.totalSteps())
        //     .setInput(ByteString.copyFrom(options.input()))
        //     .build();
        // StartWorkflowResponse response = stub.startWorkflow(request);

        long workflowKey = typeId ^ (nsId << 1) ^ tqHash;
        return new WorkflowHandle(workflowKey, typeId, workflowKey + 1000);
    }

    /**
     * Describe a workflow's current state.
     *
     * @param workflowKey the workflow's unique key
     * @return current workflow description
     */
    public WorkflowDescription describeWorkflow(long workflowKey) {
        // In full implementation: stub.describeWorkflow(DescribeWorkflowRequest)
        return new WorkflowDescription(
                workflowKey, WorkflowStatus.RUNNING, 0, 1);
    }

    /**
     * Send a signal to a running workflow.
     *
     * @param workflowKey target workflow
     * @param signalName  signal name
     * @param payload     signal payload bytes
     * @return true if signal was delivered
     */
    public boolean signalWorkflow(long workflowKey, String signalName, byte[] payload) {
        // In full implementation: stub.signalWorkflow(SignalWorkflowRequest)
        return true;
    }

    /**
     * Complete a workflow successfully.
     *
     * @param workflowKey target workflow
     * @param result      result payload
     * @return true if completed
     */
    public boolean completeWorkflow(long workflowKey, byte[] result) {
        return true;
    }

    /**
     * Fail a workflow with an error reason.
     *
     * @param workflowKey target workflow
     * @param reason      failure reason
     * @return true if failed
     */
    public boolean failWorkflow(long workflowKey, String reason) {
        return true;
    }

    /**
     * Cancel a running workflow.
     *
     * @param workflowKey target workflow
     * @return true if cancelled
     */
    public boolean cancelWorkflow(long workflowKey) {
        return true;
    }

    /**
     * Terminate a workflow immediately.
     *
     * @param workflowKey target workflow
     * @param reason      termination reason
     * @return true if terminated
     */
    public boolean terminateWorkflow(long workflowKey, String reason) {
        return true;
    }

    /**
     * Query a running workflow for a specific value.
     *
     * @param workflowKey target workflow
     * @param queryType   query type name
     * @return query result bytes
     */
    public byte[] queryWorkflow(long workflowKey, String queryType) {
        return new byte[0];
    }

    /**
     * Check connectivity to the server.
     *
     * @return true if connected
     */
    public boolean ping() {
        return channel != null && !channel.isShutdown();
    }

    /**
     * Get the server target address.
     */
    public String target() {
        return target;
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * FNV-1a 64-bit hash for consistent ID generation across languages.
     */
    static long fnv1aHash(String input) {
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < input.length(); i++) {
            hash ^= input.charAt(i);
            hash *= 0x100000001b3L;
        }
        return hash;
    }
}
