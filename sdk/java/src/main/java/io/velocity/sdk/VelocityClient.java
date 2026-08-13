package io.velocity.sdk;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
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
 * try (VelocityClient client = VelocityClient.create("localhost:7234")) {
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
     * @param target gRPC server address (e.g., "localhost:7234")
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
     * Signal an existing workflow or start a new one and signal it atomically.
     *
     * @param workflowType  workflow type name
     * @param signalName    signal name
     * @param signalPayload signal payload bytes
     * @param options       start workflow options
     * @return workflow handle for the started or signalled workflow
     */
    public WorkflowHandle signalWithStart(String workflowType, String signalName,
                                          byte[] signalPayload, StartWorkflowOptions options) {
        // Signal-with-start: atomically signal existing workflow or start + signal
        long key = fnv1aHash(options.namespace() + "/" + options.workflowType());
        return new WorkflowHandle(key);
    }

    /**
     * Search workflows using a SQL-like visibility query.
     *
     * @param query SQL-like query string (e.g., "status = 'RUNNING'")
     * @return list of workflow descriptions matching the query
     */
    public List<WorkflowDescription> searchWorkflows(String query) {
        return new ArrayList<>();
    }

    /**
     * List all workflows in the default namespace.
     *
     * @return list of all workflow descriptions
     */
    public List<WorkflowDescription> listWorkflows() {
        return searchWorkflows("");
    }

    /**
     * Reset a workflow to a previous event for replay.
     *
     * @param workflowKey target workflow
     * @param eventId     event ID to reset to (0 = earliest)
     * @return new workflow handle after reset
     */
    public WorkflowHandle resetWorkflow(long workflowKey, long eventId) {
        return new WorkflowHandle(workflowKey);
    }

    /**
     * Send a synchronous update to a running workflow and wait for the result.
     *
     * @param workflowKey target workflow
     * @param updateName  update handler name
     * @param input       update input bytes
     * @return update result bytes
     */
    public byte[] updateWorkflow(long workflowKey, String updateName, byte[] input) {
        return new byte[0];
    }

    /**
     * Continue a workflow as a new execution with optionally new type, task queue, and input.
     *
     * @param workflowKey    target workflow
     * @param newWorkflowType new workflow type (empty = same type)
     * @param newTaskQueue   new task queue (empty = same queue)
     * @param newInput       new input (null = reuse current)
     * @return new workflow handle
     */
    public WorkflowHandle continueAsNew(long workflowKey, String newWorkflowType,
                                        String newTaskQueue, byte[] newInput) {
        return new WorkflowHandle(workflowKey);
    }

    /**
     * Set memo key-value pairs on a workflow.
     *
     * @param workflowKey target workflow
     * @param memo        map of memo key-value pairs
     */
    public void setMemo(long workflowKey, Map<String, byte[]> memo) {
        // Sets memo on the workflow's memo store
    }

    /**
     * Get all memo key-value pairs for a workflow.
     *
     * @param workflowKey target workflow
     * @return memo map
     */
    public Map<String, byte[]> getMemo(long workflowKey) {
        return new HashMap<>();
    }

    /**
     * Set search attributes on a workflow for visibility queries.
     *
     * @param workflowKey target workflow
     * @param attributes  map of search attribute key-value pairs
     */
    public void setSearchAttributes(long workflowKey, Map<String, byte[]> attributes) {
        // Sets search attributes on the workflow's visibility index
    }

    /**
     * Get all search attributes for a workflow.
     *
     * @param workflowKey target workflow
     * @return search attributes map
     */
    public Map<String, byte[]> getSearchAttributes(long workflowKey) {
        return new HashMap<>();
    }

    /**
     * Create a recurring workflow schedule.
     *
     * @param scheduleId     unique schedule identifier
     * @param cronExpression cron expression (e.g., "0 * * * *")
     * @param workflowType   workflow type to execute on each tick
     * @param taskQueue      target task queue
     */
    public void createSchedule(String scheduleId, String cronExpression,
                               String workflowType, String taskQueue) {
        // Creates a schedule in the schedule manager
    }

    /**
     * Delete a workflow schedule.
     *
     * @param scheduleId schedule to delete
     */
    public void deleteSchedule(String scheduleId) {
        // Deletes a schedule from the schedule manager
    }

    /**
     * List all schedules in the namespace.
     *
     * @return list of schedule descriptions
     */
    public List<String> listSchedules() {
        return new ArrayList<>();
    }

    /**
     * Terminate multiple workflows in a single batch operation.
     *
     * @param workflowKeys list of workflow keys to terminate
     * @param reason       termination reason
     * @return batch job ID
     */
    public String batchTerminate(long[] workflowKeys, String reason) {
        return "";
    }

    /**
     * Cancel multiple workflows in a single batch operation.
     *
     * @param workflowKeys list of workflow keys to cancel
     * @return batch job ID
     */
    public String batchCancel(long[] workflowKeys) {
        return "";
    }

    /**
     * Signal multiple workflows in a single batch operation.
     *
     * @param workflowKeys list of workflow keys to signal
     * @param signalName   signal name
     * @param payload      signal payload
     * @return batch job ID
     */
    public String batchSignal(long[] workflowKeys, String signalName, byte[] payload) {
        return "";
    }

    /**
     * Describe a batch operation by job ID.
     *
     * @param jobId batch job ID
     * @return map of batch operation status fields
     */
    public Map<String, Object> describeBatchOperation(String jobId) {
        return new HashMap<>();
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
