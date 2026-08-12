package io.velocity.sdk.stub;

import io.velocity.sdk.VelocityClient;
import io.velocity.sdk.WorkflowHandle;
import io.velocity.sdk.WorkflowDescription;
import io.velocity.sdk.codec.PayloadCodec;
import io.velocity.sdk.codec.JsonPayloadCodec;

/**
 * Typed workflow execution stub.
 *
 * <p>Provides a convenient interface for starting, signaling, querying,
 * and waiting for workflow results with automatic payload encoding/decoding.
 *
 * <p>Usage:
 * <pre>{@code
 * WorkflowStub stub = new WorkflowStub(client, "order-processing")
 *     .namespace("default")
 *     .taskQueue("orders")
 *     .codec(new JsonPayloadCodec());
 *
 * stub.start(Map.of("orderId", "12345"));
 * stub.signal("approve", Map.of("approved", true));
 * Object result = stub.result(String.class);
 * }</pre>
 */
public class WorkflowStub {

    private final VelocityClient client;
    private final String workflowType;
    private String namespace = "default";
    private String taskQueue = "default";
    private PayloadCodec codec = new JsonPayloadCodec();
    private WorkflowHandle handle;

    /**
     * Create a new WorkflowStub.
     *
     * @param client       the Velocity client
     * @param workflowType the workflow type name
     */
    public WorkflowStub(VelocityClient client, String workflowType) {
        this.client = client;
        this.workflowType = workflowType;
    }

    /** Set the namespace. Returns this for chaining. */
    public WorkflowStub namespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    /** Set the task queue. Returns this for chaining. */
    public WorkflowStub taskQueue(String taskQueue) {
        this.taskQueue = taskQueue;
        return this;
    }

    /** Set the payload codec. Returns this for chaining. */
    public WorkflowStub codec(PayloadCodec codec) {
        this.codec = codec;
        return this;
    }

    /**
     * Start workflow execution.
     *
     * @param input input data (will be encoded via codec)
     * @return this stub for chaining
     * @throws Exception if start fails
     */
    public WorkflowStub start(Object input) throws Exception {
        byte[] payload = input != null ? codec.encode(input) : new byte[0];
        this.handle = client.startWorkflow(workflowType, namespace, taskQueue, payload);
        return this;
    }

    /**
     * Start workflow execution with no input.
     *
     * @return this stub for chaining
     * @throws Exception if start fails
     */
    public WorkflowStub start() throws Exception {
        return start(null);
    }

    /**
     * Send a signal to the workflow.
     *
     * @param signalName name of the signal
     * @param data       signal payload (will be encoded)
     * @throws Exception if signal fails
     */
    public void signal(String signalName, Object data) throws Exception {
        ensureStarted();
        byte[] payload = data != null ? codec.encode(data) : new byte[0];
        client.signalWorkflow(handle.getWorkflowKey(), signalName, payload);
    }

    /**
     * Send a signal with no payload.
     *
     * @param signalName name of the signal
     * @throws Exception if signal fails
     */
    public void signal(String signalName) throws Exception {
        signal(signalName, null);
    }

    /**
     * Query the workflow state.
     *
     * @param queryType type of query
     * @param args      query arguments (will be encoded)
     * @param resultType expected result type
     * @param <T>       result type
     * @return decoded query result
     * @throws Exception if query fails
     */
    public <T> T query(String queryType, Object args, Class<T> resultType) throws Exception {
        ensureStarted();
        byte[] payload = args != null ? codec.encode(args) : new byte[0];
        byte[] result = client.queryWorkflow(handle.getWorkflowKey(), queryType, payload);
        return result != null ? codec.decode(result, resultType) : null;
    }

    /**
     * Wait for workflow completion and return the result.
     *
     * @param resultType expected result type
     * @param <T>        result type
     * @return decoded workflow result
     * @throws Exception if wait fails
     */
    public <T> T result(Class<T> resultType) throws Exception {
        ensureStarted();
        WorkflowDescription desc = client.waitForCompletion(handle.getWorkflowKey());
        if (desc != null && desc.getResult() != null) {
            return codec.decode(desc.getResult(), resultType);
        }
        return null;
    }

    /**
     * Cancel the workflow.
     *
     * @throws Exception if cancel fails
     */
    public void cancel() throws Exception {
        ensureStarted();
        client.cancelWorkflow(handle.getWorkflowKey());
    }

    /**
     * Terminate the workflow.
     *
     * @param reason termination reason
     * @throws Exception if terminate fails
     */
    public void terminate(String reason) throws Exception {
        ensureStarted();
        client.terminateWorkflow(handle.getWorkflowKey(), reason);
    }

    /** Get the underlying workflow handle. */
    public WorkflowHandle getHandle() {
        return handle;
    }

    /** Get the workflow key (null if not started). */
    public Long getWorkflowKey() {
        return handle != null ? handle.getWorkflowKey() : null;
    }

    private void ensureStarted() {
        if (handle == null) {
            throw new IllegalStateException("Workflow not started. Call start() first.");
        }
    }
}
