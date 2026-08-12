/**
 * VELOCITY-WorkFlow Java SDK — JNI native bridge.
 *
 * Provides direct JNI access to the VELOCITY-WorkFlow Rust engine,
 * bypassing gRPC for ultra-low-latency workflow operations.
 * This is the native layer that connects Java workflows directly
 * to the Rust slab engine via FFI.
 */
package io.velocity.sdk.jni;

import java.nio.ByteBuffer;

/**
 * JNI bridge to the VELOCITY-WorkFlow Rust engine.
 * All methods are backed by native Rust FFI exports via JNI.
 */
public class VelocityJniBridge implements AutoCloseable {

    private static boolean nativeLoaded = false;

    /** Load the native VELOCITY JNI library. */
    public static synchronized void loadNative() {
        if (!nativeLoaded) {
            System.loadLibrary("velocity_jni");
            nativeLoaded = true;
        }
    }

    /** Native engine handle (pointer to Rust VelocityEngine). */
    private long engineHandle = 0;

    /**
     * Create a new VELOCITY engine instance.
     * @return engine handle
     */
    public native long velocityEngineCreate();

    /**
     * Create a workflow on the engine.
     * @param engineHandle engine handle
     * @param workflowKey workflow key
     * @param workflowTypeId workflow type ID
     * @param namespaceId namespace ID
     * @param taskQueueId task queue ID
     * @param totalSteps total number of steps
     * @return 0 on success
     */
    public native int velocityEngineCreateWorkflow(
        long engineHandle, long workflowKey, long workflowTypeId,
        long namespaceId, long taskQueueId, int totalSteps);

    /**
     * Complete a workflow step.
     * @param engineHandle engine handle
     * @param workflowKey workflow key
     * @param stepIndex step index
     * @param data step data
     * @return 0 on success
     */
    public native int velocityEngineCompleteStep(
        long engineHandle, long workflowKey, int stepIndex, byte[] data);

    /**
     * Signal a running workflow.
     * @param engineHandle engine handle
     * @param workflowKey workflow key
     * @param signalId signal ID
     * @param data signal data
     * @return 0 on success
     */
    public native int velocityEngineSignalWorkflow(
        long engineHandle, long workflowKey, long signalId, byte[] data);

    /**
     * Query a running workflow.
     * @param engineHandle engine handle
     * @param workflowKey workflow key
     * @param queryId query ID
     * @return query result bytes
     */
    public native byte[] velocityEngineQueryWorkflow(
        long engineHandle, long workflowKey, long queryId);

    /**
     * Get workflow status.
     * @param engineHandle engine handle
     * @param workflowKey workflow key
     * @return status code (0=Running, 1=Completed, 2=Failed, 3=Cancelled)
     */
    public native int velocityEngineGetStatus(long engineHandle, long workflowKey);

    /**
     * Cancel a running workflow.
     * @param engineHandle engine handle
     * @param workflowKey workflow key
     * @return 0 on success
     */
    public native int velocityEngineCancelWorkflow(long engineHandle, long workflowKey);

    /**
     * Fail a running workflow.
     * @param engineHandle engine handle
     * @param workflowKey workflow key
     * @return 0 on success
     */
    public native int velocityEngineFailWorkflow(long engineHandle, long workflowKey);

    /**
     * Destroy the engine and free all resources.
     * @param engineHandle engine handle
     */
    public native void velocityEngineDestroy(long engineHandle);

    /**
     * Get the number of active workflows.
     * @param engineHandle engine handle
     * @return count of active workflows
     */
    public native int velocityEngineActiveCount(long engineHandle);

    // ─── Java-side convenience ────────────────────────────────────────────

    /**
     * Initialize the bridge and create an engine.
     */
    public void init() {
        loadNative();
        engineHandle = velocityEngineCreate();
    }

    /**
     * Get the native engine handle.
     */
    public long getEngineHandle() {
        return engineHandle;
    }

    @Override
    public void close() {
        if (engineHandle != 0) {
            velocityEngineDestroy(engineHandle);
            engineHandle = 0;
        }
    }
}
