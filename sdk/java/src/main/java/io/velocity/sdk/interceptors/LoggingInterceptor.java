package io.velocity.sdk.interceptors;

import java.util.logging.Logger;

/**
 * Logs workflow lifecycle events.
 */
public class LoggingInterceptor implements WorkflowInterceptor {

    private static final Logger logger = Logger.getLogger(LoggingInterceptor.class.getName());
    private final String prefix;

    /**
     * Create a new LoggingInterceptor with default prefix.
     */
    public LoggingInterceptor() {
        this("[VELOCITY]");
    }

    /**
     * Create a new LoggingInterceptor with a custom prefix.
     *
     * @param prefix log message prefix
     */
    public LoggingInterceptor(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void onStart(String workflowType, long workflowId) {
        logger.info(String.format("%s Workflow started: type=%s, id=%d", prefix, workflowType, workflowId));
    }

    @Override
    public void onComplete(long workflowId, byte[] result) {
        logger.info(String.format("%s Workflow completed: id=%d", prefix, workflowId));
    }

    @Override
    public void onFail(long workflowId, Throwable error) {
        logger.severe(String.format("%s Workflow failed: id=%d, error=%s", prefix, workflowId, error.getMessage()));
    }

    @Override
    public void onSignal(long workflowId, String signalName) {
        logger.info(String.format("%s Workflow signal: id=%d, signal=%s", prefix, workflowId, signalName));
    }
}
