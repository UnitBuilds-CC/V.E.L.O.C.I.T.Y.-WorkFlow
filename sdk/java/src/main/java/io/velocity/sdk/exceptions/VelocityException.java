package io.velocity.sdk.exceptions;

/**
 * Base exception for all VELOCITY-WorkFlow errors.
 * <p>
 * All exceptions include an error code, message, and retryable flag.
 */
public class VelocityException extends RuntimeException {

    private final int errorCode;
    private final boolean retryable;

    /**
     * Create a new VelocityException.
     *
     * @param message   error message
     * @param errorCode numeric error code
     * @param retryable whether the operation can be retried
     */
    public VelocityException(String message, int errorCode, boolean retryable) {
        super(message);
        this.errorCode = errorCode;
        this.retryable = retryable;
    }

    /**
     * Create a new VelocityException with a cause.
     *
     * @param message   error message
     * @param errorCode numeric error code
     * @param retryable whether the operation can be retried
     * @param cause     underlying cause
     */
    public VelocityException(String message, int errorCode, boolean retryable, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.retryable = retryable;
    }

    /** Get the numeric error code. */
    public int getErrorCode() {
        return errorCode;
    }

    /** Check if the operation can be retried. */
    public boolean isRetryable() {
        return retryable;
    }

    @Override
    public String toString() {
        String retry = retryable ? " (retryable)" : "";
        return String.format("VelocityException[%d]: %s%s", errorCode, getMessage(), retry);
    }
}
