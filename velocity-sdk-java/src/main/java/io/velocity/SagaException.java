package io.velocity;

import java.util.List;

/**
 * Exception thrown when a saga step fails.
 */
public class SagaException extends Exception {
    private final List<Object> partialResults;

    public SagaException(String message, Throwable cause, List<Object> partialResults) {
        super(message, cause);
        this.partialResults = partialResults;
    }

    public List<Object> getPartialResults() { return partialResults; }
}
