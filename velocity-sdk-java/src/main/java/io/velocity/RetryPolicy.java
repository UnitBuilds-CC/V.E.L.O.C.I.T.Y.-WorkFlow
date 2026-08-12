package io.velocity;

import java.util.List;
import java.util.ArrayList;

/**
 * Retry policy for workflows and activities.
 */
public class RetryPolicy {
    private long initialInterval;
    private double backoffCoefficient;
    private Long maximumInterval;
    private int maximumAttempts;
    private List<String> nonRetryableErrorTypes;

    public RetryPolicy() {
        this.initialInterval = 1000;
        this.backoffCoefficient = 2.0;
        this.maximumAttempts = 0;
        this.nonRetryableErrorTypes = new ArrayList<>();
    }

    // Builder pattern
    public RetryPolicy setInitialInterval(long initialInterval) {
        this.initialInterval = initialInterval;
        return this;
    }

    public RetryPolicy setBackoffCoefficient(double backoffCoefficient) {
        this.backoffCoefficient = backoffCoefficient;
        return this;
    }

    public RetryPolicy setMaximumInterval(Long maximumInterval) {
        this.maximumInterval = maximumInterval;
        return this;
    }

    public RetryPolicy setMaximumAttempts(int maximumAttempts) {
        this.maximumAttempts = maximumAttempts;
        return this;
    }

    public RetryPolicy setNonRetryableErrorTypes(List<String> nonRetryableErrorTypes) {
        this.nonRetryableErrorTypes = nonRetryableErrorTypes;
        return this;
    }

    // Getters
    public long getInitialInterval() { return initialInterval; }
    public double getBackoffCoefficient() { return backoffCoefficient; }
    public Long getMaximumInterval() { return maximumInterval; }
    public int getMaximumAttempts() { return maximumAttempts; }
    public List<String> getNonRetryableErrorTypes() { return nonRetryableErrorTypes; }
}
