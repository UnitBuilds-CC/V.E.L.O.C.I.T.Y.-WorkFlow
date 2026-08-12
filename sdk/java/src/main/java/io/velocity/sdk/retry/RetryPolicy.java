package io.velocity.sdk.retry;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Retry policy with exponential backoff and jitter.
 *
 * <p>Usage:
 * <pre>{@code
 * RetryPolicy policy = RetryPolicy.builder()
 *     .maxAttempts(5)
 *     .initialIntervalMs(100)
 *     .backoffCoefficient(2.0)
 *     .maxIntervalMs(10_000)
 *     .jitter(true)
 *     .addRetryableException(IOException.class)
 *     .build();
 *
 * String result = policy.execute(() -> fetchRemoteData());
 * }</pre>
 */
public class RetryPolicy {

    private final int maxAttempts;
    private final long initialIntervalMs;
    private final double backoffCoefficient;
    private final long maxIntervalMs;
    private final boolean jitter;
    private final Set<Class<? extends Exception>> retryableExceptions;

    private RetryPolicy(Builder builder) {
        this.maxAttempts = builder.maxAttempts;
        this.initialIntervalMs = builder.initialIntervalMs;
        this.backoffCoefficient = builder.backoffCoefficient;
        this.maxIntervalMs = builder.maxIntervalMs;
        this.jitter = builder.jitter;
        this.retryableExceptions = builder.retryableExceptions;
        validate();
    }

    /** Create a new builder for RetryPolicy. */
    public static Builder builder() {
        return new Builder();
    }

    /** Create a default retry policy (3 attempts, 100ms initial, 2x backoff). */
    public static RetryPolicy defaults() {
        return builder().build();
    }

    /**
     * Execute a callable with retry logic.
     *
     * @param callable the operation to execute
     * @param <T>      the return type
     * @return the result of the callable
     * @throws Exception the last exception if all retries fail
     */
    public <T> T execute(Callable<T> callable) throws Exception {
        validate();
        Exception lastException = null;

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                return callable.call();
            } catch (Exception e) {
                lastException = e;

                if (!isRetryable(e)) {
                    throw e;
                }

                if (attempt < maxAttempts - 1) {
                    long backoff = calculateBackoff(attempt);
                    try {
                        TimeUnit.MILLISECONDS.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                }
            }
        }

        throw lastException;
    }

    /**
     * Execute a runnable with retry logic (void return).
     *
     * @param runnable the operation to execute
     * @throws Exception the last exception if all retries fail
     */
    public void execute(Runnable runnable) throws Exception {
        execute(() -> {
            runnable.run();
            return null;
        });
    }

    /** Calculate backoff duration for a given attempt. */
    public long calculateBackoff(int attempt) {
        double interval = initialIntervalMs * Math.pow(backoffCoefficient, attempt);
        interval = Math.min(interval, maxIntervalMs);

        if (jitter) {
            interval = ThreadLocalRandom.current().nextDouble() * interval;
        }

        return (long) interval;
    }

    /** Check if an exception is retryable. */
    public boolean isRetryable(Exception e) {
        if (retryableExceptions.isEmpty()) {
            return true; // retry all by default
        }
        for (Class<? extends Exception> cls : retryableExceptions) {
            if (cls.isInstance(e)) {
                return true;
            }
        }
        return false;
    }

    private void validate() {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        if (initialIntervalMs <= 0) throw new IllegalArgumentException("initialIntervalMs must be > 0");
        if (backoffCoefficient < 1.0) throw new IllegalArgumentException("backoffCoefficient must be >= 1.0");
        if (maxIntervalMs < initialIntervalMs) throw new IllegalArgumentException("maxIntervalMs must be >= initialIntervalMs");
    }

    // ─── Getters ───────────────────────────────────────────────────────────────

    public int getMaxAttempts() { return maxAttempts; }
    public long getInitialIntervalMs() { return initialIntervalMs; }
    public double getBackoffCoefficient() { return backoffCoefficient; }
    public long getMaxIntervalMs() { return maxIntervalMs; }
    public boolean isJitter() { return jitter; }

    // ─── Builder ───────────────────────────────────────────────────────────────

    public static class Builder {
        private int maxAttempts = 3;
        private long initialIntervalMs = 100;
        private double backoffCoefficient = 2.0;
        private long maxIntervalMs = 60_000;
        private boolean jitter = true;
        private final Set<Class<? extends Exception>> retryableExceptions = new HashSet<>();

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder initialIntervalMs(long initialIntervalMs) {
            this.initialIntervalMs = initialIntervalMs;
            return this;
        }

        public Builder backoffCoefficient(double backoffCoefficient) {
            this.backoffCoefficient = backoffCoefficient;
            return this;
        }

        public Builder maxIntervalMs(long maxIntervalMs) {
            this.maxIntervalMs = maxIntervalMs;
            return this;
        }

        public Builder jitter(boolean jitter) {
            this.jitter = jitter;
            return this;
        }

        public Builder addRetryableException(Class<? extends Exception> exceptionClass) {
            this.retryableExceptions.add(exceptionClass);
            return this;
        }

        public RetryPolicy build() {
            return new RetryPolicy(this);
        }
    }
}
