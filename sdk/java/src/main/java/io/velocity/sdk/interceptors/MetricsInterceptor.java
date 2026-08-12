package io.velocity.sdk.interceptors;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks workflow metrics.
 */
public class MetricsInterceptor implements WorkflowInterceptor {

    private final AtomicLong workflowStarts = new AtomicLong(0);
    private final AtomicLong workflowCompletions = new AtomicLong(0);
    private final AtomicLong workflowFailures = new AtomicLong(0);
    private final AtomicLong signalsReceived = new AtomicLong(0);

    @Override
    public void onStart(String workflowType, long workflowId) {
        workflowStarts.incrementAndGet();
    }

    @Override
    public void onComplete(long workflowId, byte[] result) {
        workflowCompletions.incrementAndGet();
    }

    @Override
    public void onFail(long workflowId, Throwable error) {
        workflowFailures.incrementAndGet();
    }

    @Override
    public void onSignal(long workflowId, String signalName) {
        signalsReceived.incrementAndGet();
    }

    /**
     * Get current metrics snapshot.
     *
     * @return map of metric names to values
     */
    public Map<String, Long> getMetrics() {
        Map<String, Long> metrics = new HashMap<>();
        metrics.put("workflowStarts", workflowStarts.get());
        metrics.put("workflowCompletions", workflowCompletions.get());
        metrics.put("workflowFailures", workflowFailures.get());
        metrics.put("signalsReceived", signalsReceived.get());
        return metrics;
    }

    /** Reset all metrics to zero. */
    public void reset() {
        workflowStarts.set(0);
        workflowCompletions.set(0);
        workflowFailures.set(0);
        signalsReceived.set(0);
    }
}
