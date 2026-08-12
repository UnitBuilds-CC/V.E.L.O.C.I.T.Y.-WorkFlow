package io.velocity.sdk;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Workflow Update API — synchronous workflow mutation.
 *
 * <p>Unlike signals (fire-and-forget), updates provide:
 * <ul>
 *   <li>Synchronous request/response semantics</li>
 *   <li>Wait policies (Accepted, Completed, Admitted)</li>
 *   <li>Validation before execution</li>
 *   <li>Named update handlers registered by workflows</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * UpdateClient client = new UpdateClient("localhost:50051");
 * client.registerHandler("setAmount", args -> args, null);
 * UpdateResult result = client.executeUpdate(42, "setAmount", args, UpdateWaitPolicy.COMPLETED);
 * }</pre>
 */
public class UpdateClient {

    public enum UpdateStatus {
        ADMITTED(0), ACCEPTED(1), COMPLETED(2), REJECTED(3);
        public final int code;
        UpdateStatus(int code) { this.code = code; }
    }

    public enum UpdateWaitPolicy {
        ADMITTED(0), ACCEPTED(1), COMPLETED(2);
        public final int code;
        UpdateWaitPolicy(int code) { this.code = code; }
    }

    public static class UpdateRequest {
        public final long workflowKey;
        public final String updateId;
        public final String updateName;
        public final Object args;
        public final UpdateWaitPolicy waitPolicy;

        public UpdateRequest(long workflowKey, String updateId, String updateName,
                             Object args, UpdateWaitPolicy waitPolicy) {
            this.workflowKey = workflowKey;
            this.updateId = updateId;
            this.updateName = updateName;
            this.args = args;
            this.waitPolicy = waitPolicy;
        }
    }

    public static class UpdateResult {
        public final String updateId;
        public final UpdateStatus status;
        public final Object result;
        public final String error;
        public final double durationMs;

        public UpdateResult(String updateId, UpdateStatus status, Object result,
                            String error, double durationMs) {
            this.updateId = updateId;
            this.status = status;
            this.result = result;
            this.error = error;
            this.durationMs = durationMs;
        }
    }

    private static class HandlerEntry {
        final String name;
        final Function<Object, Object> handler;
        final Predicate<Object> validator;

        HandlerEntry(String name, Function<Object, Object> handler, Predicate<Object> validator) {
            this.name = name;
            this.handler = handler;
            this.validator = validator;
        }
    }

    private final String serverAddress;
    private final Map<String, HandlerEntry> handlers = new ConcurrentHashMap<>();
    private final Map<String, UpdateResult> pending = new ConcurrentHashMap<>();

    public UpdateClient(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public UpdateClient() {
        this("localhost:50051");
    }

    /** Register a named update handler. */
    public void registerHandler(String name, Function<Object, Object> handler,
                                Predicate<Object> validator) {
        handlers.put(name, new HandlerEntry(name, handler, validator));
    }

    /** Execute a workflow update. */
    public UpdateResult executeUpdate(long workflowKey, String updateName,
                                      Object args, UpdateWaitPolicy waitPolicy) {
        String uid = "update-" + workflowKey + "-" + System.currentTimeMillis();
        long start = System.nanoTime();

        HandlerEntry entry = handlers.get(updateName);
        if (entry == null) {
            UpdateResult result = new UpdateResult(uid, UpdateStatus.REJECTED, null,
                    "No handler registered for update '" + updateName + "'", elapsed(start));
            pending.put(uid, result);
            return result;
        }

        if (entry.validator != null && !entry.validator.test(args)) {
            UpdateResult result = new UpdateResult(uid, UpdateStatus.REJECTED, null,
                    "Update validation failed", elapsed(start));
            pending.put(uid, result);
            return result;
        }

        try {
            Object value = entry.handler.apply(args);
            UpdateResult result = new UpdateResult(uid, UpdateStatus.COMPLETED, value,
                    null, elapsed(start));
            pending.put(uid, result);
            return result;
        } catch (Exception e) {
            UpdateResult result = new UpdateResult(uid, UpdateStatus.REJECTED, null,
                    e.getMessage(), elapsed(start));
            pending.put(uid, result);
            return result;
        }
    }

    /** Get the result of a previously executed update. */
    public UpdateResult getUpdateResult(String updateId) {
        return pending.get(updateId);
    }

    /** List registered update handler names. */
    public List<String> listHandlers() {
        return new ArrayList<>(handlers.keySet());
    }

    /** List pending update IDs. */
    public List<String> listPending() {
        return new ArrayList<>(pending.keySet());
    }

    private double elapsed(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000.0;
    }
}
