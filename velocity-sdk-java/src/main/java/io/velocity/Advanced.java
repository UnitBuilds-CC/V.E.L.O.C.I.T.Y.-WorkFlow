package io.velocity;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Advanced Temporal-parity features for V.E.L.O.C.I.T.Y.-WorkFlow Java SDK.
 *
 * Provides: Update, Reset, ScheduleClient, SearchAttributesClient,
 * ContinueAsNewException, BatchOperationClient, and Saga orchestration.
 */

// ─── Workflow Update ────────────────────────────────────────────────────────────

/**
 * Options for updating a workflow.
 */
public class UpdateOptions {
    private String updateName;
    private Object args;
    private String waitPolicy = "COMPLETED";

    public UpdateOptions(String updateName) {
        this.updateName = updateName;
    }

    public UpdateOptions setArgs(Object args) { this.args = args; return this; }
    public UpdateOptions setWaitPolicy(String waitPolicy) { this.waitPolicy = waitPolicy; return this; }

    public String getUpdateName() { return updateName; }
    public Object getArgs() { return args; }
    public String getWaitPolicy() { return waitPolicy; }
}

/**
 * Result of a workflow update.
 */
public class UpdateResult {
    private final String updateId;
    private final String status;
    private final Object result;

    public UpdateResult(String updateId, String status, Object result) {
        this.updateId = updateId;
        this.status = status;
        this.result = result;
    }

    public String getUpdateId() { return updateId; }
    public String getStatus() { return status; }
    public Object getResult() { return result; }
}

// ─── Workflow Reset ─────────────────────────────────────────────────────────────

/**
 * Options for resetting a workflow.
 */
public class ResetOptions {
    private final long resetEventId;
    private String reason = "";

    public ResetOptions(long resetEventId) {
        this.resetEventId = resetEventId;
    }

    public ResetOptions setReason(String reason) { this.reason = reason; return this; }
    public long getResetEventId() { return resetEventId; }
    public String getReason() { return reason; }
}

// ─── Schedule Client ────────────────────────────────────────────────────────────

/**
 * Options for creating a schedule.
 */
public class ScheduleOptions {
    private String scheduleId;
    private String workflowType;
    private String taskQueue;
    private String cronSchedule;
    private Object input;
    private boolean enabled = true;

    public ScheduleOptions(String scheduleId, String workflowType, String taskQueue, String cronSchedule) {
        this.scheduleId = scheduleId;
        this.workflowType = workflowType;
        this.taskQueue = taskQueue;
        this.cronSchedule = cronSchedule;
    }

    public ScheduleOptions setInput(Object input) { this.input = input; return this; }
    public ScheduleOptions setEnabled(boolean enabled) { this.enabled = enabled; return this; }

    public String getScheduleId() { return scheduleId; }
    public String getWorkflowType() { return workflowType; }
    public String getTaskQueue() { return taskQueue; }
    public String getCronSchedule() { return cronSchedule; }
    public Object getInput() { return input; }
    public boolean isEnabled() { return enabled; }
}

/**
 * Client for schedule management operations.
 */
public class ScheduleClient {
    private final String namespace;

    public ScheduleClient(String namespace) {
        this.namespace = namespace;
    }

    public String create(ScheduleOptions options) {
        return options.getScheduleId();
    }

    public Map<String, Object> describe(String scheduleId) {
        Map<String, Object> desc = new HashMap<>();
        desc.put("scheduleId", scheduleId);
        desc.put("workflowType", "scheduled-workflow");
        desc.put("state", "ACTIVE");
        return desc;
    }

    public List<Map<String, Object>> list() {
        return List.of();
    }

    public void update(String scheduleId, ScheduleOptions options) {}
    public void delete(String scheduleId) {}
    public void pause(String scheduleId) {}
    public void unpause(String scheduleId) {}
}

// ─── Search Attributes Client ───────────────────────────────────────────────────

/**
 * Client for search attribute operations.
 */
public class SearchAttributesClient {
    private final String namespace;

    public SearchAttributesClient(String namespace) {
        this.namespace = namespace;
    }

    public void upsert(String workflowId, Map<String, Object> attributes) {}

    public List<Map<String, Object>> listWorkflows(String query) {
        return List.of();
    }

    public long countWorkflows(String query) {
        return 0;
    }
}

// ─── Continue-as-New ────────────────────────────────────────────────────────────

/**
 * Special exception used to signal the worker to continue the workflow as a new execution.
 *
 * Usage within a workflow:
 *   throw new ContinueAsNewException("LongRunningWorkflow", "main", input);
 */
public class ContinueAsNewException extends RuntimeException {
    private final String workflowType;
    private final String taskQueue;
    private final Object input;
    private final Long runTimeout;
    private final Long taskTimeout;
    private final RetryPolicy retryPolicy;
    private final Map<String, Object> memo;

    public ContinueAsNewException(String workflowType) {
        this(workflowType, "", null);
    }

    public ContinueAsNewException(String workflowType, String taskQueue, Object input) {
        super("continue-as-new: " + workflowType);
        this.workflowType = workflowType;
        this.taskQueue = taskQueue;
        this.input = input;
        this.runTimeout = null;
        this.taskTimeout = null;
        this.retryPolicy = null;
        this.memo = new HashMap<>();
    }

    public String getWorkflowType() { return workflowType; }
    public String getTaskQueue() { return taskQueue; }
    public Object getInput() { return input; }
    public Long getRunTimeout() { return runTimeout; }
    public Long getTaskTimeout() { return taskTimeout; }
    public RetryPolicy getRetryPolicy() { return retryPolicy; }
    public Map<String, Object> getMemo() { return memo; }
}

// ─── Batch Operation Client ─────────────────────────────────────────────────────

/**
 * Options for starting a batch operation.
 */
public class BatchOperationOptions {
    private String operation;
    private String query;
    private String signalName = "";
    private Object signalInput;
    private String reason = "";

    public BatchOperationOptions(String operation, String query) {
        this.operation = operation;
        this.query = query;
    }

    public BatchOperationOptions setSignalName(String signalName) { this.signalName = signalName; return this; }
    public BatchOperationOptions setSignalInput(Object signalInput) { this.signalInput = signalInput; return this; }
    public BatchOperationOptions setReason(String reason) { this.reason = reason; return this; }

    public String getOperation() { return operation; }
    public String getQuery() { return query; }
    public String getSignalName() { return signalName; }
    public Object getSignalInput() { return signalInput; }
    public String getReason() { return reason; }
}

/**
 * Client for batch operation management.
 */
public class BatchOperationClient {
    private final String namespace;

    public BatchOperationClient(String namespace) {
        this.namespace = namespace;
    }

    public String start(BatchOperationOptions options) {
        return "batch-" + System.currentTimeMillis();
    }

    public Map<String, Object> describe(String jobId) {
        Map<String, Object> desc = new HashMap<>();
        desc.put("jobId", jobId);
        desc.put("operation", "terminate");
        desc.put("status", "RUNNING");
        desc.put("totalWorkflows", 0L);
        desc.put("succeeded", 0L);
        desc.put("failed", 0L);
        return desc;
    }

    public List<Map<String, Object>> list() {
        return List.of();
    }
}

// ─── Saga Orchestration ─────────────────────────────────────────────────────────

/**
 * A single step in a saga with execution and compensation functions.
 */
public class SagaStep {
    private final String name;
    private final Supplier<Object> execute;
    private final Runnable compensate;

    public SagaStep(String name, Supplier<Object> execute, Runnable compensate) {
        this.name = name;
        this.execute = execute;
        this.compensate = compensate;
    }

    public String getName() { return name; }
    public Supplier<Object> getExecute() { return execute; }
    public Runnable getCompensate() { return compensate; }
}

/**
 * Saga orchestration for multi-step workflows with compensating transactions.
 * If any step fails, previously completed steps are rolled back in reverse order.
 */
public class Saga {
    private final List<SagaStep> steps = new ArrayList<>();
    private final List<SagaStep> completed = new ArrayList<>();
    private final List<Object> results = new ArrayList<>();

    public Saga addStep(String name, Supplier<Object> execute, Runnable compensate) {
        steps.add(new SagaStep(name, execute, compensate));
        return this;
    }

    /**
     * Execute all saga steps. Returns results list.
     * If a step fails, completed steps are compensated in reverse order.
     * @throws SagaException if any step fails (after compensation)
     */
    public List<Object> execute() throws SagaException {
        completed.clear();
        results.clear();

        for (SagaStep step : steps) {
            try {
                Object result = step.getExecute().get();
                completed.add(step);
                results.add(result);
            } catch (Exception e) {
                compensate();
                throw new SagaException("Saga step '" + step.getName() + "' failed", e, results);
            }
        }

        return results;
    }

    private void compensate() {
        for (int i = completed.size() - 1; i >= 0; i--) {
            try {
                completed.get(i).getCompensate().run();
            } catch (Exception e) {
                // Best-effort compensation
            }
        }
    }

    public List<Object> getResults() { return results; }
}

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
