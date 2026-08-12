package io.velocity;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import java.util.*;

/**
 * Comprehensive tests for the V.E.L.O.C.I.T.Y.-WorkFlow Java SDK.
 */
public class VelocitySdkTest {

    @Before
    public void setUp() {
        clearWorkflowRegistry();
        clearActivityRegistry();
    }

    private void clearWorkflowRegistry() {
        try {
            var field = WorkflowRegistry.class.getDeclaredField("registry");
            field.setAccessible(true);
            ((Map<?, ?>) field.get(null)).clear();
        } catch (Exception e) { /* ignore */ }
    }

    private void clearActivityRegistry() {
        try {
            var field = ActivityRegistry.class.getDeclaredField("registry");
            field.setAccessible(true);
            ((Map<?, ?>) field.get(null)).clear();
        } catch (Exception e) { /* ignore */ }
    }

    // ─── Workflow Registry Tests ──────────────────────────────────────────────

    @Test
    public void testWorkflowRegistration() {
        WorkflowRegistry.registerWorkflow("test-wf", (ctx, input) -> "result");
        assertTrue(WorkflowRegistry.hasWorkflow("test-wf"));
        assertNotNull(WorkflowRegistry.getWorkflow("test-wf"));
    }

    @Test
    public void testWorkflowNotRegistered() {
        assertFalse(WorkflowRegistry.hasWorkflow("nonexistent"));
        assertNull(WorkflowRegistry.getWorkflow("nonexistent"));
    }

    @Test
    public void testWorkflowExecution() {
        WorkflowRegistry.registerWorkflow("greet-wf", (ctx, input) -> "Hello, " + input);
        WorkflowContext ctx = new WorkflowContext("wf-1", "run-1", "greet-wf", "test-queue");
        Object result = WorkflowRegistry.executeWorkflow("greet-wf", ctx, "World");
        assertEquals("Hello, World", result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWorkflowExecutionNotRegistered() {
        WorkflowContext ctx = new WorkflowContext("wf-1", "run-1", "missing", "test-queue");
        WorkflowRegistry.executeWorkflow("missing", ctx, null);
    }

    // ─── Activity Registry Tests ──────────────────────────────────────────────

    @Test
    public void testActivityRegistration() {
        ActivityRegistry.registerActivity("test-act", (ctx, input) -> "activity-result");
        assertTrue(ActivityRegistry.hasActivity("test-act"));
        assertNotNull(ActivityRegistry.getActivity("test-act"));
    }

    @Test
    public void testActivityNotRegistered() {
        assertFalse(ActivityRegistry.hasActivity("nonexistent"));
        assertNull(ActivityRegistry.getActivity("nonexistent"));
    }

    @Test
    public void testActivityExecution() {
        ActivityRegistry.registerActivity("multiply", (ctx, input) -> {
            int val = (Integer) input;
            return val * 2;
        });
        ActivityContext ctx = new ActivityContext("act-1", "multiply", "test-queue", "wf-1", "run-1");
        Object result = ActivityRegistry.executeActivity("multiply", ctx, 21);
        assertEquals(42, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testActivityExecutionNotRegistered() {
        ActivityContext ctx = new ActivityContext("act-1", "missing", "test-queue", "wf-1", "run-1");
        ActivityRegistry.executeActivity("missing", ctx, null);
    }

    // ─── WorkflowContext Tests ────────────────────────────────────────────────

    @Test
    public void testWorkflowContext() {
        WorkflowContext ctx = new WorkflowContext("wf-123", "run-456", "MyWorkflow", "my-queue");
        assertEquals("wf-123", ctx.getWorkflowId());
        assertEquals("run-456", ctx.getRunId());
        assertEquals("MyWorkflow", ctx.getWorkflowType());
        assertEquals("my-queue", ctx.getTaskQueue());
        assertEquals(1, ctx.getAttempt());
    }

    @Test
    public void testWorkflowContextMemo() {
        WorkflowContext ctx = new WorkflowContext("wf-1", "run-1", "wf", "q");
        Map<String, Object> memo = new HashMap<>();
        memo.put("key", "value");
        ctx.setMemo(memo);
        assertEquals("value", ctx.getMemo().get("key"));
    }

    @Test
    public void testWorkflowContextAttempt() {
        WorkflowContext ctx = new WorkflowContext("wf-1", "run-1", "wf", "q");
        assertEquals(1, ctx.getAttempt());
        ctx.setAttempt(3);
        assertEquals(3, ctx.getAttempt());
    }

    // ─── WorkflowOptions Tests ────────────────────────────────────────────────

    @Test
    public void testWorkflowOptionsBuilder() {
        WorkflowOptions opts = new WorkflowOptions()
            .setWorkflowId("wf-1")
            .setWorkflowType("TestWorkflow")
            .setTaskQueue("test-queue")
            .setInput("hello")
            .setExecutionTimeout(5000L)
            .setRunTimeout(10000L);

        assertEquals("wf-1", opts.getWorkflowId());
        assertEquals("TestWorkflow", opts.getWorkflowType());
        assertEquals("test-queue", opts.getTaskQueue());
        assertEquals("hello", opts.getInput());
        assertEquals(Long.valueOf(5000), opts.getExecutionTimeout());
        assertEquals(Long.valueOf(10000), opts.getRunTimeout());
    }

    // ─── WorkflowExecution Tests ──────────────────────────────────────────────

    @Test
    public void testWorkflowExecutionObject() {
        WorkflowExecution exec = new WorkflowExecution(
            "wf-1", "run-1", "TestWorkflow", "test-queue",
            WorkflowStatus.RUNNING, System.currentTimeMillis()
        );
        assertEquals("wf-1", exec.getWorkflowId());
        assertEquals("run-1", exec.getRunId());
        assertEquals("TestWorkflow", exec.getWorkflowType());
        assertEquals(WorkflowStatus.RUNNING, exec.getStatus());
    }

    // ─── WorkflowStatus Tests ─────────────────────────────────────────────────

    @Test
    public void testWorkflowStatusValues() {
        assertEquals(WorkflowStatus.RUNNING, WorkflowStatus.valueOf("RUNNING"));
        assertEquals(WorkflowStatus.COMPLETED, WorkflowStatus.valueOf("COMPLETED"));
        assertEquals(WorkflowStatus.FAILED, WorkflowStatus.valueOf("FAILED"));
        assertEquals(WorkflowStatus.CANCELED, WorkflowStatus.valueOf("CANCELED"));
        assertEquals(WorkflowStatus.TERMINATED, WorkflowStatus.valueOf("TERMINATED"));
    }

    // ─── RetryPolicy Tests ────────────────────────────────────────────────────

    @Test
    public void testRetryPolicy() {
        RetryPolicy policy = new RetryPolicy()
            .setInitialInterval(1000L)
            .setBackoffCoefficient(2.0)
            .setMaximumInterval(30000L)
            .setMaximumAttempts(5);

        assertEquals(1000L, policy.getInitialInterval());
        assertEquals(2.0, policy.getBackoffCoefficient(), 0.001);
        assertEquals(Long.valueOf(30000), policy.getMaximumInterval());
        assertEquals(5, policy.getMaximumAttempts());
    }

    // ─── Advanced Features Tests ──────────────────────────────────────────────

    @Test
    public void testUpdateOptions() {
        UpdateOptions opts = new UpdateOptions("update-status")
            .setArgs(Map.of("status", "active"))
            .setWaitPolicy("COMPLETED");

        assertEquals("update-status", opts.getUpdateName());
        assertEquals("COMPLETED", opts.getWaitPolicy());
    }

    @Test
    public void testUpdateResult() {
        UpdateResult result = new UpdateResult("update-1", "ACCEPTED", "ok");
        assertEquals("update-1", result.getUpdateId());
        assertEquals("ACCEPTED", result.getStatus());
        assertEquals("ok", result.getResult());
    }

    @Test
    public void testContinueAsNewException() {
        ContinueAsNewException ex = new ContinueAsNewException("LongRunning", "main", Map.of("iteration", 42));
        assertEquals("LongRunning", ex.getWorkflowType());
        assertEquals("main", ex.getTaskQueue());
        assertTrue(ex.getMessage().contains("continue-as-new"));
    }

    @Test
    public void testScheduleClient() {
        ScheduleClient sc = new ScheduleClient("default");
        String id = sc.create(new ScheduleOptions("daily-report", "GenerateReport", "reports", "0 9 * * *"));
        assertEquals("daily-report", id);

        Map<String, Object> desc = sc.describe("daily-report");
        assertEquals("daily-report", desc.get("scheduleId"));

        List<Map<String, Object>> list = sc.list();
        assertNotNull(list);

        sc.delete("daily-report");
    }

    @Test
    public void testSearchAttributesClient() {
        SearchAttributesClient sac = new SearchAttributesClient("default");
        sac.upsert("wf-1", Map.of("CustomField", "value1"));
        List<Map<String, Object>> workflows = sac.listWorkflows("CustomField = 'value1'");
        assertNotNull(workflows);
        long count = sac.countWorkflows("CustomField = 'value1'");
        assertTrue(count >= 0);
    }

    @Test
    public void testBatchOperationClient() {
        BatchOperationClient bc = new BatchOperationClient("default");
        String jobId = bc.start(new BatchOperationOptions("terminate", "WorkflowType = 'Test'"));
        assertNotNull(jobId);

        Map<String, Object> desc = bc.describe(jobId);
        assertEquals(jobId, desc.get("jobId"));

        List<Map<String, Object>> list = bc.list();
        assertNotNull(list);
    }

    // ─── Saga Tests ───────────────────────────────────────────────────────────

    @Test
    public void testSagaSuccess() throws SagaException {
        Saga saga = new Saga();
        List<String> order = new ArrayList<>();

        saga.addStep("step1",
            () -> { order.add("exec-1"); return "r1"; },
            () -> order.add("comp-1")
        );
        saga.addStep("step2",
            () -> { order.add("exec-2"); return "r2"; },
            () -> order.add("comp-2")
        );

        List<Object> results = saga.execute();
        assertEquals(2, results.size());
        assertEquals("r1", results.get(0));
        assertEquals("r2", results.get(1));
        assertEquals(List.of("exec-1", "exec-2"), order);
    }

    @Test
    public void testSagaCompensation() {
        Saga saga = new Saga();
        List<String> compensated = new ArrayList<>();

        saga.addStep("step1",
            () -> "ok",
            () -> compensated.add("step1")
        );
        saga.addStep("step2-fails",
            () -> { throw new RuntimeException("step2 failed"); },
            () -> compensated.add("step2")
        );

        try {
            saga.execute();
            fail("Expected SagaException");
        } catch (SagaException e) {
            assertTrue(e.getMessage().contains("step2-fails"));
            assertEquals(1, compensated.size());
            assertEquals("step1", compensated.get(0));
        }
    }

    @Test
    public void testSagaPartialResults() {
        Saga saga = new Saga();
        saga.addStep("step1", () -> "result-1", () -> {});
        saga.addStep("step2-fails", () -> { throw new RuntimeException("fail"); }, () -> {});

        try {
            saga.execute();
            fail("Expected SagaException");
        } catch (SagaException e) {
            List<Object> partial = e.getPartialResults();
            assertEquals(1, partial.size());
            assertEquals("result-1", partial.get(0));
        }
    }

    // ─── ClientOptions Tests ──────────────────────────────────────────────────

    @Test
    public void testClientOptions() {
        ClientOptions opts = new ClientOptions()
            .setHostPort("localhost:7233")
            .setNamespace("default");
        assertEquals("localhost:7233", opts.getHostPort());
        assertEquals("default", opts.getNamespace());
        assertFalse(opts.isUseTls());
    }

    @Test
    public void testClientOptionsWithTls() {
        ClientOptions opts = new ClientOptions()
            .setHostPort("velocity.example.com:8443")
            .setNamespace("production")
            .setUseTls(true);
        assertTrue(opts.isUseTls());
    }

    // ─── WorkerOptions Tests ──────────────────────────────────────────────────

    @Test
    public void testWorkerOptions() {
        WorkerOptions opts = new WorkerOptions()
            .setHostPort("localhost:7233")
            .setNamespace("default")
            .setTaskQueue("test-queue");
        assertEquals("localhost:7233", opts.getHostPort());
        assertEquals("default", opts.getNamespace());
        assertEquals("test-queue", opts.getTaskQueue());
    }

    @Test
    public void testWorkerOptionsDefaults() {
        WorkerOptions opts = new WorkerOptions();
        assertEquals("localhost:7233", opts.getHostPort());
        assertEquals("default", opts.getNamespace());
        assertEquals(10, opts.getMaxConcurrentWorkflowTasks());
        assertEquals(10, opts.getMaxConcurrentActivityTasks());
    }

    // ─── ActivityContext Tests ────────────────────────────────────────────────

    @Test
    public void testActivityContext() {
        ActivityContext ctx = new ActivityContext("act-1", "greet", "test-queue", "wf-1", "run-1");
        assertEquals("act-1", ctx.getActivityId());
        assertEquals("greet", ctx.getActivityType());
        assertEquals("test-queue", ctx.getTaskQueue());
        assertEquals("wf-1", ctx.getWorkflowId());
        assertEquals("run-1", ctx.getRunId());
        assertEquals(1, ctx.getAttempt());
    }

    @Test
    public void testActivityContextAttempt() {
        ActivityContext ctx = new ActivityContext("act-1", "greet", "q", "wf-1", "run-1");
        ctx.setAttempt(5);
        assertEquals(5, ctx.getAttempt());
    }

    // ─── Integration: Workflow + Activity ─────────────────────────────────────

    @Test
    public void testWorkflowWithActivity() {
        ActivityRegistry.registerActivity("double", (ctx, input) -> {
            int val = (Integer) input;
            return val * 2;
        });

        WorkflowRegistry.registerWorkflow("double-wf", (ctx, input) -> {
            int val = (Integer) input;
            ActivityContext actCtx = new ActivityContext("a", "double", "q", ctx.getWorkflowId(), ctx.getRunId());
            return ActivityRegistry.executeActivity("double", actCtx, val);
        });

        WorkflowContext wfCtx = new WorkflowContext("wf-1", "run-1", "double-wf", "test-queue");
        Object result = WorkflowRegistry.executeWorkflow("double-wf", wfCtx, 21);
        assertEquals(42, result);
    }

    // ─── ResetOptions Tests ───────────────────────────────────────────────────

    @Test
    public void testResetOptions() {
        ResetOptions opts = new ResetOptions(5).setReason("testing");
        assertEquals(5, opts.getResetEventId());
        assertEquals("testing", opts.getReason());
    }

    // ─── ScheduleOptions Tests ────────────────────────────────────────────────

    @Test
    public void testScheduleOptions() {
        ScheduleOptions opts = new ScheduleOptions("sched-1", "ReportWorkflow", "reports", "0 9 * * *")
            .setEnabled(true);
        assertEquals("sched-1", opts.getScheduleId());
        assertEquals("ReportWorkflow", opts.getWorkflowType());
        assertEquals("reports", opts.getTaskQueue());
        assertEquals("0 9 * * *", opts.getCronSchedule());
        assertTrue(opts.isEnabled());
    }
}
