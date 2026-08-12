using System;
using Velocity.Workflow.Core;
using Xunit;

namespace Velocity.Workflow.Core.Tests;

/// <summary>
/// End-to-end tests that exercise the full C# → FFI → Rust path.
/// These tests verify that the thin C# wrapper correctly delegates to the Rust engine
/// for workflow lifecycle, step execution, task queue, signals, timers, and child workflows.
/// </summary>
public unsafe class RuntimeTests : IDisposable
{
    private readonly WorkflowRuntime _runtime;

    public RuntimeTests()
    {
        _runtime = new WorkflowRuntime();
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }

    [Fact]
    public void Create_Runtime_Returns_Valid_instance()
    {
        Assert.NotNull(_runtime);
        Assert.Equal(0UL, _runtime.WorkflowCount);
    }

    [Fact]
    public void Start_Workflow_Returns_NonZero_Key()
    {
        ulong key = _runtime.StartWorkflow(
            workflowId: 1, workflowTypeId: 100, namespaceId: 1,
            taskQueueHash: 42, totalSteps: 3);

        Assert.NotEqual(0UL, key);
        Assert.Equal(1UL, _runtime.WorkflowCount);
    }

    [Fact]
    public void Start_Workflow_Status_Is_Running()
    {
        ulong key = _runtime.StartWorkflow(1, 100, 1, 42, 3);
        var status = _runtime.GetStatus(key);
        Assert.Equal(WorkflowExecutionStatus.Running, status);
    }

    [Fact]
    public void Complete_Workflow_Status_Is_Completed()
    {
        ulong key = _runtime.StartWorkflow(1, 100, 1, 42, 1);
        _runtime.CompleteWorkflow(key);
        Assert.Equal(WorkflowExecutionStatus.Completed, _runtime.GetStatus(key));
    }

    [Fact]
    public void Fail_Workflow_Status_Is_Failed()
    {
        ulong key = _runtime.StartWorkflow(2, 100, 1, 42, 1);
        _runtime.FailWorkflow(key);
        Assert.Equal(WorkflowExecutionStatus.Failed, _runtime.GetStatus(key));
    }

    [Fact]
    public void Cancel_Workflow_Status_Is_Canceled()
    {
        ulong key = _runtime.StartWorkflow(3, 100, 1, 42, 1);
        _runtime.CancelWorkflow(key);
        Assert.Equal(WorkflowExecutionStatus.Canceled, _runtime.GetStatus(key));
    }

    [Fact]
    public void Terminate_Workflow_Status_Is_Terminated()
    {
        ulong key = _runtime.StartWorkflow(4, 100, 1, 42, 1);
        _runtime.TerminateWorkflow(key);
        Assert.Equal(WorkflowExecutionStatus.Terminated, _runtime.GetStatus(key));
    }

    [Fact]
    public void Step_Completion_Bitmask_Works()
    {
        ulong key = _runtime.StartWorkflow(5, 100, 1, 42, 4);

        // Initially no steps completed
        Assert.False(_runtime.IsStepCompleted(key, 0));
        Assert.False(_runtime.IsStepCompleted(key, 1));
        Assert.False(_runtime.IsStepCompleted(key, 2));
        Assert.False(_runtime.IsStepCompleted(key, 3));

        // Complete step 0
        _runtime.CompleteStep(key, 0);
        Assert.True(_runtime.IsStepCompleted(key, 0));
        Assert.False(_runtime.IsStepCompleted(key, 1));

        // Complete step 2 (out of order — bitmask supports this)
        _runtime.CompleteStep(key, 2);
        Assert.True(_runtime.IsStepCompleted(key, 2));
        Assert.False(_runtime.IsStepCompleted(key, 1));
    }

    [Fact]
    public void Step_Result_Stored_And_Retrieved()
    {
        ulong key = _runtime.StartWorkflow(6, 100, 1, 42, 2);

        byte[] result = [0xDE, 0xAD, 0xBE, 0xEF];
        _runtime.CompleteStep(key, 0, result);

        byte[]? retrieved = _runtime.GetStepResult(key, 0);
        Assert.NotNull(retrieved);
        Assert.Equal(result, retrieved);
    }

    [Fact]
    public void Schedule_Activity_And_Poll_Task()
    {
        ulong taskQueueHash = 99;
        ulong key = _runtime.StartWorkflow(7, 100, 1, taskQueueHash, 2);

        // Drain the initial WorkflowTask that StartWorkflow enqueues
        var initialTask = _runtime.PollTask(taskQueueHash);
        Assert.NotNull(initialTask);
        Assert.Equal(TaskKind.WorkflowTask, initialTask!.TaskKind);

        // Schedule an activity
        _runtime.ScheduleActivity(key, 0, activityNameId: 555);

        // Poll the task queue — should get the activity task
        var task = _runtime.PollTask(taskQueueHash);
        Assert.NotNull(task);
        Assert.Equal(TaskKind.ActivityTask, task!.TaskKind);
        Assert.Equal(key, task.WorkflowKey);
        Assert.Equal(0U, task.StepIndex);
        Assert.Equal(555UL, task.ActivityNameId);
    }

    [Fact]
    public void Poll_Task_Returns_Null_When_Empty()
    {
        var task = _runtime.PollTask(12345);
        Assert.Null(task);
    }

    [Fact]
    public void Signal_Workflow()
    {
        ulong key = _runtime.StartWorkflow(8, 100, 1, 42, 1);

        Assert.False(_runtime.HasSignal(key, 777));

        _runtime.Signal(key, 777, [0x01, 0x02]);
        Assert.True(_runtime.HasSignal(key, 777));
    }

    [Fact]
    public void Timer_Scheduling_Increments_Pending_Count()
    {
        ulong key = _runtime.StartWorkflow(9, 100, 1, 42, 2);

        ulong initialTimers = _runtime.PendingTimers;
        _runtime.ScheduleTimer(key, TimeSpan.FromHours(1));
        Assert.Equal(initialTimers + 1, _runtime.PendingTimers);
    }

    [Fact]
    public void Merkle_Verification_Succeeds_For_Valid_Slab()
    {
        ulong key = _runtime.StartWorkflow(10, 100, 1, 42, 1);
        bool valid = _runtime.VerifySlab(key);
        Assert.True(valid);
    }

    [Fact]
    public void Child_Workflow_Linked_To_Parent()
    {
        ulong parentKey = _runtime.StartWorkflow(11, 100, 1, 42, 1);
        ulong childKey = _runtime.StartChildWorkflow(parentKey, 12, 200, 42, 2);

        Assert.NotEqual(0UL, childKey);
        Assert.NotEqual(parentKey, childKey);
        Assert.Equal(2UL, _runtime.WorkflowCount);

        Assert.Equal(WorkflowExecutionStatus.Running, _runtime.GetStatus(parentKey));
        Assert.Equal(WorkflowExecutionStatus.Running, _runtime.GetStatus(childKey));
    }

    [Fact]
    public void Multiple_Workflows_Independent()
    {
        ulong key1 = _runtime.StartWorkflow(20, 100, 1, 42, 3);
        ulong key2 = _runtime.StartWorkflow(21, 101, 1, 43, 5);

        Assert.Equal(2UL, _runtime.WorkflowCount);

        _runtime.CompleteWorkflow(key1);
        Assert.Equal(WorkflowExecutionStatus.Completed, _runtime.GetStatus(key1));
        Assert.Equal(WorkflowExecutionStatus.Running, _runtime.GetStatus(key2));
    }

    [Fact]
    public void WorkflowContext_Delegates_To_Runtime()
    {
        ulong key = _runtime.StartWorkflow(30, 100, 1, 42, 3);
        var ctx = new WorkflowContext(_runtime, key);

        Assert.Equal(key, ctx.WorkflowKey);
        Assert.False(ctx.IsStepCompleted(0));

        _runtime.CompleteStep(key, 0);
        Assert.True(ctx.IsStepCompleted(0));
        Assert.False(ctx.IsStepCompleted(1));
    }

    [Fact]
    public void WorkflowContext_ExecuteStep_Schedules_Activity()
    {
        ulong taskQueueHash = 88;
        ulong key = _runtime.StartWorkflow(31, 100, 1, taskQueueHash, 2);
        var ctx = new WorkflowContext(_runtime, key);

        // Drain the initial WorkflowTask from StartWorkflow
        var initialTask = _runtime.PollTask(taskQueueHash);
        Assert.NotNull(initialTask);
        Assert.Equal(TaskKind.WorkflowTask, initialTask!.TaskKind);

        // ExecuteStepAsync schedules an activity in Rust
        var result = ctx.ExecuteStepAsync(0, "ProcessPayment");
        Assert.NotNull(result);

        // The activity should be pollable from the task queue
        var task = _runtime.PollTask(taskQueueHash);
        Assert.NotNull(task);
        Assert.Equal(TaskKind.ActivityTask, task!.TaskKind);
        Assert.Equal(key, task.WorkflowKey);
    }

    [Fact]
    public void Dispose_Is_Idempotent()
    {
        var runtime = new WorkflowRuntime();
        runtime.Dispose();
        runtime.Dispose(); // Should not throw
    }

    // ─── Namespace Tests ──────────────────────────────────────────────────────

    [Fact]
    public void Default_Namespace_Exists()
    {
        // The engine always starts with a "default" namespace (id=0)
        Assert.True(_runtime.IsNamespaceActive(0));
        Assert.True(_runtime.NamespaceCount >= 1);
    }

    [Fact]
    public void Register_Namespace_Returns_NonZero_Id()
    {
        ulong nsId = _runtime.RegisterNamespace("production");
        Assert.NotEqual(0UL, nsId);
        Assert.True(_runtime.IsNamespaceActive(nsId));
    }

    [Fact]
    public void Namespace_Count_Increases_After_Registration()
    {
        ulong initial = _runtime.NamespaceCount;
        _runtime.RegisterNamespace("staging");
        Assert.Equal(initial + 1, _runtime.NamespaceCount);
    }

    // ─── Visibility Tests ─────────────────────────────────────────────────────

    [Fact]
    public void Visibility_Count_Tracks_Workflows()
    {
        ulong initial = _runtime.VisibilityCount;
        _runtime.StartWorkflow(100, 100, 0, 42, 1);
        _runtime.StartWorkflow(101, 100, 0, 42, 1);
        Assert.Equal(initial + 2, _runtime.VisibilityCount);
    }

    [Fact]
    public void Visibility_Count_By_Status()
    {
        ulong key1 = _runtime.StartWorkflow(200, 100, 0, 42, 1);
        ulong key2 = _runtime.StartWorkflow(201, 100, 0, 42, 1);

        Assert.Equal(2UL, _runtime.CountByStatus(WorkflowExecutionStatus.Running));

        _runtime.CompleteWorkflow(key1);
        Assert.Equal(1UL, _runtime.CountByStatus(WorkflowExecutionStatus.Running));
        Assert.Equal(1UL, _runtime.CountByStatus(WorkflowExecutionStatus.Completed));
    }

    [Fact]
    public void Visibility_Count_By_Namespace()
    {
        _runtime.StartWorkflow(300, 100, 0, 42, 1);  // namespace 0
        _runtime.StartWorkflow(301, 100, 1, 42, 1);  // namespace 1

        Assert.True(_runtime.CountByNamespace(0) >= 1);
        Assert.True(_runtime.CountByNamespace(1) >= 1);
    }

    // ─── Update Dispatch ──────────────────────────────────────────────────

    [Fact]
    public void Update_Dispatch_And_Check()
    {
        ulong key = _runtime.StartWorkflow(400, 100, 0, 42, 1);

        Assert.False(_runtime.HasUpdate(key, 500));
        _runtime.Update(key, 500, new byte[] { 10, 20, 30 });
        Assert.True(_runtime.HasUpdate(key, 500));
    }

    // ─── Cron Scheduling ──────────────────────────────────────────────────

    [Fact]
    public void Cron_Register_And_Count()
    {
        Assert.Equal(0UL, _runtime.CronScheduleCount);

        ulong scheduleId = _runtime.RegisterCron("* * * * *", 100, 0, 42, 1, 0);
        Assert.True(scheduleId > 0);
        Assert.Equal(1UL, _runtime.CronScheduleCount);
    }

    [Fact]
    public void Cron_Process_Fires()
    {
        _runtime.RegisterCron("* * * * *", 100, 0, 42, 1, 0);

        // Process at time 5 — should fire
        ulong started = _runtime.ProcessCronFires(5);
        Assert.Equal(1UL, started);
    }

    // ─── Batch Operations ─────────────────────────────────────────────────

    [Fact]
    public void Batch_Terminate_Multiple()
    {
        ulong k1 = _runtime.StartWorkflow(500, 100, 0, 42, 1);
        ulong k2 = _runtime.StartWorkflow(501, 100, 0, 42, 1);
        ulong k3 = _runtime.StartWorkflow(502, 100, 0, 42, 1);

        ulong batchId = _runtime.BatchTerminate(new[] { k1, k2, k3 });
        Assert.True(batchId > 0);
        Assert.Equal(1UL, _runtime.BatchCount);

        Assert.Equal(WorkflowExecutionStatus.Terminated, _runtime.GetStatus(k1));
        Assert.Equal(WorkflowExecutionStatus.Terminated, _runtime.GetStatus(k2));
        Assert.Equal(WorkflowExecutionStatus.Terminated, _runtime.GetStatus(k3));
    }

    [Fact]
    public void Batch_Cancel_Multiple()
    {
        ulong k1 = _runtime.StartWorkflow(600, 100, 0, 42, 1);
        ulong k2 = _runtime.StartWorkflow(601, 100, 0, 42, 1);

        _runtime.BatchCancel(new[] { k1, k2 });

        Assert.Equal(WorkflowExecutionStatus.Canceled, _runtime.GetStatus(k1));
        Assert.Equal(WorkflowExecutionStatus.Canceled, _runtime.GetStatus(k2));
    }

    [Fact]
    public void Batch_Signal_Multiple()
    {
        ulong k1 = _runtime.StartWorkflow(700, 100, 0, 42, 1);
        ulong k2 = _runtime.StartWorkflow(701, 100, 0, 42, 1);

        _runtime.BatchSignal(new[] { k1, k2 }, 999, new byte[] { 1, 2, 3 });

        Assert.True(_runtime.HasSignal(k1, 999));
        Assert.True(_runtime.HasSignal(k2, 999));
    }

    // ─── Archival ─────────────────────────────────────────────────────────

    [Fact]
    public void Archival_Auto_Archive_On_Complete()
    {
        ulong key = _runtime.StartWorkflow(800, 100, 0, 42, 1);
        _runtime.CompleteWorkflow(key, new byte[] { 42 });

        // Completed workflow should be auto-archived
        Assert.True(_runtime.IsArchived(key));
        Assert.True(_runtime.ArchiveCount >= 1);
    }

    [Fact]
    public void Archival_Count_By_Namespace()
    {
        ulong k1 = _runtime.StartWorkflow(900, 100, 10, 42, 1);
        ulong k2 = _runtime.StartWorkflow(901, 101, 10, 42, 1);
        _runtime.CompleteWorkflow(k1);
        _runtime.CompleteWorkflow(k2);

        Assert.True(_runtime.ArchiveCountByNamespace(10) >= 2);
    }

    // ─── Phase 3+ Feature Tests ──────────────────────────────────────────────

    [Fact]
    public void EventHistory_Recorded_On_Start_And_Complete()
    {
        ulong key = _runtime.StartWorkflow(10001, 1, 0, 42, 2);
        _runtime.CompleteWorkflow(key);
        Assert.True(_runtime.EventCount(key) >= 2); // Started + Completed
    }

    [Fact]
    public void WorkerVersioning_Create_Set_And_Add_Build()
    {
        ulong setId = _runtime.CreateVersionSet();
        Assert.True(setId > 0);
        Assert.True(_runtime.AddBuildId(setId, "build-1"));
        Assert.Equal(1UL, _runtime.VersionSetCount);
    }

    [Fact]
    public void RateLimiter_Allows_Under_Limit()
    {
        Assert.True(_runtime.TryRateLimit(0, 1));
    }

    [Fact]
    public void Heartbeat_Register_And_Record()
    {
        ulong key = _runtime.StartWorkflow(10002, 1, 0, 42, 1);
        _runtime.RegisterHeartbeat(key, 100, 5000);
        Assert.Equal(1UL, _runtime.HeartbeatActiveCount);
        _runtime.RecordHeartbeat(key, 100);
    }

    [Fact]
    public void Auth_Admin_Has_Access()
    {
        Assert.True(_runtime.Authorize("admin-user", "admin", 0)); // StartWorkflow
    }

    [Fact]
    public void DynamicConfig_Set_And_Get()
    {
        _runtime.ConfigSetInt("test.key", 42);
        Assert.Equal(42L, _runtime.ConfigGetInt("test.key"));
    }

    [Fact]
    public void QueryHandler_Register_And_Count()
    {
        ulong key = _runtime.StartWorkflow(10003, 1, 0, 42, 1);
        _runtime.RegisterQueryHandler(key, 1);
        Assert.True(_runtime.QueryHandlerCount >= 1);
    }

    [Fact]
    public void Memo_Set_And_Count()
    {
        ulong key = _runtime.StartWorkflow(10004, 1, 0, 42, 1);
        _runtime.SetMemo(key, "user_id", "alice"u8.ToArray());
        Assert.Equal(1UL, _runtime.MemoCount(key));
    }

    [Fact]
    public void Schedules_Create_And_Count()
    {
        ulong id = _runtime.CreateSchedule(100, 0, 42);
        Assert.True(id > 0);
        Assert.Equal(1UL, _runtime.ScheduleCount);
    }

    [Fact]
    public void WorkflowReset_Add_Reset_Point()
    {
        ulong key = _runtime.StartWorkflow(10005, 1, 0, 42, 1);
        _runtime.AddResetPoint(key, 5);
        Assert.Equal(1UL, _runtime.ResetPointCount(key));
    }

    [Fact]
    public void Patches_Register_And_Count()
    {
        ulong id = _runtime.RegisterPatch(1, "v2", 0, 100, "New logic");
        Assert.True(id > 0);
        Assert.Equal(1UL, _runtime.PatchCount);
    }

    [Fact]
    public void Cluster_Register_And_Count()
    {
        // Engine starts with 1 local cluster
        Assert.Equal(1UL, _runtime.ClusterCount);
        _runtime.RegisterCluster("dc2", "dc2.example.com:9090");
        Assert.Equal(2UL, _runtime.ClusterCount);
    }

    [Fact]
    public void Sharding_Shard_For_Key()
    {
        uint shard = _runtime.ShardForKey(100);
        Assert.True(shard < _runtime.ShardCount);
    }

    [Fact]
    public void Nexus_Register_Service_And_Count()
    {
        _runtime.RegisterNexusService("payments", "http://payments:8080");
        Assert.Equal(1UL, _runtime.NexusServiceCount);
    }

    [Fact]
    public void SignalWithStart_Starts_New_Workflow()
    {
        ulong key = _runtime.SignalWithStart(20001, 1, 0, 42, 1, 100, out bool wasStarted);
        Assert.True(wasStarted);
        Assert.Equal(WorkflowExecutionStatus.Running, _runtime.GetStatus(key));
        Assert.True(_runtime.HasSignal(key, 100));
    }

    [Fact]
    public void ContinueAsNew_Chains_Workflows()
    {
        ulong key = _runtime.StartWorkflow(30001, 1, 0, 42, 2);
        ulong newKey = _runtime.ContinueAsNew(key);
        Assert.Equal(WorkflowExecutionStatus.ContinuedAsNew, _runtime.GetStatus(key));
        Assert.Equal(WorkflowExecutionStatus.Running, _runtime.GetStatus(newKey));
    }

    [Fact]
    public void ListWorkflows_Returns_All_Running()
    {
        _runtime.StartWorkflow(40001, 1, 0, 42, 1);
        _runtime.StartWorkflow(40002, 1, 0, 42, 1);
        var list = _runtime.ListWorkflows(statusFilter: 1); // Running
        Assert.True(list.Count >= 2);
        Assert.All(list, w => Assert.Equal(WorkflowExecutionStatus.Running, w.Status));
    }

    [Fact]
    public void CompleteActivity_Completes_Step()
    {
        ulong key = _runtime.StartWorkflow(41001, 1, 0, 42, 3);
        Assert.False(_runtime.IsStepCompleted(key, 0));
        _runtime.CompleteActivity(key, 0, new byte[] { 99 });
        Assert.True(_runtime.IsStepCompleted(key, 0));
    }

    [Fact]
    public void GetEventHistory_Returns_Events()
    {
        ulong key = _runtime.StartWorkflow(42001, 1, 0, 42, 2);
        _runtime.CompleteWorkflow(key);
        var events = _runtime.GetEventHistory(key);
        Assert.True(events.Count >= 2); // At least WorkflowStarted + WorkflowCompleted
        Assert.Equal(1u, events[0].EventType); // WorkflowStarted
    }

    [Fact]
    public void SetSearchAttribute_Works()
    {
        ulong key = _runtime.StartWorkflow(43001, 1, 0, 42, 1);
        _runtime.SetSearchAttribute(key, "customer_id", "C123");
        // Verify the workflow is still accessible
        Assert.Equal(WorkflowExecutionStatus.Running, _runtime.GetStatus(key));
    }
}
