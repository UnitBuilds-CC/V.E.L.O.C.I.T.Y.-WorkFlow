using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Velocity.Workflow.Core;

/// <summary>
/// Thin C# wrapper over the Rust workflow engine. All runtime logic (state machine scheduling,
/// task queue, timer engine, WAL persistence) executes in Rust with zero GC. This class only
/// marshals calls across the FFI boundary and manages the engine handle lifetime.
/// </summary>
public sealed unsafe class WorkflowRuntime : IDisposable
{
    private void* _engineHandle;
    private bool _disposed;

    public WorkflowRuntime()
    {
        _engineHandle = NativeBridge.VelocityEngineCreate();
        if (_engineHandle is null)
            throw new InvalidOperationException("Failed to create Rust workflow engine.");
    }

    /// <summary>Get the raw engine handle for direct FFI calls.</summary>
    public void* GetHandle() => _engineHandle;

    /// <summary>Start a new workflow. Returns the workflow key (namespace_id &lt;&lt; 32 | workflow_id).</summary>
    public ulong StartWorkflow(ulong workflowId, ulong workflowTypeId, ulong namespaceId,
        ulong taskQueueHash, uint totalSteps, byte[]? input = null)
    {
        fixed (byte* ptr = input)
        {
            return NativeBridge.VelocityEngineStartWorkflow(
                _engineHandle, workflowId, workflowTypeId, namespaceId,
                taskQueueHash, totalSteps, ptr, (uint)(input?.Length ?? 0));
        }
    }

    /// <summary>Complete a workflow with an optional result.</summary>
    public void CompleteWorkflow(ulong workflowKey, byte[]? result = null)
    {
        fixed (byte* ptr = result)
        {
            NativeBridge.VelocityEngineCompleteWorkflow(_engineHandle, workflowKey, ptr, (uint)(result?.Length ?? 0));
        }
    }

    public void FailWorkflow(ulong workflowKey) => NativeBridge.VelocityEngineFailWorkflow(_engineHandle, workflowKey);
    public void CancelWorkflow(ulong workflowKey) => NativeBridge.VelocityEngineCancelWorkflow(_engineHandle, workflowKey);
    public void TerminateWorkflow(ulong workflowKey) => NativeBridge.VelocityEngineTerminateWorkflow(_engineHandle, workflowKey);

    /// <summary>Get the execution status of a workflow (maps to WorkflowExecutionStatus enum).</summary>
    public WorkflowExecutionStatus GetStatus(ulong workflowKey)
        => (WorkflowExecutionStatus)NativeBridge.VelocityEngineGetStatus(_engineHandle, workflowKey);

    /// <summary>O(1) bitmask check — is this step already completed?</summary>
    public bool IsStepCompleted(ulong workflowKey, uint step)
        => NativeBridge.VelocityEngineIsStepCompleted(_engineHandle, workflowKey, step) == 1;

    /// <summary>Complete a step with a result. Updates bitmask + Merkle root in Rust. Schedules next workflow task.</summary>
    public void CompleteStep(ulong workflowKey, uint step, byte[]? result = null)
    {
        fixed (byte* ptr = result)
        {
            NativeBridge.VelocityEngineCompleteStep(_engineHandle, workflowKey, step, ptr, (uint)(result?.Length ?? 0));
        }
    }

    /// <summary>Get the cached result for a completed step from Rust memory.</summary>
    public byte[]? GetStepResult(ulong workflowKey, uint step)
    {
        Span<byte> buf = stackalloc byte[1024];
        fixed (byte* ptr = buf)
        {
            int len = NativeBridge.VelocityEngineGetStepResult(_engineHandle, workflowKey, step, ptr, (uint)buf.Length);
            if (len <= 0) return null;
            return buf[..len].ToArray();
        }
    }

    /// <summary>Schedule an activity for execution. The Rust task queue dispatches it to a worker.</summary>
    public void ScheduleActivity(ulong workflowKey, uint step, ulong activityNameId, byte[]? args = null)
    {
        fixed (byte* ptr = args)
        {
            NativeBridge.VelocityEngineScheduleActivity(_engineHandle, workflowKey, step, activityNameId, ptr, (uint)(args?.Length ?? 0));
        }
    }

    /// <summary>Poll the Rust task queue for the next task. Non-blocking — returns null if empty.</summary>
    public PolledTask? PollTask(ulong taskQueueHash)
    {
        uint kind = 0;
        ulong workflowKey = 0;
        uint stepIndex = 0;
        ulong activityNameId = 0;
        ulong taskId = 0;
        uint attempt = 0;

        int result = NativeBridge.VelocityEnginePollTask(
            _engineHandle, taskQueueHash,
            &kind, &workflowKey, &stepIndex, &activityNameId, &taskId, &attempt);

        if (result == 0) return null;

        return new PolledTask
        {
            TaskKind = (TaskKind)kind,
            WorkflowKey = workflowKey,
            StepIndex = stepIndex,
            ActivityNameId = activityNameId,
            TaskId = taskId,
            Attempt = attempt
        };
    }

    /// <summary>Signal a running workflow. Rust handles buffering and task scheduling.</summary>
    public void Signal(ulong workflowKey, ulong signalNameId, byte[]? payload = null)
    {
        fixed (byte* ptr = payload)
        {
            NativeBridge.VelocityEngineSignal(_engineHandle, workflowKey, signalNameId, ptr, (uint)(payload?.Length ?? 0));
        }
    }

    public bool HasSignal(ulong workflowKey, ulong signalNameId)
        => NativeBridge.VelocityEngineHasSignal(_engineHandle, workflowKey, signalNameId) == 1;

    /// <summary>Dispatch an update to a running workflow.</summary>
    public void Update(ulong workflowKey, ulong updateNameId, byte[]? payload = null)
    {
        fixed (byte* ptr = payload)
        {
            NativeBridge.VelocityEngineUpdate(_engineHandle, workflowKey, updateNameId, ptr, (uint)(payload?.Length ?? 0));
        }
    }

    public bool HasUpdate(ulong workflowKey, ulong updateNameId)
        => NativeBridge.VelocityEngineHasUpdate(_engineHandle, workflowKey, updateNameId) == 1;

    /// <summary>Schedule a durable timer. Rust fires it and enqueues a workflow task.</summary>
    public ulong ScheduleTimer(ulong workflowKey, TimeSpan delay)
        => NativeBridge.VelocityEngineScheduleTimer(_engineHandle, workflowKey, (ulong)delay.TotalMilliseconds);

    /// <summary>Verify the Merkle root of a workflow's slab header (cryptographic integrity check).</summary>
    public bool VerifySlab(ulong workflowKey)
        => NativeBridge.VelocityEngineVerifySlab(_engineHandle, workflowKey) == 1;

    /// <summary>Start a child workflow linked to a parent.</summary>
    public ulong StartChildWorkflow(ulong parentKey, ulong childWorkflowId, ulong workflowTypeId,
        ulong taskQueueHash, uint totalSteps, byte[]? input = null)
    {
        fixed (byte* ptr = input)
        {
            return NativeBridge.VelocityEngineStartChildWorkflow(
                _engineHandle, parentKey, childWorkflowId, workflowTypeId,
                taskQueueHash, totalSteps, ptr, (uint)(input?.Length ?? 0));
        }
    }

    // ─── Stats ────────────────────────────────────────────────────────────────

    public ulong WorkflowCount => NativeBridge.VelocityEngineWorkflowCount(_engineHandle);
    public ulong PendingTasks(ulong taskQueueHash) => NativeBridge.VelocityEnginePendingTasks(_engineHandle, taskQueueHash);
    public ulong PendingTimers => NativeBridge.VelocityEnginePendingTimers(_engineHandle);

    // ─── Namespaces ─────────────────────────────────────────────────────────

    /// <summary>Register a new namespace. Returns the namespace ID.</summary>
    public ulong RegisterNamespace(string name)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(name);
        fixed (byte* ptr = bytes)
        {
            return NativeBridge.VelocityEngineRegisterNamespace(_engineHandle, ptr, (uint)bytes.Length);
        }
    }

    public bool IsNamespaceActive(ulong namespaceId)
        => NativeBridge.VelocityEngineIsNamespaceActive(_engineHandle, namespaceId) == 1;

    public ulong NamespaceCount => NativeBridge.VelocityEngineNamespaceCount(_engineHandle);

    /// <summary>List all registered namespaces.</summary>
    public List<NamespaceInfo> ListNamespaces()
    {
        var results = new List<NamespaceInfo>();
        var handle = GCHandle.Alloc(results);
        try
        {
            var ptr = (void*)GCHandle.ToIntPtr(handle);
            NativeBridge.NamespaceInfoCallback callback = static (id, namePtr, nameLen, isActive, retentionSecs, userData) =>
            {
                var h = (GCHandle)(nint)userData;
                var list = (List<NamespaceInfo>)h.Target!;
                var name = System.Text.Encoding.UTF8.GetString(namePtr, (int)nameLen);
                list.Add(new NamespaceInfo
                {
                    Id = id,
                    Name = name,
                    IsActive = isActive != 0,
                    RetentionDays = (long)(retentionSecs / 86400),
                });
            };
            NativeBridge.VelocityEngineListNamespaces(_engineHandle, (delegate* unmanaged[Cdecl]<ulong, byte*, uint, uint, ulong, void*, void>)
                System.Runtime.InteropServices.Marshal.GetFunctionPointerForDelegate(callback), ptr);
        }
        finally { handle.Free(); }
        return results;
    }

    // ─── Visibility / Search ─────────────────────────────────────────────────

    /// <summary>Get the total number of indexed workflow executions.</summary>
    public ulong VisibilityCount => NativeBridge.VelocityEngineVisibilityCount(_engineHandle);

    /// <summary>Count workflows by execution status.</summary>
    public ulong CountByStatus(WorkflowExecutionStatus status)
        => NativeBridge.VelocityEngineVisibilityCountByStatus(_engineHandle, (uint)status);

    /// <summary>Count workflows in a specific namespace.</summary>
    public ulong CountByNamespace(ulong namespaceId)
        => NativeBridge.VelocityEngineVisibilityCountByNamespace(_engineHandle, namespaceId);

    // ─── Cron Scheduling ──────────────────────────────────────────────────

    /// <summary>Register a cron schedule. Returns the schedule ID.</summary>
    public ulong RegisterCron(string cronExpression, ulong workflowTypeId, ulong namespaceId,
        ulong taskQueueHash, uint totalSteps, ulong currentTimeMinutes = 0)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(cronExpression);
        fixed (byte* ptr = bytes)
        {
            return NativeBridge.VelocityEngineRegisterCron(
                _engineHandle, ptr, (uint)bytes.Length,
                workflowTypeId, namespaceId, taskQueueHash,
                totalSteps, currentTimeMinutes);
        }
    }

    /// <summary>Process cron fires at the given time. Returns number of workflows started.</summary>
    public ulong ProcessCronFires(ulong currentTimeMinutes)
        => NativeBridge.VelocityEngineProcessCronFires(_engineHandle, currentTimeMinutes);

    public ulong CronScheduleCount => NativeBridge.VelocityEngineCronScheduleCount(_engineHandle);

    // ─── Batch Operations ─────────────────────────────────────────────────

    /// <summary>Batch terminate workflows. Returns the batch ID.</summary>
    public ulong BatchTerminate(ulong[] workflowKeys)
    {
        fixed (ulong* ptr = workflowKeys)
        {
            return NativeBridge.VelocityEngineBatchTerminate(_engineHandle, ptr, (uint)workflowKeys.Length);
        }
    }

    /// <summary>Batch cancel workflows. Returns the batch ID.</summary>
    public ulong BatchCancel(ulong[] workflowKeys)
    {
        fixed (ulong* ptr = workflowKeys)
        {
            return NativeBridge.VelocityEngineBatchCancel(_engineHandle, ptr, (uint)workflowKeys.Length);
        }
    }

    /// <summary>Batch signal workflows. Returns the batch ID.</summary>
    public ulong BatchSignal(ulong[] workflowKeys, ulong signalNameId, byte[]? payload = null)
    {
        fixed (ulong* keysPtr = workflowKeys)
        fixed (byte* payloadPtr = payload)
        {
            return NativeBridge.VelocityEngineBatchSignal(
                _engineHandle, keysPtr, (uint)workflowKeys.Length,
                signalNameId, payloadPtr, (uint)(payload?.Length ?? 0));
        }
    }

    public ulong BatchCount => NativeBridge.VelocityEngineBatchCount(_engineHandle);

    // ─── Archival ─────────────────────────────────────────────────────────

    /// <summary>Get the total number of archived workflows.</summary>
    public ulong ArchiveCount => NativeBridge.VelocityEngineArchiveCount(_engineHandle);

    /// <summary>Get archived workflow count for a namespace.</summary>
    public ulong ArchiveCountByNamespace(ulong namespaceId)
        => NativeBridge.VelocityEngineArchiveCountByNamespace(_engineHandle, namespaceId);

    /// <summary>Check if a workflow has been archived.</summary>
    public bool IsArchived(ulong workflowKey)
        => NativeBridge.VelocityEngineIsArchived(_engineHandle, workflowKey) == 1;

    // ─── Event History ────────────────────────────────────────────────────

    public ulong EventCount(ulong workflowKey) => NativeBridge.VelocityEngineEventCount(_engineHandle, workflowKey);

    // ─── Worker Versioning ────────────────────────────────────────────────

    public ulong CreateVersionSet() => NativeBridge.VelocityEngineCreateVersionSet(_engineHandle);

    public bool AddBuildId(ulong setId, string buildId)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(buildId);
        fixed (byte* ptr = bytes) { return NativeBridge.VelocityEngineAddBuildId(_engineHandle, setId, ptr, (uint)bytes.Length) == 0; }
    }

    public ulong VersionSetCount => NativeBridge.VelocityEngineVersionSetCount(_engineHandle);

    // ─── Rate Limiter ─────────────────────────────────────────────────────

    public bool TryRateLimit(ulong namespaceId, uint tokens = 1)
        => NativeBridge.VelocityEngineRateLimitCheck(_engineHandle, namespaceId, tokens) == 1;

    // ─── Heartbeat ────────────────────────────────────────────────────────

    public void RegisterHeartbeat(ulong workflowKey, ulong activityId, ulong timeoutMs)
        => NativeBridge.VelocityEngineRegisterHeartbeat(_engineHandle, workflowKey, activityId, timeoutMs);

    public void RecordHeartbeat(ulong workflowKey, ulong activityId)
        => NativeBridge.VelocityEngineRecordHeartbeat(_engineHandle, workflowKey, activityId);

    public ulong HeartbeatActiveCount => NativeBridge.VelocityEngineHeartbeatActiveCount(_engineHandle);

    public void UnregisterHeartbeat(ulong workflowKey, ulong activityId)
        => NativeBridge.VelocityEngineHeartbeatUnregister(_engineHandle, workflowKey, activityId);

    // ─── Auth ─────────────────────────────────────────────────────────────

    public bool Authorize(string subject, string role, uint permission)
    {
        var sb = System.Text.Encoding.UTF8.GetBytes(subject);
        var rb = System.Text.Encoding.UTF8.GetBytes(role);
        fixed (byte* sp = sb) fixed (byte* rp = rb)
        { return NativeBridge.VelocityEngineAuthCheck(_engineHandle, sp, (uint)sb.Length, rp, (uint)rb.Length, permission) == 1; }
    }

    // ─── Dynamic Config ───────────────────────────────────────────────────

    public void ConfigSetInt(string key, long value)
    {
        var kb = System.Text.Encoding.UTF8.GetBytes(key);
        fixed (byte* ptr = kb) { NativeBridge.VelocityEngineConfigSetInt(_engineHandle, ptr, (uint)kb.Length, value); }
    }

    public long ConfigGetInt(string key, long defaultValue = 0)
    {
        var kb = System.Text.Encoding.UTF8.GetBytes(key);
        fixed (byte* ptr = kb) { return NativeBridge.VelocityEngineConfigGetInt(_engineHandle, ptr, (uint)kb.Length, defaultValue); }
    }

    public void ConfigSetBool(string key, bool value)
    {
        var kb = System.Text.Encoding.UTF8.GetBytes(key);
        fixed (byte* ptr = kb) { NativeBridge.VelocityEngineConfigSetBool(_engineHandle, ptr, (uint)kb.Length, value ? 1 : 0); }
    }

    public void ConfigSetFloat(string key, double value)
    {
        var kb = System.Text.Encoding.UTF8.GetBytes(key);
        fixed (byte* ptr = kb) { NativeBridge.VelocityEngineConfigSetFloat(_engineHandle, ptr, (uint)kb.Length, value); }
    }

    public void ConfigSetString(string key, string value)
    {
        var kb = System.Text.Encoding.UTF8.GetBytes(key);
        var vb = System.Text.Encoding.UTF8.GetBytes(value);
        fixed (byte* kp = kb) fixed (byte* vp = vb)
        { NativeBridge.VelocityEngineConfigSetString(_engineHandle, kp, (uint)kb.Length, vp, (uint)vb.Length); }
    }

    public bool ConfigGetBool(string key)
    {
        var kb = System.Text.Encoding.UTF8.GetBytes(key);
        fixed (byte* ptr = kb) { return NativeBridge.VelocityEngineConfigGetBool(_engineHandle, ptr, (uint)kb.Length) == 1; }
    }

    public double ConfigGetFloat(string key)
    {
        var kb = System.Text.Encoding.UTF8.GetBytes(key);
        fixed (byte* ptr = kb) { return NativeBridge.VelocityEngineConfigGetFloat(_engineHandle, ptr, (uint)kb.Length); }
    }

    public ulong ConfigKeyCount => NativeBridge.VelocityEngineConfigKeyCount(_engineHandle);

    // ─── Query Handler ────────────────────────────────────────────────────

    public void RegisterQueryHandler(ulong workflowKey, ulong queryNameId)
        => NativeBridge.VelocityEngineRegisterQueryHandler(_engineHandle, workflowKey, queryNameId);

    public ulong QueryHandlerCount => NativeBridge.VelocityEngineQueryHandlerCount(_engineHandle);

    // ─── Memo ─────────────────────────────────────────────────────────────

    public void SetMemo(ulong workflowKey, string key, byte[]? value = null)
    {
        var kb = System.Text.Encoding.UTF8.GetBytes(key);
        fixed (byte* kp = kb) fixed (byte* vp = value)
        { NativeBridge.VelocityEngineSetMemo(_engineHandle, workflowKey, kp, (uint)kb.Length, vp, (uint)(value?.Length ?? 0)); }
    }

    public ulong MemoCount(ulong workflowKey) => NativeBridge.VelocityEngineMemoCount(_engineHandle, workflowKey);

    /// <summary>Get a memo value by key. Returns null if not found.</summary>
    public byte[]? GetMemo(ulong workflowKey, string key)
    {
        var kb = System.Text.Encoding.UTF8.GetBytes(key);
        const int BUF = 4096;
        var buf = new byte[BUF];
        uint actualLen;
        fixed (byte* kp = kb) fixed (byte* bp = buf)
        {
            int rc = NativeBridge.VelocityEngineGetMemo(_engineHandle, workflowKey, kp, (uint)kb.Length, bp, BUF, &actualLen);
            if (rc != 0 || actualLen == 0) return null;
            var result = new byte[actualLen];
            System.Runtime.InteropServices.Marshal.Copy((nint)bp, result, 0, (int)actualLen);
            return result;
        }
    }

    // ─── Schedules ────────────────────────────────────────────────────────

    public ulong CreateSchedule(ulong workflowTypeId, ulong namespaceId, ulong taskQueueHash, uint overlapPolicy = 0, ulong jitter = 0)
        => NativeBridge.VelocityEngineCreateSchedule(_engineHandle, workflowTypeId, namespaceId, taskQueueHash, overlapPolicy, jitter);

    public ulong ScheduleCount => NativeBridge.VelocityEngineScheduleCount(_engineHandle);

    public bool PauseSchedule(ulong scheduleId) => NativeBridge.VelocityEnginePauseSchedule(_engineHandle, scheduleId) == 0;
    public bool UnpauseSchedule(ulong scheduleId) => NativeBridge.VelocityEngineUnpauseSchedule(_engineHandle, scheduleId) == 0;
    public bool DeleteSchedule(ulong scheduleId) => NativeBridge.VelocityEngineDeleteSchedule(_engineHandle, scheduleId) == 0;

    // ─── Workflow Reset ───────────────────────────────────────────────────

    public void AddResetPoint(ulong workflowKey, ulong eventId)
        => NativeBridge.VelocityEngineAddResetPoint(_engineHandle, workflowKey, eventId);

    public ulong ResetPointCount(ulong workflowKey) => NativeBridge.VelocityEngineResetPointCount(_engineHandle, workflowKey);

    // ─── Patches (Version Branching) ──────────────────────────────────────

    public ulong RegisterPatch(ulong workflowTypeId, string marker, ulong minVersion, ulong maxVersion, string description = "")
    {
        var mb = System.Text.Encoding.UTF8.GetBytes(marker);
        var db = System.Text.Encoding.UTF8.GetBytes(description);
        fixed (byte* mp = mb) fixed (byte* dp = db)
        { return NativeBridge.VelocityEngineRegisterPatch(_engineHandle, workflowTypeId, mp, (uint)mb.Length, minVersion, maxVersion, dp, (uint)db.Length); }
    }

    public ulong PatchCount => NativeBridge.VelocityEnginePatchCount(_engineHandle);

    public bool DeactivatePatch(ulong patchId)
        => NativeBridge.VelocityEngineDeactivatePatch(_engineHandle, patchId) == 1;

    public ulong FindPatch(ulong workflowTypeId, ulong version)
        => NativeBridge.VelocityEngineFindPatch(_engineHandle, workflowTypeId, version);

    public PatchInfo? GetPatch(ulong patchId)
    {
        unsafe
        {
            var fields = new ulong[5];
            fixed (ulong* fp = fields)
            {
                if (NativeBridge.VelocityEngineGetPatch(_engineHandle, patchId, fp) == 0)
                    return null;
                return new PatchInfo
                {
                    PatchId = fields[0],
                    WorkflowTypeId = fields[1],
                    MinVersion = fields[2],
                    MaxVersion = fields[3],
                    IsActive = fields[4] == 1,
                };
            }
        }
    }

    public ulong[] ActivePatchesForType(ulong workflowTypeId)
    {
        unsafe
        {
            var ids = new ulong[64];
            fixed (ulong* ip = ids)
            {
                uint count = NativeBridge.VelocityEngineActivePatchesForType(_engineHandle, workflowTypeId, ip, 64);
                var result = new ulong[count];
                for (uint i = 0; i < count; i++)
                    result[i] = ids[i];
                return result;
            }
        }
    }

    // ─── Cluster ──────────────────────────────────────────────────────────

    public ulong RegisterCluster(string name, string address)
    {
        var nb = System.Text.Encoding.UTF8.GetBytes(name);
        var ab = System.Text.Encoding.UTF8.GetBytes(address);
        fixed (byte* np = nb) fixed (byte* ap = ab)
        { return NativeBridge.VelocityEngineRegisterCluster(_engineHandle, np, (uint)nb.Length, ap, (uint)ab.Length); }
    }

    public ulong ClusterCount => NativeBridge.VelocityEngineClusterCount(_engineHandle);
    public ulong PendingReplicationCount => NativeBridge.VelocityEnginePendingReplicationCount(_engineHandle);

    // ─── Sharding ─────────────────────────────────────────────────────────

    public uint ShardForKey(ulong workflowKey) => NativeBridge.VelocityEngineShardForKey(_engineHandle, workflowKey);

    public bool AssignShard(uint shardId, string host)
    {
        var hb = System.Text.Encoding.UTF8.GetBytes(host);
        fixed (byte* ptr = hb) { return NativeBridge.VelocityEngineAssignShard(_engineHandle, shardId, ptr, (uint)hb.Length) == 1; }
    }

    public uint ShardCount => NativeBridge.VelocityEngineShardCount(_engineHandle);

    // ─── Nexus ────────────────────────────────────────────────────────────

    public void RegisterNexusService(string name, string endpoint)
    {
        var nb = System.Text.Encoding.UTF8.GetBytes(name);
        var eb = System.Text.Encoding.UTF8.GetBytes(endpoint);
        fixed (byte* np = nb) fixed (byte* ep = eb)
        { NativeBridge.VelocityEngineRegisterNexusService(_engineHandle, np, (uint)nb.Length, ep, (uint)eb.Length); }
    }

    public ulong NexusServiceCount => NativeBridge.VelocityEngineNexusServiceCount(_engineHandle);
    public ulong NexusOperationCount => NativeBridge.VelocityEngineNexusOperationCount(_engineHandle);

    // ─── SignalWithStart ──────────────────────────────────────────────────

    public ulong SignalWithStart(ulong workflowId, ulong workflowTypeId, ulong namespaceId,
        ulong taskQueueHash, uint totalSteps, ulong signalNameId, out bool wasStarted, byte[]? payload = null)
    {
        uint ws = 0;
        fixed (byte* ptr = payload)
        {
            var key = NativeBridge.VelocityEngineSignalWithStart(
                _engineHandle, workflowId, workflowTypeId, namespaceId,
                taskQueueHash, totalSteps, signalNameId, ptr, (uint)(payload?.Length ?? 0), &ws);
            wasStarted = ws == 1;
            return key;
        }
    }

    // ─── ContinueAsNew ────────────────────────────────────────────────────

    public ulong ContinueAsNew(ulong workflowKey, byte[]? input = null)
    {
        fixed (byte* ptr = input)
        { return NativeBridge.VelocityEngineContinueAsNew(_engineHandle, workflowKey, ptr, (uint)(input?.Length ?? 0)); }
    }

    // ─── Payload Codec ────────────────────────────────────────────────────

    public ulong CodecChainLen => NativeBridge.VelocityEngineCodecChainLen(_engineHandle);

    // ─── Visibility Listing ─────────────────────────────────────────────────

    /// <summary>List workflows, optionally filtered by namespace and/or status.</summary>
    public List<WorkflowVisibilityInfo> ListWorkflows(ulong namespaceId = ulong.MaxValue, int statusFilter = -1)
    {
        var results = new List<WorkflowVisibilityInfo>();
        var handle = GCHandle.Alloc(results);
        try
        {
            var ptr = (void*)GCHandle.ToIntPtr(handle);
            NativeBridge.WorkflowInfoCallback cb = (wk, wid, rid, wtid, nsid, status, startMs, closeMs, tqh, ud) =>
            {
                var list = (List<WorkflowVisibilityInfo>)GCHandle.FromIntPtr((IntPtr)ud).Target!;
                list.Add(new WorkflowVisibilityInfo
                {
                    WorkflowKey = wk, WorkflowId = wid, RunId = rid,
                    WorkflowTypeId = wtid, NamespaceId = nsid,
                    Status = (WorkflowExecutionStatus)status,
                    StartTimeMs = startMs, CloseTimeMs = closeMs < 0 ? null : (ulong?)closeMs,
                    TaskQueueHash = tqh,
                });
            };
            NativeBridge.VelocityEngineListWorkflows(_engineHandle, namespaceId, statusFilter, cb, ptr);
        }
        finally { handle.Free(); }
        return results;
    }

    /// <summary>Set a search attribute on a workflow execution.</summary>
    public void SetSearchAttribute(ulong workflowKey, string key, string value)
    {
        var kb = System.Text.Encoding.UTF8.GetBytes(key);
        var vb = System.Text.Encoding.UTF8.GetBytes(value);
        fixed (byte* kp = kb) fixed (byte* vp = vb)
        { NativeBridge.VelocityEngineSetSearchAttribute(_engineHandle, workflowKey, kp, (uint)kb.Length, vp, (uint)vb.Length); }
    }

    // ─── Activity Completion ────────────────────────────────────────────────

    /// <summary>Complete an activity task. Completes the corresponding step.</summary>
    public void CompleteActivity(ulong workflowKey, uint step, byte[]? result = null)
    {
        fixed (byte* ptr = result)
        { NativeBridge.VelocityEngineCompleteActivity(_engineHandle, workflowKey, step, ptr, (uint)(result?.Length ?? 0)); }
    }

    /// <summary>Fail an activity task.</summary>
    public void FailActivity(ulong workflowKey, uint step)
        => NativeBridge.VelocityEngineFailActivity(_engineHandle, workflowKey, step);

    // ─── Event History Retrieval ────────────────────────────────────────────

    /// <summary>Get the event history for a workflow.</summary>
    public List<HistoryEventInfo> GetEventHistory(ulong workflowKey)
    {
        var results = new List<HistoryEventInfo>();
        var handle = GCHandle.Alloc(results);
        try
        {
            var ptr = (void*)GCHandle.ToIntPtr(handle);
            NativeBridge.HistoryEventCallback cb = (eventId, eventType, payloadPtr, payloadLen, ud) =>
            {
                var list = (List<HistoryEventInfo>)GCHandle.FromIntPtr((IntPtr)ud).Target!;
                byte[]? payload = null;
                if (payloadPtr != null && payloadLen > 0)
                {
                    payload = new byte[payloadLen];
                    System.Runtime.InteropServices.Marshal.Copy((IntPtr)payloadPtr, payload, 0, (int)payloadLen);
                }
                list.Add(new HistoryEventInfo
                {
                    EventId = eventId, EventType = eventType, Payload = payload,
                });
            };
            NativeBridge.VelocityEngineGetEventHistory(_engineHandle, workflowKey, cb, ptr);
        }
        finally { handle.Free(); }
        return results;
    }

    // ─── Metrics ────────────────────────────────────────────────────────────

    /// <summary>Get the number of registered metrics.</summary>
    public ulong MetricsCount() => NativeBridge.VelocityEngineMetricsCount(_engineHandle);

    /// <summary>Increment a named counter.</summary>
    public void IncCounter(string name)
    {
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(name);
        fixed (byte* p = nameBytes)
            NativeBridge.VelocityEngineIncCounter(_engineHandle, p, (uint)nameBytes.Length);
    }

    /// <summary>Get a named counter's value.</summary>
    public ulong GetCounter(string name)
    {
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(name);
        fixed (byte* p = nameBytes)
            return NativeBridge.VelocityEngineGetCounter(_engineHandle, p, (uint)nameBytes.Length);
    }

    // ─── Saga ───────────────────────────────────────────────────────────────

    /// <summary>Create a new saga for compensation tracking.</summary>
    public ulong CreateSaga(ulong workflowKey, uint stepCount)
        => NativeBridge.VelocityEngineCreateSaga(_engineHandle, workflowKey, stepCount);

    /// <summary>Complete a saga step.</summary>
    public bool CompleteSagaStep(ulong sagaId, uint stepIndex)
        => NativeBridge.VelocityEngineCompleteSagaStep(_engineHandle, sagaId, stepIndex) == 0;

    /// <summary>Fail a saga step (triggers compensation).</summary>
    public uint FailSagaStep(ulong sagaId, uint stepIndex)
        => NativeBridge.VelocityEngineFailSagaStep(_engineHandle, sagaId, stepIndex);

    /// <summary>Get the total number of sagas.</summary>
    public ulong SagaCount() => NativeBridge.VelocityEngineSagaCount(_engineHandle);

    /// <summary>Get the status of a saga.</summary>
    public int SagaStatus(ulong sagaId) => NativeBridge.VelocityEngineSagaStatus(_engineHandle, sagaId);

    // ─── Partition ──────────────────────────────────────────────────────────

    /// <summary>Create a task queue partition.</summary>
    public uint CreatePartition(ulong taskQueueHash)
        => NativeBridge.VelocityEngineCreatePartition(_engineHandle, taskQueueHash);

    /// <summary>Set up forwarding between partitions.</summary>
    public bool SetPartitionForwarding(uint fromPartition, uint toPartition, double rate)
        => NativeBridge.VelocityEngineSetPartitionForwarding(_engineHandle, fromPartition, toPartition, rate) == 0;

    /// <summary>Get the total number of partitions.</summary>
    public uint PartitionCount() => NativeBridge.VelocityEnginePartitionCount(_engineHandle);

    public PartitionInfo? DescribePartition(uint partitionId)
    {
        unsafe
        {
            var fields = new ulong[6];
            fixed (ulong* fp = fields)
            {
                if (NativeBridge.VelocityEnginePartitionDescribe(_engineHandle, partitionId, fp) == 0)
                    return null;
                return new PartitionInfo
                {
                    PartitionId = (uint)fields[0],
                    TaskQueueHash = fields[1],
                    PendingTasks = fields[2],
                    WorkerCount = fields[3],
                    ParentPartition = fields[4] == ulong.MaxValue ? (uint?)null : (uint)fields[4],
                    ForwardRate = fields[5] / 1000.0,
                };
            }
        }
    }

    public uint[] GetPartitionIds()
    {
        unsafe
        {
            var ids = new uint[64];
            fixed (uint* ip = ids)
            {
                uint count = NativeBridge.VelocityEnginePartitionIds(_engineHandle, ip, 64);
                var result = new uint[count];
                for (uint i = 0; i < count; i++) result[i] = ids[i];
                return result;
            }
        }
    }

    // ─── Replay ─────────────────────────────────────────────────────────────

    /// <summary>Replay a workflow's event history to reconstruct state.</summary>
    public bool Replay(ulong workflowKey) => NativeBridge.VelocityEngineReplay(_engineHandle, workflowKey) == 1;

    /// <summary>Get the replayed status for a workflow.</summary>
    public int ReplayStatus(ulong workflowKey) => NativeBridge.VelocityEngineReplayStatus(_engineHandle, workflowKey);

    /// <summary>Get the number of step results reconstructed during replay.</summary>
    public uint ReplayStepCount(ulong workflowKey) => NativeBridge.VelocityEngineReplayStepCount(_engineHandle, workflowKey);

    /// <summary>Get the number of events replayed.</summary>
    public uint ReplayEventCount(ulong workflowKey) => NativeBridge.VelocityEngineReplayEventCount(_engineHandle, workflowKey);

    /// <summary>Verify determinism by replaying twice and comparing.</summary>
    public bool VerifyDeterminism(ulong workflowKey)
        => NativeBridge.VelocityEngineVerifyDeterminism(_engineHandle, workflowKey) == 1;

    /// <summary>Get the total number of replays performed.</summary>
    public ulong ReplayCount() => NativeBridge.VelocityEngineReplayCount(_engineHandle);

    /// <summary>Get the number of registered roles.</summary>
    public ulong RoleCount() => NativeBridge.VelocityEngineRoleCount(_engineHandle);

    /// <summary>Set rate limit for a namespace.</summary>
    public bool SetRateLimit(ulong namespaceId, double rate, ulong capacity)
        => NativeBridge.VelocityEngineSetRateLimit(_engineHandle, namespaceId, rate, capacity) == 0;

    // ─── Timeout Enforcement ────────────────────────────────────────────────

    /// <summary>Schedule an activity with timeout parameters.</summary>
    public void ScheduleActivityWithTimeouts(ulong workflowKey, uint step, ulong activityNameId, 
        byte[]? args, ActivityOptions options)
    {
        fixed (byte* ptr = args)
        {
            NativeBridge.VelocityEngineScheduleActivityWithTimeouts(
                _engineHandle, workflowKey, step, activityNameId, ptr, (uint)(args?.Length ?? 0),
                (ulong)options.ScheduleToStart.TotalMilliseconds,
                (ulong)options.StartToClose.TotalMilliseconds,
                (ulong)options.ScheduleToClose.TotalMilliseconds,
                (ulong)options.HeartbeatTimeout.TotalMilliseconds);
        }
    }

    /// <summary>Check for timed-out activities. Returns count of timed-out activities.</summary>
    public uint CheckActivityTimeouts() => NativeBridge.VelocityEngineCheckActivityTimeouts(_engineHandle);

    /// <summary>Check for timed-out workflows. Returns count of timed-out workflows.</summary>
    public uint CheckWorkflowTimeouts() => NativeBridge.VelocityEngineCheckWorkflowTimeouts(_engineHandle);

    /// <summary>Set workflow execution timeout.</summary>
    public bool SetWorkflowTimeout(ulong workflowKey, TimeSpan timeout)
        => NativeBridge.VelocityEngineSetWorkflowTimeout(_engineHandle, workflowKey, (ulong)timeout.TotalMilliseconds) == 0;

    // ─── Parent Close Policy ────────────────────────────────────────────────

    /// <summary>Apply parent close policy (Terminate=0, Cancel=1, Abandon=2).</summary>
    public bool ApplyParentClosePolicy(ulong parentKey, ParentClosePolicy policy)
        => NativeBridge.VelocityEngineApplyParentClosePolicy(_engineHandle, parentKey, (uint)policy) == 0;

    // ─── Activity Retry ─────────────────────────────────────────────────────

    /// <summary>Fail an activity with retry logic. Returns true if retried, false if failed permanently.</summary>
    public bool FailActivityWithRetry(ulong workflowKey, uint step)
        => NativeBridge.VelocityEngineFailActivityWithRetry(_engineHandle, workflowKey, step) == 1;

    // ─── Query Dispatch ─────────────────────────────────────────────────────

    /// <summary>Execute a registered query handler. Returns result or null if no handler.</summary>
    public byte[]? ExecuteQuery(ulong workflowKey, ulong queryNameId, byte[]? input = null)
    {
        Span<byte> outputBuf = stackalloc byte[4096];
        fixed (byte* inputPtr = input, outputPtr = outputBuf)
        {
            int resultLen = NativeBridge.VelocityEngineExecuteQuery(
                _engineHandle, workflowKey, queryNameId,
                inputPtr, (uint)(input?.Length ?? 0),
                outputPtr, (uint)outputBuf.Length);
            if (resultLen < 0) return null;
            return outputBuf[..resultLen].ToArray();
        }
    }

    // ─── Workflow Reset ─────────────────────────────────────────────────────

    /// <summary>Reset a workflow to a previous event ID. Returns true if successful.</summary>
    public bool ResetWorkflow(ulong workflowKey, ulong resetToEventId)
        => NativeBridge.VelocityEngineResetWorkflow(_engineHandle, workflowKey, resetToEventId) == 1;

    // ─── Visibility SQL Query ───────────────────────────────────────────────

    /// <summary>Execute a SQL-like visibility query. Returns matching workflows.</summary>
    public List<WorkflowVisibilityInfo> ExecuteVisibilityQuery(string query)
    {
        var results = new List<WorkflowVisibilityInfo>();
        var queryBytes = System.Text.Encoding.UTF8.GetBytes(query);
        var handle = System.Runtime.InteropServices.GCHandle.Alloc(results);
        try
        {
            var ptr = (void*)System.Runtime.InteropServices.GCHandle.ToIntPtr(handle);
            NativeBridge.WorkflowInfoCallback callback = (workflowKey, workflowId, runId, workflowTypeId, namespaceId, status, startTimeMs, closeTimeMs, taskQueueHash, userData) =>
            {
                var list = (List<WorkflowVisibilityInfo>)System.Runtime.InteropServices.GCHandle.FromIntPtr((nint)userData).Target!;
                list.Add(new WorkflowVisibilityInfo
                {
                    WorkflowKey = workflowKey,
                    WorkflowId = workflowId,
                    RunId = runId,
                    WorkflowTypeId = workflowTypeId,
                    NamespaceId = namespaceId,
                    Status = (WorkflowExecutionStatus)status,
                    StartTimeMs = startTimeMs,
                    CloseTimeMs = closeTimeMs >= 0 ? (ulong?)closeTimeMs : null,
                    TaskQueueHash = taskQueueHash,
                });
            };
            fixed (byte* p = queryBytes)
                NativeBridge.VelocityEngineExecuteVisibilityQuery(_engineHandle, p, (uint)queryBytes.Length, callback, ptr);
        }
        finally { handle.Free(); }
        return results;
    }

    /// <summary>Export all engine metrics in Prometheus text exposition format.</summary>
    public string ExportPrometheusMetrics()
    {
        const int BUFFER_SIZE = 65536;
        var buffer = new byte[BUFFER_SIZE];
        uint actualLen;
        fixed (byte* ptr = buffer)
        {
            NativeBridge.VelocityEngineExportMetrics(_engineHandle, ptr, BUFFER_SIZE, &actualLen);
        }
        return System.Text.Encoding.UTF8.GetString(buffer, 0, (int)actualLen);
    }

    /// <summary>Get a rich description of a workflow including status, steps, timing, search attributes, and memo.</summary>
    public WorkflowDescription? DescribeWorkflow(ulong workflowKey)
    {
        const int BUFFER_SIZE = 16384;
        var buffer = new byte[BUFFER_SIZE];
        uint actualLen;
        fixed (byte* ptr = buffer)
        {
            int result = NativeBridge.VelocityEngineDescribeWorkflow(_engineHandle, workflowKey, ptr, BUFFER_SIZE, &actualLen);
            if (result != 0) return null;

            int pos = 0;
            var status = (WorkflowExecutionStatus)buffer[pos++];
            uint totalSteps = BitConverter.ToUInt32(buffer, pos); pos += 4;
            uint completedSteps = BitConverter.ToUInt32(buffer, pos); pos += 4;
            ulong eventSeq = BitConverter.ToUInt64(buffer, pos); pos += 8;
            ulong startTimeMs = BitConverter.ToUInt64(buffer, pos); pos += 8;
            ulong closeTimeMs = BitConverter.ToUInt64(buffer, pos); pos += 8;
            bool hasClose = buffer[pos++] != 0;
            ulong workflowTypeId = BitConverter.ToUInt64(buffer, pos); pos += 8;
            ulong namespaceId = BitConverter.ToUInt64(buffer, pos); pos += 8;
            ulong taskQueueHash = BitConverter.ToUInt64(buffer, pos); pos += 8;
            uint searchAttrCount = BitConverter.ToUInt32(buffer, pos); pos += 4;

            var searchAttrs = new Dictionary<string, object>();
            for (uint i = 0; i < searchAttrCount && pos < actualLen; i++)
            {
                uint keyLen = BitConverter.ToUInt32(buffer, pos); pos += 4;
                string key = System.Text.Encoding.UTF8.GetString(buffer, pos, (int)keyLen); pos += (int)keyLen;
                byte typeTag = buffer[pos++];
                object? val = typeTag switch
                {
                    1 => System.Text.Encoding.UTF8.GetString(buffer, pos + 4, (int)BitConverter.ToUInt32(buffer, pos)),
                    2 => BitConverter.ToInt64(buffer, pos),
                    3 => BitConverter.ToDouble(buffer, pos),
                    4 => buffer[pos] != 0,
                    5 => BitConverter.ToUInt64(buffer, pos),
                    6 => System.Text.Encoding.UTF8.GetString(buffer, pos + 4, (int)BitConverter.ToUInt32(buffer, pos)),
                    _ => null
                };
                if (val != null) searchAttrs[key] = val;
                // Advance pos past value (approximate - skip for now)
                pos += typeTag switch { 1 or 6 => (int)(4 + BitConverter.ToUInt32(buffer, pos)), 2 or 3 or 5 => 8, 4 => 1, _ => 0 };
            }

            uint memoCount = pos + 4 <= actualLen ? BitConverter.ToUInt32(buffer, pos) : 0;

            return new WorkflowDescription
            {
                WorkflowKey = workflowKey,
                Status = status,
                TotalSteps = totalSteps,
                CompletedSteps = completedSteps,
                EventSequence = eventSeq,
                StartTimeMs = startTimeMs,
                CloseTimeMs = hasClose ? closeTimeMs : null,
                WorkflowTypeId = workflowTypeId,
                NamespaceId = namespaceId,
                TaskQueueHash = taskQueueHash,
                SearchAttributeCount = (int)searchAttrCount,
                MemoCount = (int)memoCount,
            };
        }
    }

    /// <summary>Archive a workflow to file-based cold storage.</summary>
    public bool ArchiveWorkflow(ulong workflowKey, string? baseDir = null)
    {
        var dirBytes = baseDir != null ? System.Text.Encoding.UTF8.GetBytes(baseDir) : Array.Empty<byte>();
        fixed (byte* ptr = dirBytes)
        {
            return NativeBridge.VelocityEngineArchiveWorkflow(_engineHandle, workflowKey, ptr, (uint)dirBytes.Length) == 0;
        }
    }

    /// <summary>Retrieve an archived workflow from cold storage. Returns step count or -1.</summary>
    public int RetrieveWorkflow(ulong workflowKey, out WorkflowExecutionStatus status, string? baseDir = null)
    {
        var dirBytes = baseDir != null ? System.Text.Encoding.UTF8.GetBytes(baseDir) : Array.Empty<byte>();
        byte statusByte = 0;
        fixed (byte* ptr = dirBytes)
        {
            int result = NativeBridge.VelocityEngineRetrieveWorkflow(_engineHandle, workflowKey, ptr, (uint)dirBytes.Length, &statusByte);
            status = (WorkflowExecutionStatus)statusByte;
            return result;
        }
    }

    /// <summary>Count workflows archived in cold storage.</summary>
    public int ColdStorageCount(string? baseDir = null)
    {
        var dirBytes = baseDir != null ? System.Text.Encoding.UTF8.GetBytes(baseDir) : Array.Empty<byte>();
        fixed (byte* ptr = dirBytes)
        {
            return NativeBridge.VelocityEngineColdStorageCount(_engineHandle, ptr, (uint)dirBytes.Length);
        }
    }

    /// <summary>Encode a payload through the codec chain (compression, encryption, etc).</summary>
    public byte[]? CodecEncode(byte[]? input)
    {
        const int BUFFER_SIZE = 65536;
        var outBuffer = new byte[BUFFER_SIZE];
        uint outLen;
        fixed (byte* inPtr = input ?? Array.Empty<byte>())
        fixed (byte* outPtr = outBuffer)
        {
            int result = NativeBridge.VelocityEngineCodecEncode(
                _engineHandle, inPtr, (uint)(input?.Length ?? 0), outPtr, BUFFER_SIZE, &outLen);
            if (result != 0) return null;
            var output = new byte[outLen];
            Array.Copy(outBuffer, output, (int)outLen);
            return output;
        }
    }

    /// <summary>Decode a payload through the codec chain (reverse order).</summary>
    public byte[]? CodecDecode(byte[]? input)
    {
        const int BUFFER_SIZE = 65536;
        var outBuffer = new byte[BUFFER_SIZE];
        uint outLen;
        fixed (byte* inPtr = input ?? Array.Empty<byte>())
        fixed (byte* outPtr = outBuffer)
        {
            int result = NativeBridge.VelocityEngineCodecDecode(
                _engineHandle, inPtr, (uint)(input?.Length ?? 0), outPtr, BUFFER_SIZE, &outLen);
            if (result != 0) return null;
            var output = new byte[outLen];
            Array.Copy(outBuffer, output, (int)outLen);
            return output;
        }
    }

    /// <summary>Mark a saga compensation step as completed.</summary>
    public bool CompleteSagaCompensation(ulong sagaId, uint stepIndex)
        => NativeBridge.VelocityEngineCompleteSagaCompensation(_engineHandle, sagaId, stepIndex) == 0;

    /// <summary>Get the number of steps in a saga.</summary>
    public uint GetSagaStepCount(ulong sagaId)
        => NativeBridge.VelocityEngineSagaStepCount(_engineHandle, sagaId);

    /// <summary>Get the status of a specific saga step.</summary>
    public int GetSagaStepStatus(ulong sagaId, uint stepIndex)
        => NativeBridge.VelocityEngineSagaStepStatus(_engineHandle, sagaId, stepIndex);

    /// <summary>Get the current step index being executed in a saga.</summary>
    public uint GetSagaCurrentStep(ulong sagaId)
        => NativeBridge.VelocityEngineSagaCurrentStep(_engineHandle, sagaId);

    /// <summary>Recover engine state from a WAL file. Returns number of records replayed.</summary>
    public long RecoverFromWal(string? walPath = null)
    {
        var pathBytes = walPath != null ? System.Text.Encoding.UTF8.GetBytes(walPath) : Array.Empty<byte>();
        fixed (byte* ptr = pathBytes)
        {
            return NativeBridge.VelocityEngineWalRecover(_engineHandle, ptr, (uint)pathBytes.Length);
        }
    }

    /// <summary>Get a page of history events for a workflow.</summary>
    public List<HistoryEventDetail> GetHistoryEvents(ulong workflowKey, ulong startEventId = 1, uint maxCount = 100)
    {
        const int BUFFER_SIZE = 65536;
        var buffer = new byte[BUFFER_SIZE];
        uint actualLen;
        var results = new List<HistoryEventDetail>();
        fixed (byte* ptr = buffer)
        {
            int count = NativeBridge.VelocityEngineGetHistoryPage(
                _engineHandle, workflowKey, startEventId, maxCount, ptr, BUFFER_SIZE, &actualLen);
            if (count <= 0) return results;

            int pos = 0;
            for (int i = 0; i < count && pos + 24 <= actualLen; i++)
            {
                ulong eventId = BitConverter.ToUInt64(buffer, pos); pos += 8;
                int eventType = (int)BitConverter.ToUInt32(buffer, pos); pos += 4;
                ulong timestampMs = BitConverter.ToUInt64(buffer, pos); pos += 8;
                uint payloadLen = BitConverter.ToUInt32(buffer, pos); pos += 4;
                byte[]? payload = null;
                if (payloadLen > 0 && pos + payloadLen <= actualLen)
                {
                    payload = new byte[payloadLen];
                    Array.Copy(buffer, pos, payload, 0, (int)payloadLen);
                    pos += (int)payloadLen;
                }
                results.Add(new HistoryEventDetail
                {
                    EventId = eventId,
                    EventType = eventType,
                    TimestampMs = timestampMs,
                    Payload = payload,
                });
            }
        }
        return results;
    }

    /// <summary>Get a single history event by ID. Returns event type or -1.</summary>
    public byte[]? GetHistoryEventPayload(ulong workflowKey, ulong eventId)
    {
        const int BUFFER_SIZE = 65536;
        var buffer = new byte[BUFFER_SIZE];
        uint actualLen;
        fixed (byte* ptr = buffer)
        {
            int eventType = NativeBridge.VelocityEngineGetHistoryEvent(
                _engineHandle, workflowKey, eventId, ptr, BUFFER_SIZE, &actualLen);
            if (eventType < 0 || actualLen == 0) return null;
            var payload = new byte[actualLen];
            Array.Copy(buffer, payload, (int)actualLen);
            return payload;
        }
    }

    /// <summary>Get the latest reset point event ID for a workflow. Returns -1 if none.</summary>
    public long GetLatestResetEventId(ulong workflowKey)
        => NativeBridge.VelocityEngineLatestResetEventId(_engineHandle, workflowKey);

    /// <summary>Get total reset count across all workflows.</summary>
    public ulong GetTotalResetCount()
        => NativeBridge.VelocityEngineTotalResetCount(_engineHandle);

    /// <summary>Get the workflow key associated with a saga.</summary>
    public ulong GetSagaWorkflowKey(ulong sagaId)
        => NativeBridge.VelocityEngineSagaWorkflowKey(_engineHandle, sagaId);

    /// <summary>Get the overall status of a saga.</summary>
    public int GetSagaOverallStatus(ulong sagaId)
        => NativeBridge.VelocityEngineSagaOverallStatus(_engineHandle, sagaId);

    public SagaInfo? GetSagaInfo(ulong sagaId)
    {
        unsafe
        {
            var fields = new ulong[5];
            fixed (ulong* fp = fields)
            {
                if (NativeBridge.VelocityEngineSagaGet(_engineHandle, sagaId, fp) == 0)
                    return null;
                return new SagaInfo
                {
                    SagaId = fields[0],
                    WorkflowKey = fields[1],
                    CurrentStep = (uint)fields[2],
                    StepCount = (uint)fields[3],
                    Status = (int)fields[4],
                };
            }
        }
    }

    public ulong[] GetSagasByStatus(int status)
    {
        unsafe
        {
            var ids = new ulong[64];
            fixed (ulong* ip = ids)
            {
                uint count = NativeBridge.VelocityEngineSagasByStatus(_engineHandle, status, ip, 64);
                var result = new ulong[count];
                for (uint i = 0; i < count; i++) result[i] = ids[i];
                return result;
            }
        }
    }

    /// <summary>Get the number of workflows with history records.</summary>
    public ulong GetHistoryWorkflowCount()
        => NativeBridge.VelocityEngineHistoryWorkflowCount(_engineHandle);

    /// <summary>Get total history event count across all workflows.</summary>
    public ulong GetTotalHistoryEventCount()
        => NativeBridge.VelocityEngineTotalHistoryEventCount(_engineHandle);

    // ─── Worker Registry ────────────────────────────────────────────────────

    /// <summary>Register a new worker. Returns the assigned worker_id.</summary>
    public ulong RegisterWorker(string address, ulong[]? taskQueueHashes = null, string version = "1.0")
    {
        var addrBytes = System.Text.Encoding.UTF8.GetBytes(address);
        var verBytes = System.Text.Encoding.UTF8.GetBytes(version);
        fixed (byte* addrPtr = addrBytes)
        fixed (byte* verPtr = verBytes)
        fixed (ulong* tqPtr = taskQueueHashes ?? Array.Empty<ulong>())
        {
            return NativeBridge.VelocityEngineRegisterWorker(
                _engineHandle, addrPtr, (uint)addrBytes.Length,
                tqPtr, (uint)(taskQueueHashes?.Length ?? 0),
                verPtr, (uint)verBytes.Length);
        }
    }

    /// <summary>Unregister a worker. Returns true if found and removed.</summary>
    public bool UnregisterWorker(ulong workerId)
        => NativeBridge.VelocityEngineUnregisterWorker(_engineHandle, workerId) == 1;

    /// <summary>Record a heartbeat from a worker.</summary>
    public bool WorkerHeartbeat(ulong workerId)
        => NativeBridge.VelocityEngineWorkerHeartbeat(_engineHandle, workerId) == 1;

    /// <summary>Get total number of registered workers.</summary>
    public ulong GetWorkerCount() => NativeBridge.VelocityEngineWorkerCount(_engineHandle);

    /// <summary>Get number of active (healthy) workers.</summary>
    public ulong GetActiveWorkerCount() => NativeBridge.VelocityEngineActiveWorkerCount(_engineHandle);

    /// <summary>Record a task completion for a worker.</summary>
    public void WorkerTaskCompleted(ulong workerId)
        => NativeBridge.VelocityEngineWorkerTaskCompleted(_engineHandle, workerId);

    /// <summary>Record a task failure for a worker.</summary>
    public void WorkerTaskFailed(ulong workerId)
        => NativeBridge.VelocityEngineWorkerTaskFailed(_engineHandle, workerId);

    /// <summary>Set worker status (0=Active, 1=Draining, 2=Offline, 3=Unhealthy).</summary>
    public bool SetWorkerStatus(ulong workerId, int status)
        => NativeBridge.VelocityEngineSetWorkerStatus(_engineHandle, workerId, status) == 0;

    /// <summary>Detect stale workers that haven't heartbeated within timeout.</summary>
    public ulong DetectStaleWorkers(ulong timeoutMs = 30000)
        => NativeBridge.VelocityEngineDetectStaleWorkers(_engineHandle, timeoutMs);

    /// <summary>Add a task queue hash to a worker's capabilities.</summary>
    public void WorkerAddTaskQueue(ulong workerId, ulong taskQueueHash)
        => NativeBridge.VelocityEngineWorkerAddTaskQueue(_engineHandle, workerId, taskQueueHash);

    /// <summary>Get total tasks completed across all workers.</summary>
    public ulong GetTotalTasksCompleted() => NativeBridge.VelocityEngineTotalTasksCompleted(_engineHandle);

    /// <summary>Get total tasks failed across all workers.</summary>
    public ulong GetTotalTasksFailed() => NativeBridge.VelocityEngineTotalTasksFailed(_engineHandle);

    /// <summary>Get worker IDs that can handle a specific task queue.</summary>
    public ulong[] GetWorkersForQueue(ulong taskQueueHash)
    {
        const int MAX_WORKERS = 256;
        var buffer = new ulong[MAX_WORKERS];
        unsafe
        {
            fixed (ulong* ptr = buffer)
            {
                uint count = NativeBridge.VelocityEngineGetWorkersForQueue(_engineHandle, taskQueueHash, ptr, MAX_WORKERS);
                var result = new ulong[count];
                Array.Copy(buffer, result, count);
                return result;
            }
        }
    }

    // ─── Search Attribute Get/List ─────────────────────────────────────────

    /// <summary>Get a search attribute value for a workflow. Returns null if not found.</summary>
    public string? GetSearchAttribute(ulong workflowKey, string key)
    {
        var keyBytes = System.Text.Encoding.UTF8.GetBytes(key);
        const int BUFFER_SIZE = 4096;
        var buffer = new byte[BUFFER_SIZE];
        uint outLen;
        fixed (byte* keyPtr = keyBytes)
        fixed (byte* outPtr = buffer)
        {
            int found = NativeBridge.VelocityEngineGetSearchAttribute(
                _engineHandle, workflowKey, keyPtr, (uint)keyBytes.Length,
                outPtr, BUFFER_SIZE, &outLen);
            if (found == 0 || outLen == 0) return null;
            return System.Text.Encoding.UTF8.GetString(buffer, 0, (int)outLen);
        }
    }

    /// <summary>List all search attribute keys for a workflow.</summary>
    public string[] ListSearchAttributes(ulong workflowKey)
    {
        const int BUFFER_SIZE = 8192;
        var buffer = new byte[BUFFER_SIZE];
        uint outLen;
        fixed (byte* outPtr = buffer)
        {
            uint count = NativeBridge.VelocityEngineListSearchAttributes(
                _engineHandle, workflowKey, outPtr, BUFFER_SIZE, &outLen);
            if (count == 0) return Array.Empty<string>();

            var keys = new List<string>();
            int pos = 0;
            for (uint i = 0; i < count && pos + 4 <= outLen; i++)
            {
                uint keyLen = BitConverter.ToUInt32(buffer, pos); pos += 4;
                if (pos + keyLen > outLen) break;
                keys.Add(System.Text.Encoding.UTF8.GetString(buffer, pos, (int)keyLen));
                pos += (int)keyLen;
            }
            return keys.ToArray();
        }
    }

    // ─── Workflow Timeout Enforcement ──────────────────────────────────────

    /// <summary>Set workflow execution, run, and task timeouts in milliseconds.</summary>
    public bool SetWorkflowTimeouts(ulong workflowKey, ulong executionTimeoutMs = 0,
        ulong runTimeoutMs = 0, ulong taskTimeoutMs = 0)
        => NativeBridge.VelocityEngineSetWorkflowTimeouts(
            _engineHandle, workflowKey, executionTimeoutMs, runTimeoutMs, taskTimeoutMs) == 0;

    /// <summary>Check and enforce workflow timeouts. Returns count of timed-out workflows.</summary>
    public ulong CheckTimeouts() => NativeBridge.VelocityEngineCheckTimeouts(_engineHandle);

    // ─── Task Queue Stats ──────────────────────────────────────────────────

    /// <summary>Get total pending tasks across all task queues.</summary>
    public ulong GetTotalPendingTasks() => NativeBridge.VelocityEngineTotalPendingTasks(_engineHandle);

    /// <summary>Get number of distinct task queues.</summary>
    public ulong GetTaskQueueCount() => NativeBridge.VelocityEngineTaskQueueCount(_engineHandle);

    // ─── Replay Apply + Cold Storage Management ───────────────────────────

    /// <summary>
    /// Apply replay results back to the engine, reconstructing workflow state.
    /// If the workflow context doesn't exist (e.g., after crash), creates a new one.
    /// Returns true if successful.
    /// </summary>
    public bool ApplyReplay(ulong workflowKey)
        => NativeBridge.VelocityEngineApplyReplay(_engineHandle, workflowKey) == 1;

    /// <summary>
    /// Full crash recovery: replays event history from the history store and reconstructs
    /// the workflow context (creating it if it doesn't exist). After this call, the workflow
    /// can resume execution — completed steps are skipped via the bitmask.
    /// Returns the reconstructed status, or -1 if replay failed.
    /// </summary>
    public int ReplayAndRestore(ulong workflowKey)
    {
        if (!ApplyReplay(workflowKey))
            return -1;
        return ReplayStatus(workflowKey);
    }

    /// <summary>
    /// Full crash recovery from WAL + event history. Replays the WAL first to recover
    /// events, then applies replay to reconstruct workflow state.
    /// Returns the number of WAL records replayed, or 0 if no WAL is configured.
    /// After this call, use ReplayAndRestore() to reconstruct the workflow context.
    /// </summary>
    public ulong RecoverFromWal()
        => NativeBridge.VelocityEngineWalReplay(_engineHandle);

    /// <summary>
    /// Delete an archived workflow from cold storage.
    /// Returns true if deleted.
    /// </summary>
    public bool ColdStorageDelete(ulong workflowKey, string? baseDir = null)
    {
        fixed (byte* p = baseDir != null ? System.Text.Encoding.UTF8.GetBytes(baseDir) : null)
        {
            uint len = baseDir != null ? (uint)System.Text.Encoding.UTF8.GetByteCount(baseDir) : 0;
            return NativeBridge.VelocityEngineColdStorageDelete(_engineHandle, workflowKey, p, len) == 1;
        }
    }

    /// <summary>
    /// Garbage collect cold storage archives older than retentionMs.
    /// Returns count of archives deleted.
    /// </summary>
    public int ColdStorageGc(ulong retentionMs, string? baseDir = null)
    {
        fixed (byte* p = baseDir != null ? System.Text.Encoding.UTF8.GetBytes(baseDir) : null)
        {
            uint len = baseDir != null ? (uint)System.Text.Encoding.UTF8.GetByteCount(baseDir) : 0;
            return NativeBridge.VelocityEngineColdStorageGc(_engineHandle, retentionMs, p, len);
        }
    }

    /// <summary>
    /// List cold storage keys by namespace. Returns array of workflow keys.
    /// </summary>
    public ulong[] ColdStorageListByNamespace(ulong namespaceId, string? baseDir = null)
    {
        const int maxKeys = 1024;
        var buffer = new ulong[maxKeys];
        fixed (byte* p = baseDir != null ? System.Text.Encoding.UTF8.GetBytes(baseDir) : null)
        fixed (ulong* bp = buffer)
        {
            uint len = baseDir != null ? (uint)System.Text.Encoding.UTF8.GetByteCount(baseDir) : 0;
            uint count = NativeBridge.VelocityEngineColdStorageListByNamespace(
                _engineHandle, namespaceId, p, len, bp, (uint)maxKeys);
            var result = new ulong[count];
            Array.Copy(buffer, result, count);
            return result;
        }
    }

    // ─── Schedule Introspection + Dynamic Config ──────────────────────────

    /// <summary>List all schedule IDs.</summary>
    public ulong[] ListSchedules()
    {
        const int maxSchedules = 1024;
        var buffer = new ulong[maxSchedules];
        fixed (ulong* bp = buffer)
        {
            uint totalCount = NativeBridge.VelocityEngineListSchedules(_engineHandle, bp, (uint)maxSchedules);
            uint count = Math.Min(totalCount, (uint)maxSchedules);
            var result = new ulong[count];
            Array.Copy(buffer, result, count);
            return result;
        }
    }

    /// <summary>Describe a schedule. Returns null if not found.</summary>
    public ScheduleDescription? DescribeSchedule(ulong scheduleId)
    {
        var fields = new ulong[5];
        fixed (ulong* fp = fields)
        {
            if (NativeBridge.VelocityEngineDescribeSchedule(_engineHandle, scheduleId, fp) == 0)
                return null;
            return new ScheduleDescription
            {
                ScheduleId = scheduleId,
                WorkflowTypeId = fields[0],
                NamespaceId = fields[1],
                TaskQueueHash = fields[2],
                OverlapPolicy = (int)fields[3],
                ActionCount = fields[4],
                IsPaused = NativeBridge.VelocityEngineScheduleIsPaused(_engineHandle, scheduleId) == 1,
            };
        }
    }

    /// <summary>List all dynamic config keys.</summary>
    public string[] ListConfigKeys()
    {
        const int bufSize = 8192;
        var buffer = new byte[bufSize];
        fixed (byte* bp = buffer)
        {
            uint totalCount = NativeBridge.VelocityEngineListConfigKeys(_engineHandle, bp, (uint)bufSize);
            var keys = new List<string>();
            int offset = 0;
            while (offset + 4 <= bufSize && keys.Count < totalCount)
            {
                int keyLen = buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16) | (buffer[offset + 3] << 24);
                offset += 4;
                if (keyLen == 0 || offset + keyLen > bufSize) break;
                keys.Add(System.Text.Encoding.UTF8.GetString(buffer, offset, keyLen));
                offset += keyLen;
            }
            return keys.ToArray();
        }
    }

    /// <summary>Get a dynamic config value as integer.</summary>
    public long GetConfigInt(string key)
    {
        var keyBytes = System.Text.Encoding.UTF8.GetBytes(key);
        fixed (byte* kp = keyBytes)
        {
            return NativeBridge.VelocityEngineGetConfigInt(_engineHandle, kp, (uint)keyBytes.Length);
        }
    }

    // ─── Heartbeat Timeout Check + Count Aggregation ──────────────────────

    /// <summary>Check for heartbeat timeouts. Returns array of (workflow_key, activity_id) pairs.</summary>
    public (ulong WorkflowKey, ulong ActivityId)[] CheckHeartbeatTimeouts()
    {
        const int maxEntries = 256;
        var buffer = new ulong[maxEntries * 2];
        fixed (ulong* bp = buffer)
        {
            uint count = NativeBridge.VelocityEngineCheckHeartbeatTimeouts(_engineHandle, bp, (uint)(maxEntries * 2));
            var result = new (ulong, ulong)[count];
            for (int i = 0; i < count; i++)
            {
                result[i] = (buffer[i * 2], buffer[i * 2 + 1]);
            }
            return result;
        }
    }

    /// <summary>Count workflows by workflow type.</summary>
    public ulong CountByType(ulong workflowTypeId) =>
        NativeBridge.VelocityEngineCountByType(_engineHandle, workflowTypeId);

    // ─── Namespace Retention + Query Dispatch ─────────────────────────────

    /// <summary>Get namespace retention period in milliseconds.</summary>
    public ulong GetNamespaceRetentionMs(ulong namespaceId) =>
        NativeBridge.VelocityEngineGetNamespaceRetentionMs(_engineHandle, namespaceId);

    /// <summary>Cleanup expired workflows based on namespace retention policies. Returns count removed.</summary>
    public ulong CleanupExpiredWorkflows() =>
        NativeBridge.VelocityEngineCleanupExpiredWorkflows(_engineHandle);

    /// <summary>Check if a query handler is registered for a workflow.</summary>
    public bool HasQueryHandler(ulong workflowKey, ulong queryNameId) =>
        NativeBridge.VelocityEngineHasQueryHandler(_engineHandle, workflowKey, queryNameId) == 1;

    // ─── Cluster Replication (Batch 21) ──────────────────────────────────

    /// <summary>Enqueue a replication task for cross-cluster replication.</summary>
    public ulong EnqueueReplication(ulong sourceClusterId, ulong targetClusterId, ulong workflowKey, uint eventType, byte[]? payload = null)
    {
        unsafe
        {
            fixed (byte* pp = payload)
            {
                return NativeBridge.VelocityEngineEnqueueReplication(
                    _engineHandle, sourceClusterId, targetClusterId, workflowKey, eventType,
                    pp, (uint)(payload?.Length ?? 0));
            }
        }
    }

    /// <summary>Drain all pending replication tasks, returning the count drained.</summary>
    public ulong DrainReplicationTasks() =>
        NativeBridge.VelocityEngineDrainReplicationTasks(_engineHandle);

    /// <summary>Get cluster info by ID. Returns null if not found.</summary>
    public ClusterInfo? GetClusterInfo(ulong clusterId)
    {
        unsafe
        {
            var fields = new ulong[4];
            fixed (ulong* fp = fields)
            {
                if (NativeBridge.VelocityEngineGetClusterInfo(_engineHandle, clusterId, fp) == 0)
                    return null;
                return new ClusterInfo
                {
                    ClusterId = fields[0],
                    IsActive = fields[1] == 1,
                    FailoverVersion = fields[2],
                    ReplicationEnabled = fields[3] == 1,
                };
            }
        }
    }

    /// <summary>Get the local cluster ID.</summary>
    public ulong LocalClusterId() =>
        NativeBridge.VelocityEngineLocalClusterId(_engineHandle);

    // ─── Sharding Enhanced (Batch 21) ────────────────────────────────────

    /// <summary>Get count of assigned shards.</summary>
    public ulong AssignedShardCount() =>
        NativeBridge.VelocityEngineAssignedShardCount(_engineHandle);

    /// <summary>Get the owner host of a shard. Returns null if unassigned.</summary>
    public string? GetShardOwner(uint shardId)
    {
        unsafe
        {
            var buf = new byte[256];
            var len = (uint)buf.Length;
            fixed (byte* bp = buf)
            {
                if (NativeBridge.VelocityEngineGetShardOwner(_engineHandle, shardId, bp, &len) != 1)
                    return null;
                return System.Text.Encoding.UTF8.GetString(buf, 0, (int)len);
            }
        }
    }

    /// <summary>Get all shard IDs assigned to a host.</summary>
    public uint[] GetShardsForHost(string host)
    {
        unsafe
        {
            var hostBytes = System.Text.Encoding.UTF8.GetBytes(host);
            var shards = new uint[64];
            var count = (uint)shards.Length;
            fixed (byte* hp = hostBytes)
            fixed (uint* sp = shards)
            {
                NativeBridge.VelocityEngineGetShardsForHost(_engineHandle, hp, (uint)hostBytes.Length, sp, &count);
            }
            var result = new uint[count];
            Array.Copy(shards, result, count);
            return result;
        }
    }

    // ─── Nexus Operations (Batch 21) ─────────────────────────────────────

    /// <summary>Start a Nexus operation on a registered service.</summary>
    public ulong NexusStartOperation(string service, string operation, ulong workflowKey, byte[]? input = null, string? callbackUrl = null)
    {
        unsafe
        {
            var serviceBytes = System.Text.Encoding.UTF8.GetBytes(service);
            var operationBytes = System.Text.Encoding.UTF8.GetBytes(operation);
            var callbackBytes = callbackUrl != null ? System.Text.Encoding.UTF8.GetBytes(callbackUrl) : null;
            fixed (byte* sp = serviceBytes)
            fixed (byte* op = operationBytes)
            fixed (byte* ip = input)
            fixed (byte* cp = callbackBytes)
            {
                return NativeBridge.VelocityEngineNexusStartOperation(
                    _engineHandle, sp, (uint)serviceBytes.Length, op, (uint)operationBytes.Length,
                    workflowKey, ip, (uint)(input?.Length ?? 0), cp, (uint)(callbackBytes?.Length ?? 0));
            }
        }
    }

    /// <summary>Complete a Nexus operation with a result.</summary>
    public bool NexusCompleteOperation(ulong operationId, byte[]? result = null)
    {
        unsafe
        {
            fixed (byte* rp = result)
            {
                return NativeBridge.VelocityEngineNexusCompleteOperation(
                    _engineHandle, operationId, rp, (uint)(result?.Length ?? 0)) == 1;
            }
        }
    }

    /// <summary>Fail a Nexus operation.</summary>
    public bool NexusFailOperation(ulong operationId) =>
        NativeBridge.VelocityEngineNexusFailOperation(_engineHandle, operationId) == 1;

    /// <summary>Get Nexus operation details. Returns null if not found.</summary>
    public NexusOperationInfo? NexusGetOperation(ulong operationId)
    {
        unsafe
        {
            var fields = new ulong[4];
            fixed (ulong* fp = fields)
            {
                if (NativeBridge.VelocityEngineNexusGetOperation(_engineHandle, operationId, fp) == 0)
                    return null;
                return new NexusOperationInfo
                {
                    OperationId = fields[0],
                    WorkflowKey = fields[1],
                    State = (int)fields[2],
                    HasResult = fields[3] == 1,
                };
            }
        }
    }

    // ─── Rate Limiter Enhanced (Batch 22) ────────────────────────────────

    /// <summary>Set per-namespace rate limit.</summary>
    public void RateSetNamespaceLimit(ulong namespaceId, double rate, ulong capacity) =>
        NativeBridge.VelocityEngineRateSetNamespaceLimit(_engineHandle, namespaceId, rate, capacity);

    /// <summary>Get count of namespaces with rate limits configured.</summary>
    public ulong RateNamespaceCount() =>
        NativeBridge.VelocityEngineRateNamespaceCount(_engineHandle);

    // ─── Memo Enhanced (Batch 22) ────────────────────────────────────────

    /// <summary>Remove a memo entry from a workflow.</summary>
    public bool RemoveMemo(ulong workflowKey, string key)
    {
        unsafe
        {
            var keyBytes = System.Text.Encoding.UTF8.GetBytes(key);
            fixed (byte* kp = keyBytes)
            {
                return NativeBridge.VelocityEngineRemoveMemo(_engineHandle, workflowKey, kp, (uint)keyBytes.Length) == 1;
            }
        }
    }

    /// <summary>Get total count of workflows that have memos.</summary>
    public ulong MemoWorkflowCount() =>
        NativeBridge.VelocityEngineMemoWorkflowCount(_engineHandle);

    // ─── Worker Versioning Enhanced (Batch 22) ───────────────────────────

    /// <summary>Set the current build ID for a version set.</summary>
    public bool VersioningSetCurrent(ulong setId, string buildId)
    {
        unsafe
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(buildId);
            fixed (byte* bp = bytes)
            {
                return NativeBridge.VelocityEngineVersioningSetCurrent(_engineHandle, setId, bp, (uint)bytes.Length) == 1;
            }
        }
    }

    /// <summary>Get the current build ID for a version set. Returns null if not found.</summary>
    public string? VersioningGetCurrent(ulong setId)
    {
        unsafe
        {
            var buf = new byte[256];
            var len = (uint)buf.Length;
            fixed (byte* bp = buf)
            {
                if (NativeBridge.VelocityEngineVersioningGetCurrent(_engineHandle, setId, bp, &len) != 1)
                    return null;
                return System.Text.Encoding.UTF8.GetString(buf, 0, (int)len);
            }
        }
    }

    /// <summary>Add a routing rule for task queue to build ID mapping.</summary>
    public void VersioningAddRoutingRule(string taskQueue, string buildId, uint percentage = 100)
    {
        unsafe
        {
            var tqBytes = System.Text.Encoding.UTF8.GetBytes(taskQueue);
            var bidBytes = System.Text.Encoding.UTF8.GetBytes(buildId);
            fixed (byte* tp = tqBytes)
            fixed (byte* bp = bidBytes)
            {
                NativeBridge.VelocityEngineVersioningAddRoutingRule(_engineHandle, tp, (uint)tqBytes.Length, bp, (uint)bidBytes.Length, percentage);
            }
        }
    }

    /// <summary>Resolve the build ID for a task queue. Returns null if no rule matches.</summary>
    public string? VersioningResolveBuildId(string taskQueue)
    {
        unsafe
        {
            var tqBytes = System.Text.Encoding.UTF8.GetBytes(taskQueue);
            var buf = new byte[256];
            var len = (uint)buf.Length;
            fixed (byte* tp = tqBytes)
            fixed (byte* bp = buf)
            {
                if (NativeBridge.VelocityEngineVersioningResolveBuildId(_engineHandle, tp, (uint)tqBytes.Length, bp, &len) != 1)
                    return null;
                return System.Text.Encoding.UTF8.GetString(buf, 0, (int)len);
            }
        }
    }

    /// <summary>Get count of routing rules.</summary>
    public ulong VersioningRoutingRuleCount() =>
        NativeBridge.VelocityEngineVersioningRoutingRuleCount(_engineHandle);

    // ─── Auth Enhanced (Batch 22) ────────────────────────────────────────

    /// <summary>Deny a subject from all operations.</summary>
    public void AuthDenySubject(string subject)
    {
        unsafe
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(subject);
            fixed (byte* bp = bytes)
            {
                NativeBridge.VelocityEngineAuthDenySubject(_engineHandle, bp, (uint)bytes.Length);
            }
        }
    }

    /// <summary>Get count of configured roles.</summary>
    public ulong AuthRoleCount() =>
        NativeBridge.VelocityEngineAuthRoleCount(_engineHandle);

    // ─── Metrics Enhanced (Batch 23) ─────────────────────────────────────

    /// <summary>Increment a named counter.</summary>
    public void MetricsIncCounter(string name)
    {
        unsafe
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(name);
            fixed (byte* bp = bytes)
            {
                NativeBridge.VelocityEngineMetricsIncCounter(_engineHandle, bp, (uint)bytes.Length);
            }
        }
    }

    /// <summary>Get a named counter value.</summary>
    public ulong MetricsGetCounter(string name)
    {
        unsafe
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(name);
            fixed (byte* bp = bytes)
            {
                return NativeBridge.VelocityEngineMetricsGetCounter(_engineHandle, bp, (uint)bytes.Length);
            }
        }
    }

    /// <summary>Set a named gauge value.</summary>
    public void MetricsSetGauge(string name, long value)
    {
        unsafe
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(name);
            fixed (byte* bp = bytes)
            {
                NativeBridge.VelocityEngineMetricsSetGauge(_engineHandle, bp, (uint)bytes.Length, value);
            }
        }
    }

    /// <summary>Get a named gauge value.</summary>
    public long MetricsGetGauge(string name)
    {
        unsafe
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(name);
            fixed (byte* bp = bytes)
            {
                return NativeBridge.VelocityEngineMetricsGetGauge(_engineHandle, bp, (uint)bytes.Length);
            }
        }
    }

    /// <summary>Observe a value in a named histogram.</summary>
    public void MetricsObserveHistogram(string name, double value)
    {
        unsafe
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(name);
            fixed (byte* bp = bytes)
            {
                NativeBridge.VelocityEngineMetricsObserveHistogram(_engineHandle, bp, (uint)bytes.Length, value);
            }
        }
    }

    // ─── History Store Enhanced (Batch 23) ───────────────────────────────

    /// <summary>Get the event count for a workflow's history.</summary>
    public ulong HistoryEventCount(ulong workflowKey) =>
        NativeBridge.VelocityEngineHistoryEventCount(_engineHandle, workflowKey);

    /// <summary>Remove a workflow's history.</summary>
    public bool HistoryRemove(ulong workflowKey) =>
        NativeBridge.VelocityEngineHistoryRemove(_engineHandle, workflowKey) == 1;

    // ─── Archive Store Enhanced (Batch 23) ───────────────────────────────

    /// <summary>Retrieve an archived workflow record. Returns null if not found.</summary>
    public ArchiveRecordInfo? ArchiveRetrieve(ulong workflowKey)
    {
        unsafe
        {
            var fields = new ulong[5];
            fixed (ulong* fp = fields)
            {
                if (NativeBridge.VelocityEngineArchiveRetrieve(_engineHandle, workflowKey, fp) != 1)
                    return null;
                return new ArchiveRecordInfo
                {
                    WorkflowKey = fields[0],
                    NamespaceId = fields[1],
                    WorkflowTypeId = fields[2],
                    Status = (int)fields[3],
                    EventCount = fields[4],
                };
            }
        }
    }

    /// <summary>Delete an archived workflow.</summary>
    public bool ArchiveDelete(ulong workflowKey) =>
        NativeBridge.VelocityEngineArchiveDelete(_engineHandle, workflowKey) == 1;

    /// <summary>Count archived workflows by status (1=Running,2=Completed,3=Failed,4=Canceled,5=Terminated,6=ContinuedAsNew,7=TimedOut).</summary>
    public ulong ArchiveCountByStatus(int status) =>
        NativeBridge.VelocityEngineArchiveCountByStatus(_engineHandle, (uint)status);

    // ─── Namespace Enhanced (Batch 24) ───────────────────────────────────

    /// <summary>Describe a namespace by ID. Returns null if not found.</summary>
    public NamespaceDescription? DescribeNamespace(ulong namespaceId)
    {
        unsafe
        {
            var fields = new ulong[5];
            fixed (ulong* fp = fields)
            {
                if (NativeBridge.VelocityEngineDescribeNamespace(_engineHandle, namespaceId, fp) != 1)
                    return null;
                return new NamespaceDescription
                {
                    NamespaceId = fields[0],
                    RetentionMs = fields[1],
                    MaxConcurrentWorkflows = fields[2],
                    WorkflowCount = fields[3],
                    IsActive = fields[4] == 1,
                };
            }
        }
    }

    /// <summary>Get workflow count for a namespace.</summary>
    public ulong NamespaceWorkflowCount(ulong namespaceId) =>
        NativeBridge.VelocityEngineNamespaceWorkflowCount(_engineHandle, namespaceId);

    /// <summary>Deactivate a namespace (stop accepting new workflows).</summary>
    public bool DeactivateNamespace(ulong namespaceId) =>
        NativeBridge.VelocityEngineDeactivateNamespace(_engineHandle, namespaceId) == 1;

    /// <summary>Activate a namespace (resume accepting workflows).</summary>
    public bool ActivateNamespace(ulong namespaceId) =>
        NativeBridge.VelocityEngineActivateNamespace(_engineHandle, namespaceId) == 1;

    // ─── Cron Enhanced (Batch 24) ────────────────────────────────────────

    /// <summary>Get next fire time for a cron schedule.</summary>
    public ulong CronNextFireTime(ulong scheduleId) =>
        NativeBridge.VelocityEngineCronNextFireTime(_engineHandle, scheduleId);

    /// <summary>Get fire count for a cron schedule.</summary>
    public ulong CronFireCount(ulong scheduleId) =>
        NativeBridge.VelocityEngineCronFireCount(_engineHandle, scheduleId);

    /// <summary>Unregister a cron schedule.</summary>
    public bool CronUnregister(ulong scheduleId) =>
        NativeBridge.VelocityEngineCronUnregister(_engineHandle, scheduleId) == 1;

    /// <summary>Pause or resume a cron schedule.</summary>
    public bool CronSetPaused(ulong scheduleId, bool paused) =>
        NativeBridge.VelocityEngineCronSetPaused(_engineHandle, scheduleId, paused ? 1 : 0) == 1;

    // ─── Codec Enhanced (Batch 24) ───────────────────────────────────────

    /// <summary>Get the number of codecs in the codec chain.</summary>
    public ulong CodecCount() =>
        NativeBridge.VelocityEngineCodecCount(_engineHandle);

    // ─── Search Attr Enhanced (Batch 24) ─────────────────────────────────

    /// <summary>Get the number of search attributes for a workflow.</summary>
    public ulong SearchAttrCount(ulong workflowKey) =>
        NativeBridge.VelocityEngineSearchAttrCount(_engineHandle, workflowKey);

    // ─── Cloud Storage Adapter (Batch 28) ────────────────────────────────

    /// <summary>Switch the cloud storage backend. 0 = MockS3, 1 = MockGCS.</summary>
    public bool CloudSetBackend(uint backend) =>
        NativeBridge.VelocityEngineCloudStorageSetBackend(_engineHandle, backend) == 1;

    /// <summary>Archive a workflow to cloud storage.</summary>
    public bool CloudArchive(ulong workflowKey, ulong namespaceId, int status) =>
        NativeBridge.VelocityEngineCloudArchive(_engineHandle, workflowKey, namespaceId, status) == 1;

    /// <summary>Check if a workflow exists in cloud storage.</summary>
    public bool CloudContains(ulong workflowKey) =>
        NativeBridge.VelocityEngineCloudContains(_engineHandle, workflowKey) == 1;

    /// <summary>Delete a workflow from cloud storage.</summary>
    public bool CloudDelete(ulong workflowKey) =>
        NativeBridge.VelocityEngineCloudDelete(_engineHandle, workflowKey) == 1;

    /// <summary>Get the total count of records in cloud storage.</summary>
    public ulong CloudCount() =>
        NativeBridge.VelocityEngineCloudCount(_engineHandle);

    /// <summary>List workflow keys in cloud storage by namespace.</summary>
    public ulong[] CloudListByNamespace(ulong namespaceId)
    {
        const int maxKeys = 1024;
        var buffer = new ulong[maxKeys];
        fixed (ulong* bp = buffer)
        {
            uint count = NativeBridge.VelocityEngineCloudListByNamespace(
                _engineHandle, namespaceId, bp, (uint)maxKeys);
            var result = new ulong[count];
            Array.Copy(buffer, result, count);
            return result;
        }
    }

    /// <summary>Garbage collect cloud storage records older than retentionMs.</summary>
    public int CloudGc(ulong retentionMs) =>
        NativeBridge.VelocityEngineCloudGc(_engineHandle, retentionMs);

    /// <summary>Get the cloud storage backend name.</summary>
    public string CloudBackendName()
    {
        var buf = new byte[64];
        fixed (byte* bp = buf)
        {
            uint len = NativeBridge.VelocityEngineCloudBackendName(_engineHandle, bp, (uint)buf.Length);
            return System.Text.Encoding.UTF8.GetString(buf, 0, (int)len);
        }
    }

    // ─── Query/Reset Enhanced (Batch 28) ─────────────────────────────────

    /// <summary>Unregister all query handlers for a workflow.</summary>
    public void UnregisterQueryHandler(ulong workflowKey) =>
        NativeBridge.VelocityEngineUnregisterQueryHandler(_engineHandle, workflowKey);

    /// <summary>Get reset point event IDs for a workflow.</summary>
    public ulong[] GetResetPoints(ulong workflowKey)
    {
        const int maxPoints = 64;
        var buffer = new ulong[maxPoints];
        fixed (ulong* bp = buffer)
        {
            uint count = NativeBridge.VelocityEngineGetResetPoints(
                _engineHandle, workflowKey, bp, (uint)maxPoints);
            var result = new ulong[count];
            Array.Copy(buffer, result, count);
            return result;
        }
    }

    // ─── Visibility Listing Enhanced (Batch 29) ───────────────────────────

    /// <summary>List workflows by search attribute (string value).</summary>
    public ulong[] ListBySearchAttribute(string key, string value)
    {
        var keyBytes = System.Text.Encoding.UTF8.GetBytes(key);
        var valBytes = System.Text.Encoding.UTF8.GetBytes(value);
        const int maxResults = 256;
        var buffer = new ulong[maxResults];
        unsafe
        {
            fixed (byte* kp = keyBytes)
            fixed (byte* vp = valBytes)
            fixed (ulong* bp = buffer)
            {
                uint count = NativeBridge.VelocityEngineListBySearchAttribute(
                    _engineHandle, kp, (uint)keyBytes.Length, vp, (uint)valBytes.Length, bp, (uint)maxResults);
                var result = new ulong[count];
                Array.Copy(buffer, result, count);
                return result;
            }
        }
    }

    /// <summary>List workflows by time range.</summary>
    public ulong[] ListByTimeRange(ulong startTimeMs, ulong endTimeMs)
    {
        const int maxResults = 256;
        var buffer = new ulong[maxResults];
        unsafe
        {
            fixed (ulong* bp = buffer)
            {
                uint count = NativeBridge.VelocityEngineListByTimeRange(
                    _engineHandle, startTimeMs, endTimeMs, bp, (uint)maxResults);
                var result = new ulong[count];
                Array.Copy(buffer, result, count);
                return result;
            }
        }
    }

    // ─── Replay Cache Management (Batch 29) ───────────────────────────────

    /// <summary>Invalidate replay cache for a specific workflow.</summary>
    public void ReplayInvalidate(ulong workflowKey) =>
        NativeBridge.VelocityEngineReplayInvalidate(_engineHandle, workflowKey);

    /// <summary>Clear the entire replay cache.</summary>
    public void ReplayClearCache() =>
        NativeBridge.VelocityEngineReplayClearCache(_engineHandle);

    /// <summary>Get the replay cache size.</summary>
    public ulong ReplayCacheSize() =>
        NativeBridge.VelocityEngineReplayCacheSize(_engineHandle);

    // ─── Schedule Management Enhanced (Batch 29) ──────────────────────────

    /// <summary>Set overlap policy for a schedule. Returns true on success.</summary>
    public bool ScheduleSetOverlapPolicy(ulong scheduleId, uint policy) =>
        NativeBridge.VelocityEngineScheduleSetOverlapPolicy(_engineHandle, scheduleId, policy) == 1;

    /// <summary>Set remaining actions for a schedule. Returns true on success.</summary>
    public bool ScheduleSetRemainingActions(ulong scheduleId, ulong remaining) =>
        NativeBridge.VelocityEngineScheduleSetRemainingActions(_engineHandle, scheduleId, remaining) == 1;

    // ─── Event History Enhanced (Batch 29) ────────────────────────────────

    /// <summary>Get the number of workflows with event history.</summary>
    public ulong HistoryWorkflowCount() =>
        NativeBridge.VelocityEngineHistoryWorkflowCountV2(_engineHandle);

    // ─── Partition Worker Management (Batch 29) ───────────────────────────

    /// <summary>Get total pending tasks across all partitions for a task queue.</summary>
    public ulong PartitionTotalPending(ulong taskQueueHash) =>
        NativeBridge.VelocityEnginePartitionTotalPending(_engineHandle, taskQueueHash);

    // ─── Nexus Enhanced (Batch 29) ────────────────────────────────────────

    /// <summary>Register a Nexus service with endpoint. Returns true on success.</summary>
    public bool NexusRegisterService(string serviceName, string endpoint)
    {
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(serviceName);
        var epBytes = System.Text.Encoding.UTF8.GetBytes(endpoint);
        unsafe
        {
            fixed (byte* np = nameBytes)
            fixed (byte* ep = epBytes)
            {
                return NativeBridge.VelocityEngineNexusRegisterService(
                    _engineHandle, np, (uint)nameBytes.Length, ep, (uint)epBytes.Length) == 1;
            }
        }
    }

    // ─── Real Cloud Storage SDK (Batch 29) ────────────────────────────────

    /// <summary>Switch to real AWS S3 cloud storage backend. Returns true on success (requires cloud-s3 feature).</summary>
    public bool CloudSetS3(string bucket, string region, string accessKey, string secretKey)
    {
        var bBytes = System.Text.Encoding.UTF8.GetBytes(bucket);
        var rBytes = System.Text.Encoding.UTF8.GetBytes(region);
        var akBytes = System.Text.Encoding.UTF8.GetBytes(accessKey);
        var skBytes = System.Text.Encoding.UTF8.GetBytes(secretKey);
        unsafe
        {
            fixed (byte* bp = bBytes)
            fixed (byte* rp = rBytes)
            fixed (byte* akp = akBytes)
            fixed (byte* skp = skBytes)
            {
                return NativeBridge.VelocityEngineCloudSetS3(
                    _engineHandle, bp, (uint)bBytes.Length, rp, (uint)rBytes.Length,
                    akp, (uint)akBytes.Length, skp, (uint)skBytes.Length) == 1;
            }
        }
    }

    /// <summary>Switch to real GCS cloud storage backend. Returns true on success (requires cloud-gcs feature).</summary>
    public bool CloudSetGcs(string bucket, string oauthToken)
    {
        var bBytes = System.Text.Encoding.UTF8.GetBytes(bucket);
        var tBytes = System.Text.Encoding.UTF8.GetBytes(oauthToken);
        unsafe
        {
            fixed (byte* bp = bBytes)
            fixed (byte* tp = tBytes)
            {
                return NativeBridge.VelocityEngineCloudSetGcs(
                    _engineHandle, bp, (uint)bBytes.Length, tp, (uint)tBytes.Length) == 1;
            }
        }
    }

    // ─── Search Attributes + Replication (Batch 30) ─────────────────────────

    /// <summary>Start a workflow with search attributes.</summary>
    public ulong StartWorkflowWithAttributes(ulong workflowId, ulong workflowTypeId, ulong namespaceId, ulong taskQueueHash, uint totalSteps, byte[]? input, Dictionary<string, string> searchAttributes)
    {
        var inputPtr = input != null && input.Length > 0 ? input : null;
        unsafe
        {
            fixed (byte* ip = inputPtr)
            {
                uint count = (uint)searchAttributes.Count;
                var keys = new byte[count][];
                var vals = new byte[count][];
                var keyPtrs = new byte*[count];
                var valPtrs = new byte*[count];
                var keyLens = stackalloc uint[(int)count];
                var valLens = stackalloc uint[(int)count];
                int idx = 0;
                foreach (var kv in searchAttributes)
                {
                    keys[idx] = System.Text.Encoding.UTF8.GetBytes(kv.Key);
                    vals[idx] = System.Text.Encoding.UTF8.GetBytes(kv.Value);
                    fixed (byte* kp = keys[idx]) { keyPtrs[idx] = kp; keyLens[idx] = (uint)keys[idx].Length; }
                    fixed (byte* vp = vals[idx]) { valPtrs[idx] = vp; valLens[idx] = (uint)vals[idx].Length; }
                    idx++;
                }
                fixed (byte** kpp = keyPtrs)
                fixed (byte** vpp = valPtrs)
                {
                    return NativeBridge.VelocityEngineStartWorkflowWithAttrs(
                        _engineHandle, workflowId, workflowTypeId, namespaceId, taskQueueHash,
                        totalSteps, ip, inputPtr != null ? (uint)inputPtr.Length : 0,
                        kpp, (uint*)keyLens, count, vpp, (uint*)valLens);
                }
            }
        }
    }

    /// <summary>Apply an incoming replication task from a remote cluster.</summary>
    public bool ApplyReplicationTask(ulong sourceClusterId, ulong targetClusterId, ulong workflowKey, uint eventType, byte[]? payload, ulong failoverVersion)
    {
        unsafe
        {
            fixed (byte* pp = payload)
            {
                return NativeBridge.VelocityEngineApplyReplicationTask(
                    _engineHandle, sourceClusterId, targetClusterId, workflowKey,
                    eventType, pp, payload != null ? (uint)payload.Length : 0, failoverVersion) == 1;
            }
        }
    }

    /// <summary>Process a fired timer to re-enqueue pending activity retries.</summary>
    public void ProcessFiredTimer(ulong workflowKey) =>
        NativeBridge.VelocityEngineProcessFiredTimer(_engineHandle, workflowKey);

    /// <summary>Get replication status: (pending_tasks, cluster_count, active_clusters).</summary>
    public (ulong Pending, ulong Clusters, ulong Active) ReplicationStatus()
    {
        unsafe
        {
            ulong* buf = stackalloc ulong[3];
            NativeBridge.VelocityEngineReplicationStatus(_engineHandle, buf);
            return (buf[0], buf[1], buf[2]);
        }
    }

    /// <summary>Set a cluster as active or standby.</summary>
    public bool SetClusterActive(ulong clusterId, bool active) =>
        NativeBridge.VelocityEngineSetClusterActive(_engineHandle, clusterId, active ? 1 : 0) == 1;

    /// <summary>Set failover version for a cluster.</summary>
    public bool SetFailoverVersion(ulong clusterId, ulong version) =>
        NativeBridge.VelocityEngineSetFailoverVersion(_engineHandle, clusterId, version) == 1;

    // ─── Nexus Full Lifecycle (Batch 31) ──────────────────────────────────

    /// <summary>Mark a nexus operation as started.</summary>
    public bool NexusMarkStarted(ulong opId) =>
        NativeBridge.VelocityEngineNexusMarkStarted(_engineHandle, opId) == 1;

    /// <summary>Cancel a nexus operation.</summary>
    public bool NexusCancel(ulong opId) =>
        NativeBridge.VelocityEngineNexusCancel(_engineHandle, opId) == 1;

    /// <summary>Timeout a nexus operation.</summary>
    public bool NexusTimeout(ulong opId) =>
        NativeBridge.VelocityEngineNexusTimeout(_engineHandle, opId) == 1;

    /// <summary>Retry a failed/timed-out nexus operation.</summary>
    public bool NexusRetry(ulong opId) =>
        NativeBridge.VelocityEngineNexusRetry(_engineHandle, opId) == 1;

    /// <summary>Count nexus operations by state (0-5).</summary>
    public ulong NexusCountByState(uint state) =>
        NativeBridge.VelocityEngineNexusCountByState(_engineHandle, state);

    // ─── Worker Registry Load-Aware Dispatch (Batch 31) ───────────────────

    /// <summary>Select the best worker for a task queue using load-aware dispatch.</summary>
    public ulong SelectWorker(ulong tqHash) =>
        NativeBridge.VelocityEngineSelectWorker(_engineHandle, tqHash);

    /// <summary>Check if a worker has capacity.</summary>
    public bool WorkerHasCapacity(ulong workerId) =>
        NativeBridge.VelocityEngineWorkerHasCapacity(_engineHandle, workerId) == 1;

    /// <summary>Drain a worker (stop dispatching new tasks).</summary>
    public bool DrainWorker(ulong workerId) =>
        NativeBridge.VelocityEngineDrainWorker(_engineHandle, workerId) == 1;

    /// <summary>Get total current load across all workers.</summary>
    public ulong TotalWorkerLoad() =>
        NativeBridge.VelocityEngineTotalWorkerLoad(_engineHandle);

    /// <summary>Get total available capacity across all workers.</summary>
    public ulong TotalWorkerCapacity() =>
        NativeBridge.VelocityEngineTotalWorkerCapacity(_engineHandle);

    // ─── Consistent Hash Ring Sharding (Batch 31) ─────────────────────────

    /// <summary>Add a host to the consistent hash ring.</summary>
    public unsafe void ShardingAddHost(string host)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(host);
        fixed (byte* ptr = bytes)
            NativeBridge.VelocityEngineShardingAddHost(_engineHandle, ptr, (uint)bytes.Length);
    }

    /// <summary>Remove a host from the consistent hash ring.</summary>
    public unsafe bool ShardingRemoveHost(string host)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(host);
        fixed (byte* ptr = bytes)
            return NativeBridge.VelocityEngineShardingRemoveHost(_engineHandle, ptr, (uint)bytes.Length) == 1;
    }

    /// <summary>Migrate a shard to a new host.</summary>
    public unsafe bool ShardingMigrate(uint shardId, string host)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(host);
        fixed (byte* ptr = bytes)
            return NativeBridge.VelocityEngineShardingMigrate(_engineHandle, shardId, ptr, (uint)bytes.Length) == 1;
    }

    /// <summary>Get number of hosts on the hash ring.</summary>
    public ulong ShardingHostCount() =>
        NativeBridge.VelocityEngineShardingHostCount(_engineHandle);

    // ─── Hierarchical Partitions (Batch 31) ───────────────────────────────

    /// <summary>Create a child partition under an existing parent.</summary>
    public ulong CreateChildPartition(uint parentId, ulong tqHash) =>
        NativeBridge.VelocityEngineCreateChildPartition(_engineHandle, parentId, tqHash);

    /// <summary>Delete a partition.</summary>
    public bool DeletePartition(uint partitionId) =>
        NativeBridge.VelocityEngineDeletePartition(_engineHandle, partitionId) == 1;

    /// <summary>Get the depth of a partition in the hierarchy.</summary>
    public int PartitionDepth(uint partitionId) =>
        NativeBridge.VelocityEnginePartitionDepth(_engineHandle, partitionId);

    /// <summary>Get total backlog across all partitions for a task queue.</summary>
    public ulong PartitionBacklog(ulong tqHash) =>
        NativeBridge.VelocityEnginePartitionBacklog(_engineHandle, tqHash);

    // ─── Get Workflow Search Attributes (Batch 32) ─────────────────────────

    /// <summary>Get search attributes for a running workflow.</summary>
    public unsafe Dictionary<string, string> GetWorkflowSearchAttributes(ulong workflowKey)
    {
        var result = new Dictionary<string, string>();
        var count = NativeBridge.VelocityEngineGetSearchAttrCount(_engineHandle, workflowKey);
        var keyBuf = stackalloc byte[256];
        var valBuf = stackalloc byte[512];
        for (ulong i = 0; i < count; i++)
        {
            var keyLen = NativeBridge.VelocityEngineGetSearchAttrKey(_engineHandle, workflowKey, i, keyBuf, 256);
            var valLen = NativeBridge.VelocityEngineGetSearchAttrVal(_engineHandle, workflowKey, i, valBuf, 512);
            if (keyLen > 0 && valLen > 0)
            {
                var key = System.Text.Encoding.UTF8.GetString(keyBuf, (int)keyLen);
                var val = System.Text.Encoding.UTF8.GetString(valBuf, (int)valLen);
                result[key] = val;
            }
        }
        return result;
    }

    // ─── Replication Transport (Batch 33) ──────────────────────────────────

    /// <summary>Add a replication link to a remote cluster.</summary>
    public unsafe void ReplicationAddLink(string clusterName, ulong clusterId, string endpoint)
    {
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(clusterName);
        var epBytes = System.Text.Encoding.UTF8.GetBytes(endpoint);
        fixed (byte* namePtr = nameBytes)
        fixed (byte* epPtr = epBytes)
        {
            NativeBridge.VelocityEngineReplAddLink(_engineHandle, namePtr, (uint)nameBytes.Length, clusterId, epPtr, (uint)epBytes.Length);
        }
    }

    /// <summary>Remove a replication link.</summary>
    public bool ReplicationRemoveLink(ulong clusterId) =>
        NativeBridge.VelocityEngineReplRemoveLink(_engineHandle, clusterId);

    /// <summary>Set a replication link active/inactive.</summary>
    public bool ReplicationSetLinkActive(ulong clusterId, bool active) =>
        NativeBridge.VelocityEngineReplSetLinkActive(_engineHandle, clusterId, active);

    /// <summary>Pull outgoing replication tasks for a remote cluster.</summary>
    public uint ReplicationPullForCluster(ulong clusterId, uint maxCount) =>
        NativeBridge.VelocityEngineReplPullForCluster(_engineHandle, clusterId, maxCount);

    /// <summary>Push incoming replication tasks from a remote cluster.</summary>
    public unsafe bool ReplicationPushFromCluster(ulong clusterId, ulong workflowKey, uint eventType, byte[]? payload, ulong failoverVersion, ulong lastEventId)
    {
        if (payload == null || payload.Length == 0)
            return NativeBridge.VelocityEngineReplPushFromCluster(_engineHandle, clusterId, workflowKey, eventType, null, 0, failoverVersion, lastEventId);
        fixed (byte* ptr = payload)
        {
            return NativeBridge.VelocityEngineReplPushFromCluster(_engineHandle, clusterId, workflowKey, eventType, ptr, (uint)payload.Length, failoverVersion, lastEventId);
        }
    }

    /// <summary>Get the number of active replication links.</summary>
    public ulong ReplicationActiveLinkCount() =>
        NativeBridge.VelocityEngineReplActiveLinkCount(_engineHandle);

    /// <summary>Get total pending outgoing tasks.</summary>
    public ulong ReplicationTotalPendingOutgoing() =>
        NativeBridge.VelocityEngineReplTotalPendingOutgoing(_engineHandle);

    /// <summary>Get total pending incoming tasks.</summary>
    public ulong ReplicationTotalPendingIncoming() =>
        NativeBridge.VelocityEngineReplTotalPendingIncoming(_engineHandle);

    // ── Replication Daemon (Batch 34) ──────────────────────────────────────
    public bool ReplicationDaemonStart() => NativeBridge.VelocityEngineReplDaemonStart(_engineHandle) == 1;
    public bool ReplicationDaemonStop() => NativeBridge.VelocityEngineReplDaemonStop(_engineHandle) == 1;
    public bool ReplicationDaemonIsRunning() => NativeBridge.VelocityEngineReplDaemonIsRunning(_engineHandle) == 1;
    public (ulong Delivered, ulong Applied) ReplicationDaemonPollOnce()
    {
        var result = NativeBridge.VelocityEngineReplDaemonPollOnce(_engineHandle);
        return ((result >> 32) & 0xFFFFFFFF, result & 0xFFFFFFFF);
    }
    public ulong ReplicationDaemonStatCycles() => NativeBridge.VelocityEngineReplDaemonStatCycles(_engineHandle);
    public ulong ReplicationDaemonStatDelivered() => NativeBridge.VelocityEngineReplDaemonStatDelivered(_engineHandle);
    public ulong ReplicationDaemonStatApplied() => NativeBridge.VelocityEngineReplDaemonStatApplied(_engineHandle);
    public ulong ReplicationDaemonStatFailures() => NativeBridge.VelocityEngineReplDaemonStatFailures(_engineHandle);
    public ulong ReplicationDaemonStatUptime() => NativeBridge.VelocityEngineReplDaemonStatUptime(_engineHandle);
    public ulong ReplicationDaemonDeliveryCount() => NativeBridge.VelocityEngineReplDaemonDeliveryCount(_engineHandle);

    public void Dispose()
    {
        if (!_disposed && _engineHandle is not null)
        {
            NativeBridge.VelocityEngineDestroy(_engineHandle);
            _engineHandle = null;
            _disposed = true;
        }
    }
}

/// <summary>
/// Result of polling the Rust task queue.
/// </summary>
public sealed class PolledTask
{
    public TaskKind TaskKind { get; set; }
    public ulong WorkflowKey { get; set; }
    public uint StepIndex { get; set; }
    public ulong ActivityNameId { get; set; }
    public ulong TaskId { get; set; }
    public uint Attempt { get; set; }
}

/// <summary>
/// Task types dispatched by the Rust engine.
/// </summary>
public enum TaskKind : uint
{
    WorkflowTask = 0,
    ActivityTask = 1,
    TimerTask = 2,
    SignalTask = 3
}

/// <summary>
/// Workflow execution lifecycle states, mirroring the Rust engine's WorkflowStatus.
/// </summary>
public enum WorkflowExecutionStatus
{
    Void = 0,
    Running = 1,
    Completed = 2,
    Failed = 3,
    Canceled = 4,
    Terminated = 5,
    ContinuedAsNew = 6,
    TimedOut = 7
}

/// <summary>
/// Visibility information about a workflow execution, returned by ListWorkflows.
/// </summary>
public sealed class WorkflowVisibilityInfo
{
    public ulong WorkflowKey { get; init; }
    public ulong WorkflowId { get; init; }
    public ulong RunId { get; init; }
    public ulong WorkflowTypeId { get; init; }
    public ulong NamespaceId { get; init; }
    public WorkflowExecutionStatus Status { get; init; }
    public ulong StartTimeMs { get; init; }
    public ulong? CloseTimeMs { get; init; }
    public ulong TaskQueueHash { get; init; }
}

/// <summary>
/// A single event in a workflow's execution history.
/// </summary>
public sealed class HistoryEventInfo
{
    public ulong EventId { get; init; }
    public uint EventType { get; init; }
    public byte[]? Payload { get; init; }
}

/// <summary>
/// Detailed history event with timestamp, returned by GetHistoryEvents.
/// </summary>
public sealed class HistoryEventDetail
{
    public ulong EventId { get; init; }
    public int EventType { get; init; }
    public ulong TimestampMs { get; init; }
    public byte[]? Payload { get; init; }
}

/// <summary>
/// Information about a registered namespace.
/// </summary>
public sealed class NamespaceInfo
{
    public ulong Id { get; init; }
    public string Name { get; init; } = "";
    public bool IsActive { get; init; }
    public long RetentionDays { get; init; }
}

/// <summary>
/// Rich description of a workflow execution, returned by DescribeWorkflow.
/// Includes status, step progress, timing, search attributes, and memo count.
/// </summary>
public sealed class WorkflowDescription
{
    public ulong WorkflowKey { get; init; }
    public WorkflowExecutionStatus Status { get; init; }
    public uint TotalSteps { get; init; }
    public uint CompletedSteps { get; init; }
    public ulong EventSequence { get; init; }
    public ulong StartTimeMs { get; init; }
    public ulong? CloseTimeMs { get; init; }
    public ulong WorkflowTypeId { get; init; }
    public ulong NamespaceId { get; init; }
    public ulong TaskQueueHash { get; init; }
    public int SearchAttributeCount { get; init; }
    public int MemoCount { get; init; }
}

/// <summary>
/// Description of a schedule, returned by DescribeSchedule.
/// </summary>
public sealed class ScheduleDescription
{
    public ulong ScheduleId { get; init; }
    public ulong WorkflowTypeId { get; init; }
    public ulong NamespaceId { get; init; }
    public ulong TaskQueueHash { get; init; }
    public int OverlapPolicy { get; init; }
    public ulong ActionCount { get; init; }
    public bool IsPaused { get; init; }
}

/// <summary>
/// Cluster information, returned by GetClusterInfo.
/// </summary>
public sealed class ClusterInfo
{
    public ulong ClusterId { get; init; }
    public bool IsActive { get; init; }
    public ulong FailoverVersion { get; init; }
    public bool ReplicationEnabled { get; init; }
}

/// <summary>
/// Nexus operation information, returned by NexusGetOperation.
/// </summary>
public sealed class NexusOperationInfo
{
    public ulong OperationId { get; init; }
    public ulong WorkflowKey { get; init; }
    /// <summary>State: 0=Scheduled, 1=Started, 2=Completed, 3=Failed, 4=Canceled, 5=TimedOut</summary>
    public int State { get; init; }
    public bool HasResult { get; init; }
}

/// <summary>
/// Archived workflow record information, returned by ArchiveRetrieve.
/// </summary>
public sealed class ArchiveRecordInfo
{
    public ulong WorkflowKey { get; init; }
    public ulong NamespaceId { get; init; }
    public ulong WorkflowTypeId { get; init; }
    /// <summary>Status: 1=Running, 2=Completed, 3=Failed, 4=Canceled, 5=Terminated, 6=ContinuedAsNew, 7=TimedOut</summary>
    public int Status { get; init; }
    public ulong EventCount { get; init; }
}

/// <summary>
/// Namespace description, returned by DescribeNamespace.
/// </summary>
public sealed class NamespaceDescription
{
    public ulong NamespaceId { get; init; }
    public ulong RetentionMs { get; init; }
    public ulong MaxConcurrentWorkflows { get; init; }
    public ulong WorkflowCount { get; init; }
    public bool IsActive { get; init; }
}

/// <summary>
/// Patch info, returned by GetPatch.
/// </summary>
public sealed class PatchInfo
{
    public ulong PatchId { get; init; }
    public ulong WorkflowTypeId { get; init; }
    public ulong MinVersion { get; init; }
    public ulong MaxVersion { get; init; }
    public bool IsActive { get; init; }
}

/// <summary>
/// Saga execution info, returned by GetSagaInfo.
/// </summary>
public sealed class SagaInfo
{
    public ulong SagaId { get; init; }
    public ulong WorkflowKey { get; init; }
    public uint CurrentStep { get; init; }
    public uint StepCount { get; init; }
    public int Status { get; init; }
}

/// <summary>
/// Partition info, returned by DescribePartition.
/// </summary>
public sealed class PartitionInfo
{
    public uint PartitionId { get; init; }
    public ulong TaskQueueHash { get; init; }
    public ulong PendingTasks { get; init; }
    public ulong WorkerCount { get; init; }
    public uint? ParentPartition { get; init; }
    public double ForwardRate { get; init; }
}
