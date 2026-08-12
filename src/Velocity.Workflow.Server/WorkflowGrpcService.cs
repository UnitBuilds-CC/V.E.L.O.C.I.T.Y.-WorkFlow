using Grpc.Core;
using Velocity.Workflow.Core;
using Velocity.Workflow.Server.Protos;

namespace Velocity.Workflow.Server;

/// <summary>
/// gRPC service implementation that wraps the Rust-backed WorkflowRuntime.
/// Provides the external API for workflow lifecycle, visibility, namespace management,
/// activity task dispatch, and advanced operations — mirroring Temporal's WorkflowService gRPC API.
/// </summary>
public class WorkflowGrpcService : WorkflowService.WorkflowServiceBase
{
    private readonly WorkflowRuntime _runtime;
    private readonly ILogger<WorkflowGrpcService> _logger;

    public WorkflowGrpcService(WorkflowRuntime runtime, ILogger<WorkflowGrpcService> logger)
    {
        _runtime = runtime;
        _logger = logger;
    }

    // ─── Workflow Lifecycle ───────────────────────────────────────────────────

    public override Task<StartWorkflowResponse> StartWorkflow(StartWorkflowRequest request, ServerCallContext context)
    {
        ulong workflowId = (ulong)request.WorkflowId.GetHashCode();
        ulong workflowTypeId = (ulong)request.WorkflowType.GetHashCode();
        ulong namespaceId = ResolveNamespaceId(request.NamespaceName);
        ulong taskQueueHash = (ulong)request.TaskQueue.GetHashCode();

        byte[]? input = request.Input.IsEmpty ? null : request.Input.ToByteArray();
        ulong key = _runtime.StartWorkflow(workflowId, workflowTypeId, namespaceId, taskQueueHash, 1, input);

        // Set search attributes from the request
        foreach (var attr in request.SearchAttributes)
        {
            _runtime.SetSearchAttribute(key, attr.Key, attr.Value);
        }

        // Set memo from the request
        foreach (var memo in request.Memo)
        {
            _runtime.SetMemo(key, memo.Key, memo.Value.ToByteArray());
        }

        _logger.LogInformation("Started workflow {WorkflowId} (key={Key})", request.WorkflowId, key);

        return Task.FromResult(new StartWorkflowResponse { RunId = key.ToString() });
    }

    public override Task<SignalWorkflowResponse> SignalWorkflow(SignalWorkflowRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        ulong signalNameId = (ulong)request.SignalName.GetHashCode();
        byte[]? payload = request.Payload.IsEmpty ? null : request.Payload.ToByteArray();

        _runtime.Signal(workflowKey, signalNameId, payload);
        return Task.FromResult(new SignalWorkflowResponse());
    }

    public override Task<QueryWorkflowResponse> QueryWorkflow(QueryWorkflowRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        ulong queryNameId = (ulong)request.QueryType.GetHashCode();
        byte[]? input = request.Payload.IsEmpty ? null : request.Payload.ToByteArray();

        // Dispatch to registered query handler; fall back to status if no handler
        byte[]? result = _runtime.ExecuteQuery(workflowKey, queryNameId, input);
        if (result is null || result.Length == 0)
        {
            var status = _runtime.GetStatus(workflowKey);
            result = [(byte)status];
        }

        return Task.FromResult(new QueryWorkflowResponse
        {
            Result = Google.Protobuf.ByteString.CopyFrom(result)
        });
    }

    public override Task<TerminateWorkflowResponse> TerminateWorkflow(TerminateWorkflowRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        _runtime.TerminateWorkflow(workflowKey);
        _logger.LogInformation("Terminated workflow {WorkflowId}: {Reason}", request.WorkflowId, request.Reason);
        return Task.FromResult(new TerminateWorkflowResponse());
    }

    public override Task<CancelWorkflowResponse> CancelWorkflow(CancelWorkflowRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        _runtime.CancelWorkflow(workflowKey);
        return Task.FromResult(new CancelWorkflowResponse());
    }

    public override Task<DescribeWorkflowResponse> DescribeWorkflow(DescribeWorkflowRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        var status = _runtime.GetStatus(workflowKey);

        var info = new WorkflowExecutionInfo
        {
            WorkflowId = request.WorkflowId,
            RunId = request.RunId,
            Status = MapStatus(status),
            NamespaceName = request.NamespaceName,
        };

        return Task.FromResult(new DescribeWorkflowResponse { ExecutionInfo = info });
    }

    // ─── Visibility ──────────────────────────────────────────────────────────

    public override Task<CountWorkflowsResponse> CountWorkflows(CountWorkflowsRequest request, ServerCallContext context)
    {
        ulong count = _runtime.VisibilityCount;
        return Task.FromResult(new CountWorkflowsResponse { Count = (long)count });
    }

    public override Task<ListWorkflowsResponse> ListWorkflows(ListWorkflowsRequest request, ServerCallContext context)
    {
        var response = new ListWorkflowsResponse();

        // Use SQL visibility query if provided, otherwise fall back to simple filtering
        List<WorkflowVisibilityInfo> workflows;
        if (!string.IsNullOrEmpty(request.Query))
        {
            // Execute SQL-like visibility query
            workflows = _runtime.ExecuteVisibilityQuery(request.Query);
        }
        else
        {
            // Fall back to simple namespace/status filtering
            ulong nsFilter = ulong.MaxValue;
            if (!string.IsNullOrEmpty(request.NamespaceName) && request.NamespaceName != "default")
            {
                nsFilter = (ulong)request.NamespaceName.GetHashCode();
            }
            workflows = _runtime.ListWorkflows(nsFilter, -1);
        }

        foreach (var wf in workflows)
        {
            var execInfo = new WorkflowExecutionInfo
            {
                WorkflowId = wf.WorkflowId.ToString(),
                RunId = wf.RunId.ToString(),
                WorkflowType = wf.WorkflowTypeId.ToString(),
                NamespaceName = wf.NamespaceId.ToString(),
                Status = MapStatus(wf.Status),
                TaskQueue = wf.TaskQueueHash.ToString(),
            };
            if (wf.StartTimeMs > 0)
                execInfo.StartTimeUnixNano = (long)(wf.StartTimeMs * 1_000_000);
            if (wf.CloseTimeMs.HasValue)
                execInfo.CloseTimeUnixNano = (long)(wf.CloseTimeMs.Value * 1_000_000);

            response.Executions.Add(execInfo);
        }

        return Task.FromResult(response);
    }

    // ─── Namespace Management ────────────────────────────────────────────────

    public override Task<RegisterNamespaceResponse> RegisterNamespace(RegisterNamespaceRequest request, ServerCallContext context)
    {
        ulong nsId = _runtime.RegisterNamespace(request.NamespaceName);
        _logger.LogInformation("Registered namespace '{Name}' (id={Id})", request.NamespaceName, nsId);
        return Task.FromResult(new RegisterNamespaceResponse { NamespaceId = nsId.ToString() });
    }

    public override Task<DescribeNamespaceResponse> DescribeNamespace(DescribeNamespaceRequest request, ServerCallContext context)
    {
        var info = new Protos.NamespaceInfo
        {
            Name = request.NamespaceName,
            IsActive = true,
        };
        return Task.FromResult(new DescribeNamespaceResponse { Info = info });
    }

    public override Task<ListNamespacesResponse> ListNamespaces(ListNamespacesRequest request, ServerCallContext context)
    {
        var response = new ListNamespacesResponse();
        var namespaces = _runtime.ListNamespaces();
        foreach (var ns in namespaces)
        {
            response.Namespaces.Add(new Protos.NamespaceInfo
            {
                Name = ns.Name,
                Id = ns.Id.ToString(),
                IsActive = ns.IsActive,
                RetentionDays = ns.RetentionDays,
            });
        }
        return Task.FromResult(response);
    }

    // ─── Activity Task Dispatch ──────────────────────────────────────────────

    public override Task<PollActivityTaskQueueResponse> PollActivityTaskQueue(PollActivityTaskQueueRequest request, ServerCallContext context)
    {
        ulong taskQueueHash = (ulong)request.TaskQueue.GetHashCode();
        var task = _runtime.PollTask(taskQueueHash);

        if (task is null)
        {
            return Task.FromResult(new PollActivityTaskQueueResponse());
        }

        var response = new PollActivityTaskQueueResponse
        {
            TaskToken = $"{task.WorkflowKey}:{task.StepIndex}",
            WorkflowId = task.WorkflowKey.ToString(),
            RunId = task.WorkflowKey.ToString(),
            ActivityType = task.ActivityNameId.ToString(),
            Attempt = (int)task.Attempt,
        };

        return Task.FromResult(response);
    }

    public override Task<RespondActivityTaskCompletedResponse> RespondActivityTaskCompleted(
        RespondActivityTaskCompletedRequest request, ServerCallContext context)
    {
        // Parse task token: "workflowKey:step"
        var parts = request.TaskToken.Split(':');
        if (parts.Length == 2 && ulong.TryParse(parts[0], out ulong workflowKey) && uint.TryParse(parts[1], out uint step))
        {
            byte[]? result = request.Result.IsEmpty ? null : request.Result.ToByteArray();
            _runtime.CompleteActivity(workflowKey, step, result);
            _logger.LogDebug("Completed activity for workflow {WorkflowKey} step {Step}", workflowKey, step);
        }
        else
        {
            _logger.LogWarning("Invalid task token format: {Token}", request.TaskToken);
        }

        return Task.FromResult(new RespondActivityTaskCompletedResponse());
    }

    public override Task<RespondActivityTaskFailedResponse> RespondActivityTaskFailed(
        RespondActivityTaskFailedRequest request, ServerCallContext context)
    {
        var parts = request.TaskToken.Split(':');
        if (parts.Length == 2 && ulong.TryParse(parts[0], out ulong workflowKey) && uint.TryParse(parts[1], out uint step))
        {
            _runtime.FailActivity(workflowKey, step);
            _logger.LogWarning("Activity failed for workflow {WorkflowKey} step {Step}: {Message}",
                workflowKey, step, request.FailureMessage);
        }

        return Task.FromResult(new RespondActivityTaskFailedResponse());
    }

    // ─── Advanced Workflow Operations ────────────────────────────────────────

    public override Task<SignalWithStartResponse> SignalWithStart(SignalWithStartRequest request, ServerCallContext context)
    {
        ulong workflowId = (ulong)request.WorkflowId.GetHashCode();
        ulong workflowTypeId = (ulong)request.WorkflowType.GetHashCode();
        ulong namespaceId = ResolveNamespaceId(request.NamespaceName);
        ulong taskQueueHash = (ulong)request.TaskQueue.GetHashCode();
        ulong signalNameId = (ulong)request.SignalName.GetHashCode();
        byte[]? payload = request.SignalPayload.IsEmpty ? null : request.SignalPayload.ToByteArray();
        uint totalSteps = request.TotalSteps > 0 ? (uint)request.TotalSteps : 1;

        ulong key = _runtime.SignalWithStart(workflowId, workflowTypeId, namespaceId, taskQueueHash,
            totalSteps, signalNameId, out bool wasStarted, payload);

        _logger.LogInformation("SignalWithStart: workflow={WorkflowId} key={Key} started={Started}",
            request.WorkflowId, key, wasStarted);

        return Task.FromResult(new SignalWithStartResponse
        {
            RunId = key.ToString(),
            WasStarted = wasStarted,
        });
    }

    public override Task<ContinueAsNewResponse> ContinueAsNew(ContinueAsNewRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        byte[]? input = request.NewInput.IsEmpty ? null : request.NewInput.ToByteArray();

        ulong newKey = _runtime.ContinueAsNew(workflowKey, input);

        _logger.LogInformation("ContinueAsNew: old={OldKey} new={NewKey}", workflowKey, newKey);

        return Task.FromResult(new ContinueAsNewResponse { NewRunId = newKey.ToString() });
    }

    public override Task<GetWorkflowExecutionHistoryResponse> GetWorkflowExecutionHistory(
        GetWorkflowExecutionHistoryRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        var events = _runtime.GetEventHistory(workflowKey);

        var response = new GetWorkflowExecutionHistoryResponse
        {
            TotalEventCount = events.Count,
        };

        // Apply pagination
        int startIdx = 0;
        if (request.StartEventId > 0)
        {
            startIdx = events.FindIndex(e => e.EventId >= (ulong)request.StartEventId);
            if (startIdx < 0) startIdx = events.Count;
        }

        int pageSize = request.PageSize > 0 ? request.PageSize : 100;
        int endIdx = Math.Min(startIdx + pageSize, events.Count);

        for (int i = startIdx; i < endIdx; i++)
        {
            var evt = events[i];
            var historyEvent = new HistoryEvent
            {
                EventId = (long)evt.EventId,
                EventType = (int)evt.EventType,
            };
            if (evt.Payload is not null && evt.Payload.Length > 0)
            {
                historyEvent.Payload = Google.Protobuf.ByteString.CopyFrom(evt.Payload);
            }
            response.Events.Add(historyEvent);
        }

        return Task.FromResult(response);
    }

    public override Task<DescribeTaskQueueResponse> DescribeTaskQueue(DescribeTaskQueueRequest request, ServerCallContext context)
    {
        ulong taskQueueHash = (ulong)request.TaskQueue.GetHashCode();
        ulong pending = _runtime.PendingTasks(taskQueueHash);

        return Task.FromResult(new DescribeTaskQueueResponse
        {
            PendingTasks = (long)pending,
            TaskQueue = request.TaskQueue,
        });
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    // ─── Workflow Reset ──────────────────────────────────────────────────────

    public override Task<ResetWorkflowResponse> ResetWorkflow(ResetWorkflowRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        bool success = _runtime.ResetWorkflow(workflowKey, (ulong)request.ResetToEventId);
        _logger.LogInformation("ResetWorkflow: key={Key} eventId={EventId} success={Success}",
            workflowKey, request.ResetToEventId, success);
        return Task.FromResult(new ResetWorkflowResponse { Success = success });
    }

    // ─── Activity Heartbeat ──────────────────────────────────────────────────

    public override Task<RecordActivityHeartbeatResponse> RecordActivityHeartbeat(
        RecordActivityHeartbeatRequest request, ServerCallContext context)
    {
        // Parse task token: "workflowKey:step"
        var parts = request.TaskToken.Split(':');
        if (parts.Length == 2 && ulong.TryParse(parts[0], out ulong workflowKey) && uint.TryParse(parts[1], out uint step))
        {
            _runtime.RecordHeartbeat(workflowKey, step);
            _logger.LogDebug("Heartbeat recorded: workflow={Key} step={Step}", workflowKey, step);
        }
        return Task.FromResult(new RecordActivityHeartbeatResponse());
    }

    // ─── Workflow Update ─────────────────────────────────────────────────────

    public override Task<UpdateWorkflowExecutionResponse> UpdateWorkflowExecution(
        UpdateWorkflowExecutionRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        ulong updateNameId = (ulong)request.UpdateName.GetHashCode();
        byte[]? payload = request.Payload.IsEmpty ? null : request.Payload.ToByteArray();

        _runtime.Update(workflowKey, updateNameId, payload);
        _logger.LogInformation("UpdateWorkflow: key={Key} update={Update}", workflowKey, request.UpdateName);

        return Task.FromResult(new UpdateWorkflowExecutionResponse { Accepted = true });
    }

    // ─── Search Attributes ───────────────────────────────────────────────────

    public override Task<UpsertWorkflowSearchAttributesResponse> UpsertWorkflowSearchAttributes(
        UpsertWorkflowSearchAttributesRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        foreach (var attr in request.SearchAttributes)
        {
            _runtime.SetSearchAttribute(workflowKey, attr.Key, attr.Value);
        }
        _logger.LogInformation("UpsertSearchAttributes: key={Key} count={Count}",
            workflowKey, request.SearchAttributes.Count);
        return Task.FromResult(new UpsertWorkflowSearchAttributesResponse());
    }

    // ─── Replay / Recovery ───────────────────────────────────────────────────

    public override Task<ReplayWorkflowResponse> ReplayWorkflow(ReplayWorkflowRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        bool success = _runtime.Replay(workflowKey);
        int status = _runtime.ReplayStatus(workflowKey);
        uint eventsReplayed = _runtime.ReplayEventCount(workflowKey);
        uint stepsReconstructed = _runtime.ReplayStepCount(workflowKey);

        _logger.LogInformation("ReplayWorkflow: key={Key} success={Success} events={Events} steps={Steps}",
            workflowKey, success, eventsReplayed, stepsReconstructed);

        return Task.FromResult(new ReplayWorkflowResponse
        {
            Success = success,
            Status = status,
            EventsReplayed = (int)eventsReplayed,
            StepsReconstructed = (int)stepsReconstructed,
        });
    }

    // ─── Dynamic Configuration ───────────────────────────────────────────────

    public override Task<GetConfigResponse> GetConfig(GetConfigRequest request, ServerCallContext context)
    {
        long value = _runtime.ConfigGetInt(request.Key, request.DefaultValue);
        return Task.FromResult(new GetConfigResponse { Value = value });
    }

    public override Task<SetConfigResponse> SetConfig(SetConfigRequest request, ServerCallContext context)
    {
        _runtime.ConfigSetInt(request.Key, request.Value);
        _logger.LogInformation("SetConfig: key={Key} value={Value}", request.Key, request.Value);
        return Task.FromResult(new SetConfigResponse());
    }

    // ─── Worker Versioning ───────────────────────────────────────────────────

    public override Task<CreateWorkerVersionSetResponse> CreateWorkerVersionSet(
        CreateWorkerVersionSetRequest request, ServerCallContext context)
    {
        ulong setId = _runtime.CreateVersionSet();
        _logger.LogInformation("Created worker version set {SetId}", setId);
        return Task.FromResult(new CreateWorkerVersionSetResponse { VersionSetId = setId });
    }

    public override Task<AddBuildIdResponse> AddBuildId(AddBuildIdRequest request, ServerCallContext context)
    {
        bool success = _runtime.AddBuildId(request.VersionSetId, request.BuildId);
        _logger.LogInformation("AddBuildId: set={SetId} build={BuildId} success={Success}",
            request.VersionSetId, request.BuildId, success);
        return Task.FromResult(new AddBuildIdResponse());
    }

    // ─── Nexus Cross-Service ─────────────────────────────────────────────────

    public override Task<RegisterNexusServiceResponse> RegisterNexusService(
        RegisterNexusServiceRequest request, ServerCallContext context)
    {
        _runtime.RegisterNexusService(request.Name, request.Endpoint);
        _logger.LogInformation("Registered Nexus service '{Name}' at '{Endpoint}'", request.Name, request.Endpoint);
        return Task.FromResult(new RegisterNexusServiceResponse());
    }

    // ─── Schedules ───────────────────────────────────────────────────────────

    public override Task<CreateScheduleResponse> CreateSchedule(CreateScheduleRequest request, ServerCallContext context)
    {
        ulong workflowTypeId = (ulong)request.WorkflowType.GetHashCode();
        ulong namespaceId = ResolveNamespaceId(request.NamespaceName);
        ulong taskQueueHash = (ulong)request.TaskQueue.GetHashCode();

        ulong scheduleId = _runtime.CreateSchedule(workflowTypeId, namespaceId, taskQueueHash,
            request.OverlapPolicy, request.JitterSeconds);

        _logger.LogInformation("Created schedule {ScheduleId} for workflow type '{WorkflowType}'",
            scheduleId, request.WorkflowType);

        return Task.FromResult(new CreateScheduleResponse { ScheduleId = scheduleId });
    }

    public override Task<PauseScheduleResponse> PauseSchedule(PauseScheduleRequest request, ServerCallContext context)
    {
        _runtime.PauseSchedule(request.ScheduleId);
        return Task.FromResult(new PauseScheduleResponse());
    }

    public override Task<DeleteScheduleResponse> DeleteSchedule(DeleteScheduleRequest request, ServerCallContext context)
    {
        _runtime.DeleteSchedule(request.ScheduleId);
        return Task.FromResult(new DeleteScheduleResponse());
    }

    // ─── Memo ───────────────────────────────────────────────────────────────

    public override Task<SetMemoResponse> SetMemo(SetMemoRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        byte[]? value = request.Value.IsEmpty ? null : request.Value.ToByteArray();
        _runtime.SetMemo(workflowKey, request.Key, value);
        return Task.FromResult(new SetMemoResponse());
    }

    public override Task<GetMemoResponse> GetMemo(GetMemoRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        byte[]? value = _runtime.GetMemo(workflowKey, request.Key);
        var response = new GetMemoResponse();
        if (value is not null)
            response.Value = Google.Protobuf.ByteString.CopyFrom(value);
        return Task.FromResult(response);
    }

    // ─── Archival ───────────────────────────────────────────────────────────

    public override Task<ListArchivedWorkflowsResponse> ListArchivedWorkflows(
        ListArchivedWorkflowsRequest request, ServerCallContext context)
    {
        ulong nsId = ResolveNamespaceId(request.NamespaceName);
        ulong count = _runtime.ArchiveCountByNamespace(nsId);
        var response = new ListArchivedWorkflowsResponse
        {
            TotalCount = (long)count,
        };
        return Task.FromResult(response);
    }

    // ─── Cold Storage ───────────────────────────────────────────────────────

    public override Task<ArchiveWorkflowResponse> ArchiveWorkflow(ArchiveWorkflowRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        string? dir = string.IsNullOrEmpty(request.StorageDir) ? null : request.StorageDir;
        bool success = _runtime.ArchiveWorkflow(workflowKey, dir);
        return Task.FromResult(new ArchiveWorkflowResponse { Success = success });
    }

    public override Task<RetrieveColdWorkflowResponse> RetrieveColdWorkflow(
        RetrieveColdWorkflowRequest request, ServerCallContext context)
    {
        ulong workflowKey = ulong.Parse(request.RunId);
        string? dir = string.IsNullOrEmpty(request.StorageDir) ? null : request.StorageDir;
        int stepCount = _runtime.RetrieveWorkflow(workflowKey, out var status, dir);
        return Task.FromResult(new RetrieveColdWorkflowResponse
        {
            Status = (int)status,
            StepCount = stepCount >= 0 ? stepCount : 0,
        });
    }

    public override Task<ColdStorageCountResponse> ColdStorageCount(
        ColdStorageCountRequest request, ServerCallContext context)
    {
        string? dir = string.IsNullOrEmpty(request.StorageDir) ? null : request.StorageDir;
        int count = _runtime.ColdStorageCount(dir);
        return Task.FromResult(new ColdStorageCountResponse { Count = count });
    }

    // ─── Partition Management ───────────────────────────────────────────────

    public override Task<DescribePartitionResponse> DescribePartition(
        DescribePartitionRequest request, ServerCallContext context)
    {
        const int BUFFER_SIZE = 64;
        var buffer = new byte[BUFFER_SIZE];
        uint actualLen;
        unsafe
        {
            fixed (byte* ptr = buffer)
            {
                int result = NativeBridge.VelocityEngineDescribePartition(
                    ((WorkflowRuntime)_runtime).GetHandle(), request.PartitionId, ptr, BUFFER_SIZE, &actualLen);
                if (result != 0)
                {
                    return Task.FromResult(new DescribePartitionResponse());
                }

                int pos = 0;
                uint partitionId = BitConverter.ToUInt32(buffer, pos); pos += 4;
                ulong taskQueueHash = BitConverter.ToUInt64(buffer, pos); pos += 8;
                ulong pending = BitConverter.ToUInt64(buffer, pos); pos += 8;
                ulong workers = BitConverter.ToUInt64(buffer, pos); pos += 8;
                bool hasParent = buffer[pos++] != 0;
                uint parentId = BitConverter.ToUInt32(buffer, pos); pos += 4;
                double forwardRate = BitConverter.ToDouble(buffer, pos); pos += 8;

                return Task.FromResult(new DescribePartitionResponse
                {
                    PartitionId = partitionId,
                    TaskQueueHash = taskQueueHash,
                    PendingTasks = pending,
                    WorkerCount = workers,
                    HasParent = hasParent,
                    ParentPartitionId = parentId,
                    ForwardRate = forwardRate,
                });
            }
        }
    }


    // ─── Saga Orchestration ───────────────────────────────────────────────────

    public override Task<CreateSagaResponse> CreateSaga(CreateSagaRequest request, ServerCallContext context)
    {
        var sagaId = _runtime.CreateSaga(request.WorkflowKey, (uint)request.Steps.Count);
        _logger.LogInformation("CreateSaga: workflow={WorkflowKey} steps={StepCount} sagaId={SagaId}",
            request.WorkflowKey, request.Steps.Count, sagaId);
        return Task.FromResult(new CreateSagaResponse { SagaId = sagaId });
    }

    public override Task<CompensateSagaResponse> CompensateSaga(CompensateSagaRequest request, ServerCallContext context)
    {
        // Trigger compensation by failing the current step
        var info = _runtime.GetSagaInfo(request.SagaId);
        if (info == null)
            return Task.FromResult(new CompensateSagaResponse { Success = false });
        var compCount = _runtime.FailSagaStep(request.SagaId, info.CurrentStep);
        _logger.LogInformation("CompensateSaga: saga={SagaId} compensationSteps={Count}", request.SagaId, compCount);
        return Task.FromResult(new CompensateSagaResponse { Success = compCount > 0 });
    }

    public override Task<DescribeSagaResponse> DescribeSaga(DescribeSagaRequest request, ServerCallContext context)
    {
        var sagaId = request.SagaId;
        var workflowKey = _runtime.GetSagaWorkflowKey(sagaId);
        var status = _runtime.GetSagaOverallStatus(sagaId);
        var stepCount = _runtime.GetSagaStepCount(sagaId);
        var currentStep = _runtime.GetSagaCurrentStep(sagaId);

        return Task.FromResult(new DescribeSagaResponse
        {
            SagaId = sagaId,
            WorkflowKey = workflowKey,
            Status = status,
            CurrentStep = currentStep,
            StepCount = stepCount,
        });
    }

    // ─── Payload Codec ────────────────────────────────────────────────────────

    public override Task<EncodePayloadResponse> EncodePayload(EncodePayloadRequest request, ServerCallContext context)
    {
        var encoded = _runtime.CodecEncode(request.Payload.ToByteArray());
        var response = new EncodePayloadResponse();
        if (encoded != null)
            response.Encoded = Google.Protobuf.ByteString.CopyFrom(encoded);
        return Task.FromResult(response);
    }

    public override Task<DecodePayloadResponse> DecodePayload(DecodePayloadRequest request, ServerCallContext context)
    {
        var decoded = _runtime.CodecDecode(request.Encoded.ToByteArray());
        var response = new DecodePayloadResponse();
        if (decoded != null)
            response.Payload = Google.Protobuf.ByteString.CopyFrom(decoded);
        return Task.FromResult(response);
    }

    // ─── History Event Stream ─────────────────────────────────────────────────

    public override Task<GetHistoryEventsResponse> GetHistoryEvents(GetHistoryEventsRequest request, ServerCallContext context)
    {
        var events = _runtime.GetHistoryEvents(request.WorkflowKey, request.StartEventId, request.MaxCount);
        var response = new GetHistoryEventsResponse
        {
            TotalCount = (long)_runtime.EventCount(request.WorkflowKey),
        };

        foreach (var evt in events)
        {
            var detail = new Protos.HistoryEventDetail
            {
                EventId = evt.EventId,
                EventType = evt.EventType,
                TimestampMs = evt.TimestampMs,
            };
            if (evt.Payload != null)
                detail.Payload = Google.Protobuf.ByteString.CopyFrom(evt.Payload);
            response.Events.Add(detail);
        }

        return Task.FromResult(response);
    }

    // ─── Worker Management ────────────────────────────────────────────────────

    public override Task<RegisterWorkerResponse> RegisterWorker(RegisterWorkerRequest request, ServerCallContext context)
    {
        var tqHashes = request.TaskQueueHashes.ToArray();
        var workerId = _runtime.RegisterWorker(request.Address, tqHashes, request.Version);
        _logger.LogInformation("Registered worker {WorkerId} at {Address}", workerId, request.Address);
        return Task.FromResult(new RegisterWorkerResponse { WorkerId = workerId });
    }

    public override Task<UnregisterWorkerResponse> UnregisterWorker(UnregisterWorkerRequest request, ServerCallContext context)
    {
        var success = _runtime.UnregisterWorker(request.WorkerId);
        _logger.LogInformation("Unregistered worker {WorkerId}: {Success}", request.WorkerId, success);
        return Task.FromResult(new UnregisterWorkerResponse { Success = success });
    }

    public override Task<WorkerHeartbeatResponse> WorkerHeartbeat(WorkerHeartbeatRequest request, ServerCallContext context)
    {
        var ack = _runtime.WorkerHeartbeat(request.WorkerId);
        return Task.FromResult(new WorkerHeartbeatResponse { Acknowledged = ack });
    }

    public override Task<DescribeWorkersResponse> DescribeWorkers(DescribeWorkersRequest request, ServerCallContext context)
    {
        return Task.FromResult(new DescribeWorkersResponse
        {
            TotalWorkers = _runtime.GetWorkerCount(),
            ActiveWorkers = _runtime.GetActiveWorkerCount(),
            TotalTasksCompleted = _runtime.GetTotalTasksCompleted(),
            TotalTasksFailed = _runtime.GetTotalTasksFailed(),
        });
    }

    // ─── Batch Operations ──────────────────────────────────────────────────

    public override Task<BatchTerminateResponse> BatchTerminateWorkflows(BatchTerminateRequest request, ServerCallContext context)
    {
        var keys = request.WorkflowKeys.ToArray();
        var batchId = _runtime.BatchTerminate(keys);
        _logger.LogInformation("Batch terminate: {Count} workflows, batch {BatchId}", keys.Length, batchId);
        return Task.FromResult(new BatchTerminateResponse { BatchId = batchId, Submitted = (uint)keys.Length });
    }

    public override Task<BatchCancelResponse> BatchCancelWorkflows(BatchCancelRequest request, ServerCallContext context)
    {
        var keys = request.WorkflowKeys.ToArray();
        var batchId = _runtime.BatchCancel(keys);
        _logger.LogInformation("Batch cancel: {Count} workflows, batch {BatchId}", keys.Length, batchId);
        return Task.FromResult(new BatchCancelResponse { BatchId = batchId, Submitted = (uint)keys.Length });
    }

    public override Task<BatchSignalResponse> BatchSignalWorkflows(BatchSignalRequest request, ServerCallContext context)
    {
        var keys = request.WorkflowKeys.ToArray();
        var payload = request.Payload.ToByteArray();
        var batchId = _runtime.BatchSignal(keys, request.SignalNameId, payload.Length > 0 ? payload : null);
        _logger.LogInformation("Batch signal: {Count} workflows, signal {SignalId}, batch {BatchId}", keys.Length, request.SignalNameId, batchId);
        return Task.FromResult(new BatchSignalResponse { BatchId = batchId, Submitted = (uint)keys.Length });
    }

    // ─── Schedule Introspection ──────────────────────────────────────────

    public override Task<ListSchedulesResponse> ListSchedules(ListSchedulesRequest request, ServerCallContext context)
    {
        var ids = _runtime.ListSchedules();
        var response = new ListSchedulesResponse { TotalCount = (uint)ids.Length };
        response.ScheduleIds.AddRange(ids);
        return Task.FromResult(response);
    }

    public override Task<DescribeScheduleResponse> DescribeSchedule(DescribeScheduleRequest request, ServerCallContext context)
    {
        var desc = _runtime.DescribeSchedule(request.ScheduleId);
        if (desc == null)
            return Task.FromResult(new DescribeScheduleResponse { Found = false });
        return Task.FromResult(new DescribeScheduleResponse
        {
            Found = true,
            ScheduleId = desc.ScheduleId,
            WorkflowTypeId = desc.WorkflowTypeId,
            NamespaceId = desc.NamespaceId,
            TaskQueueHash = desc.TaskQueueHash,
            OverlapPolicy = desc.OverlapPolicy,
            ActionCount = desc.ActionCount,
            IsPaused = desc.IsPaused,
        });
    }

    // ─── Dynamic Config ──────────────────────────────────────────────────

    public override Task<ListConfigKeysResponse> ListConfigKeys(ListConfigKeysRequest request, ServerCallContext context)
    {
        var keys = _runtime.ListConfigKeys();
        var response = new ListConfigKeysResponse { TotalCount = (uint)keys.Length };
        response.Keys.AddRange(keys);
        return Task.FromResult(response);
    }

    // ─── Workflow Count Aggregation ──────────────────────────────────────

    public override Task<CountByStatusResponse> CountByStatus(CountByStatusRequest request, ServerCallContext context)
    {
        var count = _runtime.CountByStatus((Core.WorkflowExecutionStatus)request.Status);
        return Task.FromResult(new CountByStatusResponse { Count = count });
    }

    public override Task<CountByNamespaceResponse> CountByNamespace(CountByNamespaceRequest request, ServerCallContext context)
    {
        var count = _runtime.CountByNamespace(request.NamespaceId);
        return Task.FromResult(new CountByNamespaceResponse { Count = count });
    }

    public override Task<CountByTypeResponse> CountByType(CountByTypeRequest request, ServerCallContext context)
    {
        var count = _runtime.CountByType(request.WorkflowTypeId);
        return Task.FromResult(new CountByTypeResponse { Count = count });
    }

    // ─── Namespace Retention ─────────────────────────────────────────────

    public override Task<GetNamespaceRetentionResponse> GetNamespaceRetention(GetNamespaceRetentionRequest request, ServerCallContext context)
    {
        var retentionMs = _runtime.GetNamespaceRetentionMs(request.NamespaceId);
        return Task.FromResult(new GetNamespaceRetentionResponse { RetentionMs = retentionMs });
    }

    public override Task<CleanupExpiredWorkflowsResponse> CleanupExpiredWorkflows(CleanupExpiredWorkflowsRequest request, ServerCallContext context)
    {
        var removed = _runtime.CleanupExpiredWorkflows();
        _logger.LogInformation("Cleanup expired workflows: {Count} removed", removed);
        return Task.FromResult(new CleanupExpiredWorkflowsResponse { RemovedCount = removed });
    }

    // ─── Cluster Replication (Batch 21) ──────────────────────────────────

    public override Task<EnqueueReplicationResponse> EnqueueReplication(EnqueueReplicationRequest request, ServerCallContext context)
    {
        var taskId = _runtime.EnqueueReplication(request.SourceClusterId, request.TargetClusterId, request.WorkflowKey, request.EventType, request.Payload.ToByteArray());
        return Task.FromResult(new EnqueueReplicationResponse { TaskId = taskId });
    }

    public override Task<DrainReplicationTasksResponse> DrainReplicationTasks(DrainReplicationTasksRequest request, ServerCallContext context)
    {
        var count = _runtime.DrainReplicationTasks();
        return Task.FromResult(new DrainReplicationTasksResponse { DrainedCount = count });
    }

    public override Task<GetClusterInfoResponse> GetClusterInfo(GetClusterInfoRequest request, ServerCallContext context)
    {
        var info = _runtime.GetClusterInfo(request.ClusterId);
        if (info == null)
            return Task.FromResult(new GetClusterInfoResponse());
        return Task.FromResult(new GetClusterInfoResponse
        {
            ClusterId = info.ClusterId,
            IsActive = info.IsActive,
            FailoverVersion = info.FailoverVersion,
            ReplicationEnabled = info.ReplicationEnabled,
        });
    }

    // ─── Sharding Enhanced (Batch 21) ────────────────────────────────────

    public override Task<GetShardOwnerResponse> GetShardOwner(GetShardOwnerRequest request, ServerCallContext context)
    {
        var owner = _runtime.GetShardOwner(request.ShardId) ?? "";
        return Task.FromResult(new GetShardOwnerResponse { Owner = owner });
    }

    public override Task<GetShardsForHostResponse> GetShardsForHost(GetShardsForHostRequest request, ServerCallContext context)
    {
        var shards = _runtime.GetShardsForHost(request.Host);
        var resp = new GetShardsForHostResponse();
        resp.ShardIds.AddRange(shards);
        return Task.FromResult(resp);
    }

    // ─── Nexus Operations (Batch 21) ─────────────────────────────────────

    public override Task<NexusStartOperationResponse> NexusStartOperation(NexusStartOperationRequest request, ServerCallContext context)
    {
        var opId = _runtime.NexusStartOperation(request.Service, request.Operation, request.WorkflowKey,
            request.Input.IsEmpty ? null : request.Input.ToByteArray(),
            string.IsNullOrEmpty(request.CallbackUrl) ? null : request.CallbackUrl);
        return Task.FromResult(new NexusStartOperationResponse { OperationId = opId });
    }

    public override Task<NexusCompleteOperationResponse> NexusCompleteOperation(NexusCompleteOperationRequest request, ServerCallContext context)
    {
        var success = _runtime.NexusCompleteOperation(request.OperationId, request.Result.IsEmpty ? null : request.Result.ToByteArray());
        return Task.FromResult(new NexusCompleteOperationResponse { Success = success });
    }

    public override Task<NexusFailOperationResponse> NexusFailOperation(NexusFailOperationRequest request, ServerCallContext context)
    {
        var success = _runtime.NexusFailOperation(request.OperationId);
        return Task.FromResult(new NexusFailOperationResponse { Success = success });
    }

    public override Task<NexusGetOperationResponse> NexusGetOperation(NexusGetOperationRequest request, ServerCallContext context)
    {
        var op = _runtime.NexusGetOperation(request.OperationId);
        if (op == null)
            return Task.FromResult(new NexusGetOperationResponse());
        return Task.FromResult(new NexusGetOperationResponse
        {
            OperationId = op.OperationId,
            WorkflowKey = op.WorkflowKey,
            State = op.State,
            HasResult = op.HasResult,
        });
    }

    // ─── Rate Limiter Enhanced (Batch 22) ────────────────────────────────

    public override Task<SetNamespaceRateLimitResponse> SetNamespaceRateLimit(SetNamespaceRateLimitRequest request, ServerCallContext context)
    {
        _runtime.RateSetNamespaceLimit(request.NamespaceId, request.Rate, request.Capacity);
        return Task.FromResult(new SetNamespaceRateLimitResponse());
    }

    public override Task<GetRateLimitInfoResponse> GetRateLimitInfo(GetRateLimitInfoRequest request, ServerCallContext context)
    {
        var count = _runtime.RateNamespaceCount();
        return Task.FromResult(new GetRateLimitInfoResponse { NamespaceCount = count });
    }

    // ─── Memo Enhanced (Batch 22) ────────────────────────────────────────

    public override Task<RemoveMemoResponse> RemoveMemo(RemoveMemoRequest request, ServerCallContext context)
    {
        var success = _runtime.RemoveMemo(request.WorkflowKey, request.Key);
        return Task.FromResult(new RemoveMemoResponse { Success = success });
    }

    public override Task<GetMemoWorkflowCountResponse> GetMemoWorkflowCount(GetMemoWorkflowCountRequest request, ServerCallContext context)
    {
        var count = _runtime.MemoWorkflowCount();
        return Task.FromResult(new GetMemoWorkflowCountResponse { Count = count });
    }

    // ─── Worker Versioning Enhanced (Batch 22) ───────────────────────────

    public override Task<SetCurrentBuildIdResponse> SetCurrentBuildId(SetCurrentBuildIdRequest request, ServerCallContext context)
    {
        var success = _runtime.VersioningSetCurrent(request.SetId, request.BuildId);
        return Task.FromResult(new SetCurrentBuildIdResponse { Success = success });
    }

    public override Task<GetCurrentBuildIdResponse> GetCurrentBuildId(GetCurrentBuildIdRequest request, ServerCallContext context)
    {
        var buildId = _runtime.VersioningGetCurrent(request.SetId) ?? "";
        return Task.FromResult(new GetCurrentBuildIdResponse { BuildId = buildId });
    }

    public override Task<AddRoutingRuleResponse> AddRoutingRule(AddRoutingRuleRequest request, ServerCallContext context)
    {
        _runtime.VersioningAddRoutingRule(request.TaskQueue, request.BuildId, request.Percentage);
        return Task.FromResult(new AddRoutingRuleResponse());
    }

    public override Task<ResolveBuildIdResponse> ResolveBuildId(ResolveBuildIdRequest request, ServerCallContext context)
    {
        var buildId = _runtime.VersioningResolveBuildId(request.TaskQueue) ?? "";
        return Task.FromResult(new ResolveBuildIdResponse { BuildId = buildId });
    }

    // ─── Auth Enhanced (Batch 22) ────────────────────────────────────────

    public override Task<DenySubjectResponse> DenySubject(DenySubjectRequest request, ServerCallContext context)
    {
        _runtime.AuthDenySubject(request.Subject);
        return Task.FromResult(new DenySubjectResponse());
    }

    public override Task<GetAuthInfoResponse> GetAuthInfo(GetAuthInfoRequest request, ServerCallContext context)
    {
        var count = _runtime.AuthRoleCount();
        return Task.FromResult(new GetAuthInfoResponse { RoleCount = count });
    }

    // ─── Metrics Enhanced (Batch 23) ─────────────────────────────────────

    public override Task<MetricsIncCounterResponse> MetricsIncCounter(MetricsIncCounterRequest request, ServerCallContext context)
    {
        _runtime.MetricsIncCounter(request.Name);
        return Task.FromResult(new MetricsIncCounterResponse());
    }

    public override Task<MetricsGetCounterResponse> MetricsGetCounter(MetricsGetCounterRequest request, ServerCallContext context)
    {
        var value = _runtime.MetricsGetCounter(request.Name);
        return Task.FromResult(new MetricsGetCounterResponse { Value = value });
    }

    public override Task<MetricsSetGaugeResponse> MetricsSetGauge(MetricsSetGaugeRequest request, ServerCallContext context)
    {
        _runtime.MetricsSetGauge(request.Name, request.Value);
        return Task.FromResult(new MetricsSetGaugeResponse());
    }

    public override Task<MetricsGetGaugeResponse> MetricsGetGauge(MetricsGetGaugeRequest request, ServerCallContext context)
    {
        var value = _runtime.MetricsGetGauge(request.Name);
        return Task.FromResult(new MetricsGetGaugeResponse { Value = value });
    }

    // ─── History/Archive Enhanced (Batch 23) ────────────────────────────

    public override Task<HistoryEventCountResponse> HistoryEventCount(HistoryEventCountRequest request, ServerCallContext context)
    {
        var count = _runtime.HistoryEventCount(request.WorkflowKey);
        return Task.FromResult(new HistoryEventCountResponse { Count = count });
    }

    public override Task<HistoryRemoveResponse> HistoryRemove(HistoryRemoveRequest request, ServerCallContext context)
    {
        var success = _runtime.HistoryRemove(request.WorkflowKey);
        return Task.FromResult(new HistoryRemoveResponse { Success = success });
    }

    public override Task<ArchiveRetrieveResponse> ArchiveRetrieve(ArchiveRetrieveRequest request, ServerCallContext context)
    {
        var rec = _runtime.ArchiveRetrieve(request.WorkflowKey);
        if (rec == null)
            return Task.FromResult(new ArchiveRetrieveResponse());
        return Task.FromResult(new ArchiveRetrieveResponse
        {
            WorkflowKey = rec.WorkflowKey,
            NamespaceId = rec.NamespaceId,
            WorkflowTypeId = rec.WorkflowTypeId,
            Status = rec.Status,
            EventCount = rec.EventCount,
        });
    }

    public override Task<ArchiveDeleteResponse> ArchiveDelete(ArchiveDeleteRequest request, ServerCallContext context)
    {
        var success = _runtime.ArchiveDelete(request.WorkflowKey);
        return Task.FromResult(new ArchiveDeleteResponse { Success = success });
    }

    public override Task<ArchiveCountByStatusResponse> ArchiveCountByStatus(ArchiveCountByStatusRequest request, ServerCallContext context)
    {
        var count = _runtime.ArchiveCountByStatus(request.Status);
        return Task.FromResult(new ArchiveCountByStatusResponse { Count = count });
    }

    // ─── Namespace Enhanced (Batch 24) ───────────────────────────────────

    public override Task<DeactivateNamespaceResponse> DeactivateNamespace(DeactivateNamespaceRequest request, ServerCallContext context)
    {
        var success = _runtime.DeactivateNamespace(request.NamespaceId);
        return Task.FromResult(new DeactivateNamespaceResponse { Success = success });
    }

    public override Task<ActivateNamespaceResponse> ActivateNamespace(ActivateNamespaceRequest request, ServerCallContext context)
    {
        var success = _runtime.ActivateNamespace(request.NamespaceId);
        return Task.FromResult(new ActivateNamespaceResponse { Success = success });
    }

    // ─── Cron Enhanced (Batch 24) ────────────────────────────────────────

    public override Task<CronNextFireTimeResponse> CronNextFireTime(CronNextFireTimeRequest request, ServerCallContext context)
    {
        var time = _runtime.CronNextFireTime(request.ScheduleId);
        return Task.FromResult(new CronNextFireTimeResponse { NextFireTime = time });
    }

    public override Task<CronSetPausedResponse> CronSetPaused(CronSetPausedRequest request, ServerCallContext context)
    {
        var success = _runtime.CronSetPaused(request.ScheduleId, request.Paused);
        return Task.FromResult(new CronSetPausedResponse { Success = success });
    }

    public override Task<CronUnregisterResponse> CronUnregister(CronUnregisterRequest request, ServerCallContext context)
    {
        var success = _runtime.CronUnregister(request.ScheduleId);
        return Task.FromResult(new CronUnregisterResponse { Success = success });
    }

    // ─── Codec/Search Enhanced (Batch 24) ────────────────────────────────

    public override Task<GetCodecCountResponse> GetCodecCount(GetCodecCountRequest request, ServerCallContext context)
    {
        var count = _runtime.CodecCount();
        return Task.FromResult(new GetCodecCountResponse { Count = count });
    }

    public override Task<GetSearchAttrCountResponse> GetSearchAttrCount(GetSearchAttrCountRequest request, ServerCallContext context)
    {
        var count = _runtime.SearchAttrCount(request.WorkflowKey);
        return Task.FromResult(new GetSearchAttrCountResponse { Count = count });
    }

    // ─── Patch Management (Batch 26) ─────────────────────────────────────

    public override Task<RegisterPatchResponse> RegisterPatch(RegisterPatchRequest request, ServerCallContext context)
    {
        var patchId = _runtime.RegisterPatch(request.WorkflowTypeId, request.VersionMarker, request.MinVersion, request.MaxVersion, request.Description);
        return Task.FromResult(new RegisterPatchResponse { PatchId = patchId });
    }

    public override Task<DeactivatePatchResponse> DeactivatePatch(DeactivatePatchRequest request, ServerCallContext context)
    {
        var success = _runtime.DeactivatePatch(request.PatchId);
        return Task.FromResult(new DeactivatePatchResponse { Success = success });
    }

    public override Task<FindPatchResponse> FindPatch(FindPatchRequest request, ServerCallContext context)
    {
        var patchId = _runtime.FindPatch(request.WorkflowTypeId, request.Version);
        return Task.FromResult(new FindPatchResponse { PatchId = patchId });
    }

    public override Task<GetPatchResponse> GetPatch(GetPatchRequest request, ServerCallContext context)
    {
        var info = _runtime.GetPatch(request.PatchId);
        if (info == null)
            return Task.FromResult(new GetPatchResponse());
        return Task.FromResult(new GetPatchResponse
        {
            PatchId = info.PatchId,
            WorkflowTypeId = info.WorkflowTypeId,
            MinVersion = info.MinVersion,
            MaxVersion = info.MaxVersion,
            IsActive = info.IsActive,
        });
    }

    public override Task<ListActivePatchesResponse> ListActivePatches(ListActivePatchesRequest request, ServerCallContext context)
    {
        var ids = _runtime.ActivePatchesForType(request.WorkflowTypeId);
        var resp = new ListActivePatchesResponse();
        resp.PatchIds.AddRange(ids);
        return Task.FromResult(resp);
    }

    // ─── Parent Close Policy (Batch 26) ──────────────────────────────────

    public override Task<ApplyParentClosePolicyResponse> ApplyParentClosePolicy(ApplyParentClosePolicyRequest request, ServerCallContext context)
    {
        var success = _runtime.ApplyParentClosePolicy(request.ParentKey, (ParentClosePolicy)request.Policy);
        return Task.FromResult(new ApplyParentClosePolicyResponse { Success = success });
    }

    // ─── Activity Retry (Batch 26) ───────────────────────────────────────

    public override Task<FailActivityWithRetryResponse> FailActivityWithRetry(FailActivityWithRetryRequest request, ServerCallContext context)
    {
        var wasRetried = _runtime.FailActivityWithRetry(request.WorkflowKey, request.Step);
        return Task.FromResult(new FailActivityWithRetryResponse { WasRetried = wasRetried });
    }

    // ─── Timeout Enforcement (Batch 26) ──────────────────────────────────

    public override Task<ScheduleActivityWithTimeoutsResponse> ScheduleActivityWithTimeouts(ScheduleActivityWithTimeoutsRequest request, ServerCallContext context)
    {
        var options = new ActivityOptions
        {
            ScheduleToStart = TimeSpan.FromMilliseconds(request.ScheduleToStartMs),
            StartToClose = TimeSpan.FromMilliseconds(request.StartToCloseMs),
            ScheduleToClose = TimeSpan.FromMilliseconds(request.ScheduleToCloseMs),
            HeartbeatTimeout = TimeSpan.FromMilliseconds(request.HeartbeatMs),
        };
        _runtime.ScheduleActivityWithTimeouts(request.WorkflowKey, request.Step, request.ActivityNameId,
            request.Args.IsEmpty ? null : request.Args.ToByteArray(), options);
        return Task.FromResult(new ScheduleActivityWithTimeoutsResponse { Success = true });
    }

    public override Task<CheckActivityTimeoutsResponse> CheckActivityTimeouts(CheckActivityTimeoutsRequest request, ServerCallContext context)
    {
        var count = _runtime.CheckActivityTimeouts();
        return Task.FromResult(new CheckActivityTimeoutsResponse { TimedOutCount = count });
    }

    public override Task<CheckWorkflowTimeoutsResponse> CheckWorkflowTimeouts(CheckWorkflowTimeoutsRequest request, ServerCallContext context)
    {
        var count = _runtime.CheckWorkflowTimeouts();
        return Task.FromResult(new CheckWorkflowTimeoutsResponse { TimedOutCount = count });
    }

    public override Task<SetWorkflowExecutionTimeoutResponse> SetWorkflowExecutionTimeout(SetWorkflowExecutionTimeoutRequest request, ServerCallContext context)
    {
        var success = _runtime.SetWorkflowTimeout(request.WorkflowKey, TimeSpan.FromMilliseconds(request.TimeoutMs));
        return Task.FromResult(new SetWorkflowExecutionTimeoutResponse { Success = success });
    }

    // ─── Config Enhanced (Batch 26) ──────────────────────────────────────

    public override Task<SetConfigBoolResponse> SetConfigBool(SetConfigBoolRequest request, ServerCallContext context)
    {
        _runtime.ConfigSetBool(request.Key, request.Value);
        return Task.FromResult(new SetConfigBoolResponse());
    }

    public override Task<SetConfigFloatResponse> SetConfigFloat(SetConfigFloatRequest request, ServerCallContext context)
    {
        _runtime.ConfigSetFloat(request.Key, request.Value);
        return Task.FromResult(new SetConfigFloatResponse());
    }

    public override Task<SetConfigStringResponse> SetConfigString(SetConfigStringRequest request, ServerCallContext context)
    {
        _runtime.ConfigSetString(request.Key, request.Value);
        return Task.FromResult(new SetConfigStringResponse());
    }

    public override Task<GetConfigBoolResponse> GetConfigBool(GetConfigBoolRequest request, ServerCallContext context)
    {
        var value = _runtime.ConfigGetBool(request.Key);
        return Task.FromResult(new GetConfigBoolResponse { Value = value });
    }

    public override Task<GetConfigFloatResponse> GetConfigFloat(GetConfigFloatRequest request, ServerCallContext context)
    {
        var value = _runtime.ConfigGetFloat(request.Key);
        return Task.FromResult(new GetConfigFloatResponse { Value = value });
    }

    public override Task<GetConfigKeyCountResponse> GetConfigKeyCount(GetConfigKeyCountRequest request, ServerCallContext context)
    {
        var count = _runtime.ConfigKeyCount;
        return Task.FromResult(new GetConfigKeyCountResponse { Count = count });
    }

    // ─── Heartbeat Enhanced (Batch 26) ───────────────────────────────────

    public override Task<UnregisterHeartbeatResponse> UnregisterHeartbeat(UnregisterHeartbeatRequest request, ServerCallContext context)
    {
        _runtime.UnregisterHeartbeat(request.WorkflowKey, request.ActivityId);
        return Task.FromResult(new UnregisterHeartbeatResponse());
    }

    // ─── Cloud Storage Adapter (Batch 28) ────────────────────────────────

    public override Task<CloudSetBackendResponse> CloudSetBackend(CloudSetBackendRequest request, ServerCallContext context)
    {
        var success = _runtime.CloudSetBackend(request.Backend);
        return Task.FromResult(new CloudSetBackendResponse { Success = success });
    }

    public override Task<CloudArchiveResponse> CloudArchive(CloudArchiveRequest request, ServerCallContext context)
    {
        var success = _runtime.CloudArchive(request.WorkflowKey, request.NamespaceId, request.Status);
        return Task.FromResult(new CloudArchiveResponse { Success = success });
    }

    public override Task<CloudContainsResponse> CloudContains(CloudContainsRequest request, ServerCallContext context)
    {
        var contains = _runtime.CloudContains(request.WorkflowKey);
        return Task.FromResult(new CloudContainsResponse { Contains = contains });
    }

    public override Task<CloudDeleteResponse> CloudDelete(CloudDeleteRequest request, ServerCallContext context)
    {
        var success = _runtime.CloudDelete(request.WorkflowKey);
        return Task.FromResult(new CloudDeleteResponse { Success = success });
    }

    public override Task<CloudCountResponse> CloudCount(CloudCountRequest request, ServerCallContext context)
    {
        var count = _runtime.CloudCount();
        return Task.FromResult(new CloudCountResponse { Count = count });
    }

    public override Task<CloudListByNamespaceResponse> CloudListByNamespace(CloudListByNamespaceRequest request, ServerCallContext context)
    {
        var keys = _runtime.CloudListByNamespace(request.NamespaceId);
        var resp = new CloudListByNamespaceResponse();
        resp.WorkflowKeys.AddRange(keys);
        return Task.FromResult(resp);
    }

    public override Task<CloudGcResponse> CloudGc(CloudGcRequest request, ServerCallContext context)
    {
        var deleted = _runtime.CloudGc(request.RetentionMs);
        return Task.FromResult(new CloudGcResponse { DeletedCount = deleted });
    }

    public override Task<CloudBackendNameResponse> CloudBackendName(CloudBackendNameRequest request, ServerCallContext context)
    {
        var name = _runtime.CloudBackendName();
        return Task.FromResult(new CloudBackendNameResponse { Name = name });
    }

    // ─── Query/Reset Enhanced (Batch 28) ─────────────────────────────────

    public override Task<UnregisterQueryHandlerResponse> UnregisterQueryHandler(UnregisterQueryHandlerRequest request, ServerCallContext context)
    {
        _runtime.UnregisterQueryHandler(request.WorkflowKey);
        return Task.FromResult(new UnregisterQueryHandlerResponse());
    }

    public override Task<GetResetPointsResponse> GetResetPoints(GetResetPointsRequest request, ServerCallContext context)
    {
        var points = _runtime.GetResetPoints(request.WorkflowKey);
        var resp = new GetResetPointsResponse();
        resp.EventIds.AddRange(points);
        return Task.FromResult(resp);
    }

    // ─── Replay Recovery (Batch 28) ──────────────────────────────────────

    public override Task<ReplayAndRestoreResponse> ReplayAndRestore(ReplayAndRestoreRequest request, ServerCallContext context)
    {
        var status = _runtime.ReplayAndRestore(request.WorkflowKey);
        return Task.FromResult(new ReplayAndRestoreResponse
        {
            Success = status >= 0,
            ReconstructedStatus = status,
        });
    }

    public override Task<RecoverFromWalResponse> RecoverFromWal(RecoverFromWalRequest request, ServerCallContext context)
    {
        var records = _runtime.RecoverFromWal();
        _logger.LogInformation("RecoverFromWal: recordsReplayed={Count}", records);
        return Task.FromResult(new RecoverFromWalResponse { RecordsReplayed = records });
    }

    // ─── Visibility Listing Enhanced (Batch 29) ───────────────────────────

    public override Task<ListBySearchAttributeResponse> ListBySearchAttribute(ListBySearchAttributeRequest request, ServerCallContext context)
    {
        var keys = _runtime.ListBySearchAttribute(request.AttributeKey, request.AttributeValue);
        var resp = new ListBySearchAttributeResponse();
        resp.WorkflowKeys.AddRange(keys);
        return Task.FromResult(resp);
    }

    public override Task<ListByTimeRangeResponse> ListByTimeRange(ListByTimeRangeRequest request, ServerCallContext context)
    {
        var keys = _runtime.ListByTimeRange(request.StartTimeMs, request.EndTimeMs);
        var resp = new ListByTimeRangeResponse();
        resp.WorkflowKeys.AddRange(keys);
        return Task.FromResult(resp);
    }

    // ─── Replay Cache Management (Batch 29) ───────────────────────────────

    public override Task<ReplayInvalidateResponse> ReplayInvalidate(ReplayInvalidateRequest request, ServerCallContext context)
    {
        _runtime.ReplayInvalidate(request.WorkflowKey);
        return Task.FromResult(new ReplayInvalidateResponse());
    }

    public override Task<ReplayClearCacheResponse> ReplayClearCache(ReplayClearCacheRequest request, ServerCallContext context)
    {
        _runtime.ReplayClearCache();
        return Task.FromResult(new ReplayClearCacheResponse());
    }

    public override Task<ReplayCacheSizeResponse> ReplayCacheSize(ReplayCacheSizeRequest request, ServerCallContext context)
    {
        var size = _runtime.ReplayCacheSize();
        return Task.FromResult(new ReplayCacheSizeResponse { CacheSize = size });
    }

    // ─── Schedule Management Enhanced (Batch 29) ──────────────────────────

    public override Task<ScheduleSetOverlapPolicyResponse> ScheduleSetOverlapPolicy(ScheduleSetOverlapPolicyRequest request, ServerCallContext context)
    {
        var ok = _runtime.ScheduleSetOverlapPolicy(request.ScheduleId, request.Policy);
        return Task.FromResult(new ScheduleSetOverlapPolicyResponse { Success = ok });
    }

    public override Task<ScheduleSetRemainingActionsResponse> ScheduleSetRemainingActions(ScheduleSetRemainingActionsRequest request, ServerCallContext context)
    {
        var ok = _runtime.ScheduleSetRemainingActions(request.ScheduleId, request.Remaining);
        return Task.FromResult(new ScheduleSetRemainingActionsResponse { Success = ok });
    }

    // ─── Event History Enhanced (Batch 29) ────────────────────────────────

    public override Task<HistoryWorkflowCountResponse> HistoryWorkflowCount(HistoryWorkflowCountRequest request, ServerCallContext context)
    {
        var count = _runtime.HistoryWorkflowCount();
        return Task.FromResult(new HistoryWorkflowCountResponse { Count = count });
    }

    // ─── Partition Worker Management (Batch 29) ───────────────────────────

    public override Task<PartitionTotalPendingResponse> PartitionTotalPending(PartitionTotalPendingRequest request, ServerCallContext context)
    {
        var pending = _runtime.PartitionTotalPending(request.TaskQueueHash);
        return Task.FromResult(new PartitionTotalPendingResponse { PendingCount = pending });
    }

    // ─── Nexus Enhanced (Batch 29) ────────────────────────────────────────

    public override Task<NexusRegisterServiceResponse> NexusRegisterService(NexusRegisterServiceRequest request, ServerCallContext context)
    {
        var ok = _runtime.NexusRegisterService(request.ServiceName, request.Endpoint);
        _logger.LogInformation("NexusRegisterService: service={Service}, ok={Ok}", request.ServiceName, ok);
        return Task.FromResult(new NexusRegisterServiceResponse { Success = ok });
    }

    // ─── Real Cloud Storage SDK (Batch 29) ────────────────────────────────

    public override Task<CloudSetS3Response> CloudSetS3(CloudSetS3Request request, ServerCallContext context)
    {
        var ok = _runtime.CloudSetS3(request.Bucket, request.Region, request.AccessKey, request.SecretKey);
        _logger.LogInformation("CloudSetS3: bucket={Bucket}, region={Region}, ok={Ok}", request.Bucket, request.Region, ok);
        return Task.FromResult(new CloudSetS3Response { Success = ok });
    }

    public override Task<CloudSetGcsResponse> CloudSetGcs(CloudSetGcsRequest request, ServerCallContext context)
    {
        var ok = _runtime.CloudSetGcs(request.Bucket, request.OauthToken);
        _logger.LogInformation("CloudSetGcs: bucket={Bucket}, ok={Ok}", request.Bucket, ok);
        return Task.FromResult(new CloudSetGcsResponse { Success = ok });
    }

    // ─── Search Attributes + Replication (Batch 30) ─────────────────────────

    public override Task<StartWorkflowWithSearchAttrsResponse> StartWorkflowWithSearchAttrs(StartWorkflowWithSearchAttrsRequest request, ServerCallContext context)
    {
        var attrs = new Dictionary<string, string>();
        foreach (var kv in request.SearchAttributes) { attrs[kv.Key] = kv.Value; }
        var key = _runtime.StartWorkflowWithAttributes(
            request.WorkflowId, request.WorkflowTypeId, request.NamespaceId,
            request.TaskQueueHash, request.TotalSteps,
            request.Input.IsEmpty ? null : request.Input.ToByteArray(), attrs);
        return Task.FromResult(new StartWorkflowWithSearchAttrsResponse { WorkflowKey = key });
    }

    public override Task<ApplyReplicationTaskResponse> ApplyReplicationTask(ApplyReplicationTaskRequest request, ServerCallContext context)
    {
        var ok = _runtime.ApplyReplicationTask(
            request.SourceClusterId, request.TargetClusterId, request.WorkflowKey,
            request.EventType,
            request.Payload.IsEmpty ? null : request.Payload.ToByteArray(),
            request.FailoverVersion);
        return Task.FromResult(new ApplyReplicationTaskResponse { Success = ok });
    }

    public override Task<ProcessFiredTimerResponse> ProcessFiredTimer(ProcessFiredTimerRequest request, ServerCallContext context)
    {
        _runtime.ProcessFiredTimer(request.WorkflowKey);
        return Task.FromResult(new ProcessFiredTimerResponse());
    }

    public override Task<ReplicationStatusResponse> ReplicationStatus(ReplicationStatusRequest request, ServerCallContext context)
    {
        var (pending, clusters, active) = _runtime.ReplicationStatus();
        return Task.FromResult(new ReplicationStatusResponse {
            PendingTasks = pending, ClusterCount = clusters, ActiveClusters = active
        });
    }

    public override Task<SetClusterActiveResponse> SetClusterActive(SetClusterActiveRequest request, ServerCallContext context)
    {
        var ok = _runtime.SetClusterActive(request.ClusterId, request.Active);
        return Task.FromResult(new SetClusterActiveResponse { Success = ok });
    }

    public override Task<SetFailoverVersionResponse> SetFailoverVersion(SetFailoverVersionRequest request, ServerCallContext context)
    {
        var ok = _runtime.SetFailoverVersion(request.ClusterId, request.Version);
        return Task.FromResult(new SetFailoverVersionResponse { Success = ok });
    }

    // ─── Nexus Lifecycle (Batch 32) ────────────────────────────────────────

    public override Task<NexusMarkStartedResponse> NexusMarkStarted(NexusMarkStartedRequest request, ServerCallContext context)
        => Task.FromResult(new NexusMarkStartedResponse { Success = _runtime.NexusMarkStarted(request.OpId) });

    public override Task<NexusCancelResponse> NexusCancel(NexusCancelRequest request, ServerCallContext context)
        => Task.FromResult(new NexusCancelResponse { Success = _runtime.NexusCancel(request.OpId) });

    public override Task<NexusTimeoutOpResponse> NexusTimeoutOp(NexusTimeoutOpRequest request, ServerCallContext context)
        => Task.FromResult(new NexusTimeoutOpResponse { Success = _runtime.NexusTimeout(request.OpId) });

    public override Task<NexusRetryResponse> NexusRetry(NexusRetryRequest request, ServerCallContext context)
        => Task.FromResult(new NexusRetryResponse { Success = _runtime.NexusRetry(request.OpId) });

    public override Task<NexusCountByStateResponse> NexusCountByState(NexusCountByStateRequest request, ServerCallContext context)
        => Task.FromResult(new NexusCountByStateResponse { Count = _runtime.NexusCountByState(request.State) });

    // ─── Worker Dispatch (Batch 32) ────────────────────────────────────────

    public override Task<SelectWorkerResponse> SelectWorker(SelectWorkerRequest request, ServerCallContext context)
        => Task.FromResult(new SelectWorkerResponse { WorkerId = _runtime.SelectWorker(request.TqHash) });

    public override Task<WorkerHasCapacityResponse> WorkerHasCapacity(WorkerHasCapacityRequest request, ServerCallContext context)
        => Task.FromResult(new WorkerHasCapacityResponse { HasCapacity = _runtime.WorkerHasCapacity(request.WorkerId) });

    public override Task<DrainWorkerResponse> DrainWorker(DrainWorkerRequest request, ServerCallContext context)
        => Task.FromResult(new DrainWorkerResponse { Success = _runtime.DrainWorker(request.WorkerId) });

    public override Task<WorkerTotalLoadResponse> WorkerTotalLoad(WorkerTotalLoadRequest request, ServerCallContext context)
        => Task.FromResult(new WorkerTotalLoadResponse { Load = _runtime.TotalWorkerLoad() });

    public override Task<WorkerTotalCapacityResponse> WorkerTotalCapacity(WorkerTotalCapacityRequest request, ServerCallContext context)
        => Task.FromResult(new WorkerTotalCapacityResponse { Capacity = _runtime.TotalWorkerCapacity() });

    // ─── Sharding (Batch 32) ───────────────────────────────────────────────

    public override Task<ShardingAddHostResponse> ShardingAddHost(ShardingAddHostRequest request, ServerCallContext context)
    {
        _runtime.ShardingAddHost(request.Host);
        return Task.FromResult(new ShardingAddHostResponse());
    }

    public override Task<ShardingRemoveHostResponse> ShardingRemoveHost(ShardingRemoveHostRequest request, ServerCallContext context)
        => Task.FromResult(new ShardingRemoveHostResponse { Success = _runtime.ShardingRemoveHost(request.Host) });

    public override Task<ShardingMigrateResponse> ShardingMigrate(ShardingMigrateRequest request, ServerCallContext context)
        => Task.FromResult(new ShardingMigrateResponse { Success = _runtime.ShardingMigrate(request.ShardId, request.Host) });

    public override Task<ShardingHostCountResponse> ShardingHostCount(ShardingHostCountRequest request, ServerCallContext context)
        => Task.FromResult(new ShardingHostCountResponse { Count = _runtime.ShardingHostCount() });

    // ─── Partitions (Batch 32) ─────────────────────────────────────────────

    public override Task<PartitionCreateChildResponse> PartitionCreateChild(PartitionCreateChildRequest request, ServerCallContext context)
        => Task.FromResult(new PartitionCreateChildResponse { ChildId = _runtime.CreateChildPartition(request.ParentId, request.TqHash) });

    public override Task<PartitionDeleteResponse> PartitionDelete(PartitionDeleteRequest request, ServerCallContext context)
        => Task.FromResult(new PartitionDeleteResponse { Success = _runtime.DeletePartition(request.PartitionId) });

    public override Task<PartitionDepthResponse> PartitionDepth(PartitionDepthRequest request, ServerCallContext context)
        => Task.FromResult(new PartitionDepthResponse { Depth = _runtime.PartitionDepth(request.PartitionId) });

    public override Task<PartitionBacklogResponse> PartitionBacklog(PartitionBacklogRequest request, ServerCallContext context)
        => Task.FromResult(new PartitionBacklogResponse { Backlog = _runtime.PartitionBacklog(request.TqHash) });

    // ─── Search Attributes (Batch 32) ──────────────────────────────────────

    public override Task<GetWorkflowSearchAttributesResponse> GetWorkflowSearchAttributes(GetWorkflowSearchAttributesRequest request, ServerCallContext context)
    {
        var attrs = _runtime.GetWorkflowSearchAttributes(request.WorkflowKey);
        var resp = new GetWorkflowSearchAttributesResponse { Count = (ulong)attrs.Count };
        foreach (var kv in attrs) resp.Attributes[kv.Key] = kv.Value;
        return Task.FromResult(resp);
    }

    // ─── Replication Transport (Batch 33) ──────────────────────────────────

    public override Task<ReplicationAddLinkResponse> ReplicationAddLink(ReplicationAddLinkRequest request, ServerCallContext context)
    {
        _runtime.ReplicationAddLink(request.ClusterName, request.ClusterId, request.Endpoint);
        return Task.FromResult(new ReplicationAddLinkResponse());
    }

    public override Task<ReplicationRemoveLinkResponse> ReplicationRemoveLink(ReplicationRemoveLinkRequest request, ServerCallContext context)
        => Task.FromResult(new ReplicationRemoveLinkResponse { Success = _runtime.ReplicationRemoveLink(request.ClusterId) });

    public override Task<ReplicationSetLinkActiveResponse> ReplicationSetLinkActive(ReplicationSetLinkActiveRequest request, ServerCallContext context)
        => Task.FromResult(new ReplicationSetLinkActiveResponse { Success = _runtime.ReplicationSetLinkActive(request.ClusterId, request.Active) });

    public override Task<ReplicationPullTasksResponse> ReplicationPullTasks(ReplicationPullTasksRequest request, ServerCallContext context)
        => Task.FromResult(new ReplicationPullTasksResponse { TaskCount = _runtime.ReplicationPullForCluster(request.ClusterId, request.MaxCount) });

    public override Task<ReplicationPushTasksResponse> ReplicationPushTasks(ReplicationPushTasksRequest request, ServerCallContext context)
    {
        uint received = 0;
        foreach (var task in request.Tasks)
        {
            if (_runtime.ReplicationPushFromCluster(
                request.ClusterId, task.WorkflowKey, task.EventType,
                task.Payload.ToByteArray(), task.FailoverVersion, task.LastEventId))
            {
                received++;
            }
        }
        return Task.FromResult(new ReplicationPushTasksResponse { ReceivedCount = received });
    }

    public override Task<ReplicationLinkStatusResponse> ReplicationLinkStatus(ReplicationLinkStatusRequest request, ServerCallContext context)
        => Task.FromResult(new ReplicationLinkStatusResponse
        {
            ActiveLinks = _runtime.ReplicationActiveLinkCount(),
            PendingOutgoing = _runtime.ReplicationTotalPendingOutgoing(),
            PendingIncoming = _runtime.ReplicationTotalPendingIncoming(),
        });

    // ─── Replication Daemon (Batch 34) ──────────────────────────────────────
    public override Task<StartReplicationDaemonResponse> StartReplicationDaemon(StartReplicationDaemonRequest request, ServerCallContext context)
        => Task.FromResult(new StartReplicationDaemonResponse { Started = _runtime.ReplicationDaemonStart() });

    public override Task<StopReplicationDaemonResponse> StopReplicationDaemon(StopReplicationDaemonRequest request, ServerCallContext context)
        => Task.FromResult(new StopReplicationDaemonResponse { Stopped = _runtime.ReplicationDaemonStop() });

    public override Task<ReplicationDaemonStatusResponse> ReplicationDaemonStatus(ReplicationDaemonStatusRequest request, ServerCallContext context)
        => Task.FromResult(new ReplicationDaemonStatusResponse
        {
            IsRunning = _runtime.ReplicationDaemonIsRunning(),
            TotalCycles = _runtime.ReplicationDaemonStatCycles(),
            TotalDelivered = _runtime.ReplicationDaemonStatDelivered(),
            TotalApplied = _runtime.ReplicationDaemonStatApplied(),
            TotalFailures = _runtime.ReplicationDaemonStatFailures(),
            UptimeMs = _runtime.ReplicationDaemonStatUptime(),
            PendingOutgoing = _runtime.ReplicationTotalPendingOutgoing(),
            PendingIncoming = _runtime.ReplicationTotalPendingIncoming(),
            DeliveryLogCount = _runtime.ReplicationDaemonDeliveryCount(),
        });

    public override Task<ReplicationDaemonPollOnceResponse> ReplicationDaemonPollOnce(ReplicationDaemonPollOnceRequest request, ServerCallContext context)
    {
        var (delivered, applied) = _runtime.ReplicationDaemonPollOnce();
        return Task.FromResult(new ReplicationDaemonPollOnceResponse { Delivered = delivered, Applied = applied });
    }


    private ulong ResolveNamespaceId(string namespaceName)
    {
        if (string.IsNullOrEmpty(namespaceName) || namespaceName == "default")
            return 0;
        return (ulong)namespaceName.GetHashCode();
    }

    private static Protos.WorkflowExecutionStatus MapStatus(Core.WorkflowExecutionStatus status) => status switch
    {
        Core.WorkflowExecutionStatus.Running => Protos.WorkflowExecutionStatus.Running,
        Core.WorkflowExecutionStatus.Completed => Protos.WorkflowExecutionStatus.Completed,
        Core.WorkflowExecutionStatus.Failed => Protos.WorkflowExecutionStatus.Failed,
        Core.WorkflowExecutionStatus.Canceled => Protos.WorkflowExecutionStatus.Canceled,
        Core.WorkflowExecutionStatus.Terminated => Protos.WorkflowExecutionStatus.Terminated,
        Core.WorkflowExecutionStatus.ContinuedAsNew => Protos.WorkflowExecutionStatus.ContinuedAsNew,
        Core.WorkflowExecutionStatus.TimedOut => Protos.WorkflowExecutionStatus.TimedOut,
        _ => Protos.WorkflowExecutionStatus.Unspecified,
    };

    /// <summary>Parse a simple status filter from a SQL-like query string.</summary>
    private static int ParseStatusFilter(string query)
    {
        var upper = query.ToUpperInvariant();
        if (upper.Contains("RUNNING")) return 1;
        if (upper.Contains("COMPLETED")) return 2;
        if (upper.Contains("FAILED")) return 3;
        if (upper.Contains("CANCELED") || upper.Contains("CANCELLED")) return 4;
        if (upper.Contains("TERMINATED")) return 5;
        return -1;
    }
}
