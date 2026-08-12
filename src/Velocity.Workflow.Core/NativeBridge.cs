using System;
using System.Runtime.InteropServices;

namespace Velocity.Workflow.Core;

/// <summary>
/// P/Invoke C-ABI bridge to the Rust velocity_workflow_engine FFI library.
/// This is a THIN wrapper — all runtime logic (state machine, task queue, timer engine,
/// WAL persistence) lives in Rust with zero GC. C# only marshals calls and handles callbacks.
/// </summary>
public static unsafe partial class NativeBridge
{
    private const string CoreDll = "velocity_workflow_core";
    private const string EngineDll = "velocity_workflow_engine";

    // ─── Core Slab Operations (velocity-workflow-core) ───────────────────────

    [LibraryImport(CoreDll, EntryPoint = "velocity_slab_create")]
    public static partial int VelocitySlabCreate(ulong workflowId, ulong runId, uint totalSteps, DurableSlabHeader* outHeader);

    [LibraryImport(CoreDll, EntryPoint = "velocity_slab_mark_step")]
    public static partial int VelocitySlabMarkStep(DurableSlabHeader* header, uint stepIndex);

    [LibraryImport(CoreDll, EntryPoint = "velocity_slab_verify")]
    public static partial int VelocitySlabVerify(DurableSlabHeader* header);

    [LibraryImport(CoreDll, EntryPoint = "velocity_slab_merge_crdt")]
    public static partial int VelocitySlabMergeCrdt(void* targetCounter, void* sourceCounter);

    [LibraryImport(CoreDll, EntryPoint = "velocity_nda_verify")]
    public static partial int VelocityNdaVerify(NdaHeader* header);

    [LibraryImport(CoreDll, EntryPoint = "velocity_arena_alloc")]
    public static partial int VelocityArenaAlloc(void* arenaPage, byte* payloadPtr, nuint payloadLen, nuint* outOffset);

    [LibraryImport(CoreDll, EntryPoint = "velocity_vctp_packet_create")]
    public static partial int VelocityVctpPacketCreate(ulong sequenceNumber, ulong workflowId, uint slabOffset, uint payloadLength, VctpPacketHeader* outHeader);

    // ─── Engine Lifecycle (velocity-workflow-engine) ─────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_create")]
    public static partial void* VelocityEngineCreate();

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_destroy")]
    public static partial int VelocityEngineDestroy(void* handle);

    // ─── Workflow Lifecycle ───────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_start_workflow")]
    public static partial ulong VelocityEngineStartWorkflow(
        void* handle, ulong workflowId, ulong workflowTypeId, ulong namespaceId,
        ulong taskQueueHash, uint totalSteps, byte* inputPtr, uint inputLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_complete_workflow")]
    public static partial int VelocityEngineCompleteWorkflow(void* handle, ulong workflowKey, byte* resultPtr, uint resultLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_fail_workflow")]
    public static partial int VelocityEngineFailWorkflow(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cancel_workflow")]
    public static partial int VelocityEngineCancelWorkflow(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_terminate_workflow")]
    public static partial int VelocityEngineTerminateWorkflow(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_status")]
    public static partial int VelocityEngineGetStatus(void* handle, ulong workflowKey);

    // ─── Step Execution ───────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_is_step_completed")]
    public static partial int VelocityEngineIsStepCompleted(void* handle, ulong workflowKey, uint step);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_complete_step")]
    public static partial int VelocityEngineCompleteStep(void* handle, ulong workflowKey, uint step, byte* resultPtr, uint resultLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_step_result")]
    public static partial int VelocityEngineGetStepResult(void* handle, ulong workflowKey, uint step, byte* outBuf, uint outBufLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_current_step")]
    public static partial uint VelocityEngineGetCurrentStep(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_total_steps")]
    public static partial uint VelocityEngineGetTotalSteps(void* handle, ulong workflowKey);

    // ─── Activity Scheduling ──────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_schedule_activity")]
    public static partial int VelocityEngineScheduleActivity(void* handle, ulong workflowKey, uint step, ulong activityNameId, byte* argsPtr, uint argsLen);

    // ─── Task Queue Polling ───────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_poll_task")]
    public static partial int VelocityEnginePollTask(
        void* handle, ulong taskQueueHash,
        uint* outTaskKind, ulong* outWorkflowKey, uint* outStepIndex,
        ulong* outActivityNameId, ulong* outTaskId, uint* outAttempt);

    // ─── Signal / Query / Update ─────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_signal")]
    public static partial int VelocityEngineSignal(void* handle, ulong workflowKey, ulong signalNameId, byte* payloadPtr, uint payloadLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_has_signal")]
    public static partial int VelocityEngineHasSignal(void* handle, ulong workflowKey, ulong signalNameId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_update")]
    public static partial int VelocityEngineUpdate(void* handle, ulong workflowKey, ulong updateNameId, byte* payloadPtr, uint payloadLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_has_update")]
    public static partial int VelocityEngineHasUpdate(void* handle, ulong workflowKey, ulong updateNameId);

    // ─── Timer ────────────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_schedule_timer")]
    public static partial ulong VelocityEngineScheduleTimer(void* handle, ulong workflowKey, ulong delayMs);

    // ─── Merkle Verification ──────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_verify_slab")]
    public static partial int VelocityEngineVerifySlab(void* handle, ulong workflowKey);

    // ─── Stats ────────────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_workflow_count")]
    public static partial ulong VelocityEngineWorkflowCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_pending_tasks")]
    public static partial ulong VelocityEnginePendingTasks(void* handle, ulong taskQueueHash);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_pending_timers")]
    public static partial ulong VelocityEnginePendingTimers(void* handle);

    // ─── Child Workflows ──────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_start_child_workflow")]
    public static partial ulong VelocityEngineStartChildWorkflow(
        void* handle, ulong parentKey, ulong childWorkflowId, ulong workflowTypeId,
        ulong taskQueueHash, uint totalSteps, byte* inputPtr, uint inputLen);

    // ─── WAL Persistence ─────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_create_with_wal")]
    public static partial void* VelocityEngineCreateWithWal(byte* walPathPtr, uint walPathLen, ulong maxFileSize);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_wal_record_count")]
    public static partial ulong VelocityEngineWalRecordCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_wal_replay")]
    public static partial ulong VelocityEngineWalReplay(void* handle);

    // ─── Namespace Management ─────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_register_namespace")]
    public static partial ulong VelocityEngineRegisterNamespace(void* handle, byte* namePtr, uint nameLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_is_namespace_active")]
    public static partial int VelocityEngineIsNamespaceActive(void* handle, ulong namespaceId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_namespace_count")]
    public static partial ulong VelocityEngineNamespaceCount(void* handle);

    // ─── Visibility / Search ──────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_visibility_count")]
    public static partial ulong VelocityEngineVisibilityCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_visibility_count_by_status")]
    public static partial ulong VelocityEngineVisibilityCountByStatus(void* handle, uint status);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_visibility_count_by_namespace")]
    public static partial ulong VelocityEngineVisibilityCountByNamespace(void* handle, ulong namespaceId);

    // ─── Cron Scheduling ──────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_register_cron")]
    public static partial ulong VelocityEngineRegisterCron(
        void* handle, byte* cronExprPtr, uint cronExprLen,
        ulong workflowTypeId, ulong namespaceId, ulong taskQueueHash,
        uint totalSteps, ulong currentTimeMinutes);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_process_cron_fires")]
    public static partial ulong VelocityEngineProcessCronFires(void* handle, ulong currentTimeMinutes);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cron_schedule_count")]
    public static partial ulong VelocityEngineCronScheduleCount(void* handle);

    // ─── Batch Operations ─────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_batch_terminate")]
    public static partial ulong VelocityEngineBatchTerminate(void* handle, ulong* keysPtr, uint keysLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_batch_cancel")]
    public static partial ulong VelocityEngineBatchCancel(void* handle, ulong* keysPtr, uint keysLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_batch_signal")]
    public static partial ulong VelocityEngineBatchSignal(
        void* handle, ulong* keysPtr, uint keysLen,
        ulong signalNameId, byte* payloadPtr, uint payloadLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_batch_count")]
    public static partial ulong VelocityEngineBatchCount(void* handle);

    // ─── Archival ─────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_archive_count")]
    public static partial ulong VelocityEngineArchiveCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_archive_count_by_namespace")]
    public static partial ulong VelocityEngineArchiveCountByNamespace(void* handle, ulong namespaceId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_is_archived")]
    public static partial int VelocityEngineIsArchived(void* handle, ulong workflowKey);

    // ─── Event History ────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_event_count")]
    public static partial ulong VelocityEngineEventCount(void* handle, ulong workflowKey);

    // ─── Worker Versioning ────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_create_version_set")]
    public static partial ulong VelocityEngineCreateVersionSet(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_add_build_id")]
    public static partial int VelocityEngineAddBuildId(void* handle, ulong setId, byte* buildIdPtr, uint buildIdLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_version_set_count")]
    public static partial ulong VelocityEngineVersionSetCount(void* handle);

    // ─── Rate Limiter ─────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_rate_limit_check")]
    public static partial int VelocityEngineRateLimitCheck(void* handle, ulong namespaceId, uint tokens);

    // ─── Heartbeat ────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_register_heartbeat")]
    public static partial int VelocityEngineRegisterHeartbeat(void* handle, ulong workflowKey, ulong activityId, ulong timeoutMs);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_record_heartbeat")]
    public static partial int VelocityEngineRecordHeartbeat(void* handle, ulong workflowKey, ulong activityId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_heartbeat_active_count")]
    public static partial ulong VelocityEngineHeartbeatActiveCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_heartbeat_unregister")]
    public static partial void VelocityEngineHeartbeatUnregister(void* handle, ulong workflowKey, ulong activityId);

    // ─── Auth ─────────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_auth_check")]
    public static partial int VelocityEngineAuthCheck(void* handle, byte* subjectPtr, uint subjectLen, byte* rolePtr, uint roleLen, uint permission);

    // ─── Dynamic Config ───────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_config_set_int")]
    public static partial int VelocityEngineConfigSetInt(void* handle, byte* keyPtr, uint keyLen, long value);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_config_get_int")]
    public static partial long VelocityEngineConfigGetInt(void* handle, byte* keyPtr, uint keyLen, long defaultValue);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_config_set_bool")]
    public static partial int VelocityEngineConfigSetBool(void* handle, byte* keyPtr, uint keyLen, int value);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_config_set_float")]
    public static partial int VelocityEngineConfigSetFloat(void* handle, byte* keyPtr, uint keyLen, double value);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_config_set_string")]
    public static partial int VelocityEngineConfigSetString(void* handle, byte* keyPtr, uint keyLen, byte* valPtr, uint valLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_config_get_bool")]
    public static partial int VelocityEngineConfigGetBool(void* handle, byte* keyPtr, uint keyLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_config_get_float")]
    public static partial double VelocityEngineConfigGetFloat(void* handle, byte* keyPtr, uint keyLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_config_key_count")]
    public static partial ulong VelocityEngineConfigKeyCount(void* handle);

    // ─── Query Handler ────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_register_query_handler")]
    public static partial int VelocityEngineRegisterQueryHandler(void* handle, ulong workflowKey, ulong queryNameId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_query_handler_count")]
    public static partial ulong VelocityEngineQueryHandlerCount(void* handle);

    // ─── Memo ─────────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_set_memo")]
    public static partial int VelocityEngineSetMemo(void* handle, ulong workflowKey, byte* keyPtr, uint keyLen, byte* valPtr, uint valLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_memo_count")]
    public static partial ulong VelocityEngineMemoCount(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_memo")]
    public static partial int VelocityEngineGetMemo(void* handle, ulong workflowKey, byte* keyPtr, uint keyLen, byte* outPtr, uint outCap, uint* outLen);

    // ─── Schedules ────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_create_schedule")]
    public static partial ulong VelocityEngineCreateSchedule(void* handle, ulong workflowTypeId, ulong namespaceId, ulong taskQueueHash, uint overlapPolicy, ulong jitter);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_schedule_count")]
    public static partial ulong VelocityEngineScheduleCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_pause_schedule")]
    public static partial int VelocityEnginePauseSchedule(void* handle, ulong scheduleId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_unpause_schedule")]
    public static partial int VelocityEngineUnpauseSchedule(void* handle, ulong scheduleId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_delete_schedule")]
    public static partial int VelocityEngineDeleteSchedule(void* handle, ulong scheduleId);

    // ─── Workflow Reset ───────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_add_reset_point")]
    public static partial int VelocityEngineAddResetPoint(void* handle, ulong workflowKey, ulong eventId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_reset_point_count")]
    public static partial ulong VelocityEngineResetPointCount(void* handle, ulong workflowKey);

    // ─── Patches (Version Branching) ──────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_register_patch")]
    public static partial ulong VelocityEngineRegisterPatch(
        void* handle, ulong workflowTypeId, byte* markerPtr, uint markerLen,
        ulong minVersion, ulong maxVersion, byte* descPtr, uint descLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_patch_count")]
    public static partial ulong VelocityEnginePatchCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_deactivate_patch")]
    public static partial int VelocityEngineDeactivatePatch(void* handle, ulong patchId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_find_patch")]
    public static partial ulong VelocityEngineFindPatch(void* handle, ulong workflowTypeId, ulong version);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_patch")]
    public static partial int VelocityEngineGetPatch(void* handle, ulong patchId, ulong* outFields);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_active_patches_for_type")]
    public static partial uint VelocityEngineActivePatchesForType(void* handle, ulong workflowTypeId, ulong* outIds, uint maxCount);

    // ─── Cluster ──────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_register_cluster")]
    public static partial ulong VelocityEngineRegisterCluster(void* handle, byte* namePtr, uint nameLen, byte* addrPtr, uint addrLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cluster_count")]
    public static partial ulong VelocityEngineClusterCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_pending_replication_count")]
    public static partial ulong VelocityEnginePendingReplicationCount(void* handle);

    // ─── Sharding ─────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_shard_for_key")]
    public static partial uint VelocityEngineShardForKey(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_assign_shard")]
    public static partial int VelocityEngineAssignShard(void* handle, uint shardId, byte* hostPtr, uint hostLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_shard_count")]
    public static partial uint VelocityEngineShardCount(void* handle);

    // ─── Nexus ────────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_register_nexus_service")]
    public static partial int VelocityEngineRegisterNexusService(void* handle, byte* namePtr, uint nameLen, byte* endpointPtr, uint endpointLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_service_count")]
    public static partial ulong VelocityEngineNexusServiceCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_operation_count")]
    public static partial ulong VelocityEngineNexusOperationCount(void* handle);

    // ─── SignalWithStart ──────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_signal_with_start")]
    public static partial ulong VelocityEngineSignalWithStart(
        void* handle, ulong workflowId, ulong workflowTypeId, ulong namespaceId,
        ulong taskQueueHash, uint totalSteps, ulong signalNameId,
        byte* payloadPtr, uint payloadLen, uint* outWasStarted);

    // ─── ContinueAsNew ────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_continue_as_new")]
    public static partial ulong VelocityEngineContinueAsNew(void* handle, ulong workflowKey, byte* inputPtr, uint inputLen);

    // ─── Payload Codec ────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_codec_chain_len")]
    public static partial ulong VelocityEngineCodecChainLen(void* handle);

    // ─── Visibility Listing ─────────────────────────────────────────────────

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void WorkflowInfoCallback(
        ulong workflowKey, ulong workflowId, ulong runId, ulong workflowTypeId,
        ulong namespaceId, uint status, ulong startTimeMs, long closeTimeMs,
        ulong taskQueueHash, void* userData);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_list_workflows")]
    public static partial ulong VelocityEngineListWorkflows(
        void* handle, ulong namespaceFilter, int statusFilter,
        WorkflowInfoCallback callback, void* userData);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_set_search_attribute")]
    public static partial int VelocityEngineSetSearchAttribute(
        void* handle, ulong workflowKey, byte* keyPtr, uint keyLen, byte* valPtr, uint valLen);

    // ─── Activity Completion ────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_complete_activity")]
    public static partial int VelocityEngineCompleteActivity(
        void* handle, ulong workflowKey, uint step, byte* resultPtr, uint resultLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_fail_activity")]
    public static partial int VelocityEngineFailActivity(void* handle, ulong workflowKey, uint step);

    // ─── Event History Retrieval ────────────────────────────────────────────

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void HistoryEventCallback(
        ulong eventId, uint eventType, byte* payloadPtr, uint payloadLen, void* userData);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_event_history")]
    public static partial ulong VelocityEngineGetEventHistory(
        void* handle, ulong workflowKey, HistoryEventCallback callback, void* userData);

    // ─── Metrics ────────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_metrics_count")]
    public static partial ulong VelocityEngineMetricsCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_inc_counter")]
    public static partial int VelocityEngineIncCounter(void* handle, byte* namePtr, uint nameLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_counter")]
    public static partial ulong VelocityEngineGetCounter(void* handle, byte* namePtr, uint nameLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_set_gauge")]
    public static partial int VelocityEngineSetGauge(void* handle, byte* namePtr, uint nameLen, long value);

    // ─── Saga ───────────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_create_saga")]
    public static partial ulong VelocityEngineCreateSaga(void* handle, ulong workflowKey, uint stepCount);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_complete_saga_step")]
    public static partial int VelocityEngineCompleteSagaStep(void* handle, ulong sagaId, uint stepIndex);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_fail_saga_step")]
    public static partial uint VelocityEngineFailSagaStep(void* handle, ulong sagaId, uint stepIndex);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_saga_count")]
    public static partial ulong VelocityEngineSagaCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_saga_status")]
    public static partial int VelocityEngineSagaStatus(void* handle, ulong sagaId);

    // ─── Partition ──────────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_create_partition")]
    public static partial uint VelocityEngineCreatePartition(void* handle, ulong taskQueueHash);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_set_partition_forwarding")]
    public static partial int VelocityEngineSetPartitionForwarding(void* handle, uint fromPartition, uint toPartition, double rate);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_partition_count")]
    public static partial uint VelocityEnginePartitionCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_partition_pending")]
    public static partial ulong VelocityEnginePartitionPending(void* handle, ulong taskQueueHash);

    // ─── Replay Engine ──────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_replay")]
    public static partial int VelocityEngineReplay(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_replay_status")]
    public static partial int VelocityEngineReplayStatus(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_replay_step_count")]
    public static partial uint VelocityEngineReplayStepCount(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_replay_event_count")]
    public static partial uint VelocityEngineReplayEventCount(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_verify_determinism")]
    public static partial int VelocityEngineVerifyDeterminism(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_replay_count")]
    public static partial ulong VelocityEngineReplayCount(void* handle);

    // ─── Auth & Rate Limiting ──────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_authorize")]
    public static partial int VelocityEngineAuthorize(void* handle, byte* subjectPtr, uint subjectLen, ulong namespaceId, byte* rolesPtr, uint rolesLen, uint permission);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_role_count")]
    public static partial ulong VelocityEngineRoleCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_set_rate_limit")]
    public static partial int VelocityEngineSetRateLimit(void* handle, ulong namespaceId, double rate, ulong capacity);

    // ─── Timeout Enforcement ────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_schedule_activity_with_timeouts")]
    public static partial int VelocityEngineScheduleActivityWithTimeouts(
        void* handle, ulong workflowKey, uint step, ulong activityNameId,
        byte* argsPtr, uint argsLen,
        ulong scheduleToStartMs, ulong startToCloseMs, ulong scheduleToCloseMs, ulong heartbeatMs);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_check_activity_timeouts")]
    public static partial uint VelocityEngineCheckActivityTimeouts(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_check_workflow_timeouts")]
    public static partial uint VelocityEngineCheckWorkflowTimeouts(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_set_workflow_timeout")]
    public static partial int VelocityEngineSetWorkflowTimeout(void* handle, ulong workflowKey, ulong timeoutMs);

    // ─── Parent Close Policy ────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_apply_parent_close_policy")]
    public static partial int VelocityEngineApplyParentClosePolicy(void* handle, ulong parentKey, uint policy);

    // ─── Activity Retry ─────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_fail_activity_with_retry")]
    public static partial int VelocityEngineFailActivityWithRetry(void* handle, ulong workflowKey, uint step);

    // ─── Query Dispatch ─────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_execute_query")]
    public static partial int VelocityEngineExecuteQuery(
        void* handle, ulong workflowKey, ulong queryNameId,
        byte* inputPtr, uint inputLen, byte* outputPtr, uint outputLen);

    // ─── Workflow Reset ─────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_reset_workflow")]
    public static partial int VelocityEngineResetWorkflow(void* handle, ulong workflowKey, ulong resetToEventId);

    // ─── Visibility SQL Query ───────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_execute_visibility_query")]
    public static partial ulong VelocityEngineExecuteVisibilityQuery(
        void* handle, byte* queryPtr, uint queryLen, WorkflowInfoCallback callback, void* userData);

    // ─── Production Metrics Export ──────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_export_metrics")]
    public static partial int VelocityEngineExportMetrics(void* handle, byte* outPtr, uint outCap, uint* outLen);

    // ─── Namespace Listing ───────────────────────────────────────────────────

    public unsafe delegate void NamespaceInfoCallback(ulong id, byte* namePtr, uint nameLen, uint isActive, ulong retentionSecs, void* userData);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_list_namespaces")]
    public static partial ulong VelocityEngineListNamespaces(void* handle, delegate* unmanaged[Cdecl]<ulong, byte*, uint, uint, ulong, void*, void> callback, void* userData);

    // ─── Enhanced Describe Workflow ─────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_describe_workflow")]
    public static partial int VelocityEngineDescribeWorkflow(
        void* handle, ulong workflowKey, byte* outPtr, uint outCap, uint* outLen);

    // ─── Task Queue Partition Describe ──────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_describe_partition")]
    public static partial int VelocityEngineDescribePartition(
        void* handle, uint partitionId, byte* outPtr, uint outCap, uint* outLen);

    // ─── Cold Storage Archival ──────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_archive_workflow")]
    public static partial int VelocityEngineArchiveWorkflow(
        void* handle, ulong workflowKey, byte* baseDirPtr, uint baseDirLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_retrieve_workflow")]
    public static partial int VelocityEngineRetrieveWorkflow(
        void* handle, ulong workflowKey, byte* baseDirPtr, uint baseDirLen, byte* outStatus);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cold_storage_count")]
    public static partial int VelocityEngineColdStorageCount(
        void* handle, byte* baseDirPtr, uint baseDirLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cold_storage_list_keys")]
    public static partial uint VelocityEngineColdStorageListKeys(
        void* handle, byte* baseDirPtr, uint baseDirLen,
        delegate* unmanaged[Cdecl]<ulong, void*, void> callback, void* userData);

    // ─── Payload Codec Encode/Decode ────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_codec_encode")]
    public static partial int VelocityEngineCodecEncode(
        void* handle, byte* inPtr, uint inLen, byte* outPtr, uint outCap, uint* outLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_codec_decode")]
    public static partial int VelocityEngineCodecDecode(
        void* handle, byte* inPtr, uint inLen, byte* outPtr, uint outCap, uint* outLen);

    // ─── Saga Compensation + Step Info ──────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_complete_saga_compensation")]
    public static partial int VelocityEngineCompleteSagaCompensation(void* handle, ulong sagaId, uint stepIndex);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_saga_step_count")]
    public static partial uint VelocityEngineSagaStepCount(void* handle, ulong sagaId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_saga_step_status")]
    public static partial int VelocityEngineSagaStepStatus(void* handle, ulong sagaId, uint stepIndex);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_saga_current_step")]
    public static partial uint VelocityEngineSagaCurrentStep(void* handle, ulong sagaId);

    // ─── WAL Recovery ───────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_wal_recover")]
    public static partial long VelocityEngineWalRecover(void* handle, byte* walPathPtr, uint walPathLen);

    // ─── History Event Stream ──────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_history_page")]
    public static partial int VelocityEngineGetHistoryPage(
        void* handle, ulong workflowKey, ulong startEventId, uint maxCount,
        byte* outPtr, uint outCap, uint* outLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_history_event")]
    public static partial int VelocityEngineGetHistoryEvent(
        void* handle, ulong workflowKey, ulong eventId,
        byte* outPtr, uint outCap, uint* outLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_total_history_event_count")]
    public static partial ulong VelocityEngineTotalHistoryEventCount(void* handle);

    // ─── Enhanced Reset Introspection ──────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_latest_reset_event_id")]
    public static partial long VelocityEngineLatestResetEventId(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_total_reset_count")]
    public static partial ulong VelocityEngineTotalResetCount(void* handle);

    // ─── Saga Introspection ────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_saga_workflow_key")]
    public static partial ulong VelocityEngineSagaWorkflowKey(void* handle, ulong sagaId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_saga_overall_status")]
    public static partial int VelocityEngineSagaOverallStatus(void* handle, ulong sagaId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_saga_get")]
    public static partial int VelocityEngineSagaGet(void* handle, ulong sagaId, ulong* outFields);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_sagas_by_status")]
    public static partial uint VelocityEngineSagasByStatus(void* handle, int status, ulong* outIds, uint maxCount);

    // ─── Partition Enhanced ────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_partition_describe")]
    public static partial int VelocityEnginePartitionDescribe(void* handle, uint partitionId, ulong* outFields);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_partition_count_v2")]
    public static partial uint VelocityEnginePartitionCountV2(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_partition_ids")]
    public static partial uint VelocityEnginePartitionIds(void* handle, uint* outIds, uint maxCount);

    // ─── Engine Stats ──────────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_history_workflow_count")]
    public static partial ulong VelocityEngineHistoryWorkflowCount(void* handle);

    // ─── Worker Registry ──────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_register_worker")]
    public static partial ulong VelocityEngineRegisterWorker(
        void* handle, byte* addrPtr, uint addrLen,
        ulong* tqHashesPtr, uint tqCount,
        byte* versionPtr, uint versionLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_unregister_worker")]
    public static partial int VelocityEngineUnregisterWorker(void* handle, ulong workerId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_worker_heartbeat")]
    public static partial int VelocityEngineWorkerHeartbeat(void* handle, ulong workerId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_worker_count")]
    public static partial ulong VelocityEngineWorkerCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_active_worker_count")]
    public static partial ulong VelocityEngineActiveWorkerCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_worker_task_completed")]
    public static partial void VelocityEngineWorkerTaskCompleted(void* handle, ulong workerId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_worker_task_failed")]
    public static partial void VelocityEngineWorkerTaskFailed(void* handle, ulong workerId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_set_worker_status")]
    public static partial int VelocityEngineSetWorkerStatus(void* handle, ulong workerId, int status);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_detect_stale_workers")]
    public static partial ulong VelocityEngineDetectStaleWorkers(void* handle, ulong timeoutMs);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_worker_add_task_queue")]
    public static partial void VelocityEngineWorkerAddTaskQueue(void* handle, ulong workerId, ulong tqHash);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_total_tasks_completed")]
    public static partial ulong VelocityEngineTotalTasksCompleted(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_total_tasks_failed")]
    public static partial ulong VelocityEngineTotalTasksFailed(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_workers_for_queue")]
    public static partial uint VelocityEngineGetWorkersForQueue(
        void* handle, ulong tqHash, ulong* outPtr, uint outCap);

    // ─── Search Attribute Get/List ─────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_search_attribute")]
    public static partial int VelocityEngineGetSearchAttribute(
        void* handle, ulong workflowKey, byte* keyPtr, uint keyLen,
        byte* outPtr, uint outCap, uint* outLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_list_search_attributes")]
    public static partial uint VelocityEngineListSearchAttributes(
        void* handle, ulong workflowKey, byte* outPtr, uint outCap, uint* outLen);

    // ─── Workflow Timeout Enforcement ──────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_set_workflow_timeouts")]
    public static partial int VelocityEngineSetWorkflowTimeouts(
        void* handle, ulong workflowKey,
        ulong executionTimeoutMs, ulong runTimeoutMs, ulong taskTimeoutMs);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_check_timeouts")]
    public static partial ulong VelocityEngineCheckTimeouts(void* handle);

    // ─── Task Queue Stats ──────────────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_total_pending_tasks")]
    public static partial ulong VelocityEngineTotalPendingTasks(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_task_queue_count")]
    public static partial ulong VelocityEngineTaskQueueCount(void* handle);

    // ─── Replay Apply + Cold Storage Management ───────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_apply_replay")]
    public static partial int VelocityEngineApplyReplay(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cold_storage_delete")]
    public static partial int VelocityEngineColdStorageDelete(
        void* handle, ulong workflowKey, byte* baseDirPtr, uint baseDirLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cold_storage_gc")]
    public static partial int VelocityEngineColdStorageGc(
        void* handle, ulong retentionMs, byte* baseDirPtr, uint baseDirLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cold_storage_list_by_namespace")]
    public static partial uint VelocityEngineColdStorageListByNamespace(
        void* handle, ulong namespaceId, byte* baseDirPtr, uint baseDirLen, ulong* outPtr, uint outCap);

    // ─── Schedule Introspection + Dynamic Config ──────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_list_schedules")]
    public static partial uint VelocityEngineListSchedules(void* handle, ulong* outPtr, uint outCap);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_describe_schedule")]
    public static partial int VelocityEngineDescribeSchedule(void* handle, ulong scheduleId, ulong* outFields);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_schedule_is_paused")]
    public static partial int VelocityEngineScheduleIsPaused(void* handle, ulong scheduleId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_list_config_keys")]
    public static partial uint VelocityEngineListConfigKeys(void* handle, byte* outPtr, uint outCap);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_config_int")]
    public static partial long VelocityEngineGetConfigInt(void* handle, byte* keyPtr, uint keyLen);

    // ─── Heartbeat Timeout Check + Count Aggregation ──────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_check_heartbeat_timeouts")]
    public static partial uint VelocityEngineCheckHeartbeatTimeouts(void* handle, ulong* outPtr, uint outCap);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_count_by_status")]
    public static partial ulong VelocityEngineCountByStatus(void* handle, uint status);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_count_by_namespace")]
    public static partial ulong VelocityEngineCountByNamespace(void* handle, ulong namespaceId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_count_by_type")]
    public static partial ulong VelocityEngineCountByType(void* handle, ulong workflowTypeId);

    // ─── Namespace Retention + Query Dispatch ─────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_namespace_retention_ms")]
    public static partial ulong VelocityEngineGetNamespaceRetentionMs(void* handle, ulong namespaceId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cleanup_expired_workflows")]
    public static partial ulong VelocityEngineCleanupExpiredWorkflows(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_has_query_handler")]
    public static partial int VelocityEngineHasQueryHandler(void* handle, ulong workflowKey, ulong queryNameId);

    // ─── Cluster Replication (Batch 21) ──────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_enqueue_replication")]
    public static partial ulong VelocityEngineEnqueueReplication(void* handle, ulong sourceClusterId, ulong targetClusterId, ulong workflowKey, uint eventType, byte* payloadPtr, uint payloadLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_drain_replication_tasks")]
    public static partial ulong VelocityEngineDrainReplicationTasks(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_cluster_info")]
    public static partial ulong VelocityEngineGetClusterInfo(void* handle, ulong clusterId, ulong* outFields);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_local_cluster_id")]
    public static partial ulong VelocityEngineLocalClusterId(void* handle);

    // ─── Sharding Enhanced (Batch 21) ────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_assigned_shard_count")]
    public static partial ulong VelocityEngineAssignedShardCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_shard_owner")]
    public static partial int VelocityEngineGetShardOwner(void* handle, uint shardId, byte* outPtr, uint* outLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_shards_for_host")]
    public static partial ulong VelocityEngineGetShardsForHost(void* handle, byte* hostPtr, uint hostLen, uint* outShards, uint* outCount);

    // ─── Nexus Operations (Batch 21) ─────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_start_operation")]
    public static partial ulong VelocityEngineNexusStartOperation(void* handle, byte* servicePtr, uint serviceLen, byte* operationPtr, uint operationLen, ulong workflowKey, byte* inputPtr, uint inputLen, byte* callbackPtr, uint callbackLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_complete_operation")]
    public static partial int VelocityEngineNexusCompleteOperation(void* handle, ulong operationId, byte* resultPtr, uint resultLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_fail_operation")]
    public static partial int VelocityEngineNexusFailOperation(void* handle, ulong operationId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_get_operation")]
    public static partial ulong VelocityEngineNexusGetOperation(void* handle, ulong operationId, ulong* outFields);

    // ─── Rate Limiter Enhanced (Batch 22) ────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_rate_set_namespace_limit")]
    public static partial int VelocityEngineRateSetNamespaceLimit(void* handle, ulong namespaceId, double rate, ulong capacity);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_rate_namespace_count")]
    public static partial ulong VelocityEngineRateNamespaceCount(void* handle);

    // ─── Memo Enhanced (Batch 22) ────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_remove_memo")]
    public static partial int VelocityEngineRemoveMemo(void* handle, ulong workflowKey, byte* keyPtr, uint keyLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_memo_workflow_count")]
    public static partial ulong VelocityEngineMemoWorkflowCount(void* handle);

    // ─── Worker Versioning Enhanced (Batch 22) ───────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_versioning_set_current")]
    public static partial int VelocityEngineVersioningSetCurrent(void* handle, ulong setId, byte* buildIdPtr, uint buildIdLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_versioning_get_current")]
    public static partial int VelocityEngineVersioningGetCurrent(void* handle, ulong setId, byte* outPtr, uint* outLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_versioning_add_routing_rule")]
    public static partial int VelocityEngineVersioningAddRoutingRule(void* handle, byte* tqPtr, uint tqLen, byte* bidPtr, uint bidLen, uint percentage);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_versioning_resolve_build_id")]
    public static partial int VelocityEngineVersioningResolveBuildId(void* handle, byte* tqPtr, uint tqLen, byte* outPtr, uint* outLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_versioning_routing_rule_count")]
    public static partial ulong VelocityEngineVersioningRoutingRuleCount(void* handle);

    // ─── Auth Enhanced (Batch 22) ────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_auth_deny_subject")]
    public static partial int VelocityEngineAuthDenySubject(void* handle, byte* subjectPtr, uint subjectLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_auth_role_count")]
    public static partial ulong VelocityEngineAuthRoleCount(void* handle);

    // ─── Metrics Enhanced (Batch 23) ─────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_metrics_inc_counter")]
    public static partial int VelocityEngineMetricsIncCounter(void* handle, byte* namePtr, uint nameLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_metrics_get_counter")]
    public static partial ulong VelocityEngineMetricsGetCounter(void* handle, byte* namePtr, uint nameLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_metrics_set_gauge")]
    public static partial int VelocityEngineMetricsSetGauge(void* handle, byte* namePtr, uint nameLen, long value);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_metrics_get_gauge")]
    public static partial long VelocityEngineMetricsGetGauge(void* handle, byte* namePtr, uint nameLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_metrics_observe_histogram")]
    public static partial int VelocityEngineMetricsObserveHistogram(void* handle, byte* namePtr, uint nameLen, double value);

    // ─── History Store Enhanced (Batch 23) ───────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_history_event_count")]
    public static partial ulong VelocityEngineHistoryEventCount(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_history_remove")]
    public static partial int VelocityEngineHistoryRemove(void* handle, ulong workflowKey);

    // ─── Archive Store Enhanced (Batch 23) ───────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_archive_retrieve")]
    public static partial int VelocityEngineArchiveRetrieve(void* handle, ulong workflowKey, ulong* outFields);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_archive_delete")]
    public static partial int VelocityEngineArchiveDelete(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_archive_count_by_status")]
    public static partial ulong VelocityEngineArchiveCountByStatus(void* handle, uint status);

    // ─── Namespace Enhanced (Batch 24) ───────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_describe_namespace")]
    public static partial int VelocityEngineDescribeNamespace(void* handle, ulong namespaceId, ulong* outFields);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_namespace_workflow_count")]
    public static partial ulong VelocityEngineNamespaceWorkflowCount(void* handle, ulong namespaceId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_deactivate_namespace")]
    public static partial int VelocityEngineDeactivateNamespace(void* handle, ulong namespaceId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_activate_namespace")]
    public static partial int VelocityEngineActivateNamespace(void* handle, ulong namespaceId);

    // ─── Cron Enhanced (Batch 24) ────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cron_next_fire_time")]
    public static partial ulong VelocityEngineCronNextFireTime(void* handle, ulong scheduleId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cron_fire_count")]
    public static partial ulong VelocityEngineCronFireCount(void* handle, ulong scheduleId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cron_unregister")]
    public static partial int VelocityEngineCronUnregister(void* handle, ulong scheduleId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cron_set_paused")]
    public static partial int VelocityEngineCronSetPaused(void* handle, ulong scheduleId, int paused);

    // ─── Codec Enhanced (Batch 24) ───────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_codec_count")]
    public static partial ulong VelocityEngineCodecCount(void* handle);

    // ─── Search Attr Enhanced (Batch 24) ─────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_search_attr_count")]
    public static partial ulong VelocityEngineSearchAttrCount(void* handle, ulong workflowKey);

    // ─── Cloud Storage Adapter (Batch 28) ────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cloud_storage_set_backend")]
    public static partial int VelocityEngineCloudStorageSetBackend(void* handle, uint backend);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cloud_archive")]
    public static partial int VelocityEngineCloudArchive(void* handle, ulong workflowKey, ulong namespaceId, int status);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cloud_contains")]
    public static partial int VelocityEngineCloudContains(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cloud_delete")]
    public static partial int VelocityEngineCloudDelete(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cloud_count")]
    public static partial ulong VelocityEngineCloudCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cloud_list_by_namespace")]
    public static partial uint VelocityEngineCloudListByNamespace(void* handle, ulong namespaceId, ulong* outKeys, uint maxCount);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cloud_gc")]
    public static partial int VelocityEngineCloudGc(void* handle, ulong retentionMs);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cloud_backend_name")]
    public static partial uint VelocityEngineCloudBackendName(void* handle, byte* outName, uint maxLen);

    // ─── Query/Reset Enhanced (Batch 28) ─────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_unregister_query_handler")]
    public static partial int VelocityEngineUnregisterQueryHandler(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_reset_points")]
    public static partial uint VelocityEngineGetResetPoints(void* handle, ulong workflowKey, ulong* outEventIds, uint maxCount);

    // ─── Visibility Listing Enhanced (Batch 29) ───────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_list_by_search_attribute")]
    public static partial uint VelocityEngineListBySearchAttribute(void* handle, byte* attrKeyPtr, uint attrKeyLen, byte* attrValPtr, uint attrValLen, ulong* outKeys, uint maxCount);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_list_by_time_range")]
    public static partial uint VelocityEngineListByTimeRange(void* handle, ulong startTimeMs, ulong endTimeMs, ulong* outKeys, uint maxCount);

    // ─── Replay Cache Management (Batch 29) ───────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_replay_invalidate")]
    public static partial void VelocityEngineReplayInvalidate(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_replay_clear_cache")]
    public static partial void VelocityEngineReplayClearCache(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_replay_cache_size")]
    public static partial ulong VelocityEngineReplayCacheSize(void* handle);

    // ─── Schedule Management Enhanced (Batch 29) ──────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_schedule_set_overlap_policy")]
    public static partial int VelocityEngineScheduleSetOverlapPolicy(void* handle, ulong scheduleId, uint policy);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_schedule_set_remaining_actions")]
    public static partial int VelocityEngineScheduleSetRemainingActions(void* handle, ulong scheduleId, ulong remaining);

    // ─── Event History Enhanced (Batch 29) ────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_history_workflow_count_v2")]
    public static partial ulong VelocityEngineHistoryWorkflowCountV2(void* handle);

    // ─── Partition Worker Management (Batch 29) ───────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_partition_total_pending")]
    public static partial ulong VelocityEnginePartitionTotalPending(void* handle, ulong taskQueueHash);

    // ─── Nexus Enhanced (Batch 29) ────────────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_register_service")]
    public static partial int VelocityEngineNexusRegisterService(void* handle, byte* serviceNamePtr, uint serviceNameLen, byte* endpointPtr, uint endpointLen);

    // ─── Real Cloud Storage SDK (Batch 29) ────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cloud_set_s3")]
    public static partial int VelocityEngineCloudSetS3(void* handle, byte* bucketPtr, uint bucketLen, byte* regionPtr, uint regionLen, byte* accessKeyPtr, uint accessKeyLen, byte* secretKeyPtr, uint secretKeyLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_cloud_set_gcs")]
    public static partial int VelocityEngineCloudSetGcs(void* handle, byte* bucketPtr, uint bucketLen, byte* tokenPtr, uint tokenLen);

    // ─── Search Attributes + Replication (Batch 30) ─────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_start_workflow_with_attrs")]
    public static partial ulong VelocityEngineStartWorkflowWithAttrs(void* handle, ulong workflowId, ulong workflowTypeId, ulong namespaceId, ulong taskQueueHash, uint totalSteps, byte* inputPtr, uint inputLen, byte** attrKeys, uint* attrKeyLens, uint attrKeyCount, byte** attrVals, uint* attrValLens);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_apply_replication_task")]
    public static partial int VelocityEngineApplyReplicationTask(void* handle, ulong sourceClusterId, ulong targetClusterId, ulong workflowKey, uint eventType, byte* payloadPtr, uint payloadLen, ulong failoverVersion);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_process_fired_timer")]
    public static partial void VelocityEngineProcessFiredTimer(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_replication_status")]
    public static partial int VelocityEngineReplicationStatus(void* handle, ulong* outStatus);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_set_cluster_active")]
    public static partial int VelocityEngineSetClusterActive(void* handle, ulong clusterId, int active);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_set_failover_version")]
    public static partial int VelocityEngineSetFailoverVersion(void* handle, ulong clusterId, ulong version);

    // ─── Nexus Full Lifecycle (Batch 31) ──────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_mark_started")]
    public static partial int VelocityEngineNexusMarkStarted(void* handle, ulong opId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_cancel")]
    public static partial int VelocityEngineNexusCancel(void* handle, ulong opId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_timeout")]
    public static partial int VelocityEngineNexusTimeout(void* handle, ulong opId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_retry")]
    public static partial int VelocityEngineNexusRetry(void* handle, ulong opId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_nexus_count_by_state")]
    public static partial ulong VelocityEngineNexusCountByState(void* handle, uint state);

    // ─── Worker Registry Load-Aware Dispatch (Batch 31) ───────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_select_worker")]
    public static partial ulong VelocityEngineSelectWorker(void* handle, ulong tqHash);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_worker_has_capacity")]
    public static partial int VelocityEngineWorkerHasCapacity(void* handle, ulong workerId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_drain_worker")]
    public static partial int VelocityEngineDrainWorker(void* handle, ulong workerId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_total_worker_load")]
    public static partial ulong VelocityEngineTotalWorkerLoad(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_total_worker_capacity")]
    public static partial ulong VelocityEngineTotalWorkerCapacity(void* handle);

    // ─── Consistent Hash Ring Sharding (Batch 31) ─────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_sharding_add_host")]
    public static partial void VelocityEngineShardingAddHost(void* handle, byte* hostPtr, uint hostLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_sharding_remove_host")]
    public static partial int VelocityEngineShardingRemoveHost(void* handle, byte* hostPtr, uint hostLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_sharding_migrate")]
    public static partial int VelocityEngineShardingMigrate(void* handle, uint shardId, byte* hostPtr, uint hostLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_sharding_host_count")]
    public static partial ulong VelocityEngineShardingHostCount(void* handle);

    // ─── Hierarchical Partitions (Batch 31) ───────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_create_child_partition")]
    public static partial ulong VelocityEngineCreateChildPartition(void* handle, uint parentId, ulong tqHash);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_delete_partition")]
    public static partial int VelocityEngineDeletePartition(void* handle, uint partitionId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_partition_depth")]
    public static partial int VelocityEnginePartitionDepth(void* handle, uint partitionId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_partition_backlog")]
    public static partial ulong VelocityEnginePartitionBacklog(void* handle, ulong tqHash);

    // ─── Get Workflow Search Attributes (Batch 32) ─────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_search_attr_count")]
    public static partial ulong VelocityEngineGetSearchAttrCount(void* handle, ulong workflowKey);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_search_attr_key")]
    public static partial uint VelocityEngineGetSearchAttrKey(void* handle, ulong workflowKey, ulong index, byte* outKey, uint outKeyCap);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_get_search_attr_val")]
    public static partial uint VelocityEngineGetSearchAttrVal(void* handle, ulong workflowKey, ulong index, byte* outVal, uint outValCap);

    // ─── Replication Transport (Batch 33) ──────────────────────────────────

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_add_link")]
    public static partial void VelocityEngineReplAddLink(void* handle, byte* clusterName, uint clusterNameLen, ulong clusterId, byte* endpoint, uint endpointLen);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_remove_link")]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static partial bool VelocityEngineReplRemoveLink(void* handle, ulong clusterId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_set_link_active")]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static partial bool VelocityEngineReplSetLinkActive(void* handle, ulong clusterId, [MarshalAs(UnmanagedType.Bool)] bool active);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_pull_for_cluster")]
    public static partial uint VelocityEngineReplPullForCluster(void* handle, ulong clusterId, uint maxCount);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_push_from_cluster")]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static partial bool VelocityEngineReplPushFromCluster(void* handle, ulong clusterId, ulong workflowKey, uint eventType, byte* payload, uint payloadLen, ulong failoverVersion, ulong lastEventId);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_active_link_count")]
    public static partial ulong VelocityEngineReplActiveLinkCount(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_total_pending_outgoing")]
    public static partial ulong VelocityEngineReplTotalPendingOutgoing(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_total_pending_incoming")]
    public static partial ulong VelocityEngineReplTotalPendingIncoming(void* handle);

    // ── Replication Daemon (Batch 34) ──────────────────────────────────────
    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_daemon_start")]
    public static partial uint VelocityEngineReplDaemonStart(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_daemon_stop")]
    public static partial uint VelocityEngineReplDaemonStop(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_daemon_is_running")]
    public static partial uint VelocityEngineReplDaemonIsRunning(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_daemon_poll_once")]
    public static partial ulong VelocityEngineReplDaemonPollOnce(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_daemon_stat_cycles")]
    public static partial ulong VelocityEngineReplDaemonStatCycles(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_daemon_stat_delivered")]
    public static partial ulong VelocityEngineReplDaemonStatDelivered(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_daemon_stat_applied")]
    public static partial ulong VelocityEngineReplDaemonStatApplied(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_daemon_stat_failures")]
    public static partial ulong VelocityEngineReplDaemonStatFailures(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_daemon_stat_uptime")]
    public static partial ulong VelocityEngineReplDaemonStatUptime(void* handle);

    [LibraryImport(EngineDll, EntryPoint = "velocity_engine_repl_daemon_delivery_count")]
    public static partial ulong VelocityEngineReplDaemonDeliveryCount(void* handle);

    // --- Batch 35+: Raft Consensus ---
    [LibraryImport(EngineDll, EntryPoint = "velocity_raft_create_group")]
    public static partial ulong RaftCreateGroup(ulong nodeId);
    [LibraryImport(EngineDll, EntryPoint = "velocity_raft_become_leader")]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static partial bool RaftBecomeLeader(ulong groupId);
    [LibraryImport(EngineDll, EntryPoint = "velocity_raft_append_entry")]
    public static partial ulong RaftAppendEntry(ulong groupId, ulong workflowKey, byte eventType, byte* payload, uint payloadLen);
    [LibraryImport(EngineDll, EntryPoint = "velocity_raft_apply_committed")]
    public static partial ulong RaftApplyCommitted(ulong groupId);
    [LibraryImport(EngineDll, EntryPoint = "velocity_raft_group_count")]
    public static partial ulong RaftGroupCount();
    [LibraryImport(EngineDll, EntryPoint = "velocity_raft_stat_committed")]
    public static partial ulong RaftStatCommitted();

    // --- Batch 35+: History Compaction ---
    [LibraryImport(EngineDll, EntryPoint = "velocity_compact_append_event")]
    public static partial ulong CompactAppendEvent(ulong workflowKey, byte eventType);
    [LibraryImport(EngineDll, EntryPoint = "velocity_compact_workflow")]
    public static partial ulong CompactWorkflow(ulong workflowKey);
    [LibraryImport(EngineDll, EntryPoint = "velocity_compact_all")]
    public static partial ulong CompactAll();
    [LibraryImport(EngineDll, EntryPoint = "velocity_compact_event_count")]
    public static partial ulong CompactEventCount(ulong workflowKey);

    // --- Batch 35+: Durable RPC ---
    [LibraryImport(EngineDll, EntryPoint = "velocity_rpc_initiate")]
    public static partial ulong RpcInitiate(byte* caller, uint callerLen, byte* target, uint targetLen, byte* method, uint methodLen);
    [LibraryImport(EngineDll, EntryPoint = "velocity_rpc_complete")]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static partial bool RpcComplete(ulong rpcId);
    [LibraryImport(EngineDll, EntryPoint = "velocity_rpc_fail")]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static partial bool RpcFail(ulong rpcId);
    [LibraryImport(EngineDll, EntryPoint = "velocity_rpc_count")]
    public static partial ulong RpcCount();
    [LibraryImport(EngineDll, EntryPoint = "velocity_rpc_stat_completed")]
    public static partial ulong RpcStatCompleted();

    // --- Batch 35+: AI Context ---
    [LibraryImport(EngineDll, EntryPoint = "velocity_ai_add_message")]
    public static partial ulong AiAddMessage(byte role, byte* content, uint contentLen);
    [LibraryImport(EngineDll, EntryPoint = "velocity_ai_compress")]
    public static partial ulong AiCompress();
    [LibraryImport(EngineDll, EntryPoint = "velocity_ai_current_tokens")]
    public static partial ulong AiCurrentTokens();
    [LibraryImport(EngineDll, EntryPoint = "velocity_ai_message_count")]
    public static partial ulong AiMessageCount();
    [LibraryImport(EngineDll, EntryPoint = "velocity_ai_add_tool_call")]
    public static partial ulong AiAddToolCall(byte* tool, uint toolLen, byte* args, uint argsLen);
}
