//! velocity-workflow-engine
//!
//! Hardware-native workflow execution engine. The entire runtime — state machine scheduling,
//! task queue, timer engine, WAL persistence, signal/query routing — lives in Rust with
//! zero managed heap allocations. C# acts as a thin FFI bridge only.
//!
//! Architecture:
//!   [C# Developer Code] ──FFI──► [velocity-workflow-engine] ──► [velocity-workflow-core]
//!   (thin bridge)                (runtime engine, zero-GC)      (slab, bitmask, Merkle)

pub mod advanced_scheduler;
pub mod ai_context;
pub mod archival;
pub mod auth;
pub mod auth_v2;
pub mod batch;
pub mod chaos_endurance;
pub mod cluster;
pub mod cold_storage;
pub mod cron;
pub mod db_adapter;
pub mod durable_rpc;
pub mod dynamic_config;
pub mod engine;
pub mod errors;
pub mod event_history;
pub mod ffi;
pub mod graceful_shutdown;
pub mod hardware_traits;
pub mod hardware_integration;
pub mod health_check;
pub mod heartbeat;
pub mod history_compaction;
pub mod history_shard;
pub mod hot_swap;
pub mod matching_service;
pub mod memo;
pub mod metrics;
pub mod metrics_export;
pub mod migration_runner;
pub mod multi_region;
pub mod namespace;
pub mod ndc_replication;
pub mod network_replication;
pub mod nexus;
pub mod observability;
pub mod partition;
pub mod patch;
pub mod payload_codec;
pub mod query_handler;
pub mod raft_consensus;
pub mod rate_limiter;
pub mod replication_daemon;
pub mod replication_transport;
pub mod replay;
pub mod resource_limits;
pub mod retry;
pub mod saga;
pub mod schedules;
pub mod search_index;
pub mod sharding;
pub mod task_queue;
pub mod timer_engine;
pub mod codec_server;
pub mod deployment_api;
pub mod reachability;
pub mod update;
pub mod validation;
pub mod visibility;
pub mod visibility_query;
pub mod wal;
pub mod worker_versioning;
pub mod worker_determinism;
pub mod worker_registry;
pub mod worker_sessions;
pub mod worker_service;
pub mod workflow_reset;
pub mod failure_types;
pub mod async_activity;
pub mod advanced_operations;
pub mod depth_operations;
pub mod operational_api;
pub mod core_internals;
pub mod queue_processing;
pub mod matching_engine;
pub mod history_builder;
pub mod system_workflows;
pub mod workflow_context;
pub mod hsm_framework;
pub mod membership;
pub mod persistence_layer;
pub mod ndc_replication_deep;
pub mod matching_deep;
pub mod worker_services;
pub mod frontend_service;
pub mod namespace_mgmt;
pub mod common_utils;
pub mod persistence_sql;
pub mod persistence_visibility;
pub mod history_api;
pub mod matching_workers;
pub mod frontend_handlers;

// gRPC server module — only compiled when the `grpc` feature is enabled.
// Requires protoc to be installed for proto compilation.
#[cfg(feature = "grpc")]
pub mod grpc_server;

pub use advanced_scheduler::{CronExpression as CronExpressionV2, CronError as CronErrorV2, WorkflowSchedule, ScheduleManager as AdvancedScheduleManager, ScheduleInfo, RateLimiterV2, StickyScheduler, WorkerVersioningV2};
pub use multi_region::{RegionConfig, RegionState, RegionInfo, MultiRegionReplicator, ReplicationResult, SyncResult, ConflictResolutionStrategy, ReplicationConflict, ResolvedValue, FailoverController, FailoverResult, FailoverEvent, HealthStatus};
pub use errors::{VelocityError, VelocityResult, ErrorCategory, ErrorCode, FfiErrorCode};
pub use retry::{RetryPolicy, RetryExecutor, RetryStats, CircuitBreaker, CircuitBreakerConfig, CircuitBreakerMetrics, CircuitState};
pub use ai_context::{AiContextWindow, AiContextConfig, AiContextStats, MessageRole, ContextMessage, AgentToolCall, ToolCallStatus};
pub use archival::{ArchiveStore, ArchiveRecord, ArchivePolicy};
pub use auth::{AuthManager, Permission, Role, Claims};
pub use auth_v2::{ApiKey, ApiKeyManager, ApiPermission, OAuth2Config, OAuth2Validator, Claims as V2Claims, AuthError, AuditLog, AuditLogger, AuditFilter, AuditResult, EncryptionAtRest, EncryptionConfig, EncryptionAlgorithm};
pub use batch::{BatchExecutor, BatchResult, BatchOperationType, BatchStatus};
pub use chaos_endurance::{SoakTestConfig, SoakTestMetrics, run_soak_test, run_crash_recovery_test};
pub use cluster::{ClusterManager, ClusterInfo, ReplicationTask};
pub use cold_storage::{FileColdStorage, ColdStorageRecord};
pub use cron::{CronScheduler, CronExpression, CronError, CronFireEvent};
pub use db_adapter::{DatabaseAdapter, DatabaseConfig, DatabaseError, DatabaseResult, PostgresAdapter, InMemoryAdapter, MysqlAdapter, CassandraAdapter, CassandraConsistency, SqliteAdapter, SqliteJournalMode, WorkflowRecord, WorkflowEventRecord, SearchAttributeValue as DbSearchAttributeValue, SearchAttributes, StatusFilter, SslMode};
pub use durable_rpc::{DurableServiceMesh, DurableRpcConfig, DurableRpcStats, DurableRpcState, DurableRpcCall};
pub use dynamic_config::{DynamicConfig, ConfigValue, Constraints, ConstrainedValue, ConfigKey, Precedence, ConfigClient, MemoryConfigClient, StaticConfigClient, ConfigCollection, GradualChange, ConfigRegistry};
pub use engine::{WorkflowEngine, WorkflowContext, WorkflowStatus, WorkflowExecutionDescription, PendingActivityInfo, PendingActivityState, PendingChildInfo, PendingSignalInfo};
pub use event_history::{HistoryStore, HistoryEvent, HistoryEventType};
pub use hardware_traits::{SmartNicOffload, TeeEnclave, PeerToPeerReplication, SelfHealingEcc, HardwareError};
pub use hardware_integration::{HardwareAbstractionLayer, EccParityStore, EccStats, MerkleEccResult, compute_simple_merkle_root};
pub use heartbeat::HeartbeatTracker;
pub use history_compaction::{HistoryCompactor, CompactionConfig, CompactionStats, CompactionLevel, CompactableEventType};
pub use history_shard::{HistoryShardManager, ShardContext, MutableState, TransferTask, TransferTaskKind, ShardOwnership, ShardState, ShardStats};
pub use hot_swap::{HotSwapRegistry, HotSwapPatch, HotSwapResult, HotSwapStats};
pub use matching_service::{MatchingService, MatchTask, PollerInfo, TaskKindFilter, MatchingServiceConfig, MatchingServiceStats, TaskQueueDescription, PollerDescription, TaskQueuePartitionInfo};
pub use memo::{MemoStore, MemoEntry, MemoStats, MemoSetResult};
pub use metrics::MetricsRegistry;
pub use migration_runner::{Migration, MigrationRunner, MigrationResult, MigrationStatus, MigrationError, MigrationAdapter};
pub use namespace::{NamespaceRegistry, NamespaceConfig, NamespaceError};
pub use ndc_replication::{ConflictResolver, ConflictResolution, ReplicationConflict as NdcReplicationConflict, TaskAckTracker, TaskAckState, TaskAckRecord, TaskAckTrackerStats, ReplicationDlq, DlqTask, DlqStats, NamespaceReplicationController, NamespaceReplicationConfig, HistoryGapDetector, HistoryGap, ConsistencyChecker, ConsistencyCheckResult};
pub use network_replication::{TcpReplicationServer, TcpReplicationConfig, TcpReplicationStats, UdpReplicationTransport, UdpReplicationConfig, UdpReplicationStats, WireFrame, FrameType, encode_tasks, decode_tasks};
pub use nexus::{NexusManager, NexusOperation, NexusOperationState};
pub use observability::{ObservabilityConfig, ObservabilityContext, StructuredLogger, MetricsExporter, SpanTracker, LogLevel, SpanId, SpanStatus, init_global, global};
pub use partition::{PartitionManager, PartitionInfo};
pub use patch::{PatchRegistry, WorkflowPatch};
pub use payload_codec::{PayloadCodec, CodecChain, CodecError, IdentityCodec, XorCodec, CompressionCodec, EncryptionCodec, SizeLimitCodec, CodecRegistry, Payload, PayloadMetadata, PayloadValidator, CodecChainStats};
pub use query_handler::{QueryRegistry, QueryHandler, QueryDefinition, QueryRecord, QueryState, QueryConsistency, QueryStats, BufferedQuery, RejectionPolicy};
pub use raft_consensus::{RaftNode, RaftCluster, RaftConfig, RaftStats, RaftState, RaftLogEntry};
pub use rate_limiter::{RateLimiter, TokenBucket, ClockedRateLimiter, MultiRateLimiter, PriorityRateLimiter, RoutingRateLimiter, DelayedRateLimiter, NamespaceRateLimiter, QuotaTracker, QuotaUsage, RequestPriority, RateRequest, Reservation, MultiReservation};
pub use replication_daemon::{ReplicationDaemon, ReplicationDaemonConfig, ReplicationDaemonStats, DeliveredTask};
pub use replication_transport::{ReplicationTransport, ReplicationLinkStatus};
pub use replay::{ReplayEngine, ReplayResult, ReplayActivityState, ReplayActivityStatus};
pub use saga::{SagaOrchestrator, SagaStepDefinition, SagaStatus};
pub use schedules::{ScheduleManager, ScheduleEntry, OverlapPolicy, CalendarSpec, ScheduleState, ScheduleAction};
pub use search_index::{SearchAttributeIndex, SearchIndexStats, IndexedValue, IndexKey, SearchAttributeSchema, SearchAttributeType, SearchAttributeField, SchemaError, VisibilityQueryParser, QueryNode, QueryValue, BulkIndexer, BulkOperation, BulkIndexerStats, IndexLifecycleManager, IndexMetadata, IndexState};
pub use sharding::ShardManager;
pub use task_queue::{TaskQueue, TaskItem, TaskKind, QueueStats};
pub use timer_engine::TimerEngine;
pub use visibility::{VisibilityIndex, WorkflowExecutionInfo, SearchAttributeValue, VisibilityFilter, VisibilityQuery as AdvancedVisibilityQuery, PaginatedResult, PageToken, SortField, SortOrder, VisibilityAggregation};
pub use visibility_query::{VisibilityQuery, QueryCondition, QueryField, QueryOp};
pub use wal::{WalManager, WalWriter, WalRecord, WalEventType};
pub use worker_versioning::{WorkerVersioning, BuildId, VersionSet, RoutingRule, DeploymentInfo};
pub use worker_registry::{WorkerRegistry, WorkerInfo, WorkerStatus};
pub use workflow_reset::{WorkflowResetter, ResetPoint, ResetSpec, ResetResult, ResetReason, HistoryBranch, PendingSignal, LastFailureResetPolicy};
pub use graceful_shutdown::{ShutdownController, ShutdownStatus, GracefulShutdownConfig};
pub use health_check::{HealthChecker, HealthStatus as ComponentHealthStatus, AggregateHealth};
pub use metrics_export::MetricsSnapshot;
pub use validation::{WorkflowValidator, ValidationError, StartWorkflowRequest, SignalRequest, QueryRequest};
pub use resource_limits::{ResourceLimits, ResourceTracker, ResourceExceeded, ResourceUsage};
pub use update::{UpdateController, UpdateHandler, UpdateStore, UpdateRequest, UpdateResult, UpdateStatus, UpdateWaitPolicy};
pub use reachability::{ReachabilityTracker, ReachabilityQuery, ReachabilityResult, ReachabilityType};
pub use deployment_api::{DeploymentManager, Deployment, DeploymentStatus, DrainageStatus};
pub use codec_server::{CodecServer, CodecRequest, CodecResponse, PayloadCodec as ServerPayloadCodec, IdentityCodec as ServerIdentityCodec, Base64Codec, JsonPrettyCodec};
pub use worker_sessions::{SessionManager, WorkerSession, SessionStatus, SessionConfig};
pub use worker_service::{WorkerService, SystemWorkflowKind, SystemTask, WorkerPoolConfig, WorkerHealth, WorkerServiceStats};
pub use worker_determinism::{DeterminismChecker, DeterminismResult, DeterminismViolation, ViolationSeverity, RecordedSideEffect, WorkflowOperation, OperationType};
pub use failure_types::{FailureType, TimeoutType, RetryState, WorkflowFailure, FailureInfo, ApplicationFailureInfo, TimeoutFailureInfo, CanceledFailureInfo, ServerFailureInfo, ChildWorkflowExecutionFailureInfo, ResetWorkflowFailureInfo, ActivityTaskNotFoundInfo, WorkflowIdReusePolicy, WorkflowFinalStatus, FailureBuilder, FailureStats};
pub use async_activity::{ActivityTaskToken, AsyncActivityRegistry, PendingAsyncActivity, AsyncActivityState};
pub use advanced_operations::{
    ActivityPauseState, PauseActivityRequest, UnpauseActivityRequest, ResetActivityRequest, ActivityControlResponse, ActivityPauseRegistry,
    WorkflowPauseState, WorkflowPauseRegistry,
    MultiOperationStep, MultiOperationStepResult, MultiOperationResult, MultiOperationExecutor,
    ActivityRuntimeOptions, WorkflowRuntimeOptions, RuntimeOptionsRegistry,
    TimeSkipController,
    FairnessTracker, FairnessStats,
    ManagedWorkerInfo, WorkerHealthStatus, ListWorkersRequest, ListWorkersResponse, WorkerManagementRegistry,
    DlqAdminTask, DlqAdminController, DlqAdminStats,
};
pub use depth_operations::{
    ExtendedEventType, ExtendedHistoryEvent, ExtendedHistoryStore,
    EngineStats, EngineStatistics,
    SizeLimitConfig, SizeCheckResult, SizeLimitEnforcer,
    PollContext, PollContextManager,
    RetentionPolicy, NamespaceRetentionManager,
    WorkflowTaskState, WorkflowTaskTracker,
};
pub use operational_api::{
    ScheduleBackfillRequest, BackfillOverlapPolicy, BackfillResult, ScheduleBackfiller,
    UpdateValidationResult, UpdateValidatorFn, UpdateValidatorRegistry, UpdateValidationLogEntry, UpdateValidationStats,
    DeletionStatus, WorkflowDeletion, WorkflowDeletionPipeline,
    RebuiltMutableState, MutableStateRebuilder, RebuildStats,
    TaskValidationResult, TaskValidator, TaskValidationStats,
    ScheduledWorkflowTask, ScheduledTaskType, WorkflowTaskScheduler,
    BatchResetRequest, BatchResetResult, BatchResetItemResult, BatchResetter,
    OpSearchAttributeType, OpSearchAttributeDefinition, OpSearchAttributeSchema,
    NexusEndpointInfo, NexusEndpointManager,
    DeploymentVersion, DeploymentVersionRamp,
};
// core_internals: mutable state machine, command processing, task generation, transactions.
// Note: TransferTask, ReplicationTask, WorkflowTaskState are NOT re-exported here due to
// name conflicts with history_shard::TransferTask, cluster::ReplicationTask, depth_operations::WorkflowTaskState.
// Access them via velocity_workflow_engine::core_internals::* if needed.
pub use core_internals::{
    ActivityMutableState, ActivityMutableInfo, ActivityRetryPolicyState,
    TimerMutableState, TimerMutableInfo,
    ChildWorkflowMutableState, ChildWorkflowMutableInfo,
    ParentClosePolicyKind, ExternalRequestInfo, ExternalRequestType,
    WorkflowMutableState, MutableStateSummary,
    WorkflowCommand, ScheduleActivityCommand, StartTimerCommand,
    CompleteWorkflowCommand, FailWorkflowCommand, CancelWorkflowCommand,
    StartChildWorkflowCommand, CancelChildWorkflowCommand,
    SignalExternalCommand, CancelExternalCommand,
    ContinueAsNewCommand, CancelTimerCommand, RequestCancelActivityCommand,
    ProtocolMessageCommand, ModifyPropertiesCommand, RecordMarkerCommand,
    CommandProcessor, ProcessedCommandRecord,
    GeneratedTask, TransferTaskType, TimerTaskType, ReplicationTaskType, VisibilityTaskType,
    TimerTask, VisibilityTask,
    TaskGenerator,
    TransactionState, MutableStateSnapshot, TransactionManager, TransactionInfo, TransactionStats,
    WorkflowTaskStateMachine, WorkflowTaskInfo, WorkflowTaskStats,
    TaskRefresher,
    TimerSequence, TimerSequenceEntry,
    MutableStateChecksum,
};
// queue_processing: timer, transfer, visibility, replication, archival queue processors.
pub use queue_processing::{
    QueueProcessorStatus, QueueProcessorConfig, QueueProcessorStats, TaskExecutionResult,
    TransferQueueTask, TransferQueueTaskType, TransferQueueProcessor,
    TimerQueueTask, TimerQueueTaskType, TimerQueueProcessor,
    VisibilityQueueTask, VisibilityQueueTaskType, VisibilityQueueProcessor,
    ReplicationQueueTask, ReplicationQueueTaskType, ReplicationQueueProcessor,
    ArchivalQueueTask, ArchivalQueueTaskType, ArchivalQueueProcessor,
    QueueTaskScheduler, AllQueueStats,
};
// matching_engine: task queue partitioning, matching algorithm, poller management.
pub use matching_engine::{
    PartitionConfig, TaskQueuePartition, PartitionManager as MatchingPartitionManager,
    PhysicalTask, PartitionRedirect, PhysicalTaskQueue, PhysicalQueueStats,
    LogicalTaskQueue, TaskQueueType,
    MatchingEngineCore, MatchingEngineConfig, MatchingEngineStats, MatchResult,
    Poller, PollerRegistry, PollerInfo as MatchingPollerInfo,
    FairTaskReader, TaskReaderStats,
    TaskQueueUserData, VersioningData, RedirectRule, UserDataManager,
    PriorityMatcher,
};
// history_builder: event construction, branch tokens, serialization.
pub use history_builder::{
    HBEventType, HBHistoryEvent,
    HistoryBuilder, HistoryBranch as HBHistoryBranch, BranchAncestor, HistoryBranchManager,
    HistorySerializer, HistoryTree,
};
// system_workflows: parent close, namespace delete, scanner, batcher, archival.
pub use system_workflows::{
    ParentCloseAction, ChildWorkflowRef, ParentClosePolicyExecutor, ExecutedAction,
    NamespaceDeletionStep, NamespaceDeletionStatus, NamespaceDeletionWorkflow,
    ScanTarget, ScanResult, WorkflowScanner,
    SystemBatchOp, BatchOpItem, BatchItemStatus, SystemBatchOperation, BatchOperationProcessor,
    ArchivalWorkflowState, ArchivalStatus, HistoryArchivalWorkflow,
    QueueCleanupTarget, QueueCleanupRecord, QueueCleanupWorkflow,
    ReplicationRepairTask, RepairStatus, ReplicationRepairWorkflow,
};
// workflow_context: execution context tying mutable state, history, shards together.
pub use workflow_context::{
    ContextState, WorkflowExecutionContext,
    ShardContext as WorkflowShardContext, ShardStats as WorkflowShardStats,
    ContextManager, ExecutionStats,
};
// hsm_framework: hierarchical state machine for complex workflow state management.
pub use hsm_framework::{
    HSMState, HSMStateType, HSMTransition,
    HSMStateMachine, EventRecord, TransitionResult,
    HierarchicalStateMachine,
    HSMRegistry,
};
// membership: cluster membership, consistent hash ring, health checking.
pub use membership::{
    ClusterMember, MemberRole, MemberStatus,
    MembershipRing,
    ClusterHealthChecker, HealthCheckResult,
    ShardOwnershipManager,
};
// persistence_layer: deep persistence data models, store interfaces, managers.
pub use persistence_layer::{
    CreateWorkflowMode, UpdateWorkflowMode, ConflictResolveMode, QueueType,
    WorkflowExecutionInfo as PersistedWorkflowInfo, WorkflowExecutionStatus as PersistedExecStatus,
    ExecutionStatsPersisted,
    UpdateInfo as PersistedUpdateInfo, UpdateStatus as PersistedUpdateStatus,
    StateMachineInfo, QueueMetadata,
    WorkflowExecutionState, VersionHistory, VersionHistoryItem, VersionHistories,
    PersistentTask, PersistentTaskType, TaskKey, TaskRange,
    ShardInfo as PersistedShardInfo, FailoverLevel,
    HistoryBranch as PersistedHistoryBranch, HistoryBranchAncestor, HistoryTreeInfo,
    DataBlob, EncodingType, SerializedEventBatch, EventBatchRow,
    PageToken as PersistedPageToken, HistoryPagingToken,
    ExecutionStore, HistoryStore as PersistedHistoryStore, TaskStore, ShardStore, VisibilityStore, NamespaceStore, QueueStore,
    CreateWorkflowRequest, CreateWorkflowResponse, GetWorkflowRequest, GetWorkflowResponse,
    UpdateWorkflowRequest, UpdateWorkflowResponse, DeleteWorkflowRequest,
    GetCurrentRequest, GetCurrentResponse, ListWorkflowsRequest, ListWorkflowsResponse,
    AppendHistoryRequest, AppendHistoryResponse, ReadHistoryRequest, ReadHistoryResponse,
    DeleteHistoryRequest, ListOpenRequest, ListClosedRequest, ListVisibilityResponse,
    NamespaceDetail, NamespaceState, ArchivalState as PersistedArchivalState,
    NamespaceReplicationConfig as PersistedNsReplicationConfig,
    QueueMessage, PersistenceError,
    OperationModeValidator,
    XDCCache, XDCCacheEntry, XDCCacheStats,
    ExecutionManager as PersistedExecutionManager, ExecutionManagerStats,
    HistoryManager as PersistedHistoryManager, HistoryManagerStats,
    InMemoryExecutionStore, InMemoryHistoryStore, InMemoryShardStore,
    InMemoryVisibilityStore, InMemoryNamespaceStore, InMemoryQueueStore,
    PersistenceFactory, PersistenceStack,
};
// ndc_replication_deep: deep NDC replication subsystem.
pub use ndc_replication_deep::{
    ReplicationTaskKind, ReplicationTask as NdcReplicationTask, ReplicationTaskStatus,
    VersionedTransition,
    WorkflowStateReplicator, ReplicatorStats, ApplyResult,
    ActivityStateReplicator, ActivityReplicatorStats, SyncActivityInfo, ReplicatedActivityState,
    HsmStateReplicator, HsmReplicatorStats, SyncHsmState,
    ConflictResolver as NdcConflictResolver, ReplicationConflict as NdcConflict, ConflictType, ConflictResolution as NdcConflictResolution,
    ReplicatedWorkflowState, ReplicatedEvent,
    StateRebuilder,
    TransactionManager as NdcTransactionManager, TransactionManagerStats, TransactionResult, PendingReplicationTask,
    NewWorkflowTransaction, ExistingWorkflowTransaction,
    HistoryReplicator, HistoryReplicatorStats, HistoryReplicationBatch, NewRunInfo,
    HistoryImporter, ImporterStats, ImportHistoryRequest,
    BranchManager, ReplicationBranch, BranchAncestorInfo,
    EventsReapplier,
    ReplicationWorkflowResetter, ResetterStats, ReplicationResetSpec,
    MutableStateInitializer, MutableStateMapper, MappedState,
    BufferEventFlusher,
    ReplicationError,
};
// matching_deep: deep matching subsystem.
pub use matching_deep::{
    TaskQueueGroup, DeepTaskQueueType, TaskQueueVersion, BuildIdRedirectRule, BuildIdAssignmentRule, Ramp,
    SyncMatchProtocol, PendingMatch, DeepPhysicalTask, SyncMatchStats, SyncMatchResult,
    TaskQueueCounter, CounterPartition,
    MatchingWorker, MatchingWorkerManager, MatchingWorkerManagerStats,
    TaskForwarder, ForwardTaskRequest, ForwardResult,
    StickyMatcher, StickyAssignment, StickyMatchStats,
    RateLimitedDispatcher, DispatchRate, DispatchStats,
};
// worker_services: deep worker service subsystem.
pub use worker_services::{
    WorkerDeploymentManager, DeploymentManagerStats, WorkerDeployment, DeploymentVersion as WorkerDeploymentVersion,
    DeploymentState, VersionState, DrainageInfo, DrainageStatus as WorkerDrainageStatus,
    SchedulerService, SchedulerStats, SchedulerSchedule, SchedulerSpec, CalendarSpec as SchedulerCalendarSpec,
    SchedulerPolicy, SchedulerOverlapPolicy, SchedulerState, SchedulerInfo, SchedulerActionResult,
    ScannerService, ScannerStats, ScanExecution, ScanType, ScanStatus,
    MigrationService, MigrationStats, MigrationExecution, MigrationExecStatus,
    DlqManagementService, DlqStats as WorkerDlqStats, DlqQueue, DlqMessage,
    BatcherService, BatcherStats, BatcherJob, BatcherOperation, BatcherJobStatus,
    DeploymentError, SchedulerError, ScannerError, MigrationError as WorkerMigrationError, DlqError, BatcherError,
};
// frontend_service: deep frontend service.
pub use frontend_service::{
    FrontendService, FrontendConfig, FrontendStats,
    ApiRequest, ApiResponse, ApiStatus,
    RequestInterceptor, InterceptorError,
    AuthInterceptor, RateLimitInterceptor, ValidationInterceptor, TelemetryInterceptor,
    ApiHandler, HandlerError,
};
// namespace_mgmt: deep namespace management.
pub use namespace_mgmt::{
    NamespaceRegistry as DeepNamespaceRegistry, RegistryStats, NamespaceEntry, NamespaceLifecycleState,
    ArchivalState as NsArchivalState, ClusterReplicationConfig,
    NamespaceWatcher, NamespaceChangeEvent, NamespaceChangeType,
    NamespaceReplicationQueue, ReplicationQueueStats, ReplicationQueueMessage,
    FailoverManager, FailoverStats, FailoverRecord, FailoverState,
    ClusterMetadata,
    RegistryError,
};
// common_utils: deep common utilities.
pub use common_utils::{
    QuotaManager, QuotaStats, QuotaPolicy,
    SearchAttributeManager, SearchAttributeStats, SearchAttributeDefinition,
    SearchAttributeFieldType, SearchAttributeValue as UtilSearchAttributeValue, SearchAttributeError,
    MetricsFramework, MetricsFrameworkStats, MetricsScope, MetricDefinition, MetricType,
    TaskFramework, TaskFrameworkStats, TaskExecutor, FrameworkTask, TaskResult, TaskError,
    VersioningManager, VersioningStats, VersionSet as DeepVersionSet, VersionRedirectRule,
};
// persistence_sql: SQL query builder, schema management, connection pooling, transaction handling.
pub use persistence_sql::{
    SqlQueryBuilder, SqlDialect, SqlValue, ComparisonOp,
    SelectBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder,
    Condition, OrderByClause, JoinClause, JoinType, Assignment,
    SchemaManager, SchemaMigration, SchemaError as SqlSchemaError,
    ConnectionPool, PoolConfig, PoolConnection, PoolStats, PoolError,
    SqlTransactionManager, SqlTransaction, IsolationLevel, TransactionStats as SqlTransactionStats, TransactionError,
};
// persistence_visibility: deep visibility store with query parsing, indexing, aggregation.
pub use persistence_visibility::{
    VisibilityRecord, WorkflowExecutionStatus as VisExecStatus, SearchAttribute as VisSearchAttribute,
    QueryParser, VisibilityQuery as DeepVisibilityQuery, QueryValue as DeepQueryValue, QueryParseError,
    QueryEvaluator,
    VisibilityIndex as DeepVisibilityIndex,
    DeepVisibilityStore, VisibilityStats, VisibilityError,
};
// history_api: full history API handler implementations.
pub use history_api::{
    HistoryApiContext,
    StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
    RecordActivityTaskHeartbeatRequest, RecordActivityTaskHeartbeatResponse,
    PollActivityTaskQueueRequest, PollActivityTaskQueueResponse,
    RespondActivityTaskCompletedRequest, RespondActivityTaskFailedRequest, RespondActivityTaskCanceledRequest,
    SignalWorkflowExecutionRequest, QueryWorkflowRequest, QueryWorkflowResponse,
    RequestCancelWorkflowExecutionRequest, TerminateWorkflowExecutionRequest,
    GetWorkflowExecutionHistoryRequest, GetWorkflowExecutionHistoryResponse,
    HistoryApiHandler, HistoryApiServiceImpl, HistoryApiError, HistoryApiStats,
    WorkflowExecution as HistWorkflowExecution, WorkflowExecutionStatus as HistExecStatus,
    HistoryEvent as ApiHistoryEvent, EventType as ApiEventType, History as ApiHistory, Failure as ApiFailure, FailureType as ApiFailureType, TimeoutType as ApiTimeoutType,
    RetryPolicy as HistRetryPolicy, TaskQueueMetadata,
};
// matching_workers: deep matching worker implementations.
pub use matching_workers::{
    TaskQueuePartition as WorkerTaskQueuePartition, TaskType as WorkerTaskType, InternalTask, RedirectInfo,
    PhysicalTaskQueue as WorkerPhysicalTaskQueue, TaskQueueConfig, PollerInfo as WorkerPollerInfo,
    RateLimiterState,
    LogicalTaskQueue as WorkerLogicalTaskQueue, TaskQueueVersioning, VersionData, VersionRedirectRule as WorkerVersionRedirectRule, VersionAssignmentRule,
    DispatchResult,
    TaskQueueManager, TaskQueueManagerStats,
    TaskForwarder as WorkerTaskForwarder, ForwardStats,
    MatchingLoadBalancer, PartitionLoad,
};
// frontend_handlers: expanded frontend API handlers.
pub use frontend_handlers::{
    RegisterNamespaceRequest, RegisterNamespaceResponse,
    DescribeNamespaceRequest, DescribeNamespaceResponse,
    UpdateNamespaceRequest, UpdateNamespaceResponse,
    DeprecateNamespaceRequest,
    ListNamespacesRequest, ListNamespacesResponse, NamespaceFilter,
    NamespaceInfo, NamespaceConfig as FrontendNamespaceConfig, NamespaceReplicationConfig as FrontendNsReplicationConfig,
    NamespaceState as FrontendNamespaceState, ArchivalState as FrontendArchivalState,
    BadBinaries, BadBinaryInfo,
    ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse,
    CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse,
    WorkflowExecutionInfo as FrontendWorkflowInfo,
    GetSearchAttributesRequest, GetSearchAttributesResponse, SearchAttributeType as FrontendSearchAttributeType,
    DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse,
    PendingActivityInfo as FrontendPendingActivity, PendingActivityState as FrontendPendingActivityState,
    PendingWorkflowTaskInfo, WorkflowTaskType,
    ResetWorkflowExecutionRequest, ResetWorkflowExecutionResponse,
    FrontendServiceImpl, FrontendError, FrontendStats as HandlerFrontendStats,
};
