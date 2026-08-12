//! velocity-workflow-engine
//!
//! Hardware-native workflow execution engine. The entire runtime — state machine scheduling,
//! task queue, timer engine, WAL persistence, signal/query routing — lives in Rust with
//! zero managed heap allocations. C# acts as a thin FFI bridge only.
//!
//! Architecture:
//!   [C# Developer Code] ──FFI──► [velocity-workflow-engine] ──► [velocity-workflow-core]
//!   (thin bridge)                (runtime engine, zero-GC)      (slab, bitmask, Merkle)

pub mod advanced_operations;
pub mod advanced_scheduler;
pub mod ai_context;
pub mod archival;
pub mod archival_engine;
pub mod async_activity;
pub mod auth;
pub mod auth_v2;
pub mod backoff_retry;
pub mod batch;
pub mod chaos_endurance;
pub mod chaos_engineering;
pub mod client_sdk;
pub mod clock_abstraction;
pub mod cluster;
pub mod cluster_membership;
pub mod codec_server;
pub mod cold_storage;
pub mod common_utils;
pub mod core_internals;
pub mod cron;
pub mod db_adapter;
pub mod deep_observability;
pub mod deletion_manager;
pub mod deployment_api;
pub mod depth_operations;
pub mod distributed_locks;
pub mod durable_rpc;
pub mod dynamic_config;
pub mod engine;
pub mod errors;
pub mod event_history;
pub mod failure_types;
pub mod ffi;
pub mod frontend_handlers;
pub mod frontend_service;
pub mod graceful_shutdown;
pub mod hardware_integration;
pub mod hardware_traits;
pub mod header_propagation;
pub mod health_check;
pub mod heartbeat;
pub mod history_api;
pub mod history_builder;
pub mod history_compaction;
pub mod history_engine;
pub mod history_event_applier;
pub mod history_shard;
pub mod hot_swap;
pub mod hsm_framework;
pub mod lru_cache;
pub mod matching_deep;
pub mod matching_engine;
pub mod matching_service;
pub mod matching_workers;
pub mod membership;
pub mod memo;
pub mod metrics;
pub mod metrics_export;
pub mod migration_runner;
pub mod multi_backend_persistence;
pub mod multi_region;
pub mod namespace;
pub mod namespace_manager;
pub mod namespace_mgmt;
pub mod ndc_replication;
pub mod ndc_replication_deep;
pub mod network_replication;
pub mod nexus;
pub mod nexus_deep;
pub mod notification_system;
pub mod observability;
pub mod operational_api;
pub mod partition;
pub mod patch;
pub mod payload_codec;
pub mod persistence_layer;
pub mod persistence_serialization;
pub mod persistence_sql;
pub mod persistence_visibility;
pub mod predictive_autoscaler;
pub mod query_handler;
pub mod queue_infrastructure;
pub mod queue_processing;
pub mod quota_management;
pub mod raft_consensus;
pub mod rate_limiter;
pub mod reachability;
pub mod replay;
pub mod replication_daemon;
pub mod replication_executor;
pub mod replication_manager;
pub mod replication_transport;
pub mod resource_limits;
pub mod retry;
pub mod rpc_framework;
pub mod saga;
pub mod schedules;
pub mod search_attributes;
pub mod search_index;
pub mod self_healing;
pub mod service_errors;
pub mod shard_controller;
pub mod sharding;
pub mod system_workflows;
pub mod task_framework;
pub mod task_queue;
pub mod timer_engine;
pub mod timer_queue_executor;
pub mod transfer_queue_executor;
pub mod update;
pub mod validation;
pub mod visibility;
pub mod visibility_query;
pub mod wal;
pub mod worker_deployment;
pub mod worker_determinism;
pub mod worker_registry;
pub mod worker_service;
pub mod worker_services;
pub mod worker_sessions;
pub mod worker_versioning;
pub mod workflow_commands;
pub mod workflow_context;
pub mod workflow_execution;
pub mod workflow_replay;
pub mod workflow_reset;
pub mod workflow_state_machine;
pub mod workflow_task_handler;

// gRPC server module — only compiled when the `grpc` feature is enabled.
// Requires protoc to be installed for proto compilation.
#[cfg(feature = "grpc")]
pub mod grpc_server;

pub use advanced_operations::{
    ActivityControlResponse, ActivityPauseRegistry, ActivityPauseState, ActivityRuntimeOptions,
    DlqAdminController, DlqAdminStats, DlqAdminTask, FairnessStats, FairnessTracker,
    ListWorkersRequest, ListWorkersResponse, ManagedWorkerInfo, MultiOperationExecutor,
    MultiOperationResult, MultiOperationStep, MultiOperationStepResult, PauseActivityRequest,
    ResetActivityRequest, RuntimeOptionsRegistry, TimeSkipController, UnpauseActivityRequest,
    WorkerHealthStatus, WorkerManagementRegistry, WorkflowPauseRegistry, WorkflowPauseState,
    WorkflowRuntimeOptions,
};
pub use advanced_scheduler::{
    CronError as CronErrorV2, CronExpression as CronExpressionV2, RateLimiterV2, ScheduleInfo,
    ScheduleManager as AdvancedScheduleManager, StickyScheduler, WorkerVersioningV2,
    WorkflowSchedule,
};
pub use ai_context::{
    AgentToolCall, AiContextConfig, AiContextStats, AiContextWindow, ContextMessage, MessageRole,
    ToolCallStatus,
};
pub use archival::{ArchivePolicy, ArchiveRecord, ArchiveStore};
pub use async_activity::{
    ActivityTaskToken, AsyncActivityRegistry, AsyncActivityState, PendingAsyncActivity,
};
pub use auth::{AuthManager, Claims, Permission, Role};
pub use auth_v2::{
    ApiKey, ApiKeyManager, ApiPermission, AuditFilter, AuditLog, AuditLogger, AuditResult,
    AuthError, Claims as V2Claims, EncryptionAlgorithm, EncryptionAtRest, EncryptionConfig,
    OAuth2Config, OAuth2Validator,
};
pub use batch::{BatchExecutor, BatchOperationType, BatchResult, BatchStatus};
pub use chaos_endurance::{
    run_crash_recovery_test, run_soak_test, SoakTestConfig, SoakTestMetrics,
};
pub use cluster::{ClusterInfo, ClusterManager, ReplicationTask};
pub use codec_server::{
    Base64Codec, CodecRequest, CodecResponse, CodecServer, IdentityCodec as ServerIdentityCodec,
    JsonPrettyCodec, PayloadCodec as ServerPayloadCodec,
};
pub use cold_storage::{ColdStorageRecord, FileColdStorage};
pub use cron::{CronError, CronExpression, CronFireEvent, CronScheduler};
pub use db_adapter::{
    CassandraAdapter, CassandraConsistency, DatabaseAdapter, DatabaseConfig, DatabaseError,
    DatabaseResult, InMemoryAdapter, MysqlAdapter, PostgresAdapter,
    SearchAttributeValue as DbSearchAttributeValue, SearchAttributes, SqliteAdapter,
    SqliteJournalMode, SslMode, StatusFilter, WorkflowEventRecord, WorkflowRecord,
};
pub use deployment_api::{Deployment, DeploymentManager, DeploymentStatus, DrainageStatus};
pub use depth_operations::{
    EngineStatistics, EngineStats, ExtendedEventType, ExtendedHistoryEvent, ExtendedHistoryStore,
    NamespaceRetentionManager, PollContext, PollContextManager, RetentionPolicy, SizeCheckResult,
    SizeLimitConfig, SizeLimitEnforcer, WorkflowTaskState, WorkflowTaskTracker,
};
pub use durable_rpc::{
    DurableRpcCall, DurableRpcConfig, DurableRpcState, DurableRpcStats, DurableServiceMesh,
};
pub use dynamic_config::{
    ConfigClient, ConfigCollection, ConfigKey, ConfigRegistry, ConfigValue, ConstrainedValue,
    Constraints, DynamicConfig, GradualChange, MemoryConfigClient, Precedence, StaticConfigClient,
};
pub use engine::{
    PendingActivityInfo, PendingActivityState, PendingChildInfo, PendingSignalInfo,
    WorkflowContext, WorkflowEngine, WorkflowExecutionDescription, WorkflowStatus,
};
pub use errors::{ErrorCategory, ErrorCode, FfiErrorCode, VelocityError, VelocityResult};
pub use event_history::{HistoryEvent, HistoryEventType, HistoryStore};
pub use failure_types::{
    ActivityTaskNotFoundInfo, ApplicationFailureInfo, CanceledFailureInfo,
    ChildWorkflowExecutionFailureInfo, FailureBuilder, FailureInfo, FailureStats, FailureType,
    ResetWorkflowFailureInfo, RetryState, ServerFailureInfo, TimeoutFailureInfo, TimeoutType,
    WorkflowFailure, WorkflowFinalStatus, WorkflowIdReusePolicy,
};
pub use graceful_shutdown::{GracefulShutdownConfig, ShutdownController, ShutdownStatus};
pub use hardware_integration::{
    compute_simple_merkle_root, EccParityStore, EccStats, HardwareAbstractionLayer, MerkleEccResult,
};
pub use hardware_traits::{
    HardwareError, PeerToPeerReplication, SelfHealingEcc, SmartNicOffload, TeeEnclave,
};
pub use health_check::{AggregateHealth, HealthChecker, HealthStatus as ComponentHealthStatus};
pub use heartbeat::HeartbeatTracker;
pub use history_compaction::{
    CompactableEventType, CompactionConfig, CompactionLevel, CompactionStats, HistoryCompactor,
};
pub use history_shard::{
    HistoryShardManager, MutableState, ShardContext, ShardOwnership, ShardState, ShardStats,
    TransferTask, TransferTaskKind,
};
pub use hot_swap::{HotSwapPatch, HotSwapRegistry, HotSwapResult, HotSwapStats};
pub use matching_service::{
    MatchTask, MatchingService, MatchingServiceConfig, MatchingServiceStats, PollerDescription,
    PollerInfo, TaskKindFilter, TaskQueueDescription, TaskQueuePartitionInfo,
};
pub use memo::{MemoEntry, MemoSetResult, MemoStats, MemoStore};
pub use metrics::MetricsRegistry;
pub use metrics_export::MetricsSnapshot;
pub use migration_runner::{
    Migration, MigrationAdapter, MigrationError, MigrationResult, MigrationRunner, MigrationStatus,
};
pub use multi_region::{
    ConflictResolutionStrategy, FailoverController, FailoverEvent, FailoverResult, HealthStatus,
    MultiRegionReplicator, RegionConfig, RegionInfo, RegionState, ReplicationConflict,
    ReplicationResult, ResolvedValue, SyncResult,
};
pub use namespace::{NamespaceConfig, NamespaceError, NamespaceRegistry};
pub use ndc_replication::{
    ConflictResolution, ConflictResolver, ConsistencyCheckResult, ConsistencyChecker, DlqStats,
    DlqTask, HistoryGap, HistoryGapDetector, NamespaceReplicationConfig,
    NamespaceReplicationController, ReplicationConflict as NdcReplicationConflict, ReplicationDlq,
    TaskAckRecord, TaskAckState, TaskAckTracker, TaskAckTrackerStats,
};
pub use network_replication::{
    decode_tasks, encode_tasks, FrameType, TcpReplicationConfig, TcpReplicationServer,
    TcpReplicationStats, UdpReplicationConfig, UdpReplicationStats, UdpReplicationTransport,
    WireFrame,
};
pub use nexus::{NexusManager, NexusOperation, NexusOperationState};
pub use observability::{
    global, init_global, LogLevel, MetricsExporter, ObservabilityConfig, ObservabilityContext,
    SpanId, SpanStatus, SpanTracker, StructuredLogger,
};
pub use operational_api::{
    BackfillOverlapPolicy, BackfillResult, BatchResetItemResult, BatchResetRequest,
    BatchResetResult, BatchResetter, DeletionStatus, DeploymentVersion, DeploymentVersionRamp,
    MutableStateRebuilder, NexusEndpointInfo, NexusEndpointManager, OpSearchAttributeDefinition,
    OpSearchAttributeSchema, OpSearchAttributeType, RebuildStats, RebuiltMutableState,
    ScheduleBackfillRequest, ScheduleBackfiller, ScheduledTaskType, ScheduledWorkflowTask,
    TaskValidationResult, TaskValidationStats, TaskValidator, UpdateValidationLogEntry,
    UpdateValidationResult, UpdateValidationStats, UpdateValidatorFn, UpdateValidatorRegistry,
    WorkflowDeletion, WorkflowDeletionPipeline, WorkflowTaskScheduler,
};
pub use partition::{PartitionInfo, PartitionManager};
pub use patch::{PatchRegistry, WorkflowPatch};
pub use payload_codec::{
    CodecChain, CodecChainStats, CodecError, CodecRegistry, CompressionCodec, EncryptionCodec,
    IdentityCodec, Payload, PayloadCodec, PayloadMetadata, PayloadValidator, SizeLimitCodec,
    XorCodec,
};
pub use query_handler::{
    BufferedQuery, QueryConsistency, QueryDefinition, QueryHandler, QueryRecord, QueryRegistry,
    QueryState, QueryStats, RejectionPolicy,
};
pub use raft_consensus::{RaftCluster, RaftConfig, RaftLogEntry, RaftNode, RaftState, RaftStats};
pub use rate_limiter::{
    ClockedRateLimiter, DelayedRateLimiter, MultiRateLimiter, MultiReservation,
    NamespaceRateLimiter, PriorityRateLimiter, QuotaTracker, QuotaUsage, RateLimiter, RateRequest,
    RequestPriority, Reservation, RoutingRateLimiter, TokenBucket,
};
pub use reachability::{
    ReachabilityQuery, ReachabilityResult, ReachabilityTracker, ReachabilityType,
};
pub use replay::{ReplayActivityState, ReplayActivityStatus, ReplayEngine, ReplayResult};
pub use replication_daemon::{
    DeliveredTask, ReplicationDaemon, ReplicationDaemonConfig, ReplicationDaemonStats,
};
pub use replication_transport::{ReplicationLinkStatus, ReplicationTransport};
pub use resource_limits::{ResourceExceeded, ResourceLimits, ResourceTracker, ResourceUsage};
pub use retry::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerMetrics, CircuitState, RetryExecutor,
    RetryPolicy, RetryStats,
};
pub use saga::{SagaOrchestrator, SagaStatus, SagaStepDefinition};
pub use schedules::{
    CalendarSpec, OverlapPolicy, ScheduleAction, ScheduleEntry, ScheduleManager, ScheduleState,
};
pub use search_index::{
    BulkIndexer, BulkIndexerStats, BulkOperation, IndexKey, IndexLifecycleManager, IndexMetadata,
    IndexState, IndexedValue, QueryNode, QueryValue, SchemaError, SearchAttributeField,
    SearchAttributeIndex, SearchAttributeSchema, SearchAttributeType, SearchIndexStats,
    VisibilityQueryParser,
};
pub use sharding::ShardManager;
pub use task_queue::{QueueStats, TaskItem, TaskKind, TaskQueue};
pub use timer_engine::TimerEngine;
pub use update::{
    UpdateController, UpdateHandler, UpdateRequest, UpdateResult, UpdateStatus, UpdateStore,
    UpdateWaitPolicy,
};
pub use validation::{
    QueryRequest, SignalRequest, StartWorkflowRequest, ValidationError, WorkflowValidator,
};
pub use visibility::{
    PageToken, PaginatedResult, SearchAttributeValue, SortField, SortOrder, VisibilityAggregation,
    VisibilityFilter, VisibilityIndex, VisibilityQuery as AdvancedVisibilityQuery,
    WorkflowExecutionInfo,
};
pub use visibility_query::{QueryCondition, QueryField, QueryOp, VisibilityQuery};
pub use wal::{WalEventType, WalManager, WalRecord, WalWriter};
pub use worker_determinism::{
    DeterminismChecker, DeterminismResult, DeterminismViolation, OperationType, RecordedSideEffect,
    ViolationSeverity, WorkflowOperation,
};
pub use worker_registry::{WorkerInfo, WorkerRegistry, WorkerStatus};
pub use worker_service::{
    SystemTask, SystemWorkflowKind, WorkerHealth, WorkerPoolConfig, WorkerService,
    WorkerServiceStats,
};
pub use worker_sessions::{SessionConfig, SessionManager, SessionStatus, WorkerSession};
pub use worker_versioning::{BuildId, DeploymentInfo, RoutingRule, VersionSet, WorkerVersioning};
pub use workflow_reset::{
    HistoryBranch, LastFailureResetPolicy, PendingSignal, ResetPoint, ResetReason, ResetResult,
    ResetSpec, WorkflowResetter,
};
// core_internals: mutable state machine, command processing, task generation, transactions.
// Note: TransferTask, ReplicationTask, WorkflowTaskState are NOT re-exported here due to
// name conflicts with history_shard::TransferTask, cluster::ReplicationTask, depth_operations::WorkflowTaskState.
// Access them via velocity_workflow_engine::core_internals::* if needed.
pub use core_internals::{
    ActivityMutableInfo, ActivityMutableState, ActivityRetryPolicyState,
    CancelChildWorkflowCommand, CancelExternalCommand, CancelTimerCommand, CancelWorkflowCommand,
    ChildWorkflowMutableInfo, ChildWorkflowMutableState, CommandProcessor, CompleteWorkflowCommand,
    ContinueAsNewCommand, ExternalRequestInfo, ExternalRequestType, FailWorkflowCommand,
    GeneratedTask, ModifyPropertiesCommand, MutableStateChecksum, MutableStateSnapshot,
    MutableStateSummary, ParentClosePolicyKind, ProcessedCommandRecord, ProtocolMessageCommand,
    RecordMarkerCommand, ReplicationTaskType, RequestCancelActivityCommand,
    ScheduleActivityCommand, SignalExternalCommand, StartChildWorkflowCommand, StartTimerCommand,
    TaskGenerator, TaskRefresher, TimerMutableInfo, TimerMutableState, TimerSequence,
    TimerSequenceEntry, TimerTask, TimerTaskType, TransactionInfo, TransactionManager,
    TransactionState, TransactionStats, TransferTaskType, VisibilityTask, VisibilityTaskType,
    WorkflowCommand, WorkflowMutableState, WorkflowTaskInfo, WorkflowTaskStateMachine,
    WorkflowTaskStats,
};
// queue_processing: timer, transfer, visibility, replication, archival queue processors.
pub use queue_processing::{
    AllQueueStats, ArchivalQueueProcessor, ArchivalQueueTask, ArchivalQueueTaskType,
    QueueProcessorConfig, QueueProcessorStats, QueueProcessorStatus, QueueTaskScheduler,
    ReplicationQueueProcessor, ReplicationQueueTask, ReplicationQueueTaskType, TaskExecutionResult,
    TimerQueueProcessor, TimerQueueTask, TimerQueueTaskType, TransferQueueProcessor,
    TransferQueueTask, TransferQueueTaskType, VisibilityQueueProcessor, VisibilityQueueTask,
    VisibilityQueueTaskType,
};
// matching_engine: task queue partitioning, matching algorithm, poller management.
pub use matching_engine::{
    ForwardingInfo, MatchTask as MeMatchTask, MatchingEngine, MatchingEngineConfig as MeConfig,
    MatchingHealthReport, PartitionManager as MePartitionManager,
    PollerInfo as MatchingPollerInfo2, TaskQueue as MeTaskQueue, TaskQueueId,
    TaskQueueKind as MeTaskQueueKind, TaskQueueType as MeTaskQueueType2, VersionBranch,
    VersionedData,
};
// history_builder: event construction, branch tokens, serialization.
pub use history_builder::{
    BranchAncestor, HBEventType, HBHistoryEvent, HistoryBranch as HBHistoryBranch,
    HistoryBranchManager, HistoryBuilder, HistorySerializer, HistoryTree,
};
// system_workflows: parent close, namespace delete, scanner, batcher, archival.
pub use system_workflows::{
    ArchivalStatus, ArchivalWorkflowState, BatchItemStatus, BatchOpItem, BatchOperationProcessor,
    ChildWorkflowRef, ExecutedAction, HistoryArchivalWorkflow, NamespaceDeletionStatus,
    NamespaceDeletionStep, NamespaceDeletionWorkflow, ParentCloseAction, ParentClosePolicyExecutor,
    QueueCleanupRecord, QueueCleanupTarget, QueueCleanupWorkflow, RepairStatus,
    ReplicationRepairTask, ReplicationRepairWorkflow, ScanResult, ScanTarget, SystemBatchOp,
    SystemBatchOperation, WorkflowScanner,
};
// workflow_context: execution context tying mutable state, history, shards together.
pub use workflow_context::{
    ContextManager, ContextState, ExecutionStats, ShardContext as WorkflowShardContext,
    ShardStats as WorkflowShardStats, WorkflowExecutionContext,
};
// hsm_framework: hierarchical state machine for complex workflow state management.
pub use hsm_framework::{
    EventRecord, HSMRegistry, HSMState, HSMStateMachine, HSMStateType, HSMTransition,
    HierarchicalStateMachine, TransitionResult,
};
// membership: cluster membership, consistent hash ring, health checking.
pub use membership::{
    ClusterHealthChecker, ClusterMember, HealthCheckResult, MemberRole, MemberStatus,
    MembershipRing, ShardOwnershipManager,
};
// persistence_layer: deep persistence data models, store interfaces, managers.
pub use persistence_layer::{
    ArchivalState as PersistedArchivalState,
    ClusterReplicationConfig as PersistClusterReplicationConfig, DataStoreHealth, DataStoreManager,
    ExecutionStatus as PersistExecutionStatus, HistoryEventData, InMemoryExecutionStore,
    InMemoryHistoryStore, InMemoryMetadataStore, InMemoryQueueStore, InMemoryVisibilityStore,
    NamespaceConfig as PersistNsConfig, NamespaceData as PersistNamespaceData,
    NamespaceState as PersistNsState, PageToken as PersistedPageToken,
    PaginatedResult as PersistPaginatedResult, PersistenceError, QueueData,
    QueueType as PersistQueueType, ReplicationConfig as PersistReplicationConfig, TaskQueueData,
    TaskQueueKind as PlTaskQueueKind, TaskQueueType as PlTaskQueueType,
    Transaction as PersistTransaction, TransactionManager as PersistTransactionManager,
    TransactionOp, TransactionState as PersistTransactionState, WorkflowExecutionData,
};
// ndc_replication_deep: deep NDC replication subsystem.
pub use ndc_replication_deep::{
    ActivityReplicatorStats, ActivityStateReplicator, ApplyResult, BranchAncestorInfo,
    BranchManager, BufferEventFlusher, ConflictResolution as NdcConflictResolution,
    ConflictResolver as NdcConflictResolver, ConflictType, EventsReapplier,
    ExistingWorkflowTransaction, HistoryImporter, HistoryReplicationBatch, HistoryReplicator,
    HistoryReplicatorStats, HsmReplicatorStats, HsmStateReplicator, ImportHistoryRequest,
    ImporterStats, MappedState, MutableStateInitializer, MutableStateMapper, NewRunInfo,
    NewWorkflowTransaction, PendingReplicationTask, ReplicatedActivityState, ReplicatedEvent,
    ReplicatedWorkflowState, ReplicationBranch, ReplicationConflict as NdcConflict,
    ReplicationError, ReplicationResetSpec, ReplicationTask as NdcReplicationTask,
    ReplicationTaskKind, ReplicationTaskStatus, ReplicationWorkflowResetter, ReplicatorStats,
    ResetterStats, StateRebuilder, SyncActivityInfo, SyncHsmState,
    TransactionManager as NdcTransactionManager, TransactionManagerStats, TransactionResult,
    VersionedTransition, WorkflowStateReplicator,
};
// matching_deep: deep matching subsystem.
pub use matching_deep::{
    BuildIdAssignmentRule, BuildIdRedirectRule, CounterPartition, DeepPhysicalTask,
    DeepTaskQueueType, DispatchRate, DispatchStats, ForwardResult, ForwardTaskRequest,
    MatchingWorker, MatchingWorkerManager, MatchingWorkerManagerStats, PendingMatch, Ramp,
    RateLimitedDispatcher, StickyAssignment, StickyMatchStats, StickyMatcher, SyncMatchProtocol,
    SyncMatchResult, SyncMatchStats, TaskForwarder, TaskQueueCounter, TaskQueueGroup,
    TaskQueueVersion,
};
// worker_services: deep worker service subsystem.
pub use worker_services::{
    BatcherError, BatcherJob, BatcherJobStatus, BatcherOperation, BatcherService, BatcherStats,
    CalendarSpec as SchedulerCalendarSpec, DeploymentError, DeploymentManagerStats,
    DeploymentState, DeploymentVersion as WorkerDeploymentVersion, DlqError, DlqManagementService,
    DlqMessage, DlqQueue, DlqStats as WorkerDlqStats, DrainageInfo,
    DrainageStatus as WorkerDrainageStatus, MigrationError as WorkerMigrationError,
    MigrationExecStatus, MigrationExecution, MigrationService, MigrationStats, ScanExecution,
    ScanStatus, ScanType, ScannerError, ScannerService, ScannerStats, SchedulerActionResult,
    SchedulerError, SchedulerInfo, SchedulerOverlapPolicy, SchedulerPolicy, SchedulerSchedule,
    SchedulerService, SchedulerSpec, SchedulerState, SchedulerStats, VersionState,
    WorkerDeployment, WorkerDeploymentManager,
};
// frontend_service: deep frontend service.
pub use frontend_service::{
    ApiHandler, ApiRequest, ApiResponse, ApiStatus, AuthInterceptor, FrontendConfig,
    FrontendService, FrontendStats, HandlerError, InterceptorError, RateLimitInterceptor,
    RequestInterceptor, TelemetryInterceptor, ValidationInterceptor,
};
// namespace_mgmt: deep namespace management.
pub use namespace_mgmt::{
    ArchivalState as NsArchivalState, ClusterMetadata, ClusterReplicationConfig, FailoverManager,
    FailoverRecord, FailoverState, FailoverStats, NamespaceChangeEvent, NamespaceChangeType,
    NamespaceEntry, NamespaceLifecycleState, NamespaceRegistry as DeepNamespaceRegistry,
    NamespaceReplicationQueue, NamespaceWatcher, RegistryError, RegistryStats,
    ReplicationQueueMessage, ReplicationQueueStats,
};
// common_utils: deep common utilities.
pub use common_utils::{
    FrameworkTask, MetricDefinition, MetricType, MetricsFramework, MetricsFrameworkStats,
    MetricsScope, QuotaManager, QuotaPolicy, QuotaStats, SearchAttributeDefinition,
    SearchAttributeError, SearchAttributeFieldType, SearchAttributeManager, SearchAttributeStats,
    SearchAttributeValue as UtilSearchAttributeValue, TaskError, TaskExecutor, TaskFramework,
    TaskFrameworkStats, TaskResult, VersionRedirectRule, VersionSet as DeepVersionSet,
    VersioningManager, VersioningStats,
};
// persistence_sql: SQL query builder, schema management, connection pooling, transaction handling.
pub use persistence_sql::{
    Assignment, ComparisonOp, Condition, ConnectionPool, DeleteBuilder, InsertBuilder,
    IsolationLevel, JoinClause, JoinType, OrderByClause, PoolConfig, PoolConnection, PoolError,
    PoolStats, SchemaError as SqlSchemaError, SchemaManager, SchemaMigration, SelectBuilder,
    SqlDialect, SqlQueryBuilder, SqlTransaction, SqlTransactionManager, SqlValue, TransactionError,
    TransactionStats as SqlTransactionStats, UpdateBuilder,
};
// persistence_visibility: deep visibility store with query parsing, indexing, aggregation.
pub use persistence_visibility::{
    DeepVisibilityStore, QueryEvaluator, QueryParseError, QueryParser,
    QueryValue as DeepQueryValue, SearchAttribute as VisSearchAttribute, VisibilityError,
    VisibilityIndex as DeepVisibilityIndex, VisibilityQuery as DeepVisibilityQuery,
    VisibilityRecord, VisibilityStats, WorkflowExecutionStatus as VisExecStatus,
};
// history_api: full history API handler implementations.
pub use history_api::{
    EventType as ApiEventType, Failure as ApiFailure, FailureType as ApiFailureType,
    GetWorkflowExecutionHistoryRequest, GetWorkflowExecutionHistoryResponse, History as ApiHistory,
    HistoryApiContext, HistoryApiError, HistoryApiHandler, HistoryApiServiceImpl, HistoryApiStats,
    HistoryEvent as ApiHistoryEvent, PollActivityTaskQueueRequest, PollActivityTaskQueueResponse,
    QueryWorkflowRequest, QueryWorkflowResponse, RecordActivityTaskHeartbeatRequest,
    RecordActivityTaskHeartbeatResponse, RequestCancelWorkflowExecutionRequest,
    RespondActivityTaskCanceledRequest, RespondActivityTaskCompletedRequest,
    RespondActivityTaskFailedRequest, RetryPolicy as HistRetryPolicy,
    SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
    TaskQueueMetadata, TerminateWorkflowExecutionRequest, TimeoutType as ApiTimeoutType,
    WorkflowExecution as HistWorkflowExecution, WorkflowExecutionStatus as HistExecStatus,
};
// matching_workers: deep matching worker implementations.
pub use matching_workers::{
    DispatchResult, ForwardStats, InternalTask, LogicalTaskQueue as WorkerLogicalTaskQueue,
    MatchingLoadBalancer, PartitionLoad, PhysicalTaskQueue as WorkerPhysicalTaskQueue,
    PollerInfo as WorkerPollerInfo, RateLimiterState, RedirectInfo,
    TaskForwarder as WorkerTaskForwarder, TaskQueueConfig, TaskQueueManager, TaskQueueManagerStats,
    TaskQueuePartition as WorkerTaskQueuePartition, TaskQueueVersioning,
    TaskType as WorkerTaskType, VersionAssignmentRule, VersionData,
    VersionRedirectRule as WorkerVersionRedirectRule,
};
// frontend_handlers: expanded frontend API handlers.
pub use frontend_handlers::{
    ArchivalState as FrontendArchivalState, BadBinaries, BadBinaryInfo,
    CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse, DeprecateNamespaceRequest,
    DescribeNamespaceRequest, DescribeNamespaceResponse, DescribeWorkflowExecutionRequest,
    DescribeWorkflowExecutionResponse, FrontendError, FrontendServiceImpl,
    FrontendStats as HandlerFrontendStats, GetSearchAttributesRequest, GetSearchAttributesResponse,
    ListNamespacesRequest, ListNamespacesResponse, ListWorkflowExecutionsRequest,
    ListWorkflowExecutionsResponse, NamespaceConfig as FrontendNamespaceConfig, NamespaceFilter,
    NamespaceInfo, NamespaceReplicationConfig as FrontendNsReplicationConfig,
    NamespaceState as FrontendNamespaceState, PendingActivityInfo as FrontendPendingActivity,
    PendingActivityState as FrontendPendingActivityState, PendingWorkflowTaskInfo,
    RegisterNamespaceRequest, RegisterNamespaceResponse, ResetWorkflowExecutionRequest,
    ResetWorkflowExecutionResponse, SearchAttributeType as FrontendSearchAttributeType,
    UpdateNamespaceRequest, UpdateNamespaceResponse, WorkflowExecutionInfo as FrontendWorkflowInfo,
    WorkflowTaskType,
};
// client_sdk: workflow client, handle, schedule/namespace/search attribute clients.
pub use client_sdk::{
    AuthInterceptor as ClientAuthInterceptor, ClientConfig, ClientConnection, ClientError,
    ClientInterceptor, ClientRetryConfig, ClientStats, CreateScheduleOptions, GrpcClientConfig,
    HistoryEventEntry, LoggingInterceptor, MetricsInterceptor, NamespaceClient,
    NamespaceDescription, NamespaceOptions, ResetPointSelector,
    ScheduleAction as ClientScheduleAction, ScheduleCalendarSpec, ScheduleClient,
    ScheduleDescription, ScheduleHandle, ScheduleInterval, ScheduleOverlapPolicy, ScheduleSpec,
    SearchAttributeClient, SearchAttributeList, SearchAttributeType as ClientSearchAttributeType,
    StartWorkflowOptions, TlsConfig, TracingInterceptor, WorkflowClient, WorkflowDescription,
    WorkflowFailure as ClientWorkflowFailure, WorkflowHandle, WorkflowHistory, WorkflowListResult,
    WorkflowResult, WorkflowRetryPolicy, WorkflowStatus as ClientWorkflowStatus,
};
// rpc_framework: gRPC interceptors, connection management, load balancing.
pub use rpc_framework::{
    AuthInterceptor as RpcAuthInterceptor, BackendInfo, ConnectionManager, ConnectionManagerConfig,
    ConnectionManagerStats, ConnectionState, InterceptorChain, KeepAliveConfig,
    LoadBalanceStrategy, MethodDescriptor, NamespaceValidationInterceptor,
    RateLimitInterceptor as RpcRateLimitInterceptor, RedirectionInterceptor,
    RegistryStats as RpcRegistryStats, RetryInterceptor, RpcError, RpcInterceptor, RpcLoadBalancer,
    RpcRequest, RpcResponse, RpcServerConfig, RpcStatus, RpcTlsConfig, ServiceDescriptor,
    ServiceRegistry, TelemetryInterceptor as RpcTelemetryInterceptor, TimeoutInterceptor,
    TlsVersion, ValidationInterceptor as RpcValidationInterceptor,
};
// distributed_locks: shard ownership, leader election, fencing tokens.
pub use distributed_locks::{
    DistributedLock, LeaderElection, LockError, LockManager, LockManagerStats, ShardOwnershipInfo,
    ShardOwnershipManager as LockShardOwnershipManager,
};
// header_propagation: context propagation, header encoding/decoding.
pub use header_propagation::{
    BinaryHeaderCodec, ContextPropagator, Header, HeaderCodec, HeaderError, JsonHeaderCodec,
    PropagationChain, PropagationContext, PropagationStats,
};
// persistence_serialization: event serialization, batch serialization, schema registry.
pub use persistence_serialization::{
    BatchSerializer, EncodingType as SerializationEncoding, EventSerializer, FieldType,
    SchemaEntry, SchemaField, SchemaRegistry, SerializableEvent, SerializationError,
    SerializedData, SerializerStats,
};
// workflow_task_handler: deep RespondWorkflowTaskCompleted handler.
pub use workflow_task_handler::{
    ActivityTask as HandlerActivityTask, CancelExternalWorkflowCommand,
    CancelNexusOperationCommand, CancelTimerCommand as TaskCancelTimerCommand,
    CancelWorkflowCommand as TaskCancelWorkflowCommand, CommandRetryPolicy, CommandValidator,
    CompleteWorkflowCommand as TaskCompleteWorkflowCommand, CompletionResult,
    ContinueAsNewCommand as TaskContinueAsNewCommand,
    FailWorkflowCommand as TaskFailWorkflowCommand, GeneratedEvent,
    HandlerError as TaskHandlerError, HandlerStats, MeteringMetadata,
    ModifyWorkflowPropertiesCommand, ProcessedCommand, ProtocolMessage,
    ProtocolMessageCommand as TaskProtocolMessageCommand,
    RecordMarkerCommand as TaskRecordMarkerCommand,
    RequestCancelActivityCommand as TaskRequestCancelActivityCommand,
    RequestCancelChildWorkflowCommand, ScheduleActivityCommand as TaskScheduleActivityCommand,
    ScheduleNexusOperationCommand, SdkMetadata, SignalExternalWorkflowCommand,
    StartChildWorkflowCommand as TaskStartChildWorkflowCommand,
    StartTimerCommand as TaskStartTimerCommand, StickyAttributes, TimerTask as HandlerTimerTask,
    TransferTaskEntry, UpsertSearchAttributesCommand, ValidationError as TaskValidationError,
    VisibilityTask as HandlerVisibilityTask, WorkflowCommand as TaskWorkflowCommand,
    WorkflowTaskCompletion, WorkflowTaskHandler,
};
// nexus_deep: deep nexus operations, endpoints, callbacks.
pub use nexus_deep::{
    AuthMethod as NexusAuthMethod, CallbackResult, EndpointManagerStats, EndpointTarget,
    NexusEndpoint, NexusEndpointManager as DeepNexusEndpointManager, NexusError, NexusFailure,
    NexusLink, NexusOperation as DeepNexusOperation, NexusOperationManager,
    NexusOperationState as DeepNexusOperationState,
};
// clock_abstraction: time source, mock time, hybrid logical clock.
pub use clock_abstraction::{
    ClockStats, HybridLogicalClock, MockTimeSource, RealTimeSource, TimeSkippingTimeSource,
    TimeSource, TimerHandle,
};
// search_attributes: search attribute type system, validation, mapping.
pub use search_attributes::{
    SearchAttributeDefinition as SaDefinition, SearchAttributeError as SaError,
    SearchAttributeField as SaField, SearchAttributeMapper, SearchAttributeStats as SaStats,
    SearchAttributeType as SaType, SearchAttributeValue as SaValue,
};
// task_framework: task executor, scheduler, priority queue.
pub use task_framework::{
    PriorityTaskQueue, SchedulerStats as FrameworkSchedulerStats, Task as FwTask, TaskCategory,
    TaskExecutionError, TaskExecutionResult as FwTaskExecutionResult,
    TaskExecutor as FwTaskExecutor, TaskPriority, TaskQueueStats as FrameworkQueueStats,
    TaskScheduler, TaskState as FrameworkTaskState,
};
// quota_management: quota policies, namespace quotas, quota calculator.
pub use quota_management::{
    BucketStats, NamespaceQuotaStats, NamespaceQuotaTracker, OperationQuotaTracker, QuotaBucket,
    QuotaCalculator, QuotaPolicy as QuotaPolicyV2, QuotaPriority,
};
// lru_cache: LRU cache with TTL, pinning, metrics.
pub use lru_cache::{CacheStats as LruCacheStats, LruCache};
// backoff_retry: exponential backoff, jitter, retry budget.
pub use backoff_retry::{BackoffCalculator, BackoffCoordinator, JitterMode, RetryBudget};
// service_errors: typed gRPC service error hierarchy.
pub use service_errors::{ErrorCounter, ServiceError, ServiceErrorStatus};
// workflow_state_machine: mutable state, activity/timer/child/signal/query state.
pub use workflow_state_machine::{
    ActivityExecutionState, ActivityRetryPolicy, ActivityState, ChildWorkflowExecutionState,
    ChildWorkflowState, MutableState as WfMutableState, ParentClosePolicy, QueryExecutionState,
    QueryState as WfQueryState, SignalState, StateError, TimerExecutionState, TimerState,
    WorkflowExecutionState as WfExecutionState,
};
// history_engine: main workflow orchestrator, execution management, task scheduling.
pub use history_engine::{
    ActivityCompletionInfo, BufferedSignal, HistoryEngine, HistoryEngineConfig, HistoryEngineStats,
    PendingActivityInfo as HistPendingActivityInfo,
    PendingActivityState as HistPendingActivityState, PendingChildInfo as HistPendingChildInfo,
    PendingChildState as HistPendingChildState, PendingQuery,
    PendingSignalInfo as HistPendingSignalInfo, PendingTimerInfo as HistPendingTimerInfo,
    WorkflowExecState, WorkflowExecution as HEWorkflowExecution,
    WorkflowTaskInfo as HistWorkflowTaskInfo,
};
// replication_executor: replication task generation, execution, DLQ, stream management.
pub use replication_executor::{
    ReplicationExecError, ReplicationExecResult, ReplicationExecutorStats,
    ReplicationGeneratorStats, ReplicationPriority, ReplicationStream, ReplicationStreamManager,
    ReplicationTask as ReplTask, ReplicationTaskExecutor, ReplicationTaskGenerator,
    ReplicationTaskKind as ReplTaskKind, ReplicationTaskState as ReplTaskState, StreamManagerStats,
    StreamState as ReplStreamState,
};
// timer_queue_executor: timer task scheduling, timeout detection, backoff timers.
pub use timer_queue_executor::{
    BackoffEntry, BackoffTimerManager, TimeoutDetector, TimeoutDetectorStats, TimeoutInfo,
    TimeoutType as TimerTimeoutType, TimerQueueProcessor as TqeTimerQueueProcessor,
    TimerQueueStats, TimerTask as TqeTimerTask, TimerTaskKind, TimerTaskState,
};
// transfer_queue_executor: transfer task processing, visibility processing.
pub use transfer_queue_executor::{
    TransferProcessResult, TransferQueueProcessor as TqeTransferQueueProcessor, TransferQueueStats,
    TransferTask as TqeTransferTask, TransferTaskKind as TqeTransferTaskKind,
    TransferTaskState as TqeTransferTaskState, VisibilityProcessor, VisibilityProcessorStats,
    VisibilityTask as TqeVisibilityTask, VisibilityTaskKind,
};
// archival_engine: archival queue, history/visibility archival, namespace configs.
pub use archival_engine::{
    ArchivalKind, ArchivalManager, ArchivalManagerStats, ArchivalQueue, ArchivalQueueStats,
    ArchivalRecord, ArchivalState as ArchivalRecordState, ArchivalStoreKind,
    NamespaceArchivalConfig,
};
// worker_deployment: deployment registration, traffic routing, drain, rollback.
pub use worker_deployment::{
    DeploymentError as WdDeploymentError, DeploymentManager as WdDeploymentManager,
    DeploymentManagerStats as WdDeploymentManagerStats, DeploymentState as WdDeploymentState,
    WorkerDeployment as WdWorkerDeployment,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Batch 5: Deep subsystem modules (queue infra, workflow execution, shard ctrl, deletion, notifications)
// ═══════════════════════════════════════════════════════════════════════════════

// queue_infrastructure: slices, executables, DLQ, reader/writer, grouper, alerts
pub use queue_infrastructure::{
    ActionResult, ActiveStandbyExecutor, ActiveStandbyStats, AlertManagerStats, AlertSeverity,
    AlertThresholds, ClusterRole, DlqRecord, DlqWriter, DlqWriterStats, ExecutablePriority,
    ExecutableState, ExecutableTask, GroupBy, GrouperStats, QueueAction, QueueAlert,
    QueueAlertManager, QueueGrouper, QueueHealthReport, QueueIterator, QueueIteratorStats,
    QueueMonitor, QueueMonitorStats, QueueRange, QueueReader, QueueReaderStats, QueueSlice,
    QueueSliceStats, QueueTaskDescriptor, TaskKey as QiTaskKey, TaskPredicate,
};

// workflow_execution: deep mutable state, query/update registries, state transitions
pub use workflow_execution::{
    ActivityState as WfActivityState, ActivityStateEnum, ChildState,
    ChildWorkflowState as WfChildWorkflowState, GeneratedTask as DeepGeneratedTask,
    HistoryEvent as DeepHistoryEvent, MutableState as DeepMutableState, MutableStateStats,
    QueryEntry as DeepQueryEntry, QueryRegistry as DeepQueryRegistry, QueryState as DeepQueryState,
    RetryPolicy as DeepRetryPolicy, RetryState as DeepRetryState,
    SearchAttributeValue as DeepSearchAttributeValue, SignalInfo as WfSignalInfo, StateTransition,
    StateTransitionHistory, TaskGenerator as DeepTaskGenerator, TaskGeneratorStats,
    TimerState as WfTimerState, TimerStateEnum, UpdateEntry, UpdateRegistry, UpdateRegistryStats,
    UpdateState, WorkflowChecksum, WorkflowExecutionStatus, WorkflowState as DeepWorkflowState,
    WorkflowStatus as DeepWorkflowStatus,
};

// shard_controller: shard ownership, handover, engine factory, distribution
pub use shard_controller::{
    HandoverInfo, HandoverTracker, HandoverTrackerStats, ShardConfig,
    ShardContext as DeepShardContext, ShardContextStats, ShardController, ShardControllerConfig,
    ShardControllerStats, ShardDistribution, ShardEngine, ShardEngineFactory,
    ShardEngineFactoryConfig, ShardEngineStats, ShardError, ShardHealthReport,
    ShardState as DeepShardState,
};

// deletion_manager: workflow deletion pipeline
pub use deletion_manager::{
    DeletionManager, DeletionManagerConfig, DeletionManagerStats, DeletionRecord, DeletionStage,
    StepResult,
};

// notification_system: state change notifications, subscriptions, time-skipping
pub use notification_system::{
    NotificationCategory, NotificationEvent, NotificationFilter, NotificationHub,
    NotificationHubStats, NotificationPriority, NotificationType, SubscriberId, Subscription,
};

// namespace_manager: namespace lifecycle, registry, failover
pub use namespace_manager::{
    BadBinary, FailoverManager as NsFailoverManager, FailoverPhase,
    FailoverState as NsFailoverState, NamespaceChangeEvent as NsChangeEvent,
    NamespaceEntry as NsEntry, NamespaceEntryConfig as NsEntryConfig, NamespaceError as NsError,
    NamespaceLifecycleState as NsLifecycleState, NamespaceRegistry as NsRegistry,
    ReplicationNsConfig, ReplicationState as NsReplicationState, SearchAttrType,
};

// cluster_membership: ring hash, host info, health, topology
pub use cluster_membership::{
    ClusterReport, ClusterTopology, HealthChecker as ClusterHealthChecker2,
    HealthResult as ClusterHealthResult, HostAddress, HostInfo as ClusterHostInfo, HostState,
    RingHash as ClusterRingHash, ServiceRole,
};

// self_healing: anomaly detection, auto-recovery, deadlock detection, memory pressure
pub use self_healing::{
    AnomalyDetector, AnomalyDetectorStats, AnomalyEvent, AnomalySeverity, AnomalyType,
    AutoRecovery, AutoRecoveryStats, DeadlockDetector, DeadlockDetectorStats, EvictionEvent,
    HealingCycleResult, HealthScore, MemoryMonitor, MemoryMonitorStats, MetricWindow,
    RecoveryAction, RecoveryPlan, RecoveryPriority, RecoveryResult, RecoveryStatus,
    SelfHealingOrchestrator, SelfHealingStats, ShardRebalancer, ShardRebalancerStats,
};

// predictive_autoscaler: time-series forecasting, load prediction, proactive scaling
pub use predictive_autoscaler::{
    AutoscalerOrchestrator, AutoscalerStats, CapacityPlan, CapacityPlanner, CapacityUrgency,
    DataPoint, ForecasterStats, LoadForecaster, PoolMetrics, ResourceLimit, ScalingCycleResult,
    ScalingDecision, ScalingDecisionStatus, ScalingDirection, ScalingEngine, ScalingEngineStats,
    TimeSeriesBuffer, WorkerPoolScaler, WorkerPoolScalerStats,
};

// chaos_engineering: fault injection, resilience verification, game-day scenarios
pub use chaos_engineering::{
    ActiveFault, ChaosExperiment, CheckCondition, ExperimentConfig, ExperimentResult,
    ExperimentStatus, FaultInjector, FaultInjectorStats, FaultRecord, FaultSeverity, FaultStatus,
    FaultType, GameDayRunner, GameDayStats, ReportGenerator, ReportSection, ResilienceCheck,
    ResilienceCheckResult, ResilienceCheckType, ResilienceGrade, ResilienceReport,
    ResilienceVerifier, ScheduledFault, SectionStatus, SteadyStateCheck, VerifierStats,
};

// deep_observability: distributed tracing, metrics, structured logging, profiling, alerts
pub use deep_observability::{
    ActiveAlert, AlertCondition, AlertEngineStats, AlertRecord, AlertRule,
    AlertSeverity as DoAlertSeverity, HistogramData, HotPath, LogEntry, LogLevel as DoLogLevel,
    LoggerStats, MetricsRegistry as DeepMetricsRegistry, MetricsRegistryStats, ObservabilityHub,
    ObservabilityHubStats, PerformanceProfiler, PredictiveAlertEngine, ProfileData, ProfilerStats,
    Span, SpanEvent, SpanLink, SpanStatus as DoSpanStatus, StructuredLogger as DoStructuredLogger,
    Trace, TraceCollector, TraceCollectorStats, TraceStatus,
};

// workflow_commands: deep command validation, execution, pipeline
pub use workflow_commands::{
    CancellationType as WcCancellationType, ChildState as WcChildState,
    ChildWorkflowState as WcChildWorkflowState, CommandExecutionResult,
    CommandExecutor as WcCommandExecutor, CommandExecutorStats, CommandFailure as WcCommandFailure,
    CommandPipeline, CommandPipelineStats, CommandRetryPolicy as WcCommandRetryPolicy,
    CommandValidator as WcCommandValidator, CommandValidatorStats,
    CommandWorkflowExecution as WcCommandWorkflowExecution,
    CompleteWorkflowCommand as WcCompleteWorkflowCommand,
    ContinueAsNewCommand as WcContinueAsNewCommand, FailWorkflowCommand as WcFailWorkflowCommand,
    MarkerRecord as WcMarkerRecord, ModifyPropertiesCommand as WcModifyPropertiesCommand,
    ParentClosePolicy as WcParentClosePolicy, PendingActivity as WcPendingActivity,
    PendingActivityState as WcPendingActivityState, PendingTimer as WcPendingTimer,
    ProtocolMessageCommand as WcProtocolMessageCommand,
    RecordMarkerCommand as WcRecordMarkerCommand,
    ScheduleActivityCommand as WcScheduleActivityCommand,
    ScheduleNexusCommand as WcScheduleNexusCommand,
    SignalExternalCommand as WcSignalExternalCommand, SignalRecord as WcSignalRecord,
    StartChildWorkflowCommand as WcStartChildWorkflowCommand,
    StartTimerCommand as WcStartTimerCommand, ValidationError as WcValidationError,
    WorkflowCommand as WcWorkflowCommand, WorkflowResult as WcWorkflowResult,
};

// multi_backend_persistence: connection pooling, query builder, schema management, failover
pub use multi_backend_persistence::{
    BackendConfig as MbBackendConfig, BackendStatus as MbBackendStatus,
    BackendType as MbBackendType, BatchOperations as MbBatchOperations,
    BatchOpsStats as MbBatchOpsStats, BuiltQuery as MbBuiltQuery,
    CompactionResult as MbCompactionResult, CompactionRule as MbCompactionRule,
    CompactionStats as MbCompactionStats, ConnectionPool as MbConnectionPool,
    ConnectionPoolStats as MbConnectionPoolStats, ConnectionState as MbConnectionState,
    DataCompaction as MbDataCompaction, FailoverStats as MbFailoverStats, Migration as MbMigration,
    OrderByClause as MbOrderByClause, PersistenceFailover as MbPersistenceFailover,
    PersistenceRetryPolicy, PoolConnection as MbPoolConnection, QueryBuilder as MbQueryBuilder,
    QueryCondition as MbQueryCondition, QueryOperator as MbQueryOperator,
    QueryValue as MbQueryValue, SchemaManager as MbSchemaManager,
    SchemaManagerStats as MbSchemaManagerStats,
};

// history_event_applier: event application to mutable state, 35+ event types
pub use history_event_applier::{
    AppliedActivity, AppliedActivityState, AppliedChildState, AppliedChildWorkflow, AppliedMarker,
    AppliedSignal, AppliedState, AppliedTimer, AppliedTimerState, ApplyError, EventApplier,
    EventApplierStats, HistoryEvent as HeHistoryEvent, HistoryEventType as HeHistoryEventType,
    TimeoutType as HeTimeoutType,
};

// replication_manager: multi-cluster replication, conflict resolution
pub use replication_manager::{
    ClusterReplicationConfig as RmClusterReplicationConfig,
    ConflictResolution as RmConflictResolution, ConflictResolutionPolicy,
    ConflictResolver as RmConflictResolver, ConflictResolverStats as RmConflictResolverStats,
    ReplicationClusterStatus, ReplicationConflict as RmReplicationConflict, ReplicationManager,
    ReplicationManagerStats, ReplicationStream as RmReplicationStream,
    ReplicationStreamStats as RmReplicationStreamStats, ReplicationTask as RmReplicationTask,
    ReplicationTaskStatus as RmReplicationTaskStatus, ReplicationTaskType as RmReplicationTaskType,
};

// workflow_replay: replay engine, determinism checking, debugging
pub use workflow_replay::{
    DebugEvent, DeterminismCheck as WfDeterminismCheck, DeterminismCheckType,
    DeterminismChecker as WfDeterminismChecker,
    DeterminismCheckerStats as WfDeterminismCheckerStats, DeterminismResult as WfDeterminismResult,
    DeterminismViolation as WfDeterminismViolation, DeterminismViolationType, ReplayBreakpoint,
    ReplayDebugger, ReplayDebuggerStats, ReplayEngine as WfReplayEngine,
    ReplayEngineStats as WfReplayEngineStats, ReplayError, ReplayErrorType, ReplaySession,
    ReplayStatus, StepResult as WfStepResult, ViolationSeverity as WfViolationSeverity,
};
