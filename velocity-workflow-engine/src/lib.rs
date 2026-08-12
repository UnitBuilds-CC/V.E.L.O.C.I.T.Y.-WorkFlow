//! velocity-workflow-engine
//!
//! Hardware-native workflow execution engine. The entire runtime — state machine scheduling,
//! task queue, timer engine, WAL persistence, signal/query routing — lives in Rust with
//! zero managed heap allocations. C# acts as a thin FFI bridge only.
//!
//! Architecture:
//!   [C# Developer Code] ──FFI──► [velocity-workflow-engine] ──► [velocity-workflow-core]
//!   (thin bridge)                (runtime engine, zero-GC)      (slab, bitmask, Merkle)

pub mod ai_context;
pub mod archival;
pub mod auth;
pub mod batch;
pub mod cluster;
pub mod cold_storage;
pub mod cron;
pub mod durable_rpc;
pub mod dynamic_config;
pub mod engine;
pub mod event_history;
pub mod ffi;
pub mod hardware_traits;
pub mod hardware_integration;
pub mod heartbeat;
pub mod history_compaction;
pub mod memo;
pub mod metrics;
pub mod namespace;
pub mod nexus;
pub mod partition;
pub mod patch;
pub mod payload_codec;
pub mod query_handler;
pub mod raft_consensus;
pub mod rate_limiter;
pub mod replication_daemon;
pub mod replication_transport;
pub mod replay;
pub mod saga;
pub mod schedules;
pub mod sharding;
pub mod task_queue;
pub mod timer_engine;
pub mod visibility;
pub mod visibility_query;
pub mod wal;
pub mod worker_versioning;
pub mod worker_registry;
pub mod workflow_reset;

pub use ai_context::{AiContextWindow, AiContextConfig, AiContextStats, MessageRole, ContextMessage, AgentToolCall, ToolCallStatus};
pub use archival::{ArchiveStore, ArchiveRecord, ArchivePolicy};
pub use auth::{AuthManager, Permission, Role, Claims};
pub use batch::{BatchExecutor, BatchResult, BatchOperationType, BatchStatus};
pub use cluster::{ClusterManager, ClusterInfo, ReplicationTask};
pub use cold_storage::{FileColdStorage, ColdStorageRecord};
pub use cron::{CronScheduler, CronExpression, CronError, CronFireEvent};
pub use durable_rpc::{DurableServiceMesh, DurableRpcConfig, DurableRpcStats, DurableRpcState, DurableRpcCall};
pub use dynamic_config::DynamicConfig;
pub use engine::{WorkflowEngine, WorkflowContext, WorkflowStatus};
pub use event_history::{HistoryStore, HistoryEvent, HistoryEventType};
pub use hardware_traits::{SmartNicOffload, TeeEnclave, PeerToPeerReplication, SelfHealingEcc, HardwareError};
pub use hardware_integration::{HardwareAbstractionLayer, EccParityStore, EccStats, MerkleEccResult, compute_simple_merkle_root};
pub use heartbeat::HeartbeatTracker;
pub use history_compaction::{HistoryCompactor, CompactionConfig, CompactionStats, CompactionLevel, CompactableEventType};
pub use memo::MemoStore;
pub use metrics::MetricsRegistry;
pub use namespace::{NamespaceRegistry, NamespaceConfig, NamespaceError};
pub use nexus::{NexusManager, NexusOperation, NexusOperationState};
pub use partition::{PartitionManager, PartitionInfo};
pub use patch::{PatchRegistry, WorkflowPatch};
pub use payload_codec::{PayloadCodec, CodecChain};
pub use query_handler::QueryRegistry;
pub use raft_consensus::{RaftNode, RaftCluster, RaftConfig, RaftStats, RaftState, RaftLogEntry};
pub use rate_limiter::RateLimiter;
pub use replication_daemon::{ReplicationDaemon, ReplicationDaemonConfig, ReplicationDaemonStats, DeliveredTask};
pub use replication_transport::{ReplicationTransport, ReplicationLinkStatus};
pub use replay::{ReplayEngine, ReplayResult, ReplayActivityState, ReplayActivityStatus};
pub use saga::{SagaOrchestrator, SagaStepDefinition, SagaStatus};
pub use schedules::{ScheduleManager, ScheduleEntry, OverlapPolicy, CalendarSpec};
pub use sharding::ShardManager;
pub use task_queue::{TaskQueue, TaskItem, TaskKind};
pub use timer_engine::TimerEngine;
pub use visibility::{VisibilityIndex, WorkflowExecutionInfo, SearchAttributeValue};
pub use visibility_query::{VisibilityQuery, QueryCondition, QueryField, QueryOp};
pub use wal::{WalManager, WalWriter, WalRecord, WalEventType};
pub use worker_versioning::WorkerVersioning;
pub use worker_registry::{WorkerRegistry, WorkerInfo, WorkerStatus};
pub use workflow_reset::WorkflowResetter;
