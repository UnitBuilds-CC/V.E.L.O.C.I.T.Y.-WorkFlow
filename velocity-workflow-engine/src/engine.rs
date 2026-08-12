//! Core workflow execution engine. Manages the full workflow lifecycle: start, step execution,
//! activity scheduling, signal/query/update routing, child workflows, and timer integration.
//! All state lives in Rust-owned memory — zero managed heap, zero GC.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use velocity_workflow_core::SlabHeader;

use crate::zero_alloc::{SlotMap, SlotVec};

use crate::archival::{ArchivePolicy, ArchiveRecord, ArchiveStore};
use crate::auth::AuthManager;
use crate::batch::BatchExecutor;
use crate::cluster::{ClusterManager, VersionHistoryStore};
use crate::cold_storage::{CloudStorageAdapter, MockS3Adapter};
use crate::cron::CronScheduler;
use crate::db_adapter::{DatabaseAdapter, WorkflowRecord};
use crate::dynamic_config::DynamicConfig;
use crate::event_history::HistoryStore;
use crate::hardware_integration::HardwareAbstractionLayer;
use crate::heartbeat::HeartbeatTracker;
use crate::matching_service::{MatchTask, MatchingService, MatchingServiceConfig};
use crate::memo::MemoStore;
use crate::metrics::MetricsRegistry;
use crate::namespace::NamespaceRegistry;
use crate::nexus::NexusManager;
use crate::partition::PartitionManager;
use crate::patch::PatchRegistry;
use crate::payload_codec::CodecChain;
use crate::query_handler::QueryRegistry;
use crate::rate_limiter::RateLimiter;
use crate::replay::ReplayEngine;
use crate::replication_transport::ReplicationTransport;
use crate::saga::SagaOrchestrator;
use crate::schedules::ScheduleManager;
use crate::sharding::ShardManager;
use crate::task_queue::{TaskItem, TaskKind, TaskQueue};
use crate::timer_engine::TimerEngine;
use crate::visibility::{VisibilityIndex, WorkflowExecutionInfo};
use crate::wal::{WalEventType, WalManager};
use crate::worker_process::WorkerProcessManager;
use crate::worker_registry::WorkerRegistry;
use crate::worker_versioning::WorkerVersioning;
use crate::workflow_reset::{ResetReason, WorkflowResetter};

// ─── Helpers ────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Activity Timeouts & Retry ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ActivityRetryPolicy {
    pub max_attempts: u32,
    pub initial_interval: Duration,
    pub backoff_coefficient: f64,
    pub max_interval: Option<Duration>,
}

impl ActivityRetryPolicy {
    pub fn new(max_attempts: u32, initial_interval_ms: u64, backoff_coefficient: f64) -> Self {
        Self {
            max_attempts,
            initial_interval: Duration::from_millis(initial_interval_ms),
            backoff_coefficient,
            max_interval: None,
        }
    }

    pub fn with_max_interval(mut self, max_interval_ms: u64) -> Self {
        self.max_interval = Some(Duration::from_millis(max_interval_ms));
        self
    }

    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let delay_ms = self.initial_interval.as_millis() as f64
            * self.backoff_coefficient.powi(attempt as i32);
        let delay = Duration::from_millis(delay_ms as u64);
        match self.max_interval {
            Some(max) => delay.min(max),
            None => delay,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActivityTimeouts {
    pub schedule_to_start: Option<Duration>,
    pub start_to_close: Option<Duration>,
    pub schedule_to_close: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub scheduled_at: Instant,
    pub started_at: Option<Instant>,
    pub retry_policy: Option<ActivityRetryPolicy>,
    pub attempt: u32,
}

impl ActivityTimeouts {
    pub fn new(
        schedule_to_start: Option<Duration>,
        start_to_close: Option<Duration>,
        schedule_to_close: Option<Duration>,
        heartbeat_timeout: Option<Duration>,
    ) -> Self {
        Self {
            schedule_to_start,
            start_to_close,
            schedule_to_close,
            heartbeat_timeout,
            scheduled_at: Instant::now(),
            started_at: None,
            retry_policy: None,
            attempt: 1,
        }
    }

    pub fn with_retry_policy(mut self, policy: ActivityRetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    pub fn mark_started(&mut self) {
        self.started_at = Some(Instant::now());
    }

    pub fn check_timeouts(&self) -> Option<&'static str> {
        let now = Instant::now();

        // Check schedule_to_start
        if let Some(timeout) = self.schedule_to_start {
            if self.started_at.is_none() && now.duration_since(self.scheduled_at) > timeout {
                return Some("ScheduleToStart");
            }
        }

        // Check start_to_close
        if let (Some(timeout), Some(started)) = (self.start_to_close, self.started_at) {
            if now.duration_since(started) > timeout {
                return Some("StartToClose");
            }
        }

        // Check schedule_to_close
        if let Some(timeout) = self.schedule_to_close {
            if now.duration_since(self.scheduled_at) > timeout {
                return Some("ScheduleToClose");
            }
        }

        None
    }
}

// ─── Workflow Status ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(C)]
pub enum WorkflowStatus {
    Void = 0,
    Running = 1,
    Completed = 2,
    Failed = 3,
    Canceled = 4,
    Terminated = 5,
    ContinuedAsNew = 6,
    TimedOut = 7,
}

// ─── Parent Close Policy ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum ParentClosePolicy {
    Terminate = 0,
    Cancel = 1,
    Abandon = 2,
}

// ─── Workflow Context ──────────────────────────────────────────────────────────

/// Per-workflow execution state. Owns the slab header, step results, signal buffers,
/// and all mutable state for a single workflow instance.
pub struct WorkflowContext {
    pub workflow_id: u64,
    pub run_id: u64,
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub task_queue_hash: u64,
    pub slab: SlabHeader,
    pub status: WorkflowStatus,
    pub step_results: SlotMap<Vec<u8>>,
    pub signal_buffer: SlotVec<Vec<u8>>,
    pub update_buffer: SlotVec<Vec<u8>>,
    pub start_time: Instant,
    pub close_time: Option<Instant>,
    pub event_sequence: u64,
    pub parent_key: Option<u64>,
    pub child_keys: Vec<u64>,
    pub input_data: Option<Vec<u8>>,
    pub result_data: Option<Vec<u8>>,
    // Activity timeout tracking
    pub activity_timeouts: SlotMap<ActivityTimeouts>,
    /// Activity input payloads keyed by step index.
    pub activity_inputs: SlotMap<Vec<u8>>,
    pub workflow_execution_timeout: Option<Duration>,
    pub workflow_run_timeout: Option<Duration>,
    pub workflow_task_timeout: Option<Duration>,
}

impl WorkflowContext {
    pub fn new(
        workflow_id: u64,
        run_id: u64,
        workflow_type_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
    ) -> Self {
        Self {
            workflow_id,
            run_id,
            workflow_type_id,
            namespace_id: 0,
            task_queue_hash,
            slab: SlabHeader::new(workflow_id, run_id, total_steps),
            status: WorkflowStatus::Running,
            step_results: SlotMap::with_capacity(64),
            signal_buffer: SlotVec::with_capacity(16),
            update_buffer: SlotVec::with_capacity(16),
            start_time: Instant::now(),
            close_time: None,
            event_sequence: 0,
            parent_key: None,
            child_keys: Vec::new(),
            input_data: None,
            result_data: None,
            activity_timeouts: SlotMap::with_capacity(16),
            activity_inputs: SlotMap::with_capacity(16),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: None,
        }
    }

    /// Check if a step is completed (O(1) bitmask lookup).
    pub fn is_step_completed(&self, step: u32) -> bool {
        self.slab.step_bitmask.is_step_set(step as usize)
    }

    /// Mark a step as completed with a result. Updates bitmask + recalculates Merkle root.
    pub fn complete_step(&mut self, step: u32, result: Vec<u8>) {
        self.slab.mark_step_completed(step as usize);
        self.step_results.insert(step as u64, result);
        self.event_sequence += 1;
    }

    /// Get cached result for a completed step.
    pub fn get_step_result(&self, step: u32) -> Option<&Vec<u8>> {
        self.step_results.get(step as u64)
    }

    /// Deliver a signal to this workflow. Zero-alloc via SlotVec.
    pub fn signal(&mut self, signal_name_id: u64, payload: Vec<u8>) {
        self.signal_buffer.push(signal_name_id, payload);
        self.event_sequence += 1;
    }

    /// Deliver an update to this workflow. Zero-alloc via SlotVec.
    pub fn update(&mut self, update_name_id: u64, payload: Vec<u8>) {
        self.update_buffer.push(update_name_id, payload);
        self.event_sequence += 1;
    }

    /// Check if an update is pending for a given update name.
    pub fn has_update(&self, update_name_id: u64) -> bool {
        !self.update_buffer.is_empty_at(update_name_id)
    }

    /// Take the next pending update payload for a given update name.
    pub fn take_update(&mut self, update_name_id: u64) -> Option<Vec<u8>> {
        self.update_buffer.pop_front(update_name_id)
    }

    /// Check if a signal is pending for a given signal name.
    pub fn has_signal(&self, signal_name_id: u64) -> bool {
        !self.signal_buffer.is_empty_at(signal_name_id)
    }

    /// Take the next pending signal payload for a given signal name.
    pub fn take_signal(&mut self, signal_name_id: u64) -> Option<Vec<u8>> {
        self.signal_buffer.pop_front(signal_name_id)
    }

    pub fn complete(&mut self, result: Option<Vec<u8>>) {
        self.result_data = result;
        self.status = WorkflowStatus::Completed;
        self.close_time = Some(Instant::now());
    }

    pub fn fail(&mut self) {
        self.status = WorkflowStatus::Failed;
        self.close_time = Some(Instant::now());
    }

    pub fn cancel(&mut self) {
        self.status = WorkflowStatus::Canceled;
        self.close_time = Some(Instant::now());
    }

    pub fn terminate(&mut self) {
        self.status = WorkflowStatus::Terminated;
        self.close_time = Some(Instant::now());
    }

    /// Unique key for this workflow (namespace_id << 32 | workflow_id).
    pub fn key(&self) -> u64 {
        (self.namespace_id << 32) | self.workflow_id
    }
}

// ─── Workflow Engine ───────────────────────────────────────────────────────────

/// The central workflow runtime engine. All workflow state lives in Rust-owned memory.
/// C# interacts via FFI functions in `ffi.rs`.
pub struct WorkflowEngine {
    workflows: DashMap<u64, WorkflowContext>,
    task_queue: Arc<TaskQueue>,
    timer_engine: Arc<TimerEngine>,
    wal: Option<Arc<WalManager>>,
    namespaces: Arc<NamespaceRegistry>,
    visibility: Arc<VisibilityIndex>,
    cron_scheduler: Arc<CronScheduler>,
    batch_executor: Arc<BatchExecutor>,
    archive_store: Arc<ArchiveStore>,
    archive_policy: RwLock<ArchivePolicy>,
    next_run_id: AtomicU64,
    // ── Phase 3+ subsystems ──
    history_store: Arc<HistoryStore>,
    worker_versioning: Arc<WorkerVersioning>,
    rate_limiter: Arc<RateLimiter>,
    codec_chain: Arc<CodecChain>,
    heartbeat_tracker: Arc<HeartbeatTracker>,
    auth_manager: Arc<AuthManager>,
    dynamic_config: Arc<DynamicConfig>,
    query_registry: Arc<QueryRegistry>,
    memo_store: Arc<MemoStore>,
    schedule_manager: Arc<ScheduleManager>,
    workflow_resetter: Arc<WorkflowResetter>,
    patch_registry: Arc<PatchRegistry>,
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    nexus_manager: Arc<NexusManager>,
    metrics_registry: Arc<MetricsRegistry>,
    saga_orchestrator: Arc<SagaOrchestrator>,
    partition_manager: Arc<PartitionManager>,
    replay_engine: Arc<ReplayEngine>,
    worker_registry: Arc<WorkerRegistry>,
    matching_service: Arc<MatchingService>,
    worker_process_manager: Arc<WorkerProcessManager>,
    cloud_storage: RwLock<Arc<dyn CloudStorageAdapter>>,
    /// Pending activity retries waiting for timer-based delay. Key: workflow_key.
    pending_retries: std::sync::Mutex<HashMap<u64, Vec<(u32, u64)>>>, // (step, task_queue_hash)
    /// Version history store for multi-cluster replication conflict resolution.
    version_history_store: Arc<VersionHistoryStore>,
    /// Replication transport for sending/receiving tasks to/from remote clusters.
    replication_transport: Arc<ReplicationTransport>,
    /// Hardware abstraction layer — integrates ECC, SmartNIC, TEE into slab data paths.
    hal: RwLock<HardwareAbstractionLayer>,
    /// Optional database adapter for persistent storage (replaces in-memory-only).
    db_adapter: Option<Arc<dyn DatabaseAdapter>>,
    /// Cross-workflow dependency graph for observability and impact analysis.
    dependency_graph: Arc<crate::workflow_dependency_graph::WorkflowDependencyGraph>,
    /// Deployment pipeline for canary releases and automated rollbacks.
    deployment_pipeline: Arc<crate::deployment_pipeline::DeploymentPipeline>,
    /// Per-workflow execution tracker for SLO compliance, latency histograms, error budgets.
    execution_tracker: Arc<crate::workflow_execution_tracker::WorkflowExecutionTracker>,
    /// Per-workflow-type circuit breaker for cascade failure prevention.
    circuit_breaker: Arc<crate::circuit_breaker::CircuitBreakerRegistry>,
    /// Concurrency limiter for per-type and per-namespace workflow limits.
    concurrency_limiter: Arc<crate::concurrency_limiter::WorkflowConcurrencyLimiter>,
    /// Workflow change versioning registry for getVersion() API (safe code deployments).
    change_version_registry: Arc<crate::workflow_change_versioning::ChangeVersionRegistry>,
}

impl WorkflowEngine {
    pub fn new() -> Self {
        let tq = Arc::new(TaskQueue::new());
        let te = Arc::new(TimerEngine::new());

        Self {
            workflows: DashMap::new(),
            task_queue: tq,
            timer_engine: te,
            wal: None,
            namespaces: Arc::new(NamespaceRegistry::new()),
            visibility: Arc::new(VisibilityIndex::new()),
            cron_scheduler: Arc::new(CronScheduler::new()),
            batch_executor: Arc::new(BatchExecutor::new()),
            archive_store: Arc::new(ArchiveStore::new()),
            archive_policy: RwLock::new(ArchivePolicy::default_completed()),
            next_run_id: AtomicU64::new(1),
            history_store: Arc::new(HistoryStore::new()),
            worker_versioning: Arc::new(WorkerVersioning::new()),
            rate_limiter: Arc::new(RateLimiter::new(10_000.0, 10_000, 1_000.0)),
            codec_chain: Arc::new(CodecChain::new()),
            heartbeat_tracker: Arc::new(HeartbeatTracker::new()),
            auth_manager: Arc::new(AuthManager::new()),
            dynamic_config: Arc::new(DynamicConfig::new()),
            query_registry: Arc::new(QueryRegistry::new()),
            memo_store: Arc::new(MemoStore::new()),
            schedule_manager: Arc::new(ScheduleManager::new()),
            workflow_resetter: Arc::new(WorkflowResetter::new()),
            patch_registry: Arc::new(PatchRegistry::new()),
            cluster_manager: Arc::new(ClusterManager::new("local")),
            shard_manager: Arc::new(ShardManager::default()),
            nexus_manager: Arc::new(NexusManager::new()),
            metrics_registry: Arc::new(MetricsRegistry::new()),
            saga_orchestrator: Arc::new(SagaOrchestrator::new()),
            partition_manager: Arc::new(PartitionManager::new(4)),
            replay_engine: Arc::new(ReplayEngine::new()),
            worker_registry: Arc::new(WorkerRegistry::new()),
            matching_service: Arc::new(MatchingService::new(MatchingServiceConfig::default())),
            worker_process_manager: Arc::new(WorkerProcessManager::new(
                crate::worker_process::WorkerProcessManagerConfig::default(),
            )),
            cloud_storage: RwLock::new(Arc::new(MockS3Adapter::new("default-bucket", "us-east-1"))),
            pending_retries: std::sync::Mutex::new(HashMap::new()),
            version_history_store: Arc::new(VersionHistoryStore::new()),
            replication_transport: Arc::new(ReplicationTransport::new()),
            hal: RwLock::new(HardwareAbstractionLayer::with_simulated_hardware()),
            db_adapter: None,
            dependency_graph: Arc::new(
                crate::workflow_dependency_graph::WorkflowDependencyGraph::new(),
            ),
            deployment_pipeline: Arc::new(crate::deployment_pipeline::DeploymentPipeline::new()),
            execution_tracker: Arc::new(
                crate::workflow_execution_tracker::WorkflowExecutionTracker::new(),
            ),
            circuit_breaker: Arc::new(crate::circuit_breaker::CircuitBreakerRegistry::new()),
            concurrency_limiter: Arc::new(
                crate::concurrency_limiter::WorkflowConcurrencyLimiter::default(),
            ),
            change_version_registry: Arc::new(
                crate::workflow_change_versioning::ChangeVersionRegistry::new(),
            ),
        }
    }

    /// Create an engine with WAL persistence enabled.
    pub fn with_wal(wal_path: &str, max_file_size: u64) -> std::io::Result<Self> {
        let wal = Arc::new(WalManager::new(wal_path, max_file_size)?);
        let tq = Arc::new(TaskQueue::new());
        let te = Arc::new(TimerEngine::new());

        Ok(Self {
            workflows: DashMap::new(),
            task_queue: tq,
            timer_engine: te,
            wal: Some(wal),
            namespaces: Arc::new(NamespaceRegistry::new()),
            visibility: Arc::new(VisibilityIndex::new()),
            cron_scheduler: Arc::new(CronScheduler::new()),
            batch_executor: Arc::new(BatchExecutor::new()),
            archive_store: Arc::new(ArchiveStore::new()),
            archive_policy: RwLock::new(ArchivePolicy::default_completed()),
            next_run_id: AtomicU64::new(1),
            history_store: Arc::new(HistoryStore::new()),
            worker_versioning: Arc::new(WorkerVersioning::new()),
            rate_limiter: Arc::new(RateLimiter::new(10_000.0, 10_000, 1_000.0)),
            codec_chain: Arc::new(CodecChain::new()),
            heartbeat_tracker: Arc::new(HeartbeatTracker::new()),
            auth_manager: Arc::new(AuthManager::new()),
            dynamic_config: Arc::new(DynamicConfig::new()),
            query_registry: Arc::new(QueryRegistry::new()),
            memo_store: Arc::new(MemoStore::new()),
            schedule_manager: Arc::new(ScheduleManager::new()),
            workflow_resetter: Arc::new(WorkflowResetter::new()),
            patch_registry: Arc::new(PatchRegistry::new()),
            cluster_manager: Arc::new(ClusterManager::new("local")),
            shard_manager: Arc::new(ShardManager::default()),
            nexus_manager: Arc::new(NexusManager::new()),
            metrics_registry: Arc::new(MetricsRegistry::new()),
            saga_orchestrator: Arc::new(SagaOrchestrator::new()),
            partition_manager: Arc::new(PartitionManager::new(4)),
            replay_engine: Arc::new(ReplayEngine::new()),
            worker_registry: Arc::new(WorkerRegistry::new()),
            matching_service: Arc::new(MatchingService::new(MatchingServiceConfig::default())),
            worker_process_manager: Arc::new(WorkerProcessManager::new(
                crate::worker_process::WorkerProcessManagerConfig::default(),
            )),
            cloud_storage: RwLock::new(Arc::new(MockS3Adapter::new("default-bucket", "us-east-1"))),
            pending_retries: std::sync::Mutex::new(HashMap::new()),
            version_history_store: Arc::new(VersionHistoryStore::new()),
            replication_transport: Arc::new(ReplicationTransport::new()),
            hal: RwLock::new(HardwareAbstractionLayer::with_simulated_hardware()),
            db_adapter: None,
            dependency_graph: Arc::new(
                crate::workflow_dependency_graph::WorkflowDependencyGraph::new(),
            ),
            deployment_pipeline: Arc::new(crate::deployment_pipeline::DeploymentPipeline::new()),
            execution_tracker: Arc::new(
                crate::workflow_execution_tracker::WorkflowExecutionTracker::new(),
            ),
            circuit_breaker: Arc::new(crate::circuit_breaker::CircuitBreakerRegistry::new()),
            concurrency_limiter: Arc::new(
                crate::concurrency_limiter::WorkflowConcurrencyLimiter::default(),
            ),
            change_version_registry: Arc::new(
                crate::workflow_change_versioning::ChangeVersionRegistry::new(),
            ),
        })
    }

    /// Enable WAL persistence on an existing engine.
    pub fn enable_wal(&mut self, wal_path: &str, max_file_size: u64) -> std::io::Result<()> {
        self.wal = Some(Arc::new(WalManager::new(wal_path, max_file_size)?));
        Ok(())
    }

    /// Initialize the timer engine to record TimerFired events in history when timers expire.
    /// Should be called once after engine construction.
    pub fn init_timers(&self) {
        let history = self.history_store.clone();
        self.timer_engine
            .set_fire_callback(Box::new(move |workflow_key, timer_id| {
                // Zero-alloc: encode timer_id directly as fixed-size bytes
                history.record_event(
                    workflow_key,
                    crate::event_history::HistoryEventType::TimerFired,
                    timer_id.to_le_bytes().to_vec(),
                );
            }));
    }

    /// Purge completed workflows that exceed their namespace's retention period.
    /// Returns the number of workflows purged.
    pub fn purge_expired_workflows(&self) -> usize {
        let now = now_ms();

        let all = self.visibility.list_all();
        let mut purged = 0;

        for info in &all {
            // Only purge terminal workflows
            if info.status != WorkflowStatus::Completed
                && info.status != WorkflowStatus::Failed
                && info.status != WorkflowStatus::Canceled
                && info.status != WorkflowStatus::Terminated
                && info.status != WorkflowStatus::TimedOut
            {
                continue;
            }

            // Look up namespace retention
            let retention_ms = self
                .namespaces
                .get(info.namespace_id)
                .map(|cfg| cfg.retention_period.as_millis() as u64)
                .unwrap_or(7 * 24 * 60 * 60 * 1000); // default 7-day retention

            let close_time = info.close_time_ms.unwrap_or(info.start_time_ms);

            if now.saturating_sub(close_time) > retention_ms {
                self.visibility.remove(info.workflow_key);
                purged += 1;
            }
        }

        purged
    }

    /// Enable database persistence with the given adapter.
    /// When set, workflow state changes are persisted to the database in addition to in-memory storage.
    pub fn enable_db_adapter(&mut self, adapter: Arc<dyn DatabaseAdapter>) {
        self.db_adapter = Some(adapter);
    }

    /// Disable the database adapter.
    pub fn disable_db_adapter(&mut self) {
        self.db_adapter = None;
    }

    /// Get a reference to the database adapter (if enabled).
    pub fn db_adapter(&self) -> Option<&Arc<dyn DatabaseAdapter>> {
        self.db_adapter.as_ref()
    }

    /// Persist the current state of a workflow to the database adapter (if enabled).
    /// Returns Ok(()) if no adapter is configured (no-op).
    pub fn persist_workflow(
        &self,
        ctx: &WorkflowContext,
        namespace_name: &str,
    ) -> Result<(), String> {
        if let Some(adapter) = &self.db_adapter {
            let record = WorkflowRecord::from_context(ctx, namespace_name);
            adapter
                .save_workflow(ctx.key(), &record)
                .map_err(|e| format!("failed to persist workflow: {}", e))
        } else {
            Ok(())
        }
    }

    /// Get a reference to the WAL manager (if enabled).
    pub fn wal(&self) -> Option<&Arc<WalManager>> {
        self.wal.as_ref()
    }

    /// Access the namespace registry.
    pub fn namespaces(&self) -> &Arc<NamespaceRegistry> {
        &self.namespaces
    }

    /// Access the visibility index.
    pub fn visibility(&self) -> &Arc<VisibilityIndex> {
        &self.visibility
    }

    /// Access the matching service.
    pub fn matching_service(&self) -> &Arc<MatchingService> {
        &self.matching_service
    }

    /// Recover workflow state by replaying the WAL from disk.
    pub fn recover_from_wal(&self) -> Result<(usize, usize), String> {
        let wal = self.wal.as_ref().ok_or("WAL not enabled")?;
        let records = wal
            .replay_all()
            .map_err(|e| format!("WAL replay failed: {}", e))?;
        let total = records.len();
        let mut recovered_workflows = 0usize;

        for record in &records {
            let key = record.workflow_key;
            match record.event_type {
                WalEventType::WorkflowStarted => {
                    if record.data.len() < 36 {
                        continue;
                    }
                    let workflow_id = u64::from_le_bytes(record.data[0..8].try_into().unwrap());
                    let workflow_type_id =
                        u64::from_le_bytes(record.data[8..16].try_into().unwrap());
                    let namespace_id = u64::from_le_bytes(record.data[16..24].try_into().unwrap());
                    let task_queue_hash =
                        u64::from_le_bytes(record.data[24..32].try_into().unwrap());
                    let total_steps = u32::from_le_bytes(record.data[32..36].try_into().unwrap());
                    if let dashmap::mapref::entry::Entry::Vacant(e) = self.workflows.entry(key) {
                        let mut ctx = WorkflowContext::new(
                            workflow_id,
                            key & 0xFFFFFFFF,
                            workflow_type_id,
                            task_queue_hash,
                            total_steps,
                        );
                        ctx.namespace_id = namespace_id;
                        e.insert(ctx);
                        recovered_workflows += 1;
                    }
                }
                WalEventType::StepCompleted => {
                    if let Some(mut ctx) = self.workflows.get_mut(&key) {
                        if record.data.len() >= 4 {
                            let step = u32::from_le_bytes(record.data[0..4].try_into().unwrap());
                            ctx.complete_step(step, record.data[4..].to_vec());
                        }
                    }
                }
                WalEventType::SignalReceived => {
                    if let Some(mut ctx) = self.workflows.get_mut(&key) {
                        if record.data.len() >= 8 {
                            let signal_name_id =
                                u64::from_le_bytes(record.data[0..8].try_into().unwrap());
                            ctx.signal(signal_name_id, record.data[8..].to_vec());
                        }
                    }
                }
                WalEventType::WorkflowCompleted => {
                    if let Some(mut ctx) = self.workflows.get_mut(&key) {
                        ctx.complete(if record.data.is_empty() {
                            None
                        } else {
                            Some(record.data.clone())
                        });
                    }
                }
                WalEventType::WorkflowFailed => {
                    if let Some(mut ctx) = self.workflows.get_mut(&key) {
                        ctx.fail();
                    }
                }
                WalEventType::WorkflowCanceled => {
                    if let Some(mut ctx) = self.workflows.get_mut(&key) {
                        ctx.cancel();
                    }
                }
                WalEventType::WorkflowTerminated => {
                    if let Some(mut ctx) = self.workflows.get_mut(&key) {
                        ctx.terminate();
                    }
                }
                _ => {}
            }
        }

        // Update next_run_id
        let max_run_id = self.workflows.iter().map(|r| *r.key()).max().unwrap_or(0);
        if max_run_id > 0 {
            self.next_run_id.store(max_run_id + 1, Ordering::Relaxed);
        }

        // Register recovered workflows in visibility index
        for entry in self.workflows.iter() {
            let key = *entry.key();
            let ctx = entry.value();
            if self.visibility.get(key).is_none() {
                self.visibility.register(WorkflowExecutionInfo {
                    workflow_key: key,
                    workflow_id: ctx.workflow_id,
                    run_id: ctx.run_id,
                    workflow_type_id: ctx.workflow_type_id,
                    namespace_id: ctx.namespace_id,
                    status: ctx.status,
                    start_time_ms: 0,
                    close_time_ms: if matches!(
                        ctx.status,
                        WorkflowStatus::Completed
                            | WorkflowStatus::Failed
                            | WorkflowStatus::Canceled
                            | WorkflowStatus::Terminated
                            | WorkflowStatus::ContinuedAsNew
                            | WorkflowStatus::TimedOut
                    ) {
                        Some(0)
                    } else {
                        None
                    },
                    task_queue_hash: ctx.task_queue_hash,
                    search_attributes: HashMap::new(),
                    memo: HashMap::new(),
                });
            }
        }

        // Rebuild history from WAL records for replay verification
        for record in &records {
            let key = record.workflow_key;
            let hist_type = match record.event_type {
                WalEventType::WorkflowStarted => {
                    crate::event_history::HistoryEventType::WorkflowStarted
                }
                WalEventType::StepCompleted => {
                    crate::event_history::HistoryEventType::StepCompleted
                }
                WalEventType::SignalReceived => {
                    crate::event_history::HistoryEventType::SignalReceived
                }
                WalEventType::WorkflowCompleted => {
                    crate::event_history::HistoryEventType::WorkflowCompleted
                }
                WalEventType::WorkflowFailed => {
                    crate::event_history::HistoryEventType::WorkflowFailed
                }
                WalEventType::WorkflowCanceled => {
                    crate::event_history::HistoryEventType::WorkflowCanceled
                }
                WalEventType::WorkflowTerminated => {
                    crate::event_history::HistoryEventType::WorkflowTerminated
                }
                _ => continue,
            };
            self.history_store
                .record_event(key, hist_type, record.data.clone());
        }

        // Verify determinism via replay engine
        let mut verified = 0usize;
        for entry in self.workflows.iter() {
            let key = *entry.key();
            let history = self.history_store.get_history(key).unwrap_or_default();
            if !history.is_empty() {
                let result = self.replay_engine.replay(key, &history, None);
                if result.success {
                    verified += 1;
                }
            }
        }

        self.metrics_registry
            .inc_counter("velocity_wal_recovery_records");
        for _ in 0..verified {
            self.metrics_registry
                .inc_counter("velocity_wal_recovery_verified");
        }

        Ok((total, recovered_workflows))
    }

    /// Access the workflows map for concurrent reads/writes (DashMap — sharded, lock-free).
    pub fn workflows_write(&self) -> &DashMap<u64, WorkflowContext> {
        &self.workflows
    }

    /// Access the cron scheduler.
    pub fn cron_scheduler(&self) -> &Arc<CronScheduler> {
        &self.cron_scheduler
    }

    /// Access the batch executor.
    pub fn batch_executor(&self) -> &Arc<BatchExecutor> {
        &self.batch_executor
    }

    /// Access the archive store.
    pub fn archive_store(&self) -> &Arc<ArchiveStore> {
        &self.archive_store
    }

    // ── Phase 3+ Subsystem Accessors ────────────────────────────────────────

    pub fn history_store(&self) -> &Arc<HistoryStore> {
        &self.history_store
    }
    pub fn worker_versioning(&self) -> &Arc<WorkerVersioning> {
        &self.worker_versioning
    }
    pub fn rate_limiter(&self) -> &Arc<RateLimiter> {
        &self.rate_limiter
    }
    pub fn codec_chain(&self) -> &Arc<CodecChain> {
        &self.codec_chain
    }
    pub fn heartbeat_tracker(&self) -> &Arc<HeartbeatTracker> {
        &self.heartbeat_tracker
    }
    pub fn auth_manager(&self) -> &Arc<AuthManager> {
        &self.auth_manager
    }
    pub fn dynamic_config(&self) -> &Arc<DynamicConfig> {
        &self.dynamic_config
    }
    pub fn query_registry(&self) -> &Arc<QueryRegistry> {
        &self.query_registry
    }
    pub fn memo_store(&self) -> &Arc<MemoStore> {
        &self.memo_store
    }
    pub fn schedule_manager(&self) -> &Arc<ScheduleManager> {
        &self.schedule_manager
    }
    pub fn workflow_resetter(&self) -> &Arc<WorkflowResetter> {
        &self.workflow_resetter
    }
    pub fn patch_registry(&self) -> &Arc<PatchRegistry> {
        &self.patch_registry
    }
    pub fn cluster_manager(&self) -> &Arc<ClusterManager> {
        &self.cluster_manager
    }
    pub fn shard_manager(&self) -> &Arc<ShardManager> {
        &self.shard_manager
    }
    pub fn nexus_manager(&self) -> &Arc<NexusManager> {
        &self.nexus_manager
    }
    pub fn metrics_registry(&self) -> &Arc<MetricsRegistry> {
        &self.metrics_registry
    }
    pub fn saga_orchestrator(&self) -> &Arc<SagaOrchestrator> {
        &self.saga_orchestrator
    }
    pub fn dependency_graph(
        &self,
    ) -> &Arc<crate::workflow_dependency_graph::WorkflowDependencyGraph> {
        &self.dependency_graph
    }
    pub fn deployment_pipeline(&self) -> &Arc<crate::deployment_pipeline::DeploymentPipeline> {
        &self.deployment_pipeline
    }
    pub fn execution_tracker(
        &self,
    ) -> &Arc<crate::workflow_execution_tracker::WorkflowExecutionTracker> {
        &self.execution_tracker
    }
    pub fn circuit_breaker(&self) -> &Arc<crate::circuit_breaker::CircuitBreakerRegistry> {
        &self.circuit_breaker
    }
    pub fn concurrency_limiter(
        &self,
    ) -> &Arc<crate::concurrency_limiter::WorkflowConcurrencyLimiter> {
        &self.concurrency_limiter
    }
    pub fn change_version_registry(
        &self,
    ) -> &Arc<crate::workflow_change_versioning::ChangeVersionRegistry> {
        &self.change_version_registry
    }
    pub fn partition_manager(&self) -> &Arc<PartitionManager> {
        &self.partition_manager
    }
    pub fn replay_engine(&self) -> &Arc<ReplayEngine> {
        &self.replay_engine
    }
    pub fn worker_registry(&self) -> &Arc<WorkerRegistry> {
        &self.worker_registry
    }
    pub fn worker_process_manager(&self) -> &Arc<WorkerProcessManager> {
        &self.worker_process_manager
    }
    pub fn version_history_store(&self) -> &Arc<VersionHistoryStore> {
        &self.version_history_store
    }
    pub fn replication_transport(&self) -> &Arc<ReplicationTransport> {
        &self.replication_transport
    }
    pub fn cloud_storage(&self) -> Arc<dyn CloudStorageAdapter> {
        self.cloud_storage.read().unwrap().clone()
    }

    /// Access the hardware abstraction layer (read-only).
    pub fn hal(&self) -> std::sync::RwLockReadGuard<'_, HardwareAbstractionLayer> {
        self.hal.read().unwrap()
    }

    /// Access the hardware abstraction layer (write).
    pub fn hal_mut(&self) -> std::sync::RwLockWriteGuard<'_, HardwareAbstractionLayer> {
        self.hal.write().unwrap()
    }

    /// Set the cloud storage adapter (e.g., switch from mock S3 to mock GCS).
    pub fn set_cloud_storage(&self, adapter: Arc<dyn CloudStorageAdapter>) {
        *self.cloud_storage.write().unwrap() = adapter;
    }

    /// Apply an incoming replication task from a remote cluster.
    /// Records the event in history and updates the cluster failover version.
    /// Returns true if the task was applied successfully.
    pub fn apply_replication_task(&self, task: crate::cluster::ReplicationTask) -> bool {
        // Check version history for conflict resolution
        if !self.version_history_store.check_incoming(
            task.workflow_key,
            task.failover_version,
            task.last_event_id,
        ) {
            return false; // Stale or conflicting task
        }
        // Validate source cluster
        if !self
            .cluster_manager
            .apply_incoming_replication(task.clone())
        {
            return false;
        }
        // Record the replicated event in our history store
        let event_type = match task.event_type {
            1 => crate::event_history::HistoryEventType::WorkflowStarted,
            2 => crate::event_history::HistoryEventType::WorkflowCompleted,
            3 => crate::event_history::HistoryEventType::WorkflowFailed,
            4 => crate::event_history::HistoryEventType::ActivityScheduled,
            5 => crate::event_history::HistoryEventType::ActivityCompleted,
            6 => crate::event_history::HistoryEventType::ActivityFailed,
            7 => crate::event_history::HistoryEventType::SignalReceived,
            8 => crate::event_history::HistoryEventType::TimerStarted,
            9 => crate::event_history::HistoryEventType::TimerFired,
            _ => crate::event_history::HistoryEventType::WorkflowStarted,
        };
        self.history_store
            .record_event(task.workflow_key, event_type, task.payload);
        // Update version history for conflict resolution
        self.version_history_store.record_event(
            task.workflow_key,
            task.failover_version,
            task.last_event_id,
        );
        // Record metrics
        self.metrics_registry
            .inc_counter("velocity_replication_tasks_applied_total");
        true
    }

    /// Set the archive policy.
    pub fn set_archive_policy(&self, policy: ArchivePolicy) {
        *self.archive_policy.write().unwrap() = policy;
    }

    /// Get search attributes for a running workflow by its workflow_key.
    pub fn get_workflow_search_attributes(
        &self,
        workflow_key: u64,
    ) -> Option<std::collections::HashMap<String, crate::visibility::SearchAttributeValue>> {
        self.visibility
            .get(workflow_key)
            .map(|info| info.search_attributes)
    }

    /// Access the task queue (for polling from workers).
    pub fn task_queue(&self) -> &Arc<TaskQueue> {
        &self.task_queue
    }

    /// Access the timer engine.
    pub fn timer_engine(&self) -> &Arc<TimerEngine> {
        &self.timer_engine
    }

    /// Get or record a version decision for a workflow change ID.
    /// This is the engine-level convenience wrapper around the change version registry.
    /// Returns the version number (deterministic on replay).
    pub fn get_version(
        &self,
        workflow_key: u64,
        change_id: &str,
        min_supported: i32,
        max_supported: i32,
        is_replay: bool,
    ) -> crate::workflow_change_versioning::VersionResult {
        self.change_version_registry.get_version(
            workflow_key,
            change_id,
            min_supported,
            max_supported,
            is_replay,
        )
    }

    /// Start a new workflow execution. Creates the context, slab header, and schedules
    /// the first workflow task. Returns the workflow key.
    pub fn start_workflow(
        &self,
        workflow_id: u64,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
        input: Option<Vec<u8>>,
    ) -> u64 {
        self.start_workflow_with_attrs(
            workflow_id,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            input,
            HashMap::new(),
        )
    }

    /// Start a workflow with search attributes registered in visibility.
    pub fn start_workflow_with_attrs(
        &self,
        workflow_id: u64,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
        input: Option<Vec<u8>>,
        search_attributes: HashMap<String, crate::visibility::SearchAttributeValue>,
    ) -> u64 {
        let run_id = self.next_run_id.fetch_add(1, Ordering::Relaxed);
        let key = (namespace_id << 32) | workflow_id;

        let mut ctx = WorkflowContext::new(
            workflow_id,
            run_id,
            workflow_type_id,
            task_queue_hash,
            total_steps,
        );
        ctx.namespace_id = namespace_id;
        ctx.input_data = input;

        // Schedule the first workflow task
        self.task_queue.enqueue(
            task_queue_hash,
            TaskItem {
                task_id: 0,
                kind: TaskKind::WorkflowTask,
                workflow_key: key,
                task_queue_hash,
                step_index: 0,
                activity_name_id: 0,
                attempt: 1,
                priority: 0,
                deadline_ms: 0,
            },
        );

        // Submit to matching service for worker dispatch
        self.matching_service.add_task(MatchTask {
            task_id: 0,
            workflow_key: key,
            task_queue: format!("tq-{}", task_queue_hash),
            build_id: None,
            priority: 0,
            created_at: Instant::now(),
            forwarded_from: None,
        });

        self.workflows.insert(key, ctx);

        // Register in visibility index
        self.visibility.register(WorkflowExecutionInfo {
            workflow_key: key,
            workflow_id,
            run_id,
            workflow_type_id,
            namespace_id,
            status: WorkflowStatus::Running,
            start_time_ms: now_ms(),
            close_time_ms: None,
            task_queue_hash,
            search_attributes,
            memo: HashMap::new(),
        });

        // Record in event history — zero-alloc: encode IDs directly as bytes
        let mut hist_data = Vec::with_capacity(20);
        hist_data.extend_from_slice(b"type=");
        hist_data.extend_from_slice(&workflow_type_id.to_le_bytes());
        hist_data.extend_from_slice(b";ns=");
        hist_data.extend_from_slice(&namespace_id.to_le_bytes());
        self.history_store.record_event(
            key,
            crate::event_history::HistoryEventType::WorkflowStarted,
            hist_data,
        );

        // Record metrics
        self.metrics_registry
            .inc_counter("velocity_workflow_started_total");
        self.metrics_registry
            .set_gauge("velocity_workflows_running", self.workflows.len() as i64);

        // Track in execution tracker for SLO compliance
        self.execution_tracker.record_start(workflow_type_id);

        // Register in change version registry for getVersion() support
        self.change_version_registry.register_workflow(key);

        // Persist to WAL
        if let Some(wal) = &self.wal {
            let mut data = Vec::with_capacity(32);
            data.extend_from_slice(&workflow_id.to_le_bytes());
            data.extend_from_slice(&workflow_type_id.to_le_bytes());
            data.extend_from_slice(&namespace_id.to_le_bytes());
            data.extend_from_slice(&task_queue_hash.to_le_bytes());
            data.extend_from_slice(&total_steps.to_le_bytes());
            let _ = wal.append(WalEventType::WorkflowStarted, key, data);
            let _ = wal.sync(); // durability: fsync before returning to client
        }

        key
    }

    /// Try to start a workflow with circuit breaker protection.
    /// Returns `Some(key)` if the workflow was started, `None` if the circuit breaker rejected it.
    pub fn try_start_workflow(
        &self,
        workflow_id: u64,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
        input: Option<Vec<u8>>,
    ) -> Option<u64> {
        // Check circuit breaker before starting
        if !self.circuit_breaker.allow_request(workflow_type_id) {
            self.metrics_registry
                .inc_counter("velocity_workflow_rejected_by_circuit_breaker_total");
            return None;
        }
        let key = self.start_workflow(
            workflow_id,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            input,
        );
        Some(key)
    }

    /// Get the current step for a workflow (for the runner to know where to resume).
    pub fn get_current_step(&self, workflow_key: u64) -> u32 {
        self.workflows
            .get(&workflow_key)
            .map_or(0, |ctx| ctx.slab.current_step)
    }

    /// Check if a step is completed (bitmask check, O(1)).
    pub fn is_step_completed(&self, workflow_key: u64, step: u32) -> bool {
        self.workflows
            .get(&workflow_key)
            .is_some_and(|ctx| ctx.is_step_completed(step))
    }

    /// Get the cached result for a completed step.
    pub fn get_step_result(&self, workflow_key: u64, step: u32) -> Option<Vec<u8>> {
        self.workflows
            .get(&workflow_key)
            .and_then(|ctx| ctx.get_step_result(step).cloned())
    }

    /// Complete a step: store the result, update the bitmask + Merkle root, and schedule
    /// the next workflow task to continue execution.
    /// Also triggers ECC parity computation via the HAL (Merkle ECC self-healing write path).
    pub fn complete_step(&self, workflow_key: u64, step: u32, result: Vec<u8>) {
        // Persist to WAL first (borrows result), then move into context (zero clone)
        if let Some(wal) = &self.wal {
            let mut data = Vec::with_capacity(4 + result.len());
            data.extend_from_slice(&step.to_le_bytes());
            data.extend_from_slice(&result);
            let _ = wal.append(WalEventType::StepCompleted, workflow_key, data);
            let _ = wal.sync(); // durability: fsync before returning to client
        }

        // Update context + extract values under ONE DashMap shard lock — HAL lock is NOT nested.
        let (task_queue_hash, merkle_root) = {
            if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
                ctx.complete_step(step, result); // move — zero alloc
                (ctx.task_queue_hash, ctx.slab.merkle_root)
            } else {
                return;
            }
        }; // DashMap shard lock released here

        // ── HAL: Compute ECC parity after slab mutation (no nested lock) ──
        self.hal
            .write()
            .unwrap()
            .on_slab_write(workflow_key, &merkle_root, merkle_root);

        // Schedule next workflow task to advance the state machine
        self.task_queue.enqueue(
            task_queue_hash,
            TaskItem {
                task_id: 0,
                kind: TaskKind::WorkflowTask,
                workflow_key,
                task_queue_hash,
                step_index: step + 1,
                activity_name_id: 0,
                attempt: 1,
                priority: 0,
                deadline_ms: 0,
            },
        );
    }

    /// Schedule an activity for execution with timeout tracking.
    pub fn schedule_activity_with_timeouts(
        &self,
        workflow_key: u64,
        step: u32,
        activity_name_id: u64,
        _args: Vec<u8>,
        schedule_to_start_ms: u64,
        start_to_close_ms: u64,
        schedule_to_close_ms: u64,
        heartbeat_ms: u64,
    ) {
        if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
            // Store timeout tracking
            let timeouts = ActivityTimeouts::new(
                if schedule_to_start_ms > 0 {
                    Some(Duration::from_millis(schedule_to_start_ms))
                } else {
                    None
                },
                if start_to_close_ms > 0 {
                    Some(Duration::from_millis(start_to_close_ms))
                } else {
                    None
                },
                if schedule_to_close_ms > 0 {
                    Some(Duration::from_millis(schedule_to_close_ms))
                } else {
                    None
                },
                if heartbeat_ms > 0 {
                    Some(Duration::from_millis(heartbeat_ms))
                } else {
                    None
                },
            );
            ctx.activity_timeouts.insert(step as u64, timeouts);
            // Store activity input payload for retrieval during poll
            if !_args.is_empty() {
                ctx.activity_inputs.insert(step as u64, _args);
            }

            self.task_queue.enqueue(
                ctx.task_queue_hash,
                TaskItem {
                    task_id: 0,
                    kind: TaskKind::ActivityTask,
                    workflow_key,
                    task_queue_hash: ctx.task_queue_hash,
                    step_index: step,
                    activity_name_id,
                    attempt: 1,
                    priority: 0,
                    deadline_ms: 0,
                },
            );
        }
    }

    /// Schedule an activity for execution. The activity worker will pick this up
    /// from the task queue, execute it, and call `complete_activity` with the result.
    pub fn schedule_activity(
        &self,
        workflow_key: u64,
        step: u32,
        activity_name_id: u64,
        _args: Vec<u8>,
    ) {
        self.schedule_activity_with_timeouts(
            workflow_key,
            step,
            activity_name_id,
            _args,
            0,
            0,
            0,
            0,
        );
    }

    /// Complete an activity: store the result and schedule a workflow task to resume.
    pub fn complete_activity(&self, workflow_key: u64, step: u32, result: Vec<u8>) {
        self.complete_step(workflow_key, step, result);
    }

    /// Fail an activity with optional retry logic.
    /// Returns true if the activity was retried, false if it failed permanently.
    pub fn fail_activity_with_retry(&self, workflow_key: u64, step: u32) -> bool {
        if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
            let task_queue_hash = ctx.task_queue_hash;
            if let Some(timeouts) = ctx.activity_timeouts.get_mut(step as u64) {
                if let Some(policy) = &timeouts.retry_policy {
                    if timeouts.attempt < policy.max_attempts {
                        timeouts.attempt += 1;
                        let attempt = timeouts.attempt;
                        let delay = policy.calculate_delay(attempt);

                        if delay.as_millis() < 10 {
                            // Short delay — immediately re-enqueue
                            self.task_queue.enqueue(
                                task_queue_hash,
                                TaskItem {
                                    task_id: 0,
                                    kind: TaskKind::ActivityTask,
                                    workflow_key,
                                    task_queue_hash,
                                    step_index: step,
                                    activity_name_id: 0,
                                    attempt,
                                    priority: 0,
                                    deadline_ms: 0,
                                },
                            );
                        } else {
                            // Schedule a timer for the backoff delay
                            self.pending_retries
                                .lock()
                                .unwrap()
                                .entry(workflow_key)
                                .or_default()
                                .push((step, task_queue_hash));
                            self.timer_engine.schedule(workflow_key, delay);
                        }
                        return true; // Activity was retried
                    }
                }
            }
        }
        false // Activity failed permanently
    }

    /// Process a fired timer by re-enqueuing any pending activity retries for the workflow.
    pub fn process_fired_timer(&self, workflow_key: u64) {
        let retries = {
            let mut map = self.pending_retries.lock().unwrap();
            map.remove(&workflow_key)
        };
        if let Some(entries) = retries {
            for (step, task_queue_hash) in entries {
                let attempt = {
                    self.workflows
                        .get(&workflow_key)
                        .and_then(|ctx| ctx.activity_timeouts.get(step as u64).map(|t| t.attempt))
                        .unwrap_or(1)
                };
                self.task_queue.enqueue(
                    task_queue_hash,
                    TaskItem {
                        task_id: 0,
                        kind: TaskKind::ActivityTask,
                        workflow_key,
                        task_queue_hash,
                        step_index: step,
                        activity_name_id: 0,
                        attempt,
                        priority: 0,
                        deadline_ms: 0,
                    },
                );
            }
        }
    }

    /// Signal a running workflow.
    pub fn signal_workflow(&self, workflow_key: u64, signal_name_id: u64, payload: Vec<u8>) {
        // Persist signal payload to WAL first (borrows payload, avoids clone)
        if let Some(wal) = &self.wal {
            let mut data = Vec::with_capacity(8 + payload.len());
            data.extend_from_slice(&signal_name_id.to_le_bytes());
            data.extend_from_slice(&payload);
            let _ = wal.append(WalEventType::SignalReceived, workflow_key, data);
            let _ = wal.sync(); // durability: fsync before returning to client
        }

        {
            if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
                if ctx.status != WorkflowStatus::Running {
                    return;
                }
                // Move payload into context — zero alloc
                ctx.signal(signal_name_id, payload);
            } else {
                return;
            }
        }

        // Schedule a workflow task to process the signal
        if let Some(ctx) = self.workflows.get(&workflow_key) {
            self.task_queue.enqueue(
                ctx.task_queue_hash,
                TaskItem {
                    task_id: 0,
                    kind: TaskKind::SignalTask,
                    workflow_key,
                    task_queue_hash: ctx.task_queue_hash,
                    step_index: 0,
                    activity_name_id: signal_name_id,
                    attempt: 1,
                    priority: 0,
                    deadline_ms: 0,
                },
            );
        }
    }

    /// Check if a signal is pending for a workflow.
    pub fn has_signal(&self, workflow_key: u64, signal_name_id: u64) -> bool {
        self.workflows
            .get(&workflow_key)
            .is_some_and(|ctx| ctx.has_signal(signal_name_id))
    }

    /// Take the next pending signal payload.
    pub fn take_signal(&self, workflow_key: u64, signal_name_id: u64) -> Option<Vec<u8>> {
        self.workflows
            .get_mut(&workflow_key)
            .and_then(|mut ctx| ctx.take_signal(signal_name_id))
    }

    /// Complete a workflow with a result.
    pub fn complete_workflow(&self, workflow_key: u64, result: Option<Vec<u8>>) {
        // Clone result for WAL before moving into context
        let wal_data = result.as_ref().cloned().unwrap_or_default();
        {
            if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
                ctx.complete(result);
            }
        }
        self.visibility
            .update_status(workflow_key, WorkflowStatus::Completed, Some(now_ms()));
        self.history_store.record_event(
            workflow_key,
            crate::event_history::HistoryEventType::WorkflowCompleted,
            vec![],
        );
        self.metrics_registry
            .inc_counter("velocity_workflow_completed_total");
        self.metrics_registry
            .set_gauge("velocity_workflows_running", self.workflow_count() as i64);

        // Record deployment pipeline metrics if an active deployment exists
        {
            let wf_type_id = self
                .workflows
                .get(&workflow_key)
                .map_or(0, |c| c.workflow_type_id);
            let wf_type = format!("wf_type_{}", wf_type_id);
            if let Some(dep_id) = self.deployment_pipeline.get_active_deployment_id(&wf_type) {
                self.deployment_pipeline.record_execution(dep_id, true, 0);
            }
            // Track completion in execution tracker
            self.execution_tracker.record_completion(wf_type_id, 0);
            // Record circuit breaker success
            self.circuit_breaker.record_success(wf_type_id);
        }

        // Remove from dependency graph tracking (workflow is terminal)
        self.dependency_graph.remove_workflow(workflow_key);

        // Clean up change version registry
        self.change_version_registry
            .unregister_workflow(workflow_key);

        if let Some(wal) = &self.wal {
            // Persist result payload for crash durability
            let _ = wal.append(WalEventType::WorkflowCompleted, workflow_key, wal_data);
            let _ = wal.sync(); // durability: fsync before returning to client
        }
        // Auto-archive if policy says so
        self.maybe_auto_archive(workflow_key);
    }

    /// Fail a workflow.
    pub fn fail_workflow(&self, workflow_key: u64) {
        {
            if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
                ctx.fail();
            }
        }
        self.visibility
            .update_status(workflow_key, WorkflowStatus::Failed, Some(now_ms()));
        self.history_store.record_event(
            workflow_key,
            crate::event_history::HistoryEventType::WorkflowFailed,
            vec![],
        );
        self.metrics_registry
            .inc_counter("velocity_workflow_failed_total");

        // Record deployment pipeline failure metrics
        {
            let wf_type_id = self
                .workflows
                .get(&workflow_key)
                .map_or(0, |c| c.workflow_type_id);
            let wf_type = format!("wf_type_{}", wf_type_id);
            if let Some(dep_id) = self.deployment_pipeline.get_active_deployment_id(&wf_type) {
                self.deployment_pipeline.record_execution(dep_id, false, 0);
            }
            // Track failure in execution tracker
            self.execution_tracker.record_failure(wf_type_id);
            // Record circuit breaker failure
            self.circuit_breaker.record_failure(wf_type_id);
        }

        // Remove from dependency graph tracking
        self.dependency_graph.remove_workflow(workflow_key);

        // Clean up change version registry
        self.change_version_registry
            .unregister_workflow(workflow_key);

        if let Some(wal) = &self.wal {
            let _ = wal.append(WalEventType::WorkflowFailed, workflow_key, vec![]);
            let _ = wal.sync(); // durability: fsync before returning to client
        }
        self.maybe_auto_archive(workflow_key);
    }

    /// Cancel a workflow.
    pub fn cancel_workflow(&self, workflow_key: u64) {
        {
            if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
                ctx.cancel();
            }
        }
        self.visibility
            .update_status(workflow_key, WorkflowStatus::Canceled, Some(now_ms()));
        self.history_store.record_event(
            workflow_key,
            crate::event_history::HistoryEventType::WorkflowCanceled,
            vec![],
        );
        self.metrics_registry
            .inc_counter("velocity_workflow_canceled_total");

        // Track cancellation in execution tracker
        {
            let wf_type_id = self
                .workflows
                .get(&workflow_key)
                .map_or(0, |c| c.workflow_type_id);
            self.execution_tracker.record_cancellation(wf_type_id);
        }
        // Remove from dependency graph tracking
        self.dependency_graph.remove_workflow(workflow_key);

        // Clean up change version registry
        self.change_version_registry
            .unregister_workflow(workflow_key);

        if let Some(wal) = &self.wal {
            let _ = wal.append(WalEventType::WorkflowCanceled, workflow_key, vec![]);
            let _ = wal.sync(); // durability: fsync before returning to client
        }
        self.maybe_auto_archive(workflow_key);
    }

    /// Terminate a workflow immediately.
    pub fn terminate_workflow(&self, workflow_key: u64) {
        {
            if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
                ctx.terminate();
            }
        }
        self.visibility
            .update_status(workflow_key, WorkflowStatus::Terminated, Some(now_ms()));
        self.history_store.record_event(
            workflow_key,
            crate::event_history::HistoryEventType::WorkflowTerminated,
            vec![],
        );
        self.metrics_registry
            .inc_counter("velocity_workflow_terminated_total");

        // Track termination in execution tracker
        {
            let wf_type_id = self
                .workflows
                .get(&workflow_key)
                .map_or(0, |c| c.workflow_type_id);
            self.execution_tracker.record_termination(wf_type_id);
        }
        // Remove from dependency graph tracking
        self.dependency_graph.remove_workflow(workflow_key);

        // Clean up change version registry
        self.change_version_registry
            .unregister_workflow(workflow_key);

        if let Some(wal) = &self.wal {
            let _ = wal.append(WalEventType::WorkflowTerminated, workflow_key, vec![]);
            let _ = wal.sync(); // durability: fsync before returning to client
        }
        self.maybe_auto_archive(workflow_key);
    }

    /// Auto-archive a workflow if the current policy requires it.
    fn maybe_auto_archive(&self, workflow_key: u64) {
        let status = self.get_status(workflow_key);
        // Check policy without cloning
        let should_archive = {
            let policy = self.archive_policy.read().unwrap();
            policy.should_archive(status)
        };
        if !should_archive {
            return;
        }

        if let Some(ctx) = self.workflows.get(&workflow_key) {
            let record = ArchiveRecord {
                workflow_key,
                workflow_id: ctx.workflow_id,
                run_id: ctx.run_id,
                workflow_type_id: ctx.workflow_type_id,
                namespace_id: ctx.namespace_id,
                status: ctx.status,
                input_data: ctx.input_data.clone(),
                result_data: ctx.result_data.clone(),
                step_count: ctx.slab.total_steps,
                step_results: ctx
                    .step_results
                    .iter()
                    .map(|(k, v)| (k as u32, v.clone()))
                    .collect(),
                event_count: ctx.event_sequence,
                archived_at_ms: 0,
                start_time_ms: 0,
                close_time_ms: 0,
            };
            self.archive_store.archive(record);
        }
    }

    /// Get the status of a workflow.
    pub fn get_status(&self, workflow_key: u64) -> WorkflowStatus {
        self.workflows
            .get(&workflow_key)
            .map_or(WorkflowStatus::Void, |ctx| ctx.status)
    }

    /// Get the total steps for a workflow.
    pub fn get_total_steps(&self, workflow_key: u64) -> u32 {
        self.workflows
            .get(&workflow_key)
            .map_or(0, |ctx| ctx.slab.total_steps)
    }

    /// Get the input payload for a scheduled activity (by workflow key and step index).
    pub fn get_activity_input(&self, workflow_key: u64, step: u32) -> Option<Vec<u8>> {
        self.workflows
            .get(&workflow_key)
            .and_then(|ctx| ctx.activity_inputs.get(step as u64).cloned())
    }

    /// Get the slab header for a workflow (for Merkle verification).
    /// Also triggers ECC verification via the HAL (Merkle ECC self-healing read path).
    pub fn get_slab(&self, workflow_key: u64) -> Option<SlabHeader> {
        if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
            // ── HAL: Verify slab integrity via Merkle ECC self-healing loop ──
            let mut hal = self.hal.write().unwrap();
            let slab_data = &mut ctx.slab.merkle_root;
            let _ = hal.merkle_ecc_self_heal(workflow_key, slab_data.as_mut());
            Some(ctx.slab)
        } else {
            None
        }
    }

    /// Schedule a durable timer. When it fires, a workflow task is enqueued.
    pub fn schedule_timer(&self, workflow_key: u64, delay_ms: u64) -> u64 {
        self.timer_engine
            .schedule(workflow_key, Duration::from_millis(delay_ms))
    }

    /// Get the event sequence number for a workflow.
    pub fn get_event_sequence(&self, workflow_key: u64) -> u64 {
        self.workflows
            .get(&workflow_key)
            .map_or(0, |ctx| ctx.event_sequence)
    }

    /// Get the number of active workflows.
    pub fn workflow_count(&self) -> usize {
        self.workflows.len()
    }

    /// Start a child workflow.
    pub fn start_child_workflow(
        &self,
        parent_key: u64,
        child_workflow_id: u64,
        workflow_type_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
        input: Option<Vec<u8>>,
    ) -> u64 {
        let namespace_id = parent_key >> 32;
        let child_key = self.start_workflow(
            child_workflow_id,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            input,
        );

        // Link parent → child
        if let Some(mut parent) = self.workflows.get_mut(&parent_key) {
            parent.child_keys.push(child_key);
        }
        if let Some(mut child) = self.workflows.get_mut(&child_key) {
            child.parent_key = Some(parent_key);
        }

        // Track parent→child dependency in the dependency graph
        self.dependency_graph.add_dependency(
            parent_key,
            child_key,
            crate::workflow_dependency_graph::DependencyType::ParentChild,
            Some(format!("child_workflow_id={}", child_workflow_id)),
        );

        child_key
    }

    /// Shutdown the engine: stop task queue and timer engine.
    pub fn shutdown(&self) {
        self.task_queue.shutdown();
        self.timer_engine.shutdown();
    }

    /// Create a backup: WAL snapshot + JSON export of all workflow states.
    ///
    /// Returns `(snapshot_path, json_export_path)`.
    pub fn backup(
        &self,
        backup_dir: impl AsRef<std::path::Path>,
    ) -> std::io::Result<(std::path::PathBuf, std::path::PathBuf)> {
        let dir = backup_dir.as_ref();
        std::fs::create_dir_all(dir)?;

        // 1. WAL snapshot
        let snapshot_path = if let Some(wal) = &self.wal {
            wal.snapshot(dir)?
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "WAL not enabled",
            ));
        };

        // 2. JSON export of workflow states
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let json_path = dir.join(format!("engine_backup_{}.json", timestamp));

        let mut workflows_json = Vec::new();
        for entry in self.workflows.iter() {
            let ctx = entry.value();
            workflows_json.push(serde_json::json!({
                "workflow_key": *entry.key(),
                "workflow_id": ctx.workflow_id,
                "workflow_type_id": ctx.workflow_type_id,
                "namespace_id": ctx.namespace_id,
                "task_queue_hash": ctx.task_queue_hash,
                "status": format!("{:?}", ctx.status),
                "completed_steps": ctx.step_results.len(),
                "event_sequence": ctx.event_sequence,
            }));
        }

        let backup_json = serde_json::json!({
            "backup_timestamp": timestamp,
            "workflow_count": workflows_json.len(),
            "workflows": workflows_json,
        });

        std::fs::write(
            &json_path,
            serde_json::to_string_pretty(&backup_json).unwrap_or_default(),
        )?;

        Ok((snapshot_path, json_path))
    }

    // ─── Update Dispatch ─────────────────────────────────────────────────────

    /// Dispatch an update to a running workflow.
    pub fn update_workflow(&self, workflow_key: u64, update_name_id: u64, payload: Vec<u8>) {
        {
            if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
                if ctx.status != WorkflowStatus::Running {
                    return;
                }
                ctx.update(update_name_id, payload);
            }
        }

        // Schedule a workflow task to process the update
        if let Some(ctx) = self.workflows.get(&workflow_key) {
            self.task_queue.enqueue(
                ctx.task_queue_hash,
                TaskItem {
                    task_id: 0,
                    kind: TaskKind::SignalTask, // Reuse SignalTask kind for updates
                    workflow_key,
                    task_queue_hash: ctx.task_queue_hash,
                    step_index: 0,
                    activity_name_id: update_name_id,
                    attempt: 1,
                    priority: 0,
                    deadline_ms: 0,
                },
            );
        }
    }

    /// Check if an update is pending for a workflow.
    pub fn has_update(&self, workflow_key: u64, update_name_id: u64) -> bool {
        self.workflows
            .get(&workflow_key)
            .is_some_and(|ctx| ctx.has_update(update_name_id))
    }

    /// Take the next pending update payload.
    pub fn take_update(&self, workflow_key: u64, update_name_id: u64) -> Option<Vec<u8>> {
        self.workflows
            .get_mut(&workflow_key)
            .and_then(|mut ctx| ctx.take_update(update_name_id))
    }

    // ─── Cron ────────────────────────────────────────────────────────────────

    /// Register a cron schedule. Returns the schedule ID.
    pub fn register_cron(
        &self,
        cron_expression: &str,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
        current_time_minutes: u64,
    ) -> Result<u64, crate::cron::CronError> {
        self.cron_scheduler.register(
            cron_expression,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            current_time_minutes,
        )
    }

    /// Process cron fires: for each fired schedule, start a new workflow execution.
    pub fn process_cron_fires(&self, current_time_minutes: u64) -> Vec<u64> {
        let fires = self.cron_scheduler.advance_to(current_time_minutes);
        let mut started_keys = Vec::new();
        for fire in fires {
            let key = self.start_workflow(
                fire.workflow_type_id, // Use type_id as workflow_id for cron
                fire.workflow_type_id,
                fire.namespace_id,
                fire.task_queue_hash,
                fire.total_steps,
                None,
            );
            started_keys.push(key);
        }
        started_keys
    }

    // ─── Batch Operations ────────────────────────────────────────────────────

    /// Submit a batch terminate operation. Returns the batch ID.
    pub fn batch_terminate(&self, workflow_keys: Vec<u64>) -> u64 {
        self.batch_executor.submit_terminate(self, workflow_keys)
    }

    /// Submit a batch cancel operation. Returns the batch ID.
    pub fn batch_cancel(&self, workflow_keys: Vec<u64>) -> u64 {
        self.batch_executor.submit_cancel(self, workflow_keys)
    }

    /// Submit a batch signal operation. Returns the batch ID.
    pub fn batch_signal(
        &self,
        workflow_keys: Vec<u64>,
        signal_name_id: u64,
        payload: Vec<u8>,
    ) -> u64 {
        self.batch_executor
            .submit_signal(self, workflow_keys, signal_name_id, payload)
    }

    /// Get the result of a batch operation.
    pub fn get_batch_result(&self, batch_id: u64) -> Option<crate::batch::BatchResult> {
        self.batch_executor.get_result(batch_id)
    }

    // ─── Timeout Enforcement ──────────────────────────────────────────────────

    /// Check all activity timeouts and return timed-out activities.
    /// Returns a list of (workflow_key, step, timeout_type) for timed-out activities.
    pub fn check_activity_timeouts(&self) -> Vec<(u64, u32, String)> {
        let mut timed_out = Vec::new();

        for entry in self.workflows.iter() {
            let workflow_key = *entry.key();
            let ctx = entry.value();
            for (step, timeouts) in ctx.activity_timeouts.iter() {
                if let Some(timeout_type) = timeouts.check_timeouts() {
                    timed_out.push((workflow_key, step as u32, timeout_type.to_string()));
                }
            }
        }

        timed_out
    }

    /// Check workflow execution timeouts and terminate if needed.
    /// Returns the number of workflows that timed out.
    pub fn check_workflow_timeouts(&self) -> u32 {
        let mut timed_out = Vec::new();
        let now = Instant::now();

        for entry in self.workflows.iter() {
            let workflow_key = *entry.key();
            let ctx = entry.value();
            if ctx.status != WorkflowStatus::Running {
                continue;
            }

            // Check execution timeout
            if let Some(timeout) = ctx.workflow_execution_timeout {
                if now.duration_since(ctx.start_time) > timeout {
                    timed_out.push(workflow_key);
                    continue;
                }
            }

            // Check run timeout
            if let Some(timeout) = ctx.workflow_run_timeout {
                if now.duration_since(ctx.start_time) > timeout {
                    timed_out.push(workflow_key);
                }
            }
        }

        // Terminate timed-out workflows
        let count = timed_out.len() as u32;
        for workflow_key in timed_out {
            self.terminate_workflow(workflow_key);
        }
        count
    }

    /// Set workflow execution timeout.
    pub fn set_workflow_execution_timeout(&self, workflow_key: u64, timeout_ms: u64) {
        if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
            ctx.workflow_execution_timeout = if timeout_ms > 0 {
                Some(Duration::from_millis(timeout_ms))
            } else {
                None
            };
        }
    }

    // ─── Parent Close Policy ──────────────────────────────────────────────────

    /// Apply parent close policy when a parent workflow completes.
    /// Terminates/cancels/abandons child workflows based on the policy.
    pub fn apply_parent_close_policy(&self, parent_key: u64, policy: ParentClosePolicy) {
        let child_keys = {
            match self.workflows.get(&parent_key) {
                Some(ctx) => ctx.child_keys.clone(),
                None => return,
            }
        };

        for child_key in child_keys {
            match policy {
                ParentClosePolicy::Terminate => self.terminate_workflow(child_key),
                ParentClosePolicy::Cancel => self.cancel_workflow(child_key),
                ParentClosePolicy::Abandon => {} // Do nothing
            }
        }
    }

    // ─── Query Dispatch ──────────────────────────────────────────────────────

    /// Execute a registered query handler for a workflow.
    /// Returns the query result or None if no handler is registered.
    pub fn execute_query(
        &self,
        workflow_key: u64,
        query_name_id: u64,
        input: &[u8],
    ) -> Option<Vec<u8>> {
        self.query_registry
            .execute_query(workflow_key, query_name_id, input)
    }

    /// Register a query handler for a workflow.
    pub fn register_query_handler(
        &self,
        workflow_key: u64,
        query_name_id: u64,
        handler: crate::query_handler::QueryHandler,
    ) {
        self.query_registry
            .register_handler(workflow_key, query_name_id, handler);
    }

    // ─── Workflow Reset ──────────────────────────────────────────────────────

    /// Reset a workflow to a previous event ID.
    /// Clears step results after the reset point and resets the workflow state.
    /// Returns true if reset was successful, false otherwise.
    pub fn reset_workflow(&self, workflow_key: u64, reset_to_event_id: u64) -> bool {
        let reset_ok = {
            if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
                // Only reset running or failed workflows
                if ctx.status != WorkflowStatus::Running && ctx.status != WorkflowStatus::Failed {
                    return false;
                }

                // Create a reset point
                let _reset_id = self.workflow_resetter.create_reset_point(
                    workflow_key,
                    reset_to_event_id,
                    ResetReason::ManualReset,
                );

                // Clear step results after the reset point
                // (In a full implementation, we'd replay from event history)
                ctx.step_results.retain(|&step, _| step < reset_to_event_id);

                // Reset the slab bitmask for steps after the reset point
                for step in reset_to_event_id..ctx.slab.total_steps as u64 {
                    ctx.slab.step_bitmask.clear_step(step as usize);
                }

                // Reset status to Running
                ctx.status = WorkflowStatus::Running;
                ctx.close_time = None;
                true
            } else {
                return false;
            }
        }; // DashMap shard lock released here

        if reset_ok {
            // Update visibility
            self.visibility
                .update_status(workflow_key, WorkflowStatus::Running, None);
            self.history_store.record_event(
                workflow_key,
                crate::event_history::HistoryEventType::WorkflowReset,
                vec![],
            );
        }
        reset_ok
    }

    // ─── SignalWithStart ──────────────────────────────────────────────────────

    /// Atomically signal a workflow or start it if not already running.
    /// Returns (workflow_key, was_started).
    pub fn signal_with_start(
        &self,
        workflow_id: u64,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
        signal_name_id: u64,
        payload: Vec<u8>,
    ) -> (u64, bool) {
        let key = (namespace_id << 32) | workflow_id;
        let status = self.get_status(key);
        if status == WorkflowStatus::Running {
            self.signal_workflow(key, signal_name_id, payload);
            return (key, false);
        }
        // Start new workflow then signal it
        let wk = self.start_workflow(
            workflow_id,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            None,
        );
        self.signal_workflow(wk, signal_name_id, payload);
        (wk, true)
    }

    // ─── ContinuedAsNew ──────────────────────────────────────────────────────

    /// Complete current workflow and start a new run with the given input.
    /// Returns the new workflow key.
    pub fn continue_as_new(&self, workflow_key: u64, new_input: Option<Vec<u8>>) -> u64 {
        let (_workflow_id, workflow_type_id, namespace_id, task_queue_hash, total_steps) = {
            match self.workflows.get(&workflow_key) {
                Some(ctx) => (
                    ctx.workflow_id,
                    ctx.workflow_type_id,
                    ctx.namespace_id,
                    ctx.task_queue_hash,
                    ctx.slab.total_steps,
                ),
                None => return 0,
            }
        };
        // Mark current as ContinuedAsNew
        {
            if let Some(mut ctx) = self.workflows.get_mut(&workflow_key) {
                ctx.status = WorkflowStatus::ContinuedAsNew;
                ctx.close_time = Some(Instant::now());
            }
        }
        self.visibility
            .update_status(workflow_key, WorkflowStatus::ContinuedAsNew, Some(now_ms()));
        self.history_store.record_event(
            workflow_key,
            crate::event_history::HistoryEventType::WorkflowContinuedAsNew,
            vec![],
        );
        // Start new run with a unique workflow_id (use run_id as the new workflow_id to ensure unique key)
        let new_run_id = self.next_run_id.fetch_add(1, Ordering::Relaxed);
        let new_key = (namespace_id << 32) | new_run_id;
        let mut ctx = WorkflowContext::new(
            new_run_id,
            new_run_id,
            workflow_type_id,
            task_queue_hash,
            total_steps,
        );
        ctx.namespace_id = namespace_id;
        ctx.input_data = new_input;
        // Schedule the first workflow task
        self.task_queue.enqueue(
            task_queue_hash,
            TaskItem {
                task_id: 0,
                kind: TaskKind::WorkflowTask,
                workflow_key: new_key,
                task_queue_hash,
                step_index: 0,
                activity_name_id: 0,
                attempt: 1,
                priority: 0,
                deadline_ms: 0,
            },
        );
        self.workflows.insert(new_key, ctx);
        self.visibility.register(WorkflowExecutionInfo {
            workflow_key: new_key,
            workflow_id: new_run_id,
            run_id: new_run_id,
            workflow_type_id,
            namespace_id,
            status: WorkflowStatus::Running,
            start_time_ms: 0,
            close_time_ms: None,
            task_queue_hash,
            search_attributes: HashMap::new(),
            memo: HashMap::new(),
        });
        self.history_store.record_event(
            new_key,
            crate::event_history::HistoryEventType::WorkflowStarted,
            vec![],
        );
        new_key
    }

    // ─── Workflow Execution Description ──────────────────────────────────────

    /// Describe a workflow execution with pending activities, children, timers, and signals.
    pub fn describe_workflow(&self, workflow_key: u64) -> Option<WorkflowExecutionDescription> {
        // Extract context data under one read guard, then release
        let ctx_data = {
            let ctx = self.workflows.get(&workflow_key)?;

            // Pending activities: steps that are scheduled but not completed
            let mut pending_activities = Vec::new();
            for (step, timeouts) in ctx.activity_timeouts.iter() {
                if !ctx.is_step_completed(step as u32) {
                    pending_activities.push(PendingActivityInfo {
                        activity_id: step,
                        step: step as u32,
                        state: if timeouts.started_at.is_some() {
                            PendingActivityState::Started
                        } else {
                            PendingActivityState::Scheduled
                        },
                        attempt: timeouts.attempt,
                        heartbeat_details: Vec::new(),
                        scheduled_at_ms: 0,
                        last_heartbeat_at_ms: 0,
                    });
                }
            }

            // Pending children keys
            let child_keys = ctx.child_keys.clone();

            // Pending signals
            let pending_signals: Vec<PendingSignalInfo> = ctx
                .signal_buffer
                .iter()
                .map(|(name_id, payloads)| PendingSignalInfo {
                    signal_name_id: name_id,
                    payload_count: payloads.len() as u32,
                })
                .collect();

            let execution_duration = ctx
                .close_time
                .unwrap_or_else(Instant::now)
                .duration_since(ctx.start_time);

            (
                ctx.workflow_id,
                ctx.run_id,
                ctx.workflow_type_id,
                ctx.namespace_id,
                ctx.status,
                ctx.start_time,
                ctx.close_time,
                execution_duration,
                pending_activities,
                child_keys,
                pending_signals,
                ctx.slab.total_steps,
                ctx.slab.step_bitmask.count_completed(),
                ctx.parent_key,
            )
        }; // DashMap read guard released here

        let (
            workflow_id,
            run_id,
            workflow_type_id,
            namespace_id,
            status,
            start_time,
            close_time,
            execution_duration,
            pending_activities,
            child_keys,
            pending_signals,
            total_steps,
            completed_steps,
            parent_key,
        ) = ctx_data;

        // Resolve child statuses (separate lookups — no lock held on parent)
        let pending_children: Vec<PendingChildInfo> = child_keys
            .iter()
            .map(|&child_key| {
                let child_status = self
                    .workflows
                    .get(&child_key)
                    .map(|c| c.status)
                    .unwrap_or(WorkflowStatus::Void);
                PendingChildInfo {
                    workflow_key: child_key,
                    status: child_status,
                }
            })
            .collect();

        // Pending timers: count of non-fired timers from the timer engine
        let pending_timers = self.timer_engine.pending_count() as u32;

        Some(WorkflowExecutionDescription {
            workflow_key,
            workflow_id,
            run_id,
            workflow_type_id,
            namespace_id,
            status,
            start_time,
            close_time,
            execution_duration,
            pending_activities,
            pending_children,
            pending_signals,
            pending_timers,
            total_steps,
            completed_steps,
            has_parent: parent_key.is_some(),
            parent_key,
        })
    }
}

// ─── Workflow Execution Description Types ─────────────────────────────────────

/// Detailed description of a workflow execution.
#[derive(Debug, Clone)]
pub struct WorkflowExecutionDescription {
    pub workflow_key: u64,
    pub workflow_id: u64,
    pub run_id: u64,
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub status: WorkflowStatus,
    pub start_time: Instant,
    pub close_time: Option<Instant>,
    pub execution_duration: Duration,
    pub pending_activities: Vec<PendingActivityInfo>,
    pub pending_children: Vec<PendingChildInfo>,
    pub pending_signals: Vec<PendingSignalInfo>,
    pub pending_timers: u32,
    pub total_steps: u32,
    pub completed_steps: u32,
    pub has_parent: bool,
    pub parent_key: Option<u64>,
}

impl WorkflowExecutionDescription {
    /// Whether this workflow is still running.
    pub fn is_running(&self) -> bool {
        self.status == WorkflowStatus::Running
    }

    /// Total number of pending items (activities + children + signals + timers).
    pub fn total_pending(&self) -> usize {
        self.pending_activities.len()
            + self.pending_children.len()
            + self.pending_signals.len()
            + self.pending_timers as usize
    }

    /// Progress as a fraction (0.0 to 1.0).
    pub fn progress(&self) -> f64 {
        if self.total_steps == 0 {
            return 1.0;
        }
        self.completed_steps as f64 / self.total_steps as f64
    }
}

/// State of a pending activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingActivityState {
    Scheduled,
    Started,
    RequestCancel,
}

/// Information about a pending activity.
#[derive(Debug, Clone)]
pub struct PendingActivityInfo {
    pub activity_id: u64,
    pub step: u32,
    pub state: PendingActivityState,
    pub attempt: u32,
    pub heartbeat_details: Vec<u8>,
    pub scheduled_at_ms: u64,
    pub last_heartbeat_at_ms: u64,
}

/// Information about a pending child workflow.
#[derive(Debug, Clone)]
pub struct PendingChildInfo {
    pub workflow_key: u64,
    pub status: WorkflowStatus,
}

/// Information about a pending signal.
#[derive(Debug, Clone)]
pub struct PendingSignalInfo {
    pub signal_name_id: u64,
    pub payload_count: u32,
}

impl Default for WorkflowEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_and_complete_workflow() {
        let engine = WorkflowEngine::new();

        let key = engine.start_workflow(1001, 1, 0, 42, 3, None);
        assert_eq!(engine.get_status(key), WorkflowStatus::Running);
        assert_eq!(engine.get_total_steps(key), 3);

        // Complete step 0
        assert!(!engine.is_step_completed(key, 0));
        engine.complete_step(key, 0, vec![1, 2, 3]);
        assert!(engine.is_step_completed(key, 0));
        assert_eq!(engine.get_step_result(key, 0), Some(vec![1, 2, 3]));

        // Complete step 1
        engine.complete_step(key, 1, vec![4, 5, 6]);
        assert!(engine.is_step_completed(key, 1));

        // Complete the workflow
        engine.complete_workflow(key, Some(vec![42]));
        assert_eq!(engine.get_status(key), WorkflowStatus::Completed);

        engine.shutdown();
    }

    #[test]
    fn test_signal_workflow() {
        let engine = WorkflowEngine::new();
        let key = engine.start_workflow(2001, 1, 0, 42, 1, None);

        assert!(!engine.has_signal(key, 100));
        engine.signal_workflow(key, 100, vec![7, 8, 9]);
        assert!(engine.has_signal(key, 100));

        let payload = engine.take_signal(key, 100).unwrap();
        assert_eq!(payload, vec![7, 8, 9]);
        assert!(!engine.has_signal(key, 100));

        engine.shutdown();
    }

    #[test]
    fn test_merkle_verification() {
        let engine = WorkflowEngine::new();
        let key = engine.start_workflow(3001, 1, 0, 42, 5, None);

        let slab = engine.get_slab(key).unwrap();
        assert!(slab.verify_merkle_root());

        engine.complete_step(key, 0, vec![1]);
        let slab = engine.get_slab(key).unwrap();
        assert!(slab.verify_merkle_root());

        engine.shutdown();
    }

    #[test]
    fn test_child_workflow() {
        let engine = WorkflowEngine::new();
        let parent_key = engine.start_workflow(4001, 1, 0, 42, 2, None);
        let child_key = engine.start_child_workflow(parent_key, 4002, 2, 42, 1, None);

        assert_eq!(engine.get_status(child_key), WorkflowStatus::Running);

        let parent = engine.workflows.get(&parent_key).unwrap();
        assert!(parent.child_keys.contains(&child_key));
        drop(parent);

        let child = engine.workflows.get(&child_key).unwrap();
        assert_eq!(child.parent_key, Some(parent_key));

        engine.shutdown();
    }

    #[test]
    fn test_task_queue_integration() {
        let engine = WorkflowEngine::new();
        let key = engine.start_workflow(5001, 1, 0, 99, 2, None);

        // start_workflow enqueues a WorkflowTask — verify it's in the queue
        assert_eq!(engine.task_queue().pending_count(99), 1);

        let task = engine.task_queue().try_poll(99).unwrap();
        assert_eq!(task.kind, TaskKind::WorkflowTask);
        assert_eq!(task.workflow_key, key);

        engine.shutdown();
    }

    #[test]
    fn test_update_dispatch() {
        let engine = WorkflowEngine::new();
        let key = engine.start_workflow(6001, 1, 0, 42, 1, None);

        assert!(!engine.has_update(key, 200));
        engine.update_workflow(key, 200, vec![10, 20, 30]);
        assert!(engine.has_update(key, 200));

        let payload = engine.take_update(key, 200).unwrap();
        assert_eq!(payload, vec![10, 20, 30]);
        assert!(!engine.has_update(key, 200));

        engine.shutdown();
    }

    #[test]
    fn test_cron_registration_and_fires() {
        let engine = WorkflowEngine::new();

        // Register a cron that fires every minute
        let schedule_id = engine.register_cron("* * * * *", 100, 0, 42, 1, 0).unwrap();
        assert!(schedule_id > 0);
        assert_eq!(engine.cron_scheduler().schedule_count(), 1);

        // Process cron fires at time 5
        let started = engine.process_cron_fires(5);
        assert_eq!(started.len(), 1);

        // The started workflow should be running
        assert_eq!(engine.get_status(started[0]), WorkflowStatus::Running);

        engine.shutdown();
    }

    #[test]
    fn test_batch_terminate() {
        let engine = WorkflowEngine::new();
        let k1 = engine.start_workflow(7001, 1, 0, 42, 1, None);
        let k2 = engine.start_workflow(7002, 1, 0, 42, 1, None);
        let k3 = engine.start_workflow(7003, 1, 0, 42, 1, None);

        let batch_id = engine.batch_terminate(vec![k1, k2, k3]);
        assert!(batch_id > 0);

        let result = engine.get_batch_result(batch_id).unwrap();
        assert_eq!(result.total, 3);
        assert_eq!(result.succeeded, 3);

        assert_eq!(engine.get_status(k1), WorkflowStatus::Terminated);
        assert_eq!(engine.get_status(k2), WorkflowStatus::Terminated);
        assert_eq!(engine.get_status(k3), WorkflowStatus::Terminated);

        engine.shutdown();
    }

    #[test]
    fn test_auto_archive_on_complete() {
        let engine = WorkflowEngine::new();
        let key = engine.start_workflow(8001, 1, 10, 42, 2, None);

        // Complete the workflow — should auto-archive
        engine.complete_workflow(key, Some(vec![42]));
        assert_eq!(engine.get_status(key), WorkflowStatus::Completed);

        // Should be in the archive
        assert_eq!(engine.archive_store().count(), 1);
        let archived = engine.archive_store().get(key).unwrap();
        assert_eq!(archived.workflow_type_id, 1);
        assert_eq!(archived.namespace_id, 10);

        engine.shutdown();
    }

    #[test]
    fn test_auto_archive_on_terminate() {
        let engine = WorkflowEngine::new();
        // Set policy to archive all terminal states
        engine.set_archive_policy(ArchivePolicy::archive_all());

        let key = engine.start_workflow(9001, 1, 0, 42, 1, None);

        engine.terminate_workflow(key);
        assert_eq!(engine.archive_store().count(), 1);

        engine.shutdown();
    }

    #[test]
    fn test_describe_workflow() {
        let engine = WorkflowEngine::new();
        let key = engine.start_workflow(7001, 1, 0, 42, 3, None);

        let desc = engine.describe_workflow(key).unwrap();
        assert_eq!(desc.workflow_key, key);
        assert_eq!(desc.workflow_type_id, 1);
        assert_eq!(desc.status, WorkflowStatus::Running);
        assert!(desc.is_running());
        assert_eq!(desc.total_steps, 3);
        assert_eq!(desc.completed_steps, 0);
        assert!(!desc.has_parent);

        // Complete a step and re-check
        engine.complete_step(key, 0, vec![1]);
        let desc2 = engine.describe_workflow(key).unwrap();
        assert_eq!(desc2.completed_steps, 1);
        assert!(desc2.progress() > 0.0);

        // Describe non-existent workflow
        assert!(engine.describe_workflow(99999).is_none());

        engine.shutdown();
    }

    #[test]
    fn test_describe_workflow_with_children() {
        let engine = WorkflowEngine::new();
        let parent = engine.start_workflow(7100, 1, 0, 42, 2, None);
        let child = engine.start_child_workflow(parent, 7101, 2, 42, 1, None);

        let desc = engine.describe_workflow(parent).unwrap();
        assert_eq!(desc.pending_children.len(), 1);
        assert_eq!(desc.pending_children[0].workflow_key, child);

        engine.shutdown();
    }

    #[test]
    fn test_describe_workflow_with_signals() {
        let engine = WorkflowEngine::new();
        let key = engine.start_workflow(7200, 1, 0, 42, 1, None);
        engine.signal_workflow(key, 100, vec![1, 2, 3]);

        let desc = engine.describe_workflow(key).unwrap();
        assert_eq!(desc.pending_signals.len(), 1);
        assert_eq!(desc.pending_signals[0].signal_name_id, 100);
        assert_eq!(desc.pending_signals[0].payload_count, 1);

        engine.shutdown();
    }

    // ─── Lifecycle Integration Tests ───────────────────────────────────

    #[test]
    fn test_execution_tracker_lifecycle() {
        let engine = WorkflowEngine::new();

        // Start workflows
        let k1 = engine.start_workflow(1, 100, 0, 42, 3, None);
        let k2 = engine.start_workflow(2, 100, 0, 42, 3, None);
        let k3 = engine.start_workflow(3, 200, 0, 42, 3, None);

        let summary = engine.execution_tracker().global_summary();
        assert_eq!(summary.started, 3);

        // Complete two, fail one
        engine.complete_workflow(k1, Some(vec![1]));
        engine.complete_workflow(k2, Some(vec![2]));
        engine.fail_workflow(k3);

        let summary = engine.execution_tracker().global_summary();
        assert_eq!(summary.completed, 2);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.active, 0);

        // Per-type stats
        let stats_100 = engine.execution_tracker().get_stats(100).unwrap();
        assert_eq!(stats_100.started, 2);
        assert_eq!(stats_100.completed, 2);
        assert_eq!(stats_100.failed, 0);

        let stats_200 = engine.execution_tracker().get_stats(200).unwrap();
        assert_eq!(stats_200.started, 1);
        assert_eq!(stats_200.completed, 0);
        assert_eq!(stats_200.failed, 1);

        engine.shutdown();
    }

    #[test]
    fn test_dependency_graph_child_workflow_integration() {
        let engine = WorkflowEngine::new();
        let parent_key = engine.start_workflow(1, 1, 0, 42, 3, None);
        let child_key = engine.start_child_workflow(parent_key, 2, 2, 42, 3, None);

        // Verify dependency graph has the edge
        let deps = engine.dependency_graph().get_dependencies(parent_key);
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].to_workflow_key, child_key);
        assert_eq!(
            deps[0].dep_type,
            crate::workflow_dependency_graph::DependencyType::ParentChild
        );

        // Complete parent — should be removed from graph
        engine.complete_workflow(parent_key, None);
        let deps = engine.dependency_graph().get_dependencies(parent_key);
        assert!(deps.is_empty());

        engine.shutdown();
    }

    #[test]
    fn test_cancel_and_terminate_tracker_integration() {
        let engine = WorkflowEngine::new();
        let k1 = engine.start_workflow(1, 1, 0, 42, 3, None);
        let k2 = engine.start_workflow(2, 1, 0, 42, 3, None);

        engine.cancel_workflow(k1);
        engine.terminate_workflow(k2);

        let summary = engine.execution_tracker().global_summary();
        assert_eq!(summary.canceled, 1);
        assert_eq!(summary.terminated, 1);

        engine.shutdown();
    }

    #[test]
    fn test_deployment_pipeline_integration() {
        let engine = WorkflowEngine::new();

        // Start a deployment for workflow type 100
        let dep_id = engine.deployment_pipeline().start_deployment(
            "wf_type_100",
            "build-1",
            crate::deployment_pipeline::DeploymentConfig::aggressive(),
        );

        // Start and complete a workflow of that type
        let k1 = engine.start_workflow(1, 100, 0, 42, 3, None);
        engine.complete_workflow(k1, Some(vec![]));

        // The deployment should have recorded an execution
        let dep = engine.deployment_pipeline().get_deployment(dep_id).unwrap();
        assert_eq!(dep.metrics.total_executions, 1);
        assert_eq!(dep.metrics.successful_executions, 1);

        // Start and fail a workflow
        let k2 = engine.start_workflow(2, 100, 0, 42, 3, None);
        engine.fail_workflow(k2);

        let dep = engine.deployment_pipeline().get_deployment(dep_id).unwrap();
        assert_eq!(dep.metrics.total_executions, 2);
        assert_eq!(dep.metrics.failed_executions, 1);

        engine.shutdown();
    }

    #[test]
    fn test_circuit_breaker_lifecycle_integration() {
        let engine = WorkflowEngine::new();

        // Register a circuit breaker with low threshold
        engine.circuit_breaker().register(
            100,
            crate::circuit_breaker::CircuitBreakerConfig {
                failure_threshold: 2,
                cooldown_ms: 100,
                success_threshold: 1,
                half_open_max_requests: 1,
                failure_window_ms: 60_000,
            },
        );

        // Start and fail workflows to trip the circuit
        let k1 = engine.start_workflow(1, 100, 0, 42, 3, None);
        engine.fail_workflow(k1);
        let k2 = engine.start_workflow(2, 100, 0, 42, 3, None);
        engine.fail_workflow(k2);

        // Circuit should be open
        assert_eq!(
            engine.circuit_breaker().get_state(100),
            crate::circuit_breaker::CircuitState::Open
        );

        // try_start_workflow should return None
        assert!(engine.try_start_workflow(3, 100, 0, 42, 3, None).is_none());

        // Wait for cooldown and try again
        std::thread::sleep(std::time::Duration::from_millis(150));
        let k3 = engine.try_start_workflow(3, 100, 0, 42, 3, None);
        assert!(k3.is_some());

        // Complete successfully to close the circuit
        engine.complete_workflow(k3.unwrap(), None);
        assert_eq!(
            engine.circuit_breaker().get_state(100),
            crate::circuit_breaker::CircuitState::Closed
        );

        engine.shutdown();
    }

    #[test]
    fn test_concurrency_limiter_integration() {
        let engine = WorkflowEngine::new();

        // Set a tight limit
        engine.concurrency_limiter().set_type_limit(100, 2);

        // Acquire slots
        let result = engine.concurrency_limiter().acquire(1, 100, 0, 0);
        assert_eq!(result, crate::concurrency_limiter::AcquireResult::Acquired);
        let result = engine.concurrency_limiter().acquire(2, 100, 0, 0);
        assert_eq!(result, crate::concurrency_limiter::AcquireResult::Acquired);

        // Third should be rejected
        let result = engine.concurrency_limiter().acquire(3, 100, 0, 0);
        assert_eq!(result, crate::concurrency_limiter::AcquireResult::Rejected);

        // Release one and try again
        engine.concurrency_limiter().release(100, 0);
        let result = engine.concurrency_limiter().acquire(3, 100, 0, 0);
        assert_eq!(result, crate::concurrency_limiter::AcquireResult::Acquired);

        // Stats check
        assert_eq!(engine.concurrency_limiter().active_for_type(100), 2);
        assert_eq!(engine.concurrency_limiter().global_active(), 2);

        engine.shutdown();
    }

    #[test]
    fn test_change_version_lifecycle() {
        let engine = WorkflowEngine::new();
        let key = engine.start_workflow(1, 100, 1, 5000, 3, None);

        // Workflow is registered in change version registry
        assert_eq!(engine.change_version_registry().tracked_workflow_count(), 1);

        // Get version — first call decides
        let v = engine.get_version(key, "add-shipping", 1, 3, false);
        assert_eq!(v.version(), 3);
        assert!(v.is_decided());

        // Replay returns same version
        let v2 = engine.get_version(key, "add-shipping", 1, 5, true);
        assert_eq!(v2.version(), 3);
        assert!(v2.is_existing());

        // Complete workflow cleans up version registry
        engine.complete_workflow(key, None);
        assert_eq!(engine.change_version_registry().tracked_workflow_count(), 0);
    }

    #[test]
    fn test_change_version_multiple_workflows() {
        let engine = WorkflowEngine::new();
        let k1 = engine.start_workflow(1, 100, 1, 5000, 3, None);
        let k2 = engine.start_workflow(2, 100, 1, 5000, 3, None);

        // Different workflows get independent version decisions
        let v1 = engine.get_version(k1, "feature-x", 1, 2, false);
        let v2 = engine.get_version(k2, "feature-x", 1, 5, false);
        assert_eq!(v1.version(), 2);
        assert_eq!(v2.version(), 5);

        engine.complete_workflow(k1, None);
        assert_eq!(engine.change_version_registry().tracked_workflow_count(), 1);

        engine.fail_workflow(k2);
        assert_eq!(engine.change_version_registry().tracked_workflow_count(), 0);
    }

    #[test]
    fn test_change_version_cancel_terminate_cleanup() {
        let engine = WorkflowEngine::new();
        let k1 = engine.start_workflow(1, 100, 1, 5000, 3, None);
        let k2 = engine.start_workflow(2, 100, 1, 5000, 3, None);
        engine.get_version(k1, "x", 1, 1, false);
        engine.get_version(k2, "y", 1, 2, false);
        assert_eq!(engine.change_version_registry().tracked_workflow_count(), 2);

        engine.cancel_workflow(k1);
        assert_eq!(engine.change_version_registry().tracked_workflow_count(), 1);

        engine.terminate_workflow(k2);
        assert_eq!(engine.change_version_registry().tracked_workflow_count(), 0);
    }
}
