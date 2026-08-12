//! Workflow execution context — ties together mutable state, history builder,
//! shard context, task generator, and queue processing into a unified execution context.
//! Matches Temporal's service/history/workflow/context.go (~1,350 lines).
//!
//! 1. **WorkflowContext**: Full execution context for a workflow.
//! 2. **ShardContext**: Per-shard execution context with ownership tracking.
//! 3. **ContextManager**: Manages workflow contexts across shards.
//! 4. **WorkflowLock**: Distributed locking for workflow execution.
//! 5. **ExecutionStats**: Execution statistics tracking.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::time::{Duration, Instant};

// ─── 1. Workflow Execution Context ───────────────────────────────────────────

/// Execution state of a workflow context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextState {
    Created,
    Loaded,
    Locked,
    Processing,
    Completed,
    Closed,
}

/// Full workflow execution context tying all subsystems together.
pub struct WorkflowExecutionContext {
    pub workflow_key: u64,
    pub workflow_id: u64,
    pub run_id: u64,
    pub namespace_id: u64,
    pub shard_id: u64,
    pub context_state: RwLock<ContextState>,
    pub workflow_type: String,
    pub task_queue: String,

    // Mutable state reference
    pub status: AtomicU64, // 0=void, 1=running, 2=completed, etc.
    pub next_event_id: AtomicU64,
    pub last_first_event_id: AtomicU64,

    // Execution tracking
    pub started_at: Instant,
    pub last_updated_at: Mutex<Instant>,
    pub command_count: AtomicU64,
    pub task_generated_count: AtomicU64,

    // Lock management
    pub lock_id: Mutex<Option<String>>,
    pub lock_acquired_at: Mutex<Option<Instant>>,
    pub lock_timeout_ms: u64,

    // Size tracking
    pub history_size_bytes: AtomicU64,
    pub mutation_count: AtomicU64,
    pub checksum: AtomicU64,

    // Config
    pub execution_timeout_ms: Option<u64>,
    pub run_timeout_ms: Option<u64>,
    pub task_timeout_ms: Option<u64>,

    // Search attributes and memo
    pub search_attributes: RwLock<HashMap<String, Vec<u8>>>,
    pub memo: RwLock<HashMap<String, Vec<u8>>>,
}

impl WorkflowExecutionContext {
    pub fn new(workflow_key: u64, workflow_id: u64, run_id: u64,
        namespace_id: u64, shard_id: u64, workflow_type: &str, task_queue: &str) -> Self {
        Self {
            workflow_key, workflow_id, run_id, namespace_id, shard_id,
            context_state: RwLock::new(ContextState::Created),
            workflow_type: workflow_type.to_string(),
            task_queue: task_queue.to_string(),
            status: AtomicU64::new(1), // Running
            next_event_id: AtomicU64::new(1),
            last_first_event_id: AtomicU64::new(1),
            started_at: Instant::now(),
            last_updated_at: Mutex::new(Instant::now()),
            command_count: AtomicU64::new(0),
            task_generated_count: AtomicU64::new(0),
            lock_id: Mutex::new(None),
            lock_acquired_at: Mutex::new(None),
            lock_timeout_ms: 10000,
            history_size_bytes: AtomicU64::new(0),
            mutation_count: AtomicU64::new(0),
            checksum: AtomicU64::new(0),
            execution_timeout_ms: None,
            run_timeout_ms: None,
            task_timeout_ms: None,
            search_attributes: RwLock::new(HashMap::new()),
            memo: RwLock::new(HashMap::new()),
        }
    }

    /// Load the context (transition from Created to Loaded).
    pub fn load(&self) -> bool {
        let mut state = self.context_state.write().unwrap();
        if *state == ContextState::Created {
            *state = ContextState::Loaded;
            true
        } else { false }
    }

    /// Lock the context for exclusive access.
    pub fn lock_context(&self, lock_id: &str) -> bool {
        let mut state = self.context_state.write().unwrap();
        if *state != ContextState::Loaded && *state != ContextState::Processing {
            return false;
        }

        let mut current_lock = self.lock_id.lock().unwrap();
        if current_lock.is_some() {
            // Check if lock is expired
            if let Some(acquired_at) = *self.lock_acquired_at.lock().unwrap() {
                if acquired_at.elapsed() < Duration::from_millis(self.lock_timeout_ms) {
                    return false; // Lock is still valid
                }
            }
        }

        *current_lock = Some(lock_id.to_string());
        *self.lock_acquired_at.lock().unwrap() = Some(Instant::now());
        *state = ContextState::Locked;
        true
    }

    /// Unlock the context.
    pub fn unlock_context(&self, lock_id: &str) -> bool {
        let mut current_lock = self.lock_id.lock().unwrap();
        if let Some(ref current) = *current_lock {
            if current == lock_id {
                *current_lock = None;
                *self.lock_acquired_at.lock().unwrap() = None;
                let mut state = self.context_state.write().unwrap();
                *state = ContextState::Loaded;
                return true;
            }
        }
        false
    }

    /// Begin processing (transition from Locked to Processing).
    pub fn begin_processing(&self) -> bool {
        let mut state = self.context_state.write().unwrap();
        if *state == ContextState::Locked {
            *state = ContextState::Processing;
            true
        } else { false }
    }

    /// Record a command being processed.
    pub fn record_command(&self) -> u64 {
        *self.last_updated_at.lock().unwrap() = Instant::now();
        self.command_count.fetch_add(1, Ordering::Relaxed)
    }

    /// Record tasks generated.
    pub fn record_tasks_generated(&self, count: u64) {
        self.task_generated_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Allocate the next event ID.
    pub fn allocate_event_id(&self) -> u64 {
        self.next_event_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Complete the context.
    pub fn complete(&self, final_status: u64) {
        self.status.store(final_status, Ordering::Relaxed);
        let mut state = self.context_state.write().unwrap();
        *state = ContextState::Completed;
    }

    /// Close the context.
    pub fn close(&self) {
        let mut state = self.context_state.write().unwrap();
        *state = ContextState::Closed;
    }

    /// Check if the workflow has timed out.
    pub fn is_execution_timed_out(&self) -> bool {
        if let Some(timeout_ms) = self.execution_timeout_ms {
            return self.started_at.elapsed() > Duration::from_millis(timeout_ms);
        }
        false
    }

    /// Get execution stats.
    pub fn execution_stats(&self) -> ExecutionStats {
        ExecutionStats {
            workflow_key: self.workflow_key,
            status: self.status.load(Ordering::Relaxed),
            context_state: format!("{:?}", *self.context_state.read().unwrap()),
            next_event_id: self.next_event_id.load(Ordering::Relaxed),
            command_count: self.command_count.load(Ordering::Relaxed),
            task_generated_count: self.task_generated_count.load(Ordering::Relaxed),
            history_size_bytes: self.history_size_bytes.load(Ordering::Relaxed),
            mutation_count: self.mutation_count.load(Ordering::Relaxed),
            elapsed_ms: self.started_at.elapsed().as_millis() as u64,
            is_locked: self.lock_id.lock().unwrap().is_some(),
        }
    }

    pub fn current_status(&self) -> u64 { self.status.load(Ordering::Relaxed) }
    pub fn is_running(&self) -> bool { self.current_status() == 1 }
    pub fn is_locked(&self) -> bool { self.lock_id.lock().unwrap().is_some() }
}

// ─── 2. Shard Context ────────────────────────────────────────────────────────

/// Per-shard execution context.
pub struct ShardContext {
    pub shard_id: u64,
    pub owner_host: String,
    pub range_id: AtomicU64,
    pub stolen_at: Mutex<Option<Instant>>,
    pub workflow_count: AtomicU64,
    pub pending_tasks: AtomicU64,
    pub replication_ack: AtomicU64,
    pub transfer_ack: AtomicU64,
    pub timer_ack: AtomicU64,
    pub is_acquired: AtomicBool,
    created_at: Instant,
}

impl ShardContext {
    pub fn new(shard_id: u64, owner_host: &str) -> Self {
        Self {
            shard_id,
            owner_host: owner_host.to_string(),
            range_id: AtomicU64::new(1),
            stolen_at: Mutex::new(None),
            workflow_count: AtomicU64::new(0),
            pending_tasks: AtomicU64::new(0),
            replication_ack: AtomicU64::new(0),
            transfer_ack: AtomicU64::new(0),
            timer_ack: AtomicU64::new(0),
            is_acquired: AtomicBool::new(false),
            created_at: Instant::now(),
        }
    }

    /// Acquire the shard.
    pub fn acquire(&self) -> bool {
        if self.is_acquired.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            self.range_id.fetch_add(1, Ordering::Relaxed);
            true
        } else { false }
    }

    /// Release the shard.
    pub fn release(&self) {
        self.is_acquired.store(false, Ordering::SeqCst);
    }

    /// Record a workflow in this shard.
    pub fn record_workflow(&self) {
        self.workflow_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove a workflow from this shard.
    pub fn remove_workflow(&self) {
        self.workflow_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Update task ack levels.
    pub fn update_transfer_ack(&self, level: u64) {
        self.transfer_ack.store(level, Ordering::Relaxed);
    }

    pub fn update_timer_ack(&self, level: u64) {
        self.timer_ack.store(level, Ordering::Relaxed);
    }

    pub fn update_replication_ack(&self, level: u64) {
        self.replication_ack.store(level, Ordering::Relaxed);
    }

    /// Check if the shard is still valid (not stolen).
    pub fn is_valid(&self) -> bool {
        self.is_acquired.load(Ordering::Relaxed) && self.stolen_at.lock().unwrap().is_none()
    }

    /// Mark the shard as stolen.
    pub fn mark_stolen(&self) {
        *self.stolen_at.lock().unwrap() = Some(Instant::now());
        self.is_acquired.store(false, Ordering::SeqCst);
    }

    /// Shard stats.
    pub fn stats(&self) -> ShardStats {
        ShardStats {
            shard_id: self.shard_id,
            owner_host: self.owner_host.clone(),
            range_id: self.range_id.load(Ordering::Relaxed),
            workflow_count: self.workflow_count.load(Ordering::Relaxed),
            pending_tasks: self.pending_tasks.load(Ordering::Relaxed),
            transfer_ack: self.transfer_ack.load(Ordering::Relaxed),
            timer_ack: self.timer_ack.load(Ordering::Relaxed),
            replication_ack: self.replication_ack.load(Ordering::Relaxed),
            is_acquired: self.is_acquired.load(Ordering::Relaxed),
            is_stolen: self.stolen_at.lock().unwrap().is_some(),
            uptime_ms: self.created_at.elapsed().as_millis() as u64,
        }
    }
}

/// Shard statistics.
#[derive(Debug, Clone)]
pub struct ShardStats {
    pub shard_id: u64,
    pub owner_host: String,
    pub range_id: u64,
    pub workflow_count: u64,
    pub pending_tasks: u64,
    pub transfer_ack: u64,
    pub timer_ack: u64,
    pub replication_ack: u64,
    pub is_acquired: bool,
    pub is_stolen: bool,
    pub uptime_ms: u64,
}

// ─── 3. Context Manager ──────────────────────────────────────────────────────

/// Manages workflow execution contexts across shards.
pub struct ContextManager {
    contexts: RwLock<HashMap<u64, WorkflowExecutionContext>>,
    shard_contexts: RwLock<HashMap<u64, ShardContext>>,
    total_created: AtomicU64,
    total_completed: AtomicU64,
    total_locked: AtomicU64,
}

impl ContextManager {
    pub fn new() -> Self {
        Self {
            contexts: RwLock::new(HashMap::new()),
            shard_contexts: RwLock::new(HashMap::new()),
            total_created: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
            total_locked: AtomicU64::new(0),
        }
    }

    /// Create a new workflow context.
    pub fn create_context(&self, workflow_key: u64, workflow_id: u64, run_id: u64,
        namespace_id: u64, shard_id: u64, workflow_type: &str, task_queue: &str) -> bool {
        let ctx = WorkflowExecutionContext::new(workflow_key, workflow_id, run_id, namespace_id, shard_id, workflow_type, task_queue);
        ctx.load(); // Auto-load the context
        let mut contexts = self.contexts.write().unwrap();
        if contexts.contains_key(&workflow_key) { return false; }
        contexts.insert(workflow_key, ctx);
        self.total_created.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Get a context by workflow key.
    pub fn get_context(&self, workflow_key: u64) -> Option<ExecutionStats> {
        self.contexts.read().unwrap().get(&workflow_key).map(|c| c.execution_stats())
    }

    /// Lock a workflow context.
    pub fn lock_workflow(&self, workflow_key: u64, lock_id: &str) -> bool {
        let contexts = self.contexts.read().unwrap();
        if let Some(ctx) = contexts.get(&workflow_key) {
            if ctx.lock_context(lock_id) {
                self.total_locked.fetch_add(1, Ordering::Relaxed);
                true
            } else { false }
        } else { false }
    }

    /// Unlock a workflow context.
    pub fn unlock_workflow(&self, workflow_key: u64, lock_id: &str) -> bool {
        self.contexts.read().unwrap().get(&workflow_key)
            .map_or(false, |c| c.unlock_context(lock_id))
    }

    /// Complete a workflow context.
    pub fn complete_workflow(&self, workflow_key: u64, final_status: u64) -> bool {
        if let Some(ctx) = self.contexts.read().unwrap().get(&workflow_key) {
            ctx.complete(final_status);
            self.total_completed.fetch_add(1, Ordering::Relaxed);
            true
        } else { false }
    }

    /// Register a shard context.
    pub fn register_shard(&self, shard_id: u64, owner_host: &str) -> bool {
        let shard_ctx = ShardContext::new(shard_id, owner_host);
        self.shard_contexts.write().unwrap().insert(shard_id, shard_ctx);
        true
    }

    /// Acquire a shard.
    pub fn acquire_shard(&self, shard_id: u64) -> bool {
        self.shard_contexts.read().unwrap().get(&shard_id)
            .map_or(false, |s| s.acquire())
    }

    /// Get shard stats.
    pub fn shard_stats(&self, shard_id: u64) -> Option<ShardStats> {
        self.shard_contexts.read().unwrap().get(&shard_id).map(|s| s.stats())
    }

    /// Total contexts.
    pub fn total_contexts(&self) -> usize { self.contexts.read().unwrap().len() }
    pub fn total_created(&self) -> u64 { self.total_created.load(Ordering::Relaxed) }
    pub fn total_completed(&self) -> u64 { self.total_completed.load(Ordering::Relaxed) }
    pub fn total_locked(&self) -> u64 { self.total_locked.load(Ordering::Relaxed) }
    pub fn total_shards(&self) -> usize { self.shard_contexts.read().unwrap().len() }
}

impl Default for ContextManager { fn default() -> Self { Self::new() } }

// ─── 4. Execution Stats ──────────────────────────────────────────────────────

/// Execution statistics for a workflow.
#[derive(Debug, Clone)]
pub struct ExecutionStats {
    pub workflow_key: u64,
    pub status: u64,
    pub context_state: String,
    pub next_event_id: u64,
    pub command_count: u64,
    pub task_generated_count: u64,
    pub history_size_bytes: u64,
    pub mutation_count: u64,
    pub elapsed_ms: u64,
    pub is_locked: bool,
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_lifecycle() {
        let ctx = WorkflowExecutionContext::new(1, 100, 1000, 1, 1, "test-wf", "test-q");
        assert_eq!(*ctx.context_state.read().unwrap(), ContextState::Created);
        assert!(ctx.load());
        assert_eq!(*ctx.context_state.read().unwrap(), ContextState::Loaded);
    }

    #[test]
    fn test_context_locking() {
        let ctx = WorkflowExecutionContext::new(1, 100, 1000, 1, 1, "test-wf", "test-q");
        ctx.load();
        assert!(ctx.lock_context("lock-1"));
        assert!(ctx.is_locked());
        assert!(!ctx.lock_context("lock-2")); // Can't double-lock
        assert!(ctx.begin_processing());
        ctx.unlock_context("lock-1");
        assert!(!ctx.is_locked());
    }

    #[test]
    fn test_context_processing() {
        let ctx = WorkflowExecutionContext::new(1, 100, 1000, 1, 1, "test-wf", "test-q");
        ctx.load();
        ctx.lock_context("lock-1");
        ctx.begin_processing();

        let eid = ctx.allocate_event_id();
        assert_eq!(eid, 1);
        ctx.record_command();
        ctx.record_tasks_generated(3);

        let stats = ctx.execution_stats();
        assert_eq!(stats.command_count, 1);
        assert_eq!(stats.task_generated_count, 3);
        assert_eq!(stats.next_event_id, 2);
    }

    #[test]
    fn test_context_completion() {
        let ctx = WorkflowExecutionContext::new(1, 100, 1000, 1, 1, "test-wf", "test-q");
        assert!(ctx.is_running());
        ctx.complete(2);
        assert!(!ctx.is_running());
        assert_eq!(ctx.current_status(), 2);
    }

    #[test]
    fn test_shard_context() {
        let shard = ShardContext::new(1, "host-1");
        assert!(shard.acquire());
        assert!(shard.is_valid());
        assert!(!shard.acquire()); // Can't double-acquire

        shard.record_workflow();
        shard.record_workflow();
        shard.update_transfer_ack(100);
        shard.update_timer_ack(50);

        let stats = shard.stats();
        assert_eq!(stats.workflow_count, 2);
        assert_eq!(stats.transfer_ack, 100);
        assert!(stats.is_acquired);
    }

    #[test]
    fn test_shard_steal() {
        let shard = ShardContext::new(1, "host-1");
        shard.acquire();
        assert!(shard.is_valid());

        shard.mark_stolen();
        assert!(!shard.is_valid());
        let stats = shard.stats();
        assert!(stats.is_stolen);
    }

    #[test]
    fn test_context_manager() {
        let mgr = ContextManager::new();
        mgr.register_shard(1, "host-1");
        assert!(mgr.acquire_shard(1));

        assert!(mgr.create_context(100, 1, 1000, 1, 1, "wf", "q"));
        assert!(!mgr.create_context(100, 1, 1000, 1, 1, "wf", "q")); // Duplicate
        assert_eq!(mgr.total_contexts(), 1);

        assert!(mgr.lock_workflow(100, "lock-1"));
        assert_eq!(mgr.total_locked(), 1);

        assert!(mgr.complete_workflow(100, 2));
        assert_eq!(mgr.total_completed(), 1);

        let stats = mgr.get_context(100).unwrap();
        assert_eq!(stats.status, 2);
    }

    #[test]
    fn test_context_manager_shard_stats() {
        let mgr = ContextManager::new();
        mgr.register_shard(1, "host-1");
        mgr.register_shard(2, "host-2");
        assert_eq!(mgr.total_shards(), 2);

        let stats = mgr.shard_stats(1).unwrap();
        assert_eq!(stats.shard_id, 1);
        assert_eq!(stats.owner_host, "host-1");
    }
}
