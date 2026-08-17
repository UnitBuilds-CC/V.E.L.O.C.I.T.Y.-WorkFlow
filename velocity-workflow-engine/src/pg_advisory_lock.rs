//! PostgreSQL advisory locking for multi-instance coordination.
//!
//! When multiple Velocity server instances share a single PostgreSQL database,
//! advisory locks prevent conflicting operations:
//!
//! - **Leader election**: Exactly one instance runs periodic tasks (cleanup, archival).
//! - **Workflow locking**: Only one instance processes a given workflow at a time.
//! - **Migration locking**: Only one instance runs schema migrations at startup.
//!
//! Uses PostgreSQL's `pg_try_advisory_lock()` (non-blocking) and
//! `pg_advisory_lock()` (blocking) primitives. Locks are session-level and
//! automatically released when the connection closes.
//!
//! # Lock Key Space
//!
//! We partition the 64-bit advisory lock key space:
//! - `0xV...0` range: Leader election (one per role)
//! - `0xV...1` range: Workflow processing (one per workflow_key)
//! - `0xV...2` range: Schema migrations (one global)
//!
//! # Contention Handling
//!
//! Under high contention (many instances competing for the same lock),
//! the backoff strategy is:
//! 1. Try non-blocking `pg_try_advisory_lock()`
//! 2. If failed, sleep with exponential backoff + jitter
//! 3. Retry up to `max_retries` times
//! 4. Return `LockError::ContentionTimeout` if all retries exhausted

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

// ═══════════════════════════════════════════════════════════════════════════════
// Lock Key Constants
// ═══════════════════════════════════════════════════════════════════════════════

/// Base key for leader election locks (0xVE00_0000_0000_0000)
const LEADER_ELECTION_BASE: i64 = 0x5645_0000_0000_0000u64 as i64;

/// Base key for workflow processing locks
const WORKFLOW_LOCK_BASE: i64 = 0x5645_1000_0000_0000u64 as i64;

/// Key for schema migration lock (single global lock)
pub const MIGRATION_LOCK_KEY: i64 = 0x5645_2000_0000_0000u64 as i64;

/// Compute the advisory lock key for a specific leader election role.
pub fn leader_election_key(role: &str) -> i64 {
    let hash = simple_hash(role);
    LEADER_ELECTION_BASE.wrapping_add(hash & 0xFFFF)
}

/// Compute the advisory lock key for a specific workflow.
pub fn workflow_lock_key(workflow_key: u64) -> i64 {
    WORKFLOW_LOCK_BASE.wrapping_add(workflow_key as i64)
}

/// Simple deterministic hash for string -> i64 mapping.
fn simple_hash(s: &str) -> i64 {
    let mut h: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3); // FNV-1a prime
    }
    h as i64
}

// ═══════════════════════════════════════════════════════════════════════════════
// Advisory Lock Backend Trait
// ═══════════════════════════════════════════════════════════════════════════════

/// Result of trying to acquire an advisory lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdvisoryLockResult {
    /// Lock acquired successfully.
    Acquired,
    /// Lock was already held by another session.
    AlreadyLocked,
    /// Failed to communicate with the database.
    DatabaseError(String),
}

/// Abstraction over the PG connection for advisory lock operations.
/// This allows testing without a live database.
pub trait AdvisoryLockBackend: Send + Sync {
    /// Try to acquire an exclusive advisory lock (non-blocking).
    fn try_advisory_lock(&self, key: i64) -> AdvisoryLockResult;

    /// Release an advisory lock.
    fn advisory_unlock(&self, key: i64) -> AdvisoryLockResult;

    /// Release all advisory locks held by this session.
    fn advisory_unlock_all(&self) -> AdvisoryLockResult;

    /// Check if a specific advisory lock is currently held (by any session).
    fn is_lock_held(&self, key: i64) -> bool;

    /// Get the backend name for diagnostics.
    fn backend_name(&self) -> &str;
}

// ═══════════════════════════════════════════════════════════════════════════════
// In-Memory Backend (for testing)
// ═══════════════════════════════════════════════════════════════════════════════

/// Shared lock state simulating a single PostgreSQL database.
/// Multiple backends (sessions) share this state, just like multiple
/// connections to the same PG database share advisory lock state.
pub type SharedLockState = Arc<RwLock<HashMap<i64, String>>>;

/// Create a new shared lock state for testing multi-instance scenarios.
pub fn new_shared_lock_state() -> SharedLockState {
    Arc::new(RwLock::new(HashMap::new()))
}

/// In-memory advisory lock backend for unit testing.
/// Simulates PostgreSQL advisory lock semantics without a real database.
/// Multiple backends can share the same `SharedLockState` to simulate
/// multiple PG sessions connecting to the same database.
pub struct InMemoryAdvisoryBackend {
    /// Shared lock state (simulates the PG database's advisory lock table)
    shared_locks: SharedLockState,
    /// This backend's owner ID (simulates a PG session)
    owner_id: String,
    /// Stats
    lock_attempts: AtomicU64,
    lock_failures: AtomicU64,
}

impl InMemoryAdvisoryBackend {
    /// Create a new backend with its own private lock state (single-instance tests).
    pub fn new(owner_id: &str) -> Self {
        Self::with_shared_state(new_shared_lock_state(), owner_id)
    }

    /// Create a new backend sharing lock state with other backends (multi-instance tests).
    pub fn with_shared_state(shared: SharedLockState, owner_id: &str) -> Self {
        Self {
            shared_locks: shared,
            owner_id: owner_id.to_string(),
            lock_attempts: AtomicU64::new(0),
            lock_failures: AtomicU64::new(0),
        }
    }

    pub fn lock_attempts(&self) -> u64 {
        self.lock_attempts.load(Ordering::Relaxed)
    }

    pub fn lock_failures(&self) -> u64 {
        self.lock_failures.load(Ordering::Relaxed)
    }

    pub fn held_lock_count(&self) -> usize {
        self.shared_locks.read().unwrap().len()
    }
}

impl AdvisoryLockBackend for InMemoryAdvisoryBackend {
    fn try_advisory_lock(&self, key: i64) -> AdvisoryLockResult {
        self.lock_attempts.fetch_add(1, Ordering::Relaxed);
        let mut locks = self.shared_locks.write().unwrap();
        if let Some(existing_owner) = locks.get(&key) {
            if existing_owner == &self.owner_id {
                // Same owner can re-entrant lock
                return AdvisoryLockResult::Acquired;
            }
            self.lock_failures.fetch_add(1, Ordering::Relaxed);
            return AdvisoryLockResult::AlreadyLocked;
        }
        locks.insert(key, self.owner_id.clone());
        AdvisoryLockResult::Acquired
    }

    fn advisory_unlock(&self, key: i64) -> AdvisoryLockResult {
        let mut locks = self.shared_locks.write().unwrap();
        if let Some(owner) = locks.get(&key) {
            if owner == &self.owner_id {
                locks.remove(&key);
                return AdvisoryLockResult::Acquired;
            }
            return AdvisoryLockResult::AlreadyLocked;
        }
        AdvisoryLockResult::AlreadyLocked
    }

    fn advisory_unlock_all(&self) -> AdvisoryLockResult {
        let mut locks = self.shared_locks.write().unwrap();
        locks.retain(|_, owner| owner != &self.owner_id);
        AdvisoryLockResult::Acquired
    }

    fn is_lock_held(&self, key: i64) -> bool {
        self.shared_locks.read().unwrap().contains_key(&key)
    }

    fn backend_name(&self) -> &str {
        "in-memory"
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PG Advisory Lock Manager
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for the advisory lock manager.
#[derive(Debug, Clone)]
pub struct AdvisoryLockConfig {
    /// Maximum number of retry attempts when contention occurs.
    pub max_retries: u32,
    /// Initial backoff duration before retrying.
    pub initial_backoff: Duration,
    /// Maximum backoff duration (exponential growth caps here).
    pub max_backoff: Duration,
    /// Whether to add random jitter to backoff (recommended for multi-instance).
    pub jitter: bool,
    /// How often to renew the leader heartbeat (if leader).
    pub leader_heartbeat_interval: Duration,
    /// Leader lease duration. If no heartbeat within this period, leadership expires.
    pub leader_lease_duration: Duration,
}

impl Default for AdvisoryLockConfig {
    fn default() -> Self {
        Self {
            max_retries: 10,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            jitter: true,
            leader_heartbeat_interval: Duration::from_secs(10),
            leader_lease_duration: Duration::from_secs(30),
        }
    }
}

/// Error type for advisory lock operations.
#[derive(Debug, Clone)]
pub enum AdvisoryLockError {
    /// Lock could not be acquired after all retries.
    ContentionTimeout { key: i64, retries: u32 },
    /// Database communication error.
    DatabaseError(String),
    /// Not the current leader.
    NotLeader { role: String },
    /// Lock already released or never acquired.
    NotHeld { key: i64 },
}

impl std::fmt::Display for AdvisoryLockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ContentionTimeout { key, retries } => {
                write!(f, "advisory lock 0x{:x} timed out after {} retries", *key as u64, retries)
            }
            Self::DatabaseError(msg) => write!(f, "advisory lock DB error: {}", msg),
            Self::NotLeader { role } => write!(f, "not the leader for role '{}'", role),
            Self::NotHeld { key } => write!(f, "advisory lock 0x{:x} not held", *key as u64),
        }
    }
}

impl std::error::Error for AdvisoryLockError {}

/// Statistics for the advisory lock manager.
#[derive(Debug, Default)]
pub struct AdvisoryLockStats {
    pub locks_acquired: AtomicU64,
    pub locks_released: AtomicU64,
    pub lock_contentions: AtomicU64,
    pub lock_timeouts: AtomicU64,
    pub leader_elections_won: AtomicU64,
    pub leader_elections_lost: AtomicU64,
    pub leader_heartbeats: AtomicU64,
}

/// PostgreSQL advisory lock manager for multi-instance coordination.
///
/// Provides:
/// - Advisory lock acquisition with retry + exponential backoff
/// - Leader election with heartbeat-based lease
/// - Workflow-level locking (only one instance processes a workflow)
/// - Migration locking (only one instance runs migrations)
pub struct PgAdvisoryLockManager {
    backend: Arc<dyn AdvisoryLockBackend>,
    config: AdvisoryLockConfig,
    stats: AdvisoryLockStats,
    /// Currently held locks in this manager (key -> acquired_at)
    held_locks: Mutex<HashMap<i64, Instant>>,
    /// Leader roles this manager currently holds (role -> lease_expires_at)
    leader_roles: Mutex<HashMap<String, Instant>>,
}

impl PgAdvisoryLockManager {
    pub fn new(backend: Arc<dyn AdvisoryLockBackend>, config: AdvisoryLockConfig) -> Self {
        Self {
            backend,
            config,
            stats: AdvisoryLockStats::default(),
            held_locks: Mutex::new(HashMap::new()),
            leader_roles: Mutex::new(HashMap::new()),
        }
    }

    /// Try to acquire an advisory lock with retry and exponential backoff.
    pub fn acquire_lock(&self, key: i64) -> Result<(), AdvisoryLockError> {
        let mut backoff = self.config.initial_backoff;

        for attempt in 0..=self.config.max_retries {
            match self.backend.try_advisory_lock(key) {
                AdvisoryLockResult::Acquired => {
                    self.held_locks.lock().unwrap().insert(key, Instant::now());
                    self.stats.locks_acquired.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                AdvisoryLockResult::AlreadyLocked => {
                    self.stats.lock_contentions.fetch_add(1, Ordering::Relaxed);
                    if attempt < self.config.max_retries {
                        let sleep_duration = if self.config.jitter {
                            let jitter_ms = (backoff.as_millis() as u64) / 4;
                            let jitter = simple_jitter(jitter_ms);
                            backoff + Duration::from_millis(jitter)
                        } else {
                            backoff
                        };
                        std::thread::sleep(sleep_duration);
                        backoff = (backoff * 2).min(self.config.max_backoff);
                    }
                }
                AdvisoryLockResult::DatabaseError(msg) => {
                    return Err(AdvisoryLockError::DatabaseError(msg));
                }
            }
        }

        self.stats.lock_timeouts.fetch_add(1, Ordering::Relaxed);
        Err(AdvisoryLockError::ContentionTimeout {
            key,
            retries: self.config.max_retries,
        })
    }

    /// Release an advisory lock.
    pub fn release_lock(&self, key: i64) -> Result<(), AdvisoryLockError> {
        match self.backend.advisory_unlock(key) {
            AdvisoryLockResult::Acquired => {
                self.held_locks.lock().unwrap().remove(&key);
                self.stats.locks_released.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            AdvisoryLockResult::AlreadyLocked => {
                Err(AdvisoryLockError::NotHeld { key })
            }
            AdvisoryLockResult::DatabaseError(msg) => {
                Err(AdvisoryLockError::DatabaseError(msg))
            }
        }
    }

    /// Try to acquire a lock without retry (single attempt).
    pub fn try_lock(&self, key: i64) -> Result<bool, AdvisoryLockError> {
        match self.backend.try_advisory_lock(key) {
            AdvisoryLockResult::Acquired => {
                self.held_locks.lock().unwrap().insert(key, Instant::now());
                self.stats.locks_acquired.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            AdvisoryLockResult::AlreadyLocked => Ok(false),
            AdvisoryLockResult::DatabaseError(msg) => {
                Err(AdvisoryLockError::DatabaseError(msg))
            }
        }
    }

    /// Release all held advisory locks.
    pub fn release_all(&self) -> Result<(), AdvisoryLockError> {
        match self.backend.advisory_unlock_all() {
            AdvisoryLockResult::Acquired => {
                let released = self.held_locks.lock().unwrap().len();
                self.held_locks.lock().unwrap().clear();
                self.leader_roles.lock().unwrap().clear();
                self.stats
                    .locks_released
                    .fetch_add(released as u64, Ordering::Relaxed);
                Ok(())
            }
            AdvisoryLockResult::DatabaseError(msg) => {
                Err(AdvisoryLockError::DatabaseError(msg))
            }
            _ => Ok(()),
        }
    }

    // ─── Leader Election ─────────────────────────────────────────────────

    /// Try to become the leader for a given role.
    /// Returns the lease expiry time if successful.
    pub fn try_acquire_leadership(&self, role: &str) -> Result<Instant, AdvisoryLockError> {
        let key = leader_election_key(role);
        self.acquire_lock(key)?;

        let lease_expires = Instant::now() + self.config.leader_lease_duration;
        self.leader_roles
            .lock()
            .unwrap()
            .insert(role.to_string(), lease_expires);
        self.stats.leader_elections_won.fetch_add(1, Ordering::Relaxed);
        Ok(lease_expires)
    }

    /// Renew the leader lease (heartbeat).
    pub fn heartbeat_leadership(&self, role: &str) -> Result<Instant, AdvisoryLockError> {
        let roles = self.leader_roles.lock().unwrap();
        if !roles.contains_key(role) {
            self.stats.leader_elections_lost.fetch_add(1, Ordering::Relaxed);
            return Err(AdvisoryLockError::NotLeader {
                role: role.to_string(),
            });
        }
        drop(roles);

        // Re-acquire the lock to prove we still hold it
        let key = leader_election_key(role);
        match self.backend.try_advisory_lock(key) {
            AdvisoryLockResult::Acquired => {
                let new_lease = Instant::now() + self.config.leader_lease_duration;
                self.leader_roles
                    .lock()
                    .unwrap()
                    .insert(role.to_string(), new_lease);
                self.stats.leader_heartbeats.fetch_add(1, Ordering::Relaxed);
                Ok(new_lease)
            }
            AdvisoryLockResult::AlreadyLocked => {
                // Someone else took the lock — we lost leadership
                self.leader_roles.lock().unwrap().remove(role);
                self.stats.leader_elections_lost.fetch_add(1, Ordering::Relaxed);
                Err(AdvisoryLockError::NotLeader {
                    role: role.to_string(),
                })
            }
            AdvisoryLockResult::DatabaseError(msg) => {
                Err(AdvisoryLockError::DatabaseError(msg))
            }
        }
    }

    /// Check if this manager is currently the leader for a role.
    pub fn is_leader(&self, role: &str) -> bool {
        let roles = self.leader_roles.lock().unwrap();
        if let Some(lease_expires) = roles.get(role) {
            Instant::now() < *lease_expires
        } else {
            false
        }
    }

    /// Relinquish leadership for a role.
    pub fn relinquish_leadership(&self, role: &str) -> Result<(), AdvisoryLockError> {
        let key = leader_election_key(role);
        self.leader_roles.lock().unwrap().remove(role);
        self.release_lock(key)
    }

    // ─── Workflow Locking ────────────────────────────────────────────────

    /// Try to lock a workflow for processing (non-blocking).
    /// Returns true if this instance now owns the workflow lock.
    pub fn try_lock_workflow(&self, workflow_key: u64) -> Result<bool, AdvisoryLockError> {
        let key = workflow_lock_key(workflow_key);
        self.try_lock(key)
    }

    /// Lock a workflow for processing (with retry).
    pub fn lock_workflow(&self, workflow_key: u64) -> Result<(), AdvisoryLockError> {
        let key = workflow_lock_key(workflow_key);
        self.acquire_lock(key)
    }

    /// Release a workflow lock.
    pub fn unlock_workflow(&self, workflow_key: u64) -> Result<(), AdvisoryLockError> {
        let key = workflow_lock_key(workflow_key);
        self.release_lock(key)
    }

    // ─── Migration Locking ───────────────────────────────────────────────

    /// Try to acquire the migration lock (prevents concurrent schema migrations).
    pub fn try_lock_migrations(&self) -> Result<bool, AdvisoryLockError> {
        self.try_lock(MIGRATION_LOCK_KEY)
    }

    /// Acquire the migration lock (with retry).
    pub fn lock_migration(&self) -> Result<(), AdvisoryLockError> {
        self.acquire_lock(MIGRATION_LOCK_KEY)
    }

    /// Release the migration lock.
    pub fn unlock_migration(&self) -> Result<(), AdvisoryLockError> {
        self.release_lock(MIGRATION_LOCK_KEY)
    }

    // ─── Stats ───────────────────────────────────────────────────────────

    /// Get a snapshot of the lock manager statistics.
    pub fn stats(&self) -> AdvisoryLockStatsSnapshot {
        AdvisoryLockStatsSnapshot {
            locks_acquired: self.stats.locks_acquired.load(Ordering::Relaxed),
            locks_released: self.stats.locks_released.load(Ordering::Relaxed),
            lock_contentions: self.stats.lock_contentions.load(Ordering::Relaxed),
            lock_timeouts: self.stats.lock_timeouts.load(Ordering::Relaxed),
            leader_elections_won: self.stats.leader_elections_won.load(Ordering::Relaxed),
            leader_elections_lost: self.stats.leader_elections_lost.load(Ordering::Relaxed),
            leader_heartbeats: self.stats.leader_heartbeats.load(Ordering::Relaxed),
            currently_held_locks: self.held_locks.lock().unwrap().len(),
            currently_leader: self.leader_roles.lock().unwrap().len(),
        }
    }

    /// Get the backend reference.
    pub fn backend(&self) -> &Arc<dyn AdvisoryLockBackend> {
        &self.backend
    }

    /// Get the config reference.
    pub fn config(&self) -> &AdvisoryLockConfig {
        &self.config
    }
}

/// Snapshot of advisory lock statistics.
#[derive(Debug, Clone)]
pub struct AdvisoryLockStatsSnapshot {
    pub locks_acquired: u64,
    pub locks_released: u64,
    pub lock_contentions: u64,
    pub lock_timeouts: u64,
    pub leader_elections_won: u64,
    pub leader_elections_lost: u64,
    pub leader_heartbeats: u64,
    pub currently_held_locks: usize,
    pub currently_leader: usize,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Row-Level Workflow Lock (PG SELECT FOR UPDATE)
// ═══════════════════════════════════════════════════════════════════════════════

/// Row-level locking strategy for workflow records under contention.
///
/// When multiple instances try to process the same workflow, we use
/// PostgreSQL's `SELECT ... FOR UPDATE SKIP LOCKED` to ensure only one
/// instance processes each workflow at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLockStrategy {
    /// `FOR UPDATE` — block until the row is available.
    BlockAndWait,
    /// `FOR UPDATE SKIP LOCKED` — skip rows locked by other transactions.
    SkipLocked,
    /// `FOR UPDATE NOWAIT` — fail immediately if row is locked.
    NoWait,
    /// `FOR SHARE` — allow concurrent reads but block writes.
    ShareLock,
}

impl RowLockStrategy {
    /// Get the SQL clause for this locking strategy.
    pub fn sql_clause(&self) -> &'static str {
        match self {
            Self::BlockAndWait => "FOR UPDATE",
            Self::SkipLocked => "FOR UPDATE SKIP LOCKED",
            Self::NoWait => "FOR UPDATE NOWAIT",
            Self::ShareLock => "FOR SHARE",
        }
    }
}

/// Generates SQL for row-level locked workflow queries.
pub struct WorkflowRowLock;

impl WorkflowRowLock {
    /// Generate a SELECT query for a workflow with row-level locking.
    pub fn select_workflow_locked(
        workflow_key: u64,
        strategy: RowLockStrategy,
    ) -> String {
        format!(
            "SELECT workflow_key, status, input, created_at, updated_at \
             FROM workflows WHERE workflow_key = {} {}",
            workflow_key,
            strategy.sql_clause()
        )
    }

    /// Generate a query to claim the next pending workflow (for queue processing).
    /// Uses SKIP LOCKED so multiple instances can each claim different workflows.
    pub fn claim_next_pending_workflow(strategy: RowLockStrategy) -> String {
        format!(
            "SELECT workflow_key, status, input, created_at, updated_at \
             FROM workflows \
             WHERE status = 'pending' \
             ORDER BY created_at ASC \
             LIMIT 1 {}",
            strategy.sql_clause()
        )
    }

    /// Generate a query to claim a batch of pending workflows.
    pub fn claim_batch_pending_workflows(batch_size: u32, strategy: RowLockStrategy) -> String {
        format!(
            "SELECT workflow_key, status, input, created_at, updated_at \
             FROM workflows \
             WHERE status = 'pending' \
             ORDER BY created_at ASC \
             LIMIT {} {}",
            batch_size,
            strategy.sql_clause()
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Utility Functions
// ═══════════════════════════════════════════════════════════════════════════════

/// Simple pseudo-random jitter based on a counter.
/// Returns a value in [0, max_ms).
fn simple_jitter(max_ms: u64) -> u64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .subsec_nanos();
    (nanos as u64) % max_ms.max(1)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn test_backend(id: &str) -> Arc<InMemoryAdvisoryBackend> {
        Arc::new(InMemoryAdvisoryBackend::new(id))
    }

    fn test_manager(id: &str) -> PgAdvisoryLockManager {
        let config = AdvisoryLockConfig {
            max_retries: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            jitter: false,
            leader_heartbeat_interval: Duration::from_secs(1),
            leader_lease_duration: Duration::from_secs(5),
        };
        PgAdvisoryLockManager::new(test_backend(id), config)
    }

    // ─── Basic Lock Tests ────────────────────────────────────────────────

    #[test]
    fn test_acquire_and_release_lock() {
        let mgr = test_manager("instance-1");
        let key = 42;

        mgr.acquire_lock(key).unwrap();
        assert!(mgr.backend().is_lock_held(key));

        mgr.release_lock(key).unwrap();
        assert!(!mgr.backend().is_lock_held(key));

        let stats = mgr.stats();
        assert_eq!(stats.locks_acquired, 1);
        assert_eq!(stats.locks_released, 1);
    }

    #[test]
    fn test_try_lock_returns_bool() {
        let mgr = test_manager("instance-1");
        let key = 100;

        let acquired = mgr.try_lock(key).unwrap();
        assert!(acquired);

        // Same owner (re-entrant in InMemory)
        let acquired2 = mgr.try_lock(key).unwrap();
        assert!(acquired2);

        mgr.release_lock(key).unwrap();
    }

    #[test]
    fn test_contention_between_instances() {
        let shared = new_shared_lock_state();
        let backend1 = Arc::new(InMemoryAdvisoryBackend::with_shared_state(shared.clone(), "instance-1"));
        let config = AdvisoryLockConfig {
            max_retries: 0, // Don't retry — fail immediately
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            jitter: false,
            leader_heartbeat_interval: Duration::from_secs(1),
            leader_lease_duration: Duration::from_secs(5),
        };
        let mgr1 = PgAdvisoryLockManager::new(backend1, config);

        let backend2 = Arc::new(InMemoryAdvisoryBackend::with_shared_state(shared, "instance-2"));
        let mgr2 = PgAdvisoryLockManager::new(backend2, AdvisoryLockConfig {
            max_retries: 0,
            ..AdvisoryLockConfig::default()
        });

        let key = 999;

        // Instance 1 acquires the lock
        mgr1.acquire_lock(key).unwrap();

        // Instance 2 should fail (no retries)
        let result = mgr2.try_lock(key).unwrap();
        assert!(!result, "Instance 2 should not acquire lock held by instance 1");

        let stats2 = mgr2.stats();
        assert_eq!(stats2.lock_contentions, 0); // try_lock doesn't count contention
    }

    #[test]
    fn test_contention_with_retry() {
        let shared = new_shared_lock_state();
        let backend1 = Arc::new(InMemoryAdvisoryBackend::with_shared_state(shared.clone(), "instance-1"));
        let config = AdvisoryLockConfig {
            max_retries: 5,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            jitter: false,
            leader_heartbeat_interval: Duration::from_secs(1),
            leader_lease_duration: Duration::from_secs(5),
        };
        let mgr1 = PgAdvisoryLockManager::new(backend1, config.clone());

        let backend2 = Arc::new(InMemoryAdvisoryBackend::with_shared_state(shared, "instance-2"));
        let mgr2 = PgAdvisoryLockManager::new(backend2, config);

        let key = 888;

        // Instance 1 acquires the lock
        mgr1.acquire_lock(key).unwrap();

        // Instance 2 should timeout after retries
        let result = mgr2.acquire_lock(key);
        assert!(result.is_err());
        match result.unwrap_err() {
            AdvisoryLockError::ContentionTimeout { retries, .. } => {
                assert_eq!(retries, 5);
            }
            other => panic!("Expected ContentionTimeout, got {:?}", other),
        }

        let stats2 = mgr2.stats();
        assert!(stats2.lock_contentions > 0);
        assert_eq!(stats2.lock_timeouts, 1);
    }

    #[test]
    fn test_release_all_locks() {
        let mgr = test_manager("instance-1");

        mgr.acquire_lock(1).unwrap();
        mgr.acquire_lock(2).unwrap();
        mgr.acquire_lock(3).unwrap();

        assert_eq!(mgr.stats().currently_held_locks, 3);

        mgr.release_all().unwrap();
        assert_eq!(mgr.stats().currently_held_locks, 0);
    }

    // ─── Leader Election Tests ───────────────────────────────────────────

    #[test]
    fn test_leader_election_basic() {
        let mgr = test_manager("instance-1");

        let lease = mgr.try_acquire_leadership("scheduler").unwrap();
        assert!(lease > Instant::now());
        assert!(mgr.is_leader("scheduler"));

        mgr.relinquish_leadership("scheduler").unwrap();
        assert!(!mgr.is_leader("scheduler"));
    }

    #[test]
    fn test_leader_heartbeat() {
        let mgr = test_manager("instance-1");

        mgr.try_acquire_leadership("scheduler").unwrap();
        assert!(mgr.is_leader("scheduler"));

        // Heartbeat should succeed and extend the lease
        let new_lease = mgr.heartbeat_leadership("scheduler").unwrap();
        assert!(new_lease > Instant::now());

        let stats = mgr.stats();
        assert_eq!(stats.leader_elections_won, 1);
        assert_eq!(stats.leader_heartbeats, 1);
        assert_eq!(stats.leader_elections_lost, 0);
    }

    #[test]
    fn test_leader_heartbeat_without_leadership() {
        let mgr = test_manager("instance-1");

        // Try to heartbeat without being leader
        let result = mgr.heartbeat_leadership("scheduler");
        assert!(result.is_err());
        match result.unwrap_err() {
            AdvisoryLockError::NotLeader { role } => assert_eq!(role, "scheduler"),
            other => panic!("Expected NotLeader, got {:?}", other),
        }
    }

    #[test]
    fn test_leader_election_contention() {
        let shared = new_shared_lock_state();
        let backend1 = Arc::new(InMemoryAdvisoryBackend::with_shared_state(shared.clone(), "instance-1"));
        let backend2 = Arc::new(InMemoryAdvisoryBackend::with_shared_state(shared, "instance-2"));
        let config = AdvisoryLockConfig {
            max_retries: 0,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            jitter: false,
            leader_heartbeat_interval: Duration::from_secs(1),
            leader_lease_duration: Duration::from_secs(5),
        };

        let mgr1 = PgAdvisoryLockManager::new(backend1, config.clone());
        let mgr2 = PgAdvisoryLockManager::new(backend2, config);

        // Instance 1 becomes leader
        mgr1.try_acquire_leadership("worker").unwrap();
        assert!(mgr1.is_leader("worker"));

        // Instance 2 should fail
        let result = mgr2.try_acquire_leadership("worker");
        assert!(result.is_err());
        assert!(!mgr2.is_leader("worker"));
    }

    #[test]
    fn test_multiple_leader_roles() {
        let mgr = test_manager("instance-1");

        // Can be leader for multiple roles simultaneously
        mgr.try_acquire_leadership("scheduler").unwrap();
        mgr.try_acquire_leadership("archiver").unwrap();
        mgr.try_acquire_leadership("cleaner").unwrap();

        assert!(mgr.is_leader("scheduler"));
        assert!(mgr.is_leader("archiver"));
        assert!(mgr.is_leader("cleaner"));
        assert_eq!(mgr.stats().currently_leader, 3);
        assert_eq!(mgr.stats().leader_elections_won, 3);
    }

    // ─── Workflow Locking Tests ──────────────────────────────────────────

    #[test]
    fn test_workflow_lock_unlock() {
        let mgr = test_manager("instance-1");

        let acquired = mgr.try_lock_workflow(12345).unwrap();
        assert!(acquired);

        mgr.unlock_workflow(12345).unwrap();
    }

    #[test]
    fn test_workflow_lock_contention_between_instances() {
        let shared = new_shared_lock_state();
        let backend1 = Arc::new(InMemoryAdvisoryBackend::with_shared_state(shared.clone(), "instance-1"));
        let backend2 = Arc::new(InMemoryAdvisoryBackend::with_shared_state(shared, "instance-2"));
        let config = AdvisoryLockConfig {
            max_retries: 0,
            ..AdvisoryLockConfig::default()
        };

        let mgr1 = PgAdvisoryLockManager::new(backend1, config.clone());
        let mgr2 = PgAdvisoryLockManager::new(backend2, config);

        // Instance 1 locks workflow 42
        let acquired = mgr1.try_lock_workflow(42).unwrap();
        assert!(acquired);

        // Instance 2 tries to lock same workflow — should fail
        let acquired2 = mgr2.try_lock_workflow(42).unwrap();
        assert!(!acquired2);

        // Instance 2 can lock a different workflow
        let acquired3 = mgr2.try_lock_workflow(43).unwrap();
        assert!(acquired3);
    }

    #[test]
    fn test_workflow_lock_different_workflows_no_contention() {
        let mgr = test_manager("instance-1");

        // Lock multiple different workflows — no contention
        assert!(mgr.try_lock_workflow(1).unwrap());
        assert!(mgr.try_lock_workflow(2).unwrap());
        assert!(mgr.try_lock_workflow(3).unwrap());

        assert_eq!(mgr.stats().currently_held_locks, 3);
    }

    // ─── Migration Lock Tests ────────────────────────────────────────────

    #[test]
    fn test_migration_lock() {
        let mgr = test_manager("instance-1");

        let acquired = mgr.try_lock_migrations().unwrap();
        assert!(acquired);

        mgr.unlock_migration().unwrap();
        assert_eq!(mgr.stats().locks_released, 1);
    }

    #[test]
    fn test_migration_lock_contention() {
        let shared = new_shared_lock_state();
        let backend1 = Arc::new(InMemoryAdvisoryBackend::with_shared_state(shared.clone(), "instance-1"));
        let backend2 = Arc::new(InMemoryAdvisoryBackend::with_shared_state(shared, "instance-2"));
        let config = AdvisoryLockConfig {
            max_retries: 0,
            ..AdvisoryLockConfig::default()
        };

        let mgr1 = PgAdvisoryLockManager::new(backend1, config.clone());
        let mgr2 = PgAdvisoryLockManager::new(backend2, config);

        // Instance 1 acquires migration lock
        assert!(mgr1.try_lock_migrations().unwrap());

        // Instance 2 should fail
        assert!(!mgr2.try_lock_migrations().unwrap());
    }

    // ─── Lock Key Tests ──────────────────────────────────────────────────

    #[test]
    fn test_lock_key_deterministic() {
        let key1 = leader_election_key("scheduler");
        let key2 = leader_election_key("scheduler");
        assert_eq!(key1, key2);

        let key3 = leader_election_key("archiver");
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_workflow_lock_key_unique() {
        let key1 = workflow_lock_key(1);
        let key2 = workflow_lock_key(2);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_lock_key_spaces_dont_overlap() {
        // Leader election keys and workflow keys should be in different ranges
        let leader_key = leader_election_key("test");
        let workflow_key = workflow_lock_key(0);
        assert_ne!(leader_key, workflow_key);

        // Migration key should be unique
        assert_ne!(MIGRATION_LOCK_KEY, leader_key);
        assert_ne!(MIGRATION_LOCK_KEY, workflow_key);
    }

    // ─── Row Lock Strategy Tests ─────────────────────────────────────────

    #[test]
    fn test_row_lock_strategy_sql() {
        assert_eq!(RowLockStrategy::BlockAndWait.sql_clause(), "FOR UPDATE");
        assert_eq!(RowLockStrategy::SkipLocked.sql_clause(), "FOR UPDATE SKIP LOCKED");
        assert_eq!(RowLockStrategy::NoWait.sql_clause(), "FOR UPDATE NOWAIT");
        assert_eq!(RowLockStrategy::ShareLock.sql_clause(), "FOR SHARE");
    }

    #[test]
    fn test_workflow_row_lock_select() {
        let sql = WorkflowRowLock::select_workflow_locked(42, RowLockStrategy::SkipLocked);
        assert!(sql.contains("42"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn test_claim_next_pending_workflow() {
        let sql = WorkflowRowLock::claim_next_pending_workflow(RowLockStrategy::SkipLocked);
        assert!(sql.contains("status = 'pending'"));
        assert!(sql.contains("LIMIT 1"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn test_claim_batch_pending_workflows() {
        let sql = WorkflowRowLock::claim_batch_pending_workflows(10, RowLockStrategy::NoWait);
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("FOR UPDATE NOWAIT"));
    }

    // ─── High Contention Stress Test ─────────────────────────────────────

    #[test]
    fn test_high_contention_many_instances() {
        // Simulate 20 instances all trying to acquire the same lock
        let shared = new_shared_lock_state();
        let config = AdvisoryLockConfig {
            max_retries: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            jitter: false,
            leader_heartbeat_interval: Duration::from_secs(1),
            leader_lease_duration: Duration::from_secs(5),
        };

        let mut managers: Vec<PgAdvisoryLockManager> = (0..20)
            .map(|i| {
                let backend = Arc::new(InMemoryAdvisoryBackend::with_shared_state(
                    shared.clone(),
                    &format!("instance-{}", i),
                ));
                PgAdvisoryLockManager::new(backend, config.clone())
            })
            .collect();

        let key = 7777;

        // First instance gets the lock
        managers[0].acquire_lock(key).unwrap();

        // All other instances should fail (lock is held, low retries)
        let mut acquired_count = 0;
        let mut contention_count = 0;
        for mgr in managers.iter().skip(1) {
            match mgr.acquire_lock(key) {
                Ok(()) => acquired_count += 1,
                Err(AdvisoryLockError::ContentionTimeout { .. }) => contention_count += 1,
                Err(_) => panic!("Unexpected error"),
            }
        }

        // Only instance 0 should hold the lock
        assert_eq!(acquired_count, 0, "No other instance should acquire the lock");
        assert_eq!(contention_count, 19, "All other instances should timeout");

        // Check stats
        for mgr in managers.iter().skip(1) {
            let stats = mgr.stats();
            assert!(stats.lock_contentions > 0);
            assert_eq!(stats.lock_timeouts, 1);
        }
    }

    // ─── Stats Snapshot Test ─────────────────────────────────────────────

    #[test]
    fn test_stats_snapshot() {
        let mgr = test_manager("instance-1");

        mgr.acquire_lock(1).unwrap();
        mgr.acquire_lock(2).unwrap();
        mgr.try_acquire_leadership("scheduler").unwrap();

        let snapshot = mgr.stats();
        assert!(snapshot.locks_acquired >= 3); // 2 locks + 1 leader
        assert!(snapshot.currently_held_locks >= 2);
        assert_eq!(snapshot.currently_leader, 1);
        assert_eq!(snapshot.leader_elections_won, 1);
    }
}
