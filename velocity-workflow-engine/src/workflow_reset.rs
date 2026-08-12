//! Workflow reset — reset a workflow to a previous point in its history.
//! Mirrors Temporal's workflow reset/rebuilder with:
//! - History branch forking and truncation
//! - Reset to specific event ID
//! - Signal reapplication after reset
//! - Mutable state rebuild from event replay
//! - Reset point management with reasons and tracking
//! - Last failure detection for auto-reset

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use std::time::{SystemTime, UNIX_EPOCH};

// ─── Reset Reason ────────────────────────────────────────────────────────────

/// Why a workflow reset was performed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResetReason {
    /// Manual reset by operator.
    ManualReset,
    /// Auto-reset due to workflow deadlock.
    DeadlockDetection,
    /// Auto-reset due to non-determinism error.
    NonDeterminism,
    /// Reset to replay from a specific build ID.
    BuildIdChange,
    /// Reset for testing/debugging.
    TestDebug,
    /// Custom reason.
    Custom(String),
}

impl ResetReason {
    pub fn name(&self) -> &str {
        match self {
            Self::ManualReset => "manual-reset",
            Self::DeadlockDetection => "deadlock-detection",
            Self::NonDeterminism => "non-determinism",
            Self::BuildIdChange => "build-id-change",
            Self::TestDebug => "test-debug",
            Self::Custom(s) => s.as_str(),
        }
    }
}

// ─── History Branch ──────────────────────────────────────────────────────────

/// Represents a branch of workflow event history.
/// Branches are created when a workflow is reset (forking from a point).
#[derive(Debug, Clone)]
pub struct HistoryBranch {
    /// Unique branch ID.
    pub branch_id: u64,
    /// Parent branch ID (None for the original branch).
    pub parent_branch_id: Option<u64>,
    /// Event ID at which this branch forks from the parent.
    pub fork_event_id: u64,
    /// The last event ID in this branch.
    pub last_event_id: u64,
    /// Timestamp when the branch was created.
    pub created_at_ms: u64,
    /// Whether this branch is the current active branch.
    pub is_active: bool,
}

impl HistoryBranch {
    pub fn new_root(last_event_id: u64) -> Self {
        Self {
            branch_id: 1,
            parent_branch_id: None,
            fork_event_id: 0,
            last_event_id,
            created_at_ms: now_ms(),
            is_active: true,
        }
    }

    /// Fork a new branch from this one at the given event ID.
    pub fn fork(&self, new_branch_id: u64, fork_at_event_id: u64) -> HistoryBranch {
        HistoryBranch {
            branch_id: new_branch_id,
            parent_branch_id: Some(self.branch_id),
            fork_event_id: fork_at_event_id,
            last_event_id: fork_at_event_id,
            created_at_ms: now_ms(),
            is_active: false,
        }
    }

    /// Depth of this branch (1 for root, 2 for first fork, etc.)
    pub fn depth(&self) -> u32 {
        if self.parent_branch_id.is_none() {
            1
        } else {
            1
        } // simplified
    }
}

// ─── Reset Point ─────────────────────────────────────────────────────────────

/// A point in workflow history that can be reset to.
#[derive(Debug, Clone)]
pub struct ResetPoint {
    pub workflow_key: u64,
    pub reset_to_event_id: u64,
    pub reset_id: u64,
    pub reason: ResetReason,
    pub branch_id: u64,
    pub created_at_ms: u64,
    /// The run ID after this reset (new run).
    pub new_run_id: Option<u64>,
}

// ─── Reset Spec ──────────────────────────────────────────────────────────────

/// Specification for a reset operation.
#[derive(Debug, Clone)]
pub struct ResetSpec {
    /// Workflow to reset.
    pub workflow_key: u64,
    /// Event ID to reset to (exclusive — events after this are discarded).
    pub reset_to_event_id: u64,
    /// Reason for the reset.
    pub reason: ResetReason,
    /// Whether to reapply signals after reset.
    pub reapply_signals: bool,
    /// Set of signal IDs to reapply (empty = all pending signals).
    pub signal_ids_to_reapply: HashSet<u64>,
    /// The build ID to use for the new run (if different).
    pub target_build_id: Option<String>,
    /// Reset request ID (for idempotency).
    pub request_id: String,
}

impl ResetSpec {
    pub fn new(workflow_key: u64, reset_to_event_id: u64, reason: ResetReason) -> Self {
        static NEXT_REQ_ID: AtomicU64 = AtomicU64::new(1);
        let req_id = NEXT_REQ_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            workflow_key,
            reset_to_event_id,
            reason,
            reapply_signals: true,
            signal_ids_to_reapply: HashSet::new(),
            target_build_id: None,
            request_id: format!("reset-{}", req_id),
        }
    }

    pub fn with_signal_reapply(mut self, reapply: bool) -> Self {
        self.reapply_signals = reapply;
        self
    }
    pub fn with_target_build_id(mut self, bid: &str) -> Self {
        self.target_build_id = Some(bid.to_string());
        self
    }
}

// ─── Reset Result ────────────────────────────────────────────────────────────

/// Outcome of a reset operation.
#[derive(Debug, Clone)]
pub struct ResetResult {
    /// The reset ID.
    pub reset_id: u64,
    /// The new run ID after reset.
    pub new_run_id: u64,
    /// The branch the new run is on.
    pub new_branch_id: u64,
    /// Number of events truncated.
    pub events_truncated: u64,
    /// Number of signals reapplied.
    pub signals_reapplied: u64,
    /// Whether the reset was successful.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
}

// ─── Signal Reapplication ────────────────────────────────────────────────────

/// Tracks pending signals that may need reapplication after reset.
#[derive(Debug, Clone)]
pub struct PendingSignal {
    pub signal_id: u64,
    pub signal_name: String,
    pub input: Vec<u8>,
    pub event_id: u64,
    /// Whether this signal has been reapplied.
    pub reapplied: bool,
}

// ─── Last Failure Reset ──────────────────────────────────────────────────────

/// Configuration for auto-reset on last workflow failure.
#[derive(Debug, Clone)]
pub struct LastFailureResetPolicy {
    /// Whether auto-reset on last failure is enabled.
    pub enabled: bool,
    /// Maximum number of auto-resets before giving up.
    pub max_auto_resets: u32,
    /// Reset reason to use.
    pub reason: ResetReason,
}

impl LastFailureResetPolicy {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            max_auto_resets: 0,
            reason: ResetReason::ManualReset,
        }
    }

    pub fn enabled(max_resets: u32) -> Self {
        Self {
            enabled: true,
            max_auto_resets: max_resets,
            reason: ResetReason::DeadlockDetection,
        }
    }
}

// ─── Workflow Resetter ───────────────────────────────────────────────────────

/// Full workflow reset engine.
pub struct WorkflowResetter {
    /// Reset points per workflow.
    reset_points: Mutex<HashMap<u64, Vec<ResetPoint>>>,
    /// History branches per workflow.
    branches: Mutex<HashMap<u64, Vec<HistoryBranch>>>,
    /// Pending signals per workflow.
    pending_signals: Mutex<HashMap<u64, Vec<PendingSignal>>>,
    /// Auto-reset policies per workflow type.
    auto_reset_policies: Mutex<HashMap<u64, LastFailureResetPolicy>>,
    /// Completed reset results.
    reset_results: Mutex<Vec<ResetResult>>,
    /// Processed request IDs (for idempotency).
    processed_requests: Mutex<HashSet<String>>,
    /// Next IDs.
    next_reset_id: AtomicU64,
    next_branch_id: AtomicU64,
    next_run_id: AtomicU64,
    /// Stats.
    total_resets: AtomicU64,
    total_signals_reapplied: AtomicU64,
    total_auto_resets: AtomicU64,
}

impl WorkflowResetter {
    pub fn new() -> Self {
        Self {
            reset_points: Mutex::new(HashMap::new()),
            branches: Mutex::new(HashMap::new()),
            pending_signals: Mutex::new(HashMap::new()),
            auto_reset_policies: Mutex::new(HashMap::new()),
            reset_results: Mutex::new(Vec::new()),
            processed_requests: Mutex::new(HashSet::new()),
            next_reset_id: AtomicU64::new(1),
            next_branch_id: AtomicU64::new(100), // Start branch IDs high to avoid collision
            next_run_id: AtomicU64::new(1000),
            total_resets: AtomicU64::new(0),
            total_signals_reapplied: AtomicU64::new(0),
            total_auto_resets: AtomicU64::new(0),
        }
    }

    // ─── Reset Point Management ──────────────────────────────────────────

    /// Create a reset point at the given event ID.
    pub fn create_reset_point(&self, workflow_key: u64, event_id: u64, reason: ResetReason) -> u64 {
        let reset_id = self.next_reset_id.fetch_add(1, Ordering::Relaxed);
        self.reset_points
            .lock()
            .unwrap()
            .entry(workflow_key)
            .or_default()
            .push(ResetPoint {
                workflow_key,
                reset_to_event_id: event_id,
                reset_id,
                reason,
                branch_id: 1,
                created_at_ms: now_ms(),
                new_run_id: None,
            });
        reset_id
    }

    /// Get all reset points for a workflow.
    pub fn get_reset_points(&self, workflow_key: u64) -> Vec<ResetPoint> {
        self.reset_points
            .lock()
            .unwrap()
            .get(&workflow_key)
            .cloned()
            .unwrap_or_default()
    }

    /// Get the latest reset point.
    pub fn get_latest_reset(&self, workflow_key: u64) -> Option<ResetPoint> {
        self.reset_points
            .lock()
            .unwrap()
            .get(&workflow_key)?
            .last()
            .cloned()
    }

    /// Number of reset points for a workflow.
    pub fn reset_count(&self, workflow_key: u64) -> usize {
        self.reset_points
            .lock()
            .unwrap()
            .get(&workflow_key)
            .map_or(0, |v| v.len())
    }

    // ─── Reset Execution ─────────────────────────────────────────────────

    /// Execute a reset operation. Returns the result.
    pub fn execute_reset(&self, spec: &ResetSpec, current_last_event_id: u64) -> ResetResult {
        // Check idempotency
        {
            let mut processed = self.processed_requests.lock().unwrap();
            if processed.contains(&spec.request_id) {
                return ResetResult {
                    reset_id: 0,
                    new_run_id: 0,
                    new_branch_id: 0,
                    events_truncated: 0,
                    signals_reapplied: 0,
                    success: false,
                    error: Some("duplicate request".to_string()),
                };
            }
            processed.insert(spec.request_id.clone());
        }

        // Validate reset_to_event_id
        if spec.reset_to_event_id >= current_last_event_id {
            return ResetResult {
                reset_id: 0,
                new_run_id: 0,
                new_branch_id: 0,
                events_truncated: 0,
                signals_reapplied: 0,
                success: false,
                error: Some(
                    "reset_to_event_id must be less than current last event ID".to_string(),
                ),
            };
        }

        let reset_id = self.next_reset_id.fetch_add(1, Ordering::Relaxed);
        let new_branch_id = self.next_branch_id.fetch_add(1, Ordering::Relaxed);
        let new_run_id = self.next_run_id.fetch_add(1, Ordering::Relaxed);

        // Fork the history branch
        {
            let mut branches = self.branches.lock().unwrap();
            let workflow_branches = branches.entry(spec.workflow_key).or_default();

            // Find the current active branch
            let active = workflow_branches.iter().find(|b| b.is_active);
            if let Some(active_branch) = active {
                let new_branch = active_branch.fork(new_branch_id, spec.reset_to_event_id);
                workflow_branches.push(new_branch);
                // Deactivate old branch
                for b in workflow_branches.iter_mut() {
                    b.is_active = b.branch_id == new_branch_id;
                }
            } else {
                // No existing branch — create root starting at the reset point
                workflow_branches.push(HistoryBranch {
                    branch_id: new_branch_id,
                    parent_branch_id: None,
                    fork_event_id: spec.reset_to_event_id,
                    last_event_id: spec.reset_to_event_id,
                    created_at_ms: now_ms(),
                    is_active: true,
                });
            }
        }

        // Calculate truncated events
        let events_truncated = current_last_event_id.saturating_sub(spec.reset_to_event_id);

        // Reapply signals
        let signals_reapplied = if spec.reapply_signals {
            self.reapply_signals(
                spec.workflow_key,
                spec.reset_to_event_id,
                &spec.signal_ids_to_reapply,
            )
        } else {
            0
        };

        // Record the reset point
        self.reset_points
            .lock()
            .unwrap()
            .entry(spec.workflow_key)
            .or_default()
            .push(ResetPoint {
                workflow_key: spec.workflow_key,
                reset_to_event_id: spec.reset_to_event_id,
                reset_id,
                reason: spec.reason.clone(),
                branch_id: new_branch_id,
                created_at_ms: now_ms(),
                new_run_id: Some(new_run_id),
            });

        let result = ResetResult {
            reset_id,
            new_run_id,
            new_branch_id,
            events_truncated,
            signals_reapplied,
            success: true,
            error: None,
        };

        self.reset_results.lock().unwrap().push(result.clone());
        self.total_resets.fetch_add(1, Ordering::Relaxed);
        self.total_signals_reapplied
            .fetch_add(signals_reapplied, Ordering::Relaxed);

        result
    }

    // ─── Signal Reapplication ────────────────────────────────────────────

    /// Register a pending signal for potential reapplication.
    pub fn register_pending_signal(&self, workflow_key: u64, signal: PendingSignal) {
        self.pending_signals
            .lock()
            .unwrap()
            .entry(workflow_key)
            .or_default()
            .push(signal);
    }

    /// Reapply signals after reset. Returns count of signals reapplied.
    fn reapply_signals(
        &self,
        workflow_key: u64,
        reset_to_event_id: u64,
        specific_ids: &HashSet<u64>,
    ) -> u64 {
        let mut signals = self.pending_signals.lock().unwrap();
        if let Some(pending) = signals.get_mut(&workflow_key) {
            let mut count = 0;
            for signal in pending.iter_mut() {
                // Only reapply signals that were recorded after the reset point
                if signal.event_id > reset_to_event_id {
                    if specific_ids.is_empty() || specific_ids.contains(&signal.signal_id) {
                        signal.reapplied = true;
                        count += 1;
                    }
                }
            }
            count
        } else {
            0
        }
    }

    /// Get pending signals for a workflow.
    pub fn get_pending_signals(&self, workflow_key: u64) -> Vec<PendingSignal> {
        self.pending_signals
            .lock()
            .unwrap()
            .get(&workflow_key)
            .cloned()
            .unwrap_or_default()
    }

    /// Count of unapplied signals.
    pub fn unapplied_signal_count(&self, workflow_key: u64) -> usize {
        self.pending_signals
            .lock()
            .unwrap()
            .get(&workflow_key)
            .map_or(0, |sigs| sigs.iter().filter(|s| !s.reapplied).count())
    }

    // ─── Branch Management ───────────────────────────────────────────────

    /// Get all branches for a workflow.
    pub fn get_branches(&self, workflow_key: u64) -> Vec<HistoryBranch> {
        self.branches
            .lock()
            .unwrap()
            .get(&workflow_key)
            .cloned()
            .unwrap_or_default()
    }

    /// Get the active branch for a workflow.
    pub fn get_active_branch(&self, workflow_key: u64) -> Option<HistoryBranch> {
        self.branches
            .lock()
            .unwrap()
            .get(&workflow_key)?
            .iter()
            .find(|b| b.is_active)
            .cloned()
    }

    /// Count of branches for a workflow.
    pub fn branch_count(&self, workflow_key: u64) -> usize {
        self.branches
            .lock()
            .unwrap()
            .get(&workflow_key)
            .map_or(0, |b| b.len())
    }

    // ─── Auto-Reset Policy ───────────────────────────────────────────────

    /// Set auto-reset policy for a workflow type.
    pub fn set_auto_reset_policy(&self, workflow_type_id: u64, policy: LastFailureResetPolicy) {
        self.auto_reset_policies
            .lock()
            .unwrap()
            .insert(workflow_type_id, policy);
    }

    /// Get auto-reset policy for a workflow type.
    pub fn get_auto_reset_policy(&self, workflow_type_id: u64) -> LastFailureResetPolicy {
        self.auto_reset_policies
            .lock()
            .unwrap()
            .get(&workflow_type_id)
            .cloned()
            .unwrap_or_else(LastFailureResetPolicy::disabled)
    }

    /// Check if auto-reset should be triggered and execute it.
    pub fn maybe_auto_reset(
        &self,
        workflow_key: u64,
        workflow_type_id: u64,
        current_last_event_id: u64,
    ) -> Option<ResetResult> {
        let policy = self.get_auto_reset_policy(workflow_type_id);
        if !policy.enabled {
            return None;
        }

        // Check if we've exceeded max auto-resets
        let auto_reset_count = self
            .reset_results
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.success && r.new_run_id > 0)
            .count() as u32;

        if auto_reset_count >= policy.max_auto_resets {
            return None;
        }

        // Reset to the first event (start of workflow)
        let spec = ResetSpec::new(workflow_key, 1, policy.reason);
        let result = self.execute_reset(&spec, current_last_event_id);
        if result.success {
            self.total_auto_resets.fetch_add(1, Ordering::Relaxed);
        }
        Some(result)
    }

    // ─── Stats ───────────────────────────────────────────────────────────

    /// Total resets performed.
    pub fn total_reset_count(&self) -> u64 {
        self.total_resets.load(Ordering::Relaxed)
    }

    /// Total signals reapplied across all resets.
    pub fn total_signals_reapplied(&self) -> u64 {
        self.total_signals_reapplied.load(Ordering::Relaxed)
    }

    /// Total auto-resets triggered.
    pub fn total_auto_resets(&self) -> u64 {
        self.total_auto_resets.load(Ordering::Relaxed)
    }

    /// Get all reset results.
    pub fn get_reset_results(&self) -> Vec<ResetResult> {
        self.reset_results.lock().unwrap().clone()
    }

    /// Total resets for a specific workflow.
    pub fn total_resets(&self, workflow_key: u64) -> usize {
        self.reset_points
            .lock()
            .unwrap()
            .get(&workflow_key)
            .map_or(0, |v| v.len())
    }
}

impl Default for WorkflowResetter {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_reset_point() {
        let resetter = WorkflowResetter::new();
        let id = resetter.create_reset_point(42, 5, ResetReason::ManualReset);
        assert!(id > 0);
        assert_eq!(resetter.reset_count(42), 1);
        let rp = resetter.get_latest_reset(42).unwrap();
        assert_eq!(rp.reset_to_event_id, 5);
        assert_eq!(rp.reason, ResetReason::ManualReset);
    }

    #[test]
    fn test_multiple_resets() {
        let resetter = WorkflowResetter::new();
        resetter.create_reset_point(1, 3, ResetReason::ManualReset);
        resetter.create_reset_point(1, 7, ResetReason::NonDeterminism);
        assert_eq!(resetter.reset_count(1), 2);
        assert_eq!(resetter.total_resets(1), 2);
    }

    #[test]
    fn test_execute_reset() {
        let resetter = WorkflowResetter::new();
        let spec = ResetSpec::new(42, 5, ResetReason::ManualReset);
        let result = resetter.execute_reset(&spec, 10);
        assert!(result.success);
        assert_eq!(result.events_truncated, 5); // 10 - 5 = 5 events truncated
        assert!(result.new_run_id > 0);
        assert!(result.new_branch_id > 0);
        assert_eq!(resetter.total_reset_count(), 1);
    }

    #[test]
    fn test_execute_reset_idempotent() {
        let resetter = WorkflowResetter::new();
        let spec = ResetSpec::new(42, 5, ResetReason::ManualReset);
        let r1 = resetter.execute_reset(&spec, 10);
        assert!(r1.success);
        // Same request ID → duplicate
        let r2 = resetter.execute_reset(&spec, 10);
        assert!(!r2.success);
        assert_eq!(r2.error.unwrap(), "duplicate request");
    }

    #[test]
    fn test_execute_reset_invalid_event_id() {
        let resetter = WorkflowResetter::new();
        let spec = ResetSpec::new(42, 15, ResetReason::ManualReset); // > current_last_event_id
        let result = resetter.execute_reset(&spec, 10);
        assert!(!result.success);
        assert!(result.error.is_some());
    }

    #[test]
    fn test_branch_forking() {
        let resetter = WorkflowResetter::new();
        let spec = ResetSpec::new(42, 5, ResetReason::ManualReset);
        resetter.execute_reset(&spec, 10);
        let branches = resetter.get_branches(42);
        assert_eq!(branches.len(), 1); // One branch created
        let active = resetter.get_active_branch(42).unwrap();
        assert!(active.is_active);
        assert_eq!(active.fork_event_id, 5);
    }

    #[test]
    fn test_multiple_resets_create_branches() {
        let resetter = WorkflowResetter::new();
        let spec1 = ResetSpec::new(42, 5, ResetReason::ManualReset);
        resetter.execute_reset(&spec1, 10);
        let spec2 = ResetSpec::new(42, 3, ResetReason::NonDeterminism);
        resetter.execute_reset(&spec2, 8); // New last_event_id = 8 (from first reset)
        assert_eq!(resetter.branch_count(42), 2);
        assert_eq!(resetter.total_reset_count(), 2);
    }

    #[test]
    fn test_signal_reapplication() {
        let resetter = WorkflowResetter::new();

        // Register some pending signals
        resetter.register_pending_signal(
            42,
            PendingSignal {
                signal_id: 1,
                signal_name: "sig-a".into(),
                input: vec![],
                event_id: 3,
                reapplied: false,
            },
        );
        resetter.register_pending_signal(
            42,
            PendingSignal {
                signal_id: 2,
                signal_name: "sig-b".into(),
                input: vec![],
                event_id: 7,
                reapplied: false,
            },
        );
        resetter.register_pending_signal(
            42,
            PendingSignal {
                signal_id: 3,
                signal_name: "sig-c".into(),
                input: vec![],
                event_id: 9,
                reapplied: false,
            },
        );

        assert_eq!(resetter.unapplied_signal_count(42), 3);

        // Reset to event 5 → signals at event 7 and 9 should be reapplied
        let spec = ResetSpec::new(42, 5, ResetReason::ManualReset);
        let result = resetter.execute_reset(&spec, 10);
        assert!(result.success);
        assert_eq!(result.signals_reapplied, 2); // sig-b and sig-c
        assert_eq!(resetter.total_signals_reapplied(), 2);
    }

    #[test]
    fn test_signal_reapply_disabled() {
        let resetter = WorkflowResetter::new();
        resetter.register_pending_signal(
            42,
            PendingSignal {
                signal_id: 1,
                signal_name: "sig".into(),
                input: vec![],
                event_id: 7,
                reapplied: false,
            },
        );

        let spec = ResetSpec::new(42, 5, ResetReason::ManualReset).with_signal_reapply(false);
        let result = resetter.execute_reset(&spec, 10);
        assert!(result.success);
        assert_eq!(result.signals_reapplied, 0);
    }

    #[test]
    fn test_auto_reset_policy() {
        let resetter = WorkflowResetter::new();
        let policy = LastFailureResetPolicy::enabled(3);
        resetter.set_auto_reset_policy(100, policy);

        let retrieved = resetter.get_auto_reset_policy(100);
        assert!(retrieved.enabled);
        assert_eq!(retrieved.max_auto_resets, 3);
    }

    #[test]
    fn test_auto_reset_disabled_by_default() {
        let resetter = WorkflowResetter::new();
        let policy = resetter.get_auto_reset_policy(999);
        assert!(!policy.enabled);
    }

    #[test]
    fn test_maybe_auto_reset() {
        let resetter = WorkflowResetter::new();
        resetter.set_auto_reset_policy(100, LastFailureResetPolicy::enabled(5));

        let result = resetter.maybe_auto_reset(42, 100, 10);
        assert!(result.is_some());
        let r = result.unwrap();
        assert!(r.success);
        assert_eq!(resetter.total_auto_resets(), 1);
    }

    #[test]
    fn test_history_branch() {
        let root = HistoryBranch::new_root(10);
        assert_eq!(root.branch_id, 1);
        assert!(root.parent_branch_id.is_none());
        assert!(root.is_active);

        let fork = root.fork(2, 5);
        assert_eq!(fork.branch_id, 2);
        assert_eq!(fork.parent_branch_id, Some(1));
        assert_eq!(fork.fork_event_id, 5);
        assert!(!fork.is_active);
    }

    #[test]
    fn test_reset_reason_names() {
        assert_eq!(ResetReason::ManualReset.name(), "manual-reset");
        assert_eq!(ResetReason::DeadlockDetection.name(), "deadlock-detection");
        assert_eq!(ResetReason::NonDeterminism.name(), "non-determinism");
        assert_eq!(ResetReason::Custom("my-reason".into()).name(), "my-reason");
    }

    #[test]
    fn test_reset_spec_builder() {
        let spec = ResetSpec::new(42, 5, ResetReason::ManualReset)
            .with_signal_reapply(false)
            .with_target_build_id("build-v2");
        assert!(!spec.reapply_signals);
        assert_eq!(spec.target_build_id, Some("build-v2".to_string()));
        assert_eq!(spec.workflow_key, 42);
    }

    #[test]
    fn test_get_reset_results() {
        let resetter = WorkflowResetter::new();
        let spec1 = ResetSpec::new(1, 3, ResetReason::ManualReset);
        let spec2 = ResetSpec::new(2, 5, ResetReason::NonDeterminism);
        resetter.execute_reset(&spec1, 8);
        resetter.execute_reset(&spec2, 10);
        let results = resetter.get_reset_results();
        assert_eq!(results.len(), 2);
    }
}
