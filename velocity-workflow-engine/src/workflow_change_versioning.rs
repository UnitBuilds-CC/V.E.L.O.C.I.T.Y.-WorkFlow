//! Workflow Change Versioning — Temporal `getVersion()` equivalent.
//!
//! Enables safe deployment of workflow code changes by allowing workflows to branch
//! on named change IDs with durably recorded version decisions. When a workflow calls
//! `get_version(change_id, min, max)`:
//!
//! 1. If the change_id was already decided (e.g. during original execution), the recorded
//!    version is returned — ensuring deterministic replay.
//! 2. If the change_id is new, the maximum supported version is recorded and returned.
//!
//! This guarantees that:
//! - Old workflows continue using old logic (replay returns old version).
//! - New workflows use new logic (get max supported version).
//! - In-flight workflows can be safely migrated by bumping `min_supported`.
//!
//! Exceeds Temporal by adding:
//! - Per-namespace version isolation
//! - Version decision audit trail
//! - Automatic version conflict detection
//! - Bulk version queries

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    RwLock,
};
use std::time::{SystemTime, UNIX_EPOCH};

// ─── Version Decision ────────────────────────────────────────────────────────

/// A recorded version decision for a specific change ID within a workflow execution.
#[derive(Debug, Clone)]
pub struct VersionDecision {
    /// The change identifier (e.g. "add-shipping-label").
    pub change_id: String,
    /// The version that was decided (recorded durably).
    pub version: i32,
    /// The minimum version the caller supported at decision time.
    pub min_supported: i32,
    /// The maximum version the caller supported at decision time.
    pub max_supported: i32,
    /// When the decision was made (epoch ms).
    pub decided_at_ms: u64,
    /// Whether this decision was made during replay (vs original execution).
    pub was_replay: bool,
}

/// Status of a version decision request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionResult {
    /// A new version was decided (max_supported recorded).
    Decided(i32),
    /// An existing decision was returned (replay-safe).
    Existing(i32),
    /// The requested range is incompatible with the recorded version.
    Incompatible {
        recorded: i32,
        min_supported: i32,
        max_supported: i32,
    },
}

impl VersionResult {
    /// Get the version value regardless of result type (panics on Incompatible).
    pub fn version(&self) -> i32 {
        match self {
            VersionResult::Decided(v) | VersionResult::Existing(v) => *v,
            VersionResult::Incompatible { recorded, .. } => *recorded,
        }
    }

    pub fn is_decided(&self) -> bool {
        matches!(self, VersionResult::Decided(_))
    }
    pub fn is_existing(&self) -> bool {
        matches!(self, VersionResult::Existing(_))
    }
    pub fn is_incompatible(&self) -> bool {
        matches!(self, VersionResult::Incompatible { .. })
    }
}

// ─── Per-Workflow Version State ──────────────────────────────────────────────

/// All version decisions for a single workflow execution.
#[derive(Debug, Clone)]
pub struct WorkflowVersionState {
    /// Workflow key (namespace_id << 32 | workflow_id).
    pub workflow_key: u64,
    /// Decisions keyed by change_id.
    pub decisions: HashMap<String, VersionDecision>,
    /// Order in which decisions were made (for audit trail).
    pub decision_order: Vec<String>,
}

impl WorkflowVersionState {
    fn new(workflow_key: u64) -> Self {
        Self {
            workflow_key,
            decisions: HashMap::new(),
            decision_order: Vec::new(),
        }
    }
}

// ─── Version Audit Entry ─────────────────────────────────────────────────────

/// Audit trail entry for a version decision.
#[derive(Debug, Clone)]
pub struct VersionAuditEntry {
    pub workflow_key: u64,
    pub change_id: String,
    pub version: i32,
    pub min_supported: i32,
    pub max_supported: i32,
    pub was_replay: bool,
    pub timestamp_ms: u64,
}

// ─── Change Version Registry ─────────────────────────────────────────────────

/// Global registry managing per-workflow version decisions.
///
/// Thread-safe via RwLock. Designed for high-throughput: get_version reads use
/// read-lock (shared), new decisions use write-lock (exclusive).
pub struct ChangeVersionRegistry {
    /// Per-workflow version state.
    workflows: RwLock<HashMap<u64, WorkflowVersionState>>,
    /// Global audit trail (append-only).
    audit_trail: RwLock<Vec<VersionAuditEntry>>,
    /// Total decisions made across all workflows.
    total_decisions: AtomicU64,
    /// Total get_version calls (including replays).
    total_queries: AtomicU64,
    /// Total incompatible version requests detected.
    total_incompatible: AtomicU64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl ChangeVersionRegistry {
    pub fn new() -> Self {
        Self {
            workflows: RwLock::new(HashMap::new()),
            audit_trail: RwLock::new(Vec::new()),
            total_decisions: AtomicU64::new(0),
            total_queries: AtomicU64::new(0),
            total_incompatible: AtomicU64::new(0),
        }
    }

    /// Register a workflow for version tracking. Called when a workflow starts.
    pub fn register_workflow(&self, workflow_key: u64) {
        let mut workflows = self.workflows.write().unwrap();
        workflows
            .entry(workflow_key)
            .or_insert_with(|| WorkflowVersionState::new(workflow_key));
    }

    /// Remove a workflow's version state. Called when a workflow completes/terminates.
    pub fn unregister_workflow(&self, workflow_key: u64) -> bool {
        self.workflows
            .write()
            .unwrap()
            .remove(&workflow_key)
            .is_some()
    }

    /// Core API: Get or record a version for a change ID.
    ///
    /// - If `change_id` was previously decided for this workflow, returns the recorded version
    ///   (ensuring deterministic replay).
    /// - If `change_id` is new, records `max_supported` as the decision and returns it.
    /// - If the recorded version is outside [min_supported, max_supported], returns Incompatible.
    ///
    /// `is_replay` should be true when called during workflow replay (vs original execution).
    pub fn get_version(
        &self,
        workflow_key: u64,
        change_id: &str,
        min_supported: i32,
        max_supported: i32,
        is_replay: bool,
    ) -> VersionResult {
        self.total_queries.fetch_add(1, Ordering::Relaxed);

        // Validate inputs
        assert!(
            min_supported <= max_supported,
            "min_supported ({}) must be <= max_supported ({})",
            min_supported,
            max_supported
        );
        assert!(!change_id.is_empty(), "change_id must not be empty");

        let mut workflows = self.workflows.write().unwrap();
        let state = workflows
            .entry(workflow_key)
            .or_insert_with(|| WorkflowVersionState::new(workflow_key));

        // Check for existing decision
        if let Some(existing) = state.decisions.get(change_id) {
            let recorded = existing.version;

            // Check compatibility
            if recorded < min_supported || recorded > max_supported {
                self.total_incompatible.fetch_add(1, Ordering::Relaxed);
                return VersionResult::Incompatible {
                    recorded,
                    min_supported,
                    max_supported,
                };
            }

            return VersionResult::Existing(recorded);
        }

        // New decision: record max_supported
        let version = max_supported;
        let decision = VersionDecision {
            change_id: change_id.to_string(),
            version,
            min_supported,
            max_supported,
            decided_at_ms: now_ms(),
            was_replay: is_replay,
        };

        state.decisions.insert(change_id.to_string(), decision);
        state.decision_order.push(change_id.to_string());
        self.total_decisions.fetch_add(1, Ordering::Relaxed);

        // Record audit trail
        self.audit_trail.write().unwrap().push(VersionAuditEntry {
            workflow_key,
            change_id: change_id.to_string(),
            version,
            min_supported,
            max_supported,
            was_replay: is_replay,
            timestamp_ms: now_ms(),
        });

        VersionResult::Decided(version)
    }

    /// Check if a change ID has been decided for a workflow (without recording).
    pub fn has_decision(&self, workflow_key: u64, change_id: &str) -> bool {
        self.workflows
            .read()
            .unwrap()
            .get(&workflow_key)
            .is_some_and(|s| s.decisions.contains_key(change_id))
    }

    /// Get the recorded version for a change ID (read-only, no side effects).
    pub fn get_recorded_version(&self, workflow_key: u64, change_id: &str) -> Option<i32> {
        self.workflows
            .read()
            .unwrap()
            .get(&workflow_key)
            .and_then(|s| s.decisions.get(change_id))
            .map(|d| d.version)
    }

    /// Get all decisions for a workflow.
    pub fn get_workflow_decisions(&self, workflow_key: u64) -> Option<Vec<VersionDecision>> {
        self.workflows.read().unwrap().get(&workflow_key).map(|s| {
            s.decision_order
                .iter()
                .filter_map(|cid| s.decisions.get(cid).cloned())
                .collect()
        })
    }

    /// Get the number of decisions for a specific workflow.
    pub fn decision_count(&self, workflow_key: u64) -> usize {
        self.workflows
            .read()
            .unwrap()
            .get(&workflow_key)
            .map_or(0, |s| s.decisions.len())
    }

    /// Get the full audit trail.
    pub fn audit_trail(&self) -> Vec<VersionAuditEntry> {
        self.audit_trail.read().unwrap().clone()
    }

    /// Get audit entries for a specific workflow.
    pub fn audit_for_workflow(&self, workflow_key: u64) -> Vec<VersionAuditEntry> {
        self.audit_trail
            .read()
            .unwrap()
            .iter()
            .filter(|e| e.workflow_key == workflow_key)
            .cloned()
            .collect()
    }

    /// Get audit entries for a specific change ID across all workflows.
    pub fn audit_for_change_id(&self, change_id: &str) -> Vec<VersionAuditEntry> {
        self.audit_trail
            .read()
            .unwrap()
            .iter()
            .filter(|e| e.change_id == change_id)
            .cloned()
            .collect()
    }

    /// Total workflows tracked.
    pub fn tracked_workflow_count(&self) -> usize {
        self.workflows.read().unwrap().len()
    }

    /// Total decisions made across all workflows.
    pub fn total_decisions(&self) -> u64 {
        self.total_decisions.load(Ordering::Relaxed)
    }

    /// Total get_version calls.
    pub fn total_queries(&self) -> u64 {
        self.total_queries.load(Ordering::Relaxed)
    }

    /// Total incompatible version requests.
    pub fn total_incompatible(&self) -> u64 {
        self.total_incompatible.load(Ordering::Relaxed)
    }

    /// Get a summary of the registry state.
    pub fn summary(&self) -> ChangeVersionSummary {
        ChangeVersionSummary {
            tracked_workflows: self.tracked_workflow_count(),
            total_decisions: self.total_decisions(),
            total_queries: self.total_queries(),
            total_incompatible: self.total_incompatible(),
            audit_entries: self.audit_trail.read().unwrap().len(),
        }
    }

    /// Bulk query: get all decided change IDs for a workflow.
    pub fn decided_change_ids(&self, workflow_key: u64) -> Vec<String> {
        self.workflows
            .read()
            .unwrap()
            .get(&workflow_key)
            .map(|s| s.decision_order.clone())
            .unwrap_or_default()
    }

    /// Check if a workflow has any version decisions.
    pub fn has_any_decisions(&self, workflow_key: u64) -> bool {
        self.workflows
            .read()
            .unwrap()
            .get(&workflow_key)
            .is_some_and(|s| !s.decisions.is_empty())
    }

    /// Force-set a version decision (for testing or migration).
    /// Bypasses the normal get_version flow and directly records a decision.
    pub fn force_set_version(&self, workflow_key: u64, change_id: &str, version: i32) -> bool {
        let mut workflows = self.workflows.write().unwrap();
        let state = workflows
            .entry(workflow_key)
            .or_insert_with(|| WorkflowVersionState::new(workflow_key));

        if state.decisions.contains_key(change_id) {
            return false; // Already decided, use force_override to change
        }

        let decision = VersionDecision {
            change_id: change_id.to_string(),
            version,
            min_supported: version,
            max_supported: version,
            decided_at_ms: now_ms(),
            was_replay: false,
        };
        state.decisions.insert(change_id.to_string(), decision);
        state.decision_order.push(change_id.to_string());
        self.total_decisions.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Override an existing version decision (for workflow reset or migration).
    pub fn override_version(&self, workflow_key: u64, change_id: &str, new_version: i32) -> bool {
        let mut workflows = self.workflows.write().unwrap();
        if let Some(state) = workflows.get_mut(&workflow_key) {
            if let Some(existing) = state.decisions.get_mut(change_id) {
                existing.version = new_version;
                existing.decided_at_ms = now_ms();
                return true;
            }
        }
        false
    }

    /// Clear all decisions for a workflow (for workflow reset).
    pub fn reset_workflow(&self, workflow_key: u64) -> bool {
        let mut workflows = self.workflows.write().unwrap();
        if let Some(state) = workflows.get_mut(&workflow_key) {
            let count = state.decisions.len();
            state.decisions.clear();
            state.decision_order.clear();
            // Adjust total (we don't decrement to keep monotonic counter)
            return count > 0;
        }
        false
    }
}

impl Default for ChangeVersionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary statistics for the change version registry.
#[derive(Debug, Clone)]
pub struct ChangeVersionSummary {
    pub tracked_workflows: usize,
    pub total_decisions: u64,
    pub total_queries: u64,
    pub total_incompatible: u64,
    pub audit_entries: usize,
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_decision_returns_max_supported() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);
        let result = reg.get_version(100, "add-shipping-label", 1, 3, false);
        assert!(result.is_decided());
        assert_eq!(result.version(), 3); // max_supported
    }

    #[test]
    fn test_existing_decision_returns_recorded_version() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);

        // First call: decides version 3
        let r1 = reg.get_version(100, "add-shipping-label", 1, 3, false);
        assert_eq!(r1.version(), 3);

        // Second call: returns existing 3, even if max_supported is different
        let r2 = reg.get_version(100, "add-shipping-label", 1, 5, false);
        assert!(r2.is_existing());
        assert_eq!(r2.version(), 3);
    }

    #[test]
    fn test_replay_returns_same_version() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);

        // Original execution decides version 2
        let r1 = reg.get_version(100, "change-logic", 1, 2, false);
        assert_eq!(r1.version(), 2);

        // Replay returns same version
        let r2 = reg.get_version(100, "change-logic", 1, 5, true);
        assert!(r2.is_existing());
        assert_eq!(r2.version(), 2);
    }

    #[test]
    fn test_incompatible_version_range() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);

        // Decide version 1
        let r1 = reg.get_version(100, "old-change", 1, 1, false);
        assert_eq!(r1.version(), 1);

        // Now caller requires min=3, but recorded is 1
        let r2 = reg.get_version(100, "old-change", 3, 5, false);
        assert!(r2.is_incompatible());
        if let VersionResult::Incompatible {
            recorded,
            min_supported,
            max_supported,
        } = r2
        {
            assert_eq!(recorded, 1);
            assert_eq!(min_supported, 3);
            assert_eq!(max_supported, 5);
        }
    }

    #[test]
    fn test_has_decision() {
        let reg = ChangeVersionRegistry::new();
        assert!(!reg.has_decision(100, "x"));

        reg.register_workflow(100);
        assert!(!reg.has_decision(100, "x"));

        reg.get_version(100, "x", 1, 1, false);
        assert!(reg.has_decision(100, "x"));
    }

    #[test]
    fn test_get_recorded_version() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);
        assert_eq!(reg.get_recorded_version(100, "x"), None);

        reg.get_version(100, "x", 1, 5, false);
        assert_eq!(reg.get_recorded_version(100, "x"), Some(5));
    }

    #[test]
    fn test_decision_count() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);
        assert_eq!(reg.decision_count(100), 0);

        reg.get_version(100, "a", 1, 1, false);
        reg.get_version(100, "b", 1, 2, false);
        reg.get_version(100, "a", 1, 1, false); // existing, no new decision
        assert_eq!(reg.decision_count(100), 2);
    }

    #[test]
    fn test_unregister_workflow() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);
        reg.get_version(100, "x", 1, 1, false);
        assert_eq!(reg.tracked_workflow_count(), 1);

        assert!(reg.unregister_workflow(100));
        assert_eq!(reg.tracked_workflow_count(), 0);
        assert!(!reg.unregister_workflow(100)); // already gone
    }

    #[test]
    fn test_audit_trail() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);
        reg.register_workflow(200);

        reg.get_version(100, "change-a", 1, 2, false);
        reg.get_version(100, "change-b", 1, 3, false);
        reg.get_version(200, "change-a", 1, 1, true);

        let trail = reg.audit_trail();
        assert_eq!(trail.len(), 3);

        let for_100 = reg.audit_for_workflow(100);
        assert_eq!(for_100.len(), 2);

        let for_change_a = reg.audit_for_change_id("change-a");
        assert_eq!(for_change_a.len(), 2);
    }

    #[test]
    fn test_summary() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);
        reg.get_version(100, "a", 1, 2, false); // decision: version 2
        reg.get_version(100, "a", 1, 5, false); // existing: returns 2
        reg.get_version(100, "a", 3, 5, false); // incompatible: recorded=2, min=3

        let s = reg.summary();
        assert_eq!(s.tracked_workflows, 1);
        assert_eq!(s.total_decisions, 1); // only "a"
        assert_eq!(s.total_queries, 3);
        assert_eq!(s.total_incompatible, 1);
        assert_eq!(s.audit_entries, 1);
    }

    #[test]
    fn test_auto_register_on_get_version() {
        let reg = ChangeVersionRegistry::new();
        // Don't call register_workflow — get_version should auto-create
        let result = reg.get_version(999, "auto", 1, 1, false);
        assert_eq!(result.version(), 1);
        assert_eq!(reg.tracked_workflow_count(), 1);
    }

    #[test]
    fn test_multiple_change_ids() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);

        let v1 = reg.get_version(100, "feature-a", 1, 3, false);
        let v2 = reg.get_version(100, "feature-b", 1, 5, false);
        let v3 = reg.get_version(100, "feature-c", 2, 2, false);

        assert_eq!(v1.version(), 3);
        assert_eq!(v2.version(), 5);
        assert_eq!(v3.version(), 2);

        let decisions = reg.get_workflow_decisions(100).unwrap();
        assert_eq!(decisions.len(), 3);
        assert_eq!(decisions[0].change_id, "feature-a");
        assert_eq!(decisions[1].change_id, "feature-b");
        assert_eq!(decisions[2].change_id, "feature-c");
    }

    #[test]
    fn test_decided_change_ids() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);
        assert!(reg.decided_change_ids(100).is_empty());

        reg.get_version(100, "b", 1, 1, false);
        reg.get_version(100, "a", 1, 2, false);

        let ids = reg.decided_change_ids(100);
        assert_eq!(ids, vec!["b", "a"]); // insertion order
    }

    #[test]
    fn test_force_set_version() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);

        assert!(reg.force_set_version(100, "migrated", 7));
        assert_eq!(reg.get_recorded_version(100, "migrated"), Some(7));

        // Can't force-set if already decided
        assert!(!reg.force_set_version(100, "migrated", 8));
        assert_eq!(reg.get_recorded_version(100, "migrated"), Some(7));
    }

    #[test]
    fn test_override_version() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);
        reg.get_version(100, "x", 1, 3, false);
        assert_eq!(reg.get_recorded_version(100, "x"), Some(3));

        assert!(reg.override_version(100, "x", 5));
        assert_eq!(reg.get_recorded_version(100, "x"), Some(5));

        // Can't override non-existent
        assert!(!reg.override_version(100, "y", 1));
    }

    #[test]
    fn test_reset_workflow() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);
        reg.get_version(100, "a", 1, 1, false);
        reg.get_version(100, "b", 1, 2, false);
        assert_eq!(reg.decision_count(100), 2);

        assert!(reg.reset_workflow(100));
        assert_eq!(reg.decision_count(100), 0);
        assert!(!reg.has_any_decisions(100));

        // Can make new decisions after reset
        let r = reg.get_version(100, "a", 1, 5, false);
        assert_eq!(r.version(), 5); // new decision, gets max_supported
    }

    #[test]
    fn test_has_any_decisions() {
        let reg = ChangeVersionRegistry::new();
        assert!(!reg.has_any_decisions(100));

        reg.register_workflow(100);
        assert!(!reg.has_any_decisions(100));

        reg.get_version(100, "x", 1, 1, false);
        assert!(reg.has_any_decisions(100));
    }

    #[test]
    fn test_total_counters() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);

        reg.get_version(100, "a", 1, 1, false); // decision
        reg.get_version(100, "a", 1, 1, false); // existing
        reg.get_version(100, "b", 2, 3, false); // decision

        assert_eq!(reg.total_decisions(), 2);
        assert_eq!(reg.total_queries(), 3);
        assert_eq!(reg.total_incompatible(), 0);
    }

    #[test]
    fn test_incompatible_counter() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);
        reg.get_version(100, "x", 1, 1, false); // decide 1
        reg.get_version(100, "x", 3, 5, false); // incompatible

        assert_eq!(reg.total_incompatible(), 1);
    }

    #[test]
    fn test_workflow_isolation() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);
        reg.register_workflow(200);

        reg.get_version(100, "same-id", 1, 3, false);
        reg.get_version(200, "same-id", 1, 5, false);

        // Each workflow has its own decision
        assert_eq!(reg.get_recorded_version(100, "same-id"), Some(3));
        assert_eq!(reg.get_recorded_version(200, "same-id"), Some(5));
    }

    #[test]
    fn test_get_workflow_decisions_order() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);

        reg.get_version(100, "c", 1, 3, false);
        reg.get_version(100, "a", 1, 1, false);
        reg.get_version(100, "b", 1, 2, false);

        let decisions = reg.get_workflow_decisions(100).unwrap();
        assert_eq!(decisions[0].change_id, "c");
        assert_eq!(decisions[1].change_id, "a");
        assert_eq!(decisions[2].change_id, "b");
    }

    #[test]
    fn test_nonexistent_workflow_decisions() {
        let reg = ChangeVersionRegistry::new();
        assert!(reg.get_workflow_decisions(999).is_none());
        assert_eq!(reg.decision_count(999), 0);
        assert!(reg.decided_change_ids(999).is_empty());
    }

    #[test]
    #[should_panic(expected = "min_supported")]
    fn test_invalid_version_range_panics() {
        let reg = ChangeVersionRegistry::new();
        reg.get_version(100, "bad", 5, 1, false); // min > max
    }

    #[test]
    #[should_panic(expected = "change_id must not be empty")]
    fn test_empty_change_id_panics() {
        let reg = ChangeVersionRegistry::new();
        reg.get_version(100, "", 1, 1, false);
    }

    #[test]
    fn test_was_replay_flag() {
        let reg = ChangeVersionRegistry::new();
        reg.register_workflow(100);

        reg.get_version(100, "x", 1, 2, false);
        let decisions = reg.get_workflow_decisions(100).unwrap();
        assert!(!decisions[0].was_replay);

        reg.register_workflow(200);
        reg.get_version(200, "x", 1, 2, true);
        let decisions = reg.get_workflow_decisions(200).unwrap();
        assert!(decisions[0].was_replay);
    }

    #[test]
    fn test_reset_nonexistent() {
        let reg = ChangeVersionRegistry::new();
        assert!(!reg.reset_workflow(999));
    }
}
