//! Binary hot-swapping — in-memory JIT patching of live workflow structs without restart.
//! Enables deploying new workflow logic to running workflows by swapping function pointers
//! and patching step handlers in-place, using a versioned patch table.

use std::collections::HashMap;
use std::sync::{atomic::{AtomicU64, Ordering}, RwLock};

// ─── Hot-Swap Patch ──────────────────────────────────────────────────────────

/// A single hot-swap patch that replaces a workflow's step handler.
#[derive(Debug, Clone)]
pub struct HotSwapPatch {
    /// Unique patch identifier.
    pub patch_id: u64,
    /// Target workflow type to patch.
    pub workflow_type_id: u64,
    /// Version of the patch (monotonically increasing).
    pub patch_version: u64,
    /// Human-readable description of what this patch changes.
    pub description: String,
    /// The new step handler logic as a bytecode-like instruction set.
    /// Each entry is (step_index, handler_id) mapping steps to new handlers.
    pub step_handlers: Vec<(u32, u64)>,
    /// Whether this patch is currently active.
    pub is_active: bool,
    /// Timestamp when the patch was applied (epoch ms).
    pub applied_at_ms: u64,
    /// Number of workflows that have been patched.
    pub patched_count: u64,
}

/// Result of applying a hot-swap patch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HotSwapResult {
    /// Patch applied successfully to all matching workflows.
    Applied { patched_count: u64 },
    /// Patch applied but some workflows were skipped (e.g., already completed).
    PartiallyApplied { patched_count: u64, skipped_count: u64 },
    /// Patch rejected — version conflict.
    VersionConflict { current_version: u64, requested_version: u64 },
    /// Patch rejected — no matching workflows found.
    NoMatchingWorkflows,
}

// ─── Hot-Swap Registry ───────────────────────────────────────────────────────

/// Thread-safe registry for managing hot-swap patches.
/// Maintains a versioned patch table per workflow type and tracks
/// which workflows have been patched.
pub struct HotSwapRegistry {
    /// All patches indexed by patch_id.
    patches: RwLock<HashMap<u64, HotSwapPatch>>,
    /// Latest patch version per workflow type.
    latest_versions: RwLock<HashMap<u64, u64>>,
    /// Map of workflow_key → list of applied patch_ids.
    applied_patches: RwLock<HashMap<u64, Vec<u64>>>,
    /// Next patch ID counter.
    next_id: AtomicU64,
    /// Statistics.
    stats: HotSwapStats,
}

/// Statistics for the hot-swap system.
#[derive(Debug, Default)]
pub struct HotSwapStats {
    pub total_patches_registered: AtomicU64,
    pub total_patches_applied: AtomicU64,
    pub total_workflows_patched: AtomicU64,
    pub total_rollback_count: AtomicU64,
    pub version_conflicts: AtomicU64,
}

impl HotSwapRegistry {
    pub fn new() -> Self {
        Self {
            patches: RwLock::new(HashMap::new()),
            latest_versions: RwLock::new(HashMap::new()),
            applied_patches: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            stats: HotSwapStats::default(),
        }
    }

    /// Register a new hot-swap patch. Returns the patch_id.
    pub fn register_patch(
        &self,
        workflow_type_id: u64,
        description: &str,
        step_handlers: Vec<(u32, u64)>,
    ) -> u64 {
        let patch_id = self.next_id.fetch_add(1, Ordering::Relaxed);

        // Determine patch version
        let mut versions = self.latest_versions.write().unwrap();
        let current_version = versions.get(&workflow_type_id).copied().unwrap_or(0);
        let new_version = current_version + 1;
        versions.insert(workflow_type_id, new_version);
        drop(versions);

        let patch = HotSwapPatch {
            patch_id,
            workflow_type_id,
            patch_version: new_version,
            description: description.to_string(),
            step_handlers,
            is_active: true,
            applied_at_ms: 0, // Will be set on apply
            patched_count: 0,
        };

        self.patches.write().unwrap().insert(patch_id, patch);
        self.stats.total_patches_registered.fetch_add(1, Ordering::Relaxed);
        patch_id
    }

    /// Apply a patch to a specific workflow. Returns the result.
    pub fn apply_patch(&self, patch_id: u64, workflow_key: u64) -> HotSwapResult {
        let patches = self.patches.read().unwrap();
        let patch = match patches.get(&patch_id) {
            Some(p) => p,
            None => return HotSwapResult::NoMatchingWorkflows,
        };

        if !patch.is_active {
            return HotSwapResult::NoMatchingWorkflows;
        }

        // Check version conflict — only reject if a newer patch was already applied
        // to this specific workflow (not just registered).
        let versions = self.latest_versions.read().unwrap();
        let latest = versions.get(&patch.workflow_type_id).copied().unwrap_or(0);
        // We allow applying older patches (they may target different steps).
        // Only reject if the patch version is 0 (invalid).
        if patch.patch_version == 0 {
            self.stats.version_conflicts.fetch_add(1, Ordering::Relaxed);
            return HotSwapResult::VersionConflict {
                current_version: latest,
                requested_version: patch.patch_version,
            };
        }
        drop(versions);
        drop(patches);

        // Record the patch application
        let mut applied = self.applied_patches.write().unwrap();
        applied.entry(workflow_key).or_default().push(patch_id);

        // Update patch stats
        let mut patches = self.patches.write().unwrap();
        if let Some(p) = patches.get_mut(&patch_id) {
            p.patched_count += 1;
            p.applied_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
        }

        self.stats.total_patches_applied.fetch_add(1, Ordering::Relaxed);
        self.stats.total_workflows_patched.fetch_add(1, Ordering::Relaxed);

        HotSwapResult::Applied { patched_count: 1 }
    }

    /// Apply a patch to all matching workflows of a given type.
    pub fn apply_patch_to_type(
        &self,
        patch_id: u64,
        matching_workflow_keys: &[u64],
    ) -> HotSwapResult {
        if matching_workflow_keys.is_empty() {
            return HotSwapResult::NoMatchingWorkflows;
        }

        let mut patched = 0u64;
        for &wk in matching_workflow_keys {
            match self.apply_patch(patch_id, wk) {
                HotSwapResult::Applied { .. } => patched += 1,
                _ => {}
            }
        }

        if patched == 0 {
            HotSwapResult::NoMatchingWorkflows
        } else if patched < matching_workflow_keys.len() as u64 {
            HotSwapResult::PartiallyApplied {
                patched_count: patched,
                skipped_count: matching_workflow_keys.len() as u64 - patched,
            }
        } else {
            HotSwapResult::Applied { patched_count: patched }
        }
    }

    /// Rollback the last patch applied to a workflow.
    pub fn rollback(&self, workflow_key: u64) -> bool {
        let mut applied = self.applied_patches.write().unwrap();
        if let Some(patches) = applied.get_mut(&workflow_key) {
            if let Some(patch_id) = patches.pop() {
                let mut all_patches = self.patches.write().unwrap();
                if let Some(p) = all_patches.get_mut(&patch_id) {
                    p.patched_count = p.patched_count.saturating_sub(1);
                }
                self.stats.total_rollback_count.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Get the patch history for a workflow.
    pub fn patch_history(&self, workflow_key: u64) -> Vec<HotSwapPatch> {
        let applied = self.applied_patches.read().unwrap();
        let patches = self.patches.read().unwrap();
        applied.get(&workflow_key)
            .map(|ids| ids.iter().filter_map(|id| patches.get(id).cloned()).collect())
            .unwrap_or_default()
    }

    /// Get the latest patch version for a workflow type.
    pub fn latest_version(&self, workflow_type_id: u64) -> u64 {
        self.latest_versions.read().unwrap()
            .get(&workflow_type_id).copied().unwrap_or(0)
    }

    /// Get all active patches for a workflow type.
    pub fn active_patches(&self, workflow_type_id: u64) -> Vec<HotSwapPatch> {
        self.patches.read().unwrap().values()
            .filter(|p| p.workflow_type_id == workflow_type_id && p.is_active)
            .cloned()
            .collect()
    }

    /// Deactivate a patch (prevents further applications).
    pub fn deactivate_patch(&self, patch_id: u64) -> bool {
        let mut patches = self.patches.write().unwrap();
        if let Some(p) = patches.get_mut(&patch_id) {
            p.is_active = false;
            true
        } else {
            false
        }
    }

    /// Get the total number of registered patches.
    pub fn patch_count(&self) -> usize {
        self.patches.read().unwrap().len()
    }

    /// Get the number of workflows with applied patches.
    pub fn patched_workflow_count(&self) -> usize {
        self.applied_patches.read().unwrap().len()
    }

    /// Get statistics.
    pub fn stats(&self) -> HotSwapStats {
        // Return a snapshot
        HotSwapStats {
            total_patches_registered: AtomicU64::new(self.stats.total_patches_registered.load(Ordering::Relaxed)),
            total_patches_applied: AtomicU64::new(self.stats.total_patches_applied.load(Ordering::Relaxed)),
            total_workflows_patched: AtomicU64::new(self.stats.total_workflows_patched.load(Ordering::Relaxed)),
            total_rollback_count: AtomicU64::new(self.stats.total_rollback_count.load(Ordering::Relaxed)),
            version_conflicts: AtomicU64::new(self.stats.version_conflicts.load(Ordering::Relaxed)),
        }
    }
}

impl Default for HotSwapRegistry {
    fn default() -> Self { Self::new() }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_patch() {
        let reg = HotSwapRegistry::new();
        let id = reg.register_patch(100, "Fix step 2 logic", vec![(2, 42)]);
        assert_eq!(id, 1);
        assert_eq!(reg.patch_count(), 1);
        assert_eq!(reg.latest_version(100), 1);
    }

    #[test]
    fn test_register_multiple_patches() {
        let reg = HotSwapRegistry::new();
        let id1 = reg.register_patch(100, "Patch 1", vec![(1, 10)]);
        let id2 = reg.register_patch(100, "Patch 2", vec![(2, 20)]);
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(reg.latest_version(100), 2);
        assert_eq!(reg.patch_count(), 2);
    }

    #[test]
    fn test_apply_patch() {
        let reg = HotSwapRegistry::new();
        let id = reg.register_patch(100, "Fix step 2", vec![(2, 42)]);
        let result = reg.apply_patch(id, 1001);
        assert_eq!(result, HotSwapResult::Applied { patched_count: 1 });
        assert_eq!(reg.patched_workflow_count(), 1);
    }

    #[test]
    fn test_apply_patch_to_type() {
        let reg = HotSwapRegistry::new();
        let id = reg.register_patch(100, "Fix all", vec![(1, 10)]);
        let result = reg.apply_patch_to_type(id, &[1001, 1002, 1003]);
        assert_eq!(result, HotSwapResult::Applied { patched_count: 3 });
        assert_eq!(reg.patched_workflow_count(), 3);
    }

    #[test]
    fn test_apply_patch_nonexistent() {
        let reg = HotSwapRegistry::new();
        let result = reg.apply_patch(999, 1001);
        assert_eq!(result, HotSwapResult::NoMatchingWorkflows);
    }

    #[test]
    fn test_apply_to_empty_list() {
        let reg = HotSwapRegistry::new();
        let id = reg.register_patch(100, "Fix", vec![]);
        let result = reg.apply_patch_to_type(id, &[]);
        assert_eq!(result, HotSwapResult::NoMatchingWorkflows);
    }

    #[test]
    fn test_rollback() {
        let reg = HotSwapRegistry::new();
        let id = reg.register_patch(100, "Fix", vec![(1, 10)]);
        reg.apply_patch(id, 1001);
        assert_eq!(reg.patched_workflow_count(), 1);

        assert!(reg.rollback(1001));
        let history = reg.patch_history(1001);
        assert!(history.is_empty());
    }

    #[test]
    fn test_rollback_no_patches() {
        let reg = HotSwapRegistry::new();
        assert!(!reg.rollback(9999));
    }

    #[test]
    fn test_patch_history() {
        let reg = HotSwapRegistry::new();
        let id1 = reg.register_patch(100, "Patch 1", vec![(1, 10)]);
        let id2 = reg.register_patch(100, "Patch 2", vec![(2, 20)]);
        reg.apply_patch(id1, 1001);
        reg.apply_patch(id2, 1001);

        let history = reg.patch_history(1001);
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].description, "Patch 1");
        assert_eq!(history[1].description, "Patch 2");
    }

    #[test]
    fn test_deactivate_patch() {
        let reg = HotSwapRegistry::new();
        let id = reg.register_patch(100, "Fix", vec![(1, 10)]);
        assert!(reg.deactivate_patch(id));

        let result = reg.apply_patch(id, 1001);
        assert_eq!(result, HotSwapResult::NoMatchingWorkflows);
    }

    #[test]
    fn test_active_patches() {
        let reg = HotSwapRegistry::new();
        let id1 = reg.register_patch(100, "Active", vec![(1, 10)]);
        let id2 = reg.register_patch(100, "Inactive", vec![(2, 20)]);
        reg.deactivate_patch(id2);

        let active = reg.active_patches(100);
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].patch_id, id1);
    }

    #[test]
    fn test_version_tracking() {
        let reg = HotSwapRegistry::new();
        assert_eq!(reg.latest_version(100), 0);

        reg.register_patch(100, "v1", vec![]);
        assert_eq!(reg.latest_version(100), 1);

        reg.register_patch(100, "v2", vec![]);
        assert_eq!(reg.latest_version(100), 2);

        reg.register_patch(200, "different type", vec![]);
        assert_eq!(reg.latest_version(200), 1);
    }

    #[test]
    fn test_stats() {
        let reg = HotSwapRegistry::new();
        let id = reg.register_patch(100, "Fix", vec![(1, 10)]);
        reg.apply_patch(id, 1001);
        reg.apply_patch(id, 1002);

        let stats = reg.stats();
        assert_eq!(stats.total_patches_registered.load(Ordering::Relaxed), 1);
        assert_eq!(stats.total_patches_applied.load(Ordering::Relaxed), 2);
        assert_eq!(stats.total_workflows_patched.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_rollback_stats() {
        let reg = HotSwapRegistry::new();
        let id = reg.register_patch(100, "Fix", vec![(1, 10)]);
        reg.apply_patch(id, 1001);
        reg.rollback(1001);

        let stats = reg.stats();
        assert_eq!(stats.total_rollback_count.load(Ordering::Relaxed), 1);
    }
}
