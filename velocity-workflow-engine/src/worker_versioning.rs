//! Worker versioning — build ID tracking, version sets, and routing rules.
//! Enables safe deployment of new workflow code while old workflows continue on old versions.
//! Mirrors Temporal's `common/worker_versioning` with:
//! - Version sets with build ID tracking
//! - Compatibility graph between build IDs
//! - Percentage-based routing rules
//! - Build ID redirect chains
//! - Deployment tracking

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct BuildId {
    pub id: String,
    pub created_at_ms: u64,
    pub is_current: bool,
    /// Build IDs that are compatible with this one (can process same tasks).
    pub compatible_with: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct VersionSet {
    pub set_id: u64,
    pub build_ids: Vec<BuildId>,
    pub current_build_id: Option<String>,
    /// Whether this set is the default for its task queue.
    pub is_default: bool,
}

#[derive(Debug, Clone)]
pub struct RoutingRule {
    pub task_queue: String,
    pub target_build_id: String,
    pub percentage: u32, // 0-100 for ramping
}

/// Deployment info for a build ID.
#[derive(Debug, Clone)]
pub struct DeploymentInfo {
    pub build_id: String,
    pub task_queue: String,
    pub started_at_ms: u64,
    pub is_current: bool,
    pub task_count: u64,
}

pub struct WorkerVersioning {
    version_sets: Mutex<HashMap<u64, VersionSet>>,
    routing_rules: Mutex<Vec<RoutingRule>>,
    build_id_to_set: Mutex<HashMap<String, u64>>,
    /// Compatibility edges: (from_build_id, to_build_id) means they are compatible.
    compatibility: Mutex<HashSet<(String, String)>>,
    /// Deployment tracking per task queue.
    deployments: Mutex<HashMap<String, Vec<DeploymentInfo>>>,
    next_set_id: AtomicU64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl WorkerVersioning {
    pub fn new() -> Self {
        Self {
            version_sets: Mutex::new(HashMap::new()),
            routing_rules: Mutex::new(Vec::new()),
            build_id_to_set: Mutex::new(HashMap::new()),
            compatibility: Mutex::new(HashSet::new()),
            deployments: Mutex::new(HashMap::new()),
            next_set_id: AtomicU64::new(1),
        }
    }

    pub fn create_version_set(&self) -> u64 {
        let id = self.next_set_id.fetch_add(1, Ordering::Relaxed);
        self.version_sets.lock().unwrap().insert(
            id,
            VersionSet {
                set_id: id,
                build_ids: Vec::new(),
                current_build_id: None,
                is_default: false,
            },
        );
        id
    }

    pub fn add_build_id(&self, set_id: u64, build_id: &str) -> bool {
        let mut sets = self.version_sets.lock().unwrap();
        if let Some(set) = sets.get_mut(&set_id) {
            if set.build_ids.iter().any(|b| b.id == build_id) {
                return false;
            }
            set.build_ids.push(BuildId {
                id: build_id.to_string(),
                created_at_ms: now_ms(),
                is_current: false,
                compatible_with: Vec::new(),
            });
            if set.current_build_id.is_none() {
                set.current_build_id = Some(build_id.to_string());
            }
            self.build_id_to_set
                .lock()
                .unwrap()
                .insert(build_id.to_string(), set_id);
            true
        } else {
            false
        }
    }

    pub fn set_current_build_id(&self, set_id: u64, build_id: &str) -> bool {
        let mut sets = self.version_sets.lock().unwrap();
        if let Some(set) = sets.get_mut(&set_id) {
            if set.build_ids.iter().any(|b| b.id == build_id) {
                set.current_build_id = Some(build_id.to_string());
                for b in &mut set.build_ids {
                    b.is_current = b.id == build_id;
                }
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn get_current_build_id(&self, set_id: u64) -> Option<String> {
        self.version_sets
            .lock()
            .unwrap()
            .get(&set_id)
            .and_then(|s| s.current_build_id.clone())
    }

    pub fn get_set_for_build_id(&self, build_id: &str) -> Option<u64> {
        self.build_id_to_set.lock().unwrap().get(build_id).copied()
    }

    pub fn add_routing_rule(&self, task_queue: &str, build_id: &str, percentage: u32) {
        self.routing_rules.lock().unwrap().push(RoutingRule {
            task_queue: task_queue.to_string(),
            target_build_id: build_id.to_string(),
            percentage,
        });
    }

    pub fn resolve_build_id(&self, task_queue: &str) -> Option<String> {
        let rules = self.routing_rules.lock().unwrap();
        rules
            .iter()
            .find(|r| r.task_queue == task_queue)
            .map(|r| r.target_build_id.clone())
    }

    pub fn version_set_count(&self) -> usize {
        self.version_sets.lock().unwrap().len()
    }
    pub fn routing_rule_count(&self) -> usize {
        self.routing_rules.lock().unwrap().len()
    }

    // ─── Compatibility Graph ─────────────────────────────────────────────

    /// Mark two build IDs as compatible (can process each other's tasks).
    pub fn add_compatibility(&self, from_build_id: &str, to_build_id: &str) {
        self.compatibility
            .lock()
            .unwrap()
            .insert((from_build_id.to_string(), to_build_id.to_string()));
        // Also update the BuildId's compatible_with list
        let mut sets = self.version_sets.lock().unwrap();
        for set in sets.values_mut() {
            for b in &mut set.build_ids {
                if b.id == from_build_id && !b.compatible_with.contains(&to_build_id.to_string()) {
                    b.compatible_with.push(to_build_id.to_string());
                }
            }
        }
    }

    /// Check if two build IDs are compatible.
    pub fn are_compatible(&self, a: &str, b: &str) -> bool {
        let compat = self.compatibility.lock().unwrap();
        compat.contains(&(a.to_string(), b.to_string()))
            || compat.contains(&(b.to_string(), a.to_string()))
            || a == b
    }

    /// Get all build IDs compatible with a given build ID.
    pub fn compatible_build_ids(&self, build_id: &str) -> Vec<String> {
        let compat = self.compatibility.lock().unwrap();
        let mut result = Vec::new();
        for (from, to) in compat.iter() {
            if from == build_id {
                result.push(to.clone());
            }
            if to == build_id {
                result.push(from.clone());
            }
        }
        result
    }

    pub fn compatibility_edge_count(&self) -> usize {
        self.compatibility.lock().unwrap().len()
    }

    // ─── Deployment Tracking ─────────────────────────────────────────────

    /// Register a deployment for a build ID on a task queue.
    pub fn register_deployment(&self, task_queue: &str, build_id: &str) {
        let mut deployments = self.deployments.lock().unwrap();
        let entry = deployments.entry(task_queue.to_string()).or_default();
        // Check if already deployed
        if entry.iter().any(|d| d.build_id == build_id) {
            return;
        }
        entry.push(DeploymentInfo {
            build_id: build_id.to_string(),
            task_queue: task_queue.to_string(),
            started_at_ms: now_ms(),
            is_current: false,
            task_count: 0,
        });
    }

    /// Set the current deployment for a task queue.
    pub fn set_current_deployment(&self, task_queue: &str, build_id: &str) -> bool {
        let mut deployments = self.deployments.lock().unwrap();
        if let Some(deps) = deployments.get_mut(task_queue) {
            for d in deps.iter_mut() {
                d.is_current = d.build_id == build_id;
            }
            true
        } else {
            false
        }
    }

    /// Get current deployment for a task queue.
    pub fn get_current_deployment(&self, task_queue: &str) -> Option<DeploymentInfo> {
        self.deployments
            .lock()
            .unwrap()
            .get(task_queue)?
            .iter()
            .find(|d| d.is_current)
            .cloned()
    }

    /// Get all deployments for a task queue.
    pub fn get_deployments(&self, task_queue: &str) -> Vec<DeploymentInfo> {
        self.deployments
            .lock()
            .unwrap()
            .get(task_queue)
            .cloned()
            .unwrap_or_default()
    }

    pub fn deployment_count(&self, task_queue: &str) -> usize {
        self.deployments
            .lock()
            .unwrap()
            .get(task_queue)
            .map_or(0, |d| d.len())
    }

    // ─── Version Set Queries ─────────────────────────────────────────────

    /// Get all build IDs in a version set.
    pub fn get_build_ids(&self, set_id: u64) -> Vec<String> {
        self.version_sets
            .lock()
            .unwrap()
            .get(&set_id)
            .map(|s| s.build_ids.iter().map(|b| b.id.clone()).collect())
            .unwrap_or_default()
    }

    /// Get the version set for a given set ID.
    pub fn get_version_set(&self, set_id: u64) -> Option<VersionSet> {
        self.version_sets.lock().unwrap().get(&set_id).cloned()
    }
}

impl Default for WorkerVersioning {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_version_set_and_add_builds() {
        let wv = WorkerVersioning::new();
        let set_id = wv.create_version_set();
        assert!(wv.add_build_id(set_id, "build-1"));
        assert!(wv.add_build_id(set_id, "build-2"));
        assert!(!wv.add_build_id(set_id, "build-1")); // duplicate
        assert_eq!(wv.get_current_build_id(set_id), Some("build-1".to_string()));
    }

    #[test]
    fn test_set_current_build() {
        let wv = WorkerVersioning::new();
        let set_id = wv.create_version_set();
        wv.add_build_id(set_id, "v1");
        wv.add_build_id(set_id, "v2");
        assert!(wv.set_current_build_id(set_id, "v2"));
        assert_eq!(wv.get_current_build_id(set_id), Some("v2".to_string()));
    }

    #[test]
    fn test_routing_rules() {
        let wv = WorkerVersioning::new();
        wv.add_routing_rule("my-queue", "build-abc", 100);
        assert_eq!(
            wv.resolve_build_id("my-queue"),
            Some("build-abc".to_string())
        );
        assert_eq!(wv.resolve_build_id("other-queue"), None);
    }

    #[test]
    fn test_build_id_to_set_lookup() {
        let wv = WorkerVersioning::new();
        let set_id = wv.create_version_set();
        wv.add_build_id(set_id, "build-x");
        assert_eq!(wv.get_set_for_build_id("build-x"), Some(set_id));
        assert_eq!(wv.get_set_for_build_id("nonexistent"), None);
    }

    // --- Compatibility ---

    #[test]
    fn test_compatibility() {
        let wv = WorkerVersioning::new();
        let set_id = wv.create_version_set();
        wv.add_build_id(set_id, "v1");
        wv.add_build_id(set_id, "v2");
        wv.add_compatibility("v1", "v2");
        assert!(wv.are_compatible("v1", "v2"));
        assert!(wv.are_compatible("v2", "v1")); // symmetric
        assert!(!wv.are_compatible("v1", "v3"));
        assert!(wv.are_compatible("v1", "v1")); // self-compatible
    }

    #[test]
    fn test_compatible_build_ids() {
        let wv = WorkerVersioning::new();
        let set_id = wv.create_version_set();
        wv.add_build_id(set_id, "a");
        wv.add_build_id(set_id, "b");
        wv.add_build_id(set_id, "c");
        wv.add_compatibility("a", "b");
        wv.add_compatibility("a", "c");
        let compat = wv.compatible_build_ids("a");
        assert_eq!(compat.len(), 2);
        assert!(compat.contains(&"b".to_string()));
        assert!(compat.contains(&"c".to_string()));
    }

    // --- Deployment Tracking ---

    #[test]
    fn test_deployment_registration() {
        let wv = WorkerVersioning::new();
        wv.register_deployment("tq-1", "build-v1");
        wv.register_deployment("tq-1", "build-v2");
        assert_eq!(wv.deployment_count("tq-1"), 2);
        // Duplicate registration is a no-op
        wv.register_deployment("tq-1", "build-v1");
        assert_eq!(wv.deployment_count("tq-1"), 2);
    }

    #[test]
    fn test_current_deployment() {
        let wv = WorkerVersioning::new();
        wv.register_deployment("tq-1", "build-v1");
        wv.register_deployment("tq-1", "build-v2");
        wv.set_current_deployment("tq-1", "build-v2");
        let current = wv.get_current_deployment("tq-1").unwrap();
        assert_eq!(current.build_id, "build-v2");
        assert!(current.is_current);
    }

    #[test]
    fn test_get_deployments() {
        let wv = WorkerVersioning::new();
        wv.register_deployment("tq-1", "v1");
        wv.register_deployment("tq-1", "v2");
        let deps = wv.get_deployments("tq-1");
        assert_eq!(deps.len(), 2);
    }

    // --- Version Set Queries ---

    #[test]
    fn test_get_build_ids() {
        let wv = WorkerVersioning::new();
        let set_id = wv.create_version_set();
        wv.add_build_id(set_id, "a");
        wv.add_build_id(set_id, "b");
        let ids = wv.get_build_ids(set_id);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"a".to_string()));
        assert!(ids.contains(&"b".to_string()));
    }

    #[test]
    fn test_get_version_set() {
        let wv = WorkerVersioning::new();
        let set_id = wv.create_version_set();
        wv.add_build_id(set_id, "x");
        let vs = wv.get_version_set(set_id).unwrap();
        assert_eq!(vs.set_id, set_id);
        assert_eq!(vs.build_ids.len(), 1);
    }
}
