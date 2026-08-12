//! Worker versioning — build ID tracking, version sets, and routing rules.
//! Enables safe deployment of new workflow code while old workflows continue on old versions.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, atomic::{AtomicU64, Ordering}};

#[derive(Debug, Clone)]
pub struct BuildId {
    pub id: String,
    pub created_at_ms: u64,
    pub is_current: bool,
}

#[derive(Debug, Clone)]
pub struct VersionSet {
    pub set_id: u64,
    pub build_ids: Vec<BuildId>,
    pub current_build_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RoutingRule {
    pub task_queue: String,
    pub target_build_id: String,
    pub percentage: u32, // 0-100 for ramping
}

pub struct WorkerVersioning {
    version_sets: Mutex<HashMap<u64, VersionSet>>,
    routing_rules: Mutex<Vec<RoutingRule>>,
    build_id_to_set: Mutex<HashMap<String, u64>>,
    next_set_id: AtomicU64,
}

impl WorkerVersioning {
    pub fn new() -> Self {
        Self {
            version_sets: Mutex::new(HashMap::new()),
            routing_rules: Mutex::new(Vec::new()),
            build_id_to_set: Mutex::new(HashMap::new()),
            next_set_id: AtomicU64::new(1),
        }
    }

    pub fn create_version_set(&self) -> u64 {
        let id = self.next_set_id.fetch_add(1, Ordering::Relaxed);
        self.version_sets.lock().unwrap().insert(id, VersionSet {
            set_id: id, build_ids: Vec::new(), current_build_id: None,
        });
        id
    }

    pub fn add_build_id(&self, set_id: u64, build_id: &str) -> bool {
        let mut sets = self.version_sets.lock().unwrap();
        if let Some(set) = sets.get_mut(&set_id) {
            if set.build_ids.iter().any(|b| b.id == build_id) { return false; }
            set.build_ids.push(BuildId { id: build_id.to_string(), created_at_ms: 0, is_current: false });
            if set.current_build_id.is_none() {
                set.current_build_id = Some(build_id.to_string());
            }
            self.build_id_to_set.lock().unwrap().insert(build_id.to_string(), set_id);
            true
        } else { false }
    }

    pub fn set_current_build_id(&self, set_id: u64, build_id: &str) -> bool {
        let mut sets = self.version_sets.lock().unwrap();
        if let Some(set) = sets.get_mut(&set_id) {
            if set.build_ids.iter().any(|b| b.id == build_id) {
                set.current_build_id = Some(build_id.to_string());
                for b in &mut set.build_ids { b.is_current = b.id == build_id; }
                true
            } else { false }
        } else { false }
    }

    pub fn get_current_build_id(&self, set_id: u64) -> Option<String> {
        self.version_sets.lock().unwrap().get(&set_id).and_then(|s| s.current_build_id.clone())
    }

    pub fn get_set_for_build_id(&self, build_id: &str) -> Option<u64> {
        self.build_id_to_set.lock().unwrap().get(build_id).copied()
    }

    pub fn add_routing_rule(&self, task_queue: &str, build_id: &str, percentage: u32) {
        self.routing_rules.lock().unwrap().push(RoutingRule {
            task_queue: task_queue.to_string(), target_build_id: build_id.to_string(), percentage,
        });
    }

    pub fn resolve_build_id(&self, task_queue: &str) -> Option<String> {
        let rules = self.routing_rules.lock().unwrap();
        rules.iter().find(|r| r.task_queue == task_queue).map(|r| r.target_build_id.clone())
    }

    pub fn version_set_count(&self) -> usize { self.version_sets.lock().unwrap().len() }
    pub fn routing_rule_count(&self) -> usize { self.routing_rules.lock().unwrap().len() }
}

impl Default for WorkerVersioning { fn default() -> Self { Self::new() } }

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
        assert_eq!(wv.resolve_build_id("my-queue"), Some("build-abc".to_string()));
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
}
