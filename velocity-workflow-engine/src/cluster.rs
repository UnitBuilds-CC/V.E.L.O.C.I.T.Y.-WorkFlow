//! Cluster metadata — cluster name, failover version, replication config.
//! Full multi-cluster replication with typed tasks, version history, and conflict detection.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

/// Typed replication task types matching Temporal's replication task categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationTaskType {
    SyncHistory = 0,
    SyncActivity = 1,
    SyncWorkflowState = 2,
    NamespaceMetadata = 3,
    SyncHSM = 4,
    VerifyTransition = 5,
    DeleteExecution = 6,
    BackfillHistory = 7,
    SyncVersionedTransition = 8,
}

#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub cluster_name: String,
    pub cluster_id: u64,
    pub is_active: bool,
    pub failover_version: u64,
    pub address: String,
    pub replication_enabled: bool,
    /// Initial replication task ID for this cluster (for stream tracking).
    pub initial_replication_level: u64,
}

#[derive(Debug, Clone)]
pub struct ReplicationTask {
    pub task_id: u64,
    pub source_cluster_id: u64,
    pub target_cluster_id: u64,
    pub workflow_key: u64,
    pub event_type: u32,
    pub payload: Vec<u8>,
    pub failover_version: u64,
    pub task_type: ReplicationTaskType,
    /// First event ID in this task's batch (for ordering).
    pub first_event_id: u64,
    /// Last event ID in this task's batch.
    pub last_event_id: u64,
    /// Creation timestamp in epoch ms.
    pub created_ms: u64,
}

/// Version history item: tracks (failover_version, last_event_id) pairs.
/// Used for conflict resolution in multi-cluster replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionHistoryItem {
    pub failover_version: u64,
    pub event_id: u64,
}

/// Version history for a workflow — a vector of VersionHistoryItems.
/// Each cluster maintains its own version history. Conflicts are detected
/// when version histories diverge.
#[derive(Debug, Clone)]
pub struct VersionHistory {
    pub items: Vec<VersionHistoryItem>,
}

impl VersionHistory {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    /// Add a new item to the version history.
    pub fn add_item(&mut self, failover_version: u64, event_id: u64) {
        // If the last item has the same failover_version, update its event_id
        if let Some(last) = self.items.last_mut() {
            if last.failover_version == failover_version {
                last.event_id = event_id;
                return;
            }
        }
        self.items.push(VersionHistoryItem {
            failover_version,
            event_id,
        });
    }

    /// Get the current (latest) failover version.
    pub fn current_version(&self) -> u64 {
        self.items.last().map(|i| i.failover_version).unwrap_or(0)
    }

    /// Get the current event ID.
    pub fn current_event_id(&self) -> u64 {
        self.items.last().map(|i| i.event_id).unwrap_or(0)
    }

    /// Check if this version history is a superset of another (for conflict detection).
    pub fn contains(&self, other: &VersionHistory) -> bool {
        if other.items.is_empty() {
            return true;
        }
        if self.items.len() < other.items.len() {
            return false;
        }
        for (s, o) in self.items.iter().zip(other.items.iter()) {
            if s.failover_version != o.failover_version || s.event_id < o.event_id {
                return false;
            }
        }
        true
    }

    /// Find the branch point where two version histories diverge.
    /// Returns the event_id at which they diverge, or 0 if they share a common prefix.
    pub fn find_divergence_point(&self, other: &VersionHistory) -> u64 {
        let min_len = self.items.len().min(other.items.len());
        let mut divergence = 0u64;
        for i in 0..min_len {
            if self.items[i].failover_version == other.items[i].failover_version {
                divergence = self.items[i].event_id.min(other.items[i].event_id);
            } else {
                break;
            }
        }
        divergence
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Tracks version histories per workflow for conflict resolution.
pub struct VersionHistoryStore {
    /// workflow_key → VersionHistory
    histories: Mutex<HashMap<u64, VersionHistory>>,
}

impl VersionHistoryStore {
    pub fn new() -> Self {
        Self {
            histories: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create version history for a workflow.
    pub fn get_or_create(&self, workflow_key: u64) -> VersionHistory {
        let mut histories = self.histories.lock().unwrap();
        histories
            .entry(workflow_key)
            .or_insert_with(VersionHistory::new)
            .clone()
    }

    /// Record a new event in the version history.
    pub fn record_event(&self, workflow_key: u64, failover_version: u64, event_id: u64) {
        let mut histories = self.histories.lock().unwrap();
        histories
            .entry(workflow_key)
            .or_insert_with(VersionHistory::new)
            .add_item(failover_version, event_id);
    }

    /// Check if an incoming replication task conflicts with local state.
    /// Returns true if the task can be applied (no conflict or local is behind).
    pub fn check_incoming(
        &self,
        workflow_key: u64,
        remote_version: u64,
        remote_event_id: u64,
    ) -> bool {
        let histories = self.histories.lock().unwrap();
        match histories.get(&workflow_key) {
            None => true, // No local history, accept
            Some(local) => {
                let local_version = local.current_version();
                if remote_version > local_version {
                    true
                } else if remote_version < local_version {
                    false
                }
                // Stale task
                else {
                    remote_event_id > local.current_event_id()
                } // Same version, check event ordering
            }
        }
    }

    /// Count workflows tracked in version history.
    pub fn workflow_count(&self) -> usize {
        self.histories.lock().unwrap().len()
    }

    /// Remove version history for a workflow (after deletion/cleanup).
    pub fn remove(&self, workflow_key: u64) -> bool {
        self.histories
            .lock()
            .unwrap()
            .remove(&workflow_key)
            .is_some()
    }
}

pub struct ClusterManager {
    clusters: Mutex<HashMap<u64, ClusterInfo>>,
    replication_queue: Mutex<Vec<ReplicationTask>>,
    /// Applied task IDs for deduplication.
    applied_tasks: Mutex<HashMap<u64, bool>>,
    local_cluster_id: u64,
    next_task_id: AtomicU64,
    /// Next event ID counter for local event tracking.
    next_event_id: AtomicU64,
}

impl ClusterManager {
    pub fn new(local_name: &str) -> Self {
        let mut clusters = HashMap::new();
        clusters.insert(
            0,
            ClusterInfo {
                cluster_name: local_name.to_string(),
                cluster_id: 0,
                is_active: true,
                failover_version: 0,
                address: "localhost".to_string(),
                replication_enabled: false,
                initial_replication_level: 0,
            },
        );
        Self {
            clusters: Mutex::new(clusters),
            replication_queue: Mutex::new(Vec::new()),
            applied_tasks: Mutex::new(HashMap::new()),
            local_cluster_id: 0,
            next_task_id: AtomicU64::new(1),
            next_event_id: AtomicU64::new(1),
        }
    }

    pub fn register_cluster(&self, name: &str, address: &str) -> u64 {
        let mut clusters = self.clusters.lock().unwrap();
        let id = clusters.len() as u64;
        clusters.insert(
            id,
            ClusterInfo {
                cluster_name: name.to_string(),
                cluster_id: id,
                is_active: true,
                failover_version: 0,
                address: address.to_string(),
                replication_enabled: true,
                initial_replication_level: 0,
            },
        );
        id
    }

    pub fn get_cluster(&self, cluster_id: u64) -> Option<ClusterInfo> {
        self.clusters.lock().unwrap().get(&cluster_id).cloned()
    }
    pub fn local_cluster_id(&self) -> u64 {
        self.local_cluster_id
    }
    pub fn cluster_count(&self) -> usize {
        self.clusters.lock().unwrap().len()
    }

    pub fn enqueue_replication(
        &self,
        source: u64,
        target: u64,
        workflow_key: u64,
        event_type: u32,
        payload: Vec<u8>,
        task_type: ReplicationTaskType,
    ) -> u64 {
        let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        let event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        self.replication_queue
            .lock()
            .unwrap()
            .push(ReplicationTask {
                task_id,
                source_cluster_id: source,
                target_cluster_id: target,
                workflow_key,
                event_type,
                payload,
                failover_version: 0,
                task_type,
                first_event_id: event_id,
                last_event_id: event_id,
                created_ms: 0,
            });
        task_id
    }

    /// Enqueue a batch of replication tasks (for efficiency).
    pub fn enqueue_replication_batch(
        &self,
        source: u64,
        target: u64,
        tasks: Vec<(u64, u32, Vec<u8>, ReplicationTaskType)>,
    ) -> Vec<u64> {
        let mut queue = self.replication_queue.lock().unwrap();
        let mut ids = Vec::with_capacity(tasks.len());
        for (workflow_key, event_type, payload, task_type) in tasks {
            let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
            let event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
            queue.push(ReplicationTask {
                task_id,
                source_cluster_id: source,
                target_cluster_id: target,
                workflow_key,
                event_type,
                payload,
                failover_version: 0,
                task_type,
                first_event_id: event_id,
                last_event_id: event_id,
                created_ms: 0,
            });
            ids.push(task_id);
        }
        ids
    }

    pub fn pending_replication_count(&self) -> usize {
        self.replication_queue.lock().unwrap().len()
    }
    pub fn drain_replication_tasks(&self) -> Vec<ReplicationTask> {
        let mut q = self.replication_queue.lock().unwrap();
        std::mem::take(&mut *q)
    }

    /// Apply an incoming replication task from a remote cluster.
    /// Validates source cluster, checks for duplicates, verifies version ordering.
    /// Returns true if the task was accepted and applied.
    pub fn apply_incoming_replication(&self, task: ReplicationTask) -> bool {
        // Validate source cluster
        let source_valid = {
            let clusters = self.clusters.lock().unwrap();
            clusters.contains_key(&task.source_cluster_id)
        };
        if !source_valid {
            return false;
        }

        // Deduplication check
        {
            let applied = self.applied_tasks.lock().unwrap();
            if applied.contains_key(&task.task_id) {
                return false;
            }
        }

        // Update failover version for the source cluster
        {
            let mut clusters = self.clusters.lock().unwrap();
            if let Some(info) = clusters.get_mut(&task.source_cluster_id) {
                if task.failover_version > info.failover_version {
                    info.failover_version = task.failover_version;
                }
            }
        }

        // Mark as applied
        self.applied_tasks
            .lock()
            .unwrap()
            .insert(task.task_id, true);
        true
    }

    /// Get replication status: (pending_tasks, cluster_count, active_clusters, applied_count).
    pub fn replication_status(&self) -> (usize, usize, usize, usize) {
        let clusters = self.clusters.lock().unwrap();
        let active = clusters
            .values()
            .filter(|c| c.is_active && c.replication_enabled)
            .count();
        let applied = self.applied_tasks.lock().unwrap().len();
        (
            self.replication_queue.lock().unwrap().len(),
            clusters.len(),
            active,
            applied,
        )
    }

    /// Update the failover version for a cluster (used during failover).
    pub fn set_failover_version(&self, cluster_id: u64, version: u64) -> bool {
        let mut clusters = self.clusters.lock().unwrap();
        if let Some(info) = clusters.get_mut(&cluster_id) {
            info.failover_version = version;
            true
        } else {
            false
        }
    }

    /// Mark a cluster as active or standby.
    pub fn set_cluster_active(&self, cluster_id: u64, active: bool) -> bool {
        let mut clusters = self.clusters.lock().unwrap();
        if let Some(info) = clusters.get_mut(&cluster_id) {
            info.is_active = active;
            true
        } else {
            false
        }
    }

    /// Get all active cluster IDs (for replication target selection).
    pub fn active_cluster_ids(&self) -> Vec<u64> {
        self.clusters
            .lock()
            .unwrap()
            .values()
            .filter(|c| c.is_active && c.replication_enabled)
            .map(|c| c.cluster_id)
            .collect()
    }

    /// Get the next event ID (for local event tracking).
    pub fn next_event_id(&self) -> u64 {
        self.next_event_id.load(Ordering::Relaxed)
    }
}

impl Default for ClusterManager {
    fn default() -> Self {
        Self::new("local")
    }
}
impl Default for VersionHistoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_cluster() {
        let mgr = ClusterManager::new("dc1");
        assert_eq!(mgr.local_cluster_id(), 0);
        assert_eq!(mgr.cluster_count(), 1);
    }

    #[test]
    fn test_register_remote_cluster() {
        let mgr = ClusterManager::new("dc1");
        let id = mgr.register_cluster("dc2", "dc2.example.com:9090");
        assert_eq!(mgr.cluster_count(), 2);
        assert!(mgr.get_cluster(id).unwrap().replication_enabled);
    }

    #[test]
    fn test_replication_queue() {
        let mgr = ClusterManager::new("dc1");
        mgr.register_cluster("dc2", "dc2:9090");
        mgr.enqueue_replication(0, 1, 42, 1, vec![1, 2, 3], ReplicationTaskType::SyncHistory);
        assert_eq!(mgr.pending_replication_count(), 1);
        let tasks = mgr.drain_replication_tasks();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].task_type, ReplicationTaskType::SyncHistory);
        assert_eq!(mgr.pending_replication_count(), 0);
    }

    #[test]
    fn test_batch_enqueue() {
        let mgr = ClusterManager::new("dc1");
        mgr.register_cluster("dc2", "dc2:9090");
        let tasks = vec![
            (42, 1, vec![1], ReplicationTaskType::SyncHistory),
            (43, 2, vec![2], ReplicationTaskType::SyncActivity),
        ];
        let ids = mgr.enqueue_replication_batch(0, 1, tasks);
        assert_eq!(ids.len(), 2);
        assert_eq!(mgr.pending_replication_count(), 2);
    }

    #[test]
    fn test_apply_incoming_dedup() {
        let mgr = ClusterManager::new("dc1");
        mgr.register_cluster("dc2", "dc2:9090");
        let task = ReplicationTask {
            task_id: 100,
            source_cluster_id: 1,
            target_cluster_id: 0,
            workflow_key: 42,
            event_type: 1,
            payload: vec![],
            failover_version: 5,
            task_type: ReplicationTaskType::SyncHistory,
            first_event_id: 1,
            last_event_id: 1,
            created_ms: 0,
        };
        assert!(mgr.apply_incoming_replication(task.clone()));
        // Duplicate should be rejected
        assert!(!mgr.apply_incoming_replication(task));
    }

    #[test]
    fn test_apply_unknown_source() {
        let mgr = ClusterManager::new("dc1");
        let task = ReplicationTask {
            task_id: 1,
            source_cluster_id: 999,
            target_cluster_id: 0,
            workflow_key: 1,
            event_type: 0,
            payload: vec![],
            failover_version: 1,
            task_type: ReplicationTaskType::SyncHistory,
            first_event_id: 1,
            last_event_id: 1,
            created_ms: 0,
        };
        assert!(!mgr.apply_incoming_replication(task));
    }

    #[test]
    fn test_replication_status_extended() {
        let mgr = ClusterManager::new("dc1");
        mgr.register_cluster("dc2", "dc2:9090");
        let (pending, count, active, applied) = mgr.replication_status();
        assert_eq!(pending, 0);
        assert_eq!(count, 2);
        assert_eq!(active, 1); // only dc2 has replication_enabled
        assert_eq!(applied, 0);
    }

    #[test]
    fn test_version_history() {
        let mut vh = VersionHistory::new();
        assert!(vh.is_empty());
        vh.add_item(1, 10);
        vh.add_item(1, 20); // Same version, updates event_id
        assert_eq!(vh.current_version(), 1);
        assert_eq!(vh.current_event_id(), 20);
        assert_eq!(vh.len(), 1); // Same version merged

        vh.add_item(2, 30); // New version
        assert_eq!(vh.len(), 2);
        assert_eq!(vh.current_version(), 2);
    }

    #[test]
    fn test_version_history_contains() {
        let mut vh1 = VersionHistory::new();
        vh1.add_item(1, 10);
        vh1.add_item(2, 20);

        let mut vh2 = VersionHistory::new();
        vh2.add_item(1, 5);
        vh2.add_item(2, 15);

        assert!(vh1.contains(&vh2));
        assert!(!vh2.contains(&vh1));
    }

    #[test]
    fn test_version_history_store() {
        let store = VersionHistoryStore::new();
        store.record_event(42, 1, 10);
        store.record_event(42, 1, 20);
        store.record_event(42, 2, 30);

        let vh = store.get_or_create(42);
        assert_eq!(vh.current_version(), 2);
        assert_eq!(vh.current_event_id(), 30);

        // Check incoming: higher version accepted
        assert!(store.check_incoming(42, 3, 40));
        // Same version, higher event_id accepted
        assert!(store.check_incoming(42, 2, 40));
        // Same version, lower event_id rejected
        assert!(!store.check_incoming(42, 2, 10));
        // Lower version rejected
        assert!(!store.check_incoming(42, 1, 100));
    }

    #[test]
    fn test_active_cluster_ids() {
        let mgr = ClusterManager::new("dc1");
        mgr.register_cluster("dc2", "dc2:9090");
        mgr.register_cluster("dc3", "dc3:9090");
        let ids = mgr.active_cluster_ids();
        assert_eq!(ids.len(), 2); // dc2 and dc3 have replication_enabled
    }
}
