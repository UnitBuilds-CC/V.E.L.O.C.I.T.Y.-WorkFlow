//! NDC (cross-datacenter) replication subsystem.
//! Provides conflict resolution, task ack tracking, dead-letter queue,
//! namespace replication control, history gap detection, and cross-cluster
//! consistency verification — matching Temporal's XDC replication layer.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::cluster::{ReplicationTask, ReplicationTaskType, VersionHistory, VersionHistoryStore};

// ─── Conflict Resolution ─────────────────────────────────────────────────────

/// Outcome of a conflict resolution between local and remote state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Local state wins — remote task is stale and should be dropped.
    KeepLocal,
    /// Remote state wins — local state should be overwritten.
    AcceptRemote,
    /// States can be merged (no actual conflict).
    Merge,
    /// Conflict cannot be auto-resolved — requires manual intervention.
    Unresolvable,
}

/// A recorded conflict between two clusters.
#[derive(Debug, Clone)]
pub struct ReplicationConflict {
    pub workflow_key: u64,
    pub local_cluster_id: u64,
    pub remote_cluster_id: u64,
    pub local_version: u64,
    pub remote_version: u64,
    pub local_event_id: u64,
    pub remote_event_id: u64,
    pub resolution: ConflictResolution,
    pub detected_at_ms: u64,
}

/// Conflict resolver for NDC replication.
/// Uses vector-clock-style comparison of version histories to detect and resolve conflicts.
pub struct ConflictResolver {
    version_store: Arc<VersionHistoryStore>,
    local_cluster_id: u64,
    conflicts: Mutex<Vec<ReplicationConflict>>,
    next_conflict_id: AtomicU64,
}

impl ConflictResolver {
    pub fn new(version_store: Arc<VersionHistoryStore>, local_cluster_id: u64) -> Self {
        Self {
            version_store,
            local_cluster_id,
            conflicts: Mutex::new(Vec::new()),
            next_conflict_id: AtomicU64::new(1),
        }
    }

    /// Evaluate an incoming replication task against local state.
    /// Returns the resolution and whether the task should be applied.
    pub fn resolve(
        &self,
        task: &ReplicationTask,
        remote_version_history: &VersionHistory,
    ) -> ConflictResolution {
        let local_history = self.version_store.get_or_create(task.workflow_key);
        let local_version = local_history.current_version();
        let local_event_id = local_history.current_event_id();
        let remote_version = remote_version_history.current_version();
        let remote_event_id = remote_version_history.current_event_id();

        let resolution = if remote_version > local_version {
            // Remote has a higher failover version — it was the active cluster during a failover
            ConflictResolution::AcceptRemote
        } else if remote_version < local_version {
            // Remote is stale — local is the active cluster
            ConflictResolution::KeepLocal
        } else if remote_event_id > local_event_id {
            // Same version, remote has more events — accept if sequential
            if remote_event_id == local_event_id + 1
                || remote_version_history.contains(&local_history)
            {
                ConflictResolution::Merge
            } else {
                ConflictResolution::Unresolvable
            }
        } else if remote_event_id == local_event_id {
            // Same state — no conflict
            ConflictResolution::Merge
        } else {
            // Remote is behind local in the same version — keep local
            ConflictResolution::KeepLocal
        };

        // Record the conflict if it's not a simple merge
        if resolution != ConflictResolution::Merge {
            let mut conflicts = self.conflicts.lock().unwrap();
            conflicts.push(ReplicationConflict {
                workflow_key: task.workflow_key,
                local_cluster_id: self.local_cluster_id,
                remote_cluster_id: task.source_cluster_id,
                local_version,
                remote_version,
                local_event_id,
                remote_event_id,
                resolution,
                detected_at_ms: 0, // would use real clock
            });
        }

        resolution
    }

    /// Get all recorded conflicts.
    pub fn get_conflicts(&self) -> Vec<ReplicationConflict> {
        self.conflicts.lock().unwrap().clone()
    }

    /// Get conflicts for a specific workflow.
    pub fn get_conflicts_for_workflow(&self, workflow_key: u64) -> Vec<ReplicationConflict> {
        self.conflicts
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.workflow_key == workflow_key)
            .cloned()
            .collect()
    }

    /// Clear resolved conflicts.
    pub fn clear_conflicts(&self) {
        self.conflicts.lock().unwrap().clear();
    }

    /// Get total conflict count.
    pub fn conflict_count(&self) -> usize {
        self.conflicts.lock().unwrap().len()
    }
}

// ─── Task Ack Tracker ────────────────────────────────────────────────────────

/// State of a replication task in the ack tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskAckState {
    Pending,
    Acked,
    Nacked,
    TimedOut,
}

/// Record of a replication task being tracked for ack.
#[derive(Debug, Clone)]
pub struct TaskAckRecord {
    pub task_id: u64,
    pub workflow_key: u64,
    pub target_cluster_id: u64,
    pub state: TaskAckState,
    pub sent_at_ms: u64,
    pub acked_at_ms: Option<u64>,
    pub retry_count: u32,
}

/// Tracks replication task acknowledgements per remote cluster.
/// Detects gaps in ack sequences and triggers redelivery.
pub struct TaskAckTracker {
    /// cluster_id -> (task_id -> record)
    records: RwLock<HashMap<u64, HashMap<u64, TaskAckRecord>>>,
    /// cluster_id -> ordered list of pending task IDs (for gap detection)
    pending_sequences: RwLock<HashMap<u64, VecDeque<u64>>>,
    max_retries: u32,
    stats: RwLock<TaskAckTrackerStats>,
}

/// Stats for the task ack tracker.
#[derive(Debug, Clone, Default)]
pub struct TaskAckTrackerStats {
    pub total_tracked: u64,
    pub total_acked: u64,
    pub total_nacked: u64,
    pub total_timed_out: u64,
    pub total_retries: u64,
    pub pending_count: u64,
}

impl TaskAckTracker {
    pub fn new(max_retries: u32) -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            pending_sequences: RwLock::new(HashMap::new()),
            max_retries,
            stats: RwLock::new(TaskAckTrackerStats::default()),
        }
    }

    /// Track a newly sent replication task.
    pub fn track_sent(&self, cluster_id: u64, task_id: u64, workflow_key: u64) {
        let record = TaskAckRecord {
            task_id,
            workflow_key,
            target_cluster_id: cluster_id,
            state: TaskAckState::Pending,
            sent_at_ms: 0,
            acked_at_ms: None,
            retry_count: 0,
        };

        self.records
            .write()
            .unwrap()
            .entry(cluster_id)
            .or_default()
            .insert(task_id, record);

        self.pending_sequences
            .write()
            .unwrap()
            .entry(cluster_id)
            .or_default()
            .push_back(task_id);

        self.stats.write().unwrap().total_tracked += 1;
    }

    /// Record an acknowledgement from a remote cluster.
    pub fn record_ack(&self, cluster_id: u64, task_id: u64) -> bool {
        let mut records = self.records.write().unwrap();
        if let Some(cluster_records) = records.get_mut(&cluster_id) {
            if let Some(record) = cluster_records.get_mut(&task_id) {
                record.state = TaskAckState::Acked;
                record.acked_at_ms = Some(0); // would use real clock
                self.stats.write().unwrap().total_acked += 1;
                // Remove from pending sequence
                if let Some(seq) = self.pending_sequences.write().unwrap().get_mut(&cluster_id) {
                    seq.retain(|&id| id != task_id);
                }
                return true;
            }
        }
        false
    }

    /// Record a negative acknowledgement (task rejected by remote).
    pub fn record_nack(&self, cluster_id: u64, task_id: u64) -> bool {
        let mut records = self.records.write().unwrap();
        if let Some(cluster_records) = records.get_mut(&cluster_id) {
            if let Some(record) = cluster_records.get_mut(&task_id) {
                record.retry_count += 1;
                if record.retry_count >= self.max_retries {
                    record.state = TaskAckState::Nacked;
                    self.stats.write().unwrap().total_nacked += 1;
                    // Remove from pending sequence
                    if let Some(seq) = self.pending_sequences.write().unwrap().get_mut(&cluster_id)
                    {
                        seq.retain(|&id| id != task_id);
                    }
                } else {
                    self.stats.write().unwrap().total_retries += 1;
                }
                return true;
            }
        }
        false
    }

    /// Detect gaps in the ack sequence for a cluster.
    /// Returns task IDs that appear to be missing (sent but not acked).
    pub fn detect_gaps(&self, cluster_id: u64) -> Vec<u64> {
        let seqs = self.pending_sequences.read().unwrap();
        seqs.get(&cluster_id)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get tasks that need redelivery (pending and exceeded retry threshold).
    pub fn get_redelivery_candidates(&self, cluster_id: u64) -> Vec<TaskAckRecord> {
        let records = self.records.read().unwrap();
        records
            .get(&cluster_id)
            .map(|m| {
                m.values()
                    .filter(|r| r.state == TaskAckState::Pending && r.retry_count > 0)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get pending count for a cluster.
    pub fn pending_count(&self, cluster_id: u64) -> usize {
        self.pending_sequences
            .read()
            .unwrap()
            .get(&cluster_id)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// Get tracker statistics.
    pub fn stats(&self) -> TaskAckTrackerStats {
        let mut s = self.stats.read().unwrap().clone();
        s.pending_count = self
            .pending_sequences
            .read()
            .unwrap()
            .values()
            .map(|v| v.len() as u64)
            .sum();
        s
    }
}

// ─── Dead Letter Queue (DLQ) ─────────────────────────────────────────────────

/// A task in the dead letter queue.
#[derive(Debug, Clone)]
pub struct DlqTask {
    pub original_task: ReplicationTask,
    pub failure_reason: String,
    pub failure_count: u32,
    pub first_failure_ms: u64,
    pub last_failure_ms: u64,
}

/// Dead letter queue for replication tasks that cannot be applied after multiple retries.
pub struct ReplicationDlq {
    /// cluster_id -> DLQ tasks
    queues: RwLock<HashMap<u64, VecDeque<DlqTask>>>,
    max_queue_size: usize,
    stats: RwLock<DlqStats>,
}

/// DLQ statistics.
#[derive(Debug, Clone, Default)]
pub struct DlqStats {
    pub total_enqueued: u64,
    pub total_processed: u64,
    pub total_dropped: u64,
    pub current_size: u64,
}

impl ReplicationDlq {
    pub fn new(max_queue_size: usize) -> Self {
        Self {
            queues: RwLock::new(HashMap::new()),
            max_queue_size,
            stats: RwLock::new(DlqStats::default()),
        }
    }

    /// Enqueue a failed task to the DLQ.
    pub fn enqueue(
        &self,
        cluster_id: u64,
        task: ReplicationTask,
        reason: &str,
        failure_count: u32,
    ) {
        let dlq_task = DlqTask {
            original_task: task,
            failure_reason: reason.to_string(),
            failure_count,
            first_failure_ms: 0,
            last_failure_ms: 0,
        };

        let mut queues = self.queues.write().unwrap();
        let queue = queues.entry(cluster_id).or_default();
        if queue.len() < self.max_queue_size {
            queue.push_back(dlq_task);
            self.stats.write().unwrap().total_enqueued += 1;
        } else {
            // Drop oldest to make room
            queue.pop_front();
            queue.push_back(dlq_task);
            self.stats.write().unwrap().total_dropped += 1;
        }
    }

    /// Peek at the next DLQ task for a cluster (without removing).
    pub fn peek(&self, cluster_id: u64) -> Option<DlqTask> {
        self.queues
            .read()
            .unwrap()
            .get(&cluster_id)?
            .front()
            .cloned()
    }

    /// Process (remove) the next DLQ task for a cluster.
    pub fn process_next(&self, cluster_id: u64) -> Option<DlqTask> {
        let mut queues = self.queues.write().unwrap();
        let task = queues.get_mut(&cluster_id)?.pop_front();
        if task.is_some() {
            self.stats.write().unwrap().total_processed += 1;
        }
        task
    }

    /// Get the number of DLQ tasks for a cluster.
    pub fn queue_size(&self, cluster_id: u64) -> usize {
        self.queues
            .read()
            .unwrap()
            .get(&cluster_id)
            .map(|q| q.len())
            .unwrap_or(0)
    }

    /// Get total DLQ size across all clusters.
    pub fn total_size(&self) -> usize {
        self.queues.read().unwrap().values().map(|q| q.len()).sum()
    }

    /// Get DLQ statistics.
    pub fn stats(&self) -> DlqStats {
        let mut s = self.stats.read().unwrap().clone();
        s.current_size = self.total_size() as u64;
        s
    }

    /// List all cluster IDs that have DLQ entries.
    pub fn clusters_with_dlq(&self) -> Vec<u64> {
        self.queues
            .read()
            .unwrap()
            .iter()
            .filter(|(_, q)| !q.is_empty())
            .map(|(id, _)| *id)
            .collect()
    }
}

// ─── Namespace Replication Controller ─────────────────────────────────────────

/// Configuration for namespace replication to remote clusters.
#[derive(Debug, Clone)]
pub struct NamespaceReplicationConfig {
    pub namespace_name: String,
    pub replicated_to: HashSet<u64>, // cluster IDs
    pub is_global: bool,
}

/// Controls which namespaces are replicated to which clusters.
pub struct NamespaceReplicationController {
    /// namespace_name -> replication config
    configs: RwLock<HashMap<String, NamespaceReplicationConfig>>,
    local_cluster_id: u64,
}

impl NamespaceReplicationController {
    pub fn new(local_cluster_id: u64) -> Self {
        Self {
            configs: RwLock::new(HashMap::new()),
            local_cluster_id,
        }
    }

    /// Register a namespace for replication to specific clusters.
    pub fn register_namespace(&self, name: &str, target_clusters: Vec<u64>, is_global: bool) {
        let mut targets: HashSet<u64> = target_clusters.into_iter().collect();
        targets.insert(self.local_cluster_id); // always include local
        self.configs.write().unwrap().insert(
            name.to_string(),
            NamespaceReplicationConfig {
                namespace_name: name.to_string(),
                replicated_to: targets,
                is_global,
            },
        );
    }

    /// Add a cluster to a namespace's replication targets.
    pub fn add_replication_target(&self, namespace: &str, cluster_id: u64) -> bool {
        let mut configs = self.configs.write().unwrap();
        if let Some(config) = configs.get_mut(namespace) {
            config.replicated_to.insert(cluster_id);
            true
        } else {
            false
        }
    }

    /// Remove a cluster from a namespace's replication targets.
    pub fn remove_replication_target(&self, namespace: &str, cluster_id: u64) -> bool {
        let mut configs = self.configs.write().unwrap();
        if let Some(config) = configs.get_mut(namespace) {
            if cluster_id == self.local_cluster_id {
                return false; // can't remove local
            }
            config.replicated_to.remove(&cluster_id);
            true
        } else {
            false
        }
    }

    /// Check if a namespace should be replicated to a specific cluster.
    pub fn should_replicate_to(&self, namespace: &str, cluster_id: u64) -> bool {
        self.configs
            .read()
            .unwrap()
            .get(namespace)
            .map(|c| c.replicated_to.contains(&cluster_id))
            .unwrap_or(false)
    }

    /// Get the replication config for a namespace.
    pub fn get_config(&self, namespace: &str) -> Option<NamespaceReplicationConfig> {
        self.configs.read().unwrap().get(namespace).cloned()
    }

    /// List all registered namespaces.
    pub fn list_namespaces(&self) -> Vec<String> {
        self.configs.read().unwrap().keys().cloned().collect()
    }

    /// Get the clusters that a namespace is replicated to.
    pub fn get_replication_targets(&self, namespace: &str) -> HashSet<u64> {
        self.configs
            .read()
            .unwrap()
            .get(namespace)
            .map(|c| c.replicated_to.clone())
            .unwrap_or_default()
    }

    /// Get the count of global namespaces.
    pub fn global_namespace_count(&self) -> usize {
        self.configs
            .read()
            .unwrap()
            .values()
            .filter(|c| c.is_global)
            .count()
    }
}

// ─── History Gap Detector ────────────────────────────────────────────────────

/// A detected gap in the event history sequence.
#[derive(Debug, Clone)]
pub struct HistoryGap {
    pub workflow_key: u64,
    pub source_cluster_id: u64,
    pub expected_event_id: u64,
    pub actual_event_id: u64,
    pub gap_size: u64,
    pub detected_at_ms: u64,
}

/// Detects gaps in replicated event histories.
/// Tracks the last seen event ID per workflow per cluster and flags discontinuities.
pub struct HistoryGapDetector {
    /// (workflow_key, cluster_id) -> last_seen_event_id
    last_seen: RwLock<HashMap<(u64, u64), u64>>,
    gaps: Mutex<Vec<HistoryGap>>,
}

impl HistoryGapDetector {
    pub fn new() -> Self {
        Self {
            last_seen: RwLock::new(HashMap::new()),
            gaps: Mutex::new(Vec::new()),
        }
    }

    /// Record receipt of an event from a remote cluster.
    /// Returns Some(gap) if a gap was detected.
    pub fn record_event(
        &self,
        workflow_key: u64,
        cluster_id: u64,
        event_id: u64,
    ) -> Option<HistoryGap> {
        let mut last_seen = self.last_seen.write().unwrap();
        let key = (workflow_key, cluster_id);

        if let Some(&last) = last_seen.get(&key) {
            if event_id > last + 1 {
                let gap = HistoryGap {
                    workflow_key,
                    source_cluster_id: cluster_id,
                    expected_event_id: last + 1,
                    actual_event_id: event_id,
                    gap_size: event_id - last - 1,
                    detected_at_ms: 0,
                };
                self.gaps.lock().unwrap().push(gap.clone());
                last_seen.insert(key, event_id);
                return Some(gap);
            }
        }

        last_seen.insert(key, event_id);
        None
    }

    /// Get all detected gaps.
    pub fn get_gaps(&self) -> Vec<HistoryGap> {
        self.gaps.lock().unwrap().clone()
    }

    /// Get gaps for a specific workflow.
    pub fn get_gaps_for_workflow(&self, workflow_key: u64) -> Vec<HistoryGap> {
        self.gaps
            .lock()
            .unwrap()
            .iter()
            .filter(|g| g.workflow_key == workflow_key)
            .cloned()
            .collect()
    }

    /// Get the total number of detected gaps.
    pub fn gap_count(&self) -> usize {
        self.gaps.lock().unwrap().len()
    }

    /// Clear all tracked state (for testing or reset).
    pub fn reset(&self) {
        self.last_seen.write().unwrap().clear();
        self.gaps.lock().unwrap().clear();
    }
}

impl Default for HistoryGapDetector {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Cross-Cluster Consistency Checker ───────────────────────────────────────

/// Result of a consistency check between two clusters.
#[derive(Debug, Clone)]
pub struct ConsistencyCheckResult {
    pub workflow_key: u64,
    pub cluster_a_id: u64,
    pub cluster_b_id: u64,
    pub is_consistent: bool,
    pub cluster_a_version: u64,
    pub cluster_b_version: u64,
    pub cluster_a_event_id: u64,
    pub cluster_b_event_id: u64,
    pub divergence_point: u64,
}

/// Verifies eventual consistency across clusters by comparing version histories.
pub struct ConsistencyChecker {
    version_store: Arc<VersionHistoryStore>,
    /// (workflow_key, cluster_id) -> version history snapshot
    cluster_histories: RwLock<HashMap<(u64, u64), VersionHistory>>,
}

impl ConsistencyChecker {
    pub fn new(version_store: Arc<VersionHistoryStore>) -> Self {
        Self {
            version_store,
            cluster_histories: RwLock::new(HashMap::new()),
        }
    }

    /// Record a cluster's version history for a workflow.
    pub fn record_cluster_state(
        &self,
        workflow_key: u64,
        cluster_id: u64,
        history: VersionHistory,
    ) {
        self.cluster_histories
            .write()
            .unwrap()
            .insert((workflow_key, cluster_id), history);
    }

    /// Check consistency between two clusters for a workflow.
    pub fn check_pair(
        &self,
        workflow_key: u64,
        cluster_a: u64,
        cluster_b: u64,
    ) -> ConsistencyCheckResult {
        let histories = self.cluster_histories.read().unwrap();
        let ha = histories.get(&(workflow_key, cluster_a));
        let hb = histories.get(&(workflow_key, cluster_b));

        match (ha, hb) {
            (Some(a), Some(b)) => {
                let divergence = a.find_divergence_point(b);
                let is_consistent = a.current_version() == b.current_version()
                    && a.current_event_id() == b.current_event_id();
                ConsistencyCheckResult {
                    workflow_key,
                    cluster_a_id: cluster_a,
                    cluster_b_id: cluster_b,
                    is_consistent,
                    cluster_a_version: a.current_version(),
                    cluster_b_version: b.current_version(),
                    cluster_a_event_id: a.current_event_id(),
                    cluster_b_event_id: b.current_event_id(),
                    divergence_point: divergence,
                }
            }
            _ => ConsistencyCheckResult {
                workflow_key,
                cluster_a_id: cluster_a,
                cluster_b_id: cluster_b,
                is_consistent: false,
                cluster_a_version: 0,
                cluster_b_version: 0,
                cluster_a_event_id: 0,
                cluster_b_event_id: 0,
                divergence_point: 0,
            },
        }
    }

    /// Check consistency of a workflow across all known clusters.
    pub fn check_all_clusters(
        &self,
        workflow_key: u64,
        cluster_ids: &[u64],
    ) -> Vec<ConsistencyCheckResult> {
        let mut results = Vec::new();
        for i in 0..cluster_ids.len() {
            for j in (i + 1)..cluster_ids.len() {
                results.push(self.check_pair(workflow_key, cluster_ids[i], cluster_ids[j]));
            }
        }
        results
    }

    /// Count workflows that are inconsistent across any pair of clusters.
    pub fn count_inconsistent_workflows(&self, cluster_ids: &[u64]) -> usize {
        let histories = self.cluster_histories.read().unwrap();
        let mut workflow_keys: HashSet<u64> = HashSet::new();
        for ((wk, _), _) in histories.iter() {
            workflow_keys.insert(*wk);
        }

        let mut inconsistent = 0;
        for wk in workflow_keys {
            let results = self.check_all_clusters(wk, cluster_ids);
            if results.iter().any(|r| !r.is_consistent) {
                inconsistent += 1;
            }
        }
        inconsistent
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(workflow_key: u64, source: u64, version: u64, event_id: u64) -> ReplicationTask {
        ReplicationTask {
            task_id: event_id,
            source_cluster_id: source,
            target_cluster_id: 0,
            workflow_key,
            event_type: 1,
            payload: vec![],
            failover_version: version,
            task_type: ReplicationTaskType::SyncHistory,
            first_event_id: event_id,
            last_event_id: event_id,
            created_ms: 0,
        }
    }

    // ─── Conflict Resolver Tests ──────────────────────────────────────────

    #[test]
    fn test_conflict_accept_remote_higher_version() {
        let store = Arc::new(VersionHistoryStore::new());
        store.record_event(1, 1, 10); // local: version=1, event=10
        let resolver = ConflictResolver::new(store, 0);

        let mut remote_vh = VersionHistory::new();
        remote_vh.add_item(2, 15); // remote: version=2

        let task = make_task(1, 1, 2, 15);
        let resolution = resolver.resolve(&task, &remote_vh);
        assert_eq!(resolution, ConflictResolution::AcceptRemote);
    }

    #[test]
    fn test_conflict_keep_local_higher_version() {
        let store = Arc::new(VersionHistoryStore::new());
        store.record_event(1, 3, 20); // local: version=3
        let resolver = ConflictResolver::new(store, 0);

        let mut remote_vh = VersionHistory::new();
        remote_vh.add_item(1, 10); // remote: version=1

        let task = make_task(1, 1, 1, 10);
        let resolution = resolver.resolve(&task, &remote_vh);
        assert_eq!(resolution, ConflictResolution::KeepLocal);
    }

    #[test]
    fn test_conflict_merge_same_state() {
        let store = Arc::new(VersionHistoryStore::new());
        store.record_event(1, 1, 10);
        let resolver = ConflictResolver::new(store, 0);

        let mut remote_vh = VersionHistory::new();
        remote_vh.add_item(1, 10); // same state

        let task = make_task(1, 1, 1, 10);
        let resolution = resolver.resolve(&task, &remote_vh);
        assert_eq!(resolution, ConflictResolution::Merge);
    }

    #[test]
    fn test_conflict_recording() {
        let store = Arc::new(VersionHistoryStore::new());
        store.record_event(1, 3, 20);
        let resolver = ConflictResolver::new(store.clone(), 0);

        let mut remote_vh = VersionHistory::new();
        remote_vh.add_item(1, 10);

        let task = make_task(1, 1, 1, 10);
        resolver.resolve(&task, &remote_vh);

        assert_eq!(resolver.conflict_count(), 1);
        let conflicts = resolver.get_conflicts();
        assert_eq!(conflicts[0].resolution, ConflictResolution::KeepLocal);
    }

    // ─── Task Ack Tracker Tests ───────────────────────────────────────────

    #[test]
    fn test_ack_tracker_basic() {
        let tracker = TaskAckTracker::new(3);
        tracker.track_sent(2, 100, 42);
        tracker.track_sent(2, 101, 43);
        assert_eq!(tracker.pending_count(2), 2);

        assert!(tracker.record_ack(2, 100));
        assert_eq!(tracker.pending_count(2), 1);

        let stats = tracker.stats();
        assert_eq!(stats.total_tracked, 2);
        assert_eq!(stats.total_acked, 1);
    }

    #[test]
    fn test_ack_tracker_nack_retries() {
        let tracker = TaskAckTracker::new(3);
        tracker.track_sent(2, 100, 42);

        // Nack twice — still pending
        tracker.record_nack(2, 100);
        tracker.record_nack(2, 100);
        assert_eq!(tracker.pending_count(2), 1); // still pending

        // Third nack — exceeds max retries, becomes nacked
        tracker.record_nack(2, 100);
        assert_eq!(tracker.pending_count(2), 0); // removed from pending

        let stats = tracker.stats();
        assert_eq!(stats.total_nacked, 1);
        assert_eq!(stats.total_retries, 2);
    }

    #[test]
    fn test_ack_tracker_gap_detection() {
        let tracker = TaskAckTracker::new(5);
        tracker.track_sent(2, 100, 42);
        tracker.track_sent(2, 101, 43);
        tracker.track_sent(2, 102, 44);

        tracker.record_ack(2, 100);
        tracker.record_ack(2, 102);

        let gaps = tracker.detect_gaps(2);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0], 101);
    }

    // ─── DLQ Tests ────────────────────────────────────────────────────────

    #[test]
    fn test_dlq_enqueue_and_process() {
        let dlq = ReplicationDlq::new(100);
        let task = make_task(42, 1, 1, 10);
        dlq.enqueue(2, task, "apply failed", 3);

        assert_eq!(dlq.queue_size(2), 1);
        assert_eq!(dlq.total_size(), 1);

        let peeked = dlq.peek(2).unwrap();
        assert_eq!(peeked.original_task.workflow_key, 42);
        assert_eq!(peeked.failure_reason, "apply failed");

        let processed = dlq.process_next(2).unwrap();
        assert_eq!(processed.original_task.workflow_key, 42);
        assert_eq!(dlq.queue_size(2), 0);
    }

    #[test]
    fn test_dlq_max_size_eviction() {
        let dlq = ReplicationDlq::new(2);
        for i in 0..3 {
            dlq.enqueue(2, make_task(i, 1, 1, i as u64), "fail", 5);
        }
        // Only 2 should remain (oldest evicted)
        assert_eq!(dlq.queue_size(2), 2);
        let stats = dlq.stats();
        assert_eq!(stats.total_dropped, 1);
    }

    #[test]
    fn test_dlq_clusters_with_entries() {
        let dlq = ReplicationDlq::new(100);
        dlq.enqueue(2, make_task(1, 2, 1, 1), "fail", 1);
        dlq.enqueue(3, make_task(2, 3, 1, 1), "fail", 1);
        dlq.enqueue(2, make_task(3, 2, 1, 2), "fail", 1);

        let clusters = dlq.clusters_with_dlq();
        assert_eq!(clusters.len(), 2);
        assert!(clusters.contains(&2));
        assert!(clusters.contains(&3));
    }

    // ─── Namespace Replication Controller Tests ───────────────────────────

    #[test]
    fn test_namespace_register_and_query() {
        let ctrl = NamespaceReplicationController::new(0);
        ctrl.register_namespace("payments", vec![1, 2], true);

        assert!(ctrl.should_replicate_to("payments", 0)); // local always included
        assert!(ctrl.should_replicate_to("payments", 1));
        assert!(ctrl.should_replicate_to("payments", 2));
        assert!(!ctrl.should_replicate_to("payments", 3));
    }

    #[test]
    fn test_namespace_add_remove_target() {
        let ctrl = NamespaceReplicationController::new(0);
        ctrl.register_namespace("orders", vec![1], false);

        ctrl.add_replication_target("orders", 5);
        assert!(ctrl.should_replicate_to("orders", 5));

        ctrl.remove_replication_target("orders", 5);
        assert!(!ctrl.should_replicate_to("orders", 5));
    }

    #[test]
    fn test_namespace_cannot_remove_local() {
        let ctrl = NamespaceReplicationController::new(0);
        ctrl.register_namespace("test", vec![], false);
        assert!(!ctrl.remove_replication_target("test", 0)); // can't remove local
        assert!(ctrl.should_replicate_to("test", 0)); // still there
    }

    #[test]
    fn test_namespace_global_count() {
        let ctrl = NamespaceReplicationController::new(0);
        ctrl.register_namespace("ns1", vec![], true);
        ctrl.register_namespace("ns2", vec![], true);
        ctrl.register_namespace("ns3", vec![], false);
        assert_eq!(ctrl.global_namespace_count(), 2);
    }

    // ─── History Gap Detector Tests ───────────────────────────────────────

    #[test]
    fn test_gap_detector_no_gap() {
        let det = HistoryGapDetector::new();
        assert!(det.record_event(1, 2, 1).is_none());
        assert!(det.record_event(1, 2, 2).is_none());
        assert!(det.record_event(1, 2, 3).is_none());
        assert_eq!(det.gap_count(), 0);
    }

    #[test]
    fn test_gap_detector_detects_gap() {
        let det = HistoryGapDetector::new();
        det.record_event(1, 2, 1);
        let gap = det.record_event(1, 2, 5); // skipped 2, 3, 4

        assert!(gap.is_some());
        let gap = gap.unwrap();
        assert_eq!(gap.expected_event_id, 2);
        assert_eq!(gap.actual_event_id, 5);
        assert_eq!(gap.gap_size, 3);
        assert_eq!(det.gap_count(), 1);
    }

    #[test]
    fn test_gap_detector_per_cluster() {
        let det = HistoryGapDetector::new();
        det.record_event(1, 2, 10); // cluster 2
        det.record_event(1, 3, 20); // cluster 3 — independent tracking
        assert!(det.record_event(1, 2, 11).is_none()); // cluster 2 sequential
        assert!(det.record_event(1, 3, 25).is_some()); // cluster 3 gap
    }

    #[test]
    fn test_gap_detector_get_gaps_for_workflow() {
        let det = HistoryGapDetector::new();
        det.record_event(1, 2, 1);
        det.record_event(1, 2, 5); // gap
        det.record_event(2, 2, 10);
        det.record_event(2, 2, 20); // gap

        let wf1_gaps = det.get_gaps_for_workflow(1);
        assert_eq!(wf1_gaps.len(), 1);

        let wf2_gaps = det.get_gaps_for_workflow(2);
        assert_eq!(wf2_gaps.len(), 1);
    }

    // ─── Consistency Checker Tests ────────────────────────────────────────

    #[test]
    fn test_consistency_check_consistent() {
        let store = Arc::new(VersionHistoryStore::new());
        let checker = ConsistencyChecker::new(store);

        let mut vh = VersionHistory::new();
        vh.add_item(1, 10);
        checker.record_cluster_state(42, 1, vh.clone());
        checker.record_cluster_state(42, 2, vh);

        let result = checker.check_pair(42, 1, 2);
        assert!(result.is_consistent);
    }

    #[test]
    fn test_consistency_check_inconsistent() {
        let store = Arc::new(VersionHistoryStore::new());
        let checker = ConsistencyChecker::new(store);

        let mut vh1 = VersionHistory::new();
        vh1.add_item(1, 10);
        checker.record_cluster_state(42, 1, vh1);

        let mut vh2 = VersionHistory::new();
        vh2.add_item(2, 20); // different version
        checker.record_cluster_state(42, 2, vh2);

        let result = checker.check_pair(42, 1, 2);
        assert!(!result.is_consistent);
        assert_eq!(result.cluster_a_version, 1);
        assert_eq!(result.cluster_b_version, 2);
    }

    #[test]
    fn test_consistency_check_all_pairs() {
        let store = Arc::new(VersionHistoryStore::new());
        let checker = ConsistencyChecker::new(store);

        let mut vh = VersionHistory::new();
        vh.add_item(1, 10);
        for cluster_id in &[1, 2, 3] {
            checker.record_cluster_state(42, *cluster_id, vh.clone());
        }

        let results = checker.check_all_clusters(42, &[1, 2, 3]);
        assert_eq!(results.len(), 3); // 3 pairs
        assert!(results.iter().all(|r| r.is_consistent));
    }

    #[test]
    fn test_consistency_count_inconsistent() {
        let store = Arc::new(VersionHistoryStore::new());
        let checker = ConsistencyChecker::new(store);

        // Workflow 1: consistent
        let mut vh = VersionHistory::new();
        vh.add_item(1, 10);
        checker.record_cluster_state(1, 1, vh.clone());
        checker.record_cluster_state(1, 2, vh);

        // Workflow 2: inconsistent
        let mut vh_a = VersionHistory::new();
        vh_a.add_item(1, 10);
        let mut vh_b = VersionHistory::new();
        vh_b.add_item(2, 20);
        checker.record_cluster_state(2, 1, vh_a);
        checker.record_cluster_state(2, 2, vh_b);

        assert_eq!(checker.count_inconsistent_workflows(&[1, 2]), 1);
    }
}
