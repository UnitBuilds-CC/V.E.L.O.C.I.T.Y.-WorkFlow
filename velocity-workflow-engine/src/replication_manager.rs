//! Replication Manager — multi-cluster replication management.
//!
//! Manages replication between clusters, handles replication tasks,
//! conflict resolution, and replication lag monitoring.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, SystemTime};

// ═══════════════════════════════════════════════════════════════════════════════
// Cluster Replication Config
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct ClusterReplicationConfig {
    pub cluster_name: String,
    pub cluster_id: u16,
    pub initial_failover_version: i64,
    pub is_global_namespace_enabled: bool,
    pub is_connection_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct ReplicationClusterStatus {
    pub cluster_name: String,
    pub connected: bool,
    pub replication_lag: u64,
    pub last_replication_timestamp: i64,
    pub tasks_pending: u64,
    pub tasks_per_second: f64,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Replication Task
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct ReplicationTask {
    pub task_id: String,
    pub task_type: ReplicationTaskType,
    pub source_cluster: String,
    pub target_cluster: String,
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub version: i64,
    pub created_at: i64,
    pub status: ReplicationTaskStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationTaskType {
    SyncActivity,
    SyncWorkflowState,
    HistoryReplication,
    NamespaceReplication,
    SyncHsmState,
    BackfillHistory,
    SyncVersionedTransition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationTaskStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Retrying,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Replication Stream — bidirectional replication between clusters
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ReplicationStream {
    pub source_cluster: String,
    pub target_cluster: String,
    pub pending_tasks: RwLock<VecDeque<ReplicationTask>>,
    pub completed_tasks: RwLock<Vec<ReplicationTask>>,
    pub connected: AtomicBool,
    pub stats: ReplicationStreamStats,
}

#[derive(Debug, Default)]
pub struct ReplicationStreamStats {
    pub tasks_sent: AtomicU64,
    pub tasks_received: AtomicU64,
    pub tasks_completed: AtomicU64,
    pub tasks_failed: AtomicU64,
    pub bytes_transferred: AtomicU64,
    pub replication_lag_events: AtomicU64,
}

impl ReplicationStream {
    pub fn new(source: &str, target: &str) -> Self {
        Self {
            source_cluster: source.to_string(),
            target_cluster: target.to_string(),
            pending_tasks: RwLock::new(VecDeque::new()),
            completed_tasks: RwLock::new(Vec::new()),
            connected: AtomicBool::new(true),
            stats: ReplicationStreamStats::default(),
        }
    }

    pub fn enqueue_task(&self, task: ReplicationTask) {
        self.pending_tasks.write().unwrap().push_back(task);
    }

    pub fn process_next(&self) -> Option<ReplicationTask> {
        let mut task = self.pending_tasks.write().unwrap().pop_front()?;
        task.status = ReplicationTaskStatus::Completed;
        self.completed_tasks.write().unwrap().push(task.clone());
        self.stats.tasks_completed.fetch_add(1, Ordering::Relaxed);
        self.stats.tasks_sent.fetch_add(1, Ordering::Relaxed);
        Some(task)
    }

    pub fn pending_count(&self) -> usize {
        self.pending_tasks.read().unwrap().len()
    }
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }
    pub fn disconnect(&self) {
        self.connected.store(false, Ordering::Relaxed);
    }
    pub fn reconnect(&self) {
        self.connected.store(true, Ordering::Relaxed);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Conflict Resolver — resolves replication conflicts
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ConflictResolver {
    pub conflicts: RwLock<Vec<ReplicationConflict>>,
    pub resolution_policy: ConflictResolutionPolicy,
    pub stats: ConflictResolverStats,
}

#[derive(Debug, Clone)]
pub struct ReplicationConflict {
    pub conflict_id: String,
    pub namespace_id: String,
    pub workflow_id: String,
    pub local_version: i64,
    pub remote_version: i64,
    pub local_state: String,
    pub remote_state: String,
    pub detected_at: i64,
    pub resolution: Option<ConflictResolution>,
}

#[derive(Debug, Clone)]
pub enum ConflictResolution {
    LocalWins { reason: String },
    RemoteWins { reason: String },
    Merged { result_state: String },
    ManualIntervention { reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolutionPolicy {
    LocalWins,
    RemoteWins,
    HighestVersionWins,
    Manual,
}

#[derive(Debug, Default)]
pub struct ConflictResolverStats {
    pub conflicts_detected: AtomicU64,
    pub conflicts_resolved: AtomicU64,
    pub manual_interventions: AtomicU64,
}

impl ConflictResolver {
    pub fn new(policy: ConflictResolutionPolicy) -> Self {
        Self {
            conflicts: RwLock::new(Vec::new()),
            resolution_policy: policy,
            stats: ConflictResolverStats::default(),
        }
    }

    pub fn detect_conflict(
        &self,
        namespace_id: &str,
        workflow_id: &str,
        local_version: i64,
        remote_version: i64,
    ) -> String {
        let conflict_id = format!("conflict-{}", now_millis());
        let conflict = ReplicationConflict {
            conflict_id: conflict_id.clone(),
            namespace_id: namespace_id.to_string(),
            workflow_id: workflow_id.to_string(),
            local_version,
            remote_version,
            local_state: "unknown".into(),
            remote_state: "unknown".into(),
            detected_at: now_millis(),
            resolution: None,
        };
        self.conflicts.write().unwrap().push(conflict);
        self.stats
            .conflicts_detected
            .fetch_add(1, Ordering::Relaxed);
        conflict_id
    }

    pub fn resolve_conflict(&self, conflict_id: &str) -> Option<ConflictResolution> {
        let mut conflicts = self.conflicts.write().unwrap();
        let conflict = conflicts
            .iter_mut()
            .find(|c| c.conflict_id == conflict_id)?;
        let resolution = match self.resolution_policy {
            ConflictResolutionPolicy::LocalWins => ConflictResolution::LocalWins {
                reason: "Local cluster has priority".into(),
            },
            ConflictResolutionPolicy::RemoteWins => ConflictResolution::RemoteWins {
                reason: "Remote cluster has priority".into(),
            },
            ConflictResolutionPolicy::HighestVersionWins => {
                if conflict.local_version >= conflict.remote_version {
                    ConflictResolution::LocalWins {
                        reason: format!(
                            "local v{} >= remote v{}",
                            conflict.local_version, conflict.remote_version
                        ),
                    }
                } else {
                    ConflictResolution::RemoteWins {
                        reason: format!(
                            "remote v{} > local v{}",
                            conflict.remote_version, conflict.local_version
                        ),
                    }
                }
            }
            ConflictResolutionPolicy::Manual => {
                self.stats
                    .manual_interventions
                    .fetch_add(1, Ordering::Relaxed);
                ConflictResolution::ManualIntervention {
                    reason: "Requires manual resolution".into(),
                }
            }
        };
        conflict.resolution = Some(resolution.clone());
        self.stats
            .conflicts_resolved
            .fetch_add(1, Ordering::Relaxed);
        Some(resolution)
    }

    pub fn unresolved_count(&self) -> usize {
        self.conflicts
            .read()
            .unwrap()
            .iter()
            .filter(|c| c.resolution.is_none())
            .count()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Replication Manager — orchestrates all replication
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ReplicationManager {
    pub clusters: RwLock<HashMap<String, ClusterReplicationConfig>>,
    pub streams: RwLock<HashMap<String, Arc<ReplicationStream>>>,
    pub conflict_resolver: Arc<ConflictResolver>,
    pub stats: ReplicationManagerStats,
}

#[derive(Debug, Default)]
pub struct ReplicationManagerStats {
    pub clusters_registered: AtomicU64,
    pub streams_created: AtomicU64,
    pub total_tasks_replicated: AtomicU64,
}

impl ReplicationManager {
    pub fn new() -> Self {
        Self {
            clusters: RwLock::new(HashMap::new()),
            streams: RwLock::new(HashMap::new()),
            conflict_resolver: Arc::new(ConflictResolver::new(
                ConflictResolutionPolicy::HighestVersionWins,
            )),
            stats: ReplicationManagerStats::default(),
        }
    }

    pub fn register_cluster(&self, config: ClusterReplicationConfig) {
        self.clusters
            .write()
            .unwrap()
            .insert(config.cluster_name.clone(), config);
        self.stats
            .clusters_registered
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn create_stream(&self, source: &str, target: &str) -> Arc<ReplicationStream> {
        let stream = Arc::new(ReplicationStream::new(source, target));
        let key = format!("{}->{}", source, target);
        self.streams.write().unwrap().insert(key, stream.clone());
        self.stats.streams_created.fetch_add(1, Ordering::Relaxed);
        stream
    }

    pub fn replicate_task(&self, task: ReplicationTask) {
        let key = format!("{}->{}", task.source_cluster, task.target_cluster);
        if let Some(stream) = self.streams.read().unwrap().get(&key) {
            stream.enqueue_task(task);
        }
    }

    pub fn process_replication(&self) -> u64 {
        let streams = self.streams.read().unwrap();
        let mut processed = 0u64;
        for stream in streams.values() {
            while let Some(_task) = stream.process_next() {
                processed += 1;
            }
        }
        self.stats
            .total_tasks_replicated
            .fetch_add(processed, Ordering::Relaxed);
        processed
    }

    pub fn cluster_status(&self) -> Vec<ReplicationClusterStatus> {
        let clusters = self.clusters.read().unwrap();
        let streams = self.streams.read().unwrap();
        clusters
            .values()
            .map(|c| {
                let stream_key = format!("{}->", c.cluster_name);
                let connected = streams
                    .values()
                    .any(|s| s.source_cluster == c.cluster_name && s.is_connected());
                let lag = streams
                    .values()
                    .filter(|s| s.source_cluster == c.cluster_name)
                    .map(|s| s.pending_count() as u64)
                    .sum();
                ReplicationClusterStatus {
                    cluster_name: c.cluster_name.clone(),
                    connected,
                    replication_lag: lag,
                    last_replication_timestamp: now_millis(),
                    tasks_pending: lag,
                    tasks_per_second: 0.0,
                }
            })
            .collect()
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_stream() {
        let stream = ReplicationStream::new("us-east", "eu-west");
        assert!(stream.is_connected());
        let task = ReplicationTask {
            task_id: "t1".into(),
            task_type: ReplicationTaskType::HistoryReplication,
            source_cluster: "us-east".into(),
            target_cluster: "eu-west".into(),
            namespace_id: "ns1".into(),
            workflow_id: "wf1".into(),
            run_id: "r1".into(),
            version: 1,
            created_at: 0,
            status: ReplicationTaskStatus::Pending,
        };
        stream.enqueue_task(task);
        assert_eq!(stream.pending_count(), 1);
        let processed = stream.process_next();
        assert!(processed.is_some());
        assert_eq!(stream.pending_count(), 0);
    }

    #[test]
    fn test_stream_disconnect_reconnect() {
        let stream = ReplicationStream::new("a", "b");
        stream.disconnect();
        assert!(!stream.is_connected());
        stream.reconnect();
        assert!(stream.is_connected());
    }

    #[test]
    fn test_conflict_resolver_highest_version() {
        let resolver = ConflictResolver::new(ConflictResolutionPolicy::HighestVersionWins);
        let id = resolver.detect_conflict("ns1", "wf1", 5, 3);
        let resolution = resolver.resolve_conflict(&id).unwrap();
        assert!(matches!(resolution, ConflictResolution::LocalWins { .. }));
    }

    #[test]
    fn test_conflict_resolver_remote_wins() {
        let resolver = ConflictResolver::new(ConflictResolutionPolicy::HighestVersionWins);
        let id = resolver.detect_conflict("ns1", "wf1", 2, 10);
        let resolution = resolver.resolve_conflict(&id).unwrap();
        assert!(matches!(resolution, ConflictResolution::RemoteWins { .. }));
    }

    #[test]
    fn test_conflict_resolver_manual() {
        let resolver = ConflictResolver::new(ConflictResolutionPolicy::Manual);
        let id = resolver.detect_conflict("ns1", "wf1", 1, 1);
        let resolution = resolver.resolve_conflict(&id).unwrap();
        assert!(matches!(
            resolution,
            ConflictResolution::ManualIntervention { .. }
        ));
    }

    #[test]
    fn test_replication_manager() {
        let mgr = ReplicationManager::new();
        mgr.register_cluster(ClusterReplicationConfig {
            cluster_name: "us-east".into(),
            cluster_id: 1,
            initial_failover_version: 1,
            is_global_namespace_enabled: true,
            is_connection_enabled: true,
        });
        mgr.register_cluster(ClusterReplicationConfig {
            cluster_name: "eu-west".into(),
            cluster_id: 2,
            initial_failover_version: 2,
            is_global_namespace_enabled: true,
            is_connection_enabled: true,
        });
        let stream = mgr.create_stream("us-east", "eu-west");
        let task = ReplicationTask {
            task_id: "t1".into(),
            task_type: ReplicationTaskType::SyncActivity,
            source_cluster: "us-east".into(),
            target_cluster: "eu-west".into(),
            namespace_id: "ns".into(),
            workflow_id: "wf".into(),
            run_id: "r".into(),
            version: 1,
            created_at: 0,
            status: ReplicationTaskStatus::Pending,
        };
        mgr.replicate_task(task);
        assert_eq!(stream.pending_count(), 1);
        let processed = mgr.process_replication();
        assert_eq!(processed, 1);
    }

    #[test]
    fn test_cluster_status() {
        let mgr = ReplicationManager::new();
        mgr.register_cluster(ClusterReplicationConfig {
            cluster_name: "cluster-a".into(),
            cluster_id: 1,
            initial_failover_version: 1,
            is_global_namespace_enabled: true,
            is_connection_enabled: true,
        });
        let status = mgr.cluster_status();
        assert_eq!(status.len(), 1);
        assert_eq!(status[0].cluster_name, "cluster-a");
    }
}
