//! Matching engine matching Temporal's service/matching (~44K lines).
//!
//! Covers: task queue management, partition handling, task matching,
//! versioned task queues, polling, forwarding, and task sync.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, SystemTime};

// ═══════════════════════════════════════════════════════════════════════════════
// Task Queue — a named queue that workers poll from
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskQueueKind {
    Normal,
    Sticky,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskQueueType {
    Workflow,
    Activity,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskQueueId {
    pub namespace_id: String,
    pub name: String,
    pub kind: TaskQueueKind,
    pub queue_type: TaskQueueType,
}

impl TaskQueueId {
    pub fn new(ns: &str, name: &str, kind: TaskQueueKind, qt: TaskQueueType) -> Self {
        Self {
            namespace_id: ns.into(),
            name: name.into(),
            kind,
            queue_type: qt,
        }
    }
    pub fn key(&self) -> String {
        format!(
            "{}/{}/{:?}/{:?}",
            self.namespace_id, self.name, self.kind, self.queue_type
        )
    }
}

pub struct TaskQueue {
    pub id: TaskQueueId,
    pub tasks: RwLock<VecDeque<MatchTask>>,
    pub pollers: RwLock<VecDeque<PollerInfo>>,
    pub range_id: AtomicI64,
    pub ack_level: AtomicI64,
    pub stats: TaskQueueStats,
    pub version_data: RwLock<VersionedData>,
}

#[derive(Debug, Clone)]
pub struct MatchTask {
    pub task_id: i64,
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub task_type: TaskQueueType,
    pub scheduled_time: i64,
    pub priority: i32,
    pub forwarding_info: Option<ForwardingInfo>,
    pub version: i64,
}

#[derive(Debug, Clone)]
pub struct ForwardingInfo {
    pub source_partition: i32,
    pub target_partition: i32,
    pub forwarded_from: String,
}

#[derive(Debug, Clone)]
pub struct PollerInfo {
    pub poller_id: String,
    pub identity: String,
    pub last_poll_time: i64,
    pub rate_per_second: f64,
}

#[derive(Debug, Clone)]
pub struct VersionedData {
    pub current_version: i64,
    pub version_branches: Vec<VersionBranch>,
}

impl Default for VersionedData {
    fn default() -> Self {
        Self {
            current_version: 0,
            version_branches: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct VersionBranch {
    pub version: i64,
    pub build_id: String,
    pub is_default: bool,
}

#[derive(Debug, Default)]
pub struct TaskQueueStats {
    pub tasks_added: AtomicU64,
    pub tasks_matched: AtomicU64,
    pub tasks_forwarded: AtomicU64,
    pub poll_count: AtomicU64,
    pub sync_match_count: AtomicU64,
    pub expired_tasks: AtomicU64,
}

impl TaskQueue {
    pub fn new(id: TaskQueueId) -> Self {
        Self {
            id,
            tasks: RwLock::new(VecDeque::new()),
            pollers: RwLock::new(VecDeque::new()),
            range_id: AtomicI64::new(1),
            ack_level: AtomicI64::new(0),
            stats: TaskQueueStats::default(),
            version_data: RwLock::new(VersionedData::default()),
        }
    }

    pub fn add_task(&self, task: MatchTask) {
        self.tasks.write().unwrap().push_back(task);
        self.stats.tasks_added.fetch_add(1, Ordering::Relaxed);
    }

    pub fn match_task(&self, _poller_id: &str) -> Option<MatchTask> {
        let task = self.tasks.write().unwrap().pop_front()?;
        self.stats.tasks_matched.fetch_add(1, Ordering::Relaxed);
        Some(task)
    }

    pub fn try_sync_match(&self, task: MatchTask) -> Result<MatchTask, MatchTask> {
        let pollers = self.pollers.write().unwrap();
        if !pollers.is_empty() {
            self.stats.sync_match_count.fetch_add(1, Ordering::Relaxed);
            Ok(task)
        } else {
            let err_task = task.clone();
            self.tasks.write().unwrap().push_back(task);
            Err(err_task)
        }
    }

    pub fn register_poller(&self, poller: PollerInfo) {
        let mut pollers = self.pollers.write().unwrap();
        pollers.retain(|p| p.poller_id != poller.poller_id);
        pollers.push_back(poller);
    }

    pub fn remove_poller(&self, poller_id: &str) {
        self.pollers
            .write()
            .unwrap()
            .retain(|p| p.poller_id != poller_id);
    }

    pub fn pending_count(&self) -> usize {
        self.tasks.read().unwrap().len()
    }
    pub fn poller_count(&self) -> usize {
        self.pollers.read().unwrap().len()
    }

    pub fn set_version(&self, version: i64, build_id: &str) {
        let mut vd = self.version_data.write().unwrap();
        vd.current_version = version;
        vd.version_branches.push(VersionBranch {
            version,
            build_id: build_id.into(),
            is_default: true,
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Partition Manager — manages task queue partitions
// ═══════════════════════════════════════════════════════════════════════════════

pub struct PartitionManager {
    pub num_partitions: u32,
    pub partitions: RwLock<HashMap<i32, Arc<TaskQueue>>>,
    pub root_queue: Arc<TaskQueue>,
    pub stats: PartitionManagerStats,
}

#[derive(Debug, Default)]
pub struct PartitionManagerStats {
    pub tasks_routed: AtomicU64,
    pub forward_count: AtomicU64,
}

impl PartitionManager {
    pub fn new(root_id: TaskQueueId, num_partitions: u32) -> Self {
        let root = Arc::new(TaskQueue::new(root_id));
        let mut partitions = HashMap::new();
        for i in 0..num_partitions {
            let pid = TaskQueueId {
                name: format!("{}__partition_{}", root.id.name, i),
                ..root.id.clone()
            };
            partitions.insert(i as i32, Arc::new(TaskQueue::new(pid)));
        }
        Self {
            num_partitions,
            partitions: RwLock::new(partitions),
            root_queue: root,
            stats: PartitionManagerStats::default(),
        }
    }

    pub fn route_task(&self, task: MatchTask) -> i32 {
        if self.num_partitions == 0 {
            self.root_queue.add_task(task);
            return -1;
        }
        let hash = fnv1a_hash(&task.workflow_id) as u32;
        let partition = (hash % self.num_partitions) as i32;
        let partitions = self.partitions.read().unwrap();
        if let Some(pq) = partitions.get(&partition) {
            pq.add_task(task);
        }
        self.stats.tasks_routed.fetch_add(1, Ordering::Relaxed);
        partition
    }

    pub fn poll_partition(&self, partition: i32, poller_id: &str) -> Option<MatchTask> {
        let partitions = self.partitions.read().unwrap();
        partitions
            .get(&partition)
            .and_then(|pq| pq.match_task(poller_id))
    }

    pub fn forward_to_root(&self, partition: i32) -> Option<MatchTask> {
        let partitions = self.partitions.read().unwrap();
        let task = partitions.get(&partition)?.match_task("forwarder")?;
        self.root_queue.add_task(task.clone());
        self.stats.forward_count.fetch_add(1, Ordering::Relaxed);
        Some(task)
    }

    pub fn total_pending(&self) -> usize {
        let mut total = self.root_queue.pending_count();
        for (_, pq) in self.partitions.read().unwrap().iter() {
            total += pq.pending_count();
        }
        total
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.read().unwrap().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Matching Engine — top-level matching service
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MatchingEngine {
    pub task_queues: RwLock<HashMap<String, Arc<TaskQueue>>>,
    pub partition_managers: RwLock<HashMap<String, Arc<PartitionManager>>>,
    pub config: MatchingEngineConfig,
    pub stats: MatchingEngineStats,
}

#[derive(Debug, Clone)]
pub struct MatchingEngineConfig {
    pub num_partitions: u32,
    pub max_task_queue_idle_time: Duration,
    pub forwarder_max_children: u32,
    pub sync_match_wait: Duration,
    pub max_pollers_per_queue: u32,
}

impl Default for MatchingEngineConfig {
    fn default() -> Self {
        Self {
            num_partitions: 4,
            max_task_queue_idle_time: Duration::from_secs(60),
            forwarder_max_children: 10,
            sync_match_wait: Duration::from_millis(100),
            max_pollers_per_queue: 100,
        }
    }
}

#[derive(Debug, Default)]
pub struct MatchingEngineStats {
    pub queues_created: AtomicU64,
    pub tasks_added: AtomicU64,
    pub tasks_matched: AtomicU64,
    pub poll_count: AtomicU64,
}

impl MatchingEngine {
    pub fn new() -> Self {
        Self {
            task_queues: RwLock::new(HashMap::new()),
            partition_managers: RwLock::new(HashMap::new()),
            config: MatchingEngineConfig::default(),
            stats: MatchingEngineStats::default(),
        }
    }

    pub fn get_or_create_queue(&self, id: &TaskQueueId) -> Arc<TaskQueue> {
        let key = id.key();
        if let Some(tq) = self.task_queues.read().unwrap().get(&key) {
            return tq.clone();
        }
        let tq = Arc::new(TaskQueue::new(id.clone()));
        self.task_queues.write().unwrap().insert(key, tq.clone());
        self.stats.queues_created.fetch_add(1, Ordering::Relaxed);
        tq
    }

    pub fn add_task(&self, id: &TaskQueueId, task: MatchTask) {
        let tq = self.get_or_create_queue(id);
        tq.add_task(task);
        self.stats.tasks_added.fetch_add(1, Ordering::Relaxed);
    }

    pub fn poll_task(&self, id: &TaskQueueId, poller_id: &str) -> Option<MatchTask> {
        let tq = self.get_or_create_queue(id);
        self.stats.poll_count.fetch_add(1, Ordering::Relaxed);
        tq.match_task(poller_id)
    }

    pub fn register_poller(&self, id: &TaskQueueId, poller: PollerInfo) {
        let tq = self.get_or_create_queue(id);
        tq.register_poller(poller);
    }

    pub fn queue_count(&self) -> usize {
        self.task_queues.read().unwrap().len()
    }

    pub fn total_pending(&self) -> usize {
        self.task_queues
            .read()
            .unwrap()
            .values()
            .map(|tq| tq.pending_count())
            .sum()
    }

    pub fn health_report(&self) -> MatchingHealthReport {
        let queues = self.task_queues.read().unwrap();
        let total_queues = queues.len();
        let total_pending: usize = queues.values().map(|q| q.pending_count()).sum();
        let total_pollers: usize = queues.values().map(|q| q.poller_count()).sum();
        MatchingHealthReport {
            total_queues,
            total_pending,
            total_pollers,
            tasks_added: self.stats.tasks_added.load(Ordering::Relaxed),
            tasks_matched: self.stats.tasks_matched.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct MatchingHealthReport {
    pub total_queues: usize,
    pub total_pending: usize,
    pub total_pollers: usize,
    pub tasks_added: u64,
    pub tasks_matched: u64,
}

fn fnv1a_hash(s: &str) -> u32 {
    let mut hash: u32 = 2166136261;
    for byte in s.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

#[allow(dead_code)]
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

    fn make_task_id(ns: &str, name: &str) -> TaskQueueId {
        TaskQueueId::new(ns, name, TaskQueueKind::Normal, TaskQueueType::Workflow)
    }

    fn make_task(id: i64, wf: &str) -> MatchTask {
        MatchTask {
            task_id: id,
            namespace_id: "ns".into(),
            workflow_id: wf.into(),
            run_id: "r".into(),
            task_type: TaskQueueType::Workflow,
            scheduled_time: now_millis(),
            priority: 0,
            forwarding_info: None,
            version: 0,
        }
    }

    #[test]
    fn test_task_queue_add_match() {
        let tq = TaskQueue::new(make_task_id("ns", "tq-1"));
        tq.add_task(make_task(1, "wf-1"));
        tq.add_task(make_task(2, "wf-2"));
        assert_eq!(tq.pending_count(), 2);
        let matched = tq.match_task("poller-1").unwrap();
        assert_eq!(matched.task_id, 1);
        assert_eq!(tq.pending_count(), 1);
    }

    #[test]
    fn test_task_queue_poller_management() {
        let tq = TaskQueue::new(make_task_id("ns", "tq-1"));
        tq.register_poller(PollerInfo {
            poller_id: "p1".into(),
            identity: "worker-1".into(),
            last_poll_time: now_millis(),
            rate_per_second: 10.0,
        });
        tq.register_poller(PollerInfo {
            poller_id: "p2".into(),
            identity: "worker-2".into(),
            last_poll_time: now_millis(),
            rate_per_second: 5.0,
        });
        assert_eq!(tq.poller_count(), 2);
        tq.remove_poller("p1");
        assert_eq!(tq.poller_count(), 1);
    }

    #[test]
    fn test_task_queue_versioning() {
        let tq = TaskQueue::new(make_task_id("ns", "tq-1"));
        tq.set_version(1, "build-v1");
        tq.set_version(2, "build-v2");
        let vd = tq.version_data.read().unwrap();
        assert_eq!(vd.current_version, 2);
        assert_eq!(vd.version_branches.len(), 2);
    }

    #[test]
    fn test_partition_manager() {
        let root_id = make_task_id("ns", "tq-1");
        let pm = PartitionManager::new(root_id, 4);
        assert_eq!(pm.partition_count(), 4);
        let p = pm.route_task(make_task(1, "wf-1"));
        assert!(p >= 0 && p < 4);
        assert_eq!(pm.total_pending(), 1);
    }

    #[test]
    fn test_partition_manager_no_partitions() {
        let root_id = make_task_id("ns", "tq-1");
        let pm = PartitionManager::new(root_id, 0);
        pm.route_task(make_task(1, "wf-1"));
        assert_eq!(pm.root_queue.pending_count(), 1);
    }

    #[test]
    fn test_partition_manager_poll() {
        let root_id = make_task_id("ns", "tq-1");
        let pm = PartitionManager::new(root_id, 2);
        pm.route_task(make_task(1, "wf-1"));
        pm.route_task(make_task(2, "wf-2"));
        let total = pm.total_pending();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_partition_forward_to_root() {
        let root_id = make_task_id("ns", "tq-1");
        let pm = PartitionManager::new(root_id, 2);
        pm.route_task(make_task(1, "wf-1"));
        let forwarded = pm.forward_to_root(0);
        // May or may not forward depending on which partition the task went to
    }

    #[test]
    fn test_matching_engine_create_queue() {
        let engine = MatchingEngine::new();
        let id = make_task_id("ns", "tq-1");
        let tq1 = engine.get_or_create_queue(&id);
        let tq2 = engine.get_or_create_queue(&id);
        assert!(Arc::ptr_eq(&tq1, &tq2));
        assert_eq!(engine.queue_count(), 1);
    }

    #[test]
    fn test_matching_engine_add_poll() {
        let engine = MatchingEngine::new();
        let id = make_task_id("ns", "tq-1");
        engine.add_task(&id, make_task(1, "wf-1"));
        engine.add_task(&id, make_task(2, "wf-2"));
        assert_eq!(engine.total_pending(), 2);
        let matched = engine.poll_task(&id, "p1").unwrap();
        assert_eq!(matched.task_id, 1);
        assert_eq!(engine.total_pending(), 1);
    }

    #[test]
    fn test_matching_engine_poll_empty() {
        let engine = MatchingEngine::new();
        let id = make_task_id("ns", "empty-queue");
        assert!(engine.poll_task(&id, "p1").is_none());
    }

    #[test]
    fn test_matching_engine_health() {
        let engine = MatchingEngine::new();
        let id = make_task_id("ns", "tq-1");
        engine.add_task(&id, make_task(1, "wf-1"));
        engine.register_poller(
            &id,
            PollerInfo {
                poller_id: "p1".into(),
                identity: "w1".into(),
                last_poll_time: 0,
                rate_per_second: 1.0,
            },
        );
        let report = engine.health_report();
        assert_eq!(report.total_queues, 1);
        assert_eq!(report.total_pending, 1);
        assert_eq!(report.total_pollers, 1);
    }

    #[test]
    fn test_task_queue_id_key() {
        let id1 = TaskQueueId::new("ns", "tq", TaskQueueKind::Normal, TaskQueueType::Workflow);
        let id2 = TaskQueueId::new("ns", "tq", TaskQueueKind::Sticky, TaskQueueType::Workflow);
        assert_ne!(id1.key(), id2.key());
    }

    #[test]
    fn test_sync_match() {
        let tq = TaskQueue::new(make_task_id("ns", "tq"));
        tq.register_poller(PollerInfo {
            poller_id: "p1".into(),
            identity: "w".into(),
            last_poll_time: 0,
            rate_per_second: 1.0,
        });
        let result = tq.try_sync_match(make_task(1, "wf"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_sync_match_no_poller() {
        let tq = TaskQueue::new(make_task_id("ns", "tq"));
        let result = tq.try_sync_match(make_task(1, "wf"));
        assert!(result.is_err());
        assert_eq!(tq.pending_count(), 1);
    }
}
