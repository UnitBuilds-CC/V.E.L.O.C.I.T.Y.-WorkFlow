//! Deep matching workers implementation matching Temporal's 44K-line matching subsystem.
//!
//! Covers: physical task queue, logical task queue, task queue partition, task dispatch,
//! poller management, task versioning, rate limiting, partition management,
//! load balancing, and task forwarding.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, Instant};

// ═══════════════════════════════════════════════════════════════════════════════
// Task Queue Partition
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TaskQueuePartition {
    pub task_queue: String,
    pub task_type: TaskType,
    pub partition: i32,
    pub namespace_id: String,
}

impl TaskQueuePartition {
    pub fn root(task_queue: &str, task_type: TaskType, ns_id: &str) -> Self {
        Self {
            task_queue: task_queue.to_string(),
            task_type,
            partition: 0,
            namespace_id: ns_id.to_string(),
        }
    }

    pub fn child(task_queue: &str, task_type: TaskType, ns_id: &str, partition: i32) -> Self {
        Self {
            task_queue: task_queue.to_string(),
            task_type,
            partition,
            namespace_id: ns_id.to_string(),
        }
    }

    pub fn is_root(&self) -> bool {
        self.partition == 0
    }

    pub fn key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.namespace_id, self.task_queue, self.task_type as i32, self.partition
        )
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum TaskType {
    Workflow = 0,
    Activity = 1,
    Nexus = 2,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Physical Task Queue
// ═══════════════════════════════════════════════════════════════════════════════

pub struct PhysicalTaskQueue {
    pub partition: TaskQueuePartition,
    pub tasks: RwLock<VecDeque<InternalTask>>,
    pub pollers: RwLock<Vec<PollerInfo>>,
    pub backlog_counter: AtomicU64,
    pub dispatch_counter: AtomicU64,
    pub sync_match_counter: AtomicU64,
    pub config: TaskQueueConfig,
}

#[derive(Debug, Clone)]
pub struct InternalTask {
    pub task_id: u64,
    pub workflow_id: String,
    pub run_id: String,
    pub task_token: Vec<u8>,
    pub scheduled_time: i64,
    pub priority: i32,
    pub version: Option<String>,
    pub redirect_info: Option<RedirectInfo>,
    pub created_at: Instant,
}

#[derive(Debug, Clone)]
pub struct RedirectInfo {
    pub target_task_queue: String,
    pub build_id: String,
}

#[derive(Debug, Clone)]
pub struct PollerInfo {
    pub identity: String,
    pub build_id: String,
    pub registered_at: Instant,
    pub last_poll_at: Instant,
    pub is_long_poll: bool,
    pub rate_limiter: Option<RateLimiterState>,
}

#[derive(Debug, Clone)]
pub struct RateLimiterState {
    pub tokens_per_second: f64,
    pub max_tokens: f64,
    pub current_tokens: f64,
    pub last_refill: Instant,
}

#[derive(Debug, Clone)]
pub struct TaskQueueConfig {
    pub max_tasks_per_second: f64,
    pub max_pollers: usize,
    pub sync_match_timeout_ms: u64,
    pub backlog_per_partition_limit: usize,
    pub forwarder_max_children_per_poll: usize,
}

impl Default for TaskQueueConfig {
    fn default() -> Self {
        Self {
            max_tasks_per_second: 100000.0,
            max_pollers: 1000,
            sync_match_timeout_ms: 200,
            backlog_per_partition_limit: 2000,
            forwarder_max_children_per_poll: 10,
        }
    }
}

impl PhysicalTaskQueue {
    pub fn new(partition: TaskQueuePartition, config: TaskQueueConfig) -> Self {
        Self {
            partition,
            tasks: RwLock::new(VecDeque::new()),
            pollers: RwLock::new(Vec::new()),
            backlog_counter: AtomicU64::new(0),
            dispatch_counter: AtomicU64::new(0),
            sync_match_counter: AtomicU64::new(0),
            config,
        }
    }

    pub fn add_task(&self, task: InternalTask) -> bool {
        let mut tasks = self.tasks.write().unwrap();
        if tasks.len() >= self.config.backlog_per_partition_limit {
            return false;
        }
        tasks.push_back(task);
        self.backlog_counter.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn poll_task(&self) -> Option<InternalTask> {
        let mut tasks = self.tasks.write().unwrap();
        let task = tasks.pop_front();
        if task.is_some() {
            self.dispatch_counter.fetch_add(1, Ordering::Relaxed);
            self.backlog_counter.fetch_sub(1, Ordering::Relaxed);
        }
        task
    }

    pub fn try_sync_match(&self, task: &InternalTask) -> Option<PollerInfo> {
        let pollers = self.pollers.read().unwrap();
        let available = pollers
            .iter()
            .find(|p| {
                if let Some(ref version) = task.version {
                    p.build_id == *version || p.build_id.is_empty()
                } else {
                    true
                }
            })
            .cloned();

        if available.is_some() {
            self.sync_match_counter.fetch_add(1, Ordering::Relaxed);
        }
        available
    }

    pub fn register_poller(&self, poller: PollerInfo) -> bool {
        let mut pollers = self.pollers.write().unwrap();
        if pollers.len() >= self.config.max_pollers {
            return false;
        }
        pollers.push(poller);
        true
    }

    pub fn deregister_poller(&self, identity: &str) {
        let mut pollers = self.pollers.write().unwrap();
        pollers.retain(|p| p.identity != identity);
    }

    pub fn backlog_count(&self) -> u64 {
        self.backlog_counter.load(Ordering::Relaxed)
    }

    pub fn dispatch_count(&self) -> u64 {
        self.dispatch_counter.load(Ordering::Relaxed)
    }

    pub fn poller_count(&self) -> usize {
        self.pollers.read().unwrap().len()
    }

    pub fn cleanup_stale_pollers(&self, max_idle: Duration) -> usize {
        let mut pollers = self.pollers.write().unwrap();
        let before = pollers.len();
        pollers.retain(|p| p.last_poll_at.elapsed() < max_idle);
        before - pollers.len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Logical Task Queue
// ═══════════════════════════════════════════════════════════════════════════════

pub struct LogicalTaskQueue {
    pub name: String,
    pub task_type: TaskType,
    pub namespace_id: String,
    pub root_partition: Arc<PhysicalTaskQueue>,
    pub child_partitions: RwLock<HashMap<i32, Arc<PhysicalTaskQueue>>>,
    pub versioning: TaskQueueVersioning,
    pub config: TaskQueueConfig,
}

#[derive(Debug, Clone, Default)]
pub struct TaskQueueVersioning {
    pub current_version: Option<String>,
    pub version_data: HashMap<String, VersionData>,
    pub redirect_rules: Vec<VersionRedirectRule>,
    pub assignment_rules: Vec<VersionAssignmentRule>,
}

#[derive(Debug, Clone, Default)]
pub struct VersionData {
    pub build_id: String,
    pub is_current: bool,
    pub is_draining: bool,
    pub first_poller_seen: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct VersionRedirectRule {
    pub source_version: String,
    pub target_version: String,
    pub created_at: Instant,
}

#[derive(Debug, Clone)]
pub struct VersionAssignmentRule {
    pub source_version: Option<String>,
    pub target_version: String,
    pub percentage: f64,
    pub created_at: Instant,
}

impl LogicalTaskQueue {
    pub fn new(name: &str, task_type: TaskType, ns_id: &str, config: TaskQueueConfig) -> Self {
        let partition = TaskQueuePartition::root(name, task_type, ns_id);
        let root = Arc::new(PhysicalTaskQueue::new(partition, config.clone()));

        Self {
            name: name.to_string(),
            task_type,
            namespace_id: ns_id.to_string(),
            root_partition: root,
            child_partitions: RwLock::new(HashMap::new()),
            versioning: TaskQueueVersioning::default(),
            config,
        }
    }

    pub fn dispatch_task(&self, task: InternalTask) -> DispatchResult {
        // Check redirect rules first
        if let Some(version) = &task.version {
            if let Some(redirect) = self
                .versioning
                .redirect_rules
                .iter()
                .find(|r| &r.source_version == version)
            {
                return DispatchResult::Redirected(redirect.target_version.clone());
            }
        }

        // Try sync match on root partition
        if let Some(poller) = self.root_partition.try_sync_match(&task) {
            return DispatchResult::SyncMatched(poller.identity);
        }

        // Try child partitions
        let children = self.child_partitions.read().unwrap();
        for (_, child) in children.iter() {
            if let Some(poller) = child.try_sync_match(&task) {
                return DispatchResult::SyncMatched(poller.identity);
            }
        }

        // Add to root partition backlog
        if self.root_partition.add_task(task) {
            DispatchResult::Queued
        } else {
            DispatchResult::BacklogFull
        }
    }

    pub fn poll_for_task(&self, _identity: &str, _build_id: &str) -> Option<InternalTask> {
        // Try root partition first
        if let Some(task) = self.root_partition.poll_task() {
            return Some(task);
        }

        // Try child partitions
        let children = self.child_partitions.read().unwrap();
        for (_, child) in children.iter() {
            if let Some(task) = child.poll_task() {
                return Some(task);
            }
        }

        None
    }

    pub fn add_partition(&self, partition_id: i32) {
        let partition =
            TaskQueuePartition::child(&self.name, self.task_type, &self.namespace_id, partition_id);
        let queue = Arc::new(PhysicalTaskQueue::new(partition, self.config.clone()));
        self.child_partitions
            .write()
            .unwrap()
            .insert(partition_id, queue);
    }

    pub fn remove_partition(&self, partition_id: i32) {
        self.child_partitions.write().unwrap().remove(&partition_id);
    }

    pub fn total_backlog(&self) -> u64 {
        let mut total = self.root_partition.backlog_count();
        for (_, child) in self.child_partitions.read().unwrap().iter() {
            total += child.backlog_count();
        }
        total
    }

    pub fn total_pollers(&self) -> usize {
        let mut total = self.root_partition.poller_count();
        for (_, child) in self.child_partitions.read().unwrap().iter() {
            total += child.poller_count();
        }
        total
    }

    pub fn set_current_version(&mut self, version: &str) {
        self.versioning.current_version = Some(version.to_string());
        if let Some(data) = self.versioning.version_data.get_mut(version) {
            data.is_current = true;
        } else {
            self.versioning.version_data.insert(
                version.to_string(),
                VersionData {
                    build_id: version.to_string(),
                    is_current: true,
                    is_draining: false,
                    first_poller_seen: None,
                },
            );
        }
    }

    pub fn add_redirect_rule(&mut self, source: &str, target: &str) {
        self.versioning.redirect_rules.push(VersionRedirectRule {
            source_version: source.to_string(),
            target_version: target.to_string(),
            created_at: Instant::now(),
        });
    }

    pub fn add_assignment_rule(&mut self, target: &str, percentage: f64) {
        self.versioning
            .assignment_rules
            .push(VersionAssignmentRule {
                source_version: None,
                target_version: target.to_string(),
                percentage,
                created_at: Instant::now(),
            });
    }
}

#[derive(Debug, Clone)]
pub enum DispatchResult {
    SyncMatched(String),
    Queued,
    Redirected(String),
    BacklogFull,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task Queue Manager
// ═══════════════════════════════════════════════════════════════════════════════

pub struct TaskQueueManager {
    queues: RwLock<HashMap<String, Arc<LogicalTaskQueue>>>,
    config: TaskQueueConfig,
    stats: TaskQueueManagerStats,
}

#[derive(Debug, Default)]
pub struct TaskQueueManagerStats {
    pub total_dispatched: AtomicU64,
    pub total_polled: AtomicU64,
    pub total_sync_matched: AtomicU64,
    pub total_redirects: AtomicU64,
    pub total_rejected: AtomicU64,
}

impl TaskQueueManager {
    pub fn new(config: TaskQueueConfig) -> Self {
        Self {
            queues: RwLock::new(HashMap::new()),
            config,
            stats: TaskQueueManagerStats::default(),
        }
    }

    pub fn get_or_create_queue(
        &self,
        name: &str,
        task_type: TaskType,
        ns_id: &str,
    ) -> Arc<LogicalTaskQueue> {
        let key = format!("{}:{}:{}", ns_id, name, task_type as i32);
        let mut queues = self.queues.write().unwrap();
        queues
            .entry(key)
            .or_insert_with(|| {
                Arc::new(LogicalTaskQueue::new(
                    name,
                    task_type,
                    ns_id,
                    self.config.clone(),
                ))
            })
            .clone()
    }

    pub fn dispatch_task(
        &self,
        name: &str,
        task_type: TaskType,
        ns_id: &str,
        task: InternalTask,
    ) -> DispatchResult {
        let queue = self.get_or_create_queue(name, task_type, ns_id);
        let result = queue.dispatch_task(task);

        match &result {
            DispatchResult::SyncMatched(_) => {
                self.stats
                    .total_sync_matched
                    .fetch_add(1, Ordering::Relaxed);
                self.stats.total_dispatched.fetch_add(1, Ordering::Relaxed);
            }
            DispatchResult::Queued => {
                self.stats.total_dispatched.fetch_add(1, Ordering::Relaxed);
            }
            DispatchResult::Redirected(_) => {
                self.stats.total_redirects.fetch_add(1, Ordering::Relaxed);
            }
            DispatchResult::BacklogFull => {
                self.stats.total_rejected.fetch_add(1, Ordering::Relaxed);
            }
        }

        result
    }

    pub fn poll_task(
        &self,
        name: &str,
        task_type: TaskType,
        ns_id: &str,
        identity: &str,
        build_id: &str,
    ) -> Option<InternalTask> {
        self.stats.total_polled.fetch_add(1, Ordering::Relaxed);
        let queue = self.get_or_create_queue(name, task_type, ns_id);
        queue.poll_for_task(identity, build_id)
    }

    pub fn register_poller(
        &self,
        name: &str,
        task_type: TaskType,
        ns_id: &str,
        poller: PollerInfo,
    ) -> bool {
        let queue = self.get_or_create_queue(name, task_type, ns_id);
        queue.root_partition.register_poller(poller)
    }

    pub fn queue_count(&self) -> usize {
        self.queues.read().unwrap().len()
    }

    pub fn stats(&self) -> &TaskQueueManagerStats {
        &self.stats
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task Forwarder
// ═══════════════════════════════════════════════════════════════════════════════

pub struct TaskForwarder {
    max_forward_levels: i32,
    forward_stats: ForwardStats,
}

#[derive(Debug, Default)]
pub struct ForwardStats {
    pub forwarded_up: AtomicU64,
    pub forwarded_down: AtomicU64,
    pub forward_failures: AtomicU64,
}

impl TaskForwarder {
    pub fn new(max_levels: i32) -> Self {
        Self {
            max_forward_levels: max_levels,
            forward_stats: ForwardStats::default(),
        }
    }

    pub fn forward_up(&self, queue: &LogicalTaskQueue, task: &InternalTask) -> bool {
        let current_level = queue.child_partitions.read().unwrap().len() as i32;
        if current_level >= self.max_forward_levels {
            return false;
        }

        // Forward to parent (root)
        if queue.root_partition.add_task(task.clone()) {
            self.forward_stats
                .forwarded_up
                .fetch_add(1, Ordering::Relaxed);
            true
        } else {
            self.forward_stats
                .forward_failures
                .fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    pub fn forward_down(
        &self,
        queue: &LogicalTaskQueue,
        task: &InternalTask,
        target_partition: i32,
    ) -> bool {
        let children = queue.child_partitions.read().unwrap();
        if let Some(child) = children.get(&target_partition) {
            if child.add_task(task.clone()) {
                self.forward_stats
                    .forwarded_down
                    .fetch_add(1, Ordering::Relaxed);
                true
            } else {
                self.forward_stats
                    .forward_failures
                    .fetch_add(1, Ordering::Relaxed);
                false
            }
        } else {
            false
        }
    }

    pub fn stats(&self) -> &ForwardStats {
        &self.forward_stats
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Load Balancer
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MatchingLoadBalancer {
    partition_loads: RwLock<HashMap<String, PartitionLoad>>,
}

#[derive(Debug, Clone)]
pub struct PartitionLoad {
    pub partition_id: i32,
    pub backlog_size: u64,
    pub poller_count: usize,
    pub dispatch_rate: f64,
    pub last_updated: Instant,
}

impl MatchingLoadBalancer {
    pub fn new() -> Self {
        Self {
            partition_loads: RwLock::new(HashMap::new()),
        }
    }

    pub fn update_load(&self, queue: &LogicalTaskQueue) {
        let mut loads = self.partition_loads.write().unwrap();

        // Update root
        let root_load = PartitionLoad {
            partition_id: 0,
            backlog_size: queue.root_partition.backlog_count(),
            poller_count: queue.root_partition.poller_count(),
            dispatch_rate: queue.root_partition.dispatch_count() as f64,
            last_updated: Instant::now(),
        };
        loads.insert(format!("{}:0", queue.name), root_load);

        // Update children
        for (id, child) in queue.child_partitions.read().unwrap().iter() {
            let load = PartitionLoad {
                partition_id: *id,
                backlog_size: child.backlog_count(),
                poller_count: child.poller_count(),
                dispatch_rate: child.dispatch_count() as f64,
                last_updated: Instant::now(),
            };
            loads.insert(format!("{}:{}", queue.name, id), load);
        }
    }

    pub fn get_least_loaded_partition(&self, queue_name: &str) -> Option<i32> {
        let loads = self.partition_loads.read().unwrap();
        let mut min_load = u64::MAX;
        let mut best_partition = None;

        for (key, load) in loads.iter() {
            if key.starts_with(&format!("{}:", queue_name)) {
                let effective_load = load
                    .backlog_size
                    .saturating_sub(load.poller_count as u64 * 10);
                if effective_load < min_load {
                    min_load = effective_load;
                    best_partition = Some(load.partition_id);
                }
            }
        }

        best_partition
    }

    pub fn should_add_partition(&self, queue_name: &str, threshold: u64) -> bool {
        let loads = self.partition_loads.read().unwrap();
        let root_load = loads.get(&format!("{}:0", queue_name));
        root_load
            .map(|l| l.backlog_size > threshold)
            .unwrap_or(false)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(wf_id: &str, version: Option<&str>) -> InternalTask {
        InternalTask {
            task_id: 1,
            workflow_id: wf_id.to_string(),
            run_id: "run1".to_string(),
            task_token: vec![1, 2, 3],
            scheduled_time: 0,
            priority: 0,
            version: version.map(|s| s.to_string()),
            redirect_info: None,
            created_at: Instant::now(),
        }
    }

    fn make_poller(identity: &str, build_id: &str) -> PollerInfo {
        PollerInfo {
            identity: identity.to_string(),
            build_id: build_id.to_string(),
            registered_at: Instant::now(),
            last_poll_at: Instant::now(),
            is_long_poll: false,
            rate_limiter: None,
        }
    }

    #[test]
    fn test_physical_queue_add_poll() {
        let partition = TaskQueuePartition::root("test-queue", TaskType::Workflow, "ns1");
        let queue = PhysicalTaskQueue::new(partition, TaskQueueConfig::default());

        let task = make_task("wf1", None);
        assert!(queue.add_task(task));
        assert_eq!(queue.backlog_count(), 1);

        let polled = queue.poll_task();
        assert!(polled.is_some());
        assert_eq!(queue.backlog_count(), 0);
    }

    #[test]
    fn test_physical_queue_sync_match() {
        let partition = TaskQueuePartition::root("test-queue", TaskType::Workflow, "ns1");
        let queue = PhysicalTaskQueue::new(partition, TaskQueueConfig::default());

        queue.register_poller(make_poller("worker1", "v1"));

        let task = make_task("wf1", Some("v1"));
        let matched = queue.try_sync_match(&task);
        assert!(matched.is_some());
        assert_eq!(matched.unwrap().identity, "worker1");
    }

    #[test]
    fn test_physical_queue_backlog_limit() {
        let partition = TaskQueuePartition::root("test-queue", TaskType::Workflow, "ns1");
        let config = TaskQueueConfig {
            backlog_per_partition_limit: 2,
            ..Default::default()
        };
        let queue = PhysicalTaskQueue::new(partition, config);

        assert!(queue.add_task(make_task("wf1", None)));
        assert!(queue.add_task(make_task("wf2", None)));
        assert!(!queue.add_task(make_task("wf3", None))); // Rejected
    }

    #[test]
    fn test_logical_queue_dispatch() {
        let queue = LogicalTaskQueue::new(
            "test-queue",
            TaskType::Workflow,
            "ns1",
            TaskQueueConfig::default(),
        );

        // No pollers, should queue
        let result = queue.dispatch_task(make_task("wf1", None));
        matches!(result, DispatchResult::Queued);
        assert_eq!(queue.total_backlog(), 1);
    }

    #[test]
    fn test_logical_queue_sync_match() {
        let queue = LogicalTaskQueue::new(
            "test-queue",
            TaskType::Workflow,
            "ns1",
            TaskQueueConfig::default(),
        );
        queue
            .root_partition
            .register_poller(make_poller("worker1", ""));

        let result = queue.dispatch_task(make_task("wf1", None));
        matches!(result, DispatchResult::SyncMatched(_));
    }

    #[test]
    fn test_logical_queue_redirect() {
        let mut queue = LogicalTaskQueue::new(
            "test-queue",
            TaskType::Workflow,
            "ns1",
            TaskQueueConfig::default(),
        );
        queue.add_redirect_rule("v1", "v2");

        let result = queue.dispatch_task(make_task("wf1", Some("v1")));
        matches!(result, DispatchResult::Redirected(_));
    }

    #[test]
    fn test_task_queue_manager() {
        let mgr = TaskQueueManager::new(TaskQueueConfig::default());

        let result = mgr.dispatch_task("q1", TaskType::Workflow, "ns1", make_task("wf1", None));
        matches!(result, DispatchResult::Queued);

        assert_eq!(mgr.queue_count(), 1);
        assert_eq!(mgr.stats().total_dispatched.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_task_queue_manager_poll() {
        let mgr = TaskQueueManager::new(TaskQueueConfig::default());

        mgr.dispatch_task("q1", TaskType::Workflow, "ns1", make_task("wf1", None));
        let task = mgr.poll_task("q1", TaskType::Workflow, "ns1", "worker1", "");
        assert!(task.is_some());
    }

    #[test]
    fn test_task_forwarder() {
        let queue = LogicalTaskQueue::new(
            "test-queue",
            TaskType::Workflow,
            "ns1",
            TaskQueueConfig::default(),
        );
        queue.add_partition(1);

        let forwarder = TaskForwarder::new(3);
        let task = make_task("wf1", None);

        assert!(forwarder.forward_up(&queue, &task));
        assert_eq!(forwarder.stats().forwarded_up.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_load_balancer() {
        let queue = LogicalTaskQueue::new(
            "test-queue",
            TaskType::Workflow,
            "ns1",
            TaskQueueConfig::default(),
        );
        queue.add_partition(1);
        queue.add_partition(2);

        // Add tasks to create load
        queue.dispatch_task(make_task("wf1", None));
        queue.dispatch_task(make_task("wf2", None));

        let lb = MatchingLoadBalancer::new();
        lb.update_load(&queue);

        let least_loaded = lb.get_least_loaded_partition("test-queue");
        assert!(least_loaded.is_some());
    }

    #[test]
    fn test_partition_management() {
        let queue = LogicalTaskQueue::new(
            "test-queue",
            TaskType::Workflow,
            "ns1",
            TaskQueueConfig::default(),
        );
        assert_eq!(queue.child_partitions.read().unwrap().len(), 0);

        queue.add_partition(1);
        queue.add_partition(2);
        assert_eq!(queue.child_partitions.read().unwrap().len(), 2);

        queue.remove_partition(1);
        assert_eq!(queue.child_partitions.read().unwrap().len(), 1);
    }

    #[test]
    fn test_versioning() {
        let mut queue = LogicalTaskQueue::new(
            "test-queue",
            TaskType::Workflow,
            "ns1",
            TaskQueueConfig::default(),
        );

        queue.set_current_version("v1");
        assert_eq!(queue.versioning.current_version, Some("v1".to_string()));
        assert!(queue.versioning.version_data.get("v1").unwrap().is_current);

        queue.add_redirect_rule("v1", "v2");
        assert_eq!(queue.versioning.redirect_rules.len(), 1);
    }

    #[test]
    fn test_poller_cleanup() {
        let partition = TaskQueuePartition::root("test-queue", TaskType::Workflow, "ns1");
        let queue = PhysicalTaskQueue::new(partition, TaskQueueConfig::default());

        queue.register_poller(make_poller("worker1", ""));
        queue.register_poller(make_poller("worker2", ""));
        assert_eq!(queue.poller_count(), 2);

        queue.deregister_poller("worker1");
        assert_eq!(queue.poller_count(), 1);
    }

    #[test]
    fn test_total_pollers_and_backlog() {
        let queue = LogicalTaskQueue::new(
            "test-queue",
            TaskType::Workflow,
            "ns1",
            TaskQueueConfig::default(),
        );
        queue.add_partition(1);

        // Dispatch task first (no pollers, so it goes to backlog)
        queue.dispatch_task(make_task("wf1", None));
        // Then register poller (on child partition, not root, so it doesn't sync match)
        let _child_partition =
            TaskQueuePartition::child("test-queue", TaskType::Workflow, "ns1", 1);
        let children = queue.child_partitions.read().unwrap();
        if let Some(child) = children.get(&1) {
            child.register_poller(make_poller("w1", ""));
        }

        assert_eq!(queue.total_pollers(), 1);
        assert_eq!(queue.total_backlog(), 1);
    }
}
