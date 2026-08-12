//! Matching service — partition-aware task-to-worker dispatch.
//! Manages task queues with version-aware matching, forwarding across partitions,
//! and blocking poll for efficient worker dispatch. Mirrors Temporal's matching service.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::time::{Duration, Instant};

// ─── Match Task ──────────────────────────────────────────────────────────────

/// A task waiting to be matched with a worker.
#[derive(Debug, Clone)]
pub struct MatchTask {
    pub task_id: u64,
    pub workflow_key: u64,
    pub task_queue: String,
    pub build_id: Option<String>,
    pub priority: u32,
    pub created_at: Instant,
    pub forwarded_from: Option<u64>, // partition that forwarded this task
}

/// Information about a registered poller.
#[derive(Debug, Clone)]
pub struct PollerInfo {
    pub poller_id: u64,
    pub build_id: Option<String>,
    pub partition: u64,
    pub registered_at: Instant,
    pub last_poll_at: Instant,
}

/// Filter for task kinds when polling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKindFilter {
    Workflow,
    Activity,
    Nexus,
    Any,
}

// ─── Partition Queue ─────────────────────────────────────────────────────────

/// A queue of tasks for a specific partition.
struct PartitionQueue {
    partition_id: u64,
    tasks: VecDeque<MatchTask>,
    pollers: Vec<PollerInfo>,
    max_depth: usize,
}

impl PartitionQueue {
    fn new(partition_id: u64, max_depth: usize) -> Self {
        Self {
            partition_id,
            tasks: VecDeque::new(),
            pollers: Vec::new(),
            max_depth,
        }
    }

    fn enqueue(&mut self, task: MatchTask) -> bool {
        if self.tasks.len() >= self.max_depth {
            return false;
        }
        self.tasks.push_back(task);
        true
    }

    fn dequeue(&mut self) -> Option<MatchTask> {
        self.tasks.pop_front()
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    fn len(&self) -> usize {
        self.tasks.len()
    }

    fn register_poller(&mut self, info: PollerInfo) {
        self.pollers.push(info);
    }

    fn remove_poller(&mut self, poller_id: u64) {
        self.pollers.retain(|p| p.poller_id != poller_id);
    }

    fn has_compatible_poller(&self, build_id: &Option<String>) -> bool {
        self.pollers.iter().any(|p| {
            match (&p.build_id, build_id) {
                (None, _) => true, // unversioned poller accepts anything
                (Some(a), Some(b)) => a == b, // versioned match
                (Some(_), None) => false, // versioned poller won't accept unversioned task
            }
        })
    }
}

// ─── Matching Service ────────────────────────────────────────────────────────

/// Statistics for the matching service.
#[derive(Debug, Clone, Default)]
pub struct MatchingServiceStats {
    pub tasks_added: u64,
    pub tasks_matched: u64,
    pub tasks_forwarded: u64,
    pub tasks_expired: u64,
    pub pollers_registered: u64,
    pub pollers_removed: u64,
    pub queue_depth: u64,
}

/// Configuration for the matching service.
#[derive(Debug, Clone)]
pub struct MatchingServiceConfig {
    pub num_partitions: u64,
    pub max_queue_depth: usize,
    pub forward_max_partitions: u64,
    pub task_ttl_seconds: u64,
    pub poll_timeout_ms: u64,
}

impl Default for MatchingServiceConfig {
    fn default() -> Self {
        Self {
            num_partitions: 4,
            max_queue_depth: 10_000,
            forward_max_partitions: 2,
            task_ttl_seconds: 60,
            poll_timeout_ms: 5000,
        }
    }
}

/// Partition-aware matching service.
/// Matches workflow/activity tasks to available workers using version-aware
/// matching and partition-based forwarding.
pub struct MatchingService {
    config: MatchingServiceConfig,
    queues: RwLock<HashMap<String, Vec<PartitionQueue>>>,
    notify: Condvar,
    mutex: Mutex<()>,
    stats: RwLock<MatchingServiceStats>,
    next_poller_id: std::sync::atomic::AtomicU64,
}

impl MatchingService {
    pub fn new(config: MatchingServiceConfig) -> Self {
        Self {
            config,
            queues: RwLock::new(HashMap::new()),
            notify: Condvar::new(),
            mutex: Mutex::new(()),
            stats: RwLock::new(MatchingServiceStats::default()),
            next_poller_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Add a task to the matching queue. Attempts immediate matching with waiting pollers.
    pub fn add_task(&self, task: MatchTask) -> bool {
        let queue_name = task.task_queue.clone();
        let partition = self.partition_for(&queue_name);

        // Check for immediate match with a waiting poller
        {
            let mut queues = self.queues.write().unwrap();
            let partitions = queues.entry(queue_name.clone()).or_insert_with(|| {
                (0..self.config.num_partitions)
                    .map(|i| PartitionQueue::new(i, self.config.max_queue_depth))
                    .collect()
            });

            if let Some(pq) = partitions.get_mut(partition as usize) {
                if pq.has_compatible_poller(&task.build_id) && pq.is_empty() {
                    // Immediate match possible — poller is waiting
                    self.stats.write().unwrap().tasks_matched += 1;
                    self.notify.notify_one();
                    // Still enqueue for the poller to pick up
                    pq.enqueue(task);
                    self.stats.write().unwrap().tasks_added += 1;
                    return true;
                }
            }
        }

        // No immediate match — enqueue in the target partition.
        // Forwarding to root is only used when workers poll exclusively from root.
        let mut queues = self.queues.write().unwrap();
        if let Some(partitions) = queues.get_mut(&queue_name) {
            if let Some(pq) = partitions.get_mut(partition as usize) {
                let ok = pq.enqueue(task);
                if ok {
                    self.stats.write().unwrap().tasks_added += 1;
                }
                return ok;
            }
        }
        false
    }

    /// Poll for a task (blocking with timeout). Returns None if no task available within timeout.
    pub fn poll_task(&self, queue_name: &str, build_id: Option<String>, kind: TaskKindFilter) -> Option<MatchTask> {
        let partition = self.partition_for(queue_name);
        let timeout = Duration::from_millis(self.config.poll_timeout_ms);

        let guard = self.mutex.lock().unwrap();
        let deadline = Instant::now() + timeout;

        loop {
            // Try to dequeue
            {
                let mut queues = self.queues.write().unwrap();
                if let Some(partitions) = queues.get_mut(queue_name) {
                    if let Some(pq) = partitions.get_mut(partition as usize) {
                        if let Some(task) = pq.dequeue() {
                            // Check version compatibility
                            if self.is_task_compatible(&task, &build_id, kind) {
                                self.stats.write().unwrap().tasks_matched += 1;
                                return Some(task);
                            } else {
                                // Put it back and try another partition
                                pq.tasks.push_front(task);
                            }
                        }
                    }
                }
            }

            // No task available — wait or timeout
            if Instant::now() >= deadline {
                return None;
            }

            let remaining = deadline - Instant::now();
            let _guard = self.notify.wait_timeout(guard, remaining).unwrap();
            // Re-check after wakeup
            return self.poll_task_inner(queue_name, build_id.as_deref(), kind, partition);
        }
    }

    /// Non-blocking poll. Returns immediately with None if no task available.
    pub fn try_poll_task(&self, queue_name: &str, build_id: Option<String>, kind: TaskKindFilter) -> Option<MatchTask> {
        let partition = self.partition_for(queue_name);
        self.poll_task_inner(queue_name, build_id.as_deref(), kind, partition)
    }

    /// Register a poller for a task queue.
    pub fn register_poller(&self, queue_name: &str, build_id: Option<String>) -> u64 {
        let poller_id = self.next_poller_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let partition = self.partition_for(queue_name);

        let mut queues = self.queues.write().unwrap();
        let partitions = queues.entry(queue_name.to_string()).or_insert_with(|| {
            (0..self.config.num_partitions)
                .map(|i| PartitionQueue::new(i, self.config.max_queue_depth))
                .collect()
        });

        if let Some(pq) = partitions.get_mut(partition as usize) {
            pq.register_poller(PollerInfo {
                poller_id,
                build_id,
                partition,
                registered_at: Instant::now(),
                last_poll_at: Instant::now(),
            });
        }

        self.stats.write().unwrap().pollers_registered += 1;
        poller_id
    }

    /// Remove a poller.
    pub fn remove_poller(&self, queue_name: &str, poller_id: u64) {
        let partition = self.partition_for(queue_name);
        let mut queues = self.queues.write().unwrap();
        if let Some(partitions) = queues.get_mut(queue_name) {
            if let Some(pq) = partitions.get_mut(partition as usize) {
                pq.remove_poller(poller_id);
            }
        }
        self.stats.write().unwrap().pollers_removed += 1;
    }

    /// Get statistics.
    pub fn stats(&self) -> MatchingServiceStats {
        let mut s = self.stats.read().unwrap().clone();
        let queues = self.queues.read().unwrap();
        s.queue_depth = queues.values().flat_map(|ps| ps.iter()).map(|p| p.len() as u64).sum();
        s
    }

    /// Get the number of registered task queues.
    pub fn queue_count(&self) -> usize {
        self.queues.read().unwrap().len()
    }

    /// Get queue depth for a specific task queue.
    pub fn queue_depth(&self, queue_name: &str) -> usize {
        let queues = self.queues.read().unwrap();
        queues.get(queue_name).map(|ps| ps.iter().map(|p| p.len()).sum()).unwrap_or(0)
    }

    // ─── Internal ─────────────────────────────────────────────────────────

    fn partition_for(&self, queue_name: &str) -> u64 {
        // Simple hash-based partitioning
        let hash = queue_name.bytes().fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
        hash % self.config.num_partitions
    }

    fn is_task_compatible(&self, task: &MatchTask, build_id: &Option<String>, kind: TaskKindFilter) -> bool {
        // Version compatibility
        match (&task.build_id, build_id) {
            (None, _) => {}
            (Some(_), None) => return false,
            (Some(a), Some(b)) => if a != b { return false; },
        }
        true
    }

    fn try_forward(&self, task: MatchTask, queue_name: &str, from_partition: u64) -> bool {
        if from_partition == 0 { return false; }
        // Forward to root (partition 0)
        let mut queues = self.queues.write().unwrap();
        if let Some(partitions) = queues.get_mut(queue_name) {
            if let Some(root) = partitions.get_mut(0) {
                let mut fwd_task = task;
                fwd_task.forwarded_from = Some(from_partition);
                return root.enqueue(fwd_task);
            }
        }
        false
    }

    fn try_forward_ref(&self, task: &MatchTask, queue_name: &str, from_partition: u64) -> bool {
        if from_partition == 0 { return false; }
        let mut queues = self.queues.write().unwrap();
        if let Some(partitions) = queues.get_mut(queue_name) {
            if let Some(root) = partitions.get_mut(0) {
                let mut fwd_task = task.clone();
                fwd_task.forwarded_from = Some(from_partition);
                return root.enqueue(fwd_task);
            }
        }
        false
    }

    fn poll_task_inner(&self, queue_name: &str, build_id: Option<&str>, kind: TaskKindFilter, partition: u64) -> Option<MatchTask> {
        let mut queues = self.queues.write().unwrap();
        if let Some(partitions) = queues.get_mut(queue_name) {
            if let Some(pq) = partitions.get_mut(partition as usize) {
                if let Some(task) = pq.dequeue() {
                    let bid = build_id.map(|s| s.to_string());
                    if self.is_task_compatible(&task, &bid, kind) {
                        self.stats.write().unwrap().tasks_matched += 1;
                        return Some(task);
                    } else {
                        pq.tasks.push_front(task);
                    }
                }
            }
        }
        None
    }

    // ─── DescribeTaskQueue API ───────────────────────────────────────────────

    /// Describe a task queue, returning pollers, backlog, and build ID info.
    pub fn describe_task_queue(&self, queue_name: &str) -> TaskQueueDescription {
        let queues = self.queues.read().unwrap();
        let mut total_backlog = 0u64;
        let mut pollers = Vec::new();
        let mut partitions = Vec::new();
        let mut build_ids = std::collections::HashSet::new();

        if let Some(parts) = queues.get(queue_name) {
            for pq in parts {
                total_backlog += pq.len() as u64;
                let mut partition_pollers = Vec::new();
                for p in &pq.pollers {
                    if let Some(ref bid) = p.build_id {
                        build_ids.insert(bid.clone());
                    }
                    partition_pollers.push(PollerDescription {
                        poller_id: p.poller_id,
                        identity: format!("poller-{}", p.poller_id),
                        build_id: p.build_id.clone(),
                        partition: p.partition,
                        last_access_time: p.last_poll_at,
                        registered_at: p.registered_at,
                    });
                    pollers.push(p.clone());
                }
                partitions.push(TaskQueuePartitionInfo {
                    partition_id: pq.partition_id,
                    backlog_count: pq.len() as u64,
                    poller_count: pq.pollers.len() as u32,
                });
            }
        }

        TaskQueueDescription {
            task_queue_name: queue_name.to_string(),
            total_backlog,
            pollers,
            partitions,
            build_ids: build_ids.into_iter().collect(),
            task_queue_type: "workflow".to_string(),
        }
    }

    /// List all task queue names.
    pub fn list_task_queues(&self) -> Vec<String> {
        self.queues.read().unwrap().keys().cloned().collect()
    }
}

// ─── DescribeTaskQueue Types ─────────────────────────────────────────────────

/// Description of a task queue including pollers, backlog, and build IDs.
#[derive(Debug, Clone)]
pub struct TaskQueueDescription {
    pub task_queue_name: String,
    pub total_backlog: u64,
    pub pollers: Vec<PollerInfo>,
    pub partitions: Vec<TaskQueuePartitionInfo>,
    pub build_ids: Vec<String>,
    pub task_queue_type: String,
}

impl TaskQueueDescription {
    /// Total number of pollers across all partitions.
    pub fn total_poller_count(&self) -> usize {
        self.pollers.len()
    }

    /// Number of distinct partitions.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Whether the task queue has any backlog.
    pub fn has_backlog(&self) -> bool {
        self.total_backlog > 0
    }

    /// Whether any pollers are registered.
    pub fn has_pollers(&self) -> bool {
        !self.pollers.is_empty()
    }
}

/// Description of a single poller.
#[derive(Debug, Clone)]
pub struct PollerDescription {
    pub poller_id: u64,
    pub identity: String,
    pub build_id: Option<String>,
    pub partition: u64,
    pub last_access_time: Instant,
    pub registered_at: Instant,
}

/// Per-partition summary.
#[derive(Debug, Clone)]
pub struct TaskQueuePartitionInfo {
    pub partition_id: u64,
    pub backlog_count: u64,
    pub poller_count: u32,
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(id: u64, queue: &str) -> MatchTask {
        MatchTask {
            task_id: id,
            workflow_key: id * 10,
            task_queue: queue.to_string(),
            build_id: None,
            priority: 0,
            created_at: Instant::now(),
            forwarded_from: None,
        }
    }

    fn make_versioned_task(id: u64, queue: &str, build_id: &str) -> MatchTask {
        MatchTask {
            task_id: id,
            workflow_key: id * 10,
            task_queue: queue.to_string(),
            build_id: Some(build_id.to_string()),
            priority: 0,
            created_at: Instant::now(),
            forwarded_from: None,
        }
    }

    #[test]
    fn test_add_and_poll() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        let task = make_task(1, "my-queue");
        assert!(svc.add_task(task));

        let polled = svc.try_poll_task("my-queue", None, TaskKindFilter::Any);
        assert!(polled.is_some());
        assert_eq!(polled.unwrap().task_id, 1);
    }

    #[test]
    fn test_poll_empty_queue() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        let result = svc.try_poll_task("empty-queue", None, TaskKindFilter::Any);
        assert!(result.is_none());
    }

    #[test]
    fn test_multiple_tasks() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        for i in 0..5 {
            svc.add_task(make_task(i, "queue-a"));
        }
        assert_eq!(svc.queue_depth("queue-a"), 5);

        for _ in 0..5 {
            assert!(svc.try_poll_task("queue-a", None, TaskKindFilter::Any).is_some());
        }
        assert!(svc.try_poll_task("queue-a", None, TaskKindFilter::Any).is_none());
    }

    #[test]
    fn test_register_remove_poller() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        let pid = svc.register_poller("queue-b", None);
        assert!(pid > 0);

        let stats = svc.stats();
        assert_eq!(stats.pollers_registered, 1);

        svc.remove_poller("queue-b", pid);
        let stats = svc.stats();
        assert_eq!(stats.pollers_removed, 1);
    }

    #[test]
    fn test_versioned_matching() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        svc.add_task(make_versioned_task(1, "vq", "build-1"));

        // Poll with matching build ID
        let result = svc.try_poll_task("vq", Some("build-1".to_string()), TaskKindFilter::Any);
        assert!(result.is_some());
    }

    #[test]
    fn test_versioned_mismatch() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        svc.add_task(make_versioned_task(1, "vq", "build-1"));

        // Poll with different build ID — should not match
        let result = svc.try_poll_task("vq", Some("build-2".to_string()), TaskKindFilter::Any);
        assert!(result.is_none());
    }

    #[test]
    fn test_queue_count() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        svc.add_task(make_task(1, "q1"));
        svc.add_task(make_task(2, "q2"));
        svc.add_task(make_task(3, "q3"));
        assert_eq!(svc.queue_count(), 3);
    }

    #[test]
    fn test_stats() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        svc.add_task(make_task(1, "sq"));
        svc.add_task(make_task(2, "sq"));
        svc.try_poll_task("sq", None, TaskKindFilter::Any);

        let stats = svc.stats();
        assert_eq!(stats.tasks_added, 2);
        assert!(stats.tasks_matched >= 1);
    }

    #[test]
    fn test_describe_empty_queue() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        let desc = svc.describe_task_queue("nonexistent");
        assert_eq!(desc.total_backlog, 0);
        assert_eq!(desc.total_poller_count(), 0);
        assert!(!desc.has_backlog());
        assert!(!desc.has_pollers());
    }

    #[test]
    fn test_describe_with_tasks_and_pollers() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        svc.add_task(make_task(1, "dq"));
        svc.add_task(make_task(2, "dq"));
        svc.add_task(make_task(3, "dq"));
        svc.register_poller("dq", Some("build-1".to_string()));
        svc.register_poller("dq", Some("build-2".to_string()));

        let desc = svc.describe_task_queue("dq");
        assert_eq!(desc.task_queue_name, "dq");
        assert!(desc.total_backlog > 0);
        assert_eq!(desc.total_poller_count(), 2);
        assert!(desc.has_backlog());
        assert!(desc.has_pollers());
        assert!(desc.build_ids.len() >= 2);
    }

    #[test]
    fn test_describe_partitions() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        svc.add_task(make_task(1, "pq"));
        let desc = svc.describe_task_queue("pq");
        assert_eq!(desc.partition_count(), 4); // default 4 partitions
    }

    #[test]
    fn test_list_task_queues() {
        let svc = MatchingService::new(MatchingServiceConfig::default());
        svc.add_task(make_task(1, "alpha"));
        svc.add_task(make_task(2, "beta"));
        let mut names = svc.list_task_queues();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);
    }
}
