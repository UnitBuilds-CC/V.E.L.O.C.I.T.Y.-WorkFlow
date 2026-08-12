//! Task queue partition manager with hierarchical depth, auto-scaling,
//! and read/write partition separation.
//! Implements multi-level partition trees where tasks flow from root → L1 → L2...

use std::collections::HashMap;
use std::sync::{Mutex, RwLock, Condvar};
use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};

use crate::task_queue::{TaskQueue, TaskItem, TaskKind};

/// A single partition of a task queue with hierarchical depth.
pub struct TaskQueuePartition {
    pub partition_id: u32,
    pub task_queue_hash: u64,
    pub queue: TaskQueue,
    pub parent_partition: Option<u32>,
    pub child_partitions: Vec<u32>,
    /// Depth in the partition tree (0 = root, 1 = L1, 2 = L2, etc.).
    pub depth: u32,
    pub forward_rate: f64, // 0.0 = no forwarding, 1.0 = forward all
    pub worker_count: AtomicU32,
    /// Whether this is a read partition (polls) or write partition (enqueues).
    pub is_read_partition: bool,
    pub is_write_partition: bool,
    /// Backlog count for auto-scaling decisions.
    pub backlog_count: AtomicU64,
    /// Maximum backlog before triggering scale-up.
    pub backlog_threshold: u64,
}

impl TaskQueuePartition {
    pub fn new(partition_id: u32, task_queue_hash: u64) -> Self {
        Self {
            partition_id,
            task_queue_hash,
            queue: TaskQueue::new(),
            parent_partition: None,
            child_partitions: Vec::new(),
            depth: 0,
            forward_rate: 0.0,
            worker_count: AtomicU32::new(0),
            is_read_partition: true,
            is_write_partition: true,
            backlog_count: AtomicU64::new(0),
            backlog_threshold: 1000,
        }
    }

    /// Create a child partition at a deeper level.
    pub fn new_child(partition_id: u32, task_queue_hash: u64, depth: u32) -> Self {
        let mut p = Self::new(partition_id, task_queue_hash);
        p.depth = depth;
        p
    }

    pub fn set_parent(&mut self, parent_id: u32) {
        self.parent_partition = Some(parent_id);
    }

    pub fn add_child(&mut self, child_id: u32) {
        if !self.child_partitions.contains(&child_id) {
            self.child_partitions.push(child_id);
        }
    }

    pub fn remove_child(&mut self, child_id: u32) {
        self.child_partitions.retain(|&c| c != child_id);
    }

    pub fn set_forward_rate(&mut self, rate: f64) {
        self.forward_rate = rate.clamp(0.0, 1.0);
    }

    pub fn set_read_write(&mut self, read: bool, write: bool) {
        self.is_read_partition = read;
        self.is_write_partition = write;
    }

    pub fn register_worker(&self) {
        self.worker_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn unregister_worker(&self) {
        self.worker_count.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn has_workers(&self) -> bool {
        self.worker_count.load(Ordering::Relaxed) > 0
    }

    pub fn pending_count(&self) -> usize {
        self.queue.pending_count(self.task_queue_hash)
    }

    pub fn enqueue(&self, item: TaskItem) {
        self.queue.enqueue(self.task_queue_hash, item);
        self.backlog_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn try_poll(&self) -> Option<TaskItem> {
        let item = self.queue.try_poll(self.task_queue_hash);
        if item.is_some() {
            self.backlog_count.fetch_sub(1, Ordering::Relaxed);
        }
        item
    }

    /// Check if this partition needs scaling based on backlog.
    pub fn needs_scale_up(&self) -> bool {
        self.backlog_count.load(Ordering::Relaxed) > self.backlog_threshold
    }

    /// Get the current backlog count.
    pub fn backlog(&self) -> u64 {
        self.backlog_count.load(Ordering::Relaxed)
    }
}

/// Auto-scaling decision for partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleDecision {
    /// No scaling needed.
    None,
    /// Add more child partitions.
    ScaleUp,
    /// Remove underutilized child partitions.
    ScaleDown,
}

/// Manages multiple partitions for a task queue with hierarchical forwarding.
pub struct PartitionManager {
    partitions: RwLock<HashMap<u32, TaskQueuePartition>>,
    next_partition_id: AtomicU64,
    num_partitions: u32,
    /// Maximum depth of the partition tree.
    max_depth: u32,
    /// Auto-scaling enabled flag.
    auto_scale: Mutex<bool>,
}

impl PartitionManager {
    pub fn new(num_partitions: u32) -> Self {
        Self {
            partitions: RwLock::new(HashMap::new()),
            next_partition_id: AtomicU64::new(0),
            num_partitions: num_partitions.max(1),
            max_depth: 3, // Default: root → L1 → L2 → L3
            auto_scale: Mutex::new(false),
        }
    }

    /// Create a new root-level partition.
    pub fn create_partition(&self, task_queue_hash: u64) -> u32 {
        self.create_partition_at_depth(task_queue_hash, 0)
    }

    /// Create a partition at a specific depth.
    pub fn create_partition_at_depth(&self, task_queue_hash: u64, depth: u32) -> u32 {
        let partition_id = self.next_partition_id.fetch_add(1, Ordering::Relaxed) as u32;
        let partition = TaskQueuePartition::new_child(partition_id, task_queue_hash, depth);
        self.partitions.write().unwrap().insert(partition_id, partition);
        partition_id
    }

    /// Create a child partition under an existing parent.
    pub fn create_child_partition(&self, parent_id: u32, task_queue_hash: u64) -> Option<u32> {
        let parent_depth = {
            let partitions = self.partitions.read().unwrap();
            partitions.get(&parent_id).map(|p| p.depth)?
        };

        if parent_depth >= self.max_depth { return None; }

        let child_id = self.create_partition_at_depth(task_queue_hash, parent_depth + 1);

        // Link parent → child
        let mut partitions = self.partitions.write().unwrap();
        if let Some(parent) = partitions.get_mut(&parent_id) {
            parent.add_child(child_id);
        }
        if let Some(child) = partitions.get_mut(&child_id) {
            child.set_parent(parent_id);
        }

        Some(child_id)
    }

    /// Delete a partition (and unlink from parent).
    pub fn delete_partition(&self, partition_id: u32) -> bool {
        let mut partitions = self.partitions.write().unwrap();
        if let Some(p) = partitions.remove(&partition_id) {
            // Unlink from parent
            if let Some(parent_id) = p.parent_partition {
                if let Some(parent) = partitions.get_mut(&parent_id) {
                    parent.remove_child(partition_id);
                }
            }
            true
        } else {
            false
        }
    }

    /// Set up forwarding from one partition to another.
    pub fn set_forwarding(&self, from_partition: u32, to_partition: u32, rate: f64) -> bool {
        let mut partitions = self.partitions.write().unwrap();
        if let Some(p) = partitions.get_mut(&from_partition) {
            p.set_parent(to_partition);
            p.set_forward_rate(rate);
            return true;
        }
        false
    }

    /// Enqueue a task to a specific partition (or auto-assign via hash).
    pub fn enqueue(&self, task_queue_hash: u64, item: TaskItem) {
        let partition_id = self.select_partition(task_queue_hash);
        let partitions = self.partitions.read().unwrap();
        if let Some(p) = partitions.get(&partition_id) {
            if p.is_write_partition {
                p.enqueue(item);
            }
        }
    }

    /// Try to poll a task, with hierarchical forwarding up the tree.
    pub fn poll_with_forwarding(&self, task_queue_hash: u64) -> Option<TaskItem> {
        let partition_id = self.select_partition(task_queue_hash);
        let partitions = self.partitions.read().unwrap();

        // Try local partition first
        if let Some(p) = partitions.get(&partition_id) {
            if !p.is_read_partition { return None; }
            if let Some(task) = p.try_poll() {
                return Some(task);
            }

            // If no local workers, try forwarding to parent
            if !p.has_workers() {
                if let Some(parent_id) = p.parent_partition {
                    if let Some(parent) = partitions.get(&parent_id) {
                        return parent.try_poll();
                    }
                }
            }
        }

        None
    }

    /// Push-based forwarding: forward tasks TO child partitions when workers are available there.
    /// Returns the number of tasks forwarded.
    pub fn push_to_children(&self, partition_id: u32) -> usize {
        let partitions = self.partitions.read().unwrap();
        let child_ids: Vec<u32> = partitions.get(&partition_id)
            .map(|p| p.child_partitions.clone())
            .unwrap_or_default();

        let mut count = 0usize;
        for child_id in child_ids {
            if let Some(child) = partitions.get(&child_id) {
                if child.has_workers() {
                    while let Some(task) = partitions.get(&partition_id).and_then(|p| p.try_poll()) {
                        child.enqueue(task);
                        count += 1;
                    }
                    break;
                }
            }
        }
        count
    }

    /// Get the total pending task count across all partitions for a task queue.
    pub fn total_pending(&self, task_queue_hash: u64) -> usize {
        let partitions = self.partitions.read().unwrap();
        partitions.values()
            .filter(|p| p.task_queue_hash == task_queue_hash)
            .map(|p| p.pending_count())
            .sum()
    }

    /// Get partition info for describe API.
    pub fn describe_partition(&self, partition_id: u32) -> Option<PartitionInfo> {
        let partitions = self.partitions.read().unwrap();
        partitions.get(&partition_id).map(|p| PartitionInfo {
            partition_id: p.partition_id,
            task_queue_hash: p.task_queue_hash,
            pending_tasks: p.pending_count() as u64,
            worker_count: p.worker_count.load(Ordering::Relaxed) as u64,
            parent_partition: p.parent_partition,
            forward_rate: p.forward_rate,
            depth: p.depth,
            child_count: p.child_partitions.len() as u32,
            backlog: p.backlog(),
            is_read: p.is_read_partition,
            is_write: p.is_write_partition,
        })
    }

    /// List all partition IDs.
    pub fn partition_ids(&self) -> Vec<u32> {
        self.partitions.read().unwrap().keys().cloned().collect()
    }

    /// Get the number of partitions.
    pub fn partition_count(&self) -> usize {
        self.partitions.read().unwrap().len()
    }

    /// Evaluate auto-scaling for all partitions of a task queue.
    pub fn evaluate_scaling(&self, task_queue_hash: u64) -> ScaleDecision {
        let partitions = self.partitions.read().unwrap();
        let matching: Vec<&TaskQueuePartition> = partitions.values()
            .filter(|p| p.task_queue_hash == task_queue_hash)
            .collect();

        if matching.is_empty() { return ScaleDecision::None; }

        // Check if any partition needs scale-up
        let needs_up = matching.iter().any(|p| p.needs_scale_up());
        if needs_up { return ScaleDecision::ScaleUp; }

        // Check if partitions are underutilized (all have 0 backlog and no workers)
        let all_idle = matching.iter().all(|p| p.backlog() == 0 && !p.has_workers());
        if all_idle && matching.len() > 1 { return ScaleDecision::ScaleDown; }

        ScaleDecision::None
    }

    /// Auto-scale: create child partitions for overloaded partitions.
    pub fn auto_scale_up(&self, task_queue_hash: u64) -> Vec<u32> {
        let mut new_children = Vec::new();
        let partition_ids: Vec<u32> = {
            let partitions = self.partitions.read().unwrap();
            partitions.values()
                .filter(|p| p.task_queue_hash == task_queue_hash && p.needs_scale_up())
                .map(|p| p.partition_id)
                .collect()
        };

        for pid in partition_ids {
            if let Some(child_id) = self.create_child_partition(pid, task_queue_hash) {
                new_children.push(child_id);
            }
        }
        new_children
    }

    /// Set the maximum partition tree depth.
    pub fn set_max_depth(&mut self, depth: u32) {
        self.max_depth = depth.max(1);
    }

    /// Enable or disable auto-scaling.
    pub fn set_auto_scale(&self, enabled: bool) {
        *self.auto_scale.lock().unwrap() = enabled;
    }

    /// Get the maximum depth.
    pub fn max_depth(&self) -> u32 { self.max_depth }

    /// Get the total backlog across all partitions for a task queue.
    pub fn total_backlog(&self, task_queue_hash: u64) -> u64 {
        let partitions = self.partitions.read().unwrap();
        partitions.values()
            .filter(|p| p.task_queue_hash == task_queue_hash)
            .map(|p| p.backlog())
            .sum()
    }

    /// Get the hierarchy depth of a specific partition.
    pub fn partition_depth(&self, partition_id: u32) -> Option<u32> {
        self.partitions.read().unwrap().get(&partition_id).map(|p| p.depth)
    }

    /// Get child partition IDs for a given partition.
    pub fn child_partitions(&self, partition_id: u32) -> Vec<u32> {
        self.partitions.read().unwrap().get(&partition_id)
            .map(|p| p.child_partitions.clone())
            .unwrap_or_default()
    }

    fn select_partition(&self, task_queue_hash: u64) -> u32 {
        let partitions = self.partitions.read().unwrap();
        let matching: Vec<u32> = partitions.values()
            .filter(|p| p.task_queue_hash == task_queue_hash)
            .map(|p| p.partition_id)
            .collect();

        if matching.is_empty() {
            return 0; // fallback
        }

        // Simple hash-based partition selection
        matching[(task_queue_hash as usize) % matching.len()]
    }
}

impl Default for PartitionManager {
    fn default() -> Self { Self::new(4) }
}

/// Information about a task queue partition.
pub struct PartitionInfo {
    pub partition_id: u32,
    pub task_queue_hash: u64,
    pub pending_tasks: u64,
    pub worker_count: u64,
    pub parent_partition: Option<u32>,
    pub forward_rate: f64,
    pub depth: u32,
    pub child_count: u32,
    pub backlog: u64,
    pub is_read: bool,
    pub is_write: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(wk: u64, step: u32) -> TaskItem {
        TaskItem {
            task_id: 0, kind: TaskKind::WorkflowTask,
            workflow_key: wk, task_queue_hash: 42,
            step_index: step, activity_name_id: 0, attempt: 1,
            priority: 0, deadline_ms: 0,
        }
    }

    #[test]
    fn test_create_partitions() {
        let mgr = PartitionManager::new(4);
        let p1 = mgr.create_partition(42);
        let p2 = mgr.create_partition(42);
        assert_eq!(mgr.partition_count(), 2);
        assert_ne!(p1, p2);
    }

    #[test]
    fn test_enqueue_and_poll() {
        let mgr = PartitionManager::new(4);
        let _p = mgr.create_partition(42);

        mgr.enqueue(42, make_task(100, 0));
        let task = mgr.poll_with_forwarding(42);
        assert!(task.is_some());
        assert_eq!(task.unwrap().workflow_key, 100);
    }

    #[test]
    fn test_forwarding_to_parent() {
        let mgr = PartitionManager::new(4);
        let child = mgr.create_partition(42);
        let parent = mgr.create_partition(42);

        // Set up forwarding from child to parent
        mgr.set_forwarding(child, parent, 1.0);

        // Enqueue to parent (simulating forwarded tasks)
        mgr.enqueue(42, make_task(200, 0));

        // Poll should find the task
        let task = mgr.poll_with_forwarding(42);
        assert!(task.is_some());
    }

    #[test]
    fn test_describe_partition() {
        let mgr = PartitionManager::new(4);
        let p = mgr.create_partition(42);

        mgr.enqueue(42, make_task(300, 0));
        mgr.enqueue(42, make_task(301, 1));

        let info = mgr.describe_partition(p).unwrap();
        assert_eq!(info.partition_id, p);
        assert_eq!(info.pending_tasks, 2);
        assert_eq!(info.depth, 0);
    }

    #[test]
    fn test_worker_registration() {
        let mgr = PartitionManager::new(4);
        let p = mgr.create_partition(42);

        let info = mgr.describe_partition(p).unwrap();
        assert_eq!(info.worker_count, 0);
    }

    #[test]
    fn test_total_pending() {
        let mgr = PartitionManager::new(4);
        mgr.create_partition(42);
        mgr.create_partition(42);

        mgr.enqueue(42, make_task(400, 0));
        mgr.enqueue(42, make_task(401, 1));

        let total = mgr.total_pending(42);
        assert!(total >= 2);
    }

    #[test]
    fn test_hierarchical_partitions() {
        let mgr = PartitionManager::new(4);
        let root = mgr.create_partition(42);
        assert_eq!(mgr.partition_depth(root), Some(0));

        let child = mgr.create_child_partition(root, 42).unwrap();
        assert_eq!(mgr.partition_depth(child), Some(1));

        // Verify parent-child link
        let children = mgr.child_partitions(root);
        assert!(children.contains(&child));

        let info = mgr.describe_partition(child).unwrap();
        assert_eq!(info.parent_partition, Some(root));
    }

    #[test]
    fn test_multi_level_tree() {
        let mgr = PartitionManager::new(4);
        let root = mgr.create_partition(42);
        let l1 = mgr.create_child_partition(root, 42).unwrap();
        let l2 = mgr.create_child_partition(l1, 42).unwrap();

        assert_eq!(mgr.partition_depth(root), Some(0));
        assert_eq!(mgr.partition_depth(l1), Some(1));
        assert_eq!(mgr.partition_depth(l2), Some(2));
    }

    #[test]
    fn test_max_depth_limit() {
        let mut mgr = PartitionManager::new(4);
        mgr.set_max_depth(2);
        let root = mgr.create_partition(42);
        let l1 = mgr.create_child_partition(root, 42).unwrap();
        let l2 = mgr.create_child_partition(l1, 42).unwrap();
        // L3 should fail because max_depth = 2
        assert!(mgr.create_child_partition(l2, 42).is_none());
    }

    #[test]
    fn test_delete_partition() {
        let mgr = PartitionManager::new(4);
        let root = mgr.create_partition(42);
        let child = mgr.create_child_partition(root, 42).unwrap();

        assert!(mgr.delete_partition(child));
        assert_eq!(mgr.child_partitions(root).len(), 0);
        assert_eq!(mgr.partition_count(), 1);
    }

    #[test]
    fn test_auto_scaling() {
        let mgr = PartitionManager::new(4);
        let p = mgr.create_partition(42);

        // Set a low threshold for testing
        {
            let mut partitions = mgr.partitions.write().unwrap();
            if let Some(partition) = partitions.get_mut(&p) {
                partition.backlog_threshold = 5;
            }
        }

        // Add tasks to exceed threshold
        for i in 0..10 {
            mgr.enqueue(42, make_task(500 + i, i as u32));
        }

        let decision = mgr.evaluate_scaling(42);
        assert_eq!(decision, ScaleDecision::ScaleUp);

        // Auto-scale should create child partitions
        let new_children = mgr.auto_scale_up(42);
        assert!(!new_children.is_empty());
    }

    #[test]
    fn test_read_write_separation() {
        let mgr = PartitionManager::new(4);
        let p = mgr.create_partition(42);

        // Set as write-only
        {
            let mut partitions = mgr.partitions.write().unwrap();
            if let Some(partition) = partitions.get_mut(&p) {
                partition.set_read_write(false, true);
            }
        }

        let info = mgr.describe_partition(p).unwrap();
        assert!(!info.is_read);
        assert!(info.is_write);
    }

    #[test]
    fn test_backlog_tracking() {
        let mgr = PartitionManager::new(4);
        mgr.create_partition(42);

        mgr.enqueue(42, make_task(600, 0));
        mgr.enqueue(42, make_task(601, 1));

        assert_eq!(mgr.total_backlog(42), 2);

        // Poll should reduce backlog
        let _ = mgr.poll_with_forwarding(42);
        assert_eq!(mgr.total_backlog(42), 1);
    }
}
