//! Reachability API — determines if workers are reachable by workflows.
//!
//! This is used to check whether task queues have active workers polling,
//! which helps determine if workflows can make progress.

use std::collections::HashMap;
use std::sync::Mutex;

/// Type of reachability being checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReachabilityType {
    /// Reachable by open (running) workflows.
    OpenWorkflows,
    /// Reachable by closed (completed) workflows.
    ClosedWorkflows,
    /// Reachable by open scheduled workflows.
    OpenScheduledWorkflows,
    /// Reachable by new workflows.
    NewWorkflows,
}

/// A reachability query.
#[derive(Debug, Clone)]
pub struct ReachabilityQuery {
    pub task_queue: String,
    pub workflow_type: Option<String>,
    pub namespace: Option<String>,
}

/// Result of a reachability check.
#[derive(Debug, Clone)]
pub struct ReachabilityResult {
    pub task_queue: String,
    pub is_reachable: bool,
    pub reachability_type: ReachabilityType,
    pub last_seen: Option<u64>,
    pub worker_count: usize,
}

/// Tracks worker polling activity for reachability checks.
pub struct ReachabilityTracker {
    /// task_queue -> (last_poll_timestamp, worker_count)
    task_queue_activity: Mutex<HashMap<String, (u64, usize)>>,
}

impl ReachabilityTracker {
    pub fn new() -> Self {
        Self {
            task_queue_activity: Mutex::new(HashMap::new()),
        }
    }

    /// Record a poll from a worker on a task queue.
    pub fn record_poll(&self, task_queue: &str, timestamp: u64) {
        let mut activity = self.task_queue_activity.lock().unwrap();
        let entry = activity.entry(task_queue.to_string()).or_insert((0, 0));
        entry.0 = timestamp;
        entry.1 += 1;
    }

    /// Record a worker disconnect.
    pub fn record_disconnect(&self, task_queue: &str) {
        let mut activity = self.task_queue_activity.lock().unwrap();
        if let Some(entry) = activity.get_mut(task_queue) {
            entry.1 = entry.1.saturating_sub(1);
        }
    }

    /// Check reachability for a task queue.
    pub fn check_reachability(&self, query: &ReachabilityQuery) -> ReachabilityResult {
        let activity = self.task_queue_activity.lock().unwrap();
        let (last_seen, worker_count) = activity.get(&query.task_queue).copied().unwrap_or((0, 0));

        let is_reachable = worker_count > 0;

        ReachabilityResult {
            task_queue: query.task_queue.clone(),
            is_reachable,
            reachability_type: ReachabilityType::OpenWorkflows,
            last_seen: if last_seen > 0 { Some(last_seen) } else { None },
            worker_count,
        }
    }

    /// Check reachability for a specific task queue.
    pub fn check_task_queue(&self, task_queue: &str) -> ReachabilityResult {
        let query = ReachabilityQuery {
            task_queue: task_queue.to_string(),
            workflow_type: None,
            namespace: None,
        };
        self.check_reachability(&query)
    }

    /// List all unreachable task queues.
    pub fn list_unreachable(&self) -> Vec<String> {
        let activity = self.task_queue_activity.lock().unwrap();
        activity
            .iter()
            .filter(|(_, (_, count))| *count == 0)
            .map(|(queue, _)| queue.clone())
            .collect()
    }

    /// List all reachable task queues.
    pub fn list_reachable(&self) -> Vec<String> {
        let activity = self.task_queue_activity.lock().unwrap();
        activity
            .iter()
            .filter(|(_, (_, count))| *count > 0)
            .map(|(queue, _)| queue.clone())
            .collect()
    }

    /// Get activity summary.
    pub fn activity_summary(&self) -> HashMap<String, (u64, usize)> {
        self.task_queue_activity.lock().unwrap().clone()
    }

    /// Clear all activity data.
    pub fn clear(&self) {
        self.task_queue_activity.lock().unwrap().clear();
    }

    /// Count tracked task queues.
    pub fn tracked_count(&self) -> usize {
        self.task_queue_activity.lock().unwrap().len()
    }
}

impl Default for ReachabilityTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reachability_tracker_basic() {
        let tracker = ReachabilityTracker::new();
        tracker.record_poll("orders", 1000);
        tracker.record_poll("orders", 1001);
        tracker.record_poll("payments", 1002);

        let result = tracker.check_task_queue("orders");
        assert!(result.is_reachable);
        assert_eq!(result.worker_count, 2);
        assert_eq!(result.last_seen, Some(1001));

        let result = tracker.check_task_queue("payments");
        assert!(result.is_reachable);
        assert_eq!(result.worker_count, 1);
    }

    #[test]
    fn test_reachability_unreachable() {
        let tracker = ReachabilityTracker::new();
        tracker.record_poll("orders", 1000);
        tracker.record_disconnect("orders");
        tracker.record_disconnect("orders");

        let result = tracker.check_task_queue("orders");
        assert!(!result.is_reachable);
        assert_eq!(result.worker_count, 0);
    }

    #[test]
    fn test_reachability_unknown_queue() {
        let tracker = ReachabilityTracker::new();
        let result = tracker.check_task_queue("nonexistent");
        assert!(!result.is_reachable);
        assert_eq!(result.worker_count, 0);
        assert!(result.last_seen.is_none());
    }

    #[test]
    fn test_list_reachable_unreachable() {
        let tracker = ReachabilityTracker::new();
        tracker.record_poll("active-queue", 1000);
        tracker.record_poll("dead-queue", 500);
        tracker.record_disconnect("dead-queue");

        let reachable = tracker.list_reachable();
        assert_eq!(reachable.len(), 1);
        assert!(reachable.contains(&"active-queue".to_string()));

        let unreachable = tracker.list_unreachable();
        assert_eq!(unreachable.len(), 1);
        assert!(unreachable.contains(&"dead-queue".to_string()));
    }

    #[test]
    fn test_reachability_query_with_filters() {
        let tracker = ReachabilityTracker::new();
        tracker.record_poll("orders", 1000);

        let query = ReachabilityQuery {
            task_queue: "orders".to_string(),
            workflow_type: Some("OrderWorkflow".to_string()),
            namespace: Some("default".to_string()),
        };

        let result = tracker.check_reachability(&query);
        assert!(result.is_reachable);
        assert_eq!(result.task_queue, "orders");
    }

    #[test]
    fn test_activity_summary() {
        let tracker = ReachabilityTracker::new();
        tracker.record_poll("q1", 100);
        tracker.record_poll("q1", 200);
        tracker.record_poll("q2", 300);

        let summary = tracker.activity_summary();
        assert_eq!(summary.len(), 2);
        assert_eq!(summary["q1"], (200, 2));
        assert_eq!(summary["q2"], (300, 1));
    }

    #[test]
    fn test_clear() {
        let tracker = ReachabilityTracker::new();
        tracker.record_poll("q1", 100);
        tracker.record_poll("q2", 200);
        assert_eq!(tracker.tracked_count(), 2);

        tracker.clear();
        assert_eq!(tracker.tracked_count(), 0);
    }

    #[test]
    fn test_disconnect_saturates_at_zero() {
        let tracker = ReachabilityTracker::new();
        tracker.record_disconnect("never-existed");
        let result = tracker.check_task_queue("never-existed");
        assert_eq!(result.worker_count, 0);
    }
}
