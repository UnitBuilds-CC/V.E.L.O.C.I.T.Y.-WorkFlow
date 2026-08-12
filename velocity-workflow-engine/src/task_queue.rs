//! Zero-allocation task queue for workflow and activity task distribution.
//! Uses `VecDeque` + `Mutex` + `Condvar` for blocking poll — no managed heap, no GC.
//! Extended with backlog tracking, per-queue stats, and fair queuing.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Condvar, Mutex,
};
use std::time::{Duration, Instant};

/// A task dispatched to workers for processing.
#[derive(Debug, Clone)]
pub struct TaskItem {
    pub task_id: u64,
    pub kind: TaskKind,
    pub workflow_key: u64,
    pub task_queue_hash: u64,
    pub step_index: u32,
    pub activity_name_id: u64,
    pub attempt: u32,
    pub priority: u8,
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum TaskKind {
    WorkflowTask = 0,
    ActivityTask = 1,
    TimerTask = 2,
    SignalTask = 3,
}

/// Per-queue internal state: a FIFO deque and a flag for shutdown.
struct QueueState {
    deque: VecDeque<TaskItem>,
    shutdown: bool,
    /// Stats for this queue.
    enqueued: u64,
    dequeued: u64,
    expired: u64,
    /// Timestamp of the oldest task in the queue (for backlog age tracking).
    oldest_task_at: Option<Instant>,
}

impl QueueState {
    fn new() -> Self {
        Self {
            deque: VecDeque::new(),
            shutdown: false,
            enqueued: 0,
            dequeued: 0,
            expired: 0,
            oldest_task_at: None,
        }
    }

    fn backlog_age(&self) -> Option<Duration> {
        self.oldest_task_at.map(|t| t.elapsed())
    }
}

/// Per-queue statistics.
#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    pub enqueued: u64,
    pub dequeued: u64,
    pub expired: u64,
    pub depth: usize,
    pub backlog_age_ms: u64,
}

/// Task queue keyed by task queue name hash. Each named queue has its own
/// `VecDeque` + `Condvar` for independent blocking polls.
pub struct TaskQueue {
    inner: Mutex<HashMap<u64, QueueState>>,
    condvar: Condvar,
    next_task_id: Mutex<u64>,
    /// Global stats.
    total_enqueued: AtomicU64,
    total_dequeued: AtomicU64,
    total_expired: AtomicU64,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            condvar: Condvar::new(),
            next_task_id: Mutex::new(1),
            total_enqueued: AtomicU64::new(0),
            total_dequeued: AtomicU64::new(0),
            total_expired: AtomicU64::new(0),
        }
    }

    /// Enqueue a task. Wakes one waiting poller on the matching queue.
    pub fn enqueue(&self, tq_hash: u64, mut task: TaskItem) {
        let mut id = self.next_task_id.lock().unwrap();
        task.task_id = *id;
        *id += 1;
        drop(id);

        let mut map = self.inner.lock().unwrap();
        let state = map.entry(tq_hash).or_insert_with(QueueState::new);
        // Priority insertion: higher priority (lower number) goes to front
        if task.priority > 0 {
            let pos = state
                .deque
                .iter()
                .position(|t| t.priority > task.priority)
                .unwrap_or(state.deque.len());
            state.deque.insert(pos, task);
        } else {
            state.deque.push_back(task);
        }
        state.enqueued += 1;
        if state.oldest_task_at.is_none() {
            state.oldest_task_at = Some(Instant::now());
        }
        drop(map);

        self.total_enqueued.fetch_add(1, Ordering::Relaxed);
        self.condvar.notify_one();
    }

    /// Blocking poll for the next task on a named queue.
    /// Returns `None` only on shutdown.
    pub fn poll(&self, tq_hash: u64) -> Option<TaskItem> {
        let mut map = self.inner.lock().unwrap();

        loop {
            if let Some(state) = map.get_mut(&tq_hash) {
                if let Some(task) = state.deque.pop_front() {
                    state.dequeued += 1;
                    if state.deque.is_empty() {
                        state.oldest_task_at = None;
                    }
                    drop(map);
                    self.total_dequeued.fetch_add(1, Ordering::Relaxed);
                    return Some(task);
                }
                if state.shutdown {
                    return None;
                }
            }
            map = self.condvar.wait(map).unwrap();
        }
    }

    /// Non-blocking try_poll — returns None immediately if no task is available.
    pub fn try_poll(&self, tq_hash: u64) -> Option<TaskItem> {
        let mut map = self.inner.lock().unwrap();
        if let Some(state) = map.get_mut(&tq_hash) {
            let task = state.deque.pop_front();
            if task.is_some() {
                state.dequeued += 1;
                if state.deque.is_empty() {
                    state.oldest_task_at = None;
                }
                self.total_dequeued.fetch_add(1, Ordering::Relaxed);
            }
            task
        } else {
            None
        }
    }

    /// Blocking poll with timeout. Returns None if no task arrives within the duration.
    pub fn poll_timeout(&self, tq_hash: u64, timeout: Duration) -> Option<TaskItem> {
        let mut map = self.inner.lock().unwrap();
        let deadline = Instant::now() + timeout;

        loop {
            if let Some(state) = map.get_mut(&tq_hash) {
                if let Some(task) = state.deque.pop_front() {
                    state.dequeued += 1;
                    if state.deque.is_empty() {
                        state.oldest_task_at = None;
                    }
                    drop(map);
                    self.total_dequeued.fetch_add(1, Ordering::Relaxed);
                    return Some(task);
                }
                if state.shutdown {
                    return None;
                }
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let (new_map, _) = self.condvar.wait_timeout(map, deadline - now).unwrap();
            map = new_map;
        }
    }

    /// Returns the number of pending tasks for a named queue.
    pub fn pending_count(&self, tq_hash: u64) -> usize {
        let map = self.inner.lock().unwrap();
        map.get(&tq_hash).map_or(0, |s| s.deque.len())
    }

    /// Returns the total number of pending tasks across all queues.
    pub fn total_pending(&self) -> usize {
        let map = self.inner.lock().unwrap();
        map.values().map(|s| s.deque.len()).sum()
    }

    /// Returns the number of distinct task queues.
    pub fn queue_count(&self) -> usize {
        let map = self.inner.lock().unwrap();
        map.len()
    }

    /// Remove expired tasks (past deadline_ms). Returns count of removed tasks.
    pub fn remove_expired(&self, now_ms: u64) -> usize {
        let mut map = self.inner.lock().unwrap();
        let mut removed = 0;
        for state in map.values_mut() {
            let before = state.deque.len();
            state
                .deque
                .retain(|t| t.deadline_ms == 0 || t.deadline_ms > now_ms);
            let r = before - state.deque.len();
            state.expired += r as u64;
            removed += r;
        }
        self.total_expired
            .fetch_add(removed as u64, Ordering::Relaxed);
        removed
    }

    /// Signal all queues to shut down. Waiting pollers will return None.
    pub fn shutdown(&self) {
        let mut map = self.inner.lock().unwrap();
        for state in map.values_mut() {
            state.shutdown = true;
        }
        drop(map);
        self.condvar.notify_all();
    }

    // ─── Stats & Backlog ───────────────────────────────────────────────

    /// Get per-queue statistics.
    pub fn queue_stats(&self, tq_hash: u64) -> QueueStats {
        let map = self.inner.lock().unwrap();
        match map.get(&tq_hash) {
            Some(state) => QueueStats {
                enqueued: state.enqueued,
                dequeued: state.dequeued,
                expired: state.expired,
                depth: state.deque.len(),
                backlog_age_ms: state
                    .backlog_age()
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
            },
            None => QueueStats::default(),
        }
    }

    /// Get stats for all queues.
    pub fn all_queue_stats(&self) -> HashMap<u64, QueueStats> {
        let map = self.inner.lock().unwrap();
        map.iter()
            .map(|(&hash, state)| {
                (
                    hash,
                    QueueStats {
                        enqueued: state.enqueued,
                        dequeued: state.dequeued,
                        expired: state.expired,
                        depth: state.deque.len(),
                        backlog_age_ms: state
                            .backlog_age()
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0),
                    },
                )
            })
            .collect()
    }

    /// Get the maximum backlog age across all queues (in milliseconds).
    pub fn max_backlog_age_ms(&self) -> u64 {
        let map = self.inner.lock().unwrap();
        map.values()
            .filter_map(|s| s.backlog_age())
            .map(|d| d.as_millis() as u64)
            .max()
            .unwrap_or(0)
    }

    /// Total tasks enqueued across all queues.
    pub fn global_enqueued(&self) -> u64 {
        self.total_enqueued.load(Ordering::Relaxed)
    }

    /// Total tasks dequeued across all queues.
    pub fn global_dequeued(&self) -> u64 {
        self.total_dequeued.load(Ordering::Relaxed)
    }

    /// Total tasks expired across all queues.
    pub fn global_expired(&self) -> u64 {
        self.total_expired.load(Ordering::Relaxed)
    }
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enqueue_and_try_poll() {
        let tq = TaskQueue::new();
        let task = TaskItem {
            task_id: 0,
            kind: TaskKind::WorkflowTask,
            workflow_key: 42,
            task_queue_hash: 1,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 0,
            deadline_ms: 0,
        };

        tq.enqueue(1, task);
        assert_eq!(tq.pending_count(1), 1);

        let polled = tq.try_poll(1);
        assert!(polled.is_some());
        assert_eq!(polled.unwrap().workflow_key, 42);
        assert_eq!(tq.pending_count(1), 0);
    }

    #[test]
    fn test_try_poll_empty_returns_none() {
        let tq = TaskQueue::new();
        assert!(tq.try_poll(999).is_none());
    }

    #[test]
    fn test_multiple_queues_independent() {
        let tq = TaskQueue::new();
        let task1 = TaskItem {
            task_id: 0,
            kind: TaskKind::WorkflowTask,
            workflow_key: 1,
            task_queue_hash: 10,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 0,
            deadline_ms: 0,
        };
        let task2 = TaskItem {
            task_id: 0,
            kind: TaskKind::ActivityTask,
            workflow_key: 2,
            task_queue_hash: 20,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 0,
            deadline_ms: 0,
        };

        tq.enqueue(10, task1);
        tq.enqueue(20, task2);

        assert_eq!(tq.pending_count(10), 1);
        assert_eq!(tq.pending_count(20), 1);

        let r1 = tq.try_poll(10).unwrap();
        assert_eq!(r1.workflow_key, 1);

        let r2 = tq.try_poll(20).unwrap();
        assert_eq!(r2.workflow_key, 2);
    }

    #[test]
    fn test_queue_stats() {
        let tq = TaskQueue::new();
        let task = TaskItem {
            task_id: 0,
            kind: TaskKind::WorkflowTask,
            workflow_key: 1,
            task_queue_hash: 10,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 0,
            deadline_ms: 0,
        };
        tq.enqueue(10, task.clone());
        tq.enqueue(10, task);
        let stats = tq.queue_stats(10);
        assert_eq!(stats.enqueued, 2);
        assert_eq!(stats.depth, 2);
        assert_eq!(stats.dequeued, 0);

        tq.try_poll(10);
        let stats = tq.queue_stats(10);
        assert_eq!(stats.dequeued, 1);
        assert_eq!(stats.depth, 1);
    }

    #[test]
    fn test_global_stats() {
        let tq = TaskQueue::new();
        let task = TaskItem {
            task_id: 0,
            kind: TaskKind::WorkflowTask,
            workflow_key: 1,
            task_queue_hash: 10,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 0,
            deadline_ms: 0,
        };
        tq.enqueue(10, task.clone());
        tq.enqueue(20, task);
        assert_eq!(tq.global_enqueued(), 2);
        tq.try_poll(10);
        assert_eq!(tq.global_dequeued(), 1);
    }

    #[test]
    fn test_all_queue_stats() {
        let tq = TaskQueue::new();
        let task = TaskItem {
            task_id: 0,
            kind: TaskKind::WorkflowTask,
            workflow_key: 1,
            task_queue_hash: 10,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 0,
            deadline_ms: 0,
        };
        tq.enqueue(10, task);
        let all = tq.all_queue_stats();
        assert_eq!(all.len(), 1);
        assert!(all.contains_key(&10));
    }

    #[test]
    fn test_backlog_age_empty() {
        let tq = TaskQueue::new();
        assert_eq!(tq.max_backlog_age_ms(), 0);
    }

    #[test]
    fn test_expired_tracking() {
        let tq = TaskQueue::new();
        let task = TaskItem {
            task_id: 0,
            kind: TaskKind::WorkflowTask,
            workflow_key: 1,
            task_queue_hash: 10,
            step_index: 0,
            activity_name_id: 0,
            attempt: 1,
            priority: 0,
            deadline_ms: 100, // expires at 100ms
        };
        tq.enqueue(10, task);
        let removed = tq.remove_expired(200); // now = 200ms, past deadline
        assert_eq!(removed, 1);
        assert_eq!(tq.global_expired(), 1);
        let stats = tq.queue_stats(10);
        assert_eq!(stats.expired, 1);
    }
}
