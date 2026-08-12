//! Zero-allocation task queue for workflow and activity task distribution.
//! Uses `VecDeque` + `Mutex` + `Condvar` for blocking poll — no managed heap, no GC.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, Condvar};

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
}

impl QueueState {
    fn new() -> Self {
        Self {
            deque: VecDeque::new(),
            shutdown: false,
        }
    }
}

/// Task queue keyed by task queue name hash. Each named queue has its own
/// `VecDeque` + `Condvar` for independent blocking polls.
pub struct TaskQueue {
    inner: Mutex<HashMap<u64, QueueState>>,
    condvar: Condvar,
    next_task_id: Mutex<u64>,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            condvar: Condvar::new(),
            next_task_id: Mutex::new(1),
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
            // Find insertion point based on priority
            let pos = state.deque.iter().position(|t| t.priority > task.priority).unwrap_or(state.deque.len());
            state.deque.insert(pos, task);
        } else {
            state.deque.push_back(task);
        }
        drop(map);

        self.condvar.notify_one();
    }

    /// Blocking poll for the next task on a named queue.
    /// Returns `None` only on shutdown.
    pub fn poll(&self, tq_hash: u64) -> Option<TaskItem> {
        let mut map = self.inner.lock().unwrap();

        loop {
            // Check if there's a task available
            if let Some(state) = map.get_mut(&tq_hash) {
                if let Some(task) = state.deque.pop_front() {
                    return Some(task);
                }
                if state.shutdown {
                    return None;
                }
            }

            // No task available — block until signaled
            map = self.condvar.wait(map).unwrap();
        }
    }

    /// Non-blocking try_poll — returns None immediately if no task is available.
    pub fn try_poll(&self, tq_hash: u64) -> Option<TaskItem> {
        let mut map = self.inner.lock().unwrap();
        if let Some(state) = map.get_mut(&tq_hash) {
            state.deque.pop_front()
        } else {
            None
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
            state.deque.retain(|t| t.deadline_ms == 0 || t.deadline_ms > now_ms);
            removed += before - state.deque.len();
        }
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
            task_id: 0, kind: TaskKind::WorkflowTask, workflow_key: 1,
            task_queue_hash: 10, step_index: 0, activity_name_id: 0, attempt: 1,
            priority: 0, deadline_ms: 0,
        };
        let task2 = TaskItem {
            task_id: 0, kind: TaskKind::ActivityTask, workflow_key: 2,
            task_queue_hash: 20, step_index: 0, activity_name_id: 0, attempt: 1,
            priority: 0, deadline_ms: 0,
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
}
