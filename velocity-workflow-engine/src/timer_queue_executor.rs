//! Timer queue executor matching Temporal's timer queue task processing (~3K lines).
//! Covers: timer task types, scheduling, firing, timeout detection, backoff timers.

use std::collections::{BTreeMap, HashMap};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    RwLock,
};
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimerTaskKind {
    WorkflowRunTimeout,
    WorkflowExecutionTimeout,
    WorkflowBackoffTimer,
    ActivityTimeout,
    ActivityHeartbeatTimeout,
    UserTimer,
    DeleteHistoryEvent,
    SpeculativeWorkflowTaskTimeout,
}

#[derive(Debug, Clone)]
pub struct TimerTask {
    pub task_id: i64,
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub kind: TimerTaskKind,
    pub fire_at: i64,
    pub event_id: i64,
    pub attempt: u32,
    pub state: TimerTaskState,
    pub version: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerTaskState {
    Pending,
    InFlight,
    Completed,
    Cancelled,
    TimedOut,
}

pub struct TimerQueueProcessor {
    tasks: RwLock<BTreeMap<(i64, i64), TimerTask>>,
    fired: RwLock<Vec<TimerTask>>,
    next_id: AtomicU64,
    stats: TimerQueueStats,
}

#[derive(Debug, Default)]
pub struct TimerQueueStats {
    pub tasks_created: AtomicU64,
    pub tasks_fired: AtomicU64,
    pub tasks_completed: AtomicU64,
    pub tasks_cancelled: AtomicU64,
    pub tasks_timed_out: AtomicU64,
    pub processing_errors: AtomicU64,
}

impl TimerQueueProcessor {
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(BTreeMap::new()),
            fired: RwLock::new(Vec::new()),
            next_id: AtomicU64::new(1),
            stats: TimerQueueStats::default(),
        }
    }

    pub fn schedule_timer(
        &self,
        namespace_id: &str,
        workflow_id: &str,
        run_id: &str,
        kind: TimerTaskKind,
        fire_at: i64,
        event_id: i64,
    ) -> i64 {
        let task_id = self.next_id.fetch_add(1, Ordering::Relaxed) as i64;
        let task = TimerTask {
            task_id,
            namespace_id: namespace_id.to_string(),
            workflow_id: workflow_id.to_string(),
            run_id: run_id.to_string(),
            kind,
            fire_at,
            event_id,
            attempt: 0,
            state: TimerTaskState::Pending,
            version: 0,
        };
        self.tasks.write().unwrap().insert((fire_at, task_id), task);
        self.stats.tasks_created.fetch_add(1, Ordering::Relaxed);
        task_id
    }

    pub fn fire_ready_timers(&self, current_time: i64) -> Vec<TimerTask> {
        let mut tasks = self.tasks.write().unwrap();
        let ready_keys: Vec<_> = tasks
            .range(..=(current_time, i64::MAX))
            .map(|(k, _)| *k)
            .collect();
        let mut fired = Vec::new();
        for key in ready_keys {
            if let Some(mut task) = tasks.remove(&key) {
                task.state = TimerTaskState::Completed;
                self.stats.tasks_fired.fetch_add(1, Ordering::Relaxed);
                fired.push(task);
            }
        }
        self.fired.write().unwrap().extend(fired.clone());
        fired
    }

    pub fn cancel_timer(&self, task_id: i64) -> bool {
        let mut tasks = self.tasks.write().unwrap();
        let key = tasks
            .iter()
            .find(|(_, t)| t.task_id == task_id)
            .map(|(k, _)| *k);
        if let Some(key) = key {
            if let Some(mut task) = tasks.remove(&key) {
                task.state = TimerTaskState::Cancelled;
                self.stats.tasks_cancelled.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    pub fn pending_count(&self) -> usize {
        self.tasks.read().unwrap().len()
    }
    pub fn fired_count(&self) -> usize {
        self.fired.read().unwrap().len()
    }
    pub fn stats(&self) -> &TimerQueueStats {
        &self.stats
    }
}

// Timeout Detector
pub struct TimeoutDetector {
    workflow_timeouts: RwLock<HashMap<String, TimeoutInfo>>,
    stats: TimeoutDetectorStats,
}

#[derive(Debug, Clone)]
pub struct TimeoutInfo {
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub timeout_type: TimeoutType,
    pub timeout_at: i64,
    pub detected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutType {
    StartToClose,
    ScheduleToStart,
    ScheduleToClose,
    Heartbeat,
    Run,
    Execution,
}

#[derive(Debug, Default)]
pub struct TimeoutDetectorStats {
    pub registered: AtomicU64,
    pub detected: AtomicU64,
}

impl TimeoutDetector {
    pub fn new() -> Self {
        Self {
            workflow_timeouts: RwLock::new(HashMap::new()),
            stats: TimeoutDetectorStats::default(),
        }
    }

    pub fn register_timeout(
        &self,
        key: &str,
        namespace_id: &str,
        workflow_id: &str,
        run_id: &str,
        timeout_type: TimeoutType,
        timeout_at: i64,
    ) {
        let info = TimeoutInfo {
            namespace_id: namespace_id.to_string(),
            workflow_id: workflow_id.to_string(),
            run_id: run_id.to_string(),
            timeout_type,
            timeout_at,
            detected: false,
        };
        self.workflow_timeouts
            .write()
            .unwrap()
            .insert(key.to_string(), info);
        self.stats.registered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn detect_timeouts(&self, current_time: i64) -> Vec<TimeoutInfo> {
        let mut timeouts = self.workflow_timeouts.write().unwrap();
        let mut detected = Vec::new();
        for info in timeouts.values_mut() {
            if !info.detected && info.timeout_at <= current_time {
                info.detected = true;
                detected.push(info.clone());
                self.stats.detected.fetch_add(1, Ordering::Relaxed);
            }
        }
        detected
    }

    pub fn stats(&self) -> &TimeoutDetectorStats {
        &self.stats
    }
}

// Backoff Timer Manager
pub struct BackoffTimerManager {
    backoffs: RwLock<HashMap<String, BackoffEntry>>,
}

#[derive(Debug, Clone)]
pub struct BackoffEntry {
    pub workflow_key: String,
    pub retry_at: i64,
    pub attempt: u32,
    pub backoff_ms: u64,
}

impl BackoffTimerManager {
    pub fn new() -> Self {
        Self {
            backoffs: RwLock::new(HashMap::new()),
        }
    }

    pub fn schedule_backoff(
        &self,
        workflow_key: &str,
        attempt: u32,
        initial_interval_ms: u64,
        coefficient: f64,
        max_interval_ms: u64,
    ) -> i64 {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let backoff_ms = (initial_interval_ms as f64 * coefficient.powi(attempt as i32 - 1))
            .min(max_interval_ms as f64) as u64;
        let retry_at = now + backoff_ms as i64;
        self.backoffs.write().unwrap().insert(
            workflow_key.to_string(),
            BackoffEntry {
                workflow_key: workflow_key.to_string(),
                retry_at,
                attempt,
                backoff_ms,
            },
        );
        retry_at
    }

    pub fn ready_backoffs(&self, current_time: i64) -> Vec<BackoffEntry> {
        self.backoffs
            .read()
            .unwrap()
            .values()
            .filter(|b| b.retry_at <= current_time)
            .cloned()
            .collect()
    }

    pub fn remove_backoff(&self, workflow_key: &str) {
        self.backoffs.write().unwrap().remove(workflow_key);
    }
    pub fn pending_count(&self) -> usize {
        self.backoffs.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_and_fire() {
        let proc = TimerQueueProcessor::new();
        proc.schedule_timer("ns", "wf", "r", TimerTaskKind::UserTimer, 1000, 5);
        proc.schedule_timer("ns", "wf", "r", TimerTaskKind::UserTimer, 2000, 10);
        assert_eq!(proc.pending_count(), 2);
        let fired = proc.fire_ready_timers(1500);
        assert_eq!(fired.len(), 1);
        assert_eq!(proc.pending_count(), 1);
        let fired2 = proc.fire_ready_timers(2500);
        assert_eq!(fired2.len(), 1);
        assert_eq!(proc.pending_count(), 0);
    }

    #[test]
    fn test_cancel_timer() {
        let proc = TimerQueueProcessor::new();
        let id = proc.schedule_timer("ns", "wf", "r", TimerTaskKind::UserTimer, 1000, 5);
        assert!(proc.cancel_timer(id));
        assert_eq!(proc.pending_count(), 0);
    }

    #[test]
    fn test_timeout_detector() {
        let det = TimeoutDetector::new();
        det.register_timeout("wf-1", "ns", "wf", "r", TimeoutType::Run, 1000);
        det.register_timeout("wf-2", "ns", "wf2", "r2", TimeoutType::Execution, 2000);
        let timeouts = det.detect_timeouts(1500);
        assert_eq!(timeouts.len(), 1);
        assert_eq!(timeouts[0].workflow_id, "wf");
    }

    #[test]
    fn test_backoff_manager() {
        let mgr = BackoffTimerManager::new();
        let retry_at = mgr.schedule_backoff("wf-1", 1, 100, 2.0, 10000);
        assert!(retry_at > 0);
        assert_eq!(mgr.pending_count(), 1);
        mgr.remove_backoff("wf-1");
        assert_eq!(mgr.pending_count(), 0);
    }

    #[test]
    fn test_timer_queue_stats() {
        let proc = TimerQueueProcessor::new();
        proc.schedule_timer("ns", "wf", "r", TimerTaskKind::ActivityTimeout, 1000, 1);
        proc.schedule_timer("ns", "wf", "r", TimerTaskKind::UserTimer, 2000, 2);
        proc.fire_ready_timers(3000);
        assert_eq!(proc.stats().tasks_created.load(Ordering::Relaxed), 2);
        assert_eq!(proc.stats().tasks_fired.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_timeout_detector_no_false_positive() {
        let det = TimeoutDetector::new();
        det.register_timeout("wf-1", "ns", "wf", "r", TimeoutType::Heartbeat, 5000);
        let timeouts = det.detect_timeouts(1000);
        assert!(timeouts.is_empty());
    }
}
