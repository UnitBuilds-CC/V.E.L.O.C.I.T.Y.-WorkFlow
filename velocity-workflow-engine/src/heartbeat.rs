//! Activity heartbeat protocol — records heartbeats, detects timeouts, and manages
//! the full heartbeat lifecycle for activities.
//!
//! Features:
//! - Registration with configurable timeout per activity
//! - Heartbeat recording with detail payload persistence
//! - Timeout detection with configurable miss threshold
//! - Last-details retrieval (for retry context passing)
//! - Batch timeout scanning for background timer integration
//! - Activity lifecycle hooks (start, heartbeat, complete, fail, timeout)
//! - Statistics tracking (total heartbeats, timeout count, etc.)

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Record of a single activity's heartbeat state.
#[derive(Debug, Clone)]
pub struct HeartbeatRecord {
    pub workflow_key: u64,
    pub activity_id: u64,
    pub registered_at: Instant,
    pub last_heartbeat: Instant,
    pub timeout: Duration,
    pub max_misses: u32,
    pub miss_count: u32,
    pub last_details: Option<Vec<u8>>,
    pub detail_history: Vec<HeartbeatDetail>,
    pub total_heartbeats: u64,
    pub state: HeartbeatState,
}

#[derive(Debug, Clone)]
pub struct HeartbeatDetail {
    pub sequence: u64,
    pub timestamp: Instant,
    pub payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum HeartbeatState {
    Pending = 0,
    Active = 1,
    Completed = 2,
    Failed = 3,
    TimedOut = 4,
    Cancelled = 5,
}

#[derive(Debug, Clone)]
pub struct TimeoutEvent {
    pub workflow_key: u64,
    pub activity_id: u64,
    pub miss_count: u32,
    pub last_details: Option<Vec<u8>>,
    pub elapsed_since_last: Duration,
}

#[derive(Debug, Clone, Default)]
pub struct HeartbeatStats {
    pub total_registered: u64,
    pub total_heartbeats: u64,
    pub total_timeouts: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub total_cancelled: u64,
    pub currently_active: u64,
}

pub struct HeartbeatTracker {
    records: Mutex<HashMap<(u64, u64), HeartbeatRecord>>,
    stats: Mutex<HeartbeatStats>,
}

impl HeartbeatTracker {
    pub fn new() -> Self {
        Self {
            records: Mutex::new(HashMap::new()),
            stats: Mutex::new(HeartbeatStats::default()),
        }
    }

    pub fn register(&self, workflow_key: u64, activity_id: u64, timeout_ms: u64, max_misses: u32) {
        let record = HeartbeatRecord {
            workflow_key,
            activity_id,
            registered_at: Instant::now(),
            last_heartbeat: Instant::now(),
            timeout: Duration::from_millis(timeout_ms),
            max_misses: max_misses.max(1),
            miss_count: 0,
            last_details: None,
            detail_history: Vec::new(),
            total_heartbeats: 0,
            state: HeartbeatState::Pending,
        };
        let mut records = self.records.lock().unwrap();
        records.insert((workflow_key, activity_id), record);
        let mut stats = self.stats.lock().unwrap();
        stats.total_registered += 1;
        stats.currently_active += 1;
    }

    pub fn record_heartbeat(
        &self,
        workflow_key: u64,
        activity_id: u64,
        details: Option<Vec<u8>>,
    ) -> bool {
        let mut records = self.records.lock().unwrap();
        if let Some(rec) = records.get_mut(&(workflow_key, activity_id)) {
            if rec.state != HeartbeatState::Pending && rec.state != HeartbeatState::Active {
                return false;
            }
            rec.last_heartbeat = Instant::now();
            rec.miss_count = 0;
            rec.total_heartbeats += 1;
            rec.state = HeartbeatState::Active;
            let seq = rec.total_heartbeats;
            rec.detail_history.push(HeartbeatDetail {
                sequence: seq,
                timestamp: Instant::now(),
                payload: details.clone(),
            });
            rec.last_details = details;
            let mut stats = self.stats.lock().unwrap();
            stats.total_heartbeats += 1;
            true
        } else {
            false
        }
    }

    pub fn get_last_details(&self, workflow_key: u64, activity_id: u64) -> Option<Vec<u8>> {
        let records = self.records.lock().unwrap();
        records
            .get(&(workflow_key, activity_id))
            .and_then(|rec| rec.last_details.clone())
    }

    pub fn get_detail_history(&self, workflow_key: u64, activity_id: u64) -> Vec<HeartbeatDetail> {
        let records = self.records.lock().unwrap();
        records
            .get(&(workflow_key, activity_id))
            .map(|rec| rec.detail_history.clone())
            .unwrap_or_default()
    }

    pub fn get_state(&self, workflow_key: u64, activity_id: u64) -> Option<HeartbeatState> {
        let records = self.records.lock().unwrap();
        records
            .get(&(workflow_key, activity_id))
            .map(|rec| rec.state)
    }

    pub fn check_timeouts(&self) -> Vec<TimeoutEvent> {
        let mut records = self.records.lock().unwrap();
        let mut timed_out = Vec::new();
        for (key, rec) in records.iter_mut() {
            if rec.state != HeartbeatState::Pending && rec.state != HeartbeatState::Active {
                continue;
            }
            let elapsed = rec.last_heartbeat.elapsed();
            if elapsed > rec.timeout {
                rec.miss_count += 1;
                if rec.miss_count >= rec.max_misses {
                    rec.state = HeartbeatState::TimedOut;
                    timed_out.push(TimeoutEvent {
                        workflow_key: key.0,
                        activity_id: key.1,
                        miss_count: rec.miss_count,
                        last_details: rec.last_details.clone(),
                        elapsed_since_last: elapsed,
                    });
                }
            }
        }
        if !timed_out.is_empty() {
            let mut stats = self.stats.lock().unwrap();
            stats.total_timeouts += timed_out.len() as u64;
            stats.currently_active -= timed_out.len() as u64;
        }
        timed_out
    }

    pub fn complete(&self, workflow_key: u64, activity_id: u64) {
        let mut records = self.records.lock().unwrap();
        if let Some(rec) = records.get_mut(&(workflow_key, activity_id)) {
            rec.state = HeartbeatState::Completed;
            let mut stats = self.stats.lock().unwrap();
            stats.total_completed += 1;
            stats.currently_active = stats.currently_active.saturating_sub(1);
        }
    }

    pub fn fail(&self, workflow_key: u64, activity_id: u64) {
        let mut records = self.records.lock().unwrap();
        if let Some(rec) = records.get_mut(&(workflow_key, activity_id)) {
            rec.state = HeartbeatState::Failed;
            let mut stats = self.stats.lock().unwrap();
            stats.total_failed += 1;
            stats.currently_active = stats.currently_active.saturating_sub(1);
        }
    }

    pub fn cancel(&self, workflow_key: u64, activity_id: u64) {
        let mut records = self.records.lock().unwrap();
        if let Some(rec) = records.get_mut(&(workflow_key, activity_id)) {
            rec.state = HeartbeatState::Cancelled;
            let mut stats = self.stats.lock().unwrap();
            stats.total_cancelled += 1;
            stats.currently_active = stats.currently_active.saturating_sub(1);
        }
    }

    pub fn unregister(&self, workflow_key: u64, activity_id: u64) {
        self.records
            .lock()
            .unwrap()
            .remove(&(workflow_key, activity_id));
    }

    pub fn active_count(&self) -> usize {
        self.records
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.state == HeartbeatState::Pending || r.state == HeartbeatState::Active)
            .count()
    }

    pub fn total_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }

    pub fn stats(&self) -> HeartbeatStats {
        self.stats.lock().unwrap().clone()
    }

    pub fn time_since_last_heartbeat(
        &self,
        workflow_key: u64,
        activity_id: u64,
    ) -> Option<Duration> {
        let records = self.records.lock().unwrap();
        records
            .get(&(workflow_key, activity_id))
            .map(|rec| rec.last_heartbeat.elapsed())
    }

    pub fn purge_terminal(&self) -> usize {
        let mut records = self.records.lock().unwrap();
        let before = records.len();
        records.retain(|_, rec| {
            rec.state == HeartbeatState::Pending || rec.state == HeartbeatState::Active
        });
        before - records.len()
    }
}

impl Default for HeartbeatTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_heartbeat() {
        let tracker = HeartbeatTracker::new();
        tracker.register(1, 100, 5000, 3);
        assert_eq!(tracker.active_count(), 1);
        assert_eq!(tracker.get_state(1, 100), Some(HeartbeatState::Pending));
        assert!(tracker.record_heartbeat(1, 100, Some(vec![1, 2])));
        assert_eq!(tracker.get_state(1, 100), Some(HeartbeatState::Active));
        assert_eq!(tracker.get_last_details(1, 100), Some(vec![1, 2]));
    }

    #[test]
    fn test_heartbeat_not_registered() {
        let tracker = HeartbeatTracker::new();
        assert!(!tracker.record_heartbeat(1, 999, None));
    }

    #[test]
    fn test_detail_history() {
        let tracker = HeartbeatTracker::new();
        tracker.register(1, 100, 5000, 3);
        tracker.record_heartbeat(1, 100, Some(vec![1]));
        tracker.record_heartbeat(1, 100, Some(vec![2]));
        tracker.record_heartbeat(1, 100, Some(vec![3]));
        let history = tracker.get_detail_history(1, 100);
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].payload, Some(vec![1]));
        assert_eq!(history[2].payload, Some(vec![3]));
    }

    #[test]
    fn test_lifecycle_complete() {
        let tracker = HeartbeatTracker::new();
        tracker.register(1, 100, 5000, 3);
        tracker.record_heartbeat(1, 100, None);
        tracker.complete(1, 100);
        assert_eq!(tracker.get_state(1, 100), Some(HeartbeatState::Completed));
        assert_eq!(tracker.active_count(), 0);
        assert!(!tracker.record_heartbeat(1, 100, None));
    }

    #[test]
    fn test_lifecycle_fail() {
        let tracker = HeartbeatTracker::new();
        tracker.register(1, 100, 5000, 3);
        tracker.fail(1, 100);
        assert_eq!(tracker.get_state(1, 100), Some(HeartbeatState::Failed));
    }

    #[test]
    fn test_lifecycle_cancel() {
        let tracker = HeartbeatTracker::new();
        tracker.register(1, 100, 5000, 3);
        tracker.cancel(1, 100);
        assert_eq!(tracker.get_state(1, 100), Some(HeartbeatState::Cancelled));
    }

    #[test]
    fn test_unregister() {
        let tracker = HeartbeatTracker::new();
        tracker.register(1, 100, 5000, 3);
        tracker.unregister(1, 100);
        assert_eq!(tracker.active_count(), 0);
        assert_eq!(tracker.get_state(1, 100), None);
    }

    #[test]
    fn test_stats() {
        let tracker = HeartbeatTracker::new();
        tracker.register(1, 100, 5000, 3);
        tracker.register(1, 200, 5000, 3);
        tracker.record_heartbeat(1, 100, None);
        tracker.complete(1, 100);
        tracker.fail(1, 200);
        let stats = tracker.stats();
        assert_eq!(stats.total_registered, 2);
        assert_eq!(stats.total_heartbeats, 1);
        assert_eq!(stats.total_completed, 1);
        assert_eq!(stats.total_failed, 1);
        assert_eq!(stats.currently_active, 0);
    }

    #[test]
    fn test_purge_terminal() {
        let tracker = HeartbeatTracker::new();
        tracker.register(1, 100, 5000, 3);
        tracker.register(1, 200, 5000, 3);
        tracker.register(1, 300, 5000, 3);
        tracker.complete(1, 100);
        tracker.fail(1, 200);
        let purged = tracker.purge_terminal();
        assert_eq!(purged, 2);
        assert_eq!(tracker.total_count(), 1);
    }

    #[test]
    fn test_time_since_last_heartbeat() {
        let tracker = HeartbeatTracker::new();
        tracker.register(1, 100, 5000, 3);
        let elapsed = tracker.time_since_last_heartbeat(1, 100);
        assert!(elapsed.is_some());
        assert!(elapsed.unwrap() < Duration::from_secs(1));
    }
}
