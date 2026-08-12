//! Activity heartbeat protocol — records heartbeats and detects timeouts.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Instant, Duration};

#[derive(Debug, Clone)]
pub struct HeartbeatRecord {
    pub workflow_key: u64,
    pub activity_id: u64,
    pub last_heartbeat: Instant,
    pub timeout_ms: u64,
    pub details: Option<Vec<u8>>,
    pub miss_count: u32,
}

pub struct HeartbeatTracker {
    records: Mutex<HashMap<(u64, u64), HeartbeatRecord>>,
}

impl HeartbeatTracker {
    pub fn new() -> Self { Self { records: Mutex::new(HashMap::new()) } }

    pub fn register(&self, workflow_key: u64, activity_id: u64, timeout_ms: u64) {
        self.records.lock().unwrap().insert((workflow_key, activity_id), HeartbeatRecord {
            workflow_key, activity_id, last_heartbeat: Instant::now(), timeout_ms, details: None, miss_count: 0,
        });
    }

    pub fn record_heartbeat(&self, workflow_key: u64, activity_id: u64, details: Option<Vec<u8>>) -> bool {
        let mut records = self.records.lock().unwrap();
        if let Some(rec) = records.get_mut(&(workflow_key, activity_id)) {
            rec.last_heartbeat = Instant::now();
            rec.details = details;
            rec.miss_count = 0;
            true
        } else { false }
    }

    pub fn check_timeouts(&self) -> Vec<(u64, u64)> {
        let mut records = self.records.lock().unwrap();
        let mut timed_out = Vec::new();
        for (key, rec) in records.iter_mut() {
            let elapsed = rec.last_heartbeat.elapsed();
            if elapsed > Duration::from_millis(rec.timeout_ms) {
                rec.miss_count += 1;
                if rec.miss_count >= 2 { timed_out.push(*key); }
            }
        }
        timed_out
    }

    pub fn unregister(&self, workflow_key: u64, activity_id: u64) {
        self.records.lock().unwrap().remove(&(workflow_key, activity_id));
    }

    pub fn active_count(&self) -> usize { self.records.lock().unwrap().len() }
}

impl Default for HeartbeatTracker { fn default() -> Self { Self::new() } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_heartbeat_register_and_record() {
        let tracker = HeartbeatTracker::new();
        tracker.register(1, 100, 5000);
        assert_eq!(tracker.active_count(), 1);
        assert!(tracker.record_heartbeat(1, 100, Some(vec![1, 2])));
        assert!(!tracker.record_heartbeat(1, 999, None)); // not registered
    }
    #[test]
    fn test_heartbeat_unregister() {
        let tracker = HeartbeatTracker::new();
        tracker.register(1, 100, 5000);
        tracker.unregister(1, 100);
        assert_eq!(tracker.active_count(), 0);
    }
}
