//! Workflow reset — reset a workflow to a previous point in its history.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ResetPoint {
    pub workflow_key: u64,
    pub reset_to_event_id: u64,
    pub reset_id: u64,
    pub reason: String,
}

pub struct WorkflowResetter {
    reset_points: Mutex<HashMap<u64, Vec<ResetPoint>>>,
    next_reset_id: std::sync::atomic::AtomicU64,
}

impl WorkflowResetter {
    pub fn new() -> Self { Self { reset_points: Mutex::new(HashMap::new()), next_reset_id: std::sync::atomic::AtomicU64::new(1) } }

    pub fn create_reset_point(&self, workflow_key: u64, event_id: u64, reason: &str) -> u64 {
        let reset_id = self.next_reset_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.reset_points.lock().unwrap().entry(workflow_key).or_default().push(ResetPoint {
            workflow_key, reset_to_event_id: event_id, reset_id, reason: reason.to_string(),
        });
        reset_id
    }

    pub fn get_reset_points(&self, workflow_key: u64) -> Vec<ResetPoint> {
        self.reset_points.lock().unwrap().get(&workflow_key).cloned().unwrap_or_default()
    }

    pub fn get_latest_reset(&self, workflow_key: u64) -> Option<ResetPoint> {
        self.reset_points.lock().unwrap().get(&workflow_key)?.last().cloned()
    }

    pub fn reset_count(&self, workflow_key: u64) -> usize {
        self.reset_points.lock().unwrap().get(&workflow_key).map_or(0, |v| v.len())
    }

    pub fn total_resets(&self) -> usize {
        self.reset_points.lock().unwrap().values().map(|v| v.len()).sum()
    }
}
impl Default for WorkflowResetter { fn default() -> Self { Self::new() } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_create_reset_point() {
        let resetter = WorkflowResetter::new();
        let id = resetter.create_reset_point(42, 5, "rollback to step 5");
        assert!(id > 0);
        assert_eq!(resetter.reset_count(42), 1);
        let rp = resetter.get_latest_reset(42).unwrap();
        assert_eq!(rp.reset_to_event_id, 5);
    }
    #[test]
    fn test_multiple_resets() {
        let resetter = WorkflowResetter::new();
        resetter.create_reset_point(1, 3, "first");
        resetter.create_reset_point(1, 7, "second");
        assert_eq!(resetter.reset_count(1), 2);
        assert_eq!(resetter.total_resets(), 2);
    }
}
