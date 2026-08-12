//! Schedules API — CRUD schedule management with calendar specs, overlap policy, jitter.
//! More featureful than cron: supports pause, list, update, overlap policies.

use std::collections::HashMap;
use std::sync::{Mutex, atomic::{AtomicU64, Ordering}};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlapPolicy { Skip = 0, BufferOne = 1, BufferAll = 2, TerminateOther = 3, AllowAll = 4 }

#[derive(Debug, Clone)]
pub struct CalendarSpec {
    pub second: String, pub minute: String, pub hour: String,
    pub day_of_month: String, pub month: String, pub day_of_week: String,
    pub comment: String,
}

#[derive(Debug, Clone)]
pub struct ScheduleEntry {
    pub schedule_id: u64,
    pub calendar_spec: CalendarSpec,
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub task_queue_hash: u64,
    pub overlap_policy: OverlapPolicy,
    pub jitter_seconds: u64,
    pub paused: bool,
    pub remaining_actions: u64, // 0 = unlimited
    pub start_time_ms: u64,
    pub end_time_ms: Option<u64>,
    pub last_action_time_ms: u64,
    pub next_action_time_ms: u64,
    pub action_count: u64,
    pub running_workflow_keys: Vec<u64>,
}

pub struct ScheduleManager {
    schedules: Mutex<HashMap<u64, ScheduleEntry>>,
    next_id: AtomicU64,
}

impl ScheduleManager {
    pub fn new() -> Self { Self { schedules: Mutex::new(HashMap::new()), next_id: AtomicU64::new(1) } }

    pub fn create_schedule(&self, spec: CalendarSpec, workflow_type_id: u64, namespace_id: u64, task_queue_hash: u64, overlap: OverlapPolicy, jitter: u64) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.schedules.lock().unwrap().insert(id, ScheduleEntry {
            schedule_id: id, calendar_spec: spec, workflow_type_id, namespace_id, task_queue_hash,
            overlap_policy: overlap, jitter_seconds: jitter, paused: false, remaining_actions: 0,
            start_time_ms: 0, end_time_ms: None, last_action_time_ms: 0, next_action_time_ms: 0,
            action_count: 0, running_workflow_keys: Vec::new(),
        });
        id
    }

    pub fn pause(&self, schedule_id: u64) -> bool { let mut s = self.schedules.lock().unwrap(); if let Some(e) = s.get_mut(&schedule_id) { e.paused = true; true } else { false } }
    pub fn unpause(&self, schedule_id: u64) -> bool { let mut s = self.schedules.lock().unwrap(); if let Some(e) = s.get_mut(&schedule_id) { e.paused = false; true } else { false } }
    pub fn delete(&self, schedule_id: u64) -> bool { self.schedules.lock().unwrap().remove(&schedule_id).is_some() }

    pub fn get(&self, schedule_id: u64) -> Option<ScheduleEntry> { self.schedules.lock().unwrap().get(&schedule_id).cloned() }
    pub fn list(&self) -> Vec<ScheduleEntry> { self.schedules.lock().unwrap().values().cloned().collect() }
    pub fn count(&self) -> usize { self.schedules.lock().unwrap().len() }

    pub fn update_overlap_policy(&self, schedule_id: u64, policy: OverlapPolicy) -> bool {
        let mut s = self.schedules.lock().unwrap();
        if let Some(e) = s.get_mut(&schedule_id) { e.overlap_policy = policy; true } else { false }
    }

    pub fn set_remaining_actions(&self, schedule_id: u64, count: u64) -> bool {
        let mut s = self.schedules.lock().unwrap();
        if let Some(e) = s.get_mut(&schedule_id) { e.remaining_actions = count; true } else { false }
    }
}
impl Default for ScheduleManager { fn default() -> Self { Self::new() } }

#[cfg(test)]
mod tests {
    use super::*;
    fn default_spec() -> CalendarSpec { CalendarSpec { second: "0".into(), minute: "*/5".into(), hour: "*".into(), day_of_month: "*".into(), month: "*".into(), day_of_week: "*".into(), comment: "test".into() } }

    #[test]
    fn test_create_and_list() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::Skip, 0);
        assert!(id > 0);
        assert_eq!(mgr.count(), 1);
        assert_eq!(mgr.list().len(), 1);
    }
    #[test]
    fn test_pause_unpause() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::Skip, 0);
        assert!(mgr.pause(id));
        assert!(mgr.get(id).unwrap().paused);
        assert!(mgr.unpause(id));
        assert!(!mgr.get(id).unwrap().paused);
    }
    #[test]
    fn test_delete() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::Skip, 0);
        assert!(mgr.delete(id));
        assert_eq!(mgr.count(), 0);
    }
}
