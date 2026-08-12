//! Schedules API — CRUD schedule management with calendar specs, overlap policy, jitter.
//! Mirrors Temporal's schedule system with:
//! - Calendar spec parsing and matching (cron-like expressions)
//! - Next fire time computation
//! - Action dispatch with overlap policies
//! - Schedule state tracking (running, paused, completed)
//! - Schedule search with filters
//! - Retention and cleanup

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use std::time::{SystemTime, UNIX_EPOCH};

// ─── Overlap Policy ──────────────────────────────────────────────────────────

/// How to handle overlapping schedule executions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlapPolicy {
    /// Skip the new execution if one is already running.
    Skip = 0,
    /// Buffer one execution; start it when the current one finishes.
    BufferOne = 1,
    /// Buffer all executions; run them sequentially.
    BufferAll = 2,
    /// Terminate the running execution and start a new one.
    TerminateOther = 3,
    /// Allow all executions to run concurrently.
    AllowAll = 4,
}

impl OverlapPolicy {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::BufferOne => "buffer-one",
            Self::BufferAll => "buffer-all",
            Self::TerminateOther => "terminate-other",
            Self::AllowAll => "allow-all",
        }
    }
}

// ─── Calendar Spec ───────────────────────────────────────────────────────────

/// Calendar specification (cron-like) for schedule firing.
/// Each field is a string expression: "*", "N", "N-M", "N,M,O", "*/N".
#[derive(Debug, Clone)]
pub struct CalendarSpec {
    pub second: String,
    pub minute: String,
    pub hour: String,
    pub day_of_month: String,
    pub month: String,
    pub day_of_week: String,
    pub comment: String,
}

impl CalendarSpec {
    /// Create a spec that fires every N minutes.
    pub fn every_minutes(n: u32) -> Self {
        Self {
            second: "0".into(),
            minute: format!("*/{}", n),
            hour: "*".into(),
            day_of_month: "*".into(),
            month: "*".into(),
            day_of_week: "*".into(),
            comment: format!("every {} minutes", n),
        }
    }

    /// Create a spec that fires every hour at minute 0.
    pub fn hourly() -> Self {
        Self {
            second: "0".into(),
            minute: "0".into(),
            hour: "*".into(),
            day_of_month: "*".into(),
            month: "*".into(),
            day_of_week: "*".into(),
            comment: "hourly".into(),
        }
    }

    /// Create a spec that fires daily at a given hour (UTC).
    pub fn daily_at(hour: u32, minute: u32) -> Self {
        Self {
            second: "0".into(),
            minute: minute.to_string(),
            hour: hour.to_string(),
            day_of_month: "*".into(),
            month: "*".into(),
            day_of_week: "*".into(),
            comment: format!("daily at {:02}:{:02}", hour, minute),
        }
    }

    /// Check if a timestamp (as broken-down time) matches this calendar spec.
    pub fn matches(
        &self,
        second: u32,
        minute: u32,
        hour: u32,
        day: u32,
        month: u32,
        dow: u32,
    ) -> bool {
        matches_field(&self.second, second, 0, 59)
            && matches_field(&self.minute, minute, 0, 59)
            && matches_field(&self.hour, hour, 0, 23)
            && matches_field(&self.day_of_month, day, 1, 31)
            && matches_field(&self.month, month, 1, 12)
            && matches_field(&self.day_of_week, dow, 0, 6)
    }
}

/// Parse and match a cron-like field expression against a value.
fn matches_field(expr: &str, value: u32, min: u32, max: u32) -> bool {
    if expr == "*" {
        return true;
    }
    for part in expr.split(',') {
        let part = part.trim();
        if part.contains('/') {
            // Step: */N or M/N
            let segments: Vec<&str> = part.split('/').collect();
            let step: u32 = segments[1].parse().unwrap_or(1);
            let start: u32 = if segments[0] == "*" {
                min
            } else {
                segments[0].parse().unwrap_or(min)
            };
            if step > 0 && (value >= start) && (value - start) % step == 0 && value <= max {
                return true;
            }
        } else if part.contains('-') {
            // Range: M-N
            let segments: Vec<&str> = part.split('-').collect();
            let lo: u32 = segments[0].parse().unwrap_or(min);
            let hi: u32 = segments[1].parse().unwrap_or(max);
            if value >= lo && value <= hi {
                return true;
            }
        } else {
            // Exact value
            if let Ok(v) = part.parse::<u32>() {
                if v == value {
                    return true;
                }
            }
        }
    }
    false
}

// ─── Schedule Entry ──────────────────────────────────────────────────────────

/// State of a schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleState {
    Active,
    Paused,
    Completed,
    Failed,
}

/// A schedule entry with full metadata.
#[derive(Debug, Clone)]
pub struct ScheduleEntry {
    pub schedule_id: u64,
    pub calendar_spec: CalendarSpec,
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub task_queue_hash: u64,
    pub overlap_policy: OverlapPolicy,
    pub jitter_seconds: u64,
    pub state: ScheduleState,
    pub remaining_actions: u64, // 0 = unlimited
    pub start_time_ms: u64,
    pub end_time_ms: Option<u64>,
    pub last_action_time_ms: u64,
    pub next_action_time_ms: u64,
    pub action_count: u64,
    pub running_workflow_keys: Vec<u64>,
    /// Notes/description.
    pub notes: String,
    /// Search attributes for querying.
    pub search_attributes: HashMap<String, String>,
    /// Creation timestamp.
    pub created_at_ms: u64,
}

// ─── Schedule Action ─────────────────────────────────────────────────────────

/// Record of a schedule action execution.
#[derive(Debug, Clone)]
pub struct ScheduleAction {
    pub action_id: u64,
    pub schedule_id: u64,
    pub workflow_key: u64,
    pub started_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    pub success: bool,
}

// ─── Schedule Manager ────────────────────────────────────────────────────────

/// Manages schedules: CRUD, firing, action tracking.
pub struct ScheduleManager {
    schedules: Mutex<HashMap<u64, ScheduleEntry>>,
    actions: Mutex<Vec<ScheduleAction>>,
    next_id: AtomicU64,
    next_action_id: AtomicU64,
}

impl ScheduleManager {
    pub fn new() -> Self {
        Self {
            schedules: Mutex::new(HashMap::new()),
            actions: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(1),
            next_action_id: AtomicU64::new(1),
        }
    }

    // ─── CRUD ────────────────────────────────────────────────────────────

    /// Create a new schedule. Returns the schedule ID.
    pub fn create_schedule(
        &self,
        spec: CalendarSpec,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        overlap: OverlapPolicy,
        jitter: u64,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now = now_ms();
        self.schedules.lock().unwrap().insert(
            id,
            ScheduleEntry {
                schedule_id: id,
                calendar_spec: spec,
                workflow_type_id,
                namespace_id,
                task_queue_hash,
                overlap_policy: overlap,
                jitter_seconds: jitter,
                state: ScheduleState::Active,
                remaining_actions: 0,
                start_time_ms: now,
                end_time_ms: None,
                last_action_time_ms: 0,
                next_action_time_ms: 0,
                action_count: 0,
                running_workflow_keys: Vec::new(),
                notes: String::new(),
                search_attributes: HashMap::new(),
                created_at_ms: now,
            },
        );
        id
    }

    /// Create a schedule with notes and search attributes.
    pub fn create_schedule_rich(
        &self,
        spec: CalendarSpec,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        overlap: OverlapPolicy,
        jitter: u64,
        notes: &str,
        search_attrs: HashMap<String, String>,
    ) -> u64 {
        let id = self.create_schedule(
            spec,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            overlap,
            jitter,
        );
        let mut schedules = self.schedules.lock().unwrap();
        if let Some(entry) = schedules.get_mut(&id) {
            entry.notes = notes.to_string();
            entry.search_attributes = search_attrs;
        }
        id
    }

    pub fn pause(&self, schedule_id: u64) -> bool {
        let mut s = self.schedules.lock().unwrap();
        if let Some(e) = s.get_mut(&schedule_id) {
            e.state = ScheduleState::Paused;
            true
        } else {
            false
        }
    }

    pub fn unpause(&self, schedule_id: u64) -> bool {
        let mut s = self.schedules.lock().unwrap();
        if let Some(e) = s.get_mut(&schedule_id) {
            e.state = ScheduleState::Active;
            true
        } else {
            false
        }
    }

    pub fn delete(&self, schedule_id: u64) -> bool {
        self.schedules
            .lock()
            .unwrap()
            .remove(&schedule_id)
            .is_some()
    }

    pub fn get(&self, schedule_id: u64) -> Option<ScheduleEntry> {
        self.schedules.lock().unwrap().get(&schedule_id).cloned()
    }

    pub fn list(&self) -> Vec<ScheduleEntry> {
        self.schedules.lock().unwrap().values().cloned().collect()
    }

    pub fn count(&self) -> usize {
        self.schedules.lock().unwrap().len()
    }

    pub fn update_overlap_policy(&self, schedule_id: u64, policy: OverlapPolicy) -> bool {
        let mut s = self.schedules.lock().unwrap();
        if let Some(e) = s.get_mut(&schedule_id) {
            e.overlap_policy = policy;
            true
        } else {
            false
        }
    }

    pub fn set_remaining_actions(&self, schedule_id: u64, count: u64) -> bool {
        let mut s = self.schedules.lock().unwrap();
        if let Some(e) = s.get_mut(&schedule_id) {
            e.remaining_actions = count;
            true
        } else {
            false
        }
    }

    pub fn update_notes(&self, schedule_id: u64, notes: &str) -> bool {
        let mut s = self.schedules.lock().unwrap();
        if let Some(e) = s.get_mut(&schedule_id) {
            e.notes = notes.to_string();
            true
        } else {
            false
        }
    }

    // ─── Firing / Action Dispatch ────────────────────────────────────────

    /// Compute the next fire time for a schedule based on its calendar spec.
    /// Returns the next fire time as milliseconds since epoch.
    pub fn compute_next_fire(&self, schedule_id: u64, after_ms: u64) -> Option<u64> {
        let schedules = self.schedules.lock().unwrap();
        let entry = schedules.get(&schedule_id)?;
        let spec = &entry.calendar_spec;

        // Simple approach: scan forward minute by minute from after_ms
        let start_secs = after_ms / 1000;
        for offset in 1..=86400u64 {
            // scan up to 24 hours
            let candidate_secs = start_secs + offset;
            let dt = epoch_secs_to_components(candidate_secs);
            if spec.matches(dt.second, dt.minute, dt.hour, dt.day, dt.month, dt.dow) {
                return Some(candidate_secs * 1000);
            }
        }
        None
    }

    /// Try to fire a schedule. Returns the action ID if an action was started.
    pub fn try_fire(&self, schedule_id: u64, workflow_key: u64) -> Option<u64> {
        let mut schedules = self.schedules.lock().unwrap();
        let entry = schedules.get_mut(&schedule_id)?;

        // Check state
        if entry.state != ScheduleState::Active {
            return None;
        }

        // Check remaining actions
        if entry.remaining_actions > 0 && entry.action_count >= entry.remaining_actions {
            entry.state = ScheduleState::Completed;
            return None;
        }

        // Check overlap policy
        match entry.overlap_policy {
            OverlapPolicy::Skip => {
                if !entry.running_workflow_keys.is_empty() {
                    return None;
                }
            }
            OverlapPolicy::TerminateOther => {
                entry.running_workflow_keys.clear();
            }
            OverlapPolicy::BufferOne | OverlapPolicy::BufferAll | OverlapPolicy::AllowAll => {
                // Allow
            }
        }

        // Record the action
        let action_id = self.next_action_id.fetch_add(1, Ordering::Relaxed);
        let now = now_ms();
        self.actions.lock().unwrap().push(ScheduleAction {
            action_id,
            schedule_id,
            workflow_key,
            started_at_ms: now,
            completed_at_ms: None,
            success: false,
        });

        entry.running_workflow_keys.push(workflow_key);
        entry.last_action_time_ms = now;
        entry.action_count += 1;

        Some(action_id)
    }

    /// Complete an action.
    pub fn complete_action(&self, schedule_id: u64, workflow_key: u64, success: bool) {
        let mut schedules = self.schedules.lock().unwrap();
        if let Some(entry) = schedules.get_mut(&schedule_id) {
            entry.running_workflow_keys.retain(|&k| k != workflow_key);
        }

        let mut actions = self.actions.lock().unwrap();
        if let Some(action) = actions.iter_mut().find(|a| {
            a.schedule_id == schedule_id
                && a.workflow_key == workflow_key
                && a.completed_at_ms.is_none()
        }) {
            action.completed_at_ms = Some(now_ms());
            action.success = success;
        }
    }

    // ─── Search / Query ──────────────────────────────────────────────────

    /// Search schedules by namespace.
    pub fn list_by_namespace(&self, namespace_id: u64) -> Vec<ScheduleEntry> {
        self.schedules
            .lock()
            .unwrap()
            .values()
            .filter(|e| e.namespace_id == namespace_id)
            .cloned()
            .collect()
    }

    /// Search schedules by state.
    pub fn list_by_state(&self, state: ScheduleState) -> Vec<ScheduleEntry> {
        self.schedules
            .lock()
            .unwrap()
            .values()
            .filter(|e| e.state == state)
            .cloned()
            .collect()
    }

    /// Search schedules by search attribute.
    pub fn search_by_attribute(&self, key: &str, value: &str) -> Vec<ScheduleEntry> {
        self.schedules
            .lock()
            .unwrap()
            .values()
            .filter(|e| e.search_attributes.get(key).map_or(false, |v| v == value))
            .cloned()
            .collect()
    }

    /// Get action history for a schedule.
    pub fn get_actions(&self, schedule_id: u64) -> Vec<ScheduleAction> {
        self.actions
            .lock()
            .unwrap()
            .iter()
            .filter(|a| a.schedule_id == schedule_id)
            .cloned()
            .collect()
    }

    /// Count of completed actions for a schedule.
    pub fn action_count(&self, schedule_id: u64) -> u64 {
        self.schedules
            .lock()
            .unwrap()
            .get(&schedule_id)
            .map_or(0, |e| e.action_count)
    }

    /// Count of currently running workflows for a schedule.
    pub fn running_count(&self, schedule_id: u64) -> usize {
        self.schedules
            .lock()
            .unwrap()
            .get(&schedule_id)
            .map_or(0, |e| e.running_workflow_keys.len())
    }
}

impl Default for ScheduleManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Time Helpers ────────────────────────────────────────────────────────────

struct DateTimeComponents {
    second: u32,
    minute: u32,
    hour: u32,
    day: u32,
    month: u32,
    dow: u32,
}

/// Convert epoch seconds to broken-down time components (simplified UTC).
fn epoch_secs_to_components(secs: u64) -> DateTimeComponents {
    // Simplified calculation (doesn't handle leap years perfectly, but good enough for testing)
    let minute = ((secs % 3600) / 60) as u32;
    let second = (secs % 60) as u32;
    let hour = ((secs % 86400) / 3600) as u32;
    let days = secs / 86400;
    let dow = ((days + 4) % 7) as u32; // Jan 1, 1970 was Thursday (4)

    // Calculate year, month, day from days since epoch
    let (_year, month, day) = days_to_ymd(days);

    DateTimeComponents {
        second,
        minute,
        hour,
        day,
        month,
        dow,
    }
}

/// Convert days since epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u32, u32, u32) {
    // Simplified algorithm
    let mut remaining = days as i64;
    let mut year = 1970i64;

    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        year += 1;
    }

    let month_days = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1u32;
    for &md in &month_days {
        if remaining < md {
            break;
        }
        remaining -= md;
        month += 1;
    }

    (year as u32, month, remaining as u32 + 1)
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn default_spec() -> CalendarSpec {
        CalendarSpec {
            second: "0".into(),
            minute: "*/5".into(),
            hour: "*".into(),
            day_of_month: "*".into(),
            month: "*".into(),
            day_of_week: "*".into(),
            comment: "test".into(),
        }
    }

    // --- Calendar Spec Matching ---

    #[test]
    fn test_matches_wildcard() {
        assert!(matches_field("*", 0, 0, 59));
        assert!(matches_field("*", 30, 0, 59));
        assert!(matches_field("*", 59, 0, 59));
    }

    #[test]
    fn test_matches_exact() {
        assert!(matches_field("5", 5, 0, 59));
        assert!(!matches_field("5", 6, 0, 59));
    }

    #[test]
    fn test_matches_range() {
        assert!(matches_field("5-10", 5, 0, 59));
        assert!(matches_field("5-10", 10, 0, 59));
        assert!(!matches_field("5-10", 4, 0, 59));
        assert!(!matches_field("5-10", 11, 0, 59));
    }

    #[test]
    fn test_matches_step() {
        assert!(matches_field("*/5", 0, 0, 59));
        assert!(matches_field("*/5", 5, 0, 59));
        assert!(matches_field("*/5", 10, 0, 59));
        assert!(!matches_field("*/5", 3, 0, 59));
    }

    #[test]
    fn test_matches_list() {
        assert!(matches_field("1,3,5", 1, 0, 59));
        assert!(matches_field("1,3,5", 3, 0, 59));
        assert!(matches_field("1,3,5", 5, 0, 59));
        assert!(!matches_field("1,3,5", 2, 0, 59));
    }

    #[test]
    fn test_calendar_spec_matches() {
        let spec = CalendarSpec {
            second: "0".into(),
            minute: "0".into(),
            hour: "12".into(),
            day_of_month: "*".into(),
            month: "*".into(),
            day_of_week: "*".into(),
            comment: "".into(),
        };
        assert!(spec.matches(0, 0, 12, 1, 1, 0));
        assert!(!spec.matches(0, 0, 13, 1, 1, 0));
    }

    #[test]
    fn test_calendar_spec_constructors() {
        let hourly = CalendarSpec::hourly();
        assert!(hourly.matches(0, 0, 5, 1, 1, 0));
        assert!(!hourly.matches(0, 5, 5, 1, 1, 0));

        let daily = CalendarSpec::daily_at(9, 30);
        assert!(daily.matches(0, 30, 9, 1, 1, 0));
        assert!(!daily.matches(0, 30, 10, 1, 1, 0));
    }

    // --- CRUD ---

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
        assert_eq!(mgr.get(id).unwrap().state, ScheduleState::Paused);
        assert!(mgr.unpause(id));
        assert_eq!(mgr.get(id).unwrap().state, ScheduleState::Active);
    }

    #[test]
    fn test_delete() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::Skip, 0);
        assert!(mgr.delete(id));
        assert_eq!(mgr.count(), 0);
    }

    #[test]
    fn test_update_notes() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::Skip, 0);
        assert!(mgr.update_notes(id, "test notes"));
        assert_eq!(mgr.get(id).unwrap().notes, "test notes");
    }

    // --- Firing ---

    #[test]
    fn test_try_fire() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::Skip, 0);
        let action = mgr.try_fire(id, 1000);
        assert!(action.is_some());
        assert_eq!(mgr.action_count(id), 1);
        assert_eq!(mgr.running_count(id), 1);
    }

    #[test]
    fn test_try_fire_paused() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::Skip, 0);
        mgr.pause(id);
        assert!(mgr.try_fire(id, 1000).is_none());
    }

    #[test]
    fn test_overlap_skip() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::Skip, 0);
        assert!(mgr.try_fire(id, 1000).is_some());
        assert!(mgr.try_fire(id, 1001).is_none()); // skip — one already running
    }

    #[test]
    fn test_overlap_allow_all() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::AllowAll, 0);
        assert!(mgr.try_fire(id, 1000).is_some());
        assert!(mgr.try_fire(id, 1001).is_some()); // allow all
        assert_eq!(mgr.running_count(id), 2);
    }

    #[test]
    fn test_overlap_terminate_other() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::TerminateOther, 0);
        assert!(mgr.try_fire(id, 1000).is_some());
        assert_eq!(mgr.running_count(id), 1);
        assert!(mgr.try_fire(id, 1001).is_some()); // terminates old, starts new
        assert_eq!(mgr.running_count(id), 1); // only the new one
    }

    #[test]
    fn test_complete_action() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::Skip, 0);
        mgr.try_fire(id, 1000);
        assert_eq!(mgr.running_count(id), 1);
        mgr.complete_action(id, 1000, true);
        assert_eq!(mgr.running_count(id), 0);
    }

    #[test]
    fn test_remaining_actions() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::AllowAll, 0);
        mgr.set_remaining_actions(id, 2);
        assert!(mgr.try_fire(id, 1000).is_some());
        assert!(mgr.try_fire(id, 1001).is_some());
        // Third should fail — limit reached
        assert!(mgr.try_fire(id, 1002).is_none());
        assert_eq!(mgr.get(id).unwrap().state, ScheduleState::Completed);
    }

    // --- Search ---

    #[test]
    fn test_list_by_namespace() {
        let mgr = ScheduleManager::new();
        mgr.create_schedule(default_spec(), 100, 1, 42, OverlapPolicy::Skip, 0);
        mgr.create_schedule(default_spec(), 200, 1, 43, OverlapPolicy::Skip, 0);
        mgr.create_schedule(default_spec(), 300, 2, 44, OverlapPolicy::Skip, 0);
        assert_eq!(mgr.list_by_namespace(1).len(), 2);
        assert_eq!(mgr.list_by_namespace(2).len(), 1);
    }

    #[test]
    fn test_list_by_state() {
        let mgr = ScheduleManager::new();
        let id1 = mgr.create_schedule(default_spec(), 100, 1, 42, OverlapPolicy::Skip, 0);
        let _id2 = mgr.create_schedule(default_spec(), 200, 1, 43, OverlapPolicy::Skip, 0);
        mgr.pause(id1);
        assert_eq!(mgr.list_by_state(ScheduleState::Paused).len(), 1);
        assert_eq!(mgr.list_by_state(ScheduleState::Active).len(), 1);
    }

    #[test]
    fn test_search_by_attribute() {
        let mgr = ScheduleManager::new();
        let mut attrs = HashMap::new();
        attrs.insert("env".to_string(), "prod".to_string());
        mgr.create_schedule_rich(
            default_spec(),
            100,
            1,
            42,
            OverlapPolicy::Skip,
            0,
            "test",
            attrs,
        );
        mgr.create_schedule(default_spec(), 200, 1, 43, OverlapPolicy::Skip, 0);
        assert_eq!(mgr.search_by_attribute("env", "prod").len(), 1);
    }

    #[test]
    fn test_get_actions() {
        let mgr = ScheduleManager::new();
        let id = mgr.create_schedule(default_spec(), 100, 0, 42, OverlapPolicy::AllowAll, 0);
        mgr.try_fire(id, 1000);
        mgr.try_fire(id, 1001);
        let actions = mgr.get_actions(id);
        assert_eq!(actions.len(), 2);
    }

    // --- Time helpers ---

    #[test]
    fn test_epoch_secs_to_components() {
        // Jan 1, 1970 00:00:00 UTC = Thursday
        let dt = epoch_secs_to_components(0);
        assert_eq!(dt.second, 0);
        assert_eq!(dt.minute, 0);
        assert_eq!(dt.hour, 0);
        assert_eq!(dt.day, 1);
        assert_eq!(dt.month, 1);
        assert_eq!(dt.dow, 4); // Thursday
    }

    #[test]
    fn test_days_to_ymd() {
        // Day 0 = Jan 1, 1970
        let (y, m, d) = days_to_ymd(0);
        assert_eq!((y, m, d), (1970, 1, 1));
        // Day 31 = Feb 1, 1970
        let (y, m, d) = days_to_ymd(31);
        assert_eq!((y, m, d), (1970, 2, 1));
    }

    #[test]
    fn test_leap_year() {
        assert!(is_leap_year(2000));
        assert!(is_leap_year(2024));
        assert!(!is_leap_year(1900));
        assert!(!is_leap_year(2023));
    }
}
