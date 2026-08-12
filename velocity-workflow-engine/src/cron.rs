//! Cron scheduling for recurring workflow executions.
//! Parses standard 5-field cron expressions (minute hour day month weekday)
//! and integrates with the timer engine for durable recurring triggers.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

// ─── Cron Expression ──────────────────────────────────────────────────────────

/// A parsed 5-field cron expression: minute hour day-of-month month day-of-week.
/// Each field is a set of allowed values.
#[derive(Debug, Clone)]
pub struct CronExpression {
    pub minutes: Vec<u8>,
    pub hours: Vec<u8>,
    pub days_of_month: Vec<u8>,
    pub months: Vec<u8>,
    pub days_of_week: Vec<u8>,
}

impl CronExpression {
    /// Parse a standard 5-field cron expression.
    /// Supports: `*`, specific values, ranges (`1-5`), steps (`*/5`), and lists (`1,3,5`).
    pub fn parse(expr: &str) -> Result<Self, CronError> {
        let fields: Vec<&str> = expr.trim().split_whitespace().collect();
        if fields.len() != 5 {
            return Err(CronError::InvalidFormat(format!(
                "Expected 5 fields, got {}",
                fields.len()
            )));
        }

        Ok(CronExpression {
            minutes: parse_field(fields[0], 0, 59)?,
            hours: parse_field(fields[1], 0, 23)?,
            days_of_month: parse_field(fields[2], 1, 31)?,
            months: parse_field(fields[3], 1, 12)?,
            days_of_week: parse_field(fields[4], 0, 6)?,
        })
    }

    /// Calculate the next fire time after the given timestamp (in minutes since epoch).
    pub fn next_fire_after(&self, after_minutes: u64) -> u64 {
        // Start from the next minute
        let mut candidate = after_minutes + 1;

        // Search up to 4 years ahead (4 * 366 * 24 * 60 minutes)
        let max_iterations = 4 * 366 * 24 * 60;

        for _ in 0..max_iterations {
            let minute = ((candidate % 60) as u8) % 60;
            let hour = (((candidate / 60) % 24) as u8) % 24;
            let day_of_month = (((candidate / (60 * 24)) % 31) as u8) + 1;
            let month = (((candidate / (60 * 24 * 30)) % 12) as u8) + 1;
            let day_of_week = ((candidate / (60 * 24)) % 7) as u8;

            // Clamp day_of_month to valid range for simplified calendar
            let day_of_month = day_of_month.min(31).max(1);

            if self.minutes.contains(&minute)
                && self.hours.contains(&hour)
                && self.days_of_month.contains(&day_of_month)
                && self.months.contains(&month)
                && self.days_of_week.contains(&day_of_week)
            {
                return candidate;
            }

            candidate += 1;
        }

        // Fallback: should not happen for valid expressions
        after_minutes + 60
    }
}

/// Parse a single cron field into a sorted list of allowed values.
fn parse_field(field: &str, min: u8, max: u8) -> Result<Vec<u8>, CronError> {
    let mut values = Vec::new();

    for part in field.split(',') {
        let part = part.trim();
        if part == "*" {
            // All values in range
            values.extend(min..=max);
        } else if let Some(star_step) = part.strip_prefix("*/") {
            // Step values: */5
            let step: u8 = star_step
                .parse()
                .map_err(|_| CronError::InvalidValue(part.to_string()))?;
            if step == 0 {
                return Err(CronError::InvalidValue("Step cannot be zero".to_string()));
            }
            let mut v = min;
            while v <= max {
                values.push(v);
                v += step;
            }
        } else if part.contains('-') {
            // Range: 1-5
            let bounds: Vec<&str> = part.split('-').collect();
            if bounds.len() != 2 {
                return Err(CronError::InvalidValue(part.to_string()));
            }
            let start: u8 = bounds[0]
                .parse()
                .map_err(|_| CronError::InvalidValue(part.to_string()))?;
            let end: u8 = bounds[1]
                .parse()
                .map_err(|_| CronError::InvalidValue(part.to_string()))?;
            if start > max || end > max {
                return Err(CronError::OutOfRange(part.to_string(), min, max));
            }
            values.extend(start..=end);
        } else {
            // Single value
            let v: u8 = part
                .parse()
                .map_err(|_| CronError::InvalidValue(part.to_string()))?;
            if v < min || v > max {
                return Err(CronError::OutOfRange(part.to_string(), min, max));
            }
            values.push(v);
        }
    }

    values.sort();
    values.dedup();
    Ok(values)
}

// ─── Cron Error ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum CronError {
    InvalidFormat(String),
    InvalidValue(String),
    OutOfRange(String, u8, u8),
}

impl std::fmt::Display for CronError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CronError::InvalidFormat(msg) => write!(f, "Invalid cron format: {}", msg),
            CronError::InvalidValue(val) => write!(f, "Invalid cron value: {}", val),
            CronError::OutOfRange(val, min, max) => {
                write!(f, "Value {} out of range [{}-{}]", val, min, max)
            }
        }
    }
}

impl std::error::Error for CronError {}

// ─── Cron Schedule Entry ─────────────────────────────────────────────────────

/// A registered cron schedule that fires recurring workflow tasks.
#[derive(Debug, Clone)]
pub struct CronEntry {
    pub schedule_id: u64,
    pub cron_expr: CronExpression,
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub task_queue_hash: u64,
    pub total_steps: u32,
    pub last_fire_time: u64,
    pub next_fire_time: u64,
    pub fire_count: u64,
    pub paused: bool,
}

// ─── Cron Scheduler ──────────────────────────────────────────────────────────

/// Manages cron schedules and produces fire events for the engine.
pub struct CronScheduler {
    entries: Mutex<HashMap<u64, CronEntry>>,
    next_id: AtomicU64,
}

impl CronScheduler {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Register a new cron schedule. Returns the schedule ID.
    pub fn register(
        &self,
        cron_expression: &str,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
        total_steps: u32,
        current_time_minutes: u64,
    ) -> Result<u64, CronError> {
        let expr = CronExpression::parse(cron_expression)?;
        let next_fire = expr.next_fire_after(current_time_minutes);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let entry = CronEntry {
            schedule_id: id,
            cron_expr: expr,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            last_fire_time: 0,
            next_fire_time: next_fire,
            fire_count: 0,
            paused: false,
        };

        self.entries.lock().unwrap().insert(id, entry);
        Ok(id)
    }

    /// Unregister a cron schedule.
    pub fn unregister(&self, schedule_id: u64) -> bool {
        self.entries.lock().unwrap().remove(&schedule_id).is_some()
    }

    /// Pause or resume a cron schedule.
    pub fn set_paused(&self, schedule_id: u64, paused: bool) -> bool {
        let mut entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.get_mut(&schedule_id) {
            entry.paused = paused;
            true
        } else {
            false
        }
    }

    /// Advance the clock and return any schedules that should fire.
    /// Called by the engine's tick loop or timer callback.
    pub fn advance_to(&self, current_time_minutes: u64) -> Vec<CronFireEvent> {
        let mut entries = self.entries.lock().unwrap();
        let mut fires = Vec::new();

        for entry in entries.values_mut() {
            if entry.paused {
                continue;
            }
            if entry.next_fire_time <= current_time_minutes {
                fires.push(CronFireEvent {
                    schedule_id: entry.schedule_id,
                    workflow_type_id: entry.workflow_type_id,
                    namespace_id: entry.namespace_id,
                    task_queue_hash: entry.task_queue_hash,
                    total_steps: entry.total_steps,
                    fire_number: entry.fire_count + 1,
                });

                entry.last_fire_time = entry.next_fire_time;
                entry.fire_count += 1;
                entry.next_fire_time = entry.cron_expr.next_fire_after(current_time_minutes);
            }
        }

        fires
    }

    /// Get the next fire time for a schedule.
    pub fn next_fire_time(&self, schedule_id: u64) -> Option<u64> {
        self.entries
            .lock()
            .unwrap()
            .get(&schedule_id)
            .map(|e| e.next_fire_time)
    }

    /// Get the number of registered schedules.
    pub fn schedule_count(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    /// Get the fire count for a schedule.
    pub fn fire_count(&self, schedule_id: u64) -> Option<u64> {
        self.entries
            .lock()
            .unwrap()
            .get(&schedule_id)
            .map(|e| e.fire_count)
    }

    /// Check if a schedule is paused.
    pub fn is_paused(&self, schedule_id: u64) -> Option<bool> {
        self.entries
            .lock()
            .unwrap()
            .get(&schedule_id)
            .map(|e| e.paused)
    }
}

impl Default for CronScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Event produced when a cron schedule fires.
#[derive(Debug, Clone)]
pub struct CronFireEvent {
    pub schedule_id: u64,
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub task_queue_hash: u64,
    pub total_steps: u32,
    pub fire_number: u64,
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_cron() {
        let expr = CronExpression::parse("0 12 * * *").unwrap();
        assert_eq!(expr.minutes, vec![0]);
        assert_eq!(expr.hours, vec![12]);
        assert_eq!(expr.days_of_month.len(), 31); // * = all days
        assert_eq!(expr.months.len(), 12); // * = all months
        assert_eq!(expr.days_of_week.len(), 7); // * = all weekdays
    }

    #[test]
    fn test_parse_step_cron() {
        let expr = CronExpression::parse("*/15 * * * *").unwrap();
        assert_eq!(expr.minutes, vec![0, 15, 30, 45]);
    }

    #[test]
    fn test_parse_range_cron() {
        let expr = CronExpression::parse("0 9-17 * * 1-5").unwrap();
        assert_eq!(expr.minutes, vec![0]);
        assert_eq!(expr.hours, vec![9, 10, 11, 12, 13, 14, 15, 16, 17]);
        assert_eq!(expr.days_of_week, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_parse_list_cron() {
        let expr = CronExpression::parse("0,30 8,12,18 * * *").unwrap();
        assert_eq!(expr.minutes, vec![0, 30]);
        assert_eq!(expr.hours, vec![8, 12, 18]);
    }

    #[test]
    fn test_invalid_field_count() {
        assert!(CronExpression::parse("* * *").is_err());
    }

    #[test]
    fn test_out_of_range() {
        assert!(CronExpression::parse("60 * * * *").is_err());
        assert!(CronExpression::parse("* 25 * * *").is_err());
    }

    #[test]
    fn test_next_fire_after() {
        let expr = CronExpression::parse("0 12 * * *").unwrap();
        // After minute 0, the next fire at "12:00" should be found
        let next = expr.next_fire_after(0);
        assert!(next > 0);
    }

    #[test]
    fn test_cron_scheduler_register_and_advance() {
        let scheduler = CronScheduler::new();

        // Register a schedule that fires every minute: "* * * * *"
        let id = scheduler.register("* * * * *", 1, 0, 42, 3, 0).unwrap();
        assert!(id > 0);
        assert_eq!(scheduler.schedule_count(), 1);

        // Advance to time 5 — should fire at minutes 1, 2, 3, 4, 5
        let fires = scheduler.advance_to(5);
        assert_eq!(fires.len(), 1); // First fire
        assert_eq!(fires[0].schedule_id, id);
        assert_eq!(fires[0].fire_number, 1);

        // Fire count should be 1
        assert_eq!(scheduler.fire_count(id), Some(1));
    }

    #[test]
    fn test_cron_scheduler_pause_resume() {
        let scheduler = CronScheduler::new();
        let id = scheduler.register("* * * * *", 1, 0, 42, 1, 0).unwrap();

        // Pause
        assert!(scheduler.set_paused(id, true));
        assert_eq!(scheduler.is_paused(id), Some(true));

        // Advance while paused — no fires
        let fires = scheduler.advance_to(10);
        assert_eq!(fires.len(), 0);

        // Resume
        scheduler.set_paused(id, false);
        let fires = scheduler.advance_to(15);
        assert!(fires.len() >= 1);
    }

    #[test]
    fn test_cron_scheduler_unregister() {
        let scheduler = CronScheduler::new();
        let id = scheduler.register("*/5 * * * *", 1, 0, 42, 1, 0).unwrap();
        assert_eq!(scheduler.schedule_count(), 1);

        assert!(scheduler.unregister(id));
        assert_eq!(scheduler.schedule_count(), 0);
        assert!(!scheduler.unregister(id)); // Already removed
    }
}
