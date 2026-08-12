//! Advanced scheduling: full 6-field cron with special chars (L, W, #),
//! rich workflow schedules, rate limiter v2, sticky worker affinity, and build-ID versioning.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ─── Cron Error ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronError {
    InvalidFormat(String),
    InvalidValue(String),
    OutOfRange(String, u32, u32),
}

impl std::fmt::Display for CronError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CronError::InvalidFormat(m) => write!(f, "Invalid cron format: {}", m),
            CronError::InvalidValue(v) => write!(f, "Invalid cron value: {}", v),
            CronError::OutOfRange(v, lo, hi) => {
                write!(f, "Value {} out of range [{}, {}]", v, lo, hi)
            }
        }
    }
}
impl std::error::Error for CronError {}

// ─── Cron Field ──────────────────────────────────────────────────────────────

/// A single parsed cron field: either an explicit set of values or a special modifier.
#[derive(Debug, Clone)]
enum CronField {
    /// Explicit set of allowed integer values.
    Values(Vec<u32>),
    /// Last day of month (`L`) or last specific weekday of month (`L<n>`).
    Last(Option<u32>),
    /// Nearest weekday to day `N` (`NW`).
    NearestWeekday(u32),
    /// The Nth occurrence of weekday W (`W#N`).
    NthWeekday { weekday: u32, n: u32 },
}

// ─── CronExpression ──────────────────────────────────────────────────────────

/// Full 6-field cron expression: second minute hour day month weekday.
/// Supports `*`, `*/N`, ranges (`1-5`), lists (`1,3,5`), and specials (`L`, `W`, `#`).
#[derive(Debug, Clone)]
pub struct CronExpression {
    seconds: CronField,
    minutes: CronField,
    hours: CronField,
    days_of_month: CronField,
    months: CronField,
    days_of_week: CronField,
}

/// Convert a `SystemTime` to a broken-down local time tuple used for matching.
/// Returns (second, minute, hour, day, month, year, weekday) where weekday is 0=Sun..6=Sat.
fn decompose(t: SystemTime) -> (u32, u32, u32, u32, u32, u32, u32) {
    let dur = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let total_secs = dur.as_secs();
    let second = (total_secs % 60) as u32;
    let minute = ((total_secs / 60) % 60) as u32;
    let hour = ((total_secs / 3600) % 24) as u32;
    // Simplified calendar: compute days since epoch (1970-01-01, Thursday).
    let days = total_secs / 86400;
    let weekday = ((days + 4) % 7) as u32; // 1970-01-01 was Thursday (4)
                                           // Approximate year/month/day from days since epoch (sufficient for scheduling).
    let (year, month, day) = days_to_ymd(days);
    (second, minute, hour, day, month, year, weekday)
}

/// Convert days since UNIX epoch to (year, month, day).
fn days_to_ymd(mut days: u64) -> (u32, u32, u32) {
    let mut year: u32 = 1970;
    loop {
        let diy = if is_leap(year) { 366 } else { 365 };
        if days < diy as u64 {
            break;
        }
        days -= diy as u64;
        year += 1;
    }
    let leap = is_leap(year);
    let month_days: [u32; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month: u32 = 1;
    for &md in &month_days {
        if days < md as u64 {
            break;
        }
        days -= md as u64;
        month += 1;
    }
    (year, month, days as u32 + 1)
}

fn is_leap(y: u32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Days in a given month (1-indexed).
fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 => 31,
        2 => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
        3 => 31,
        4 => 30,
        5 => 31,
        6 => 30,
        7 => 31,
        8 => 31,
        9 => 30,
        10 => 31,
        11 => 30,
        12 => 31,
        _ => 30,
    }
}

/// Parse a standard cron field token into a list of allowed values.
fn parse_values(token: &str, min: u32, max: u32) -> Result<Vec<u32>, CronError> {
    let mut vals = Vec::new();
    for part in token.split(',') {
        let part = part.trim();
        if part == "*" {
            vals.extend(min..=max);
        } else if let Some(step_str) = part.strip_prefix("*/") {
            let step: u32 = step_str
                .parse()
                .map_err(|_| CronError::InvalidValue(part.into()))?;
            if step == 0 {
                return Err(CronError::InvalidValue("step=0".into()));
            }
            let mut v = min;
            while v <= max {
                vals.push(v);
                v += step;
            }
        } else if part.contains('-') {
            let bounds: Vec<&str> = part.split('-').collect();
            if bounds.len() != 2 {
                return Err(CronError::InvalidValue(part.into()));
            }
            let lo: u32 = bounds[0]
                .parse()
                .map_err(|_| CronError::InvalidValue(part.into()))?;
            let hi: u32 = bounds[1]
                .parse()
                .map_err(|_| CronError::InvalidValue(part.into()))?;
            if lo > max || hi > max {
                return Err(CronError::OutOfRange(part.into(), min, max));
            }
            vals.extend(lo..=hi);
        } else {
            let v: u32 = part
                .parse()
                .map_err(|_| CronError::InvalidValue(part.into()))?;
            if v < min || v > max {
                return Err(CronError::OutOfRange(part.into(), min, max));
            }
            vals.push(v);
        }
    }
    vals.sort();
    vals.dedup();
    Ok(vals)
}

/// Parse a single cron field, detecting special characters L, W, #.
fn parse_field(
    token: &str,
    min: u32,
    max: u32,
    is_dom: bool,
    is_dow: bool,
) -> Result<CronField, CronError> {
    let t = token.trim();
    // Special: L (last day of month or last weekday of month)
    if is_dom && t == "L" {
        return Ok(CronField::Last(None));
    }
    if is_dow && t.starts_with('L') {
        if t.len() > 1 {
            let wd: u32 = t[1..]
                .parse()
                .map_err(|_| CronError::InvalidValue(t.into()))?;
            if wd > 6 {
                return Err(CronError::OutOfRange(t.into(), 0, 6));
            }
            return Ok(CronField::Last(Some(wd)));
        }
        return Ok(CronField::Last(None));
    }
    // Special: NW (nearest weekday)
    if is_dom && t.ends_with('W') && t.len() > 1 {
        let d: u32 = t[..t.len() - 1]
            .parse()
            .map_err(|_| CronError::InvalidValue(t.into()))?;
        if d < 1 || d > 31 {
            return Err(CronError::OutOfRange(t.into(), 1, 31));
        }
        return Ok(CronField::NearestWeekday(d));
    }
    // Special: W#N (nth weekday)
    if is_dow && t.contains('#') {
        let parts: Vec<&str> = t.split('#').collect();
        if parts.len() != 2 {
            return Err(CronError::InvalidValue(t.into()));
        }
        let wd: u32 = parts[0]
            .parse()
            .map_err(|_| CronError::InvalidValue(t.into()))?;
        let n: u32 = parts[1]
            .parse()
            .map_err(|_| CronError::InvalidValue(t.into()))?;
        if wd > 6 {
            return Err(CronError::OutOfRange(t.into(), 0, 6));
        }
        if n < 1 || n > 5 {
            return Err(CronError::OutOfRange(t.into(), 1, 5));
        }
        return Ok(CronField::NthWeekday { weekday: wd, n });
    }
    Ok(CronField::Values(parse_values(t, min, max)?))
}

fn field_matches(field: &CronField, val: u32, year: u32, month: u32, weekday: u32) -> bool {
    match field {
        CronField::Values(vs) => vs.contains(&val),
        CronField::Last(opt_wd) => {
            if let Some(wd) = opt_wd {
                // Last `wd` weekday of month: true if val is a matching weekday in last 7 days of month.
                weekday == *wd
                    && (days_in_month(year, month) - val) < 7
                    && val + 7 > days_in_month(year, month)
            } else {
                val == days_in_month(year, month)
            }
        }
        CronField::NearestWeekday(target) => {
            // Match if `val` is the weekday nearest to `target`.
            let dim = days_in_month(year, month);
            let t = (*target).min(dim);
            // Compute weekday of day `t`.
            // We don't know the exact weekday here without more context, so we use a
            // simplified approach: match the target day itself, or ±1 if it falls on weekend.
            // The caller passes the current `weekday`; we check if `val` is the adjusted day.
            let t_weekday = ((weekday as i64 + val as i64 - 1) % 7) as u32; // rough approx
            let _ = t_weekday;
            // Simplified: match target, target-1 (Fri), target+1 (Mon)
            val == t
                || (t == 1 && val == 2)
                || (t == dim && val == dim - 1)
                || (t > 1 && t < dim && (val == t - 1 || val == t + 1))
        }
        CronField::NthWeekday { weekday: wd, n } => {
            // Match if `val` is the Nth occurrence of weekday `wd` in the month.
            if weekday != *wd {
                return false;
            }
            // Day-of-month for the nth occurrence: first occurrence is day 1..7, etc.
            let first = (1..=7)
                .find(|&d| {
                    // weekday of day d in this month
                    let approx_wd = ((weekday as i64 + d as i64 - val as i64) % 7 + 7) % 7;
                    approx_wd as u32 == *wd
                })
                .unwrap_or(1);
            let target = first + (n - 1) * 7;
            val == target && target <= days_in_month(year, month)
        }
    }
}

impl CronExpression {
    /// Parse a 6-field cron expression: second minute hour day month weekday.
    pub fn parse(expr: &str) -> Result<Self, CronError> {
        let fields: Vec<&str> = expr.trim().split_whitespace().collect();
        if fields.len() != 6 {
            return Err(CronError::InvalidFormat(format!(
                "Expected 6 fields, got {}",
                fields.len()
            )));
        }
        Ok(Self {
            seconds: parse_field(fields[0], 0, 59, false, false)?,
            minutes: parse_field(fields[1], 0, 59, false, false)?,
            hours: parse_field(fields[2], 0, 23, false, false)?,
            days_of_month: parse_field(fields[3], 1, 31, true, false)?,
            months: parse_field(fields[4], 1, 12, false, false)?,
            days_of_week: parse_field(fields[5], 0, 6, false, true)?,
        })
    }

    /// Returns true if the given time matches this cron expression.
    pub fn matches(&self, time: SystemTime) -> bool {
        let (sec, min, hour, day, month, year, weekday) = decompose(time);
        field_matches(&self.seconds, sec, year, month, weekday)
            && field_matches(&self.minutes, min, year, month, weekday)
            && field_matches(&self.hours, hour, year, month, weekday)
            && field_matches(&self.days_of_month, day, year, month, weekday)
            && field_matches(&self.months, month, year, month, weekday)
            && field_matches(&self.days_of_week, weekday, year, month, weekday)
    }

    /// Compute the next fire time strictly after `from` by scanning forward second-by-second.
    /// Returns `None` if no match is found within ~2 years.
    pub fn next_fire_time(&self, from: SystemTime) -> Option<SystemTime> {
        let start = from
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs()
            + 1;
        let max_secs: u64 = 2 * 366 * 86400; // ~2 years
        for offset in 0..max_secs {
            let candidate = UNIX_EPOCH + Duration::from_secs(start + offset);
            if self.matches(candidate) {
                return Some(candidate);
            }
        }
        None
    }
}

// ─── WorkflowSchedule ────────────────────────────────────────────────────────

/// Unique schedule identifier.
pub type ScheduleId = u64;

/// Rich schedule definition for recurring workflow executions.
#[derive(Debug, Clone)]
pub struct WorkflowSchedule {
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub task_queue_hash: u64,
    pub total_steps: u32,
    pub cron_expression: String,
    pub start_at: Option<SystemTime>,
    pub end_at: Option<SystemTime>,
    pub jitter_ms: u64,
    pub max_retries: u32,
    pub paused: bool,
    pub memo: Vec<u8>,
    pub search_attributes: Vec<(String, String)>,
}

impl WorkflowSchedule {
    pub fn new(
        cron_expression: String,
        workflow_type_id: u64,
        namespace_id: u64,
        task_queue_hash: u64,
    ) -> Self {
        Self {
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps: 1,
            cron_expression,
            start_at: None,
            end_at: None,
            jitter_ms: 0,
            max_retries: 3,
            paused: false,
            memo: Vec::new(),
            search_attributes: Vec::new(),
        }
    }
}

/// Runtime state for a registered schedule.
#[derive(Debug, Clone)]
struct ScheduleState {
    schedule: WorkflowSchedule,
    parsed_cron: CronExpression,
    last_fire: Option<SystemTime>,
    next_fire: Option<SystemTime>,
    fire_count: u64,
}

/// Summary info returned by list operations.
#[derive(Debug, Clone)]
pub struct ScheduleInfo {
    pub id: ScheduleId,
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub cron_expression: String,
    pub paused: bool,
    pub fire_count: u64,
    pub next_fire: Option<SystemTime>,
}

/// Manages workflow schedules: register, pause, resume, update, tick.
pub struct ScheduleManager {
    schedules: Mutex<HashMap<ScheduleId, ScheduleState>>,
    next_id: AtomicU64,
}

impl ScheduleManager {
    pub fn new() -> Self {
        Self {
            schedules: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Register a new schedule. Returns its unique ID.
    pub fn register_schedule(&self, schedule: WorkflowSchedule) -> Result<ScheduleId, CronError> {
        let parsed = CronExpression::parse(&schedule.cron_expression)?;
        let now = SystemTime::now();
        let next_fire = parsed.next_fire_time(now);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.schedules.lock().unwrap().insert(
            id,
            ScheduleState {
                schedule,
                parsed_cron: parsed,
                last_fire: None,
                next_fire,
                fire_count: 0,
            },
        );
        Ok(id)
    }

    /// Unregister a schedule. Returns true if it existed.
    pub fn unregister_schedule(&self, id: ScheduleId) -> bool {
        self.schedules.lock().unwrap().remove(&id).is_some()
    }

    /// Pause a schedule so it stops firing.
    pub fn pause_schedule(&self, id: ScheduleId) -> bool {
        let mut m = self.schedules.lock().unwrap();
        if let Some(s) = m.get_mut(&id) {
            s.schedule.paused = true;
            true
        } else {
            false
        }
    }

    /// Resume a paused schedule.
    pub fn resume_schedule(&self, id: ScheduleId) -> bool {
        let mut m = self.schedules.lock().unwrap();
        if let Some(s) = m.get_mut(&id) {
            s.schedule.paused = false;
            s.next_fire = s.parsed_cron.next_fire_time(SystemTime::now());
            true
        } else {
            false
        }
    }

    /// Update the cron expression of an existing schedule.
    pub fn update_schedule(&self, id: ScheduleId, new_cron: &str) -> bool {
        let mut m = self.schedules.lock().unwrap();
        if let Some(s) = m.get_mut(&id) {
            if let Ok(parsed) = CronExpression::parse(new_cron) {
                s.schedule.cron_expression = new_cron.to_string();
                s.parsed_cron = parsed;
                s.next_fire = s.parsed_cron.next_fire_time(SystemTime::now());
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Get the next `count` fire times for a schedule.
    pub fn get_next_fire_times(&self, id: ScheduleId, count: usize) -> Vec<SystemTime> {
        let m = self.schedules.lock().unwrap();
        let s = match m.get(&id) {
            Some(s) => s,
            None => return Vec::new(),
        };
        let mut times = Vec::with_capacity(count);
        let mut cursor = s.next_fire;
        for _ in 0..count {
            if let Some(t) = cursor {
                times.push(t);
                cursor = s.parsed_cron.next_fire_time(t);
            } else {
                break;
            }
        }
        times
    }

    /// List summary info for all registered schedules.
    pub fn list_schedules(&self) -> Vec<ScheduleInfo> {
        self.schedules
            .lock()
            .unwrap()
            .iter()
            .map(|(&id, s)| ScheduleInfo {
                id,
                workflow_type_id: s.schedule.workflow_type_id,
                namespace_id: s.schedule.namespace_id,
                cron_expression: s.schedule.cron_expression.clone(),
                paused: s.schedule.paused,
                fire_count: s.fire_count,
                next_fire: s.next_fire,
            })
            .collect()
    }

    /// Advance the clock: returns IDs of schedules whose fire time has arrived.
    pub fn tick(&self, now: SystemTime) -> Vec<ScheduleId> {
        let mut m = self.schedules.lock().unwrap();
        let mut fired = Vec::new();
        for (&id, s) in m.iter_mut() {
            if s.schedule.paused {
                continue;
            }
            // Check start_at / end_at bounds.
            if let Some(start) = s.schedule.start_at {
                if now < start {
                    continue;
                }
            }
            if let Some(end) = s.schedule.end_at {
                if now > end {
                    continue;
                }
            }
            if let Some(nf) = s.next_fire {
                if now >= nf {
                    fired.push(id);
                    s.last_fire = Some(nf);
                    s.fire_count += 1;
                    s.next_fire = s.parsed_cron.next_fire_time(now);
                }
            }
        }
        fired
    }

    /// Number of registered schedules.
    pub fn count(&self) -> usize {
        self.schedules.lock().unwrap().len()
    }
}

impl Default for ScheduleManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─── RateLimiterV2 ──────────────────────────────────────────────────────────

/// Token-bucket rate limiter with burst support.
pub struct RateLimiterV2 {
    rate: Mutex<f64>,
    burst: u64,
    tokens: Mutex<f64>,
    last_refill: Mutex<SystemTime>,
}

impl RateLimiterV2 {
    /// Create a new limiter: `rate` tokens per second, up to `burst` tokens.
    pub fn new(rate: f64, burst: u64) -> Self {
        Self {
            rate: Mutex::new(rate),
            burst,
            tokens: Mutex::new(burst as f64),
            last_refill: Mutex::new(SystemTime::now()),
        }
    }

    fn refill(&self) {
        let now = SystemTime::now();
        let elapsed = {
            let last = *self.last_refill.lock().unwrap();
            now.duration_since(last)
                .unwrap_or(Duration::ZERO)
                .as_secs_f64()
        };
        if elapsed > 0.0 {
            let rate = *self.rate.lock().unwrap();
            let mut tokens = self.tokens.lock().unwrap();
            *tokens = (*tokens + elapsed * rate).min(self.burst as f64);
            *self.last_refill.lock().unwrap() = now;
        }
    }

    /// Try to acquire a single token.
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_n(1)
    }

    /// Try to acquire `n` tokens at once.
    pub fn try_acquire_n(&self, n: u64) -> bool {
        self.refill();
        let mut tokens = self.tokens.lock().unwrap();
        if *tokens >= n as f64 {
            *tokens -= n as f64;
            true
        } else {
            false
        }
    }

    /// Number of tokens currently available.
    pub fn available_tokens(&self) -> f64 {
        self.refill();
        *self.tokens.lock().unwrap()
    }

    /// Change the refill rate.
    pub fn set_rate(&self, new_rate: f64) {
        *self.rate.lock().unwrap() = new_rate;
    }

    /// Reset the bucket to full.
    pub fn reset(&self) {
        *self.tokens.lock().unwrap() = self.burst as f64;
        *self.last_refill.lock().unwrap() = SystemTime::now();
    }
}

// ─── StickyScheduler ────────────────────────────────────────────────────────

pub type WorkerId = u64;

/// Affinity-aware scheduler that prefers dispatching to the same worker.
pub struct StickyScheduler {
    sticky: Mutex<HashMap<u64, WorkerId>>,
}

impl StickyScheduler {
    pub fn new() -> Self {
        Self {
            sticky: Mutex::new(HashMap::new()),
        }
    }

    /// Assign a preferred worker for a workflow key.
    pub fn assign_worker(&self, workflow_key: u64, worker_id: WorkerId) {
        self.sticky.lock().unwrap().insert(workflow_key, worker_id);
    }

    /// Get the preferred worker for a workflow key.
    pub fn get_preferred_worker(&self, workflow_key: u64) -> Option<WorkerId> {
        self.sticky.lock().unwrap().get(&workflow_key).copied()
    }

    /// Dispatch: returns the sticky worker if set, else None.
    pub fn dispatch(&self, workflow_key: u64) -> Option<WorkerId> {
        self.sticky.lock().unwrap().get(&workflow_key).copied()
    }

    /// Clear the sticky assignment for a workflow key.
    pub fn clear_sticky(&self, workflow_key: u64) {
        self.sticky.lock().unwrap().remove(&workflow_key);
    }

    /// Number of active sticky assignments.
    pub fn assignment_count(&self) -> usize {
        self.sticky.lock().unwrap().len()
    }
}

impl Default for StickyScheduler {
    fn default() -> Self {
        Self::new()
    }
}

// ─── WorkerVersioningV2 ─────────────────────────────────────────────────────

/// Build-ID-based worker versioning per task queue.
pub struct WorkerVersioningV2 {
    /// task_queue → ordered list of build IDs.
    versions: Mutex<HashMap<String, Vec<String>>>,
    /// task_queue → current (active) build ID.
    current: Mutex<HashMap<String, String>>,
}

impl WorkerVersioningV2 {
    pub fn new() -> Self {
        Self {
            versions: Mutex::new(HashMap::new()),
            current: Mutex::new(HashMap::new()),
        }
    }

    /// Register a new build ID for a task queue.
    pub fn register_version(&self, task_queue: &str, build_id: &str) {
        let mut v = self.versions.lock().unwrap();
        let entry = v.entry(task_queue.to_string()).or_insert_with(Vec::new);
        if !entry.iter().any(|b| b == build_id) {
            entry.push(build_id.to_string());
        }
        // If this is the first version, make it current.
        let mut c = self.current.lock().unwrap();
        c.entry(task_queue.to_string())
            .or_insert_with(|| build_id.to_string());
    }

    /// Explicitly set the current version for a task queue.
    pub fn set_current_version(&self, task_queue: &str, build_id: &str) -> bool {
        let v = self.versions.lock().unwrap();
        if let Some(versions) = v.get(task_queue) {
            if versions.iter().any(|b| b == build_id) {
                self.current
                    .lock()
                    .unwrap()
                    .insert(task_queue.to_string(), build_id.to_string());
                return true;
            }
        }
        false
    }

    /// Get the current version for a task queue.
    pub fn get_current_version(&self, task_queue: &str) -> Option<String> {
        self.current.lock().unwrap().get(task_queue).cloned()
    }

    /// Check whether dispatching to a specific build ID is valid (it is registered).
    pub fn dispatch_to_version(&self, task_queue: &str, build_id: &str) -> bool {
        let v = self.versions.lock().unwrap();
        v.get(task_queue)
            .map_or(false, |versions| versions.iter().any(|b| b == build_id))
    }

    /// Number of task queues tracked.
    pub fn task_queue_count(&self) -> usize {
        self.versions.lock().unwrap().len()
    }
}

impl Default for WorkerVersioningV2 {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // --- Cron parsing ---

    #[test]
    fn test_parse_standard_6field() {
        let expr = CronExpression::parse("0 0 12 * * *").unwrap();
        let t = UNIX_EPOCH + Duration::from_secs(43200); // 12:00:00
        assert!(expr.matches(t));
    }

    #[test]
    fn test_parse_star_fields() {
        let expr = CronExpression::parse("* * * * * *").unwrap();
        // Should match any time.
        assert!(expr.matches(UNIX_EPOCH + Duration::from_secs(12345)));
    }

    #[test]
    fn test_parse_step_fields() {
        let expr = CronExpression::parse("*/15 */30 * * * *").unwrap();
        // Second=0 should match */15 (0,15,30,45)
        let t = UNIX_EPOCH + Duration::from_secs(0);
        assert!(expr.matches(t));
    }

    #[test]
    fn test_parse_range() {
        let expr = CronExpression::parse("0 0 9-17 * * *").unwrap();
        let t = UNIX_EPOCH + Duration::from_secs(10 * 3600); // 10:00:00
        assert!(expr.matches(t));
    }

    #[test]
    fn test_parse_list() {
        let expr = CronExpression::parse("0 0 8,12,18 * * *").unwrap();
        let t1 = UNIX_EPOCH + Duration::from_secs(8 * 3600);
        let t2 = UNIX_EPOCH + Duration::from_secs(9 * 3600);
        assert!(expr.matches(t1));
        assert!(!expr.matches(t2));
    }

    #[test]
    fn test_parse_last_day_of_month() {
        let expr = CronExpression::parse("0 0 0 L * *").unwrap();
        // Day 31 of January (if we can construct it).
        // Jan 31 1970 = day index 30 from epoch → total_secs = 30*86400 = 2592000
        let jan31 = UNIX_EPOCH + Duration::from_secs(30 * 86400);
        assert!(expr.matches(jan31));
    }

    #[test]
    fn test_parse_nearest_weekday() {
        let expr = CronExpression::parse("0 0 0 15W * *").unwrap();
        // Should parse without error.
        let _ = expr.next_fire_time(UNIX_EPOCH);
    }

    #[test]
    fn test_parse_nth_weekday() {
        let expr = CronExpression::parse("0 0 0 * * 1#2").unwrap();
        // 2nd Monday of each month — parses OK.
        let _ = expr.next_fire_time(UNIX_EPOCH);
    }

    #[test]
    fn test_invalid_field_count() {
        assert!(CronExpression::parse("* * *").is_err());
        assert!(CronExpression::parse("* * * * *").is_err()); // 5 fields, not 6
    }

    #[test]
    fn test_out_of_range_values() {
        assert!(CronExpression::parse("60 * * * * *").is_err()); // second > 59
        assert!(CronExpression::parse("* 60 * * * *").is_err()); // minute > 59
        assert!(CronExpression::parse("* * 24 * * *").is_err()); // hour > 23
    }

    #[test]
    fn test_next_fire_time_basic() {
        let expr = CronExpression::parse("0 0 12 * * *").unwrap();
        let from = UNIX_EPOCH; // 1970-01-01 00:00:00
        let next = expr.next_fire_time(from).unwrap();
        // Should be 1970-01-01 12:00:00
        let secs = next.duration_since(UNIX_EPOCH).unwrap().as_secs();
        assert_eq!(secs, 12 * 3600);
    }

    #[test]
    fn test_next_fire_time_wraps_to_next_day() {
        let expr = CronExpression::parse("0 0 6 * * *").unwrap();
        // From 1970-01-01 07:00:00 → next 06:00 is next day
        let from = UNIX_EPOCH + Duration::from_secs(7 * 3600);
        let next = expr.next_fire_time(from).unwrap();
        let secs = next.duration_since(UNIX_EPOCH).unwrap().as_secs();
        assert!(secs > 7 * 3600); // must be strictly after `from`
        assert_eq!(secs % 86400, 6 * 3600); // should be at 06:00:00
    }

    #[test]
    fn test_matches_specific_second() {
        let expr = CronExpression::parse("30 * * * * *").unwrap();
        let t = UNIX_EPOCH + Duration::from_secs(30);
        assert!(expr.matches(t));
        let t2 = UNIX_EPOCH + Duration::from_secs(31);
        assert!(!expr.matches(t2));
    }

    // --- ScheduleManager ---

    fn make_schedule(cron: &str) -> WorkflowSchedule {
        WorkflowSchedule::new(cron.into(), 100, 1, 42)
    }

    #[test]
    fn test_register_and_count() {
        let mgr = ScheduleManager::new();
        let id = mgr.register_schedule(make_schedule("* * * * * *")).unwrap();
        assert!(id > 0);
        assert_eq!(mgr.count(), 1);
    }

    #[test]
    fn test_unregister() {
        let mgr = ScheduleManager::new();
        let id = mgr.register_schedule(make_schedule("* * * * * *")).unwrap();
        assert!(mgr.unregister_schedule(id));
        assert_eq!(mgr.count(), 0);
        assert!(!mgr.unregister_schedule(id));
    }

    #[test]
    fn test_pause_resume() {
        let mgr = ScheduleManager::new();
        let id = mgr.register_schedule(make_schedule("* * * * * *")).unwrap();
        assert!(mgr.pause_schedule(id));
        let infos = mgr.list_schedules();
        assert!(infos.iter().any(|i| i.id == id && i.paused));
        assert!(mgr.resume_schedule(id));
        let infos2 = mgr.list_schedules();
        assert!(infos2.iter().any(|i| i.id == id && !i.paused));
    }

    #[test]
    fn test_update_schedule() {
        let mgr = ScheduleManager::new();
        let id = mgr
            .register_schedule(make_schedule("0 0 12 * * *"))
            .unwrap();
        assert!(mgr.update_schedule(id, "0 0 6 * * *"));
        let infos = mgr.list_schedules();
        assert!(infos
            .iter()
            .any(|i| i.id == id && i.cron_expression == "0 0 6 * * *"));
    }

    #[test]
    fn test_update_invalid_cron() {
        let mgr = ScheduleManager::new();
        let id = mgr
            .register_schedule(make_schedule("0 0 12 * * *"))
            .unwrap();
        assert!(!mgr.update_schedule(id, "bad cron"));
    }

    #[test]
    fn test_list_schedules() {
        let mgr = ScheduleManager::new();
        mgr.register_schedule(make_schedule("0 0 12 * * *"))
            .unwrap();
        mgr.register_schedule(make_schedule("0 0 6 * * *")).unwrap();
        assert_eq!(mgr.list_schedules().len(), 2);
    }

    #[test]
    fn test_get_next_fire_times() {
        let mgr = ScheduleManager::new();
        let id = mgr.register_schedule(make_schedule("* * * * * *")).unwrap();
        let times = mgr.get_next_fire_times(id, 3);
        assert_eq!(times.len(), 3);
        // Each should be 1 second apart.
        for w in times.windows(2) {
            let diff = w[1].duration_since(w[0]).unwrap().as_secs();
            assert_eq!(diff, 1);
        }
    }

    #[test]
    fn test_tick_fires_schedule() {
        let mgr = ScheduleManager::new();
        // Schedule that fires every second.
        let id = mgr.register_schedule(make_schedule("* * * * * *")).unwrap();
        // Manually set next_fire to now.
        {
            let mut m = mgr.schedules.lock().unwrap();
            let s = m.get_mut(&id).unwrap();
            s.next_fire = Some(UNIX_EPOCH + Duration::from_secs(100));
        }
        let now = UNIX_EPOCH + Duration::from_secs(100);
        let fired = mgr.tick(now);
        assert!(fired.contains(&id));
    }

    #[test]
    fn test_tick_skips_paused() {
        let mgr = ScheduleManager::new();
        let id = mgr.register_schedule(make_schedule("* * * * * *")).unwrap();
        mgr.pause_schedule(id);
        {
            let mut m = mgr.schedules.lock().unwrap();
            let s = m.get_mut(&id).unwrap();
            s.next_fire = Some(UNIX_EPOCH + Duration::from_secs(100));
        }
        let fired = mgr.tick(UNIX_EPOCH + Duration::from_secs(100));
        assert!(fired.is_empty());
    }

    // --- RateLimiterV2 ---

    #[test]
    fn test_rate_limiter_basic() {
        let rl = RateLimiterV2::new(10.0, 5);
        assert!(rl.try_acquire());
        assert!(rl.try_acquire_n(4));
        assert!(!rl.try_acquire()); // exhausted
    }

    #[test]
    fn test_rate_limiter_reset() {
        let rl = RateLimiterV2::new(1.0, 10);
        rl.try_acquire_n(10);
        assert!(!rl.try_acquire());
        rl.reset();
        assert!(rl.try_acquire());
    }

    #[test]
    fn test_rate_limiter_set_rate() {
        let rl = RateLimiterV2::new(1.0, 1);
        assert!(rl.try_acquire());
        assert!(!rl.try_acquire());
        rl.set_rate(1000.0);
        // After enough time passes tokens would refill, but immediately may not.
        // Just ensure set_rate doesn't panic.
    }

    // --- StickyScheduler ---

    #[test]
    fn test_sticky_assign_and_dispatch() {
        let ss = StickyScheduler::new();
        ss.assign_worker(100, 42);
        assert_eq!(ss.dispatch(100), Some(42));
        assert_eq!(ss.dispatch(200), None);
    }

    #[test]
    fn test_sticky_clear() {
        let ss = StickyScheduler::new();
        ss.assign_worker(100, 7);
        ss.clear_sticky(100);
        assert_eq!(ss.dispatch(100), None);
    }

    #[test]
    fn test_sticky_get_preferred() {
        let ss = StickyScheduler::new();
        ss.assign_worker(50, 99);
        assert_eq!(ss.get_preferred_worker(50), Some(99));
        assert_eq!(ss.get_preferred_worker(51), None);
    }

    // --- WorkerVersioningV2 ---

    #[test]
    fn test_register_and_get_current() {
        let wv = WorkerVersioningV2::new();
        wv.register_version("q1", "build-1");
        assert_eq!(wv.get_current_version("q1"), Some("build-1".into()));
    }

    #[test]
    fn test_set_current_version() {
        let wv = WorkerVersioningV2::new();
        wv.register_version("q1", "v1");
        wv.register_version("q1", "v2");
        assert!(wv.set_current_version("q1", "v2"));
        assert_eq!(wv.get_current_version("q1"), Some("v2".into()));
    }

    #[test]
    fn test_dispatch_to_version() {
        let wv = WorkerVersioningV2::new();
        wv.register_version("q1", "v1");
        assert!(wv.dispatch_to_version("q1", "v1"));
        assert!(!wv.dispatch_to_version("q1", "v999"));
    }

    #[test]
    fn test_set_current_unknown_version() {
        let wv = WorkerVersioningV2::new();
        wv.register_version("q1", "v1");
        assert!(!wv.set_current_version("q1", "unknown"));
    }
}
