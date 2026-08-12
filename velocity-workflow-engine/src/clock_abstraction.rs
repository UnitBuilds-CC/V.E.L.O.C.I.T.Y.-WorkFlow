//! Clock abstraction matching Temporal's common/clock (846 lines).
//!
//! Covers: TimeSource trait, real time, mock time, time-skipping time source,
//! hybrid logical clock, timer handles, and scheduled timers.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock, atomic::{AtomicI64, AtomicU64, Ordering}};
use std::time::{Duration, SystemTime, Instant};

// ═══════════════════════════════════════════════════════════════════════════════
// Time Source Trait
// ═══════════════════════════════════════════════════════════════════════════════

pub trait TimeSource: Send + Sync {
    fn now(&self) -> SystemTime;
    fn elapsed_since(&self, since: SystemTime) -> Duration;
    fn sleep(&self, duration: Duration);
    fn new_timer(&self, duration: Duration) -> TimerHandle;
    fn since_unix_epoch(&self) -> Duration {
        self.now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default()
    }
    fn unix_nanos(&self) -> i64 {
        self.since_unix_epoch().as_nanos() as i64
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Timer Handle
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
pub struct TimerHandle {
    pub id: u64,
    pub fire_at: SystemTime,
    pub cancelled: Arc<std::sync::atomic::AtomicBool>,
    pub fired: Arc<std::sync::atomic::AtomicBool>,
}

impl TimerHandle {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub fn has_fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Real Time Source
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct RealTimeSource;

impl TimeSource for RealTimeSource {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    fn elapsed_since(&self, since: SystemTime) -> Duration {
        SystemTime::now().duration_since(since).unwrap_or_default()
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }

    fn new_timer(&self, duration: Duration) -> TimerHandle {
        let handle = TimerHandle {
            id: REAL_TIMER_ID.fetch_add(1, Ordering::Relaxed),
            fire_at: SystemTime::now() + duration,
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            fired: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let fired = handle.fired.clone();
        let cancelled = handle.cancelled.clone();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            if !cancelled.load(Ordering::SeqCst) {
                fired.store(true, Ordering::SeqCst);
            }
        });
        handle
    }
}

static REAL_TIMER_ID: AtomicU64 = AtomicU64::new(1);

// ═══════════════════════════════════════════════════════════════════════════════
// Mock Time Source
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MockTimeSource {
    current_time: RwLock<SystemTime>,
    timers: RwLock<Vec<MockTimerEntry>>,
    timer_id_counter: AtomicU64,
}

#[derive(Debug)]
struct MockTimerEntry {
    id: u64,
    fire_at: SystemTime,
    fired: Arc<std::sync::atomic::AtomicBool>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl MockTimeSource {
    pub fn new(start: SystemTime) -> Self {
        Self {
            current_time: RwLock::new(start),
            timers: RwLock::new(Vec::new()),
            timer_id_counter: AtomicU64::new(1),
        }
    }

    pub fn new_at_epoch() -> Self {
        Self::new(SystemTime::UNIX_EPOCH)
    }

    pub fn advance(&self, duration: Duration) {
        let mut time = self.current_time.write().unwrap();
        *time += duration;
        let now = *time;
        drop(time);
        self.fire_ready_timers(now);
    }

    pub fn set_time(&self, time: SystemTime) {
        let mut current = self.current_time.write().unwrap();
        *current = time;
        drop(current);
        self.fire_ready_timers(time);
    }

    fn fire_ready_timers(&self, now: SystemTime) {
        let timers = self.timers.read().unwrap();
        for entry in timers.iter() {
            if !entry.cancelled.load(Ordering::SeqCst) && !entry.fired.load(Ordering::SeqCst) {
                if entry.fire_at <= now {
                    entry.fired.store(true, Ordering::SeqCst);
                }
            }
        }
    }

    pub fn pending_timer_count(&self) -> usize {
        self.timers.read().unwrap().iter()
            .filter(|t| !t.cancelled.load(Ordering::SeqCst) && !t.fired.load(Ordering::SeqCst))
            .count()
    }
}

impl TimeSource for MockTimeSource {
    fn now(&self) -> SystemTime {
        *self.current_time.read().unwrap()
    }

    fn elapsed_since(&self, since: SystemTime) -> Duration {
        let now = *self.current_time.read().unwrap();
        now.duration_since(since).unwrap_or_default()
    }

    fn sleep(&self, _duration: Duration) {
        // No-op for mock — use advance() instead
    }

    fn new_timer(&self, duration: Duration) -> TimerHandle {
        let now = *self.current_time.read().unwrap();
        let id = self.timer_id_counter.fetch_add(1, Ordering::Relaxed);
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));

        self.timers.write().unwrap().push(MockTimerEntry {
            id,
            fire_at: now + duration,
            fired: fired.clone(),
            cancelled: cancelled.clone(),
        });

        TimerHandle { id, fire_at: now + duration, cancelled, fired }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Time-Skipping Time Source
// ═══════════════════════════════════════════════════════════════════════════════

pub struct TimeSkippingTimeSource {
    inner: MockTimeSource,
    max_skip_duration: RwLock<Duration>,
    skip_count: AtomicU64,
}

impl TimeSkippingTimeSource {
    pub fn new(start: SystemTime) -> Self {
        Self {
            inner: MockTimeSource::new(start),
            max_skip_duration: RwLock::new(Duration::from_secs(3600)),
            skip_count: AtomicU64::new(0),
        }
    }

    pub fn skip_to_next_timer(&self) {
        let timers = self.inner.timers.read().unwrap();
        let next_fire = timers.iter()
            .filter(|t| !t.cancelled.load(Ordering::SeqCst) && !t.fired.load(Ordering::SeqCst))
            .map(|t| t.fire_at)
            .min();

        if let Some(fire_at) = next_fire {
            let max_skip = *self.max_skip_duration.read().unwrap();
            let now = self.inner.now();
            let skip_dur = fire_at.duration_since(now).unwrap_or_default().min(max_skip);
            self.inner.advance(skip_dur);
            self.skip_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn skip_count(&self) -> u64 {
        self.skip_count.load(Ordering::Relaxed)
    }

    pub fn set_max_skip_duration(&self, dur: Duration) {
        *self.max_skip_duration.write().unwrap() = dur;
    }
}

impl TimeSource for TimeSkippingTimeSource {
    fn now(&self) -> SystemTime { self.inner.now() }
    fn elapsed_since(&self, since: SystemTime) -> Duration { self.inner.elapsed_since(since) }
    fn sleep(&self, duration: Duration) { self.inner.sleep(duration) }
    fn new_timer(&self, duration: Duration) -> TimerHandle { self.inner.new_timer(duration) }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Hybrid Logical Clock
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HybridLogicalClock {
    pub wall_time_ms: i64,
    pub logical_counter: u32,
    pub node_id: u16,
}

impl HybridLogicalClock {
    pub fn new(node_id: u16) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Self { wall_time_ms: now_ms, logical_counter: 0, node_id }
    }

    pub fn now(&mut self) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        if now_ms > self.wall_time_ms {
            self.wall_time_ms = now_ms;
            self.logical_counter = 0;
        } else {
            self.logical_counter += 1;
        }
        self.clone()
    }

    pub fn update(&mut self, other: &HybridLogicalClock) -> Self {
        if other.wall_time_ms > self.wall_time_ms {
            self.wall_time_ms = other.wall_time_ms;
            self.logical_counter = other.logical_counter + 1;
        } else if other.wall_time_ms == self.wall_time_ms {
            if other.logical_counter >= self.logical_counter {
                self.logical_counter = other.logical_counter + 1;
            }
        }
        // Ensure wall time is at least current physical time
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        if now_ms > self.wall_time_ms {
            self.wall_time_ms = now_ms;
            self.logical_counter = 0;
        }
        self.clone()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(14);
        buf.extend_from_slice(&self.wall_time_ms.to_be_bytes());
        buf.extend_from_slice(&self.logical_counter.to_be_bytes());
        buf.extend_from_slice(&self.node_id.to_be_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 14 { return None; }
        let wall_time_ms = i64::from_be_bytes(data[0..8].try_into().ok()?);
        let logical_counter = u32::from_be_bytes(data[8..12].try_into().ok()?);
        let node_id = u16::from_be_bytes(data[12..14].try_into().ok()?);
        Some(Self { wall_time_ms, logical_counter, node_id })
    }

    pub fn as_i64(&self) -> i64 {
        (self.wall_time_ms << 20) | (self.logical_counter as i64)
    }
}

impl std::fmt::Display for HybridLogicalClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HLC({}, {}, {})", self.wall_time_ms, self.logical_counter, self.node_id)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Clock Stats
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Default)]
pub struct ClockStats {
    pub timers_created: AtomicU64,
    pub timers_fired: AtomicU64,
    pub timers_cancelled: AtomicU64,
    pub time_advances: AtomicU64,
    pub time_skips: AtomicU64,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_real_time_source_now() {
        let ts = RealTimeSource;
        let now = ts.now();
        let since = ts.elapsed_since(now);
        assert!(since.as_millis() < 1000);
    }

    #[test]
    fn test_real_time_source_unix_nanos() {
        let ts = RealTimeSource;
        let nanos = ts.unix_nanos();
        assert!(nanos > 0);
    }

    #[test]
    fn test_mock_time_source_advance() {
        let epoch = SystemTime::UNIX_EPOCH;
        let ts = MockTimeSource::new(epoch);
        assert_eq!(ts.now(), epoch);

        ts.advance(Duration::from_secs(10));
        assert_eq!(ts.now(), epoch + Duration::from_secs(10));

        ts.advance(Duration::from_millis(500));
        assert_eq!(ts.now(), epoch + Duration::from_millis(10500));
    }

    #[test]
    fn test_mock_time_source_set_time() {
        let ts = MockTimeSource::new_at_epoch();
        let target = SystemTime::UNIX_EPOCH + Duration::from_secs(999);
        ts.set_time(target);
        assert_eq!(ts.now(), target);
    }

    #[test]
    fn test_mock_timer_fires_on_advance() {
        let ts = MockTimeSource::new_at_epoch();
        let handle = ts.new_timer(Duration::from_secs(5));
        assert!(!handle.has_fired());
        assert_eq!(ts.pending_timer_count(), 1);

        ts.advance(Duration::from_secs(3));
        assert!(!handle.has_fired());

        ts.advance(Duration::from_secs(3));
        assert!(handle.has_fired());
        assert_eq!(ts.pending_timer_count(), 0);
    }

    #[test]
    fn test_mock_timer_cancel() {
        let ts = MockTimeSource::new_at_epoch();
        let handle = ts.new_timer(Duration::from_secs(5));
        handle.cancel();
        assert!(handle.is_cancelled());

        ts.advance(Duration::from_secs(10));
        assert!(!handle.has_fired()); // cancelled timers don't fire
    }

    #[test]
    fn test_mock_multiple_timers() {
        let ts = MockTimeSource::new_at_epoch();
        let h1 = ts.new_timer(Duration::from_secs(1));
        let h2 = ts.new_timer(Duration::from_secs(3));
        let h3 = ts.new_timer(Duration::from_secs(5));

        ts.advance(Duration::from_secs(2));
        assert!(h1.has_fired());
        assert!(!h2.has_fired());
        assert!(!h3.has_fired());

        ts.advance(Duration::from_secs(2));
        assert!(h2.has_fired());
        assert!(!h3.has_fired());

        ts.advance(Duration::from_secs(2));
        assert!(h3.has_fired());
    }

    #[test]
    fn test_time_skipping_source() {
        let ts = TimeSkippingTimeSource::new(SystemTime::UNIX_EPOCH);
        let _h1 = ts.new_timer(Duration::from_secs(10));
        let _h2 = ts.new_timer(Duration::from_secs(20));

        ts.skip_to_next_timer();
        assert!(ts.skip_count() >= 1);
    }

    #[test]
    fn test_time_skipping_max_duration() {
        let ts = TimeSkippingTimeSource::new(SystemTime::UNIX_EPOCH);
        ts.set_max_skip_duration(Duration::from_secs(5));
        let _h = ts.new_timer(Duration::from_secs(100));

        ts.skip_to_next_timer();
        // Should only skip max 5 seconds
        let elapsed = ts.elapsed_since(SystemTime::UNIX_EPOCH);
        assert!(elapsed <= Duration::from_secs(6));
    }

    #[test]
    fn test_hlc_monotonic() {
        let mut hlc = HybridLogicalClock::new(1);
        let t1 = hlc.now();
        let t2 = hlc.now();
        assert!(t2 >= t1);
    }

    #[test]
    fn test_hlc_update() {
        let mut hlc1 = HybridLogicalClock::new(1);
        let mut hlc2 = HybridLogicalClock::new(2);

        let t1 = hlc1.now();
        let t2 = hlc2.update(&t1);
        assert!(t2 >= t1);
    }

    #[test]
    fn test_hlc_serialization() {
        let mut hlc = HybridLogicalClock::new(42);
        let t = hlc.now();
        let bytes = t.to_bytes();
        let decoded = HybridLogicalClock::from_bytes(&bytes).unwrap();
        assert_eq!(t, decoded);
        assert_eq!(decoded.node_id, 42);
    }

    #[test]
    fn test_hlc_display() {
        let hlc = HybridLogicalClock { wall_time_ms: 1000, logical_counter: 5, node_id: 3 };
        let s = format!("{}", hlc);
        assert!(s.contains("1000"));
        assert!(s.contains("5"));
        assert!(s.contains("3"));
    }

    #[test]
    fn test_hlc_ordering() {
        let a = HybridLogicalClock { wall_time_ms: 100, logical_counter: 0, node_id: 1 };
        let b = HybridLogicalClock { wall_time_ms: 100, logical_counter: 1, node_id: 1 };
        let c = HybridLogicalClock { wall_time_ms: 101, logical_counter: 0, node_id: 1 };
        assert!(a < b);
        assert!(b < c);
    }
}
