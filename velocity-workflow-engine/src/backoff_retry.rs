//! Backoff and retry matching Temporal's common/backoff (1,216 lines).
//!
//! Covers: exponential backoff, jitter, retry policy calculation,
//! backoff coordinator, and retry budget tracking.

use std::time::Duration;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;

// ═══════════════════════════════════════════════════════════════════════════════
// Backoff Calculator
// ═══════════════════════════════════════════════════════════════════════════════

pub struct BackoffCalculator {
    pub initial_interval: Duration,
    pub backoff_coefficient: f64,
    pub max_interval: Duration,
    pub max_attempts: u32,
    pub jitter: JitterMode,
}

#[derive(Debug, Clone, Copy)]
pub enum JitterMode {
    None,
    Full,
    Percent(u8), // 0-100
}

impl BackoffCalculator {
    pub fn new(initial: Duration, coefficient: f64, max: Duration, max_attempts: u32) -> Self {
        Self {
            initial_interval: initial,
            backoff_coefficient: coefficient,
            max_interval: max,
            max_attempts,
            jitter: JitterMode::Full,
        }
    }

    pub fn with_jitter(mut self, jitter: JitterMode) -> Self {
        self.jitter = jitter;
        self
    }

    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        if attempt == 0 { return Duration::ZERO; }
        if attempt > self.max_attempts { return Duration::MAX; }

        let base_ms = self.initial_interval.as_millis() as f64;
        let raw = base_ms * self.backoff_coefficient.powi(attempt as i32 - 1);
        let capped_ms = raw.min(self.max_interval.as_millis() as f64);

        let final_ms = match self.jitter {
            JitterMode::None => capped_ms,
            JitterMode::Full => {
                let jitter_factor = pseudo_random_f64(attempt);
                capped_ms * jitter_factor
            }
            JitterMode::Percent(pct) => {
                let jitter_range = capped_ms * (pct as f64 / 100.0);
                let jitter_offset = jitter_range * pseudo_random_f64(attempt);
                capped_ms - jitter_range / 2.0 + jitter_offset
            }
        };

        Duration::from_millis(final_ms.max(0.0) as u64)
    }

    pub fn next_backoff(&self, current_attempt: u32) -> Option<Duration> {
        if current_attempt >= self.max_attempts {
            None
        } else {
            Some(self.calculate_backoff(current_attempt + 1))
        }
    }

    pub fn total_time_estimate(&self) -> Duration {
        let mut total = Duration::ZERO;
        for i in 1..=self.max_attempts {
            let d = self.calculate_backoff(i);
            if d == Duration::MAX { break; }
            total += d;
        }
        total
    }
}

fn pseudo_random_f64(seed: u32) -> f64 {
    let x = seed.wrapping_mul(1103515245).wrapping_add(12345);
    let x = x.wrapping_mul(1103515245).wrapping_add(12345);
    (x % 10000) as f64 / 10000.0
}

// ═══════════════════════════════════════════════════════════════════════════════
// Retry Budget
// ═══════════════════════════════════════════════════════════════════════════════

pub struct RetryBudget {
    max_retries_per_second: f64,
    min_retry_ratio: f64,
    current_window_retries: AtomicU64,
    window_start_ms: AtomicU64,
    total_requests: AtomicU64,
    total_retries: AtomicU64,
}

impl RetryBudget {
    pub fn new(max_retries_per_second: f64, min_retry_ratio: f64) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            max_retries_per_second,
            min_retry_ratio,
            current_window_retries: AtomicU64::new(0),
            window_start_ms: AtomicU64::new(now_ms),
            total_requests: AtomicU64::new(0),
            total_retries: AtomicU64::new(0),
        }
    }

    pub fn allow_retry(&self) -> bool {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let window_start = self.window_start_ms.load(Ordering::Relaxed);
        if now_ms - window_start > 1000 {
            self.window_start_ms.store(now_ms, Ordering::Relaxed);
            self.current_window_retries.store(0, Ordering::Relaxed);
        }

        let current = self.current_window_retries.fetch_add(1, Ordering::Relaxed);
        let max_allowed = self.max_retries_per_second as u64;

        let total_req = self.total_requests.fetch_add(1, Ordering::Relaxed);
        let total_ret = self.total_retries.load(Ordering::Relaxed);

        // Check ratio
        if total_req > 10 {
            let ratio = total_ret as f64 / total_req as f64;
            if ratio > self.min_retry_ratio {
                return false;
            }
        }

        current < max_allowed
    }

    pub fn record_retry(&self) {
        self.total_retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn retry_ratio(&self) -> f64 {
        let req = self.total_requests.load(Ordering::Relaxed);
        let ret = self.total_retries.load(Ordering::Relaxed);
        if req > 0 { ret as f64 / req as f64 } else { 0.0 }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Backoff Coordinator
// ═══════════════════════════════════════════════════════════════════════════════

pub struct BackoffCoordinator {
    calculator: BackoffCalculator,
    budget: Option<RetryBudget>,
    attempts: AtomicU64,
}

impl BackoffCoordinator {
    pub fn new(calculator: BackoffCalculator) -> Self {
        Self { calculator, budget: None, attempts: AtomicU64::new(0) }
    }

    pub fn with_budget(mut self, budget: RetryBudget) -> Self {
        self.budget = Some(budget);
        self
    }

    pub fn should_retry(&self) -> Option<Duration> {
        let attempt = self.attempts.load(Ordering::Relaxed);
        if attempt >= self.calculator.max_attempts as u64 {
            return None;
        }
        if let Some(ref budget) = self.budget {
            if !budget.allow_retry() {
                return None;
            }
            budget.record_retry();
        }
        self.attempts.fetch_add(1, Ordering::Relaxed);
        self.calculator.next_backoff(attempt as u32)
    }

    pub fn reset(&self) {
        self.attempts.store(0, Ordering::Relaxed);
    }

    pub fn attempt_count(&self) -> u64 {
        self.attempts.load(Ordering::Relaxed)
    }

    pub fn calculator(&self) -> &BackoffCalculator { &self.calculator }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_exponential() {
        let calc = BackoffCalculator::new(
            Duration::from_millis(100), 2.0, Duration::from_secs(10), 5
        ).with_jitter(JitterMode::None);

        assert_eq!(calc.calculate_backoff(0), Duration::ZERO);
        assert_eq!(calc.calculate_backoff(1), Duration::from_millis(100));
        assert_eq!(calc.calculate_backoff(2), Duration::from_millis(200));
        assert_eq!(calc.calculate_backoff(3), Duration::from_millis(400));
        assert_eq!(calc.calculate_backoff(4), Duration::from_millis(800));
    }

    #[test]
    fn test_backoff_max_interval() {
        let calc = BackoffCalculator::new(
            Duration::from_millis(100), 2.0, Duration::from_millis(500), 10
        ).with_jitter(JitterMode::None);

        assert_eq!(calc.calculate_backoff(5), Duration::from_millis(500)); // capped
        assert_eq!(calc.calculate_backoff(10), Duration::from_millis(500)); // still capped
    }

    #[test]
    fn test_backoff_max_attempts() {
        let calc = BackoffCalculator::new(
            Duration::from_millis(100), 2.0, Duration::from_secs(10), 3
        ).with_jitter(JitterMode::None);
        assert_eq!(calc.calculate_backoff(3), Duration::from_millis(400)); // last valid attempt
        assert_eq!(calc.calculate_backoff(4), Duration::MAX); // beyond max
        assert_eq!(calc.next_backoff(3), None);
    }

    #[test]
    fn test_backoff_with_jitter() {
        let calc = BackoffCalculator::new(
            Duration::from_millis(1000), 2.0, Duration::from_secs(30), 5
        ).with_jitter(JitterMode::Full);

        let b1 = calc.calculate_backoff(1);
        let b2 = calc.calculate_backoff(2);
        // With jitter, values should be less than raw
        assert!(b1.as_millis() <= 1000);
        assert!(b2.as_millis() <= 2000);
    }

    #[test]
    fn test_next_backoff() {
        let calc = BackoffCalculator::new(
            Duration::from_millis(100), 2.0, Duration::from_secs(10), 3
        ).with_jitter(JitterMode::None);

        assert_eq!(calc.next_backoff(0), Some(Duration::from_millis(100)));
        assert_eq!(calc.next_backoff(1), Some(Duration::from_millis(200)));
        assert_eq!(calc.next_backoff(2), Some(Duration::from_millis(400)));
        assert_eq!(calc.next_backoff(3), None);
    }

    #[test]
    fn test_total_time_estimate() {
        let calc = BackoffCalculator::new(
            Duration::from_millis(100), 2.0, Duration::from_secs(10), 4
        ).with_jitter(JitterMode::None);

        let total = calc.total_time_estimate();
        // 100 + 200 + 400 + 800 = 1500ms
        assert_eq!(total, Duration::from_millis(1500));
    }

    #[test]
    fn test_retry_budget() {
        let budget = RetryBudget::new(100.0, 0.5);
        assert!(budget.allow_retry());
        budget.record_retry();
        assert!(budget.retry_ratio() > 0.0);
    }

    #[test]
    fn test_backoff_coordinator() {
        let calc = BackoffCalculator::new(
            Duration::from_millis(100), 2.0, Duration::from_secs(10), 3
        ).with_jitter(JitterMode::None);
        let coord = BackoffCoordinator::new(calc);

        assert!(coord.should_retry().is_some());
        assert!(coord.should_retry().is_some());
        assert!(coord.should_retry().is_some());
        assert!(coord.should_retry().is_none()); // max attempts
        assert_eq!(coord.attempt_count(), 3);
    }

    #[test]
    fn test_backoff_coordinator_reset() {
        let calc = BackoffCalculator::new(
            Duration::from_millis(100), 2.0, Duration::from_secs(10), 2
        ).with_jitter(JitterMode::None);
        let coord = BackoffCoordinator::new(calc);

        coord.should_retry();
        coord.should_retry();
        assert!(coord.should_retry().is_none());

        coord.reset();
        assert_eq!(coord.attempt_count(), 0);
        assert!(coord.should_retry().is_some());
    }

    #[test]
    fn test_coefficient_one() {
        let calc = BackoffCalculator::new(
            Duration::from_millis(100), 1.0, Duration::from_secs(10), 5
        ).with_jitter(JitterMode::None);

        // With coefficient=1, all backoffs should be the same
        assert_eq!(calc.calculate_backoff(1), Duration::from_millis(100));
        assert_eq!(calc.calculate_backoff(2), Duration::from_millis(100));
        assert_eq!(calc.calculate_backoff(3), Duration::from_millis(100));
    }
}
