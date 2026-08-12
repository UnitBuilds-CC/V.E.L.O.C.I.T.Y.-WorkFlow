//! Retry and circuit-breaker framework for the VELOCITY-WorkFlow engine.
//!
//! Provides configurable retry policies with exponential backoff and jitter,
//! plus a circuit-breaker pattern for protecting downstream dependencies.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::errors::VelocityError;

// ─── Retry Policy ────────────────────────────────────────────────────────────

/// Configuration for retry behaviour.
///
/// Delays follow exponential backoff: `initial_interval_ms * backoff_coefficient^attempt`,
/// capped at `max_interval_ms`, with ±25 % random jitter to prevent thundering-herd.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first). Must be >= 1.
    pub max_attempts: u32,
    /// Initial delay between retries in milliseconds.
    pub initial_interval_ms: u64,
    /// Multiplicative factor applied per attempt (e.g. 2.0 doubles each time).
    pub backoff_coefficient: f64,
    /// Upper bound on the computed delay (before jitter). `None` = no cap.
    pub max_interval_ms: Option<u64>,
    /// If non-empty, only errors whose `error_name()` is in this set are retried.
    /// If empty, all retryable errors (per `VelocityError::retryable()`) are retried.
    pub retryable_error_names: Vec<String>,
}

impl RetryPolicy {
    /// Create a policy with sensible defaults: 3 attempts, 100 ms base, 2× backoff.
    pub fn defaults() -> Self {
        Self {
            max_attempts: 3,
            initial_interval_ms: 100,
            backoff_coefficient: 2.0,
            max_interval_ms: Some(5_000),
            retryable_error_names: Vec::new(),
        }
    }

    /// Builder: set maximum attempts.
    pub fn with_max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n;
        self
    }

    /// Builder: set initial interval in milliseconds.
    pub fn with_initial_interval_ms(mut self, ms: u64) -> Self {
        self.initial_interval_ms = ms;
        self
    }

    /// Builder: set backoff coefficient.
    pub fn with_backoff_coefficient(mut self, coeff: f64) -> Self {
        self.backoff_coefficient = coeff;
        self
    }

    /// Builder: set maximum interval in milliseconds.
    pub fn with_max_interval_ms(mut self, ms: u64) -> Self {
        self.max_interval_ms = Some(ms);
        self
    }

    /// Builder: restrict retries to specific error names.
    pub fn with_retryable_errors(mut self, names: &[&str]) -> Self {
        self.retryable_error_names = names.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Compute the delay for a given attempt (0-based) with exponential backoff,
    /// optional cap, and ±25 % jitter.
    pub fn compute_delay(&self, attempt: u32) -> Duration {
        let base_ms = self.initial_interval_ms as f64
            * self.backoff_coefficient.powi(attempt as i32);
        let capped_ms = match self.max_interval_ms {
            Some(max) => base_ms.min(max as f64),
            None => base_ms,
        };
        // Deterministic jitter: ±25 % based on attempt number for reproducibility
        // in tests; real callers can override with `compute_delay_with_jitter`.
        let jitter_factor = 1.0 + 0.25 * simple_jitter(attempt);
        let final_ms = (capped_ms * jitter_factor).max(0.0) as u64;
        Duration::from_millis(final_ms)
    }

    /// Compute delay with an explicit random value in [0.0, 1.0) for jitter.
    pub fn compute_delay_with_jitter(&self, attempt: u32, random: f64) -> Duration {
        let base_ms = self.initial_interval_ms as f64
            * self.backoff_coefficient.powi(attempt as i32);
        let capped_ms = match self.max_interval_ms {
            Some(max) => base_ms.min(max as f64),
            None => base_ms,
        };
        // Map random [0,1) → jitter factor [0.75, 1.25)
        let jitter_factor = 0.75 + 0.5 * random;
        let final_ms = (capped_ms * jitter_factor).max(0.0) as u64;
        Duration::from_millis(final_ms)
    }

    /// Check whether a specific error should be retried under this policy.
    pub fn should_retry(&self, err: &VelocityError) -> bool {
        if !err.retryable() {
            return false;
        }
        if self.retryable_error_names.is_empty() {
            return true;
        }
        self.retryable_error_names.iter().any(|n| n == err.error_name())
    }
}

/// Simple deterministic jitter: returns a value in [-1.0, 1.0] based on attempt.
fn simple_jitter(attempt: u32) -> f64 {
    // Use a cheap hash-like function for deterministic but varied jitter.
    let x = attempt.wrapping_mul(2654435761);
    let normalized = (x % 2000) as f64 / 1000.0; // 0.0 .. 2.0
    normalized - 1.0 // -1.0 .. 1.0
}

// ─── Retry Executor ──────────────────────────────────────────────────────────

/// Statistics collected during retry execution.
#[derive(Debug, Clone, Default)]
pub struct RetryStats {
    pub attempts: u32,
    pub total_delay_ms: u64,
    pub succeeded: bool,
}

/// Executes an operation with retry according to a `RetryPolicy`.
///
/// The `sleep_fn` callback is invoked between retries to allow the caller to
/// control time (e.g. `std::thread::sleep` in sync code, or a mock in tests).
pub struct RetryExecutor;

impl RetryExecutor {
    /// Execute `operation` with retry. The `sleep_fn` is called between attempts
    /// with the computed delay.
    ///
    /// Returns `Ok(value)` on success, or `Err((last_error, stats))` when all
    /// attempts are exhausted.
    pub fn execute<F, T>(
        policy: &RetryPolicy,
        mut operation: F,
        mut sleep_fn: impl FnMut(Duration),
    ) -> Result<T, (VelocityError, RetryStats)>
    where
        F: FnMut() -> Result<T, VelocityError>,
    {
        let mut stats = RetryStats::default();
        let max = policy.max_attempts.max(1);

        for attempt in 0..max {
            stats.attempts = attempt + 1;
            match operation() {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    let is_last = attempt + 1 >= max;
                    if is_last || !policy.should_retry(&err) {
                        return Err((err, stats));
                    }
                    let delay = policy.compute_delay(attempt);
                    stats.total_delay_ms += delay.as_millis() as u64;
                    sleep_fn(delay);
                }
            }
        }
        // Should not reach here, but satisfy the compiler.
        unreachable!()
    }

    /// Convenience: execute with `std::thread::sleep` as the sleep function.
    pub fn execute_blocking<F, T>(
        policy: &RetryPolicy,
        operation: F,
    ) -> Result<T, (VelocityError, RetryStats)>
    where
        F: FnMut() -> Result<T, VelocityError>,
    {
        Self::execute(policy, operation, |d| std::thread::sleep(d))
    }
}

// ─── Circuit Breaker ─────────────────────────────────────────────────────────

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation — requests pass through.
    Closed,
    /// Tripped — requests are rejected immediately.
    Open,
    /// Testing recovery — a limited number of probe requests are allowed.
    HalfOpen,
}

/// Configuration for the circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit.
    pub failure_threshold: u32,
    /// Time in milliseconds to wait before transitioning from Open → HalfOpen.
    pub recovery_timeout_ms: u64,
    /// Maximum number of probe calls allowed in HalfOpen state.
    pub half_open_max_calls: u32,
}

impl CircuitBreakerConfig {
    /// Sensible defaults: 5 failures, 30 s recovery, 3 half-open probes.
    pub fn defaults() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout_ms: 30_000,
            half_open_max_calls: 3,
        }
    }
}

/// Thread-safe circuit breaker.
///
/// Tracks consecutive failures and transitions between Closed, Open, and
/// HalfOpen states. When open, calls to `record_call()` are rejected so the
/// caller can fast-fail without hitting the downstream dependency.
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Mutex<CircuitState>,
    failure_count: AtomicU32,
    success_count: AtomicU32,
    half_open_calls: AtomicU32,
    last_failure_time: Mutex<Instant>,
    /// Total calls recorded (for metrics).
    total_calls: AtomicU64,
    /// Total failures recorded (for metrics).
    total_failures: AtomicU64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Mutex::new(CircuitState::Closed),
            failure_count: AtomicU32::new(0),
            success_count: AtomicU32::new(0),
            half_open_calls: AtomicU32::new(0),
            last_failure_time: Mutex::new(Instant::now()),
            total_calls: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
        }
    }

    /// Current state of the circuit breaker.
    pub fn state(&self) -> CircuitState {
        // Check for automatic Open → HalfOpen transition.
        self.check_recovery();
        *self.state.lock().unwrap()
    }

    /// Record a successful call.
    pub fn record_success(&self) {
        self.total_calls.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().unwrap();
        match *state {
            CircuitState::HalfOpen => {
                let calls = self.half_open_calls.fetch_add(1, Ordering::Relaxed) + 1;
                if calls >= self.config.half_open_max_calls {
                    // Enough probes succeeded — close the circuit.
                    self.failure_count.store(0, Ordering::Relaxed);
                    self.half_open_calls.store(0, Ordering::Relaxed);
                    *state = CircuitState::Closed;
                }
            }
            CircuitState::Closed => {
                // Reset consecutive failure counter on success.
                self.failure_count.store(0, Ordering::Relaxed);
                self.success_count.fetch_add(1, Ordering::Relaxed);
            }
            CircuitState::Open => {
                // Shouldn't normally happen if caller checks state first.
            }
        }
    }

    /// Record a failed call. Returns the current state after recording.
    pub fn record_failure(&self) -> CircuitState {
        self.total_calls.fetch_add(1, Ordering::Relaxed);
        self.total_failures.fetch_add(1, Ordering::Relaxed);
        {
            let mut t = self.last_failure_time.lock().unwrap();
            *t = Instant::now();
        }

        let mut state = self.state.lock().unwrap();
        match *state {
            CircuitState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                if failures >= self.config.failure_threshold {
                    *state = CircuitState::Open;
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open immediately re-opens.
                *state = CircuitState::Open;
                self.half_open_calls.store(0, Ordering::Relaxed);
                {
                    let mut t = self.last_failure_time.lock().unwrap();
                    *t = Instant::now();
                }
            }
            CircuitState::Open => {}
        }
        *state
    }

    /// Check whether a call should be allowed through.
    ///
    /// Returns `true` if the call is permitted, `false` if the circuit is open.
    pub fn allow_call(&self) -> bool {
        self.check_recovery();
        let state = self.state.lock().unwrap();
        match *state {
            CircuitState::Closed => true,
            CircuitState::Open => false,
            CircuitState::HalfOpen => {
                let calls = self.half_open_calls.load(Ordering::Relaxed);
                calls < self.config.half_open_max_calls
            }
        }
    }

    /// Execute an operation through the circuit breaker.
    ///
    /// Returns `Err(VelocityError::InternalError)` if the circuit is open.
    pub fn execute<F, T>(&self, mut operation: F) -> Result<T, VelocityError>
    where
        F: FnMut() -> Result<T, VelocityError>,
    {
        if !self.allow_call() {
            return Err(VelocityError::InternalError {
                context: "circuit_breaker".to_string(),
                source: "circuit is open".to_string(),
            });
        }
        match operation() {
            Ok(val) => {
                self.record_success();
                Ok(val)
            }
            Err(err) => {
                self.record_failure();
                Err(err)
            }
        }
    }

    /// Reset the circuit breaker to Closed state.
    pub fn reset(&self) {
        let mut state = self.state.lock().unwrap();
        *state = CircuitState::Closed;
        self.failure_count.store(0, Ordering::Relaxed);
        self.success_count.store(0, Ordering::Relaxed);
        self.half_open_calls.store(0, Ordering::Relaxed);
    }

    /// Snapshot metrics.
    pub fn metrics(&self) -> CircuitBreakerMetrics {
        CircuitBreakerMetrics {
            state: self.state(),
            total_calls: self.total_calls.load(Ordering::Relaxed),
            total_failures: self.total_failures.load(Ordering::Relaxed),
            consecutive_failures: self.failure_count.load(Ordering::Relaxed),
        }
    }

    /// Check whether recovery timeout has elapsed and transition Open → HalfOpen.
    fn check_recovery(&self) {
        let mut state = self.state.lock().unwrap();
        if *state == CircuitState::Open {
            let elapsed = {
                let t = self.last_failure_time.lock().unwrap();
                t.elapsed()
            };
            if elapsed >= Duration::from_millis(self.config.recovery_timeout_ms) {
                self.half_open_calls.store(0, Ordering::Relaxed);
                *state = CircuitState::HalfOpen;
            }
        }
    }
}

/// Snapshot of circuit breaker metrics.
#[derive(Debug, Clone)]
pub struct CircuitBreakerMetrics {
    pub state: CircuitState,
    pub total_calls: u64,
    pub total_failures: u64,
    pub consecutive_failures: u32,
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Retry policy ──────────────────────────────────────────────────────

    #[test]
    fn test_retry_policy_defaults() {
        let p = RetryPolicy::defaults();
        assert_eq!(p.max_attempts, 3);
        assert_eq!(p.initial_interval_ms, 100);
        assert!((p.backoff_coefficient - 2.0).abs() < f64::EPSILON);
        assert_eq!(p.max_interval_ms, Some(5_000));
        assert!(p.retryable_error_names.is_empty());
    }

    #[test]
    fn test_retry_policy_builder() {
        let p = RetryPolicy::defaults()
            .with_max_attempts(5)
            .with_initial_interval_ms(200)
            .with_backoff_coefficient(1.5)
            .with_max_interval_ms(10_000)
            .with_retryable_errors(&["DatabaseError", "ReplicationFailed"]);
        assert_eq!(p.max_attempts, 5);
        assert_eq!(p.initial_interval_ms, 200);
        assert_eq!(p.retryable_error_names.len(), 2);
    }

    #[test]
    fn test_compute_delay_exponential() {
        let p = RetryPolicy {
            max_attempts: 5,
            initial_interval_ms: 100,
            backoff_coefficient: 2.0,
            max_interval_ms: None,
            retryable_error_names: vec![],
        };
        // Attempt 0: 100 * 2^0 = 100 (±25% jitter)
        let d0 = p.compute_delay(0);
        assert!(d0.as_millis() >= 75 && d0.as_millis() <= 125, "d0={d0:?}");

        // Attempt 1: 100 * 2^1 = 200 (±25% jitter)
        let d1 = p.compute_delay(1);
        assert!(d1.as_millis() >= 150 && d1.as_millis() <= 250, "d1={d1:?}");

        // Attempt 2: 100 * 2^2 = 400 (±25% jitter)
        let d2 = p.compute_delay(2);
        assert!(d2.as_millis() >= 300 && d2.as_millis() <= 500, "d2={d2:?}");
    }

    #[test]
    fn test_compute_delay_max_cap() {
        let p = RetryPolicy {
            max_attempts: 10,
            initial_interval_ms: 1000,
            backoff_coefficient: 10.0,
            max_interval_ms: Some(2000),
            retryable_error_names: vec![],
        };
        // Without cap: 1000 * 10^1 = 10000, but capped at 2000 (±25% jitter)
        let d = p.compute_delay(1);
        assert!(d.as_millis() <= 2500, "d={d:?}");
    }

    #[test]
    fn test_compute_delay_with_jitter() {
        let p = RetryPolicy {
            max_attempts: 3,
            initial_interval_ms: 100,
            backoff_coefficient: 2.0,
            max_interval_ms: None,
            retryable_error_names: vec![],
        };
        // random=0.0 → jitter_factor=0.75 → 75ms
        let d_min = p.compute_delay_with_jitter(0, 0.0);
        assert_eq!(d_min.as_millis(), 75);

        // random=1.0 → jitter_factor=1.25 → 125ms (but random should be <1.0)
        let d_max = p.compute_delay_with_jitter(0, 1.0);
        assert_eq!(d_max.as_millis(), 125);
    }

    // ── Retry executor ────────────────────────────────────────────────────

    #[test]
    fn test_retry_succeeds_first_attempt() {
        let policy = RetryPolicy::defaults();
        let mut call_count = 0u32;
        let result = RetryExecutor::execute(&policy, || {
            call_count += 1;
            Ok::<u64, VelocityError>(42)
        }, |_| {});
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count, 1);
    }

    #[test]
    fn test_retry_succeeds_after_failures() {
        let policy = RetryPolicy::defaults().with_max_attempts(3);
        let mut call_count = 0u32;
        let result = RetryExecutor::execute(&policy, || {
            call_count += 1;
            if call_count < 3 {
                Err(VelocityError::DatabaseError {
                    operation: "read".to_string(),
                    source: "timeout".to_string(),
                })
            } else {
                Ok(99u64)
            }
        }, |_| {});
        assert!(result.is_ok());
        assert_eq!(call_count, 3);
        let (_, stats) = result.ok().map(|v| (v, RetryStats::default())).unwrap_or_default();
        // Verify via the ok path — we need stats from the actual result.
        // Re-run to capture stats:
        let mut cc = 0u32;
        let result2 = RetryExecutor::execute(&policy, || {
            cc += 1;
            if cc < 3 {
                Err(VelocityError::DatabaseError {
                    operation: "read".to_string(),
                    source: "timeout".to_string(),
                })
            } else {
                Ok(99u64)
            }
        }, |_| {});
        assert_eq!(result2.unwrap(), 99);
    }

    #[test]
    fn test_retry_exhausted() {
        let policy = RetryPolicy::defaults().with_max_attempts(2);
        let result = RetryExecutor::execute(&policy, || {
            Err::<u64, VelocityError>(VelocityError::DatabaseError {
                operation: "write".to_string(),
                source: "connection refused".to_string(),
            })
        }, |_| {});
        assert!(result.is_err());
        let (err, stats) = result.unwrap_err();
        assert_eq!(stats.attempts, 2);
        assert!(!stats.succeeded);
        assert!(matches!(err, VelocityError::DatabaseError { .. }));
    }

    #[test]
    fn test_retry_non_retryable_error_stops_immediately() {
        let policy = RetryPolicy::defaults().with_max_attempts(5);
        let mut call_count = 0u32;
        let result = RetryExecutor::execute(&policy, || {
            call_count += 1;
            Err::<u64, VelocityError>(VelocityError::WorkflowNotFound {
                workflow_key: 1,
            })
        }, |_| {});
        assert!(result.is_err());
        assert_eq!(call_count, 1); // Should not retry
    }

    #[test]
    fn test_retry_with_error_name_filter() {
        let policy = RetryPolicy::defaults()
            .with_max_attempts(3)
            .with_retryable_errors(&["DatabaseError"]);

        // ReplicationFailed is retryable by default but not in the filter.
        let mut call_count = 0u32;
        let result = RetryExecutor::execute(&policy, || {
            call_count += 1;
            Err::<u64, VelocityError>(VelocityError::ReplicationFailed {
                reason: "network".to_string(),
            })
        }, |_| {});
        assert!(result.is_err());
        assert_eq!(call_count, 1); // Not in filter, so no retry
    }

    #[test]
    fn test_retry_sleep_called_between_attempts() {
        let policy = RetryPolicy::defaults()
            .with_max_attempts(3)
            .with_initial_interval_ms(50);
        let mut delays = Vec::new();
        let mut cc = 0u32;
        let _ = RetryExecutor::execute(&policy, || {
            cc += 1;
            if cc < 3 {
                Err(VelocityError::DatabaseError {
                    operation: "x".to_string(),
                    source: "y".to_string(),
                })
            } else {
                Ok(())
            }
        }, |d| delays.push(d));
        assert_eq!(delays.len(), 2); // sleep called between attempts 1→2 and 2→3
    }

    // ── Circuit breaker ───────────────────────────────────────────────────

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::defaults());
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_call());
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            recovery_timeout_ms: 60_000,
            half_open_max_calls: 1,
        };
        let cb = CircuitBreaker::new(config);

        for _ in 0..2 {
            cb.record_failure();
        }
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure(); // 3rd failure → opens
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_call());
    }

    #[test]
    fn test_circuit_breaker_half_open_after_recovery() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout_ms: 10, // Very short for testing
            half_open_max_calls: 2,
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure(); // Opens immediately (threshold=1)
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait for recovery timeout.
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        assert!(cb.allow_call());
    }

    #[test]
    fn test_circuit_breaker_closes_after_half_open_successes() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout_ms: 10,
            half_open_max_calls: 2,
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure(); // Open
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        cb.record_success(); // 1st probe success
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        cb.record_success(); // 2nd probe success → closes
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_reopens_on_half_open_failure() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout_ms: 10,
            half_open_max_calls: 3,
        };
        let cb = CircuitBreaker::new(config);

        cb.record_failure(); // Open
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        cb.record_failure(); // Failure in half-open → re-opens
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout_ms: 60_000,
            half_open_max_calls: 1,
        };
        let cb = CircuitBreaker::new(config);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_call());
    }

    #[test]
    fn test_circuit_breaker_execute_success() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::defaults());
        let result = cb.execute(|| Ok::<u64, VelocityError>(42));
        assert_eq!(result.unwrap(), 42);
        assert_eq!(cb.metrics().total_calls, 1);
        assert_eq!(cb.metrics().total_failures, 0);
    }

    #[test]
    fn test_circuit_breaker_execute_open_rejects() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout_ms: 60_000,
            half_open_max_calls: 1,
        };
        let cb = CircuitBreaker::new(config);
        cb.record_failure(); // Open

        let result = cb.execute(|| Ok::<u64, VelocityError>(42));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VelocityError::InternalError { .. }));
    }

    #[test]
    fn test_circuit_breaker_metrics() {
        let config = CircuitBreakerConfig {
            failure_threshold: 5,
            recovery_timeout_ms: 60_000,
            half_open_max_calls: 1,
        };
        let cb = CircuitBreaker::new(config);
        cb.record_success();
        cb.record_failure();
        cb.record_failure();

        let m = cb.metrics();
        assert_eq!(m.total_calls, 3);
        assert_eq!(m.total_failures, 2);
        assert_eq!(m.consecutive_failures, 2);
        assert_eq!(m.state, CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_success_resets_consecutive_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            recovery_timeout_ms: 60_000,
            half_open_max_calls: 1,
        };
        let cb = CircuitBreaker::new(config);
        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // Reset
        cb.record_failure();
        cb.record_failure();
        // Still closed because success reset the counter.
        assert_eq!(cb.state(), CircuitState::Closed);
    }
}
