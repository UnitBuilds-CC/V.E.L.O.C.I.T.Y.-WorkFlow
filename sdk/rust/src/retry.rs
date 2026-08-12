//! Retry utilities with exponential backoff and jitter.
//!
//! Provides a builder-pattern `RetryPolicy` and a generic `execute_with_retry`
//! function for wrapping fallible operations with automatic retry logic.
//!
//! # Example
//!
//! ```rust,no_run
//! use velocity_sdk::retry::{RetryPolicy, execute_with_retry};
//!
//! let policy = RetryPolicy::builder()
//!     .max_attempts(5)
//!     .initial_interval_ms(100)
//!     .backoff_coefficient(2.0)
//!     .max_interval_ms(10_000)
//!     .jitter(true)
//!     .build();
//!
//! let result = execute_with_retry(&policy, || {
//!     fetch_remote_data()
//! });
//! ```

use std::thread;
use std::time::Duration;

/// Configuration for retry behavior with exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of attempts (must be >= 1).
    pub max_attempts: u32,
    /// Initial backoff interval in milliseconds.
    pub initial_interval_ms: u64,
    /// Backoff coefficient (e.g., 2.0 means exponential doubling). Must be >= 1.0.
    pub backoff_coefficient: f64,
    /// Maximum backoff interval in milliseconds.
    pub max_interval_ms: u64,
    /// Whether to add random jitter to backoff.
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_interval_ms: 100,
            backoff_coefficient: 2.0,
            max_interval_ms: 60_000,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Create a new builder for RetryPolicy.
    pub fn builder() -> RetryPolicyBuilder {
        RetryPolicyBuilder::default()
    }

    /// Validate the policy configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_attempts < 1 {
            return Err("max_attempts must be >= 1".into());
        }
        if self.initial_interval_ms == 0 {
            return Err("initial_interval_ms must be > 0".into());
        }
        if self.backoff_coefficient < 1.0 {
            return Err("backoff_coefficient must be >= 1.0".into());
        }
        if self.max_interval_ms < self.initial_interval_ms {
            return Err("max_interval_ms must be >= initial_interval_ms".into());
        }
        Ok(())
    }

    /// Calculate backoff duration for a given attempt (0-based).
    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        let interval = self.initial_interval_ms as f64
            * self.backoff_coefficient.powi(attempt as i32);
        let interval = interval.min(self.max_interval_ms as f64);

        let interval = if self.jitter {
            // Full jitter: random value between 0 and calculated interval
            rand_f64() * interval
        } else {
            interval
        };

        Duration::from_millis(interval as u64)
    }
}

/// Builder for RetryPolicy.
#[derive(Debug, Default)]
pub struct RetryPolicyBuilder {
    policy: RetryPolicy,
}

impl RetryPolicyBuilder {
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.policy.max_attempts = n;
        self
    }

    pub fn initial_interval_ms(mut self, ms: u64) -> Self {
        self.policy.initial_interval_ms = ms;
        self
    }

    pub fn backoff_coefficient(mut self, coeff: f64) -> Self {
        self.policy.backoff_coefficient = coeff;
        self
    }

    pub fn max_interval_ms(mut self, ms: u64) -> Self {
        self.policy.max_interval_ms = ms;
        self
    }

    pub fn jitter(mut self, enabled: bool) -> Self {
        self.policy.jitter = enabled;
        self
    }

    pub fn build(self) -> RetryPolicy {
        self.policy
    }
}

/// Execute a fallible function with retry logic.
///
/// The function is called up to `policy.max_attempts` times. Between attempts,
/// the thread sleeps for a calculated backoff duration.
///
/// Returns `Ok(T)` on success or `Err(E)` if all retries fail.
pub fn execute_with_retry<T, E>(
    policy: &RetryPolicy,
    mut f: impl FnMut() -> Result<T, E>,
) -> Result<T, E> {
    policy.validate().expect("Invalid retry policy");

    let mut last_err: Option<E> = None;

    for attempt in 0..policy.max_attempts {
        match f() {
            Ok(val) => return Ok(val),
            Err(e) => {
                last_err = Some(e);
                if attempt < policy.max_attempts - 1 {
                    let backoff = policy.calculate_backoff(attempt);
                    thread::sleep(backoff);
                }
            }
        }
    }

    Err(last_err.unwrap())
}

/// Execute a fallible function with retry logic and a predicate for retryable errors.
///
/// Only retries if `is_retryable` returns `true` for the error.
pub fn execute_with_retry_if<T, E>(
    policy: &RetryPolicy,
    is_retryable: impl Fn(&E) -> bool,
    mut f: impl FnMut() -> Result<T, E>,
) -> Result<T, E> {
    policy.validate().expect("Invalid retry policy");

    let mut last_err: Option<E> = None;

    for attempt in 0..policy.max_attempts {
        match f() {
            Ok(val) => return Ok(val),
            Err(e) => {
                if !is_retryable(&e) {
                    return Err(e);
                }
                last_err = Some(e);
                if attempt < policy.max_attempts - 1 {
                    let backoff = policy.calculate_backoff(attempt);
                    thread::sleep(backoff);
                }
            }
        }
    }

    Err(last_err.unwrap())
}

/// Simple pseudo-random f64 in [0, 1) — avoids pulling in the `rand` crate.
fn rand_f64() -> f64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::SystemTime;

    let mut hasher = DefaultHasher::new();
    SystemTime::now().hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    let h = hasher.finish();
    (h as f64) / (u64::MAX as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_policy_valid() {
        let policy = RetryPolicy::default();
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn test_invalid_max_attempts() {
        let policy = RetryPolicy { max_attempts: 0, ..Default::default() };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn test_calculate_backoff_no_jitter() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_interval_ms: 100,
            backoff_coefficient: 2.0,
            max_interval_ms: 10_000,
            jitter: false,
        };
        assert_eq!(policy.calculate_backoff(0), Duration::from_millis(100));
        assert_eq!(policy.calculate_backoff(1), Duration::from_millis(200));
        assert_eq!(policy.calculate_backoff(2), Duration::from_millis(400));
    }

    #[test]
    fn test_execute_with_retry_succeeds() {
        let policy = RetryPolicy::default();
        let mut counter = 0;
        let result = execute_with_retry(&policy, || {
            counter += 1;
            if counter < 3 {
                Err("not yet")
            } else {
                Ok(42)
            }
        });
        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter, 3);
    }

    #[test]
    fn test_execute_with_retry_exhausted() {
        let policy = RetryPolicy { max_attempts: 2, ..Default::default() };
        let result: Result<(), &str> = execute_with_retry(&policy, || Err("always fails"));
        assert_eq!(result.unwrap_err(), "always fails");
    }

    #[test]
    fn test_builder_pattern() {
        let policy = RetryPolicy::builder()
            .max_attempts(5)
            .initial_interval_ms(50)
            .backoff_coefficient(1.5)
            .max_interval_ms(5000)
            .jitter(false)
            .build();
        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.initial_interval_ms, 50);
        assert!(!policy.jitter);
    }
}
