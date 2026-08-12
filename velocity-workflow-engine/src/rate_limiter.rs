//! Rate limiting and quota management for workflow operations.
//! Token bucket algorithm for per-namespace and global rate limits.

use std::collections::HashMap;
use std::sync::{Mutex, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::time::{Instant, Duration};

pub struct TokenBucket {
    capacity: u64,
    tokens: Mutex<f64>,
    last_refill: Mutex<Instant>,
    rate_per_second: f64,
}

impl TokenBucket {
    pub fn new(rate_per_second: f64, capacity: u64) -> Self {
        Self { capacity, tokens: Mutex::new(capacity as f64), last_refill: Mutex::new(Instant::now()), rate_per_second }
    }
    pub fn try_acquire(&self, count: u64) -> bool {
        let mut tokens = self.tokens.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.duration_since(*self.last_refill.lock().unwrap()).as_secs_f64();
        *tokens = (*tokens + elapsed * self.rate_per_second).min(self.capacity as f64);
        *self.last_refill.lock().unwrap() = now;
        if *tokens >= count as f64 { *tokens -= count as f64; true } else { false }
    }
}

pub struct RateLimiter {
    global_bucket: TokenBucket,
    namespace_buckets: Mutex<HashMap<u64, TokenBucket>>,
    default_rate: f64,
    default_capacity: u64,
}

impl RateLimiter {
    pub fn new(global_rate: f64, global_capacity: u64, default_ns_rate: f64) -> Self {
        Self { global_bucket: TokenBucket::new(global_rate, global_capacity), namespace_buckets: Mutex::new(HashMap::new()), default_rate: default_ns_rate, default_capacity: (default_ns_rate * 2.0) as u64 }
    }
    pub fn set_namespace_limit(&self, namespace_id: u64, rate: f64, capacity: u64) {
        self.namespace_buckets.lock().unwrap().insert(namespace_id, TokenBucket::new(rate, capacity));
    }
    pub fn try_acquire(&self, namespace_id: u64, count: u64) -> bool {
        if !self.global_bucket.try_acquire(count) { return false; }
        let buckets = self.namespace_buckets.lock().unwrap();
        if let Some(bucket) = buckets.get(&namespace_id) { bucket.try_acquire(count) } else { true }
    }
    pub fn namespace_count(&self) -> usize { self.namespace_buckets.lock().unwrap().len() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_token_bucket() {
        let bucket = TokenBucket::new(10.0, 10);
        assert!(bucket.try_acquire(5));
        assert!(bucket.try_acquire(5));
        assert!(!bucket.try_acquire(1)); // exhausted
    }
    #[test]
    fn test_rate_limiter_global() {
        let limiter = RateLimiter::new(100.0, 10, 50.0);
        assert!(limiter.try_acquire(0, 5));
        assert!(limiter.try_acquire(0, 5));
        assert!(!limiter.try_acquire(0, 1)); // global exhausted
    }
}
