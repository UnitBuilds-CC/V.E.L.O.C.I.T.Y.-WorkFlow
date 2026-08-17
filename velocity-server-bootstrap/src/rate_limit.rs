//! Token bucket rate limiter for Velocity servers.
//!
//! Provides per-client IP rate limiting using a sliding token bucket algorithm.
//! Thread-safe via `DashMap` for concurrent access from async HTTP handlers.
//!
//! # Configuration
//!
//! - `max_tokens`: Maximum burst size (tokens in the bucket)
//! - `refill_rate`: Tokens added per second
//! - `cleanup_interval`: How often to evict stale client entries
//!
//! # Example
//!
//! ```rust
//! let limiter = RateLimiter::new(100, 10.0); // 100 burst, 10/sec sustained
//! assert!(limiter.check("192.168.1.1"));
//! ```

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Per-client token bucket state.
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

/// Token bucket rate limiter with per-client tracking.
///
/// Thread-safe: uses `DashMap` for lock-free concurrent reads/writes.
pub struct RateLimiter {
    /// Maximum tokens per bucket (burst capacity).
    max_tokens: f64,
    /// Tokens added per second (sustained rate).
    refill_rate: f64,
    /// Per-client buckets keyed by client identifier (IP or API key).
    buckets: DashMap<String, TokenBucket>,
    /// Total requests rejected.
    rejected_total: AtomicU64,
    /// Total requests allowed.
    allowed_total: AtomicU64,
    /// Total unique clients tracked.
    active_clients: AtomicU64,
}

/// Result of a rate limit check.
#[derive(Debug, Clone)]
pub struct RateLimitResult {
    /// Whether the request is allowed.
    pub allowed: bool,
    /// Remaining tokens in the bucket (for response headers).
    pub remaining: u64,
    /// Seconds until the bucket is full again.
    pub retry_after_secs: f64,
    /// The limit ceiling (for X-RateLimit-Limit header).
    pub limit: u64,
}

impl RateLimiter {
    /// Create a new rate limiter.
    ///
    /// - `max_tokens`: Maximum burst capacity per client.
    /// - `refill_rate`: Tokens added per second per client.
    pub fn new(max_tokens: u64, refill_rate: f64) -> Self {
        Self {
            max_tokens: max_tokens as f64,
            refill_rate,
            buckets: DashMap::new(),
            rejected_total: AtomicU64::new(0),
            allowed_total: AtomicU64::new(0),
            active_clients: AtomicU64::new(0),
        }
    }

    /// Check if a request from `client_id` is allowed.
    ///
    /// Returns `true` if the request is within rate limits.
    /// This is the fast-path check used in HTTP handlers.
    pub fn check(&self, client_id: &str) -> bool {
        self.check_detailed(client_id).allowed
    }

    /// Check rate limit with detailed result (for response headers).
    pub fn check_detailed(&self, client_id: &str) -> RateLimitResult {
        let now = Instant::now();

        let mut entry = self.buckets.entry(client_id.to_string()).or_insert_with(|| {
            self.active_clients.fetch_add(1, Ordering::Relaxed);
            TokenBucket {
                tokens: self.max_tokens,
                last_refill: now,
            }
        });

        let bucket = entry.value_mut();

        // Refill tokens based on elapsed time
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_rate).min(self.max_tokens);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            self.allowed_total.fetch_add(1, Ordering::Relaxed);

            RateLimitResult {
                allowed: true,
                remaining: bucket.tokens as u64,
                retry_after_secs: 0.0,
                limit: self.max_tokens as u64,
            }
        } else {
            self.rejected_total.fetch_add(1, Ordering::Relaxed);
            let retry_after = (1.0 - bucket.tokens) / self.refill_rate;

            RateLimitResult {
                allowed: false,
                remaining: 0,
                retry_after_secs: retry_after,
                limit: self.max_tokens as u64,
            }
        }
    }

    /// Evict stale client entries that haven't been seen recently.
    ///
    /// Call this periodically (e.g., every 60 seconds) to prevent memory growth.
    /// Returns the number of entries evicted.
    pub fn cleanup(&self, max_idle_secs: u64) -> usize {
        let now = Instant::now();
        let idle_threshold = std::time::Duration::from_secs(max_idle_secs);
        let mut to_remove = Vec::new();

        for entry in self.buckets.iter() {
            if now.duration_since(entry.value().last_refill) > idle_threshold {
                to_remove.push(entry.key().clone());
            }
        }

        let count = to_remove.len();
        for key in to_remove {
            self.buckets.remove(&key);
        }
        self.active_clients.fetch_sub(count as u64, Ordering::Relaxed);
        count
    }

    /// Get rate limiter statistics.
    pub fn stats(&self) -> RateLimiterStats {
        RateLimiterStats {
            allowed: self.allowed_total.load(Ordering::Relaxed),
            rejected: self.rejected_total.load(Ordering::Relaxed),
            active_clients: self.buckets.len() as u64,
        }
    }

    /// Reset all rate limiter state (for testing).
    #[cfg(test)]
    pub fn reset(&self) {
        self.buckets.clear();
        self.allowed_total.store(0, Ordering::Relaxed);
        self.rejected_total.store(0, Ordering::Relaxed);
        self.active_clients.store(0, Ordering::Relaxed);
    }
}

/// Rate limiter statistics.
#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    pub allowed: u64,
    pub rejected: u64,
    pub active_clients: u64,
}

impl RateLimiterStats {
    /// Render as Prometheus text format metrics.
    pub fn render_prometheus(&self) -> String {
        format!(
            "# HELP velocity_rate_limit_allowed_total Total requests allowed by rate limiter\n\
             # TYPE velocity_rate_limit_allowed_total counter\n\
             velocity_rate_limit_allowed_total {}\n\
             # HELP velocity_rate_limit_rejected_total Total requests rejected by rate limiter\n\
             # TYPE velocity_rate_limit_rejected_total counter\n\
             velocity_rate_limit_rejected_total {}\n\
             # HELP velocity_rate_limit_active_clients Unique clients tracked by rate limiter\n\
             # TYPE velocity_rate_limit_active_clients gauge\n\
             velocity_rate_limit_active_clients {}\n",
            self.allowed, self.rejected, self.active_clients,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_within_burst() {
        let limiter = RateLimiter::new(10, 1.0);
        for _ in 0..10 {
            assert!(limiter.check("client1"));
        }
    }

    #[test]
    fn test_rate_limiter_rejects_over_burst() {
        let limiter = RateLimiter::new(5, 1.0);
        for _ in 0..5 {
            assert!(limiter.check("client1"));
        }
        // 6th request should be rejected
        assert!(!limiter.check("client1"));
    }

    #[test]
    fn test_rate_limiter_per_client_isolation() {
        let limiter = RateLimiter::new(2, 1.0);
        assert!(limiter.check("client1"));
        assert!(limiter.check("client1"));
        assert!(!limiter.check("client1")); // client1 exhausted

        // client2 should still be allowed
        assert!(limiter.check("client2"));
        assert!(limiter.check("client2"));
        assert!(!limiter.check("client2")); // client2 exhausted
    }

    #[test]
    fn test_rate_limiter_refill() {
        let limiter = RateLimiter::new(2, 1000.0); // 1000 tokens/sec for fast test
        assert!(limiter.check("client1"));
        assert!(limiter.check("client1"));
        assert!(!limiter.check("client1")); // exhausted

        // Wait for refill
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(limiter.check("client1")); // should be refilled
    }

    #[test]
    fn test_rate_limiter_stats() {
        let limiter = RateLimiter::new(2, 1.0);
        limiter.check("client1"); // allowed
        limiter.check("client1"); // allowed
        limiter.check("client1"); // rejected

        let stats = limiter.stats();
        assert_eq!(stats.allowed, 2);
        assert_eq!(stats.rejected, 1);
        assert_eq!(stats.active_clients, 1);
    }

    #[test]
    fn test_rate_limiter_detailed_result() {
        let limiter = RateLimiter::new(5, 10.0);
        let result = limiter.check_detailed("client1");
        assert!(result.allowed);
        assert_eq!(result.limit, 5);
        assert!(result.remaining < 5); // one token consumed
    }

    #[test]
    fn test_rate_limiter_cleanup() {
        let limiter = RateLimiter::new(10, 1.0);
        limiter.check("client1");
        limiter.check("client2");

        // Cleanup with 0 max idle should evict everything
        // (entries were just created, but with 0 idle threshold they're all stale)
        std::thread::sleep(std::time::Duration::from_millis(10));
        let evicted = limiter.cleanup(0);
        assert_eq!(evicted, 2);
        assert_eq!(limiter.stats().active_clients, 0);
    }

    #[test]
    fn test_prometheus_rendering() {
        let stats = RateLimiterStats {
            allowed: 100,
            rejected: 5,
            active_clients: 12,
        };
        let prom = stats.render_prometheus();
        assert!(prom.contains("velocity_rate_limit_allowed_total 100"));
        assert!(prom.contains("velocity_rate_limit_rejected_total 5"));
        assert!(prom.contains("velocity_rate_limit_active_clients 12"));
    }
}
