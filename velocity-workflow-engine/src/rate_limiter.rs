//! Rate limiting and quota management for workflow operations.
//! Mirrors Temporal's `common/quotas` package with:
//! - Token bucket (ClockedRateLimiter) with time-based refill
//! - Reservation system (delay-based admission)
//! - Multi-rate limiter (chained stages)
//! - Priority-based rate limiting
//! - Routing/per-operation rate limiting
//! - Delayed request rate limiter (queued admission)
//! - Token recycling for failed dispatches
//! - Per-namespace and per-API limiting

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, Arc, Condvar, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::time::{Instant, Duration};

// ─── Priority Levels ─────────────────────────────────────────────────────────

/// Request priority levels, matching Temporal's quotas.OperatorPriority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestPriority {
    /// Operator calls (web UI, tctl) — highest precedence.
    Operator = 0,
    /// High-priority workflow operations.
    High = 1,
    /// Normal workflow operations.
    Normal = 2,
    /// Low-priority background operations.
    Low = 3,
}

impl RequestPriority {
    pub fn from_u32(v: u32) -> Self {
        match v {
            0 => Self::Operator,
            1 => Self::High,
            3 => Self::Low,
            _ => Self::Normal,
        }
    }
}

// ─── Request ─────────────────────────────────────────────────────────────────

/// A rate-limited request, mirroring Temporal's quotas.Request.
#[derive(Debug, Clone)]
pub struct RateRequest {
    /// API name (e.g., "StartWorkflowExecution", "SignalWorkflowExecution").
    pub api: String,
    /// Number of tokens requested.
    pub token_count: u32,
    /// Caller namespace ID.
    pub caller: String,
    /// Caller type ("api", "worker", "operator").
    pub caller_type: String,
    /// Priority of the request.
    pub priority: RequestPriority,
}

impl RateRequest {
    pub fn new(api: &str, caller: &str) -> Self {
        Self {
            api: api.to_string(),
            token_count: 1,
            caller: caller.to_string(),
            caller_type: "api".to_string(),
            priority: RequestPriority::Normal,
        }
    }

    pub fn with_tokens(mut self, n: u32) -> Self { self.token_count = n; self }
    pub fn with_priority(mut self, p: RequestPriority) -> Self { self.priority = p; self }
    pub fn with_caller_type(mut self, ct: &str) -> Self { self.caller_type = ct.to_string(); self }
}

// ─── Reservation ─────────────────────────────────────────────────────────────

/// Outcome of a ReserveN call — indicates how long the caller must wait.
#[derive(Debug, Clone)]
pub struct Reservation {
    /// Whether the reservation is valid (enough capacity eventually).
    ok: bool,
    /// How long to wait before the action can proceed.
    delay: Duration,
    /// Reference back to the limiter for cancellation.
    limiter_id: u64,
    /// Tokens reserved.
    tokens: u32,
}

impl Reservation {
    pub fn ok(ok: bool, delay: Duration) -> Self {
        Self { ok, delay, limiter_id: 0, tokens: 0 }
    }

    pub fn rejected() -> Self {
        Self { ok: false, delay: Duration::MAX, limiter_id: 0, tokens: 0 }
    }

    /// Whether this reservation permits the action.
    pub fn is_ok(&self) -> bool { self.ok }

    /// How long the caller must wait. Zero means act immediately.
    pub fn delay(&self) -> Duration { self.delay }

    /// Cancel this reservation (return tokens to the bucket).
    pub fn cancel(&self) -> bool { self.ok }
}

/// A multi-reservation spanning several limiter stages.
#[derive(Debug)]
pub struct MultiReservation {
    pub ok: bool,
    pub max_delay: Duration,
    pub sub_delays: Vec<Duration>,
}

impl MultiReservation {
    pub fn is_ok(&self) -> bool { self.ok }
    pub fn delay(&self) -> Duration { self.max_delay }
}

// ─── ClockedRateLimiter (Token Bucket) ───────────────────────────────────────

/// Token bucket rate limiter with time-based refill, mirroring Temporal's
/// `ClockedRateLimiter`. Supports Reserve, Wait, and token recycling.
pub struct ClockedRateLimiter {
    rate_per_sec: f64,
    burst: u64,
    tokens: Mutex<f64>,
    last_refill: Mutex<Instant>,
    /// Channel-like mechanism for token recycling.
    recycle_notify: Condvar,
    recycle_mutex: Mutex<bool>,
    id: u64,
}

impl ClockedRateLimiter {
    pub fn new(rate_per_sec: f64, burst: u64) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        Self {
            rate_per_sec,
            burst,
            tokens: Mutex::new(burst as f64),
            last_refill: Mutex::new(Instant::now()),
            recycle_notify: Condvar::new(),
            recycle_mutex: Mutex::new(false),
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Refill tokens based on elapsed time. Must be called under tokens lock.
    fn refill(&self, now: Instant) {
        let mut last = self.last_refill.lock().unwrap();
        let elapsed = now.duration_since(*last).as_secs_f64();
        if elapsed > 0.0 {
            let mut tokens = self.tokens.lock().unwrap();
            *tokens = (*tokens + elapsed * self.rate_per_sec).min(self.burst as f64);
            *last = now;
        }
    }

    /// Try to allow a single request immediately.
    pub fn allow(&self) -> bool {
        self.allow_n(Instant::now(), 1)
    }

    /// Try to allow N tokens immediately. Atomic: either all tokens are consumed or none.
    pub fn allow_n(&self, now: Instant, n: u32) -> bool {
        self.refill(now);
        let mut tokens = self.tokens.lock().unwrap();
        if *tokens >= n as f64 {
            *tokens -= n as f64;
            true
        } else {
            false
        }
    }

    /// Reserve N tokens, returning how long the caller must wait.
    /// This is a non-consuming calculation — use allow_n() to actually consume tokens.
    pub fn reserve_n(&self, now: Instant, n: u32) -> Reservation {
        self.refill(now);
        let tokens = self.tokens.lock().unwrap();
        let available = *tokens;
        if available >= n as f64 {
            Reservation { ok: true, delay: Duration::ZERO, limiter_id: self.id, tokens: n }
        } else if self.rate_per_sec > 0.0 {
            let deficit = n as f64 - available;
            let wait_secs = deficit / self.rate_per_sec;
            Reservation {
                ok: true,
                delay: Duration::from_secs_f64(wait_secs),
                limiter_id: self.id,
                tokens: n,
            }
        } else {
            Reservation::rejected()
        }
    }

    /// Wait (blocking) for N tokens, up to a deadline.
    pub fn wait_n(&self, n: u32, deadline: Duration) -> bool {
        let now = Instant::now();
        let res = self.reserve_n(now, n);
        if !res.is_ok() { return false; }
        if res.delay() == Duration::ZERO { return true; }
        if res.delay() > deadline {
            // Cancel the reservation
            self.recycle_token();
            return false;
        }
        std::thread::sleep(res.delay());
        true
    }

    /// Return a token to the bucket (e.g., when a dispatched task was invalid).
    pub fn recycle_token(&self) {
        let mut tokens = self.tokens.lock().unwrap();
        *tokens = (*tokens + 1.0).min(self.burst as f64);
        self.recycle_notify.notify_one();
    }

    /// Current token count.
    pub fn tokens_at(&self, now: Instant) -> u64 {
        self.refill(now);
        let tokens = self.tokens.lock().unwrap();
        (*tokens).max(0.0) as u64
    }

    /// Configured rate (tokens/sec).
    pub fn rate(&self) -> f64 { self.rate_per_sec }

    /// Configured burst (max tokens).
    pub fn burst(&self) -> u64 { self.burst }

    /// Update the rate dynamically.
    pub fn set_rate(&mut self, new_rate: f64) { self.rate_per_sec = new_rate; }

    /// Update the burst dynamically.
    pub fn set_burst(&mut self, new_burst: u64) { self.burst = new_burst; }
}

// ─── TokenBucket (legacy compat) ────────────────────────────────────────────

/// Simple token bucket (kept for backward compatibility with existing tests).
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

// ─── MultiRateLimiter ────────────────────────────────────────────────────────

/// Chains multiple rate limiters in stages. A request must pass ALL stages.
/// Mirrors Temporal's `MultiRateLimiterImpl`.
pub struct MultiRateLimiter {
    stages: Vec<Arc<ClockedRateLimiter>>,
    /// Recycle channel for token return on failed dispatch.
    recycle_count: AtomicU64,
}

impl MultiRateLimiter {
    pub fn new(stages: Vec<Arc<ClockedRateLimiter>>) -> Self {
        assert!(!stages.is_empty(), "MultiRateLimiter requires at least one stage");
        Self { stages, recycle_count: AtomicU64::new(0) }
    }

    /// Allow a single request through all stages.
    pub fn allow(&self) -> bool {
        self.allow_n(Instant::now(), 1)
    }

    /// Allow N tokens through all stages. If any stage rejects, prior stages get tokens recycled.
    pub fn allow_n(&self, now: Instant, n: u32) -> bool {
        let mut consumed_stages = Vec::with_capacity(self.stages.len());
        for stage in &self.stages {
            if stage.allow_n(now, n) {
                consumed_stages.push(stage);
            } else {
                // Refund tokens to stages that already consumed
                for s in &consumed_stages {
                    s.recycle_token();
                }
                return false;
            }
        }
        true
    }

    /// Reserve through all stages, returning the maximum delay.
    pub fn reserve_n(&self, now: Instant, n: u32) -> MultiReservation {
        let mut sub_delays = Vec::with_capacity(self.stages.len());
        let mut max_delay = Duration::ZERO;

        for stage in &self.stages {
            let res = stage.reserve_n(now, n);
            if !res.is_ok() {
                return MultiReservation { ok: false, max_delay: Duration::MAX, sub_delays };
            }
            if res.delay() > max_delay { max_delay = res.delay(); }
            sub_delays.push(res.delay());
        }
        MultiReservation { ok: true, max_delay, sub_delays }
    }

    /// Recycle a token back to all stages.
    pub fn recycle_token(&self) {
        self.recycle_count.fetch_add(1, Ordering::Relaxed);
        for stage in &self.stages {
            stage.recycle_token();
        }
    }

    /// Minimum rate across all stages.
    pub fn rate(&self) -> f64 {
        self.stages.iter().map(|s| s.rate()).fold(f64::INFINITY, f64::min)
    }

    /// Minimum burst across all stages.
    pub fn burst(&self) -> u64 {
        self.stages.iter().map(|s| s.burst()).fold(u64::MAX, u64::min)
    }

    /// Number of stages.
    pub fn stage_count(&self) -> usize { self.stages.len() }

    /// Total tokens recycled.
    pub fn recycled_tokens(&self) -> u64 { self.recycle_count.load(Ordering::Relaxed) }
}

// ─── PriorityRateLimiter ─────────────────────────────────────────────────────

/// Per-priority rate limiting. Higher-priority requests get larger quotas.
/// Mirrors Temporal's `PriorityRateLimiterImpl`.
pub struct PriorityRateLimiter {
    per_priority: HashMap<RequestPriority, Arc<ClockedRateLimiter>>,
    default_limiter: Arc<ClockedRateLimiter>,
}

impl PriorityRateLimiter {
    pub fn new(default_rate: f64, default_burst: u64) -> Self {
        Self {
            per_priority: HashMap::new(),
            default_limiter: Arc::new(ClockedRateLimiter::new(default_rate, default_burst)),
        }
    }

    /// Set a dedicated limiter for a priority level.
    pub fn set_priority_limit(&mut self, priority: RequestPriority, rate: f64, burst: u64) {
        self.per_priority.insert(priority, Arc::new(ClockedRateLimiter::new(rate, burst)));
    }

    /// Try to allow a request based on its priority.
    pub fn allow(&self, priority: RequestPriority) -> bool {
        let limiter = self.per_priority.get(&priority).unwrap_or(&self.default_limiter);
        limiter.allow()
    }

    /// Allow N tokens for a given priority.
    pub fn allow_n(&self, priority: RequestPriority, n: u32) -> bool {
        let limiter = self.per_priority.get(&priority).unwrap_or(&self.default_limiter);
        limiter.allow_n(Instant::now(), n)
    }

    /// Reserve for a given priority.
    pub fn reserve(&self, priority: RequestPriority, n: u32) -> Reservation {
        let limiter = self.per_priority.get(&priority).unwrap_or(&self.default_limiter);
        limiter.reserve_n(Instant::now(), n)
    }

    pub fn priority_count(&self) -> usize { self.per_priority.len() }
}

// ─── RoutingRateLimiter ──────────────────────────────────────────────────────

/// Per-API routing rate limiter. Different API operations can have different limits.
/// Mirrors Temporal's `RoutingRateLimiterImpl` and `MapRequestRateLimiter`.
pub struct RoutingRateLimiter {
    per_api: Mutex<HashMap<String, Arc<ClockedRateLimiter>>>,
    default_rate: f64,
    default_burst: u64,
}

impl RoutingRateLimiter {
    pub fn new(default_rate: f64, default_burst: u64) -> Self {
        Self {
            per_api: Mutex::new(HashMap::new()),
            default_rate,
            default_burst,
        }
    }

    /// Set rate limit for a specific API operation.
    pub fn set_api_limit(&self, api: &str, rate: f64, burst: u64) {
        self.per_api.lock().unwrap().insert(api.to_string(), Arc::new(ClockedRateLimiter::new(rate, burst)));
    }

    /// Remove API-specific limit (falls back to default).
    pub fn remove_api_limit(&self, api: &str) -> bool {
        self.per_api.lock().unwrap().remove(api).is_some()
    }

    /// Allow a request based on its API name.
    pub fn allow(&self, request: &RateRequest) -> bool {
        let api_limits = self.per_api.lock().unwrap();
        let limiter = api_limits.get(&request.api);
        match limiter {
            Some(l) => l.allow_n(Instant::now(), request.token_count),
            None => true, // No specific limit = allow
        }
    }

    /// Reserve for a request.
    pub fn reserve(&self, request: &RateRequest) -> Reservation {
        let api_limits = self.per_api.lock().unwrap();
        match api_limits.get(&request.api) {
            Some(l) => l.reserve_n(Instant::now(), request.token_count),
            None => Reservation::ok(true, Duration::ZERO),
        }
    }

    /// Number of API-specific limits configured.
    pub fn api_limit_count(&self) -> usize { self.per_api.lock().unwrap().len() }

    /// List all configured API names.
    pub fn configured_apis(&self) -> Vec<String> {
        self.per_api.lock().unwrap().keys().cloned().collect()
    }
}

// ─── DelayedRateLimiter ──────────────────────────────────────────────────────

/// Instead of rejecting requests that exceed the rate limit, queues them and
/// admits them when capacity becomes available. Mirrors Temporal's
/// `DelayedRequestRateLimiter`.
pub struct DelayedRateLimiter {
    inner: Arc<ClockedRateLimiter>,
    queue: Mutex<VecDeque<DelayedEntry>>,
    max_queue_size: usize,
    /// Stats.
    admitted: AtomicU64,
    rejected: AtomicU64,
    queued: AtomicU64,
}

#[derive(Debug)]
struct DelayedEntry {
    request: RateRequest,
    enqueued_at: Instant,
    max_wait: Duration,
}

impl DelayedRateLimiter {
    pub fn new(rate: f64, burst: u64, max_queue_size: usize) -> Self {
        Self {
            inner: Arc::new(ClockedRateLimiter::new(rate, burst)),
            queue: Mutex::new(VecDeque::new()),
            max_queue_size,
            admitted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            queued: AtomicU64::new(0),
        }
    }

    /// Try to admit a request. If rate limit allows, admit immediately.
    /// Otherwise, queue it if there's room. Returns true if admitted (now or queued).
    pub fn admit(&self, request: RateRequest, max_wait: Duration) -> bool {
        // Try immediate admission
        if self.inner.allow_n(Instant::now(), request.token_count) {
            self.admitted.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        // Try to queue
        let mut queue = self.queue.lock().unwrap();
        if queue.len() >= self.max_queue_size {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        queue.push_back(DelayedEntry { request, enqueued_at: Instant::now(), max_wait });
        self.queued.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Process the queue — admit queued requests if capacity allows.
    /// Returns the number of requests admitted from the queue.
    pub fn drain_queue(&self) -> usize {
        let mut admitted = 0;
        let mut queue = self.queue.lock().unwrap();
        let now = Instant::now();

        // Remove expired entries
        queue.retain(|e| e.enqueued_at.elapsed() < e.max_wait);

        // Try to admit from front of queue
        while let Some(entry) = queue.front() {
            if self.inner.allow_n(now, entry.request.token_count) {
                queue.pop_front();
                self.admitted.fetch_add(1, Ordering::Relaxed);
                admitted += 1;
            } else {
                break;
            }
        }
        admitted
    }

    /// Current queue depth.
    pub fn queue_depth(&self) -> usize { self.queue.lock().unwrap().len() }

    /// Total requests admitted (immediate + from queue).
    pub fn total_admitted(&self) -> u64 { self.admitted.load(Ordering::Relaxed) }

    /// Total requests rejected (queue full).
    pub fn total_rejected(&self) -> u64 { self.rejected.load(Ordering::Relaxed) }

    /// Total requests queued.
    pub fn total_queued(&self) -> u64 { self.queued.load(Ordering::Relaxed) }
}

// ─── NamespaceRateLimiter ────────────────────────────────────────────────────

/// Per-namespace rate limiting with a global cap. Mirrors Temporal's approach
/// of having both global and per-namespace limits.
pub struct NamespaceRateLimiter {
    global: Arc<ClockedRateLimiter>,
    namespace_limiters: Mutex<HashMap<String, Arc<ClockedRateLimiter>>>,
    default_ns_rate: f64,
    default_ns_burst: u64,
}

impl NamespaceRateLimiter {
    pub fn new(global_rate: f64, global_burst: u64, default_ns_rate: f64, default_ns_burst: u64) -> Self {
        Self {
            global: Arc::new(ClockedRateLimiter::new(global_rate, global_burst)),
            namespace_limiters: Mutex::new(HashMap::new()),
            default_ns_rate,
            default_ns_burst,
        }
    }

    /// Set a specific rate limit for a namespace.
    pub fn set_namespace_limit(&self, namespace: &str, rate: f64, burst: u64) {
        self.namespace_limiters.lock().unwrap()
            .insert(namespace.to_string(), Arc::new(ClockedRateLimiter::new(rate, burst)));
    }

    /// Remove a namespace-specific limit.
    pub fn remove_namespace_limit(&self, namespace: &str) -> bool {
        self.namespace_limiters.lock().unwrap().remove(namespace).is_some()
    }

    /// Allow a request from a namespace. Must pass both namespace and global limits.
    pub fn allow(&self, namespace: &str, tokens: u32) -> bool {
        let now = Instant::now();

        // Check namespace limit
        let ns_limiters = self.namespace_limiters.lock().unwrap();
        let ns_limiter = ns_limiters.get(namespace);
        let ns_ok = match ns_limiter {
            Some(l) => l.allow_n(now, tokens),
            None => true, // No specific limit
        };
        drop(ns_limiters);

        if !ns_ok { return false; }

        // Check global limit
        self.global.allow_n(now, tokens)
    }

    /// Reserve for a namespace request.
    pub fn reserve(&self, namespace: &str, tokens: u32) -> MultiReservation {
        let now = Instant::now();
        let ns_limiters = self.namespace_limiters.lock().unwrap();
        let ns_res = match ns_limiters.get(namespace) {
            Some(l) => l.reserve_n(now, tokens),
            None => Reservation::ok(true, Duration::ZERO),
        };
        drop(ns_limiters);

        if !ns_res.is_ok() {
            return MultiReservation { ok: false, max_delay: Duration::MAX, sub_delays: vec![] };
        }

        let global_res = self.global.reserve_n(now, tokens);
        if !global_res.is_ok() {
            return MultiReservation { ok: false, max_delay: Duration::MAX, sub_delays: vec![] };
        }

        let max_delay = ns_res.delay().max(global_res.delay());
        MultiReservation { ok: true, max_delay, sub_delays: vec![ns_res.delay(), global_res.delay()] }
    }

    /// Number of namespace-specific limits configured.
    pub fn namespace_count(&self) -> usize { self.namespace_limiters.lock().unwrap().len() }

    /// Recycle a token to the global limiter.
    pub fn recycle_token(&self) { self.global.recycle_token(); }
}

// ─── RateLimiter (legacy compat) ────────────────────────────────────────────

/// Legacy rate limiter (kept for backward compatibility).
pub struct RateLimiter {
    global_bucket: TokenBucket,
    namespace_buckets: Mutex<HashMap<u64, TokenBucket>>,
    default_rate: f64,
    default_capacity: u64,
}

impl RateLimiter {
    pub fn new(global_rate: f64, global_capacity: u64, default_ns_rate: f64) -> Self {
        Self {
            global_bucket: TokenBucket::new(global_rate, global_capacity),
            namespace_buckets: Mutex::new(HashMap::new()),
            default_rate: default_ns_rate,
            default_capacity: (default_ns_rate * 2.0) as u64,
        }
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

// ─── QuotaTracker ────────────────────────────────────────────────────────────

/// Tracks quota usage statistics per namespace and per API.
#[derive(Debug, Clone, Default)]
pub struct QuotaUsage {
    pub total_allowed: u64,
    pub total_rejected: u64,
    pub total_reserved: u64,
    pub total_recycled: u64,
}

/// Statistics tracker for rate limiting decisions.
pub struct QuotaTracker {
    per_namespace: Mutex<HashMap<String, QuotaUsage>>,
    per_api: Mutex<HashMap<String, QuotaUsage>>,
    total: Mutex<QuotaUsage>,
}

impl QuotaTracker {
    pub fn new() -> Self {
        Self {
            per_namespace: Mutex::new(HashMap::new()),
            per_api: Mutex::new(HashMap::new()),
            total: Mutex::new(QuotaUsage::default()),
        }
    }

    pub fn record_allowed(&self, namespace: &str, api: &str) {
        self.record(namespace, api, |u| u.total_allowed += 1);
    }

    pub fn record_rejected(&self, namespace: &str, api: &str) {
        self.record(namespace, api, |u| u.total_rejected += 1);
    }

    pub fn record_reserved(&self, namespace: &str, api: &str) {
        self.record(namespace, api, |u| u.total_reserved += 1);
    }

    pub fn record_recycled(&self, namespace: &str, api: &str) {
        self.record(namespace, api, |u| u.total_recycled += 1);
    }

    fn record(&self, namespace: &str, api: &str, f: impl Fn(&mut QuotaUsage)) {
        {
            let mut ns = self.per_namespace.lock().unwrap();
            f(ns.entry(namespace.to_string()).or_default());
        }
        {
            let mut apis = self.per_api.lock().unwrap();
            f(apis.entry(api.to_string()).or_default());
        }
        {
            let mut total = self.total.lock().unwrap();
            f(&mut *total);
        }
    }

    pub fn namespace_usage(&self, namespace: &str) -> QuotaUsage {
        self.per_namespace.lock().unwrap().get(namespace).cloned().unwrap_or_default()
    }

    pub fn api_usage(&self, api: &str) -> QuotaUsage {
        self.per_api.lock().unwrap().get(api).cloned().unwrap_or_default()
    }

    pub fn total_usage(&self) -> QuotaUsage {
        self.total.lock().unwrap().clone()
    }

    pub fn tracked_namespaces(&self) -> Vec<String> {
        self.per_namespace.lock().unwrap().keys().cloned().collect()
    }

    pub fn tracked_apis(&self) -> Vec<String> {
        self.per_api.lock().unwrap().keys().cloned().collect()
    }
}

impl Default for QuotaTracker {
    fn default() -> Self { Self::new() }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // --- TokenBucket (legacy) ---

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

    // --- ClockedRateLimiter ---

    #[test]
    fn test_clocked_allow() {
        let rl = ClockedRateLimiter::new(100.0, 10);
        assert!(rl.allow());
        assert!(rl.allow_n(Instant::now(), 5));
    }

    #[test]
    fn test_clocked_exhaust() {
        // Use very low rate so no meaningful refill between assertions
        let rl = ClockedRateLimiter::new(0.001, 5);
        for _ in 0..5 { assert!(rl.allow()); }
        assert!(!rl.allow()); // exhausted
    }

    #[test]
    fn test_clocked_reserve_immediate() {
        let rl = ClockedRateLimiter::new(100.0, 10);
        let res = rl.reserve_n(Instant::now(), 3);
        assert!(res.is_ok());
        assert_eq!(res.delay(), Duration::ZERO);
    }

    #[test]
    fn test_clocked_reserve_delayed() {
        let rl = ClockedRateLimiter::new(0.001, 2); // Very low rate, burst 2
        // Exhaust burst
        assert!(rl.allow_n(Instant::now(), 2));
        // Reserve should require waiting
        let res = rl.reserve_n(Instant::now(), 1);
        assert!(res.is_ok());
        assert!(res.delay() > Duration::ZERO);
    }

    #[test]
    fn test_clocked_recycle() {
        let rl = ClockedRateLimiter::new(10.0, 2);
        assert!(rl.allow_n(Instant::now(), 2));
        assert!(!rl.allow()); // exhausted
        rl.recycle_token();
        assert!(rl.allow()); // recycled!
    }

    #[test]
    fn test_clocked_tokens_at() {
        let rl = ClockedRateLimiter::new(100.0, 10);
        let tokens = rl.tokens_at(Instant::now());
        assert_eq!(tokens, 10);
    }

    #[test]
    fn test_clocked_rate_and_burst() {
        let rl = ClockedRateLimiter::new(42.0, 100);
        assert_eq!(rl.rate(), 42.0);
        assert_eq!(rl.burst(), 100);
    }

    // --- MultiRateLimiter ---

    #[test]
    fn test_multi_all_pass() {
        let s1 = Arc::new(ClockedRateLimiter::new(100.0, 10));
        let s2 = Arc::new(ClockedRateLimiter::new(200.0, 20));
        let multi = MultiRateLimiter::new(vec![s1, s2]);
        assert!(multi.allow());
        assert_eq!(multi.stage_count(), 2);
    }

    #[test]
    fn test_multi_first_rejects() {
        let s1 = Arc::new(ClockedRateLimiter::new(0.001, 1)); // Very low rate
        let s2 = Arc::new(ClockedRateLimiter::new(100.0, 100));
        let multi = MultiRateLimiter::new(vec![s1, s2]);
        assert!(multi.allow()); // first token passes
        assert!(!multi.allow()); // second token blocked by stage 1
    }

    #[test]
    fn test_multi_reserve() {
        let s1 = Arc::new(ClockedRateLimiter::new(100.0, 10));
        let s2 = Arc::new(ClockedRateLimiter::new(50.0, 5));
        let multi = MultiRateLimiter::new(vec![s1, s2]);
        let res = multi.reserve_n(Instant::now(), 1);
        assert!(res.is_ok());
    }

    #[test]
    fn test_multi_recycle() {
        // Use very low rate so no meaningful refill between assertions
        let s1 = Arc::new(ClockedRateLimiter::new(0.001, 1));
        let multi = MultiRateLimiter::new(vec![s1]);
        assert!(multi.allow()); // consume the 1 token
        assert!(!multi.allow()); // exhausted (rate too low to refill)
        multi.recycle_token(); // return 1 token
        assert!(multi.allow()); // now passes
        assert_eq!(multi.recycled_tokens(), 1);
    }

    #[test]
    fn test_multi_rate_and_burst() {
        let s1 = Arc::new(ClockedRateLimiter::new(100.0, 50));
        let s2 = Arc::new(ClockedRateLimiter::new(50.0, 100));
        let multi = MultiRateLimiter::new(vec![s1, s2]);
        assert_eq!(multi.rate(), 50.0); // min
        assert_eq!(multi.burst(), 50); // min
    }

    // --- PriorityRateLimiter ---

    #[test]
    fn test_priority_default() {
        let mut rl = PriorityRateLimiter::new(100.0, 100);
        assert!(rl.allow(RequestPriority::Normal));
        assert!(rl.allow(RequestPriority::Operator));
    }

    #[test]
    fn test_priority_per_level() {
        let mut rl = PriorityRateLimiter::new(100.0, 100);
        rl.set_priority_limit(RequestPriority::Low, 1.0, 1);
        assert!(rl.allow(RequestPriority::Low));
        assert!(!rl.allow(RequestPriority::Low)); // exhausted
        assert!(rl.allow(RequestPriority::Normal)); // default still works
    }

    #[test]
    fn test_priority_reserve() {
        let rl = PriorityRateLimiter::new(100.0, 100);
        let res = rl.reserve(RequestPriority::High, 1);
        assert!(res.is_ok());
    }

    // --- RoutingRateLimiter ---

    #[test]
    fn test_routing_no_limits() {
        let rl = RoutingRateLimiter::new(100.0, 100);
        let req = RateRequest::new("StartWorkflow", "ns-1");
        assert!(rl.allow(&req)); // No API-specific limit = allow
    }

    #[test]
    fn test_routing_per_api() {
        let rl = RoutingRateLimiter::new(100.0, 100);
        rl.set_api_limit("StartWorkflow", 1.0, 1);
        let req = RateRequest::new("StartWorkflow", "ns-1");
        assert!(rl.allow(&req));
        assert!(!rl.allow(&req)); // exhausted
        // Different API not limited
        let req2 = RateRequest::new("SignalWorkflow", "ns-1");
        assert!(rl.allow(&req2));
    }

    #[test]
    fn test_routing_remove_limit() {
        let rl = RoutingRateLimiter::new(100.0, 100);
        rl.set_api_limit("Test", 1.0, 1);
        assert_eq!(rl.api_limit_count(), 1);
        assert!(rl.remove_api_limit("Test"));
        assert_eq!(rl.api_limit_count(), 0);
    }

    #[test]
    fn test_routing_configured_apis() {
        let rl = RoutingRateLimiter::new(100.0, 100);
        rl.set_api_limit("A", 10.0, 10);
        rl.set_api_limit("B", 20.0, 20);
        let apis = rl.configured_apis();
        assert_eq!(apis.len(), 2);
        assert!(apis.contains(&"A".to_string()));
        assert!(apis.contains(&"B".to_string()));
    }

    // --- DelayedRateLimiter ---

    #[test]
    fn test_delayed_immediate_admit() {
        let dl = DelayedRateLimiter::new(100.0, 100, 10);
        let req = RateRequest::new("Test", "ns-1");
        assert!(dl.admit(req, Duration::from_secs(5)));
        assert_eq!(dl.total_admitted(), 1);
        assert_eq!(dl.queue_depth(), 0);
    }

    #[test]
    fn test_delayed_queued() {
        let dl = DelayedRateLimiter::new(1.0, 1, 10); // Very low rate
        // Exhaust capacity
        let req1 = RateRequest::new("Test", "ns-1");
        assert!(dl.admit(req1, Duration::from_secs(5)));
        // Next should be queued
        let req2 = RateRequest::new("Test", "ns-1");
        assert!(dl.admit(req2, Duration::from_secs(5)));
        assert_eq!(dl.queue_depth(), 1);
        assert_eq!(dl.total_queued(), 1);
    }

    #[test]
    fn test_delayed_queue_full() {
        let dl = DelayedRateLimiter::new(1.0, 1, 1);
        let req1 = RateRequest::new("Test", "ns-1");
        assert!(dl.admit(req1, Duration::from_secs(5))); // immediate
        let req2 = RateRequest::new("Test", "ns-1");
        assert!(dl.admit(req2, Duration::from_secs(5))); // queued (1 slot)
        let req3 = RateRequest::new("Test", "ns-1");
        assert!(!dl.admit(req3, Duration::from_secs(5))); // rejected (queue full)
        assert_eq!(dl.total_rejected(), 1);
    }

    #[test]
    fn test_delayed_drain() {
        let dl = DelayedRateLimiter::new(1000.0, 100, 10); // High rate
        // Exhaust the entire burst
        for _ in 0..100 { dl.admit(RateRequest::new("Test", "ns-1"), Duration::from_secs(1)); }
        // Next should be queued (burst exhausted)
        let req = RateRequest::new("Test", "ns-1");
        assert!(dl.admit(req, Duration::from_secs(5)));
        assert_eq!(dl.queue_depth(), 1);
        // Wait a bit for tokens to refill at 1000/sec
        std::thread::sleep(Duration::from_millis(10));
        // Drain should admit the queued request (tokens refilled)
        let drained = dl.drain_queue();
        assert_eq!(drained, 1);
        assert_eq!(dl.queue_depth(), 0);
    }

    // --- NamespaceRateLimiter ---

    #[test]
    fn test_namespace_no_limit() {
        let rl = NamespaceRateLimiter::new(1000.0, 1000, 100.0, 100);
        assert!(rl.allow("ns-1", 1));
    }

    #[test]
    fn test_namespace_specific_limit() {
        let rl = NamespaceRateLimiter::new(1000.0, 1000, 100.0, 100);
        rl.set_namespace_limit("ns-1", 1.0, 1);
        assert!(rl.allow("ns-1", 1));
        assert!(!rl.allow("ns-1", 1)); // exhausted
        // Other namespace not affected
        assert!(rl.allow("ns-2", 1));
    }

    #[test]
    fn test_namespace_global_cap() {
        let rl = NamespaceRateLimiter::new(2.0, 2, 100.0, 100);
        assert!(rl.allow("ns-1", 1));
        assert!(rl.allow("ns-2", 1));
        assert!(!rl.allow("ns-3", 1)); // global exhausted
    }

    #[test]
    fn test_namespace_remove_limit() {
        let rl = NamespaceRateLimiter::new(1000.0, 1000, 100.0, 100);
        rl.set_namespace_limit("ns-1", 1.0, 1);
        assert!(rl.remove_namespace_limit("ns-1"));
        assert_eq!(rl.namespace_count(), 0);
    }

    #[test]
    fn test_namespace_reserve() {
        let rl = NamespaceRateLimiter::new(100.0, 100, 50.0, 50);
        let res = rl.reserve("ns-1", 1);
        assert!(res.is_ok());
    }

    #[test]
    fn test_namespace_recycle() {
        let rl = NamespaceRateLimiter::new(1.0, 1, 100.0, 100);
        assert!(rl.allow("ns-1", 1));
        assert!(!rl.allow("ns-2", 1)); // global exhausted
        rl.recycle_token();
        assert!(rl.allow("ns-2", 1)); // recycled
    }

    // --- QuotaTracker ---

    #[test]
    fn test_quota_tracker_basic() {
        let tracker = QuotaTracker::new();
        tracker.record_allowed("ns-1", "StartWorkflow");
        tracker.record_allowed("ns-1", "StartWorkflow");
        tracker.record_rejected("ns-1", "StartWorkflow");

        let ns = tracker.namespace_usage("ns-1");
        assert_eq!(ns.total_allowed, 2);
        assert_eq!(ns.total_rejected, 1);

        let api = tracker.api_usage("StartWorkflow");
        assert_eq!(api.total_allowed, 2);
        assert_eq!(api.total_rejected, 1);

        let total = tracker.total_usage();
        assert_eq!(total.total_allowed, 2);
        assert_eq!(total.total_rejected, 1);
    }

    #[test]
    fn test_quota_tracker_multiple_namespaces() {
        let tracker = QuotaTracker::new();
        tracker.record_allowed("ns-1", "StartWorkflow");
        tracker.record_allowed("ns-2", "SignalWorkflow");

        let namespaces = tracker.tracked_namespaces();
        assert_eq!(namespaces.len(), 2);

        let apis = tracker.tracked_apis();
        assert_eq!(apis.len(), 2);
    }

    #[test]
    fn test_quota_tracker_reserved_and_recycled() {
        let tracker = QuotaTracker::new();
        tracker.record_reserved("ns-1", "Test");
        tracker.record_recycled("ns-1", "Test");

        let usage = tracker.namespace_usage("ns-1");
        assert_eq!(usage.total_reserved, 1);
        assert_eq!(usage.total_recycled, 1);
    }

    // --- RateRequest ---

    #[test]
    fn test_rate_request_builder() {
        let req = RateRequest::new("StartWorkflow", "ns-1")
            .with_tokens(5)
            .with_priority(RequestPriority::High)
            .with_caller_type("worker");
        assert_eq!(req.api, "StartWorkflow");
        assert_eq!(req.caller, "ns-1");
        assert_eq!(req.token_count, 5);
        assert_eq!(req.priority, RequestPriority::High);
        assert_eq!(req.caller_type, "worker");
    }

    // --- Reservation ---

    #[test]
    fn test_reservation_ok() {
        let r = Reservation::ok(true, Duration::from_millis(100));
        assert!(r.is_ok());
        assert_eq!(r.delay(), Duration::from_millis(100));
    }

    #[test]
    fn test_reservation_rejected() {
        let r = Reservation::rejected();
        assert!(!r.is_ok());
    }

    // --- RequestPriority ---

    #[test]
    fn test_priority_from_u32() {
        assert_eq!(RequestPriority::from_u32(0), RequestPriority::Operator);
        assert_eq!(RequestPriority::from_u32(1), RequestPriority::High);
        assert_eq!(RequestPriority::from_u32(2), RequestPriority::Normal);
        assert_eq!(RequestPriority::from_u32(3), RequestPriority::Low);
        assert_eq!(RequestPriority::from_u32(99), RequestPriority::Normal);
    }
}
