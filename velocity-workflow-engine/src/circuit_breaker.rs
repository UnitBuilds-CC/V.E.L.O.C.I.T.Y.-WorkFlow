//! Circuit Breaker for workflow types — prevents cascade failures by automatically
//! rejecting workflow starts when a workflow type is experiencing excessive failures.
//!
//! Implements the three-state circuit breaker pattern (Closed → Open → HalfOpen)
//! with configurable thresholds, cooldown periods, and per-workflow-type tracking.
//!
//! This exceeds Temporal's capabilities — Temporal has no native circuit breaker.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

// ─── Circuit States ────────────────────────────────────────────────────────

/// The state of a circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation — all requests pass through.
    Closed,
    /// Circuit tripped — all requests are rejected immediately.
    Open,
    /// Testing if the downstream has recovered — limited requests pass through.
    HalfOpen,
}

impl CircuitState {
    pub fn as_str(&self) -> &str {
        match self {
            CircuitState::Closed => "closed",
            CircuitState::Open => "open",
            CircuitState::HalfOpen => "half_open",
        }
    }
}

// ─── Circuit Breaker Config ────────────────────────────────────────────────

/// Configuration for a circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit.
    pub failure_threshold: u32,
    /// Time in milliseconds to wait before transitioning from Open to HalfOpen.
    pub cooldown_ms: u64,
    /// Number of successful requests in HalfOpen before closing the circuit.
    pub success_threshold: u32,
    /// Maximum number of requests allowed through in HalfOpen state.
    pub half_open_max_requests: u32,
    /// Time window for failure counting (ms). Failures older than this are ignored.
    pub failure_window_ms: u64,
}

impl CircuitBreakerConfig {
    /// Default: 5 failures, 30s cooldown, 3 successes to close.
    pub fn default_config() -> Self {
        Self {
            failure_threshold: 5,
            cooldown_ms: 30_000,
            success_threshold: 3,
            half_open_max_requests: 3,
            failure_window_ms: 60_000,
        }
    }

    /// Aggressive: 3 failures, 10s cooldown, 2 successes to close.
    pub fn aggressive() -> Self {
        Self {
            failure_threshold: 3,
            cooldown_ms: 10_000,
            success_threshold: 2,
            half_open_max_requests: 2,
            failure_window_ms: 30_000,
        }
    }

    /// Conservative: 10 failures, 60s cooldown, 5 successes to close.
    pub fn conservative() -> Self {
        Self {
            failure_threshold: 10,
            cooldown_ms: 60_000,
            success_threshold: 5,
            half_open_max_requests: 5,
            failure_window_ms: 120_000,
        }
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self::default_config()
    }
}

// ─── Per-Workflow Circuit Breaker ──────────────────────────────────────────

/// Circuit breaker state for a single workflow type.
#[derive(Debug, Clone)]
pub struct WorkflowCircuitBreaker {
    /// Workflow type identifier.
    pub workflow_type_id: u64,
    /// Current state.
    pub state: CircuitState,
    /// Configuration.
    pub config: CircuitBreakerConfig,
    /// Consecutive failure count (reset on success in Closed state).
    pub consecutive_failures: u32,
    /// Total failures in the current window.
    pub window_failures: u32,
    /// Total successes in HalfOpen state.
    pub half_open_successes: u32,
    /// Total requests allowed through in HalfOpen state.
    pub half_open_requests: u32,
    /// Timestamp when the circuit was opened (ms).
    pub opened_at_ms: u64,
    /// Timestamp of the last state transition (ms).
    pub last_transition_ms: u64,
    /// Total number of times the circuit has been opened.
    pub times_opened: u64,
    /// Total requests rejected.
    pub total_rejected: u64,
    /// Total requests allowed.
    pub total_allowed: u64,
    /// Recent failure timestamps (ms) for windowed failure counting.
    pub failure_timestamps: Vec<u64>,
}

impl WorkflowCircuitBreaker {
    pub fn new(workflow_type_id: u64, config: CircuitBreakerConfig) -> Self {
        let now = now_ms();
        Self {
            workflow_type_id,
            state: CircuitState::Closed,
            config,
            consecutive_failures: 0,
            window_failures: 0,
            half_open_successes: 0,
            half_open_requests: 0,
            opened_at_ms: 0,
            last_transition_ms: now,
            times_opened: 0,
            total_rejected: 0,
            total_allowed: 0,
            failure_timestamps: Vec::new(),
        }
    }

    /// Check if a request should be allowed. Returns true if allowed.
    pub fn allow_request(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => {
                self.total_allowed += 1;
                true
            }
            CircuitState::Open => {
                // Check if cooldown has elapsed → transition to HalfOpen
                let now = now_ms();
                if now >= self.opened_at_ms + self.config.cooldown_ms {
                    self.transition_to(CircuitState::HalfOpen);
                    self.half_open_successes = 0;
                    self.half_open_requests = 0;
                    self.total_allowed += 1;
                    self.half_open_requests += 1;
                    true
                } else {
                    self.total_rejected += 1;
                    false
                }
            }
            CircuitState::HalfOpen => {
                if self.half_open_requests < self.config.half_open_max_requests {
                    self.half_open_requests += 1;
                    self.total_allowed += 1;
                    true
                } else {
                    self.total_rejected += 1;
                    false
                }
            }
        }
    }

    /// Record a successful request.
    pub fn record_success(&mut self) {
        match self.state {
            CircuitState::Closed => {
                // Reset consecutive failures on success
                self.consecutive_failures = 0;
                self.window_failures = 0;
                self.failure_timestamps.clear();
            }
            CircuitState::HalfOpen => {
                self.half_open_successes += 1;
                if self.half_open_successes >= self.config.success_threshold {
                    self.transition_to(CircuitState::Closed);
                    self.consecutive_failures = 0;
                    self.window_failures = 0;
                    self.failure_timestamps.clear();
                }
            }
            CircuitState::Open => {
                // Shouldn't happen — requests are rejected in Open state
            }
        }
    }

    /// Record a failed request.
    pub fn record_failure(&mut self) {
        let now = now_ms();
        self.failure_timestamps.push(now);
        self.prune_old_failures(now);

        match self.state {
            CircuitState::Closed => {
                self.consecutive_failures += 1;
                self.window_failures = self.failure_timestamps.len() as u32;

                if self.consecutive_failures >= self.config.failure_threshold
                    || self.window_failures >= self.config.failure_threshold
                {
                    self.transition_to(CircuitState::Open);
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in HalfOpen → back to Open
                self.transition_to(CircuitState::Open);
            }
            CircuitState::Open => {
                // Already open, just track
            }
        }
    }

    /// Force the circuit breaker to a specific state (for testing/admin).
    pub fn force_state(&mut self, state: CircuitState) {
        self.transition_to(state);
    }

    /// Reset the circuit breaker to Closed state.
    pub fn reset(&mut self) {
        self.transition_to(CircuitState::Closed);
        self.consecutive_failures = 0;
        self.window_failures = 0;
        self.half_open_successes = 0;
        self.half_open_requests = 0;
        self.failure_timestamps.clear();
    }

    /// Get a summary of this circuit breaker's state.
    pub fn summary(&self) -> CircuitBreakerSummary {
        CircuitBreakerSummary {
            workflow_type_id: self.workflow_type_id,
            state: self.state,
            consecutive_failures: self.consecutive_failures,
            total_rejected: self.total_rejected,
            total_allowed: self.total_allowed,
            times_opened: self.times_opened,
            rejection_rate: if self.total_allowed + self.total_rejected > 0 {
                self.total_rejected as f64 / (self.total_allowed + self.total_rejected) as f64
            } else {
                0.0
            },
        }
    }

    fn transition_to(&mut self, new_state: CircuitState) {
        if self.state == new_state {
            return;
        }
        let now = now_ms();
        if new_state == CircuitState::Open {
            self.opened_at_ms = now;
            self.times_opened += 1;
        }
        self.state = new_state;
        self.last_transition_ms = now;
    }

    fn prune_old_failures(&mut self, now: u64) {
        let cutoff = now.saturating_sub(self.config.failure_window_ms);
        self.failure_timestamps.retain(|&t| t >= cutoff);
    }
}

// ─── Circuit Breaker Summary ───────────────────────────────────────────────

/// Summary of a circuit breaker's state.
#[derive(Debug, Clone)]
pub struct CircuitBreakerSummary {
    pub workflow_type_id: u64,
    pub state: CircuitState,
    pub consecutive_failures: u32,
    pub total_rejected: u64,
    pub total_allowed: u64,
    pub times_opened: u64,
    pub rejection_rate: f64,
}

// ─── Circuit Breaker Registry ──────────────────────────────────────────────

/// Registry managing circuit breakers for all workflow types.
pub struct CircuitBreakerRegistry {
    breakers: RwLock<HashMap<u64, WorkflowCircuitBreaker>>,
    default_config: CircuitBreakerConfig,
    /// Global stats.
    total_requests: AtomicU64,
    total_rejected: AtomicU64,
    total_opened: AtomicU64,
}

impl CircuitBreakerRegistry {
    pub fn new() -> Self {
        Self {
            breakers: RwLock::new(HashMap::new()),
            default_config: CircuitBreakerConfig::default_config(),
            total_requests: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            total_opened: AtomicU64::new(0),
        }
    }

    pub fn with_default_config(config: CircuitBreakerConfig) -> Self {
        Self {
            breakers: RwLock::new(HashMap::new()),
            default_config: config,
            total_requests: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            total_opened: AtomicU64::new(0),
        }
    }

    /// Register a workflow type with a specific circuit breaker config.
    pub fn register(&self, workflow_type_id: u64, config: CircuitBreakerConfig) {
        let mut breakers = self.breakers.write().unwrap();
        breakers.insert(
            workflow_type_id,
            WorkflowCircuitBreaker::new(workflow_type_id, config),
        );
    }

    /// Check if a request for a workflow type should be allowed.
    /// Returns true if allowed, false if the circuit is open.
    pub fn allow_request(&self, workflow_type_id: u64) -> bool {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        let mut breakers = self.breakers.write().unwrap();
        let breaker = breakers.entry(workflow_type_id).or_insert_with(|| {
            WorkflowCircuitBreaker::new(workflow_type_id, self.default_config.clone())
        });
        let allowed = breaker.allow_request();
        if !allowed {
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
        }
        allowed
    }

    /// Record a success for a workflow type.
    pub fn record_success(&self, workflow_type_id: u64) {
        let mut breakers = self.breakers.write().unwrap();
        if let Some(breaker) = breakers.get_mut(&workflow_type_id) {
            let prev_state = breaker.state;
            breaker.record_success();
            if prev_state != CircuitState::Closed && breaker.state == CircuitState::Closed {
                self.total_opened.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Record a failure for a workflow type.
    pub fn record_failure(&self, workflow_type_id: u64) {
        let mut breakers = self.breakers.write().unwrap();
        if let Some(breaker) = breakers.get_mut(&workflow_type_id) {
            let prev_state = breaker.state;
            breaker.record_failure();
            if prev_state != CircuitState::Open && breaker.state == CircuitState::Open {
                self.total_opened.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Get the current state of a workflow type's circuit breaker.
    pub fn get_state(&self, workflow_type_id: u64) -> CircuitState {
        let breakers = self.breakers.read().unwrap();
        breakers
            .get(&workflow_type_id)
            .map_or(CircuitState::Closed, |b| b.state)
    }

    /// Get a summary for a specific workflow type.
    pub fn get_summary(&self, workflow_type_id: u64) -> Option<CircuitBreakerSummary> {
        let breakers = self.breakers.read().unwrap();
        breakers.get(&workflow_type_id).map(|b| b.summary())
    }

    /// Get summaries for all registered workflow types.
    pub fn all_summaries(&self) -> Vec<CircuitBreakerSummary> {
        let breakers = self.breakers.read().unwrap();
        breakers.values().map(|b| b.summary()).collect()
    }

    /// Force reset a specific circuit breaker.
    pub fn reset(&self, workflow_type_id: u64) {
        let mut breakers = self.breakers.write().unwrap();
        if let Some(breaker) = breakers.get_mut(&workflow_type_id) {
            breaker.reset();
        }
    }

    /// Force reset all circuit breakers.
    pub fn reset_all(&self) {
        let mut breakers = self.breakers.write().unwrap();
        for breaker in breakers.values_mut() {
            breaker.reset();
        }
    }

    /// Get the number of currently open circuits.
    pub fn open_circuit_count(&self) -> usize {
        let breakers = self.breakers.read().unwrap();
        breakers
            .values()
            .filter(|b| b.state == CircuitState::Open)
            .count()
    }

    /// Get the total number of registered workflow types.
    pub fn registered_count(&self) -> usize {
        self.breakers.read().unwrap().len()
    }

    /// Get global stats.
    pub fn global_stats(&self) -> (u64, u64, u64) {
        (
            self.total_requests.load(Ordering::Relaxed),
            self.total_rejected.load(Ordering::Relaxed),
            self.total_opened.load(Ordering::Relaxed),
        )
    }
}

impl Default for CircuitBreakerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Helpers ───────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            failure_threshold: 3,
            cooldown_ms: 100, // Short for testing
            success_threshold: 2,
            half_open_max_requests: 2,
            failure_window_ms: 10_000,
        }
    }

    #[test]
    fn test_circuit_starts_closed() {
        let mut cb = WorkflowCircuitBreaker::new(1, test_config());
        assert_eq!(cb.state, CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_opens_after_threshold() {
        let mut cb = WorkflowCircuitBreaker::new(1, test_config());

        // Record failures up to threshold
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);

        // Requests should be rejected
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_circuit_success_resets_failures() {
        let mut cb = WorkflowCircuitBreaker::new(1, test_config());

        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // Reset
        assert_eq!(cb.consecutive_failures, 0);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Closed); // Still closed
    }

    #[test]
    fn test_circuit_halfopen_after_cooldown() {
        let mut cb = WorkflowCircuitBreaker::new(1, test_config());

        // Open the circuit
        for _ in 0..3 {
            cb.record_failure();
        }
        assert_eq!(cb.state, CircuitState::Open);

        // Wait for cooldown
        std::thread::sleep(std::time::Duration::from_millis(150));

        // Should transition to HalfOpen and allow request
        assert!(cb.allow_request());
        assert_eq!(cb.state, CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_closes_after_halfopen_successes() {
        let mut cb = WorkflowCircuitBreaker::new(1, test_config());

        // Open the circuit
        for _ in 0..3 {
            cb.record_failure();
        }

        // Wait and transition to HalfOpen
        std::thread::sleep(std::time::Duration::from_millis(150));
        cb.allow_request(); // Triggers transition

        // Record successes
        cb.record_success();
        assert_eq!(cb.state, CircuitState::HalfOpen);
        cb.record_success();
        assert_eq!(cb.state, CircuitState::Closed); // Closed again!
    }

    #[test]
    fn test_circuit_reopens_on_halfopen_failure() {
        let mut cb = WorkflowCircuitBreaker::new(1, test_config());

        // Open the circuit
        for _ in 0..3 {
            cb.record_failure();
        }

        // Wait and transition to HalfOpen
        std::thread::sleep(std::time::Duration::from_millis(150));
        cb.allow_request();
        assert_eq!(cb.state, CircuitState::HalfOpen);

        // Failure → back to Open
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
    }

    #[test]
    fn test_circuit_reset() {
        let mut cb = WorkflowCircuitBreaker::new(1, test_config());
        for _ in 0..3 {
            cb.record_failure();
        }
        assert_eq!(cb.state, CircuitState::Open);

        cb.reset();
        assert_eq!(cb.state, CircuitState::Closed);
        assert_eq!(cb.consecutive_failures, 0);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_summary() {
        let mut cb = WorkflowCircuitBreaker::new(1, test_config());
        for _ in 0..3 {
            cb.record_failure();
        }
        // Some rejected requests
        cb.allow_request();
        cb.allow_request();

        let summary = cb.summary();
        assert_eq!(summary.state, CircuitState::Open);
        assert_eq!(summary.times_opened, 1);
        assert!(summary.total_rejected >= 2);
    }

    // ─── Registry Tests ────────────────────────────────────────────────

    #[test]
    fn test_registry_basic() {
        let registry = CircuitBreakerRegistry::new();

        // Auto-creates breaker on first request
        assert!(registry.allow_request(1));
        assert_eq!(registry.get_state(1), CircuitState::Closed);
    }

    #[test]
    fn test_registry_with_custom_config() {
        let registry = CircuitBreakerRegistry::new();
        registry.register(1, test_config());

        // Record failures
        for _ in 0..3 {
            registry.record_failure(1);
        }

        assert_eq!(registry.get_state(1), CircuitState::Open);
        assert!(!registry.allow_request(1));
        assert_eq!(registry.open_circuit_count(), 1);
    }

    #[test]
    fn test_registry_recovery() {
        let registry = CircuitBreakerRegistry::new();
        registry.register(1, test_config());

        // Trip the circuit
        for _ in 0..3 {
            registry.record_failure(1);
        }
        assert_eq!(registry.get_state(1), CircuitState::Open);

        // Wait for cooldown
        std::thread::sleep(std::time::Duration::from_millis(150));

        // Should allow request (transitions to HalfOpen)
        assert!(registry.allow_request(1));

        // Record successes to close
        registry.record_success(1);
        registry.record_success(1);
        assert_eq!(registry.get_state(1), CircuitState::Closed);
        assert_eq!(registry.open_circuit_count(), 0);
    }

    #[test]
    fn test_registry_reset_all() {
        let registry = CircuitBreakerRegistry::new();
        registry.register(1, test_config());
        registry.register(2, test_config());

        for _ in 0..3 {
            registry.record_failure(1);
            registry.record_failure(2);
        }

        assert_eq!(registry.open_circuit_count(), 2);
        registry.reset_all();
        assert_eq!(registry.open_circuit_count(), 0);
    }

    #[test]
    fn test_registry_global_stats() {
        let registry = CircuitBreakerRegistry::new();
        registry.register(1, test_config());

        registry.allow_request(1);
        registry.allow_request(1);
        registry.record_failure(1);
        registry.record_failure(1);
        registry.record_failure(1);
        registry.allow_request(1); // Rejected

        let (total, rejected, _opened) = registry.global_stats();
        assert_eq!(total, 3); // 2 allowed + 1 rejected (record_failure doesn't go through allow_request)
        assert!(rejected >= 1);
    }

    #[test]
    fn test_registry_all_summaries() {
        let registry = CircuitBreakerRegistry::new();
        registry.register(1, test_config());
        registry.register(2, test_config());

        registry.allow_request(1);
        registry.allow_request(2);

        let summaries = registry.all_summaries();
        assert_eq!(summaries.len(), 2);
    }

    #[test]
    fn test_config_presets() {
        let default = CircuitBreakerConfig::default_config();
        assert_eq!(default.failure_threshold, 5);

        let aggressive = CircuitBreakerConfig::aggressive();
        assert_eq!(aggressive.failure_threshold, 3);

        let conservative = CircuitBreakerConfig::conservative();
        assert_eq!(conservative.failure_threshold, 10);
    }

    #[test]
    fn test_circuit_state_strings() {
        assert_eq!(CircuitState::Closed.as_str(), "closed");
        assert_eq!(CircuitState::Open.as_str(), "open");
        assert_eq!(CircuitState::HalfOpen.as_str(), "half_open");
    }

    #[test]
    fn test_half_open_max_requests() {
        let mut cb = WorkflowCircuitBreaker::new(1, test_config());

        // Open the circuit
        for _ in 0..3 {
            cb.record_failure();
        }

        // Wait and transition
        std::thread::sleep(std::time::Duration::from_millis(150));
        assert!(cb.allow_request()); // 1st half-open request
        assert!(cb.allow_request()); // 2nd half-open request (max)
        assert!(!cb.allow_request()); // Rejected — exceeded half_open_max_requests
    }

    #[test]
    fn test_force_state() {
        let mut cb = WorkflowCircuitBreaker::new(1, test_config());
        cb.force_state(CircuitState::Open);
        assert_eq!(cb.state, CircuitState::Open);
        cb.force_state(CircuitState::Closed);
        assert_eq!(cb.state, CircuitState::Closed);
    }
}
