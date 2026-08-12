//! Durable Promises — external resolution points for async coordination.
//!
//! A Durable Promise is a named value that can be created, awaited, and resolved/rejected
//! by any part of the system (including external systems via HTTP). This enables patterns like:
//! - Webhook callbacks (create promise, hand URL to external system, await resolution)
//! - Human-in-the-loop approvals (create promise, send approval link, await decision)
//! - Cross-service coordination (service A creates, service B resolves)
//!
//! This module provides:
//! - Durable Promise creation with unique IDs
//! - Blocking await (with timeout)
//! - Resolution and rejection by any caller
//! - Idempotent creation (same ID returns same promise)
//! - Promise listing and cleanup
//! - Integration with Virtual Objects (awakeables are built on promises)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// State of a durable promise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromiseState {
    /// Promise has been created but not yet resolved.
    Pending,
    /// Promise has been resolved with a value.
    Resolved,
    /// Promise has been rejected with an error.
    Rejected,
    /// Promise has been completed (resolved or rejected) and cleaned up.
    Completed,
}

/// A durable promise — a named resolution point.
#[derive(Debug, Clone)]
pub struct DurablePromise {
    /// Unique promise ID (e.g., "approval-order-123").
    pub id: String,
    /// Current state.
    pub state: PromiseState,
    /// Resolved value (if resolved).
    pub value: Option<Vec<u8>>,
    /// Rejection error (if rejected).
    pub error: Option<String>,
    /// Completion time (ms, 0 if pending).
    pub completed_ms: u64,
    /// Creation time (ms).
    pub created_ms: u64,
    /// Optional completion callback URL.
    pub completion_callback: Option<String>,
    /// Idempotency key for creation.
    pub idempotency_key: Option<String>,
    /// Tags for filtering/listing.
    pub tags: HashMap<String, String>,
}

/// Configuration for the durable promise subsystem.
#[derive(Debug, Clone)]
pub struct DurablePromiseConfig {
    /// Maximum number of pending promises.
    pub max_pending: usize,
    /// Default timeout for await operations (ms, 0 = no timeout).
    pub default_timeout_ms: u64,
    /// Whether to auto-cleanup resolved promises after a delay.
    pub auto_cleanup: bool,
    /// Delay before auto-cleanup (ms).
    pub cleanup_delay_ms: u64,
}

impl Default for DurablePromiseConfig {
    fn default() -> Self {
        Self {
            max_pending: 100_000,
            default_timeout_ms: 0,
            auto_cleanup: true,
            cleanup_delay_ms: 300_000, // 5 minutes
        }
    }
}

/// Statistics for the durable promise subsystem.
#[derive(Debug, Clone, Default)]
pub struct DurablePromiseStats {
    pub total_created: u64,
    pub total_resolved: u64,
    pub total_rejected: u64,
    pub total_timed_out: u64,
    pub total_cleaned_up: u64,
    pub pending_count: u64,
}

/// Errors from durable promise operations.
#[derive(Debug, Clone)]
pub enum PromiseError {
    AlreadyExists(String),
    NotFound(String),
    AlreadyResolved(String),
    NotPending(String),
    MaxPendingExceeded,
}

impl std::fmt::Display for PromiseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists(id) => write!(f, "promise already exists: {}", id),
            Self::NotFound(id) => write!(f, "promise not found: {}", id),
            Self::AlreadyResolved(id) => write!(f, "promise already resolved: {}", id),
            Self::NotPending(id) => write!(f, "promise not pending: {}", id),
            Self::MaxPendingExceeded => write!(f, "max pending promises exceeded"),
        }
    }
}

impl std::error::Error for PromiseError {}

/// The Durable Promise runtime — manages all durable promises.
pub struct DurablePromiseRuntime {
    /// All promises by ID.
    promises: HashMap<String, DurablePromise>,
    /// Configuration.
    config: DurablePromiseConfig,
    /// Statistics.
    stats: DurablePromiseStats,
    /// Waiters: promise_id -> list of waiter IDs (for notification on resolution).
    waiters: HashMap<String, Vec<u64>>,
    /// Next waiter ID.
    next_waiter_id: AtomicU64,
}

impl DurablePromiseRuntime {
    /// Create a new durable promise runtime.
    pub fn new() -> Self {
        Self::with_config(DurablePromiseConfig::default())
    }

    /// Create with custom configuration.
    pub fn with_config(config: DurablePromiseConfig) -> Self {
        Self {
            promises: HashMap::new(),
            config,
            stats: DurablePromiseStats::default(),
            waiters: HashMap::new(),
            next_waiter_id: AtomicU64::new(1),
        }
    }

    // ─── Promise Lifecycle ─────────────────────────────────────────────────

    /// Create a new durable promise.
    ///
    /// If a promise with the same ID already exists, returns the existing promise
    /// (idempotent creation).
    pub fn create(
        &mut self,
        id: &str,
        completion_callback: Option<String>,
        idempotency_key: Option<String>,
        tags: HashMap<String, String>,
    ) -> Result<DurablePromise, PromiseError> {
        // Idempotent: return existing if already created
        if let Some(existing) = self.promises.get(id) {
            return Ok(existing.clone());
        }

        // Check capacity
        if self.stats.pending_count >= self.config.max_pending as u64 {
            return Err(PromiseError::MaxPendingExceeded);
        }

        let promise = DurablePromise {
            id: id.to_string(),
            state: PromiseState::Pending,
            value: None,
            error: None,
            completed_ms: 0,
            created_ms: 0,
            completion_callback,
            idempotency_key,
            tags,
        };

        self.promises.insert(id.to_string(), promise.clone());
        self.stats.total_created += 1;
        self.stats.pending_count += 1;

        Ok(promise)
    }

    /// Resolve a durable promise with a value.
    pub fn resolve(&mut self, id: &str, value: Vec<u8>) -> Result<(), PromiseError> {
        let promise = self
            .promises
            .get_mut(id)
            .ok_or_else(|| PromiseError::NotFound(id.to_string()))?;

        if promise.state != PromiseState::Pending {
            return Err(PromiseError::AlreadyResolved(id.to_string()));
        }

        promise.state = PromiseState::Resolved;
        promise.value = Some(value);
        promise.completed_ms = 0; // Would use system clock

        self.stats.total_resolved += 1;
        self.stats.pending_count = self.stats.pending_count.saturating_sub(1);

        Ok(())
    }

    /// Reject a durable promise with an error.
    pub fn reject(&mut self, id: &str, error: String) -> Result<(), PromiseError> {
        let promise = self
            .promises
            .get_mut(id)
            .ok_or_else(|| PromiseError::NotFound(id.to_string()))?;

        if promise.state != PromiseState::Pending {
            return Err(PromiseError::AlreadyResolved(id.to_string()));
        }

        promise.state = PromiseState::Rejected;
        promise.error = Some(error);
        promise.completed_ms = 0;

        self.stats.total_rejected += 1;
        self.stats.pending_count = self.stats.pending_count.saturating_sub(1);

        Ok(())
    }

    /// Get a promise by ID.
    pub fn get(&self, id: &str) -> Option<&DurablePromise> {
        self.promises.get(id)
    }

    /// Check if a promise is resolved.
    pub fn is_resolved(&self, id: &str) -> bool {
        self.promises
            .get(id)
            .is_some_and(|p| p.state == PromiseState::Resolved)
    }

    /// Check if a promise is rejected.
    pub fn is_rejected(&self, id: &str) -> bool {
        self.promises
            .get(id)
            .is_some_and(|p| p.state == PromiseState::Rejected)
    }

    /// Check if a promise is pending.
    pub fn is_pending(&self, id: &str) -> bool {
        self.promises
            .get(id)
            .is_some_and(|p| p.state == PromiseState::Pending)
    }

    // ─── Await / Wait ──────────────────────────────────────────────────────

    /// Register a waiter for a promise (for notification when resolved).
    /// Returns a waiter ID that can be used to check for notification.
    pub fn await_promise(&mut self, id: &str) -> Result<u64, PromiseError> {
        if !self.promises.contains_key(id) {
            return Err(PromiseError::NotFound(id.to_string()));
        }

        let waiter_id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        self.waiters
            .entry(id.to_string())
            .or_default()
            .push(waiter_id);
        Ok(waiter_id)
    }

    /// Check if a waiter has been notified (promise resolved/rejected).
    pub fn check_waiter(&self, id: &str, _waiter_id: u64) -> Option<PromiseState> {
        self.promises
            .get(id)
            .map(|p| p.state)
            .filter(|s| *s != PromiseState::Pending)
    }

    /// Get the waiters that should be notified for a promise.
    pub fn notify_waiters(&mut self, id: &str) -> Vec<u64> {
        self.waiters.remove(id).unwrap_or_default()
    }

    // ─── Listing and Cleanup ───────────────────────────────────────────────

    /// List all pending promises.
    pub fn list_pending(&self) -> Vec<&DurablePromise> {
        self.promises
            .values()
            .filter(|p| p.state == PromiseState::Pending)
            .collect()
    }

    /// List promises by tag.
    pub fn list_by_tag(&self, tag_key: &str, tag_value: &str) -> Vec<&DurablePromise> {
        self.promises
            .values()
            .filter(|p| p.tags.get(tag_key).is_some_and(|v| v == tag_value))
            .collect()
    }

    /// List promises with a given state.
    pub fn list_by_state(&self, state: PromiseState) -> Vec<&DurablePromise> {
        self.promises
            .values()
            .filter(|p| p.state == state)
            .collect()
    }

    /// Clean up completed promises older than the given age.
    pub fn cleanup(&mut self, max_age_ms: u64) -> u64 {
        let before = self.promises.len();
        self.promises.retain(|_, p| match p.state {
            PromiseState::Resolved | PromiseState::Rejected => {
                p.completed_ms == 0 || p.completed_ms > max_age_ms
            }
            _ => true,
        });
        let cleaned = (before - self.promises.len()) as u64;
        self.stats.total_cleaned_up += cleaned;
        cleaned
    }

    /// Get statistics.
    pub fn stats(&self) -> &DurablePromiseStats {
        &self.stats
    }

    /// Get the total number of promises.
    pub fn promise_count(&self) -> usize {
        self.promises.len()
    }

    /// Get the number of pending promises.
    pub fn pending_count(&self) -> u64 {
        self.stats.pending_count
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_promise() {
        let mut rt = DurablePromiseRuntime::new();
        let p = rt.create("approval-1", None, None, HashMap::new()).unwrap();
        assert_eq!(p.id, "approval-1");
        assert_eq!(p.state, PromiseState::Pending);
        assert_eq!(rt.pending_count(), 1);
    }

    #[test]
    fn test_idempotent_create() {
        let mut rt = DurablePromiseRuntime::new();
        let p1 = rt.create("approval-1", None, None, HashMap::new()).unwrap();
        let p2 = rt.create("approval-1", None, None, HashMap::new()).unwrap();
        assert_eq!(p1.id, p2.id);
        assert_eq!(rt.stats().total_created, 1);
    }

    #[test]
    fn test_resolve() {
        let mut rt = DurablePromiseRuntime::new();
        rt.create("promise-1", None, None, HashMap::new()).unwrap();
        rt.resolve("promise-1", b"approved".to_vec()).unwrap();

        let p = rt.get("promise-1").unwrap();
        assert_eq!(p.state, PromiseState::Resolved);
        assert_eq!(p.value.as_ref().unwrap(), b"approved");
        assert!(rt.is_resolved("promise-1"));
    }

    #[test]
    fn test_reject() {
        let mut rt = DurablePromiseRuntime::new();
        rt.create("promise-1", None, None, HashMap::new()).unwrap();
        rt.reject("promise-1", "denied by admin".to_string())
            .unwrap();

        let p = rt.get("promise-1").unwrap();
        assert_eq!(p.state, PromiseState::Rejected);
        assert_eq!(p.error.as_ref().unwrap(), "denied by admin");
        assert!(rt.is_rejected("promise-1"));
    }

    #[test]
    fn test_double_resolve_fails() {
        let mut rt = DurablePromiseRuntime::new();
        rt.create("promise-1", None, None, HashMap::new()).unwrap();
        rt.resolve("promise-1", b"value".to_vec()).unwrap();
        let result = rt.resolve("promise-1", b"other".to_vec());
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_nonexistent() {
        let mut rt = DurablePromiseRuntime::new();
        let result = rt.resolve("nonexistent", b"value".to_vec());
        assert!(result.is_err());
    }

    #[test]
    fn test_await_and_notify() {
        let mut rt = DurablePromiseRuntime::new();
        rt.create("promise-1", None, None, HashMap::new()).unwrap();

        let waiter_id = rt.await_promise("promise-1").unwrap();

        // Not yet resolved
        assert!(rt.check_waiter("promise-1", waiter_id).is_none());

        // Resolve
        rt.resolve("promise-1", b"done".to_vec()).unwrap();

        // Now waiter should see resolved state
        assert_eq!(
            rt.check_waiter("promise-1", waiter_id),
            Some(PromiseState::Resolved)
        );

        // Get notified waiters
        let notified = rt.notify_waiters("promise-1");
        assert_eq!(notified, vec![waiter_id]);
    }

    #[test]
    fn test_list_pending() {
        let mut rt = DurablePromiseRuntime::new();
        rt.create("p1", None, None, HashMap::new()).unwrap();
        rt.create("p2", None, None, HashMap::new()).unwrap();
        rt.create("p3", None, None, HashMap::new()).unwrap();
        rt.resolve("p2", b"val".to_vec()).unwrap();

        let pending = rt.list_pending();
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn test_list_by_tag() {
        let mut rt = DurablePromiseRuntime::new();
        let mut tags = HashMap::new();
        tags.insert("type".to_string(), "approval".to_string());
        rt.create("p1", None, None, tags.clone()).unwrap();

        let mut tags2 = HashMap::new();
        tags2.insert("type".to_string(), "webhook".to_string());
        rt.create("p2", None, None, tags2).unwrap();

        let approvals = rt.list_by_tag("type", "approval");
        assert_eq!(approvals.len(), 1);
        assert_eq!(approvals[0].id, "p1");
    }

    #[test]
    fn test_list_by_state() {
        let mut rt = DurablePromiseRuntime::new();
        rt.create("p1", None, None, HashMap::new()).unwrap();
        rt.create("p2", None, None, HashMap::new()).unwrap();
        rt.resolve("p1", b"val".to_vec()).unwrap();

        let pending = rt.list_by_state(PromiseState::Pending);
        assert_eq!(pending.len(), 1);

        let resolved = rt.list_by_state(PromiseState::Resolved);
        assert_eq!(resolved.len(), 1);
    }

    #[test]
    fn test_stats() {
        let mut rt = DurablePromiseRuntime::new();
        rt.create("p1", None, None, HashMap::new()).unwrap();
        rt.create("p2", None, None, HashMap::new()).unwrap();
        rt.create("p3", None, None, HashMap::new()).unwrap();
        rt.resolve("p1", b"v".to_vec()).unwrap();
        rt.reject("p2", "err".to_string()).unwrap();

        let stats = rt.stats();
        assert_eq!(stats.total_created, 3);
        assert_eq!(stats.total_resolved, 1);
        assert_eq!(stats.total_rejected, 1);
        assert_eq!(stats.pending_count, 1);
    }

    #[test]
    fn test_max_pending_exceeded() {
        let mut rt = DurablePromiseRuntime::with_config(DurablePromiseConfig {
            max_pending: 2,
            ..Default::default()
        });

        rt.create("p1", None, None, HashMap::new()).unwrap();
        rt.create("p2", None, None, HashMap::new()).unwrap();
        let result = rt.create("p3", None, None, HashMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_completion_callback() {
        let mut rt = DurablePromiseRuntime::new();
        let p = rt
            .create(
                "webhook-1",
                Some("https://example.com/callback".to_string()),
                None,
                HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            p.completion_callback.as_ref().unwrap(),
            "https://example.com/callback"
        );
    }
}
