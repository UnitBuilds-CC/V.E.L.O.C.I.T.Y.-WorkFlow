//! Async activity completion via task tokens.
//!
//! Temporal supports completing activities asynchronously — the worker receives an activity task,
//! does not complete it immediately, and instead later reports completion using a unique task token.
//! This module implements the token registry that maps tokens to pending async activities.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use std::time::{Duration, Instant};

/// A unique token identifying an in-flight activity task.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActivityTaskToken {
    /// Opaque token bytes (encoded workflow_key + activity_id + attempt + schedule_event_id).
    pub raw: Vec<u8>,
}

impl ActivityTaskToken {
    /// Create a new token from its components.
    pub fn new(workflow_key: u64, activity_id: u64, attempt: u32, schedule_event_id: u64) -> Self {
        let mut raw = Vec::with_capacity(28);
        raw.extend_from_slice(&workflow_key.to_le_bytes());
        raw.extend_from_slice(&activity_id.to_le_bytes());
        raw.extend_from_slice(&attempt.to_le_bytes());
        raw.extend_from_slice(&schedule_event_id.to_le_bytes());
        Self { raw }
    }

    /// Decode the workflow key from the token.
    pub fn workflow_key(&self) -> u64 {
        if self.raw.len() >= 8 {
            u64::from_le_bytes(self.raw[0..8].try_into().unwrap_or_default())
        } else {
            0
        }
    }

    /// Decode the activity ID from the token.
    pub fn activity_id(&self) -> u64 {
        if self.raw.len() >= 16 {
            u64::from_le_bytes(self.raw[8..16].try_into().unwrap_or_default())
        } else {
            0
        }
    }

    /// Decode the attempt number from the token.
    pub fn attempt(&self) -> u32 {
        if self.raw.len() >= 20 {
            u32::from_le_bytes(self.raw[16..20].try_into().unwrap_or_default())
        } else {
            0
        }
    }

    /// Decode the schedule event ID from the token.
    pub fn schedule_event_id(&self) -> u64 {
        if self.raw.len() >= 28 {
            u64::from_le_bytes(self.raw[20..28].try_into().unwrap_or_default())
        } else {
            0
        }
    }

    /// Create a token from raw bytes.
    pub fn from_raw(raw: Vec<u8>) -> Self {
        Self { raw }
    }

    /// Encode as a hex string for external use.
    pub fn to_hex(&self) -> String {
        self.raw.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Decode from a hex string.
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() % 2 != 0 {
            return None;
        }
        let raw: Result<Vec<u8>, _> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
            .collect();
        raw.ok().map(|r| Self { raw: r })
    }
}

/// State of a pending async activity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncActivityState {
    /// Activity is in-flight, waiting for completion.
    InFlight,
    /// Activity was completed successfully.
    Completed,
    /// Activity failed.
    Failed,
    /// Activity was canceled.
    Canceled,
}

/// A pending async activity tracked by the registry.
#[derive(Debug, Clone)]
pub struct PendingAsyncActivity {
    pub token: ActivityTaskToken,
    pub state: AsyncActivityState,
    pub registered_at: Instant,
    pub completed_at: Option<Instant>,
    pub result: Option<Vec<u8>>,
    pub failure_message: Option<String>,
}

impl PendingAsyncActivity {
    /// How long this activity has been pending.
    pub fn pending_duration(&self) -> Duration {
        self.completed_at
            .unwrap_or_else(Instant::now)
            .duration_since(self.registered_at)
    }

    /// Whether this activity is still in-flight.
    pub fn is_pending(&self) -> bool {
        self.state == AsyncActivityState::InFlight
    }
}

/// Registry for tracking async activity completions.
pub struct AsyncActivityRegistry {
    /// Map from token hash to pending activity.
    pending: Mutex<HashMap<u64, PendingAsyncActivity>>,
    #[allow(dead_code)]
    next_id: AtomicU64,
    /// Stats.
    total_registered: AtomicU64,
    total_completed: AtomicU64,
    total_failed: AtomicU64,
    total_canceled: AtomicU64,
    total_not_found: AtomicU64,
}

impl AsyncActivityRegistry {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            total_registered: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
            total_failed: AtomicU64::new(0),
            total_canceled: AtomicU64::new(0),
            total_not_found: AtomicU64::new(0),
        }
    }

    /// Register a new async activity. Returns the task token.
    pub fn register(
        &self,
        workflow_key: u64,
        activity_id: u64,
        attempt: u32,
        schedule_event_id: u64,
    ) -> ActivityTaskToken {
        let token = ActivityTaskToken::new(workflow_key, activity_id, attempt, schedule_event_id);
        let hash = self.token_hash(&token);

        let mut map = self.pending.lock().unwrap();
        map.insert(
            hash,
            PendingAsyncActivity {
                token: token.clone(),
                state: AsyncActivityState::InFlight,
                registered_at: Instant::now(),
                completed_at: None,
                result: None,
                failure_message: None,
            },
        );
        drop(map);

        self.total_registered.fetch_add(1, Ordering::Relaxed);
        token
    }

    /// Complete an async activity by token. Returns the workflow key and result if found.
    pub fn complete_by_token(
        &self,
        token: &ActivityTaskToken,
        result: Vec<u8>,
    ) -> Option<(u64, u64)> {
        let hash = self.token_hash(token);
        let mut map = self.pending.lock().unwrap();
        if let Some(activity) = map.get_mut(&hash) {
            if activity.state != AsyncActivityState::InFlight {
                return None;
            }
            activity.state = AsyncActivityState::Completed;
            activity.completed_at = Some(Instant::now());
            activity.result = Some(result);
            self.total_completed.fetch_add(1, Ordering::Relaxed);
            Some((token.workflow_key(), token.activity_id()))
        } else {
            self.total_not_found.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Fail an async activity by token.
    pub fn fail_by_token(&self, token: &ActivityTaskToken, message: String) -> Option<(u64, u64)> {
        let hash = self.token_hash(token);
        let mut map = self.pending.lock().unwrap();
        if let Some(activity) = map.get_mut(&hash) {
            if activity.state != AsyncActivityState::InFlight {
                return None;
            }
            activity.state = AsyncActivityState::Failed;
            activity.completed_at = Some(Instant::now());
            activity.failure_message = Some(message);
            self.total_failed.fetch_add(1, Ordering::Relaxed);
            Some((token.workflow_key(), token.activity_id()))
        } else {
            self.total_not_found.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Cancel an async activity by token.
    pub fn cancel_by_token(&self, token: &ActivityTaskToken) -> Option<(u64, u64)> {
        let hash = self.token_hash(token);
        let mut map = self.pending.lock().unwrap();
        if let Some(activity) = map.get_mut(&hash) {
            if activity.state != AsyncActivityState::InFlight {
                return None;
            }
            activity.state = AsyncActivityState::Canceled;
            activity.completed_at = Some(Instant::now());
            self.total_canceled.fetch_add(1, Ordering::Relaxed);
            Some((token.workflow_key(), token.activity_id()))
        } else {
            self.total_not_found.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Look up a pending async activity by token.
    pub fn get_pending(&self, token: &ActivityTaskToken) -> Option<PendingAsyncActivity> {
        let hash = self.token_hash(token);
        let map = self.pending.lock().unwrap();
        map.get(&hash).cloned()
    }

    /// Get all pending (in-flight) async activities.
    pub fn all_pending(&self) -> Vec<PendingAsyncActivity> {
        let map = self.pending.lock().unwrap();
        map.values().filter(|a| a.is_pending()).cloned().collect()
    }

    /// Get pending activities for a specific workflow.
    pub fn pending_for_workflow(&self, workflow_key: u64) -> Vec<PendingAsyncActivity> {
        let map = self.pending.lock().unwrap();
        map.values()
            .filter(|a| a.is_pending() && a.token.workflow_key() == workflow_key)
            .cloned()
            .collect()
    }

    /// Remove completed/failed/canceled activities older than `max_age`.
    pub fn cleanup_completed(&self, max_age: Duration) -> usize {
        let mut map = self.pending.lock().unwrap();
        let now = Instant::now();
        let before = map.len();
        map.retain(|_, activity| {
            if activity.is_pending() {
                return true; // keep in-flight
            }
            if let Some(completed_at) = activity.completed_at {
                now.duration_since(completed_at) < max_age
            } else {
                true
            }
        });
        before - map.len()
    }

    /// Cancel all in-flight activities for a workflow.
    pub fn cancel_all_for_workflow(&self, workflow_key: u64) -> usize {
        let mut map = self.pending.lock().unwrap();
        let mut count = 0;
        for activity in map.values_mut() {
            if activity.is_pending() && activity.token.workflow_key() == workflow_key {
                activity.state = AsyncActivityState::Canceled;
                activity.completed_at = Some(Instant::now());
                count += 1;
                self.total_canceled.fetch_add(1, Ordering::Relaxed);
            }
        }
        count
    }

    /// Number of in-flight async activities.
    pub fn pending_count(&self) -> usize {
        let map = self.pending.lock().unwrap();
        map.values().filter(|a| a.is_pending()).count()
    }

    /// Total number of registered activities (including completed).
    pub fn total_registered(&self) -> u64 {
        self.total_registered.load(Ordering::Relaxed)
    }

    /// Total completed successfully.
    pub fn total_completed(&self) -> u64 {
        self.total_completed.load(Ordering::Relaxed)
    }

    /// Total failed.
    pub fn total_failed(&self) -> u64 {
        self.total_failed.load(Ordering::Relaxed)
    }

    /// Total canceled.
    pub fn total_canceled(&self) -> u64 {
        self.total_canceled.load(Ordering::Relaxed)
    }

    /// Total not found on completion attempt.
    pub fn total_not_found(&self) -> u64 {
        self.total_not_found.load(Ordering::Relaxed)
    }

    /// Compute a hash for the token (used as map key).
    fn token_hash(&self, token: &ActivityTaskToken) -> u64 {
        // Simple FNV-1a hash
        let mut hash: u64 = 0xcbf29ce484222325;
        for &b in &token.raw {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

impl Default for AsyncActivityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_roundtrip() {
        let token = ActivityTaskToken::new(42, 100, 3, 500);
        assert_eq!(token.workflow_key(), 42);
        assert_eq!(token.activity_id(), 100);
        assert_eq!(token.attempt(), 3);
        assert_eq!(token.schedule_event_id(), 500);
    }

    #[test]
    fn test_token_hex_roundtrip() {
        let token = ActivityTaskToken::new(42, 100, 3, 500);
        let hex = token.to_hex();
        let decoded = ActivityTaskToken::from_hex(&hex).unwrap();
        assert_eq!(decoded.workflow_key(), 42);
        assert_eq!(decoded.activity_id(), 100);
        assert_eq!(decoded.attempt(), 3);
        assert_eq!(decoded.schedule_event_id(), 500);
    }

    #[test]
    fn test_token_from_raw() {
        let token = ActivityTaskToken::new(1, 2, 3, 4);
        let raw = token.raw.clone();
        let t2 = ActivityTaskToken::from_raw(raw);
        assert_eq!(t2.workflow_key(), 1);
        assert_eq!(t2.activity_id(), 2);
    }

    #[test]
    fn test_register_and_complete() {
        let registry = AsyncActivityRegistry::new();
        let token = registry.register(42, 100, 1, 500);
        assert_eq!(registry.pending_count(), 1);
        assert_eq!(registry.total_registered(), 1);

        let result = registry.complete_by_token(&token, vec![1, 2, 3]);
        assert_eq!(result, Some((42, 100)));
        assert_eq!(registry.pending_count(), 0);
        assert_eq!(registry.total_completed(), 1);
    }

    #[test]
    fn test_register_and_fail() {
        let registry = AsyncActivityRegistry::new();
        let token = registry.register(42, 100, 1, 500);
        let result = registry.fail_by_token(&token, "something went wrong".to_string());
        assert_eq!(result, Some((42, 100)));
        assert_eq!(registry.total_failed(), 1);
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn test_register_and_cancel() {
        let registry = AsyncActivityRegistry::new();
        let token = registry.register(42, 100, 1, 500);
        let result = registry.cancel_by_token(&token);
        assert_eq!(result, Some((42, 100)));
        assert_eq!(registry.total_canceled(), 1);
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn test_complete_unknown_token() {
        let registry = AsyncActivityRegistry::new();
        let token = ActivityTaskToken::new(999, 999, 1, 999);
        let result = registry.complete_by_token(&token, vec![]);
        assert_eq!(result, None);
        assert_eq!(registry.total_not_found(), 1);
    }

    #[test]
    fn test_double_complete_rejected() {
        let registry = AsyncActivityRegistry::new();
        let token = registry.register(42, 100, 1, 500);
        assert!(registry.complete_by_token(&token, vec![1]).is_some());
        // Second completion should fail (already completed)
        assert!(registry.complete_by_token(&token, vec![2]).is_none());
    }

    #[test]
    fn test_get_pending() {
        let registry = AsyncActivityRegistry::new();
        let token = registry.register(42, 100, 1, 500);
        let pending = registry.get_pending(&token).unwrap();
        assert!(pending.is_pending());
        assert_eq!(pending.state, AsyncActivityState::InFlight);
    }

    #[test]
    fn test_all_pending() {
        let registry = AsyncActivityRegistry::new();
        registry.register(1, 10, 1, 100);
        registry.register(2, 20, 1, 200);
        registry.register(3, 30, 1, 300);
        assert_eq!(registry.all_pending().len(), 3);
    }

    #[test]
    fn test_pending_for_workflow() {
        let registry = AsyncActivityRegistry::new();
        registry.register(42, 10, 1, 100);
        registry.register(42, 20, 1, 200);
        registry.register(99, 30, 1, 300);
        let pending = registry.pending_for_workflow(42);
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn test_cancel_all_for_workflow() {
        let registry = AsyncActivityRegistry::new();
        registry.register(42, 10, 1, 100);
        registry.register(42, 20, 1, 200);
        registry.register(99, 30, 1, 300);
        let canceled = registry.cancel_all_for_workflow(42);
        assert_eq!(canceled, 2);
        assert_eq!(registry.pending_count(), 1); // only workflow 99 remains
    }

    #[test]
    fn test_cleanup_completed() {
        let registry = AsyncActivityRegistry::new();
        let t1 = registry.register(1, 10, 1, 100);
        registry.register(2, 20, 1, 200); // still pending
        registry.complete_by_token(&t1, vec![]);

        // Sleep long enough so the completed activity is older than max_age
        std::thread::sleep(Duration::from_millis(20));
        let removed = registry.cleanup_completed(Duration::from_millis(10));
        assert!(removed >= 1, "expected at least 1 removed, got {}", removed);
        assert_eq!(registry.pending_count(), 1); // only the pending one remains
    }

    #[test]
    fn test_pending_duration() {
        let registry = AsyncActivityRegistry::new();
        let token = registry.register(1, 10, 1, 100);
        std::thread::sleep(Duration::from_millis(10));
        let pending = registry.get_pending(&token).unwrap();
        assert!(pending.pending_duration() >= Duration::from_millis(5));
    }

    #[test]
    fn test_stats() {
        let registry = AsyncActivityRegistry::new();
        let t1 = registry.register(1, 10, 1, 100);
        let t2 = registry.register(2, 20, 1, 200);
        let t3 = registry.register(3, 30, 1, 300);

        registry.complete_by_token(&t1, vec![]);
        registry.fail_by_token(&t2, "err".into());
        registry.cancel_by_token(&t3);

        assert_eq!(registry.total_registered(), 3);
        assert_eq!(registry.total_completed(), 1);
        assert_eq!(registry.total_failed(), 1);
        assert_eq!(registry.total_canceled(), 1);
    }

    #[test]
    fn test_token_short_raw() {
        let token = ActivityTaskToken::from_raw(vec![1, 2, 3]);
        assert_eq!(token.workflow_key(), 0); // too short
        assert_eq!(token.activity_id(), 0);
    }

    #[test]
    fn test_hex_invalid() {
        assert!(ActivityTaskToken::from_hex("zzzz").is_none());
        assert!(ActivityTaskToken::from_hex("abc").is_none()); // odd length
    }
}
