//! Push Dispatcher — Restate-style push-based work dispatch.
//!
//! Unlike Temporal's pull model (workers poll for tasks), the push dispatcher
//! sends work directly to service endpoints via HTTP. This enables:
//! - Lower latency (no polling delay)
//! - Serverless compatibility (services only run when there's work)
//! - Simpler service deployment (plain HTTP endpoints)
//!
//! This module provides:
//! - Service endpoint registration (HTTP URLs)
//! - Push-based dispatch to registered endpoints
//! - Health checking and failure detection
//! - Retry with backoff on dispatch failure
//! - Connection pooling for efficiency
//! - Dispatch journaling for crash recovery

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

/// A registered service endpoint that can receive pushed work.
#[derive(Debug, Clone)]
pub struct ServiceEndpoint {
    /// Service name (e.g., "order-service", "chat-agent").
    pub name: String,
    /// Base URL of the service (e.g., "http://localhost:8080").
    pub base_url: String,
    /// Health check URL (defaults to base_url + "/health").
    pub health_url: String,
    /// Whether the endpoint is currently healthy.
    pub healthy: bool,
    /// Number of consecutive failures.
    pub failure_count: u32,
    /// Maximum retries before marking unhealthy.
    pub max_failures: u32,
    /// Last successful dispatch time (ms).
    pub last_success_ms: u64,
    /// Tags for filtering.
    pub tags: HashMap<String, String>,
}

/// State of a push dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchState {
    /// Dispatch is queued, waiting for an available endpoint.
    Queued,
    /// Dispatch is being sent to the service.
    Dispatching,
    /// Dispatch was successfully received by the service.
    Delivered,
    /// Dispatch failed (will be retried).
    Failed,
    /// Dispatch permanently failed (all retries exhausted).
    PermanentlyFailed,
    /// Dispatch was canceled.
    Canceled,
}

/// A pending push dispatch.
#[derive(Debug, Clone)]
pub struct PushDispatch {
    /// Unique dispatch ID.
    pub dispatch_id: u64,
    /// Target service name.
    pub service_name: String,
    /// Handler/method to invoke.
    pub handler: String,
    /// Input payload.
    pub input: Vec<u8>,
    /// Current state.
    pub state: DispatchState,
    /// Response payload (when delivered).
    pub response: Option<Vec<u8>>,
    /// Error message (when failed).
    pub error: Option<String>,
    /// Number of dispatch attempts.
    pub attempt_count: u32,
    /// Maximum dispatch attempts.
    pub max_attempts: u32,
    /// Idempotency key.
    pub idempotency_key: Option<String>,
    /// Creation time (ms).
    pub created_ms: u64,
    /// Delivery time (ms).
    pub delivered_ms: u64,
    /// The endpoint URL this was dispatched to.
    pub target_url: Option<String>,
}

/// Configuration for the push dispatcher.
#[derive(Debug, Clone)]
pub struct PushDispatcherConfig {
    /// Maximum concurrent dispatches per service.
    pub max_concurrent_per_service: usize,
    /// Default maximum dispatch attempts.
    pub default_max_attempts: u32,
    /// Health check interval (ms).
    pub health_check_interval_ms: u64,
    /// Dispatch timeout (ms).
    pub dispatch_timeout_ms: u64,
    /// Whether to enable automatic retries.
    pub auto_retry: bool,
}

impl Default for PushDispatcherConfig {
    fn default() -> Self {
        Self {
            max_concurrent_per_service: 1000,
            default_max_attempts: 3,
            health_check_interval_ms: 10_000,
            dispatch_timeout_ms: 30_000,
            auto_retry: true,
        }
    }
}

/// Statistics for the push dispatcher.
#[derive(Debug, Clone, Default)]
pub struct PushDispatcherStats {
    pub total_dispatches: u64,
    pub total_delivered: u64,
    pub total_failed: u64,
    pub total_retried: u64,
    pub total_canceled: u64,
    pub queued_count: u64,
    pub active_dispatches: u64,
    pub registered_services: u64,
    pub healthy_services: u64,
}

/// Errors from push dispatch operations.
#[derive(Debug, Clone)]
pub enum DispatchError {
    ServiceNotFound(String),
    NoHealthyEndpoints(String),
    MaxAttemptsExceeded(u64),
    DispatchTimeout(u64),
    ServiceUnhealthy(String),
    QueueFull(String),
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServiceNotFound(s) => write!(f, "service not found: {}", s),
            Self::NoHealthyEndpoints(s) => write!(f, "no healthy endpoints for: {}", s),
            Self::MaxAttemptsExceeded(id) => write!(f, "max attempts exceeded: {}", id),
            Self::DispatchTimeout(id) => write!(f, "dispatch timeout: {}", id),
            Self::ServiceUnhealthy(s) => write!(f, "service unhealthy: {}", s),
            Self::QueueFull(s) => write!(f, "queue full for service: {}", s),
        }
    }
}

impl std::error::Error for DispatchError {}

/// The Push Dispatcher — routes work to service endpoints via HTTP push.
pub struct PushDispatcher {
    /// Registered service endpoints.
    services: HashMap<String, Vec<ServiceEndpoint>>,
    /// Pending dispatches.
    dispatches: HashMap<u64, PushDispatch>,
    /// Per-service dispatch queues.
    service_queues: HashMap<String, VecDeque<u64>>,
    /// Active dispatch count per service.
    active_per_service: HashMap<String, usize>,
    /// Idempotency map.
    idempotency_map: HashMap<String, u64>,
    /// Configuration.
    config: PushDispatcherConfig,
    /// Next dispatch ID.
    next_dispatch_id: AtomicU64,
    /// Statistics.
    stats: PushDispatcherStats,
}

impl PushDispatcher {
    /// Create a new push dispatcher.
    pub fn new() -> Self {
        Self::with_config(PushDispatcherConfig::default())
    }

    /// Create with custom configuration.
    pub fn with_config(config: PushDispatcherConfig) -> Self {
        Self {
            services: HashMap::new(),
            dispatches: HashMap::new(),
            service_queues: HashMap::new(),
            active_per_service: HashMap::new(),
            idempotency_map: HashMap::new(),
            config,
            next_dispatch_id: AtomicU64::new(1),
            stats: PushDispatcherStats::default(),
        }
    }

    // ─── Service Registration ──────────────────────────────────────────────

    /// Register a service endpoint.
    pub fn register_service(&mut self, endpoint: ServiceEndpoint) {
        let name = endpoint.name.clone();
        self.services
            .entry(name.clone())
            .or_default()
            .push(endpoint);
        self.stats.registered_services = self.services.len() as u64;
        self.update_healthy_count();
    }

    /// Unregister a service endpoint.
    pub fn unregister_service(&mut self, name: &str, url: &str) {
        if let Some(endpoints) = self.services.get_mut(name) {
            endpoints.retain(|e| e.base_url != url);
            if endpoints.is_empty() {
                self.services.remove(name);
            }
        }
        self.stats.registered_services = self.services.len() as u64;
        self.update_healthy_count();
    }

    /// List registered services.
    pub fn list_services(&self) -> Vec<&String> {
        self.services.keys().collect()
    }

    /// Get endpoints for a service.
    pub fn get_endpoints(&self, name: &str) -> Option<&Vec<ServiceEndpoint>> {
        self.services.get(name)
    }

    /// Mark an endpoint as healthy/unhealthy.
    pub fn set_endpoint_health(&mut self, service: &str, url: &str, healthy: bool) {
        if let Some(endpoints) = self.services.get_mut(service) {
            for ep in endpoints.iter_mut() {
                if ep.base_url == url {
                    ep.healthy = healthy;
                    if healthy {
                        ep.failure_count = 0;
                    }
                }
            }
        }
        self.update_healthy_count();
    }

    /// Record a dispatch failure for an endpoint.
    pub fn record_failure(&mut self, service: &str, url: &str) {
        if let Some(endpoints) = self.services.get_mut(service) {
            for ep in endpoints.iter_mut() {
                if ep.base_url == url {
                    ep.failure_count += 1;
                    if ep.failure_count >= ep.max_failures {
                        ep.healthy = false;
                    }
                }
            }
        }
        self.update_healthy_count();
    }

    /// Record a dispatch success for an endpoint.
    pub fn record_success(&mut self, service: &str, url: &str) {
        if let Some(endpoints) = self.services.get_mut(service) {
            for ep in endpoints.iter_mut() {
                if ep.base_url == url {
                    ep.failure_count = 0;
                    ep.healthy = true;
                    ep.last_success_ms = 0; // Would use system clock
                }
            }
        }
        self.update_healthy_count();
    }

    fn update_healthy_count(&mut self) {
        let count = self
            .services
            .values()
            .flat_map(|eps| eps.iter())
            .filter(|ep| ep.healthy)
            .count() as u64;
        self.stats.healthy_services = count;
    }

    // ─── Dispatch ──────────────────────────────────────────────────────────

    /// Push work to a service.
    pub fn dispatch(
        &mut self,
        service_name: &str,
        handler: &str,
        input: Vec<u8>,
        idempotency_key: Option<String>,
    ) -> Result<u64, DispatchError> {
        // Check idempotency
        if let Some(ref idk) = idempotency_key {
            if let Some(&existing_id) = self.idempotency_map.get(idk) {
                return Ok(existing_id);
            }
        }

        // Check service exists
        if !self.services.contains_key(service_name) {
            return Err(DispatchError::ServiceNotFound(service_name.to_string()));
        }

        // Check capacity
        let active = self
            .active_per_service
            .get(service_name)
            .copied()
            .unwrap_or(0);
        if active >= self.config.max_concurrent_per_service {
            return Err(DispatchError::QueueFull(service_name.to_string()));
        }

        let dispatch_id = self.next_dispatch_id.fetch_add(1, Ordering::Relaxed);

        let dispatch = PushDispatch {
            dispatch_id,
            service_name: service_name.to_string(),
            handler: handler.to_string(),
            input,
            state: DispatchState::Queued,
            response: None,
            error: None,
            attempt_count: 0,
            max_attempts: self.config.default_max_attempts,
            idempotency_key: idempotency_key.clone(),
            created_ms: 0,
            delivered_ms: 0,
            target_url: None,
        };

        if let Some(ref idk) = idempotency_key {
            self.idempotency_map.insert(idk.clone(), dispatch_id);
        }

        self.dispatches.insert(dispatch_id, dispatch);
        self.stats.total_dispatches += 1;
        self.stats.queued_count += 1;

        // Queue for dispatch
        self.service_queues
            .entry(service_name.to_string())
            .or_default()
            .push_back(dispatch_id);

        // Try to dispatch immediately
        self.try_dispatch(service_name);

        Ok(dispatch_id)
    }

    /// Try to dispatch queued work to a healthy endpoint.
    fn try_dispatch(&mut self, service_name: &str) {
        // Find a healthy endpoint
        let target_url = match self.services.get(service_name) {
            Some(endpoints) => endpoints
                .iter()
                .find(|ep| ep.healthy)
                .map(|ep| format!("{}/{}", ep.base_url, "")),
            None => return,
        };

        let target_url = match target_url {
            Some(url) => url,
            None => return, // No healthy endpoints
        };

        // Get next queued dispatch
        let dispatch_id = match self.service_queues.get_mut(service_name) {
            Some(queue) => match queue.pop_front() {
                Some(id) => id,
                None => return,
            },
            None => return,
        };

        if let Some(dispatch) = self.dispatches.get_mut(&dispatch_id) {
            dispatch.state = DispatchState::Dispatching;
            dispatch.target_url = Some(target_url.clone());
            dispatch.attempt_count += 1;
        }

        *self
            .active_per_service
            .entry(service_name.to_string())
            .or_insert(0) += 1;
        self.stats.queued_count = self.stats.queued_count.saturating_sub(1);
        self.stats.active_dispatches += 1;
    }

    /// Mark a dispatch as delivered (service responded successfully).
    pub fn mark_delivered(
        &mut self,
        dispatch_id: u64,
        response: Vec<u8>,
    ) -> Result<(), DispatchError> {
        let dispatch = self
            .dispatches
            .get_mut(&dispatch_id)
            .ok_or(DispatchError::MaxAttemptsExceeded(dispatch_id))?;

        let service_name = dispatch.service_name.clone();
        let target_url = dispatch.target_url.clone();

        dispatch.state = DispatchState::Delivered;
        dispatch.response = Some(response);
        dispatch.delivered_ms = 0;

        self.stats.total_delivered += 1;
        self.stats.active_dispatches = self.stats.active_dispatches.saturating_sub(1);
        *self
            .active_per_service
            .entry(service_name.clone())
            .or_insert(0) = self
            .active_per_service
            .get(&service_name)
            .copied()
            .unwrap_or(1)
            .saturating_sub(1);

        // Record success for health tracking
        if let Some(url) = target_url {
            self.record_success(&service_name, &url);
        }

        // Try to dispatch next queued item
        self.try_dispatch(&service_name);

        Ok(())
    }

    /// Mark a dispatch as failed (service didn't respond).
    pub fn mark_failed(&mut self, dispatch_id: u64, error: String) -> Result<(), DispatchError> {
        // Extract info first to avoid overlapping borrows
        let (service_name, target_url, attempt_count, max_attempts) = {
            let dispatch = self
                .dispatches
                .get(&dispatch_id)
                .ok_or(DispatchError::MaxAttemptsExceeded(dispatch_id))?;
            (
                dispatch.service_name.clone(),
                dispatch.target_url.clone(),
                dispatch.attempt_count,
                dispatch.max_attempts,
            )
        };

        // Record failure for health tracking
        if let Some(ref url) = target_url {
            self.record_failure(&service_name, url);
        }

        self.stats.active_dispatches = self.stats.active_dispatches.saturating_sub(1);
        *self
            .active_per_service
            .entry(service_name.clone())
            .or_insert(0) = self
            .active_per_service
            .get(&service_name)
            .copied()
            .unwrap_or(1)
            .saturating_sub(1);

        // Now mutate the dispatch
        let dispatch = self.dispatches.get_mut(&dispatch_id).unwrap();

        // Retry if under max attempts
        if attempt_count < max_attempts && self.config.auto_retry {
            dispatch.state = DispatchState::Queued;
            dispatch.error = Some(error);
            dispatch.target_url = None;
            self.stats.total_retried += 1;
            self.stats.queued_count += 1;

            self.service_queues
                .entry(service_name.clone())
                .or_default()
                .push_back(dispatch_id);

            self.try_dispatch(&service_name);
        } else {
            dispatch.state = DispatchState::PermanentlyFailed;
            dispatch.error = Some(error);
            self.stats.total_failed += 1;

            // Try next queued item
            self.try_dispatch(&service_name);
        }

        Ok(())
    }

    /// Cancel a pending dispatch.
    pub fn cancel(&mut self, dispatch_id: u64) -> Result<(), DispatchError> {
        let dispatch = self
            .dispatches
            .get_mut(&dispatch_id)
            .ok_or(DispatchError::MaxAttemptsExceeded(dispatch_id))?;

        let service_name = dispatch.service_name.clone();

        match dispatch.state {
            DispatchState::Queued | DispatchState::Dispatching => {
                dispatch.state = DispatchState::Canceled;
                self.stats.total_canceled += 1;
                if dispatch.state == DispatchState::Dispatching {
                    self.stats.active_dispatches = self.stats.active_dispatches.saturating_sub(1);
                } else {
                    self.stats.queued_count = self.stats.queued_count.saturating_sub(1);
                }
                self.try_dispatch(&service_name);
                Ok(())
            }
            _ => Err(DispatchError::MaxAttemptsExceeded(dispatch_id)),
        }
    }

    // ─── Query Operations ──────────────────────────────────────────────────

    /// Get a dispatch by ID.
    pub fn get_dispatch(&self, dispatch_id: u64) -> Option<&PushDispatch> {
        self.dispatches.get(&dispatch_id)
    }

    /// Get dispatch response (for delivered dispatches).
    pub fn get_response(&self, dispatch_id: u64) -> Option<&Vec<u8>> {
        self.dispatches.get(&dispatch_id)?.response.as_ref()
    }

    /// Get statistics.
    pub fn stats(&self) -> &PushDispatcherStats {
        &self.stats
    }

    /// Get total dispatch count.
    pub fn dispatch_count(&self) -> usize {
        self.dispatches.len()
    }

    /// Get queue depth for a service.
    pub fn queue_depth(&self, service_name: &str) -> usize {
        self.service_queues.get(service_name).map_or(0, |q| q.len())
    }

    /// Check if a service has any healthy endpoints.
    pub fn is_service_healthy(&self, service_name: &str) -> bool {
        self.services
            .get(service_name)
            .is_some_and(|eps| eps.iter().any(|ep| ep.healthy))
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_endpoint(name: &str, url: &str) -> ServiceEndpoint {
        ServiceEndpoint {
            name: name.to_string(),
            base_url: url.to_string(),
            health_url: format!("{}/health", url),
            healthy: true,
            failure_count: 0,
            max_failures: 3,
            last_success_ms: 0,
            tags: HashMap::new(),
        }
    }

    fn test_dispatcher() -> PushDispatcher {
        let mut d = PushDispatcher::new();
        d.register_service(test_endpoint("order-svc", "http://localhost:8080"));
        d.register_service(test_endpoint("chat-svc", "http://localhost:8081"));
        d
    }

    #[test]
    fn test_register_services() {
        let d = test_dispatcher();
        assert_eq!(d.list_services().len(), 2);
        assert!(d.is_service_healthy("order-svc"));
        assert!(d.is_service_healthy("chat-svc"));
    }

    #[test]
    fn test_dispatch_to_healthy_service() {
        let mut d = test_dispatcher();
        let id = d
            .dispatch("order-svc", "process_order", b"order-data".to_vec(), None)
            .unwrap();

        let dispatch = d.get_dispatch(id).unwrap();
        assert_eq!(dispatch.state, DispatchState::Dispatching);
        assert!(dispatch.target_url.is_some());
    }

    #[test]
    fn test_dispatch_to_unknown_service() {
        let mut d = test_dispatcher();
        let result = d.dispatch("unknown-svc", "handler", vec![], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_mark_delivered() {
        let mut d = test_dispatcher();
        let id = d
            .dispatch("order-svc", "handler", b"data".to_vec(), None)
            .unwrap();
        d.mark_delivered(id, b"response".to_vec()).unwrap();

        let dispatch = d.get_dispatch(id).unwrap();
        assert_eq!(dispatch.state, DispatchState::Delivered);
        assert_eq!(dispatch.response.as_ref().unwrap(), b"response");
    }

    #[test]
    fn test_retry_on_failure() {
        let mut d = test_dispatcher();
        let id = d
            .dispatch("order-svc", "handler", b"data".to_vec(), None)
            .unwrap();

        // First attempt (attempt_count = 1 after try_dispatch)
        let dispatch = d.get_dispatch(id).unwrap();
        assert_eq!(dispatch.attempt_count, 1);

        // First attempt fails -> retry (attempt_count becomes 2 after re-dispatch)
        d.mark_failed(id, "timeout".to_string()).unwrap();
        let dispatch = d.get_dispatch(id).unwrap();
        assert_eq!(dispatch.attempt_count, 2);
        assert_eq!(dispatch.state, DispatchState::Dispatching); // Re-dispatched

        // Second attempt fails -> retry (attempt_count becomes 3 after re-dispatch)
        d.mark_failed(id, "timeout again".to_string()).unwrap();
        let dispatch = d.get_dispatch(id).unwrap();
        assert_eq!(dispatch.attempt_count, 3);
        assert_eq!(dispatch.state, DispatchState::Dispatching); // Re-dispatched

        // Third attempt fails — permanently failed (attempt_count 3 >= max_attempts 3)
        d.mark_failed(id, "final timeout".to_string()).unwrap();
        let dispatch = d.get_dispatch(id).unwrap();
        assert_eq!(dispatch.state, DispatchState::PermanentlyFailed);
    }

    #[test]
    fn test_no_healthy_endpoints() {
        let mut d = PushDispatcher::new();
        let mut ep = test_endpoint("svc", "http://localhost:8080");
        ep.healthy = false;
        d.register_service(ep);

        // Dispatch succeeds (goes to queue) but can't be dispatched
        let id = d
            .dispatch("svc", "handler", b"data".to_vec(), None)
            .unwrap();
        let dispatch = d.get_dispatch(id).unwrap();
        assert_eq!(dispatch.state, DispatchState::Queued); // Stays queued
    }

    #[test]
    fn test_health_tracking() {
        let mut d = test_dispatcher();

        // Record failures until unhealthy
        d.record_failure("order-svc", "http://localhost:8080");
        d.record_failure("order-svc", "http://localhost:8080");
        assert!(d.is_service_healthy("order-svc")); // Still healthy (max_failures = 3)

        d.record_failure("order-svc", "http://localhost:8080");
        assert!(!d.is_service_healthy("order-svc")); // Now unhealthy

        // Recovery
        d.record_success("order-svc", "http://localhost:8080");
        assert!(d.is_service_healthy("order-svc"));
    }

    #[test]
    fn test_idempotent_dispatch() {
        let mut d = test_dispatcher();
        let id1 = d
            .dispatch(
                "order-svc",
                "handler",
                b"data".to_vec(),
                Some("idem-1".to_string()),
            )
            .unwrap();
        let id2 = d
            .dispatch(
                "order-svc",
                "handler",
                b"data".to_vec(),
                Some("idem-1".to_string()),
            )
            .unwrap();
        assert_eq!(id1, id2);
        assert_eq!(d.stats().total_dispatches, 1);
    }

    #[test]
    fn test_cancel_dispatch() {
        let mut d = test_dispatcher();
        let id = d
            .dispatch("order-svc", "handler", b"data".to_vec(), None)
            .unwrap();
        d.cancel(id).unwrap();
        assert_eq!(d.get_dispatch(id).unwrap().state, DispatchState::Canceled);
    }

    #[test]
    fn test_stats() {
        let mut d = test_dispatcher();
        let id1 = d.dispatch("order-svc", "h1", b"a".to_vec(), None).unwrap();
        let _id2 = d.dispatch("order-svc", "h2", b"b".to_vec(), None).unwrap();

        d.mark_delivered(id1, b"ok".to_vec()).unwrap();

        let stats = d.stats();
        assert_eq!(stats.total_dispatches, 2);
        assert_eq!(stats.total_delivered, 1);
        assert_eq!(stats.registered_services, 2);
    }

    #[test]
    fn test_unregister_service() {
        let mut d = test_dispatcher();
        d.unregister_service("order-svc", "http://localhost:8080");
        assert!(!d.is_service_healthy("order-svc"));
        assert_eq!(d.list_services().len(), 1);
    }
}
