//! Nexus — cross-service asynchronous operations.
//! Full lifecycle: Scheduled → Started → Completed/Failed/Canceled/TimedOut.
//! Supports callback delivery, timeout scheduling, retry with attempt tracking,
//! and persistent endpoint registry.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NexusOperationState {
    Scheduled = 0,
    Started = 1,
    Completed = 2,
    Failed = 3,
    Canceled = 4,
    TimedOut = 5,
}

/// Callback result delivered by the Nexus handler.
#[derive(Debug, Clone)]
pub struct CallbackResult {
    pub operation_id: u64,
    pub success: bool,
    pub payload: Option<Vec<u8>>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NexusOperation {
    pub operation_id: u64,
    pub service_name: String,
    pub operation_name: String,
    pub workflow_key: u64,
    pub state: NexusOperationState,
    pub input: Option<Vec<u8>>,
    pub result: Option<Vec<u8>>,
    pub callback_url: Option<String>,
    /// Token for the external handler to use when completing the operation.
    pub operation_token: Option<String>,
    /// Number of attempts made so far.
    pub attempt: u32,
    /// Maximum number of attempts before giving up.
    pub max_attempts: u32,
    /// Timeout in milliseconds (0 = no timeout).
    pub timeout_ms: u64,
    /// Timestamp when the operation was started (epoch ms).
    pub started_at_ms: u64,
    /// Routing key for shard affinity.
    pub routing_key: Option<String>,
    /// Error message if failed.
    pub error_message: Option<String>,
}

/// Endpoint registry entry with metadata.
#[derive(Debug, Clone)]
pub struct NexusEndpoint {
    pub name: String,
    pub url: String,
    pub description: Option<String>,
    /// Maximum operations allowed concurrently.
    pub max_concurrent: u32,
    /// Current active operations count.
    pub active_operations: u32,
}

pub struct NexusManager {
    operations: Mutex<HashMap<u64, NexusOperation>>,
    endpoints: Mutex<HashMap<String, NexusEndpoint>>,
    /// Pending callbacks waiting to be delivered.
    pending_callbacks: Mutex<Vec<CallbackResult>>,
    next_id: AtomicU64,
    /// Counter for generating unique operation tokens.
    next_token: AtomicU64,
}

impl NexusManager {
    pub fn new() -> Self {
        Self {
            operations: Mutex::new(HashMap::new()),
            endpoints: Mutex::new(HashMap::new()),
            pending_callbacks: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(1),
            next_token: AtomicU64::new(1),
        }
    }

    /// Register a Nexus endpoint with metadata.
    pub fn register_service(&self, name: &str, endpoint: &str) {
        self.endpoints.lock().unwrap().insert(
            name.to_string(),
            NexusEndpoint {
                name: name.to_string(),
                url: endpoint.to_string(),
                description: None,
                max_concurrent: 100,
                active_operations: 0,
            },
        );
    }

    /// Register an endpoint with full configuration.
    pub fn register_endpoint(
        &self,
        name: &str,
        url: &str,
        description: &str,
        max_concurrent: u32,
    ) -> bool {
        self.endpoints.lock().unwrap().insert(
            name.to_string(),
            NexusEndpoint {
                name: name.to_string(),
                url: url.to_string(),
                description: Some(description.to_string()),
                max_concurrent,
                active_operations: 0,
            },
        );
        true
    }

    /// Unregister an endpoint. Returns false if it has active operations.
    pub fn unregister_endpoint(&self, name: &str) -> bool {
        let mut endpoints = self.endpoints.lock().unwrap();
        if let Some(ep) = endpoints.get(name) {
            if ep.active_operations > 0 {
                return false;
            }
            endpoints.remove(name);
            true
        } else {
            false
        }
    }

    /// Start a Nexus operation. Returns the operation ID.
    /// Validates the service exists and has capacity.
    pub fn start_operation(
        &self,
        service: &str,
        operation: &str,
        workflow_key: u64,
        input: Option<Vec<u8>>,
        callback: Option<String>,
    ) -> Option<u64> {
        self.start_operation_with_config(service, operation, workflow_key, input, callback, 0, 3)
    }

    /// Start a Nexus operation with full configuration (timeout, max_attempts).
    pub fn start_operation_with_config(
        &self,
        service: &str,
        operation: &str,
        workflow_key: u64,
        input: Option<Vec<u8>>,
        callback: Option<String>,
        timeout_ms: u64,
        max_attempts: u32,
    ) -> Option<u64> {
        // Validate service exists and has capacity
        {
            let mut endpoints = self.endpoints.lock().unwrap();
            let ep = endpoints.get_mut(service)?;
            if ep.active_operations >= ep.max_concurrent {
                return None;
            }
            ep.active_operations += 1;
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let token_id = self.next_token.fetch_add(1, Ordering::Relaxed);
        let token = format!("nexus-token-{}", token_id);

        self.operations.lock().unwrap().insert(
            id,
            NexusOperation {
                operation_id: id,
                service_name: service.to_string(),
                operation_name: operation.to_string(),
                workflow_key,
                state: NexusOperationState::Scheduled,
                input,
                result: None,
                callback_url: callback,
                operation_token: Some(token),
                attempt: 1,
                max_attempts,
                timeout_ms,
                started_at_ms: 0,
                routing_key: None,
                error_message: None,
            },
        );
        Some(id)
    }

    /// Transition operation from Scheduled to Started (handler acknowledged).
    pub fn mark_started(&self, op_id: u64, operation_token: Option<String>) -> bool {
        let mut ops = self.operations.lock().unwrap();
        if let Some(op) = ops.get_mut(&op_id) {
            if op.state != NexusOperationState::Scheduled {
                return false;
            }
            op.state = NexusOperationState::Started;
            if let Some(token) = operation_token {
                op.operation_token = Some(token);
            }
            true
        } else {
            false
        }
    }

    /// Complete an operation successfully with a result.
    pub fn complete_operation(&self, op_id: u64, result: Vec<u8>) -> bool {
        let mut ops = self.operations.lock().unwrap();
        if let Some(op) = ops.get_mut(&op_id) {
            if op.state != NexusOperationState::Started
                && op.state != NexusOperationState::Scheduled
            {
                return false;
            }
            op.state = NexusOperationState::Completed;
            op.result = Some(result);
            let service = op.service_name.clone();
            drop(ops);
            self.decrement_active(&service);
            true
        } else {
            false
        }
    }

    /// Fail an operation. If attempts remain, it can be retried.
    pub fn fail_operation(&self, op_id: u64) -> bool {
        self.fail_operation_with_error(op_id, None)
    }

    /// Fail an operation with an error message.
    pub fn fail_operation_with_error(&self, op_id: u64, error: Option<String>) -> bool {
        let mut ops = self.operations.lock().unwrap();
        if let Some(op) = ops.get_mut(&op_id) {
            if op.state == NexusOperationState::Completed
                || op.state == NexusOperationState::Canceled
            {
                return false;
            }
            op.state = NexusOperationState::Failed;
            op.error_message = error;
            let service = op.service_name.clone();
            drop(ops);
            self.decrement_active(&service);
            true
        } else {
            false
        }
    }

    /// Cancel an operation (transitions to Canceled state).
    pub fn cancel_operation(&self, op_id: u64) -> bool {
        let mut ops = self.operations.lock().unwrap();
        if let Some(op) = ops.get_mut(&op_id) {
            if op.state == NexusOperationState::Completed
                || op.state == NexusOperationState::Canceled
            {
                return false;
            }
            op.state = NexusOperationState::Canceled;
            let service = op.service_name.clone();
            drop(ops);
            self.decrement_active(&service);
            true
        } else {
            false
        }
    }

    /// Mark an operation as timed out.
    pub fn timeout_operation(&self, op_id: u64) -> bool {
        let mut ops = self.operations.lock().unwrap();
        if let Some(op) = ops.get_mut(&op_id) {
            if op.state == NexusOperationState::Completed
                || op.state == NexusOperationState::Canceled
            {
                return false;
            }
            op.state = NexusOperationState::TimedOut;
            let service = op.service_name.clone();
            drop(ops);
            self.decrement_active(&service);
            true
        } else {
            false
        }
    }

    /// Retry a failed/timed-out operation. Increments attempt counter.
    /// Returns false if max attempts exceeded or operation not in retryable state.
    pub fn retry_operation(&self, op_id: u64) -> bool {
        let mut ops = self.operations.lock().unwrap();
        if let Some(op) = ops.get_mut(&op_id) {
            if op.state != NexusOperationState::Failed && op.state != NexusOperationState::TimedOut
            {
                return false;
            }
            if op.attempt >= op.max_attempts {
                return false;
            }
            op.attempt += 1;
            op.state = NexusOperationState::Scheduled;
            op.result = None;
            op.error_message = None;
            let service = op.service_name.clone();
            // Re-increment endpoint active count
            drop(ops);
            self.increment_active(&service);
            true
        } else {
            false
        }
    }

    /// Deliver a callback result from the Nexus handler.
    /// Queues the result for processing.
    pub fn deliver_callback(&self, result: CallbackResult) -> bool {
        let mut ops = self.operations.lock().unwrap();
        if let Some(op) = ops.get_mut(&result.operation_id) {
            if op.state != NexusOperationState::Started {
                return false;
            }
            if result.success {
                op.state = NexusOperationState::Completed;
                op.result = result.payload;
            } else {
                op.state = NexusOperationState::Failed;
                op.error_message = result.error_message;
            }
            let service = op.service_name.clone();
            drop(ops);
            self.decrement_active(&service);
            true
        } else {
            false
        }
    }

    /// Queue a callback for later delivery (async callback path).
    pub fn queue_callback(&self, result: CallbackResult) {
        self.pending_callbacks.lock().unwrap().push(result);
    }

    /// Drain pending callbacks for processing.
    pub fn drain_callbacks(&self) -> Vec<CallbackResult> {
        let mut cbs = self.pending_callbacks.lock().unwrap();
        std::mem::take(&mut *cbs)
    }

    /// Check for timed-out operations. Returns IDs of operations that have timed out.
    pub fn check_timeouts(&self, current_time_ms: u64) -> Vec<u64> {
        let mut timed_out = Vec::new();
        let mut ops = self.operations.lock().unwrap();
        for op in ops.values_mut() {
            if op.timeout_ms > 0
                && op.started_at_ms > 0
                && op.state == NexusOperationState::Started
                && current_time_ms.saturating_sub(op.started_at_ms) > op.timeout_ms
            {
                op.state = NexusOperationState::TimedOut;
                timed_out.push(op.operation_id);
            }
        }
        timed_out
    }

    /// Set routing key for shard affinity.
    pub fn set_routing_key(&self, op_id: u64, routing_key: &str) -> bool {
        let mut ops = self.operations.lock().unwrap();
        if let Some(op) = ops.get_mut(&op_id) {
            op.routing_key = Some(routing_key.to_string());
            true
        } else {
            false
        }
    }

    pub fn get_operation(&self, op_id: u64) -> Option<NexusOperation> {
        self.operations.lock().unwrap().get(&op_id).cloned()
    }

    pub fn operation_count(&self) -> usize {
        self.operations.lock().unwrap().len()
    }
    pub fn service_count(&self) -> usize {
        self.endpoints.lock().unwrap().len()
    }

    /// Count operations in a specific state.
    pub fn count_by_state(&self, state: NexusOperationState) -> usize {
        self.operations
            .lock()
            .unwrap()
            .values()
            .filter(|op| op.state == state)
            .count()
    }

    /// List all operation IDs.
    pub fn list_operation_ids(&self) -> Vec<u64> {
        self.operations.lock().unwrap().keys().copied().collect()
    }

    /// Get endpoint info.
    pub fn get_endpoint(&self, name: &str) -> Option<NexusEndpoint> {
        self.endpoints.lock().unwrap().get(name).cloned()
    }

    /// List all endpoint names.
    pub fn list_endpoints(&self) -> Vec<String> {
        self.endpoints.lock().unwrap().keys().cloned().collect()
    }

    fn decrement_active(&self, service: &str) {
        let mut endpoints = self.endpoints.lock().unwrap();
        if let Some(ep) = endpoints.get_mut(service) {
            ep.active_operations = ep.active_operations.saturating_sub(1);
        }
    }

    fn increment_active(&self, service: &str) {
        let mut endpoints = self.endpoints.lock().unwrap();
        if let Some(ep) = endpoints.get_mut(service) {
            ep.active_operations += 1;
        }
    }
}

impl Default for NexusManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nexus_lifecycle() {
        let mgr = NexusManager::new();
        mgr.register_service("payments", "http://payments:8080");
        let op_id = mgr
            .start_operation("payments", "charge", 42, Some(vec![1]), None)
            .unwrap();
        assert_eq!(
            mgr.get_operation(op_id).unwrap().state,
            NexusOperationState::Scheduled
        );

        // Transition to Started
        assert!(mgr.mark_started(op_id, None));
        assert_eq!(
            mgr.get_operation(op_id).unwrap().state,
            NexusOperationState::Started
        );

        // Complete
        assert!(mgr.complete_operation(op_id, vec![2, 3]));
        assert_eq!(
            mgr.get_operation(op_id).unwrap().state,
            NexusOperationState::Completed
        );
    }

    #[test]
    fn test_nexus_cancel() {
        let mgr = NexusManager::new();
        mgr.register_service("svc", "http://svc:8080");
        let op_id = mgr.start_operation("svc", "op", 1, None, None).unwrap();
        assert!(mgr.cancel_operation(op_id));
        assert_eq!(
            mgr.get_operation(op_id).unwrap().state,
            NexusOperationState::Canceled
        );
        // Can't cancel again
        assert!(!mgr.cancel_operation(op_id));
    }

    #[test]
    fn test_nexus_timeout() {
        let mgr = NexusManager::new();
        mgr.register_service("svc", "http://svc:8080");
        let op_id = mgr
            .start_operation_with_config("svc", "op", 1, None, None, 1000, 1)
            .unwrap();
        assert!(mgr.mark_started(op_id, None));
        assert!(mgr.timeout_operation(op_id));
        assert_eq!(
            mgr.get_operation(op_id).unwrap().state,
            NexusOperationState::TimedOut
        );
    }

    #[test]
    fn test_nexus_retry() {
        let mgr = NexusManager::new();
        mgr.register_service("svc", "http://svc:8080");
        let op_id = mgr
            .start_operation_with_config("svc", "op", 1, None, None, 0, 3)
            .unwrap();
        assert!(mgr.mark_started(op_id, None));
        assert!(mgr.fail_operation(op_id));
        assert_eq!(
            mgr.get_operation(op_id).unwrap().state,
            NexusOperationState::Failed
        );
        assert_eq!(mgr.get_operation(op_id).unwrap().attempt, 1);

        // Retry
        assert!(mgr.retry_operation(op_id));
        assert_eq!(
            mgr.get_operation(op_id).unwrap().state,
            NexusOperationState::Scheduled
        );
        assert_eq!(mgr.get_operation(op_id).unwrap().attempt, 2);
    }

    #[test]
    fn test_nexus_max_retries() {
        let mgr = NexusManager::new();
        mgr.register_service("svc", "http://svc:8080");
        let op_id = mgr
            .start_operation_with_config("svc", "op", 1, None, None, 0, 2)
            .unwrap();
        assert!(mgr.mark_started(op_id, None));

        // Fail + retry (attempt 1 → 2)
        assert!(mgr.fail_operation(op_id));
        assert!(mgr.retry_operation(op_id));
        assert_eq!(mgr.get_operation(op_id).unwrap().attempt, 2);

        // Fail + retry (attempt 2 → 3) — should fail because max_attempts = 2
        assert!(mgr.mark_started(op_id, None));
        assert!(mgr.fail_operation(op_id));
        assert!(!mgr.retry_operation(op_id)); // max exceeded
    }

    #[test]
    fn test_callback_delivery() {
        let mgr = NexusManager::new();
        mgr.register_service("svc", "http://svc:8080");
        let op_id = mgr
            .start_operation("svc", "op", 1, None, Some("http://callback:9090".into()))
            .unwrap();
        assert!(mgr.mark_started(op_id, None));

        // Deliver callback
        let result = CallbackResult {
            operation_id: op_id,
            success: true,
            payload: Some(vec![42]),
            error_message: None,
        };
        assert!(mgr.deliver_callback(result));
        assert_eq!(
            mgr.get_operation(op_id).unwrap().state,
            NexusOperationState::Completed
        );
        assert_eq!(mgr.get_operation(op_id).unwrap().result, Some(vec![42]));
    }

    #[test]
    fn test_callback_failure() {
        let mgr = NexusManager::new();
        mgr.register_service("svc", "http://svc:8080");
        let op_id = mgr.start_operation("svc", "op", 1, None, None).unwrap();
        assert!(mgr.mark_started(op_id, None));

        let result = CallbackResult {
            operation_id: op_id,
            success: false,
            payload: None,
            error_message: Some("timeout".into()),
        };
        assert!(mgr.deliver_callback(result));
        assert_eq!(
            mgr.get_operation(op_id).unwrap().state,
            NexusOperationState::Failed
        );
        assert_eq!(
            mgr.get_operation(op_id).unwrap().error_message,
            Some("timeout".into())
        );
    }

    #[test]
    fn test_unknown_service() {
        let mgr = NexusManager::new();
        assert!(mgr
            .start_operation("unknown", "op", 1, None, None)
            .is_none());
    }

    #[test]
    fn test_endpoint_registry() {
        let mgr = NexusManager::new();
        mgr.register_endpoint("payments", "http://payments:8080", "Payment service", 50);
        let ep = mgr.get_endpoint("payments").unwrap();
        assert_eq!(ep.url, "http://payments:8080");
        assert_eq!(ep.max_concurrent, 50);
        assert_eq!(mgr.list_endpoints().len(), 1);
    }

    #[test]
    fn test_count_by_state() {
        let mgr = NexusManager::new();
        mgr.register_service("svc", "http://svc:8080");
        let op1 = mgr.start_operation("svc", "op", 1, None, None).unwrap();
        let _op2 = mgr.start_operation("svc", "op", 2, None, None).unwrap();
        assert!(mgr.mark_started(op1, None));

        assert_eq!(mgr.count_by_state(NexusOperationState::Scheduled), 1);
        assert_eq!(mgr.count_by_state(NexusOperationState::Started), 1);
        assert_eq!(mgr.count_by_state(NexusOperationState::Completed), 0);
    }

    #[test]
    fn test_routing_key() {
        let mgr = NexusManager::new();
        mgr.register_service("svc", "http://svc:8080");
        let op_id = mgr.start_operation("svc", "op", 1, None, None).unwrap();
        assert!(mgr.set_routing_key(op_id, "shard-42"));
        assert_eq!(
            mgr.get_operation(op_id).unwrap().routing_key,
            Some("shard-42".into())
        );
    }
}
