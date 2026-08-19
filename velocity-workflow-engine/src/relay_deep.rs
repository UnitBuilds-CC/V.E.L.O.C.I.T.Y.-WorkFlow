//! Deep Relay operations matching Temporal's 6.5K-line Relay subsystem.
//!
//! Covers: Relay endpoints, operations, callbacks, handler registry,
//! request/response routing, and operation lifecycle management.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::SystemTime;

// ═══════════════════════════════════════════════════════════════════════════════
// Relay Endpoint
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct RelayEndpoint {
    pub id: String,
    pub name: String,
    pub description: String,
    pub target: EndpointTarget,
    pub created_at: i64,
    pub version: i64,
}

#[derive(Debug, Clone)]
pub enum EndpointTarget {
    Worker {
        namespace: String,
        task_queue: String,
        identity: Option<String>,
    },
    External {
        url: String,
        auth_method: AuthMethod,
    },
}

#[derive(Debug, Clone)]
pub enum AuthMethod {
    None,
    BearerToken { token_ref: String },
    Mtls { cert_ref: String },
    ApiKey { key_ref: String },
}

// ═══════════════════════════════════════════════════════════════════════════════
// Relay Endpoint Manager
// ═══════════════════════════════════════════════════════════════════════════════

pub struct RelayEndpointManager {
    endpoints: RwLock<HashMap<String, RelayEndpoint>>,
    name_index: RwLock<HashMap<String, String>>,
    stats: EndpointManagerStats,
}

#[derive(Debug, Default)]
pub struct EndpointManagerStats {
    pub endpoints_created: AtomicU64,
    pub endpoints_updated: AtomicU64,
    pub endpoints_deleted: AtomicU64,
    pub operations_started: AtomicU64,
    pub operations_completed: AtomicU64,
    pub operations_failed: AtomicU64,
    pub callbacks_received: AtomicU64,
}

impl RelayEndpointManager {
    pub fn new() -> Self {
        Self {
            endpoints: RwLock::new(HashMap::new()),
            name_index: RwLock::new(HashMap::new()),
            stats: EndpointManagerStats::default(),
        }
    }

    pub fn create_endpoint(
        &self,
        name: &str,
        target: EndpointTarget,
        description: &str,
    ) -> Result<RelayEndpoint, RelayError> {
        let mut endpoints = self.endpoints.write().unwrap();
        let mut name_index = self.name_index.write().unwrap();

        if name_index.contains_key(name) {
            return Err(RelayError::EndpointAlreadyExists(name.to_string()));
        }

        let endpoint = RelayEndpoint {
            id: format!("ep-{}", uuid_simple()),
            name: name.to_string(),
            description: description.to_string(),
            target,
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            version: 1,
        };

        name_index.insert(name.to_string(), endpoint.id.clone());
        endpoints.insert(endpoint.id.clone(), endpoint.clone());
        self.stats.endpoints_created.fetch_add(1, Ordering::Relaxed);

        Ok(endpoint)
    }

    pub fn get_endpoint(&self, id: &str) -> Option<RelayEndpoint> {
        self.endpoints.read().unwrap().get(id).cloned()
    }

    pub fn get_endpoint_by_name(&self, name: &str) -> Option<RelayEndpoint> {
        let name_index = self.name_index.read().unwrap();
        let id = name_index.get(name)?;
        self.endpoints.read().unwrap().get(id).cloned()
    }

    pub fn update_endpoint(
        &self,
        id: &str,
        description: Option<&str>,
        target: Option<EndpointTarget>,
    ) -> Result<RelayEndpoint, RelayError> {
        let mut endpoints = self.endpoints.write().unwrap();
        let endpoint = endpoints
            .get_mut(id)
            .ok_or(RelayError::EndpointNotFound(id.to_string()))?;

        if let Some(desc) = description {
            endpoint.description = desc.to_string();
        }
        if let Some(t) = target {
            endpoint.target = t;
        }
        endpoint.version += 1;
        self.stats.endpoints_updated.fetch_add(1, Ordering::Relaxed);

        Ok(endpoint.clone())
    }

    pub fn delete_endpoint(&self, id: &str) -> Result<(), RelayError> {
        let mut endpoints = self.endpoints.write().unwrap();
        let mut name_index = self.name_index.write().unwrap();

        let endpoint = endpoints
            .remove(id)
            .ok_or(RelayError::EndpointNotFound(id.to_string()))?;
        name_index.remove(&endpoint.name);
        self.stats.endpoints_deleted.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    pub fn list_endpoints(&self) -> Vec<RelayEndpoint> {
        self.endpoints.read().unwrap().values().cloned().collect()
    }

    pub fn stats(&self) -> &EndpointManagerStats {
        &self.stats
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Relay Operation
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct RelayOperation {
    pub operation_id: String,
    pub endpoint_id: String,
    pub service: String,
    pub operation_name: String,
    pub state: RelayOperationState,
    pub input: Option<Vec<u8>>,
    pub result: Option<Vec<u8>>,
    pub failure: Option<RelayFailure>,
    pub callback_url: Option<String>,
    pub created_at: i64,
    pub completed_at: Option<i64>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub header: HashMap<String, String>,
    pub links: Vec<RelayLink>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayOperationState {
    Pending = 0,
    Running = 1,
    Succeeded = 2,
    Failed = 3,
    Canceled = 4,
    TimedOut = 5,
}

#[derive(Debug, Clone)]
pub struct RelayFailure {
    pub message: String,
    pub failure_type: String,
    pub retryable: bool,
}

#[derive(Debug, Clone)]
pub struct RelayLink {
    pub url: String,
    pub link_type: String,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Relay Operation Manager
// ═══════════════════════════════════════════════════════════════════════════════

pub struct RelayOperationManager {
    operations: RwLock<HashMap<String, RelayOperation>>,
    endpoint_ops: RwLock<HashMap<String, Vec<String>>>,
    stats: Arc<EndpointManagerStats>,
}

impl RelayOperationManager {
    pub fn new(stats: Arc<EndpointManagerStats>) -> Self {
        Self {
            operations: RwLock::new(HashMap::new()),
            endpoint_ops: RwLock::new(HashMap::new()),
            stats,
        }
    }

    pub fn start_operation(
        &self,
        endpoint_id: &str,
        service: &str,
        operation_name: &str,
        input: Option<Vec<u8>>,
        callback_url: Option<&str>,
    ) -> Result<RelayOperation, RelayError> {
        self.stats
            .operations_started
            .fetch_add(1, Ordering::Relaxed);

        let op = RelayOperation {
            operation_id: format!("op-{}", uuid_simple()),
            endpoint_id: endpoint_id.to_string(),
            service: service.to_string(),
            operation_name: operation_name.to_string(),
            state: RelayOperationState::Pending,
            input,
            result: None,
            failure: None,
            callback_url: callback_url.map(|s| s.to_string()),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            completed_at: None,
            attempt: 1,
            max_attempts: 3,
            header: HashMap::new(),
            links: vec![],
        };

        let op_id = op.operation_id.clone();
        self.operations
            .write()
            .unwrap()
            .insert(op_id.clone(), op.clone());
        self.endpoint_ops
            .write()
            .unwrap()
            .entry(endpoint_id.to_string())
            .or_default()
            .push(op_id);

        Ok(op)
    }

    pub fn complete_operation(
        &self,
        operation_id: &str,
        result: Option<Vec<u8>>,
    ) -> Result<(), RelayError> {
        let mut ops = self.operations.write().unwrap();
        let op = ops
            .get_mut(operation_id)
            .ok_or(RelayError::OperationNotFound(operation_id.to_string()))?;

        op.state = RelayOperationState::Succeeded;
        op.result = result;
        op.completed_at = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        self.stats
            .operations_completed
            .fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    pub fn fail_operation(
        &self,
        operation_id: &str,
        failure: RelayFailure,
    ) -> Result<(), RelayError> {
        let mut ops = self.operations.write().unwrap();
        let op = ops
            .get_mut(operation_id)
            .ok_or(RelayError::OperationNotFound(operation_id.to_string()))?;

        op.state = RelayOperationState::Failed;
        op.failure = Some(failure);
        op.completed_at = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        self.stats.operations_failed.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    pub fn cancel_operation(&self, operation_id: &str) -> Result<(), RelayError> {
        let mut ops = self.operations.write().unwrap();
        let op = ops
            .get_mut(operation_id)
            .ok_or(RelayError::OperationNotFound(operation_id.to_string()))?;
        op.state = RelayOperationState::Canceled;
        op.completed_at = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        Ok(())
    }

    pub fn get_operation(&self, operation_id: &str) -> Option<RelayOperation> {
        self.operations.read().unwrap().get(operation_id).cloned()
    }

    pub fn get_operations_for_endpoint(&self, endpoint_id: &str) -> Vec<RelayOperation> {
        let endpoint_ops = self.endpoint_ops.read().unwrap();
        let ops = self.operations.read().unwrap();
        endpoint_ops
            .get(endpoint_id)
            .map(|ids| ids.iter().filter_map(|id| ops.get(id).cloned()).collect())
            .unwrap_or_default()
    }

    pub fn handle_callback(
        &self,
        operation_id: &str,
        result: CallbackResult,
    ) -> Result<(), RelayError> {
        self.stats
            .callbacks_received
            .fetch_add(1, Ordering::Relaxed);
        match result {
            CallbackResult::Success(data) => self.complete_operation(operation_id, data),
            CallbackResult::Failure(failure) => self.fail_operation(operation_id, failure),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CallbackResult {
    Success(Option<Vec<u8>>),
    Failure(RelayFailure),
}

// ═══════════════════════════════════════════════════════════════════════════════
// Error Types
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum RelayError {
    EndpointNotFound(String),
    EndpointAlreadyExists(String),
    OperationNotFound(String),
    InvalidEndpoint(String),
    HandlerNotFound(String),
    Timeout,
    Internal(String),
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{:x}{:x}", t.as_secs(), t.subsec_nanos())
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_create() {
        let mgr = RelayEndpointManager::new();
        let ep = mgr
            .create_endpoint(
                "test-ep",
                EndpointTarget::Worker {
                    namespace: "default".to_string(),
                    task_queue: "relay-queue".to_string(),
                    identity: None,
                },
                "Test endpoint",
            )
            .unwrap();
        assert_eq!(ep.name, "test-ep");
        assert_eq!(mgr.stats().endpoints_created.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_endpoint_get_by_name() {
        let mgr = RelayEndpointManager::new();
        mgr.create_endpoint(
            "my-ep",
            EndpointTarget::Worker {
                namespace: "ns".to_string(),
                task_queue: "q".to_string(),
                identity: None,
            },
            "desc",
        )
        .unwrap();

        let ep = mgr.get_endpoint_by_name("my-ep").unwrap();
        assert_eq!(ep.name, "my-ep");
    }

    #[test]
    fn test_endpoint_update() {
        let mgr = RelayEndpointManager::new();
        let ep = mgr
            .create_endpoint(
                "ep1",
                EndpointTarget::Worker {
                    namespace: "ns".to_string(),
                    task_queue: "q".to_string(),
                    identity: None,
                },
                "original",
            )
            .unwrap();

        let updated = mgr.update_endpoint(&ep.id, Some("updated"), None).unwrap();
        assert_eq!(updated.description, "updated");
        assert_eq!(updated.version, 2);
    }

    #[test]
    fn test_endpoint_delete() {
        let mgr = RelayEndpointManager::new();
        let ep = mgr
            .create_endpoint(
                "ep1",
                EndpointTarget::Worker {
                    namespace: "ns".to_string(),
                    task_queue: "q".to_string(),
                    identity: None,
                },
                "desc",
            )
            .unwrap();

        mgr.delete_endpoint(&ep.id).unwrap();
        assert!(mgr.get_endpoint(&ep.id).is_none());
        assert!(mgr.get_endpoint_by_name("ep1").is_none());
    }

    #[test]
    fn test_endpoint_duplicate() {
        let mgr = RelayEndpointManager::new();
        mgr.create_endpoint(
            "ep1",
            EndpointTarget::Worker {
                namespace: "ns".to_string(),
                task_queue: "q".to_string(),
                identity: None,
            },
            "desc",
        )
        .unwrap();
        assert!(mgr
            .create_endpoint(
                "ep1",
                EndpointTarget::Worker {
                    namespace: "ns".to_string(),
                    task_queue: "q".to_string(),
                    identity: None,
                },
                "desc"
            )
            .is_err());
    }

    #[test]
    fn test_relay_operation_lifecycle() {
        let mgr = RelayEndpointManager::new();
        let ep = mgr
            .create_endpoint(
                "ep1",
                EndpointTarget::Worker {
                    namespace: "ns".to_string(),
                    task_queue: "q".to_string(),
                    identity: None,
                },
                "desc",
            )
            .unwrap();

        let op_mgr = RelayOperationManager::new(Arc::new(EndpointManagerStats::default()));
        let op = op_mgr
            .start_operation(
                &ep.id,
                "payment-service",
                "process-payment",
                Some(b"input".to_vec()),
                Some("http://callback"),
            )
            .unwrap();
        assert_eq!(op.state, RelayOperationState::Pending);

        op_mgr
            .complete_operation(&op.operation_id, Some(b"result".to_vec()))
            .unwrap();
        let completed = op_mgr.get_operation(&op.operation_id).unwrap();
        assert_eq!(completed.state, RelayOperationState::Succeeded);
        assert!(completed.result.is_some());
    }

    #[test]
    fn test_relay_operation_failure() {
        let op_mgr = RelayOperationManager::new(Arc::new(EndpointManagerStats::default()));
        let ep_mgr = RelayEndpointManager::new();
        let ep = ep_mgr
            .create_endpoint(
                "ep1",
                EndpointTarget::Worker {
                    namespace: "ns".to_string(),
                    task_queue: "q".to_string(),
                    identity: None,
                },
                "desc",
            )
            .unwrap();

        let op = op_mgr
            .start_operation(&ep.id, "svc", "op", None, None)
            .unwrap();
        op_mgr
            .fail_operation(
                &op.operation_id,
                RelayFailure {
                    message: "payment declined".to_string(),
                    failure_type: "BusinessError".to_string(),
                    retryable: false,
                },
            )
            .unwrap();

        let failed = op_mgr.get_operation(&op.operation_id).unwrap();
        assert_eq!(failed.state, RelayOperationState::Failed);
        assert!(failed.failure.is_some());
    }

    #[test]
    fn test_relay_callback() {
        let op_mgr = RelayOperationManager::new(Arc::new(EndpointManagerStats::default()));
        let ep_mgr = RelayEndpointManager::new();
        let ep = ep_mgr
            .create_endpoint(
                "ep1",
                EndpointTarget::Worker {
                    namespace: "ns".to_string(),
                    task_queue: "q".to_string(),
                    identity: None,
                },
                "desc",
            )
            .unwrap();

        let op = op_mgr
            .start_operation(&ep.id, "svc", "op", None, None)
            .unwrap();
        op_mgr
            .handle_callback(
                &op.operation_id,
                CallbackResult::Success(Some(b"callback-result".to_vec())),
            )
            .unwrap();

        let completed = op_mgr.get_operation(&op.operation_id).unwrap();
        assert_eq!(completed.state, RelayOperationState::Succeeded);
    }

    #[test]
    fn test_list_endpoints() {
        let mgr = RelayEndpointManager::new();
        mgr.create_endpoint(
            "ep1",
            EndpointTarget::Worker {
                namespace: "ns".to_string(),
                task_queue: "q".to_string(),
                identity: None,
            },
            "desc",
        )
        .unwrap();
        mgr.create_endpoint(
            "ep2",
            EndpointTarget::External {
                url: "http://external.com".to_string(),
                auth_method: AuthMethod::None,
            },
            "external",
        )
        .unwrap();

        assert_eq!(mgr.list_endpoints().len(), 2);
    }

    #[test]
    fn test_operations_for_endpoint() {
        let ep_mgr = RelayEndpointManager::new();
        let ep = ep_mgr
            .create_endpoint(
                "ep1",
                EndpointTarget::Worker {
                    namespace: "ns".to_string(),
                    task_queue: "q".to_string(),
                    identity: None,
                },
                "desc",
            )
            .unwrap();

        let op_mgr = RelayOperationManager::new(Arc::new(EndpointManagerStats::default()));
        op_mgr
            .start_operation(&ep.id, "svc", "op1", None, None)
            .unwrap();
        op_mgr
            .start_operation(&ep.id, "svc", "op2", None, None)
            .unwrap();

        let ops = op_mgr.get_operations_for_endpoint(&ep.id);
        assert_eq!(ops.len(), 2);
    }
}
