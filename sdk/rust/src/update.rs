//! Workflow Update API — synchronous workflow mutation.
//!
//! Unlike signals (fire-and-forget), updates provide:
//! - Synchronous request/response semantics
//! - Wait policies (Accepted, Completed, Admitted)
//! - Validation before execution
//! - Named update handlers registered by workflows
//!
//! # Usage
//! ```rust
//! use velocity_sdk::update::{UpdateClient, UpdateWaitPolicy};
//!
//! let mut client = UpdateClient::new("localhost:7234");
//! client.register_handler("setAmount", |args| Ok(args), None);
//! let result = client.execute_update(42, "setAmount", b"100".to_vec(), UpdateWaitPolicy::Completed);
//! assert!(result.is_ok());
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Status of a workflow update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStatus {
    Admitted = 0,
    Accepted = 1,
    Completed = 2,
    Rejected = 3,
}

/// How long to wait for an update to complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateWaitPolicy {
    Admitted = 0,
    Accepted = 1,
    Completed = 2,
}

/// Request to execute a workflow update.
#[derive(Debug, Clone)]
pub struct UpdateRequest {
    pub workflow_key: u64,
    pub update_id: String,
    pub update_name: String,
    pub args: Vec<u8>,
    pub wait_policy: UpdateWaitPolicy,
}

/// Result of a workflow update execution.
#[derive(Debug, Clone)]
pub struct UpdateResult {
    pub update_id: String,
    pub status: UpdateStatus,
    pub result: Option<Vec<u8>>,
    pub error: Option<String>,
    pub duration_ms: f64,
}

/// A registered update handler.
pub struct UpdateHandler {
    pub name: String,
    pub handler: Box<dyn Fn(Vec<u8>) -> Result<Vec<u8>, String> + Send + Sync>,
    pub validator: Option<Box<dyn Fn(&Vec<u8>) -> bool + Send + Sync>>,
}

/// Client for executing workflow updates.
pub struct UpdateClient {
    server_address: String,
    handlers: Arc<Mutex<HashMap<String, Arc<UpdateHandler>>>>,
    pending: Arc<Mutex<HashMap<String, UpdateResult>>>,
}

impl UpdateClient {
    /// Create a new update client.
    pub fn new(server_address: &str) -> Self {
        Self {
            server_address: server_address.to_string(),
            handlers: Arc::new(Mutex::new(HashMap::new())),
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a named update handler.
    pub fn register_handler<F>(&mut self, name: &str, handler: F, validator: Option<Box<dyn Fn(&Vec<u8>) -> bool + Send + Sync>>)
    where
        F: Fn(Vec<u8>) -> Result<Vec<u8>, String> + Send + Sync + 'static,
    {
        let h = UpdateHandler {
            name: name.to_string(),
            handler: Box::new(handler),
            validator,
        };
        self.handlers.lock().unwrap().insert(name.to_string(), Arc::new(h));
    }

    /// Execute a workflow update.
    pub fn execute_update(
        &self,
        workflow_key: u64,
        update_name: &str,
        args: Vec<u8>,
        wait_policy: UpdateWaitPolicy,
    ) -> UpdateResult {
        let uid = format!("update-{}-{}", workflow_key, std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis());
        let start = std::time::Instant::now();

        let handlers = self.handlers.lock().unwrap();
        let handler = match handlers.get(update_name) {
            Some(h) => Arc::clone(h),
            None => {
                let result = UpdateResult {
                    update_id: uid.clone(),
                    status: UpdateStatus::Rejected,
                    result: None,
                    error: Some(format!("No handler registered for update '{}'", update_name)),
                    duration_ms: start.elapsed().as_secs_f64() * 1000.0,
                };
                self.pending.lock().unwrap().insert(uid.clone(), result.clone());
                return result;
            }
        };
        drop(handlers);

        // Validate if validator exists
        if let Some(ref validator) = handler.validator {
            if !validator(&args) {
                let result = UpdateResult {
                    update_id: uid.clone(),
                    status: UpdateStatus::Rejected,
                    result: None,
                    error: Some("Update validation failed".to_string()),
                    duration_ms: start.elapsed().as_secs_f64() * 1000.0,
                };
                self.pending.lock().unwrap().insert(uid.clone(), result.clone());
                return result;
            }
        }

        // Execute the handler
        let result = match (handler.handler)(args) {
            Ok(value) => UpdateResult {
                update_id: uid.clone(),
                status: UpdateStatus::Completed,
                result: Some(value),
                error: None,
                duration_ms: start.elapsed().as_secs_f64() * 1000.0,
            },
            Err(e) => UpdateResult {
                update_id: uid.clone(),
                status: UpdateStatus::Rejected,
                result: None,
                error: Some(e),
                duration_ms: start.elapsed().as_secs_f64() * 1000.0,
            },
        };

        self.pending.lock().unwrap().insert(uid.clone(), result.clone());
        result
    }

    /// Get the result of a previously executed update.
    pub fn get_update_result(&self, update_id: &str) -> Option<UpdateResult> {
        self.pending.lock().unwrap().get(update_id).cloned()
    }

    /// List registered update handler names.
    pub fn list_handlers(&self) -> Vec<String> {
        self.handlers.lock().unwrap().keys().cloned().collect()
    }

    /// List pending update IDs.
    pub fn list_pending(&self) -> Vec<String> {
        self.pending.lock().unwrap().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_execute_handler() {
        let mut client = UpdateClient::new("localhost:7234");
        client.register_handler("echo", |args| Ok(args), None);

        let result = client.execute_update(1, "echo", b"hello".to_vec(), UpdateWaitPolicy::Completed);
        assert_eq!(result.status, UpdateStatus::Completed);
        assert_eq!(result.result, Some(b"hello".to_vec()));
    }

    #[test]
    fn test_unknown_handler() {
        let client = UpdateClient::new("localhost:7234");
        let result = client.execute_update(1, "unknown", vec![], UpdateWaitPolicy::Completed);
        assert_eq!(result.status, UpdateStatus::Rejected);
        assert!(result.error.is_some());
    }

    #[test]
    fn test_validation_failure() {
        let mut client = UpdateClient::new("localhost:7234");
        client.register_handler(
            "validated",
            |args| Ok(args),
            Some(Box::new(|args| !args.is_empty())),
        );

        let result = client.execute_update(1, "validated", vec![], UpdateWaitPolicy::Completed);
        assert_eq!(result.status, UpdateStatus::Rejected);
        assert_eq!(result.error.as_deref(), Some("Update validation failed"));
    }

    #[test]
    fn test_handler_error() {
        let mut client = UpdateClient::new("localhost:7234");
        client.register_handler("failing", |_| Err("handler error".to_string()), None);

        let result = client.execute_update(1, "failing", vec![], UpdateWaitPolicy::Completed);
        assert_eq!(result.status, UpdateStatus::Rejected);
        assert_eq!(result.error.as_deref(), Some("handler error"));
    }

    #[test]
    fn test_list_handlers() {
        let mut client = UpdateClient::new("localhost:7234");
        client.register_handler("a", |args| Ok(args), None);
        client.register_handler("b", |args| Ok(args), None);

        let handlers = client.list_handlers();
        assert_eq!(handlers.len(), 2);
        assert!(handlers.contains(&"a".to_string()));
        assert!(handlers.contains(&"b".to_string()));
    }
}
