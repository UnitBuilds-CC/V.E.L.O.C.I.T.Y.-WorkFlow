//! Workflow Update API — synchronous workflow mutation.
//!
//! Unlike signals (fire-and-forget), updates allow callers to send a mutation
//! to a running workflow and wait for the result. This is Temporal's newest
//! workflow interaction primitive.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Wait policy for update completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateWaitPolicy {
    /// Wait until the update is accepted by the workflow.
    Accepted,
    /// Wait until the update completes.
    Completed,
    /// Admit the update without waiting for processing.
    Admitted,
}

/// Status of an update request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStatus {
    /// Update has been admitted (received but not yet processed).
    Admitted,
    /// Update has been accepted by the workflow handler.
    Accepted,
    /// Update has completed with a result.
    Completed,
    /// Update was rejected by the handler.
    Rejected,
}

/// An update request sent to a workflow.
#[derive(Debug, Clone)]
pub struct UpdateRequest {
    pub workflow_key: u64,
    pub update_id: String,
    pub update_name: String,
    pub args: Vec<u8>,
    pub wait_policy: UpdateWaitPolicy,
}

/// The result of an update request.
#[derive(Debug, Clone)]
pub struct UpdateResult {
    pub update_id: String,
    pub status: UpdateStatus,
    pub result: Option<Vec<u8>>,
    pub failure: Option<String>,
}

/// A registered update handler.
pub type UpdateHandlerFn = Box<dyn Fn(&[u8]) -> Result<Vec<u8>, String> + Send + Sync>;

/// Manages update handlers for a workflow.
pub struct UpdateHandler {
    handlers: HashMap<String, UpdateHandlerFn>,
}

impl UpdateHandler {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register an update handler by name.
    pub fn register_handler(
        &mut self,
        name: &str,
        handler: impl Fn(&[u8]) -> Result<Vec<u8>, String> + Send + Sync + 'static,
    ) {
        self.handlers.insert(name.to_string(), Box::new(handler));
    }

    /// Execute an update by name.
    pub fn execute_update(&self, request: &UpdateRequest) -> UpdateResult {
        match self.handlers.get(&request.update_name) {
            Some(handler) => match handler(&request.args) {
                Ok(result) => UpdateResult {
                    update_id: request.update_id.clone(),
                    status: UpdateStatus::Completed,
                    result: Some(result),
                    failure: None,
                },
                Err(e) => UpdateResult {
                    update_id: request.update_id.clone(),
                    status: UpdateStatus::Rejected,
                    result: None,
                    failure: Some(e),
                },
            },
            None => UpdateResult {
                update_id: request.update_id.clone(),
                status: UpdateStatus::Rejected,
                result: None,
                failure: Some(format!(
                    "No handler registered for update '{}'",
                    request.update_name
                )),
            },
        }
    }

    /// List registered handler names.
    pub fn list_handlers(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    /// Check if a handler exists for the given name.
    pub fn has_handler(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
    }
}

impl Default for UpdateHandler {
    fn default() -> Self {
        Self::new()
    }
}

/// Tracks pending and completed updates.
pub struct UpdateStore {
    pending: Mutex<HashMap<String, UpdateRequest>>,
    completed: Mutex<HashMap<String, UpdateResult>>,
    notifier: Condvar,
}

impl UpdateStore {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            completed: Mutex::new(HashMap::new()),
            notifier: Condvar::new(),
        }
    }

    /// Submit an update and return its ID.
    pub fn submit_update(&self, request: UpdateRequest) -> String {
        let id = request.update_id.clone();
        self.pending.lock().unwrap().insert(id.clone(), request);
        id
    }

    /// Mark an update as completed.
    pub fn complete_update(&self, result: UpdateResult) {
        let id = result.update_id.clone();
        self.pending.lock().unwrap().remove(&id);
        self.completed.lock().unwrap().insert(id.clone(), result);
        self.notifier.notify_all();
    }

    /// Get the result of a completed update.
    pub fn get_result(&self, update_id: &str) -> Option<UpdateResult> {
        self.completed.lock().unwrap().get(update_id).cloned()
    }

    /// Wait for an update to complete with a timeout.
    pub fn wait_for_update(&self, update_id: &str, timeout: Duration) -> Option<UpdateResult> {
        let start = Instant::now();
        let guard = self.completed.lock().unwrap();
        let mut guard = guard;

        while !guard.contains_key(update_id) {
            if start.elapsed() >= timeout {
                return None;
            }
            let remaining = timeout - start.elapsed();
            let result = self.notifier.wait_timeout(guard, remaining).unwrap();
            guard = result.0;
        }

        guard.get(update_id).cloned()
    }

    /// Get a pending update by ID.
    pub fn get_pending(&self, update_id: &str) -> Option<UpdateRequest> {
        self.pending.lock().unwrap().get(update_id).cloned()
    }

    /// List all pending update IDs.
    pub fn list_pending(&self) -> Vec<String> {
        self.pending.lock().unwrap().keys().cloned().collect()
    }

    /// List all completed update IDs.
    pub fn list_completed(&self) -> Vec<String> {
        self.completed.lock().unwrap().keys().cloned().collect()
    }

    /// Count pending updates.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// Count completed updates.
    pub fn completed_count(&self) -> usize {
        self.completed.lock().unwrap().len()
    }
}

impl Default for UpdateStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Combined update controller for a workflow engine.
pub struct UpdateController {
    handler: UpdateHandler,
    store: Arc<UpdateStore>,
}

impl UpdateController {
    pub fn new() -> Self {
        Self {
            handler: UpdateHandler::new(),
            store: Arc::new(UpdateStore::new()),
        }
    }

    /// Register an update handler.
    pub fn register_handler(
        &mut self,
        name: &str,
        handler: impl Fn(&[u8]) -> Result<Vec<u8>, String> + Send + Sync + 'static,
    ) {
        self.handler.register_handler(name, handler);
    }

    /// Submit and execute an update.
    pub fn submit_update(&self, request: UpdateRequest) -> UpdateResult {
        let update_id = self.store.submit_update(request.clone());
        let result = self.handler.execute_update(&request);
        self.store.complete_update(result.clone());
        result
    }

    /// Get the result of an update.
    pub fn get_result(&self, update_id: &str) -> Option<UpdateResult> {
        self.store.get_result(update_id)
    }

    /// Wait for an update to complete.
    pub fn wait_for_update(&self, update_id: &str, timeout: Duration) -> Option<UpdateResult> {
        self.store.wait_for_update(update_id, timeout)
    }

    /// List registered handlers.
    pub fn list_handlers(&self) -> Vec<String> {
        self.handler.list_handlers()
    }
}

impl Default for UpdateController {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_handler_register_and_execute() {
        let mut handler = UpdateHandler::new();
        handler.register_handler("add-item", |args| {
            Ok(format!("processed: {} bytes", args.len()).into_bytes())
        });

        let request = UpdateRequest {
            workflow_key: 1,
            update_id: "u1".to_string(),
            update_name: "add-item".to_string(),
            args: b"hello".to_vec(),
            wait_policy: UpdateWaitPolicy::Completed,
        };

        let result = handler.execute_update(&request);
        assert_eq!(result.status, UpdateStatus::Completed);
        assert!(result.result.is_some());
        assert!(result.failure.is_none());
    }

    #[test]
    fn test_update_handler_not_found() {
        let handler = UpdateHandler::new();
        let request = UpdateRequest {
            workflow_key: 1,
            update_id: "u1".to_string(),
            update_name: "nonexistent".to_string(),
            args: vec![],
            wait_policy: UpdateWaitPolicy::Completed,
        };

        let result = handler.execute_update(&request);
        assert_eq!(result.status, UpdateStatus::Rejected);
        assert!(result.failure.is_some());
    }

    #[test]
    fn test_update_handler_rejection() {
        let mut handler = UpdateHandler::new();
        handler.register_handler("validate", |_args| Err("validation failed".to_string()));

        let request = UpdateRequest {
            workflow_key: 1,
            update_id: "u1".to_string(),
            update_name: "validate".to_string(),
            args: vec![],
            wait_policy: UpdateWaitPolicy::Completed,
        };

        let result = handler.execute_update(&request);
        assert_eq!(result.status, UpdateStatus::Rejected);
        assert_eq!(result.failure.unwrap(), "validation failed");
    }

    #[test]
    fn test_update_store_submit_and_complete() {
        let store = UpdateStore::new();
        let request = UpdateRequest {
            workflow_key: 1,
            update_id: "u1".to_string(),
            update_name: "test".to_string(),
            args: vec![],
            wait_policy: UpdateWaitPolicy::Completed,
        };

        let id = store.submit_update(request);
        assert_eq!(store.pending_count(), 1);
        assert_eq!(store.completed_count(), 0);

        store.complete_update(UpdateResult {
            update_id: id.clone(),
            status: UpdateStatus::Completed,
            result: Some(b"ok".to_vec()),
            failure: None,
        });

        assert_eq!(store.pending_count(), 0);
        assert_eq!(store.completed_count(), 1);

        let result = store.get_result(&id).unwrap();
        assert_eq!(result.status, UpdateStatus::Completed);
    }

    #[test]
    fn test_update_store_list() {
        let store = UpdateStore::new();

        for i in 0..3 {
            store.submit_update(UpdateRequest {
                workflow_key: 1,
                update_id: format!("u{}", i),
                update_name: "test".to_string(),
                args: vec![],
                wait_policy: UpdateWaitPolicy::Completed,
            });
        }

        assert_eq!(store.pending_count(), 3);
        assert_eq!(store.list_pending().len(), 3);
    }

    #[test]
    fn test_update_controller() {
        let mut controller = UpdateController::new();
        controller.register_handler("greet", |args| {
            Ok(format!("Hello, {}!", String::from_utf8_lossy(args)).into_bytes())
        });

        let request = UpdateRequest {
            workflow_key: 1,
            update_id: "u1".to_string(),
            update_name: "greet".to_string(),
            args: b"World".to_vec(),
            wait_policy: UpdateWaitPolicy::Completed,
        };

        let result = controller.submit_update(request);
        assert_eq!(result.status, UpdateStatus::Completed);
        assert!(result.result.is_some());

        let stored = controller.get_result("u1").unwrap();
        assert_eq!(stored.status, UpdateStatus::Completed);
    }

    #[test]
    fn test_update_handler_list() {
        let mut handler = UpdateHandler::new();
        handler.register_handler("a", |_| Ok(vec![]));
        handler.register_handler("b", |_| Ok(vec![]));
        handler.register_handler("c", |_| Ok(vec![]));

        let handlers = handler.list_handlers();
        assert_eq!(handlers.len(), 3);
        assert!(handler.has_handler("a"));
        assert!(handler.has_handler("b"));
        assert!(handler.has_handler("c"));
        assert!(!handler.has_handler("d"));
    }

    #[test]
    fn test_update_with_binary_payload() {
        let mut handler = UpdateHandler::new();
        handler.register_handler("binary-op", |args| {
            let sum: u32 = args
                .chunks(4)
                .map(|chunk| {
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(chunk);
                    u32::from_be_bytes(arr)
                })
                .sum();
            Ok(sum.to_be_bytes().to_vec())
        });

        let request = UpdateRequest {
            workflow_key: 1,
            update_id: "u1".to_string(),
            update_name: "binary-op".to_string(),
            args: vec![0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3],
            wait_policy: UpdateWaitPolicy::Completed,
        };

        let result = handler.execute_update(&request);
        assert_eq!(result.status, UpdateStatus::Completed);
        let result_bytes = result.result.unwrap();
        assert_eq!(u32::from_be_bytes(result_bytes.try_into().unwrap()), 6);
    }

    #[test]
    fn test_update_multiple_handlers() {
        let mut controller = UpdateController::new();
        controller.register_handler("add", |args| {
            Ok(format!("added: {}", args.len()).into_bytes())
        });
        controller.register_handler("remove", |args| {
            Ok(format!("removed: {}", args.len()).into_bytes())
        });

        let handlers = controller.list_handlers();
        assert_eq!(handlers.len(), 2);

        let r1 = controller.submit_update(UpdateRequest {
            workflow_key: 1,
            update_id: "u1".to_string(),
            update_name: "add".to_string(),
            args: b"item1".to_vec(),
            wait_policy: UpdateWaitPolicy::Completed,
        });
        assert_eq!(r1.status, UpdateStatus::Completed);

        let r2 = controller.submit_update(UpdateRequest {
            workflow_key: 1,
            update_id: "u2".to_string(),
            update_name: "remove".to_string(),
            args: b"item2".to_vec(),
            wait_policy: UpdateWaitPolicy::Completed,
        });
        assert_eq!(r2.status, UpdateStatus::Completed);
    }

    #[test]
    fn test_update_store_wait_timeout() {
        let store = UpdateStore::new();
        store.submit_update(UpdateRequest {
            workflow_key: 1,
            update_id: "u1".to_string(),
            update_name: "test".to_string(),
            args: vec![],
            wait_policy: UpdateWaitPolicy::Completed,
        });

        // Should timeout since update is never completed
        let result = store.wait_for_update("u1", Duration::from_millis(10));
        assert!(result.is_none());
    }
}
