//! Storage backend trait and implementations.
//!
//! The storage backend is responsible for persisting workflow state,
//! journal entries, and durable key-value state.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ─── Storage Error ───────────────────────────────────────────────────────────

/// Errors from the storage backend.
#[derive(Debug, Clone)]
pub enum StorageError {
    Connection(String),
    Query(String),
    Serialization(String),
    NotFound(String),
    Conflict(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connection(msg) => write!(f, "Connection error: {}", msg),
            Self::Query(msg) => write!(f, "Query error: {}", msg),
            Self::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Self::NotFound(msg) => write!(f, "Not found: {}", msg),
            Self::Conflict(msg) => write!(f, "Conflict: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {}

// ─── Storage Backend Trait ───────────────────────────────────────────────────

/// Trait for storage backends.
///
/// Implement this trait to provide persistent storage for the embedded engine.
/// The primary implementation is `PostgresAdapter`.
pub trait StorageBackend: Send {
    /// Initialize the database schema (create tables, indexes).
    fn init_schema(&self) -> Result<(), StorageError>;

    /// Save a workflow's output.
    fn save_workflow(
        &self,
        workflow_id: &str,
        function_name: &str,
        output: &serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Load a workflow's output (for crash recovery).
    fn load_workflow(&self, workflow_id: &str) -> Result<Option<serde_json::Value>, StorageError>;

    /// Save a journal entry.
    fn save_journal_entry(
        &self,
        workflow_id: &str,
        entry: &serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Load all journal entries for a workflow (for replay).
    fn load_journal(&self, workflow_id: &str) -> Result<Vec<serde_json::Value>, StorageError>;

    /// Save a key-value state entry.
    fn save_state(
        &self,
        workflow_id: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Load a key-value state entry.
    fn load_state(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError>;

    /// Delete a key-value state entry.
    fn delete_state(&self, workflow_id: &str, key: &str) -> Result<bool, StorageError>;

    /// List all workflow IDs.
    fn list_workflows(&self) -> Result<Vec<String>, StorageError>;

    /// Delete a workflow and its associated data.
    fn delete_workflow(&self, workflow_id: &str) -> Result<(), StorageError>;
}

// ─── In-Memory Storage ───────────────────────────────────────────────────────

/// In-memory storage backend for testing.
///
/// All data is lost when the process exits. Useful for unit tests
/// and development.
pub struct InMemoryStorage {
    workflows: Arc<Mutex<HashMap<String, WorkflowData>>>,
    state: Arc<Mutex<HashMap<String, HashMap<String, serde_json::Value>>>>,
    journals: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
}

struct WorkflowData {
    #[allow(dead_code)]
    function_name: String,
    output: serde_json::Value,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            workflows: Arc::new(Mutex::new(HashMap::new())),
            state: Arc::new(Mutex::new(HashMap::new())),
            journals: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for InMemoryStorage {
    fn init_schema(&self) -> Result<(), StorageError> {
        // No-op for in-memory storage
        Ok(())
    }

    fn save_workflow(
        &self,
        workflow_id: &str,
        function_name: &str,
        output: &serde_json::Value,
    ) -> Result<(), StorageError> {
        let mut workflows = self.workflows.lock().map_err(|_| StorageError::Connection("lock poisoned".to_string()))?;
        workflows.insert(workflow_id.to_string(), WorkflowData {
            function_name: function_name.to_string(),
            output: output.clone(),
        });
        Ok(())
    }

    fn load_workflow(&self, workflow_id: &str) -> Result<Option<serde_json::Value>, StorageError> {
        let workflows = self.workflows.lock().map_err(|_| StorageError::Connection("lock poisoned".to_string()))?;
        Ok(workflows.get(workflow_id).map(|w| w.output.clone()))
    }

    fn save_journal_entry(
        &self,
        workflow_id: &str,
        entry: &serde_json::Value,
    ) -> Result<(), StorageError> {
        let mut journals = self.journals.lock().map_err(|_| StorageError::Connection("lock poisoned".to_string()))?;
        journals.entry(workflow_id.to_string())
            .or_insert_with(Vec::new)
            .push(entry.clone());
        Ok(())
    }

    fn load_journal(&self, workflow_id: &str) -> Result<Vec<serde_json::Value>, StorageError> {
        let journals = self.journals.lock().map_err(|_| StorageError::Connection("lock poisoned".to_string()))?;
        Ok(journals.get(workflow_id).cloned().unwrap_or_default())
    }

    fn save_state(
        &self,
        workflow_id: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        let mut state = self.state.lock().map_err(|_| StorageError::Connection("lock poisoned".to_string()))?;
        state.entry(workflow_id.to_string())
            .or_insert_with(HashMap::new)
            .insert(key.to_string(), value.clone());
        Ok(())
    }

    fn load_state(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let state = self.state.lock().map_err(|_| StorageError::Connection("lock poisoned".to_string()))?;
        Ok(state.get(workflow_id).and_then(|m| m.get(key)).cloned())
    }

    fn delete_state(&self, workflow_id: &str, key: &str) -> Result<bool, StorageError> {
        let mut state = self.state.lock().map_err(|_| StorageError::Connection("lock poisoned".to_string()))?;
        Ok(state.get_mut(workflow_id)
            .map(|m| m.remove(key).is_some())
            .unwrap_or(false))
    }

    fn list_workflows(&self) -> Result<Vec<String>, StorageError> {
        let workflows = self.workflows.lock().map_err(|_| StorageError::Connection("lock poisoned".to_string()))?;
        Ok(workflows.keys().cloned().collect())
    }

    fn delete_workflow(&self, workflow_id: &str) -> Result<(), StorageError> {
        {
            let mut workflows = self.workflows.lock().map_err(|_| StorageError::Connection("lock poisoned".to_string()))?;
            workflows.remove(workflow_id);
        }
        {
            let mut state = self.state.lock().map_err(|_| StorageError::Connection("lock poisoned".to_string()))?;
            state.remove(workflow_id);
        }
        {
            let mut journals = self.journals.lock().map_err(|_| StorageError::Connection("lock poisoned".to_string()))?;
            journals.remove(workflow_id);
        }
        Ok(())
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_init() {
        let storage = InMemoryStorage::new();
        assert!(storage.init_schema().is_ok());
    }

    #[test]
    fn test_save_and_load_workflow() {
        let storage = InMemoryStorage::new();
        let output = serde_json::json!({"result": "hello"});
        storage.save_workflow("wf-1", "greet", &output).unwrap();

        let loaded = storage.load_workflow("wf-1").unwrap();
        assert_eq!(loaded, Some(output));
    }

    #[test]
    fn test_load_nonexistent_workflow() {
        let storage = InMemoryStorage::new();
        let loaded = storage.load_workflow("wf-999").unwrap();
        assert_eq!(loaded, None);
    }

    #[test]
    fn test_journal_entries() {
        let storage = InMemoryStorage::new();

        let entry1 = serde_json::json!({"seq": 0, "fn": "step1", "output": 42});
        let entry2 = serde_json::json!({"seq": 1, "fn": "step2", "output": 84});

        storage.save_journal_entry("wf-1", &entry1).unwrap();
        storage.save_journal_entry("wf-1", &entry2).unwrap();

        let journal = storage.load_journal("wf-1").unwrap();
        assert_eq!(journal.len(), 2);
        assert_eq!(journal[0], entry1);
        assert_eq!(journal[1], entry2);
    }

    #[test]
    fn test_journal_isolation() {
        let storage = InMemoryStorage::new();

        storage.save_journal_entry("wf-1", &serde_json::json!("a")).unwrap();
        storage.save_journal_entry("wf-2", &serde_json::json!("b")).unwrap();

        let j1 = storage.load_journal("wf-1").unwrap();
        let j2 = storage.load_journal("wf-2").unwrap();
        assert_eq!(j1.len(), 1);
        assert_eq!(j2.len(), 1);
    }

    #[test]
    fn test_state_operations() {
        let storage = InMemoryStorage::new();

        storage.save_state("wf-1", "count", &serde_json::json!(42)).unwrap();
        let val = storage.load_state("wf-1", "count").unwrap();
        assert_eq!(val, Some(serde_json::json!(42)));

        let deleted = storage.delete_state("wf-1", "count").unwrap();
        assert!(deleted);

        let val = storage.load_state("wf-1", "count").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_state_isolation() {
        let storage = InMemoryStorage::new();

        storage.save_state("wf-1", "x", &serde_json::json!("a")).unwrap();
        storage.save_state("wf-2", "x", &serde_json::json!("b")).unwrap();

        let v1 = storage.load_state("wf-1", "x").unwrap();
        let v2 = storage.load_state("wf-2", "x").unwrap();
        assert_eq!(v1, Some(serde_json::json!("a")));
        assert_eq!(v2, Some(serde_json::json!("b")));
    }

    #[test]
    fn test_list_workflows() {
        let storage = InMemoryStorage::new();
        storage.save_workflow("wf-1", "fn1", &serde_json::json!("a")).unwrap();
        storage.save_workflow("wf-2", "fn2", &serde_json::json!("b")).unwrap();

        let list = storage.list_workflows().unwrap();
        assert_eq!(list.len(), 2);
        assert!(list.contains(&"wf-1".to_string()));
        assert!(list.contains(&"wf-2".to_string()));
    }

    #[test]
    fn test_delete_workflow() {
        let storage = InMemoryStorage::new();
        storage.save_workflow("wf-1", "fn", &serde_json::json!("out")).unwrap();
        storage.save_state("wf-1", "key", &serde_json::json!("val")).unwrap();
        storage.save_journal_entry("wf-1", &serde_json::json!("entry")).unwrap();

        storage.delete_workflow("wf-1").unwrap();

        assert!(storage.load_workflow("wf-1").unwrap().is_none());
        assert!(storage.load_state("wf-1", "key").unwrap().is_none());
        assert!(storage.load_journal("wf-1").unwrap().is_empty());
    }

    #[test]
    fn test_storage_error_display() {
        let err = StorageError::Connection("timeout".to_string());
        assert_eq!(format!("{}", err), "Connection error: timeout");

        let err = StorageError::Conflict("duplicate key".to_string());
        assert_eq!(format!("{}", err), "Conflict: duplicate key");
    }
}
