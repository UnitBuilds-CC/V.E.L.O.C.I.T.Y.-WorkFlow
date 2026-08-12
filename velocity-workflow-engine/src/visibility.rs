//! Workflow visibility and search index. Provides O(1) workflow listing by status,
//! type, namespace, and time range. Supports custom search attributes for advanced queries.

use std::collections::{HashMap, BTreeMap};
use std::sync::RwLock;

use crate::engine::WorkflowStatus;

// ─── Workflow Execution Info ──────────────────────────────────────────────────

/// Summary information about a workflow execution for visibility/listing.
#[derive(Debug, Clone)]
pub struct WorkflowExecutionInfo {
    pub workflow_key: u64,
    pub workflow_id: u64,
    pub run_id: u64,
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub status: WorkflowStatus,
    pub start_time_ms: u64,
    pub close_time_ms: Option<u64>,
    pub task_queue_hash: u64,
    pub search_attributes: HashMap<String, SearchAttributeValue>,
}

/// Custom search attribute values (supports multiple types for flexible querying).
#[derive(Debug, Clone, PartialEq)]
pub enum SearchAttributeValue {
    String(String),
    Integer(i64),
    Double(f64),
    Bool(bool),
    DateTime(u64), // epoch millis
    Keyword(String),
}

// ─── Visibility Index ────────────────────────────────────────────────────────

/// Thread-safe workflow visibility index. Maintains multiple indices for fast queries.
pub struct VisibilityIndex {
    /// All executions by workflow key.
    executions: RwLock<HashMap<u64, WorkflowExecutionInfo>>,
    /// Index by status: status -> set of workflow keys.
    by_status: RwLock<HashMap<u8, Vec<u64>>>,
    /// Index by namespace: namespace_id -> set of workflow keys.
    by_namespace: RwLock<HashMap<u64, Vec<u64>>>,
    /// Index by workflow type: type_id -> set of workflow keys.
    by_type: RwLock<HashMap<u64, Vec<u64>>>,
    /// Index by start time (sorted): start_time_ms -> workflow_key.
    by_start_time: RwLock<BTreeMap<u64, Vec<u64>>>,
}

impl VisibilityIndex {
    pub fn new() -> Self {
        Self {
            executions: RwLock::new(HashMap::new()),
            by_status: RwLock::new(HashMap::new()),
            by_namespace: RwLock::new(HashMap::new()),
            by_type: RwLock::new(HashMap::new()),
            by_start_time: RwLock::new(BTreeMap::new()),
        }
    }

    /// Register a new workflow execution in the index.
    pub fn register(&self, info: WorkflowExecutionInfo) {
        let key = info.workflow_key;

        // Main index
        self.executions.write().unwrap().insert(key, info.clone());

        // Status index
        self.by_status.write().unwrap()
            .entry(info.status as u8)
            .or_default()
            .push(key);

        // Namespace index
        self.by_namespace.write().unwrap()
            .entry(info.namespace_id)
            .or_default()
            .push(key);

        // Type index
        self.by_type.write().unwrap()
            .entry(info.workflow_type_id)
            .or_default()
            .push(key);

        // Time index
        self.by_start_time.write().unwrap()
            .entry(info.start_time_ms)
            .or_default()
            .push(key);
    }

    /// Update the status of a workflow (e.g., Running -> Completed).
    pub fn update_status(&self, workflow_key: u64, new_status: WorkflowStatus, close_time_ms: Option<u64>) {
        let mut executions = self.executions.write().unwrap();
        if let Some(info) = executions.get_mut(&workflow_key) {
            let old_status = info.status as u8;
            info.status = new_status;
            info.close_time_ms = close_time_ms;

            // Update status index
            let mut by_status = self.by_status.write().unwrap();
            if let Some(keys) = by_status.get_mut(&old_status) {
                keys.retain(|k| *k != workflow_key);
            }
            by_status.entry(new_status as u8).or_default().push(workflow_key);
        }
    }

    /// Set a custom search attribute on a workflow.
    pub fn set_search_attribute(&self, workflow_key: u64, key: String, value: SearchAttributeValue) {
        let mut executions = self.executions.write().unwrap();
        if let Some(info) = executions.get_mut(&workflow_key) {
            info.search_attributes.insert(key, value);
        }
    }

    // ─── Query Methods ────────────────────────────────────────────────────

    /// List workflows by status.
    pub fn list_by_status(&self, status: WorkflowStatus) -> Vec<WorkflowExecutionInfo> {
        let by_status = self.by_status.read().unwrap();
        let executions = self.executions.read().unwrap();
        by_status.get(&(status as u8))
            .map(|keys| keys.iter().filter_map(|k| executions.get(k).cloned()).collect())
            .unwrap_or_default()
    }

    /// List workflows by namespace.
    pub fn list_by_namespace(&self, namespace_id: u64) -> Vec<WorkflowExecutionInfo> {
        let by_ns = self.by_namespace.read().unwrap();
        let executions = self.executions.read().unwrap();
        by_ns.get(&namespace_id)
            .map(|keys| keys.iter().filter_map(|k| executions.get(k).cloned()).collect())
            .unwrap_or_default()
    }

    /// List workflows by type.
    pub fn list_by_type(&self, type_id: u64) -> Vec<WorkflowExecutionInfo> {
        let by_type = self.by_type.read().unwrap();
        let executions = self.executions.read().unwrap();
        by_type.get(&type_id)
            .map(|keys| keys.iter().filter_map(|k| executions.get(k).cloned()).collect())
            .unwrap_or_default()
    }

    /// List workflows started within a time range (inclusive).
    pub fn list_by_time_range(&self, start_ms: u64, end_ms: u64) -> Vec<WorkflowExecutionInfo> {
        let by_time = self.by_start_time.read().unwrap();
        let executions = self.executions.read().unwrap();
        by_time.range(start_ms..=end_ms)
            .flat_map(|(_, keys)| keys.iter().filter_map(|k| executions.get(k).cloned()))
            .collect()
    }

    /// List workflows matching a custom search attribute.
    pub fn list_by_search_attribute(&self, key: &str, value: &SearchAttributeValue) -> Vec<WorkflowExecutionInfo> {
        let executions = self.executions.read().unwrap();
        executions.values()
            .filter(|info| info.search_attributes.get(key) == Some(value))
            .cloned()
            .collect()
    }

    /// Get a single workflow execution by key.
    pub fn get(&self, workflow_key: u64) -> Option<WorkflowExecutionInfo> {
        self.executions.read().unwrap().get(&workflow_key).cloned()
    }

    /// Get the total number of indexed workflows.
    pub fn count(&self) -> usize {
        self.executions.read().unwrap().len()
    }

    /// Count workflows by status.
    pub fn count_by_status(&self, status: WorkflowStatus) -> usize {
        let by_status = self.by_status.read().unwrap();
        by_status.get(&(status as u8)).map_or(0, |v| v.len())
    }

    /// Count workflows by namespace.
    pub fn count_by_namespace(&self, namespace_id: u64) -> usize {
        let by_ns = self.by_namespace.read().unwrap();
        by_ns.get(&namespace_id).map_or(0, |v| v.len())
    }

    /// Count workflows by workflow type.
    pub fn count_by_type(&self, workflow_type_id: u64) -> usize {
        let by_type = self.by_type.read().unwrap();
        by_type.get(&workflow_type_id).map_or(0, |v| v.len())
    }

    /// Remove a workflow from the index (e.g., after retention expiry).
    pub fn remove(&self, workflow_key: u64) {
        let mut executions = self.executions.write().unwrap();
        if let Some(info) = executions.remove(&workflow_key) {
            // Clean up status index
            let mut by_status = self.by_status.write().unwrap();
            if let Some(keys) = by_status.get_mut(&(info.status as u8)) {
                keys.retain(|k| *k != workflow_key);
            }

            // Clean up namespace index
            let mut by_ns = self.by_namespace.write().unwrap();
            if let Some(keys) = by_ns.get_mut(&info.namespace_id) {
                keys.retain(|k| *k != workflow_key);
            }

            // Clean up type index
            let mut by_type = self.by_type.write().unwrap();
            if let Some(keys) = by_type.get_mut(&info.workflow_type_id) {
                keys.retain(|k| *k != workflow_key);
            }

            // Clean up time index
            let mut by_time = self.by_start_time.write().unwrap();
            if let Some(keys) = by_time.get_mut(&info.start_time_ms) {
                keys.retain(|k| *k != workflow_key);
            }
        }
    }
}

impl Default for VisibilityIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_info(key: u64, ns: u64, type_id: u64, status: WorkflowStatus) -> WorkflowExecutionInfo {
        WorkflowExecutionInfo {
            workflow_key: key,
            workflow_id: key,
            run_id: key + 1000,
            workflow_type_id: type_id,
            namespace_id: ns,
            status,
            start_time_ms: key * 1000,
            close_time_ms: None,
            task_queue_hash: 42,
            search_attributes: HashMap::new(),
        }
    }

    #[test]
    fn test_register_and_get() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running));

        let info = index.get(1).unwrap();
        assert_eq!(info.workflow_id, 1);
        assert_eq!(info.status, WorkflowStatus::Running);
        assert_eq!(index.count(), 1);
    }

    #[test]
    fn test_list_by_status() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running));
        index.register(make_info(2, 0, 100, WorkflowStatus::Completed));
        index.register(make_info(3, 0, 101, WorkflowStatus::Running));

        let running = index.list_by_status(WorkflowStatus::Running);
        assert_eq!(running.len(), 2);

        let completed = index.list_by_status(WorkflowStatus::Completed);
        assert_eq!(completed.len(), 1);
    }

    #[test]
    fn test_list_by_namespace() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running));
        index.register(make_info(2, 1, 100, WorkflowStatus::Running));
        index.register(make_info(3, 0, 101, WorkflowStatus::Running));

        let ns0 = index.list_by_namespace(0);
        assert_eq!(ns0.len(), 2);

        let ns1 = index.list_by_namespace(1);
        assert_eq!(ns1.len(), 1);
    }

    #[test]
    fn test_update_status() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running));

        assert_eq!(index.count_by_status(WorkflowStatus::Running), 1);
        assert_eq!(index.count_by_status(WorkflowStatus::Completed), 0);

        index.update_status(1, WorkflowStatus::Completed, Some(5000));

        assert_eq!(index.count_by_status(WorkflowStatus::Running), 0);
        assert_eq!(index.count_by_status(WorkflowStatus::Completed), 1);

        let info = index.get(1).unwrap();
        assert_eq!(info.close_time_ms, Some(5000));
    }

    #[test]
    fn test_search_attributes() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running));

        index.set_search_attribute(1, "customer_id".into(), SearchAttributeValue::String("C123".into()));

        let results = index.list_by_search_attribute("customer_id", &SearchAttributeValue::String("C123".into()));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].workflow_key, 1);
    }

    #[test]
    fn test_time_range_query() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running)); // start=1000
        index.register(make_info(2, 0, 100, WorkflowStatus::Running)); // start=2000
        index.register(make_info(3, 0, 100, WorkflowStatus::Running)); // start=3000

        let results = index.list_by_time_range(1000, 2000);
        assert_eq!(results.len(), 2);

        let all = index.list_by_time_range(0, 10000);
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_remove() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running));
        assert_eq!(index.count(), 1);

        index.remove(1);
        assert_eq!(index.count(), 0);
        assert!(index.get(1).is_none());
    }
}
