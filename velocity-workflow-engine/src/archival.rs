//! Workflow archival for moving completed workflows to cold storage.
//! Completed/terminated/canceled workflows are archived off the hot path,
//! reducing memory footprint while preserving history for compliance/queries.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::WorkflowStatus;

// ─── Archive Record ──────────────────────────────────────────────────────────

/// A complete archived workflow execution record.
#[derive(Debug, Clone)]
pub struct ArchiveRecord {
    pub workflow_key: u64,
    pub workflow_id: u64,
    pub run_id: u64,
    pub workflow_type_id: u64,
    pub namespace_id: u64,
    pub status: WorkflowStatus,
    pub input_data: Option<Vec<u8>>,
    pub result_data: Option<Vec<u8>>,
    pub step_count: u32,
    pub step_results: HashMap<u32, Vec<u8>>,
    pub event_count: u64,
    pub archived_at_ms: u64,
    pub start_time_ms: u64,
    pub close_time_ms: u64,
}

// ─── Archive Store ───────────────────────────────────────────────────────────

/// In-memory archive store for completed workflow executions.
/// In production this would be backed by S3/GCS/blob storage, but the
/// interface remains the same.
pub struct ArchiveStore {
    records: Mutex<HashMap<u64, ArchiveRecord>>,
    /// Secondary index: namespace_id → list of workflow_keys
    by_namespace: Mutex<HashMap<u64, Vec<u64>>>,
    /// Secondary index: workflow_type_id → list of workflow_keys
    by_type: Mutex<HashMap<u64, Vec<u64>>>,
    /// Secondary index: status → list of workflow_keys
    by_status: Mutex<HashMap<u8, Vec<u64>>>,
    next_archive_id: AtomicU64,
}

impl ArchiveStore {
    pub fn new() -> Self {
        Self {
            records: Mutex::new(HashMap::new()),
            by_namespace: Mutex::new(HashMap::new()),
            by_type: Mutex::new(HashMap::new()),
            by_status: Mutex::new(HashMap::new()),
            next_archive_id: AtomicU64::new(1),
        }
    }

    /// Archive a completed workflow. Returns the archive ID.
    pub fn archive(&self, record: ArchiveRecord) -> u64 {
        let archive_id = self.next_archive_id.fetch_add(1, Ordering::Relaxed);
        let key = record.workflow_key;
        let ns = record.namespace_id;
        let wf_type = record.workflow_type_id;
        let status_byte = record.status as u8;

        // Store the record
        self.records.lock().unwrap().insert(key, record);

        // Update secondary indices
        self.by_namespace
            .lock()
            .unwrap()
            .entry(ns)
            .or_default()
            .push(key);

        self.by_type
            .lock()
            .unwrap()
            .entry(wf_type)
            .or_default()
            .push(key);

        self.by_status
            .lock()
            .unwrap()
            .entry(status_byte)
            .or_default()
            .push(key);

        archive_id
    }

    /// Retrieve an archived workflow by key.
    pub fn get(&self, workflow_key: u64) -> Option<ArchiveRecord> {
        self.records.lock().unwrap().get(&workflow_key).cloned()
    }

    /// List archived workflows by namespace.
    pub fn list_by_namespace(&self, namespace_id: u64) -> Vec<ArchiveRecord> {
        let by_ns = self.by_namespace.lock().unwrap();
        let keys = by_ns.get(&namespace_id).cloned().unwrap_or_default();
        let records = self.records.lock().unwrap();
        keys.iter()
            .filter_map(|k| records.get(k).cloned())
            .collect()
    }

    /// List archived workflows by workflow type.
    pub fn list_by_type(&self, workflow_type_id: u64) -> Vec<ArchiveRecord> {
        let by_type = self.by_type.lock().unwrap();
        let keys = by_type.get(&workflow_type_id).cloned().unwrap_or_default();
        let records = self.records.lock().unwrap();
        keys.iter()
            .filter_map(|k| records.get(k).cloned())
            .collect()
    }

    /// List archived workflows by status.
    pub fn list_by_status(&self, status: WorkflowStatus) -> Vec<ArchiveRecord> {
        let by_status = self.by_status.lock().unwrap();
        let keys = by_status.get(&(status as u8)).cloned().unwrap_or_default();
        let records = self.records.lock().unwrap();
        keys.iter()
            .filter_map(|k| records.get(k).cloned())
            .collect()
    }

    /// Get the total number of archived workflows.
    pub fn count(&self) -> usize {
        self.records.lock().unwrap().len()
    }

    /// Count archived workflows by namespace.
    pub fn count_by_namespace(&self, namespace_id: u64) -> usize {
        self.by_namespace
            .lock()
            .unwrap()
            .get(&namespace_id)
            .map_or(0, |v| v.len())
    }

    /// Count archived workflows by status.
    pub fn count_by_status(&self, status: WorkflowStatus) -> usize {
        self.by_status
            .lock()
            .unwrap()
            .get(&(status as u8))
            .map_or(0, |v| v.len())
    }

    /// Delete an archived workflow (for retention policy enforcement).
    pub fn delete(&self, workflow_key: u64) -> bool {
        let record = self.records.lock().unwrap().remove(&workflow_key);
        if let Some(rec) = &record {
            // Clean up secondary indices
            if let Some(keys) = self.by_namespace.lock().unwrap().get_mut(&rec.namespace_id) {
                keys.retain(|k| *k != workflow_key);
            }
            if let Some(keys) = self.by_type.lock().unwrap().get_mut(&rec.workflow_type_id) {
                keys.retain(|k| *k != workflow_key);
            }
            if let Some(keys) = self.by_status.lock().unwrap().get_mut(&(rec.status as u8)) {
                keys.retain(|k| *k != workflow_key);
            }
            true
        } else {
            false
        }
    }

    /// Get current time in milliseconds since epoch.
    #[allow(dead_code)]
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

impl Default for ArchiveStore {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Archive Policy ──────────────────────────────────────────────────────────

/// Policy for when to automatically archive workflows.
#[derive(Debug, Clone)]
pub struct ArchivePolicy {
    /// Automatically archive completed workflows.
    pub auto_archive_completed: bool,
    /// Automatically archive failed workflows.
    pub auto_archive_failed: bool,
    /// Automatically archive canceled workflows.
    pub auto_archive_canceled: bool,
    /// Automatically archive terminated workflows.
    pub auto_archive_terminated: bool,
    /// Retention period in days (0 = forever).
    pub retention_days: u32,
}

impl ArchivePolicy {
    pub fn default_completed() -> Self {
        Self {
            auto_archive_completed: true,
            auto_archive_failed: false,
            auto_archive_canceled: false,
            auto_archive_terminated: false,
            retention_days: 0,
        }
    }

    pub fn archive_all() -> Self {
        Self {
            auto_archive_completed: true,
            auto_archive_failed: true,
            auto_archive_canceled: true,
            auto_archive_terminated: true,
            retention_days: 0,
        }
    }

    /// Check if a workflow with the given status should be auto-archived.
    pub fn should_archive(&self, status: WorkflowStatus) -> bool {
        match status {
            WorkflowStatus::Completed => self.auto_archive_completed,
            WorkflowStatus::Failed => self.auto_archive_failed,
            WorkflowStatus::Canceled => self.auto_archive_canceled,
            WorkflowStatus::Terminated => self.auto_archive_terminated,
            _ => false,
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(key: u64, ns: u64, wf_type: u64, status: WorkflowStatus) -> ArchiveRecord {
        ArchiveRecord {
            workflow_key: key,
            workflow_id: key,
            run_id: key + 1000,
            workflow_type_id: wf_type,
            namespace_id: ns,
            status,
            input_data: Some(vec![1, 2, 3]),
            result_data: Some(vec![4, 5, 6]),
            step_count: 3,
            step_results: HashMap::new(),
            event_count: 10,
            archived_at_ms: 0,
            start_time_ms: 0,
            close_time_ms: 100,
        }
    }

    #[test]
    fn test_archive_and_retrieve() {
        let store = ArchiveStore::new();
        let record = make_record(1, 0, 100, WorkflowStatus::Completed);

        let archive_id = store.archive(record.clone());
        assert!(archive_id > 0);

        let retrieved = store.get(1).unwrap();
        assert_eq!(retrieved.workflow_key, 1);
        assert_eq!(retrieved.workflow_type_id, 100);
        assert_eq!(retrieved.status, WorkflowStatus::Completed);
    }

    #[test]
    fn test_archive_count() {
        let store = ArchiveStore::new();
        assert_eq!(store.count(), 0);

        store.archive(make_record(1, 0, 100, WorkflowStatus::Completed));
        store.archive(make_record(2, 0, 100, WorkflowStatus::Failed));
        assert_eq!(store.count(), 2);
    }

    #[test]
    fn test_archive_by_namespace() {
        let store = ArchiveStore::new();
        store.archive(make_record(1, 10, 100, WorkflowStatus::Completed));
        store.archive(make_record(2, 10, 200, WorkflowStatus::Completed));
        store.archive(make_record(3, 20, 100, WorkflowStatus::Completed));

        let ns10 = store.list_by_namespace(10);
        assert_eq!(ns10.len(), 2);

        let ns20 = store.list_by_namespace(20);
        assert_eq!(ns20.len(), 1);

        assert_eq!(store.count_by_namespace(10), 2);
        assert_eq!(store.count_by_namespace(20), 1);
        assert_eq!(store.count_by_namespace(99), 0);
    }

    #[test]
    fn test_archive_by_type() {
        let store = ArchiveStore::new();
        store.archive(make_record(1, 0, 100, WorkflowStatus::Completed));
        store.archive(make_record(2, 0, 100, WorkflowStatus::Completed));
        store.archive(make_record(3, 0, 200, WorkflowStatus::Completed));

        let type100 = store.list_by_type(100);
        assert_eq!(type100.len(), 2);

        let type200 = store.list_by_type(200);
        assert_eq!(type200.len(), 1);
    }

    #[test]
    fn test_archive_by_status() {
        let store = ArchiveStore::new();
        store.archive(make_record(1, 0, 100, WorkflowStatus::Completed));
        store.archive(make_record(2, 0, 100, WorkflowStatus::Failed));
        store.archive(make_record(3, 0, 100, WorkflowStatus::Terminated));

        let completed = store.list_by_status(WorkflowStatus::Completed);
        assert_eq!(completed.len(), 1);

        let failed = store.list_by_status(WorkflowStatus::Failed);
        assert_eq!(failed.len(), 1);

        assert_eq!(store.count_by_status(WorkflowStatus::Completed), 1);
        assert_eq!(store.count_by_status(WorkflowStatus::Failed), 1);
        assert_eq!(store.count_by_status(WorkflowStatus::Terminated), 1);
    }

    #[test]
    fn test_archive_delete() {
        let store = ArchiveStore::new();
        store.archive(make_record(1, 10, 100, WorkflowStatus::Completed));
        store.archive(make_record(2, 10, 200, WorkflowStatus::Failed));
        assert_eq!(store.count(), 2);

        assert!(store.delete(1));
        assert_eq!(store.count(), 1);
        assert!(store.get(1).is_none());
        assert!(store.get(2).is_some());

        // Secondary indices should be cleaned up
        assert_eq!(store.count_by_namespace(10), 1);

        assert!(!store.delete(999)); // Non-existent
    }

    #[test]
    fn test_archive_policy_should_archive() {
        let policy = ArchivePolicy::default_completed();
        assert!(policy.should_archive(WorkflowStatus::Completed));
        assert!(!policy.should_archive(WorkflowStatus::Failed));
        assert!(!policy.should_archive(WorkflowStatus::Running));

        let all_policy = ArchivePolicy::archive_all();
        assert!(all_policy.should_archive(WorkflowStatus::Completed));
        assert!(all_policy.should_archive(WorkflowStatus::Failed));
        assert!(all_policy.should_archive(WorkflowStatus::Canceled));
        assert!(all_policy.should_archive(WorkflowStatus::Terminated));
        assert!(!all_policy.should_archive(WorkflowStatus::Running));
    }
}
