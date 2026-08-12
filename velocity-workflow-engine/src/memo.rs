//! Memo — unstructured key-value payload attached to a workflow execution.
//!
//! Memos are mutable metadata that can be set, updated, and retrieved during workflow
//! execution. They support:
//! - **Versioning**: Each memo key tracks its version for optimistic concurrency
//! - **TTL**: Optional time-to-live for automatic expiration
//! - **Bulk operations**: Set/get/remove multiple keys atomically
//! - **Statistics**: Track memo usage per workflow and globally
//! - **Search integration**: Memos can be indexed for visibility queries

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A single memo entry with metadata.
#[derive(Debug, Clone)]
pub struct MemoEntry {
    /// The memo value payload.
    pub value: Vec<u8>,
    /// Version number (incremented on each update).
    pub version: u64,
    /// When this memo was last updated.
    pub updated_at: Instant,
    /// Optional TTL — if set, the memo expires after this duration.
    pub ttl: Option<Duration>,
    /// When this memo was created.
    pub created_at: Instant,
}

impl MemoEntry {
    /// Check if this memo entry has expired.
    pub fn is_expired(&self) -> bool {
        if let Some(ttl) = self.ttl {
            self.updated_at.elapsed() > ttl
        } else {
            false
        }
    }
}

/// Statistics for memo usage.
#[derive(Debug, Clone, Default)]
pub struct MemoStats {
    pub total_set_operations: u64,
    pub total_get_operations: u64,
    pub total_remove_operations: u64,
    pub total_expired: u64,
    pub current_active_memos: u64,
    pub workflows_with_memos: u64,
}

/// Result of a versioned set operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum MemoSetResult {
    /// Memo was set successfully (new or updated).
    Success = 0,
    /// Set failed due to version mismatch (optimistic concurrency).
    VersionMismatch = 1,
    /// Set failed because the memo has expired.
    Expired = 2,
}

/// Manages memos for all workflows.
///
/// Thread-safe: uses internal `Mutex` for concurrent access.
pub struct MemoStore {
    /// Memos keyed by (workflow_key, memo_key).
    memos: Mutex<HashMap<(u64, String), MemoEntry>>,
    /// Aggregate statistics.
    stats: Mutex<MemoStats>,
}

impl MemoStore {
    /// Create a new memo store.
    pub fn new() -> Self {
        Self {
            memos: Mutex::new(HashMap::new()),
            stats: Mutex::new(MemoStats::default()),
        }
    }

    /// Set a memo value for a workflow.
    ///
    /// If the memo key doesn't exist, it's created with version 1.
    /// If it exists, the version is incremented.
    pub fn set(&self, workflow_key: u64, key: &str, value: Vec<u8>, ttl: Option<Duration>) {
        let now = Instant::now();
        let mut memos = self.memos.lock().unwrap();
        let entry = memos
            .entry((workflow_key, key.to_string()))
            .or_insert_with(|| {
                let mut stats = self.stats.lock().unwrap();
                stats.workflows_with_memos += 1;
                MemoEntry {
                    value: value.clone(),
                    version: 0,
                    updated_at: now,
                    ttl,
                    created_at: now,
                }
            });

        entry.value = value;
        entry.version += 1;
        entry.updated_at = now;
        entry.ttl = ttl;

        let mut stats = self.stats.lock().unwrap();
        stats.total_set_operations += 1;
        stats.current_active_memos = memos.values().filter(|e| !e.is_expired()).count() as u64;
    }

    /// Set a memo with optimistic concurrency control.
    ///
    /// If `expected_version` is provided, the set only succeeds if the current
    /// version matches. Returns `MemoSetResult` indicating success or failure.
    pub fn set_versioned(
        &self,
        workflow_key: u64,
        key: &str,
        value: Vec<u8>,
        expected_version: Option<u64>,
        ttl: Option<Duration>,
    ) -> MemoSetResult {
        let now = Instant::now();
        let mut memos = self.memos.lock().unwrap();
        let memo_key = (workflow_key, key.to_string());

        if let Some(entry) = memos.get(&memo_key) {
            // Check if expired
            if entry.is_expired() {
                return MemoSetResult::Expired;
            }

            // Check version
            if let Some(expected) = expected_version {
                if entry.version != expected {
                    return MemoSetResult::VersionMismatch;
                }
            }
        }

        // Perform the set
        let entry = memos.entry(memo_key).or_insert_with(|| {
            let mut stats = self.stats.lock().unwrap();
            stats.workflows_with_memos += 1;
            MemoEntry {
                value: value.clone(),
                version: 0,
                updated_at: now,
                ttl,
                created_at: now,
            }
        });

        entry.value = value;
        entry.version += 1;
        entry.updated_at = now;
        entry.ttl = ttl;

        let mut stats = self.stats.lock().unwrap();
        stats.total_set_operations += 1;

        MemoSetResult::Success
    }

    /// Get a memo value for a workflow.
    ///
    /// Returns `None` if the memo doesn't exist or has expired.
    pub fn get(&self, workflow_key: u64, key: &str) -> Option<Vec<u8>> {
        let memos = self.memos.lock().unwrap();
        let mut stats = self.stats.lock().unwrap();
        stats.total_get_operations += 1;

        if let Some(entry) = memos.get(&(workflow_key, key.to_string())) {
            if entry.is_expired() {
                stats.total_expired += 1;
                None
            } else {
                Some(entry.value.clone())
            }
        } else {
            None
        }
    }

    /// Get a memo entry with full metadata (value, version, TTL, timestamps).
    pub fn get_entry(&self, workflow_key: u64, key: &str) -> Option<MemoEntry> {
        let memos = self.memos.lock().unwrap();
        let mut stats = self.stats.lock().unwrap();
        stats.total_get_operations += 1;

        if let Some(entry) = memos.get(&(workflow_key, key.to_string())) {
            if entry.is_expired() {
                stats.total_expired += 1;
                None
            } else {
                Some(entry.clone())
            }
        } else {
            None
        }
    }

    /// Get all memos for a workflow.
    pub fn get_all(&self, workflow_key: u64) -> HashMap<String, Vec<u8>> {
        let memos = self.memos.lock().unwrap();
        memos
            .iter()
            .filter(|((wk, _), entry)| *wk == workflow_key && !entry.is_expired())
            .map(|((_, key), entry)| (key.clone(), entry.value.clone()))
            .collect()
    }

    /// Get all memo entries for a workflow (with metadata).
    pub fn get_all_entries(&self, workflow_key: u64) -> HashMap<String, MemoEntry> {
        let memos = self.memos.lock().unwrap();
        memos
            .iter()
            .filter(|((wk, _), entry)| *wk == workflow_key && !entry.is_expired())
            .map(|((_, key), entry)| (key.clone(), entry.clone()))
            .collect()
    }

    /// Remove a specific memo key.
    pub fn remove(&self, workflow_key: u64, key: &str) -> bool {
        let mut memos = self.memos.lock().unwrap();
        let removed = memos.remove(&(workflow_key, key.to_string())).is_some();

        if removed {
            let mut stats = self.stats.lock().unwrap();
            stats.total_remove_operations += 1;
            stats.current_active_memos = memos.values().filter(|e| !e.is_expired()).count() as u64;
        }

        removed
    }

    /// Remove all memos for a workflow.
    pub fn remove_all(&self, workflow_key: u64) -> usize {
        let mut memos = self.memos.lock().unwrap();
        let before = memos.len();
        memos.retain(|(wk, _), _| *wk != workflow_key);
        let removed = before - memos.len();

        if removed > 0 {
            let mut stats = self.stats.lock().unwrap();
            stats.total_remove_operations += removed as u64;
            stats.current_active_memos = memos.values().filter(|e| !e.is_expired()).count() as u64;
        }

        removed
    }

    /// Bulk set multiple memos atomically.
    pub fn set_bulk(
        &self,
        workflow_key: u64,
        entries: HashMap<String, (Vec<u8>, Option<Duration>)>,
    ) {
        let now = Instant::now();
        let mut memos = self.memos.lock().unwrap();

        for (key, (value, ttl)) in entries {
            let memo_key = (workflow_key, key);
            let entry = memos.entry(memo_key).or_insert_with(|| MemoEntry {
                value: value.clone(),
                version: 0,
                updated_at: now,
                ttl,
                created_at: now,
            });

            entry.value = value;
            entry.version += 1;
            entry.updated_at = now;
            entry.ttl = ttl;
        }

        let mut stats = self.stats.lock().unwrap();
        stats.total_set_operations += 1;
        stats.current_active_memos = memos.values().filter(|e| !e.is_expired()).count() as u64;
    }

    /// Remove expired memos across all workflows.
    pub fn purge_expired(&self) -> usize {
        let mut memos = self.memos.lock().unwrap();
        let before = memos.len();
        memos.retain(|_, entry| !entry.is_expired());
        let removed = before - memos.len();

        if removed > 0 {
            let mut stats = self.stats.lock().unwrap();
            stats.total_expired += removed as u64;
            stats.current_active_memos = memos.len() as u64;
        }

        removed
    }

    /// Get the number of active (non-expired) memos for a workflow.
    pub fn count(&self, workflow_key: u64) -> usize {
        let memos = self.memos.lock().unwrap();
        memos
            .iter()
            .filter(|((wk, _), entry)| *wk == workflow_key && !entry.is_expired())
            .count()
    }

    /// Get the total number of workflows that have memos.
    pub fn workflow_count(&self) -> usize {
        let memos = self.memos.lock().unwrap();
        memos
            .keys()
            .map(|(wk, _)| *wk)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    /// Get aggregate statistics.
    pub fn stats(&self) -> MemoStats {
        self.stats.lock().unwrap().clone()
    }

    /// Get the version of a specific memo key.
    pub fn get_version(&self, workflow_key: u64, key: &str) -> Option<u64> {
        let memos = self.memos.lock().unwrap();
        memos
            .get(&(workflow_key, key.to_string()))
            .map(|e| e.version)
    }
}

impl Default for MemoStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let store = MemoStore::new();
        store.set(1, "user_id", b"alice".to_vec(), None);
        assert_eq!(store.get(1, "user_id"), Some(b"alice".to_vec()));
        assert_eq!(store.get(1, "nonexistent"), None);
    }

    #[test]
    fn test_versioning() {
        let store = MemoStore::new();
        store.set(1, "key", b"v1".to_vec(), None);
        assert_eq!(store.get_version(1, "key"), Some(1));

        store.set(1, "key", b"v2".to_vec(), None);
        assert_eq!(store.get_version(1, "key"), Some(2));
    }

    #[test]
    fn test_versioned_set_success() {
        let store = MemoStore::new();
        store.set(1, "key", b"v1".to_vec(), None);

        let result = store.set_versioned(1, "key", b"v2".to_vec(), Some(1), None);
        assert_eq!(result, MemoSetResult::Success);
        assert_eq!(store.get_version(1, "key"), Some(2));
    }

    #[test]
    fn test_versioned_set_mismatch() {
        let store = MemoStore::new();
        store.set(1, "key", b"v1".to_vec(), None);

        let result = store.set_versioned(1, "key", b"v2".to_vec(), Some(999), None);
        assert_eq!(result, MemoSetResult::VersionMismatch);
        assert_eq!(store.get(1, "key"), Some(b"v1".to_vec())); // unchanged
    }

    #[test]
    fn test_ttl_expiration() {
        let store = MemoStore::new();
        store.set(1, "temp", b"data".to_vec(), Some(Duration::from_millis(1)));

        // Should be available immediately
        assert!(store.get(1, "temp").is_some());

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(10));

        // Should be expired
        assert!(store.get(1, "temp").is_none());
    }

    #[test]
    fn test_get_all() {
        let store = MemoStore::new();
        store.set(1, "a", vec![1], None);
        store.set(1, "b", vec![2], None);
        store.set(2, "c", vec![3], None); // different workflow

        let all = store.get_all(1);
        assert_eq!(all.len(), 2);
        assert_eq!(all.get("a"), Some(&vec![1]));
        assert_eq!(all.get("b"), Some(&vec![2]));
    }

    #[test]
    fn test_remove() {
        let store = MemoStore::new();
        store.set(1, "key", b"value".to_vec(), None);
        assert!(store.remove(1, "key"));
        assert_eq!(store.get(1, "key"), None);
        assert!(!store.remove(1, "key")); // already removed
    }

    #[test]
    fn test_remove_all() {
        let store = MemoStore::new();
        store.set(1, "a", vec![1], None);
        store.set(1, "b", vec![2], None);
        store.set(2, "c", vec![3], None);

        let removed = store.remove_all(1);
        assert_eq!(removed, 2);
        assert_eq!(store.count(1), 0);
        assert_eq!(store.count(2), 1);
    }

    #[test]
    fn test_bulk_set() {
        let store = MemoStore::new();
        let mut entries = HashMap::new();
        entries.insert("a".to_string(), (vec![1], None));
        entries.insert("b".to_string(), (vec![2], Some(Duration::from_secs(60))));

        store.set_bulk(1, entries);
        assert_eq!(store.count(1), 2);
        assert_eq!(store.get(1, "a"), Some(vec![1]));
        assert_eq!(store.get(1, "b"), Some(vec![2]));
    }

    #[test]
    fn test_purge_expired() {
        let store = MemoStore::new();
        store.set(1, "temp1", b"data".to_vec(), Some(Duration::from_millis(1)));
        store.set(1, "perm", b"data".to_vec(), None);

        std::thread::sleep(Duration::from_millis(10));

        let purged = store.purge_expired();
        assert_eq!(purged, 1);
        assert_eq!(store.count(1), 1);
    }

    #[test]
    fn test_stats() {
        let store = MemoStore::new();
        store.set(1, "a", vec![1], None);
        store.set(1, "b", vec![2], None);
        store.get(1, "a");
        store.remove(1, "a");

        let stats = store.stats();
        assert_eq!(stats.total_set_operations, 2);
        assert_eq!(stats.total_get_operations, 1);
        assert_eq!(stats.total_remove_operations, 1);
        assert_eq!(stats.current_active_memos, 1);
    }

    #[test]
    fn test_workflow_count() {
        let store = MemoStore::new();
        store.set(1, "a", vec![1], None);
        store.set(1, "b", vec![2], None);
        store.set(2, "c", vec![3], None);

        assert_eq!(store.workflow_count(), 2);
    }
}
