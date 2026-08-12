//! Memo — unstructured key-value payload attached to a workflow execution.

use std::collections::HashMap;
use std::sync::Mutex;

pub struct MemoStore { memos: Mutex<HashMap<u64, HashMap<String, Vec<u8>>>> }

impl MemoStore {
    pub fn new() -> Self { Self { memos: Mutex::new(HashMap::new()) } }
    pub fn set_memo(&self, workflow_key: u64, key: &str, value: Vec<u8>) {
        self.memos.lock().unwrap().entry(workflow_key).or_default().insert(key.to_string(), value);
    }
    pub fn get_memo(&self, workflow_key: u64, key: &str) -> Option<Vec<u8>> {
        self.memos.lock().unwrap().get(&workflow_key)?.get(key).cloned()
    }
    pub fn get_all_memos(&self, workflow_key: u64) -> HashMap<String, Vec<u8>> {
        self.memos.lock().unwrap().get(&workflow_key).cloned().unwrap_or_default()
    }
    pub fn remove_memo(&self, workflow_key: u64, key: &str) -> bool {
        self.memos.lock().unwrap().get_mut(&workflow_key).and_then(|m| m.remove(key)).is_some()
    }
    pub fn count(&self, workflow_key: u64) -> usize {
        self.memos.lock().unwrap().get(&workflow_key).map_or(0, |m| m.len())
    }
    pub fn workflow_count(&self) -> usize { self.memos.lock().unwrap().len() }
}
impl Default for MemoStore { fn default() -> Self { Self::new() } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_set_and_get_memo() {
        let store = MemoStore::new();
        store.set_memo(42, "user_id", b"alice".to_vec());
        assert_eq!(store.get_memo(42, "user_id"), Some(b"alice".to_vec()));
        assert_eq!(store.get_memo(42, "nonexistent"), None);
    }
    #[test]
    fn test_get_all_memos() {
        let store = MemoStore::new();
        store.set_memo(1, "a", vec![1]);
        store.set_memo(1, "b", vec![2]);
        let all = store.get_all_memos(1);
        assert_eq!(all.len(), 2);
    }
}
