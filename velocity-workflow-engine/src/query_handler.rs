//! Query handler registry — named query handlers for workflow state inspection.

use std::collections::HashMap;
use std::sync::Mutex;

pub type QueryHandler = Box<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>;

pub struct QueryRegistry {
    handlers: Mutex<HashMap<u64, HashMap<u64, QueryHandler>>>,
}

impl QueryRegistry {
    pub fn new() -> Self { Self { handlers: Mutex::new(HashMap::new()) } }
    pub fn register_handler(&self, workflow_key: u64, query_name_id: u64, handler: QueryHandler) {
        self.handlers.lock().unwrap().entry(workflow_key).or_default().insert(query_name_id, handler);
    }
    pub fn execute_query(&self, workflow_key: u64, query_name_id: u64, input: &[u8]) -> Option<Vec<u8>> {
        let handlers = self.handlers.lock().unwrap();
        handlers.get(&workflow_key)?.get(&query_name_id).map(|h| h(input))
    }
    pub fn has_handler(&self, workflow_key: u64, query_name_id: u64) -> bool {
        self.handlers.lock().unwrap().get(&workflow_key).and_then(|m| m.get(&query_name_id)).is_some()
    }
    pub fn unregister_workflow(&self, workflow_key: u64) { self.handlers.lock().unwrap().remove(&workflow_key); }
    pub fn workflow_count(&self) -> usize { self.handlers.lock().unwrap().len() }
}
impl Default for QueryRegistry { fn default() -> Self { Self::new() } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_register_and_execute() {
        let reg = QueryRegistry::new();
        reg.register_handler(42, 1, Box::new(|input| { let mut r = input.to_vec(); r.push(0xFF); r }));
        let result = reg.execute_query(42, 1, &[1, 2, 3]).unwrap();
        assert_eq!(result, vec![1, 2, 3, 0xFF]);
    }
    #[test]
    fn test_no_handler() {
        let reg = QueryRegistry::new();
        assert!(reg.execute_query(42, 1, &[]).is_none());
    }
}
