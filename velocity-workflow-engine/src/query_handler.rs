//! Query handler registry — named query handlers for workflow state inspection.
//!
//! Queries are synchronous requests that read workflow state without modifying it.
//! This module provides:
//! - **Typed query definitions**: Input/output schemas with validation
//! - **Query lifecycle tracking**: Pending → Dispatched → Completed/Failed/TimedOut/Rejected
//! - **Execution context**: Timeouts, consistency levels, rejection policies
//! - **Buffered queries**: Queue queries until workflow reaches a consistent state
//! - **Statistics**: Query throughput, latency, rejection rates per workflow

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The callable query handler type.
pub type QueryHandler = Box<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>;

/// Consistency level for query execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum QueryConsistency {
    /// Execute immediately against current state (may be stale).
    Eventual = 0,
    /// Wait until the workflow has processed all pending commands before executing.
    Strong = 1,
    /// Execute only if the workflow is in a known consistent state (e.g., between commands).
    Scoped = 2,
}

/// Current lifecycle state of a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum QueryState {
    /// Query has been received but not yet dispatched to a handler.
    Pending = 0,
    /// Query is being executed by a handler.
    Dispatched = 1,
    /// Query completed successfully.
    Completed = 2,
    /// Query execution failed.
    Failed = 3,
    /// Query timed out before completion.
    TimedOut = 4,
    /// Query was rejected by the workflow (e.g., wrong state).
    Rejected = 5,
}

/// A registered query definition with metadata.
#[derive(Debug, Clone)]
pub struct QueryDefinition {
    /// The query name (string form, for diagnostics).
    pub name: String,
    /// Expected input schema hint (opaque bytes, for validation).
    pub input_schema_hint: Vec<u8>,
    /// Expected output schema hint.
    pub output_schema_hint: Vec<u8>,
    /// Required consistency level.
    pub consistency: QueryConsistency,
    /// Maximum time to wait for query execution.
    pub timeout: Duration,
    /// If true, queries are buffered until workflow reaches a consistent state.
    pub buffer_until_consistent: bool,
}

impl QueryDefinition {
    /// Create a new query definition with defaults.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            input_schema_hint: Vec::new(),
            output_schema_hint: Vec::new(),
            consistency: QueryConsistency::Eventual,
            timeout: Duration::from_secs(10),
            buffer_until_consistent: false,
        }
    }

    /// Set the consistency level.
    pub fn with_consistency(mut self, c: QueryConsistency) -> Self {
        self.consistency = c;
        self
    }

    /// Set the timeout.
    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }

    /// Enable buffering until consistent.
    pub fn with_buffer_until_consistent(mut self, enabled: bool) -> Self {
        self.buffer_until_consistent = enabled;
        self
    }
}

/// A record of a query execution for tracking and diagnostics.
#[derive(Debug, Clone)]
pub struct QueryRecord {
    /// Unique query ID.
    pub query_id: u64,
    /// The workflow this query targets.
    pub workflow_key: u64,
    /// The query name hash (registered handler).
    pub query_name_id: u64,
    /// Current state of the query.
    pub state: QueryState,
    /// When the query was received.
    pub received_at: Instant,
    /// When the query was dispatched to a handler.
    pub dispatched_at: Option<Instant>,
    /// When the query completed/failed/timed_out/rejected.
    pub completed_at: Option<Instant>,
    /// The result payload (if completed).
    pub result: Option<Vec<u8>>,
    /// Error message (if failed/rejected).
    pub error: Option<String>,
}

impl QueryRecord {
    /// Get the latency from received to completed.
    pub fn latency(&self) -> Option<Duration> {
        match (self.completed_at, self.received_at) {
            (Some(completed), received) => Some(completed.duration_since(received)),
            _ => None,
        }
    }

    /// Check if this query has timed out based on the given deadline.
    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        self.state == QueryState::Pending
            || self.state == QueryState::Dispatched && self.received_at.elapsed() > timeout
    }
}

/// Aggregate query statistics.
#[derive(Debug, Clone, Default)]
pub struct QueryStats {
    pub total_queries: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub total_timed_out: u64,
    pub total_rejected: u64,
    pub total_buffered: u64,
    pub currently_pending: u64,
    pub currently_buffered: u64,
}

/// Rejection policy for queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionPolicy {
    /// Reject if the workflow is not running.
    NotRunning = 0,
    /// Reject if the workflow is not in a consistent state.
    NotConsistent = 1,
    /// Never reject (always buffer or execute).
    Never = 2,
}

/// A buffered query waiting for workflow consistency.
#[derive(Debug, Clone)]
pub struct BufferedQuery {
    pub query_id: u64,
    pub workflow_key: u64,
    pub query_name_id: u64,
    pub input: Vec<u8>,
    pub buffered_at: Instant,
    pub max_wait: Duration,
}

/// The query registry manages all query handlers and execution state.
///
/// Thread-safe: uses internal `Mutex` for concurrent access.
pub struct QueryRegistry {
    /// Registered handlers: workflow_key → (query_name_id → handler).
    handlers: Mutex<HashMap<u64, HashMap<u64, QueryHandler>>>,
    /// Query definitions: workflow_key → (query_name_id → definition).
    definitions: Mutex<HashMap<u64, HashMap<u64, QueryDefinition>>>,
    /// Active query records: query_id → record.
    records: Mutex<HashMap<u64, QueryRecord>>,
    /// Buffered queries per workflow: workflow_key → queue.
    buffered: Mutex<HashMap<u64, VecDeque<BufferedQuery>>>,
    /// Next query ID counter.
    next_query_id: Mutex<u64>,
    /// Aggregate statistics.
    stats: Mutex<QueryStats>,
}

impl QueryRegistry {
    /// Create a new empty query registry.
    pub fn new() -> Self {
        Self {
            handlers: Mutex::new(HashMap::new()),
            definitions: Mutex::new(HashMap::new()),
            records: Mutex::new(HashMap::new()),
            buffered: Mutex::new(HashMap::new()),
            next_query_id: Mutex::new(1),
            stats: Mutex::new(QueryStats::default()),
        }
    }

    /// Register a query handler for a workflow.
    pub fn register_handler(&self, workflow_key: u64, query_name_id: u64, handler: QueryHandler) {
        self.handlers
            .lock()
            .unwrap()
            .entry(workflow_key)
            .or_default()
            .insert(query_name_id, handler);
    }

    /// Register a query handler with a typed definition.
    pub fn register_typed_handler(
        &self,
        workflow_key: u64,
        query_name_id: u64,
        definition: QueryDefinition,
        handler: QueryHandler,
    ) {
        self.definitions
            .lock()
            .unwrap()
            .entry(workflow_key)
            .or_default()
            .insert(query_name_id, definition);
        self.handlers
            .lock()
            .unwrap()
            .entry(workflow_key)
            .or_default()
            .insert(query_name_id, handler);
    }

    /// Submit a query for execution. Returns a query ID for tracking.
    ///
    /// If the query has a definition requiring consistency and the workflow
    /// is not consistent, the query is buffered instead of executed.
    pub fn submit_query(&self, workflow_key: u64, query_name_id: u64, input: &[u8]) -> u64 {
        let mut id_gen = self.next_query_id.lock().unwrap();
        let query_id = *id_gen;
        *id_gen += 1;
        drop(id_gen);

        let record = QueryRecord {
            query_id,
            workflow_key,
            query_name_id,
            state: QueryState::Pending,
            received_at: Instant::now(),
            dispatched_at: None,
            completed_at: None,
            result: None,
            error: None,
        };

        self.records.lock().unwrap().insert(query_id, record);
        self.stats.lock().unwrap().total_queries += 1;
        self.stats.lock().unwrap().currently_pending += 1;

        // Check if we need to buffer this query
        let should_buffer = {
            let defs = self.definitions.lock().unwrap();
            if let Some(wf_defs) = defs.get(&workflow_key) {
                if let Some(def) = wf_defs.get(&query_name_id) {
                    def.buffer_until_consistent
                } else {
                    false
                }
            } else {
                false
            }
        };

        if should_buffer {
            let timeout = {
                let defs = self.definitions.lock().unwrap();
                defs.get(&workflow_key)
                    .and_then(|m| m.get(&query_name_id))
                    .map(|d| d.timeout)
                    .unwrap_or(Duration::from_secs(10))
            };

            let buffered = BufferedQuery {
                query_id,
                workflow_key,
                query_name_id,
                input: input.to_vec(),
                buffered_at: Instant::now(),
                max_wait: timeout,
            };

            self.buffered
                .lock()
                .unwrap()
                .entry(workflow_key)
                .or_default()
                .push_back(buffered);

            self.stats.lock().unwrap().total_buffered += 1;
            self.stats.lock().unwrap().currently_buffered += 1;
            self.stats.lock().unwrap().currently_pending -= 1;
        }

        query_id
    }

    /// Execute a query immediately (bypassing buffering). Returns the result.
    pub fn execute_query(
        &self,
        workflow_key: u64,
        query_name_id: u64,
        input: &[u8],
    ) -> Option<Vec<u8>> {
        let handlers = self.handlers.lock().unwrap();
        handlers
            .get(&workflow_key)?
            .get(&query_name_id)
            .map(|h| h(input))
    }

    /// Execute a submitted query by ID. Updates the query record with the result.
    pub fn execute_query_by_id(&self, query_id: u64, input: &[u8]) -> Option<Vec<u8>> {
        let (workflow_key, query_name_id) = {
            let records = self.records.lock().unwrap();
            let record = records.get(&query_id)?;
            (record.workflow_key, record.query_name_id)
        };

        // Mark as dispatched
        {
            let mut records = self.records.lock().unwrap();
            if let Some(record) = records.get_mut(&query_id) {
                record.state = QueryState::Dispatched;
                record.dispatched_at = Some(Instant::now());
            }
        }

        // Execute the handler
        let handlers = self.handlers.lock().unwrap();
        let result = handlers
            .get(&workflow_key)
            .and_then(|m| m.get(&query_name_id))
            .map(|h| h(input));

        // Update the record
        let mut records = self.records.lock().unwrap();
        if let Some(record) = records.get_mut(&query_id) {
            record.completed_at = Some(Instant::now());
            match &result {
                Some(val) => {
                    record.state = QueryState::Completed;
                    record.result = Some(val.clone());
                }
                None => {
                    record.state = QueryState::Failed;
                    record.error = Some("No handler found".to_string());
                }
            }
        }

        let mut stats = self.stats.lock().unwrap();
        stats.currently_pending = stats.currently_pending.saturating_sub(1);
        if result.is_some() {
            stats.total_completed += 1;
        } else {
            stats.total_failed += 1;
        }

        result
    }

    /// Reject a query (e.g., workflow not in correct state).
    pub fn reject_query(&self, query_id: u64, reason: &str) {
        let mut records = self.records.lock().unwrap();
        if let Some(record) = records.get_mut(&query_id) {
            record.state = QueryState::Rejected;
            record.completed_at = Some(Instant::now());
            record.error = Some(reason.to_string());
        }
        let mut stats = self.stats.lock().unwrap();
        stats.total_rejected += 1;
        stats.currently_pending = stats.currently_pending.saturating_sub(1);
    }

    /// Mark a query as timed out.
    pub fn timeout_query(&self, query_id: u64) {
        let mut records = self.records.lock().unwrap();
        if let Some(record) = records.get_mut(&query_id) {
            record.state = QueryState::TimedOut;
            record.completed_at = Some(Instant::now());
            record.error = Some("Query timed out".to_string());
        }
        let mut stats = self.stats.lock().unwrap();
        stats.total_timed_out += 1;
        stats.currently_pending = stats.currently_pending.saturating_sub(1);
    }

    /// Check for timed-out queries and return their IDs.
    pub fn check_timeouts(&self) -> Vec<u64> {
        let defs = self.definitions.lock().unwrap();
        let mut records = self.records.lock().unwrap();
        let mut timed_out = Vec::new();

        for (query_id, record) in records.iter_mut() {
            if record.state == QueryState::Pending || record.state == QueryState::Dispatched {
                let timeout = defs
                    .get(&record.workflow_key)
                    .and_then(|m| m.get(&record.query_name_id))
                    .map(|d| d.timeout)
                    .unwrap_or(Duration::from_secs(10));

                if record.received_at.elapsed() > timeout {
                    record.state = QueryState::TimedOut;
                    record.completed_at = Some(Instant::now());
                    record.error = Some("Query timed out".to_string());
                    timed_out.push(*query_id);
                }
            }
        }

        if !timed_out.is_empty() {
            let mut stats = self.stats.lock().unwrap();
            stats.total_timed_out += timed_out.len() as u64;
            stats.currently_pending = stats
                .currently_pending
                .saturating_sub(timed_out.len() as u64);
        }

        timed_out
    }

    /// Flush buffered queries for a workflow that has reached a consistent state.
    /// Returns the flushed queries so they can be executed.
    pub fn flush_buffered(&self, workflow_key: u64) -> Vec<BufferedQuery> {
        let mut buffered = self.buffered.lock().unwrap();
        let queries = buffered.remove(&workflow_key).unwrap_or_default();
        let count = queries.len();

        // Remove expired buffered queries
        let (valid, expired): (Vec<_>, Vec<_>) = queries
            .into_iter()
            .partition(|q| q.buffered_at.elapsed() < q.max_wait);

        if !expired.is_empty() {
            let mut records = self.records.lock().unwrap();
            for eq in &expired {
                if let Some(record) = records.get_mut(&eq.query_id) {
                    record.state = QueryState::TimedOut;
                    record.completed_at = Some(Instant::now());
                    record.error = Some("Buffered query timed out".to_string());
                }
            }
            let mut stats = self.stats.lock().unwrap();
            stats.total_timed_out += expired.len() as u64;
            stats.currently_buffered = stats
                .currently_buffered
                .saturating_sub(expired.len() as u64);
        }

        // Mark valid queries as pending execution
        {
            let mut records = self.records.lock().unwrap();
            for vq in &valid {
                if let Some(record) = records.get_mut(&vq.query_id) {
                    record.state = QueryState::Pending;
                }
            }
        }

        let mut stats = self.stats.lock().unwrap();
        stats.currently_buffered = stats.currently_buffered.saturating_sub(count as u64);

        valid
    }

    /// Get the number of buffered queries for a workflow.
    pub fn buffered_count(&self, workflow_key: u64) -> usize {
        self.buffered
            .lock()
            .unwrap()
            .get(&workflow_key)
            .map_or(0, |q| q.len())
    }

    /// Get a query record by ID.
    pub fn get_record(&self, query_id: u64) -> Option<QueryRecord> {
        self.records.lock().unwrap().get(&query_id).cloned()
    }

    /// Check if a handler is registered.
    pub fn has_handler(&self, workflow_key: u64, query_name_id: u64) -> bool {
        self.handlers
            .lock()
            .unwrap()
            .get(&workflow_key)
            .and_then(|m| m.get(&query_name_id))
            .is_some()
    }

    /// Get the query definition for a handler.
    pub fn get_definition(&self, workflow_key: u64, query_name_id: u64) -> Option<QueryDefinition> {
        self.definitions
            .lock()
            .unwrap()
            .get(&workflow_key)
            .and_then(|m| m.get(&query_name_id))
            .cloned()
    }

    /// List all registered query name IDs for a workflow.
    pub fn list_queries(&self, workflow_key: u64) -> Vec<u64> {
        self.handlers
            .lock()
            .unwrap()
            .get(&workflow_key)
            .map(|m| m.keys().copied().collect())
            .unwrap_or_default()
    }

    /// List all registered query definitions for a workflow.
    pub fn list_definitions(&self, workflow_key: u64) -> Vec<(u64, QueryDefinition)> {
        self.definitions
            .lock()
            .unwrap()
            .get(&workflow_key)
            .map(|m| m.iter().map(|(k, v)| (*k, v.clone())).collect())
            .unwrap_or_default()
    }

    /// Unregister all handlers and definitions for a workflow.
    /// Also rejects any pending queries for that workflow.
    pub fn unregister_workflow(&self, workflow_key: u64) {
        self.handlers.lock().unwrap().remove(&workflow_key);
        self.definitions.lock().unwrap().remove(&workflow_key);
        self.buffered.lock().unwrap().remove(&workflow_key);

        // Reject all pending queries for this workflow
        let mut records = self.records.lock().unwrap();
        let pending_ids: Vec<u64> = records
            .iter()
            .filter(|(_, r)| {
                r.workflow_key == workflow_key
                    && (r.state == QueryState::Pending || r.state == QueryState::Dispatched)
            })
            .map(|(id, _)| *id)
            .collect();

        for id in &pending_ids {
            if let Some(record) = records.get_mut(id) {
                record.state = QueryState::Rejected;
                record.completed_at = Some(Instant::now());
                record.error = Some("Workflow unregistered".to_string());
            }
        }

        let mut stats = self.stats.lock().unwrap();
        stats.total_rejected += pending_ids.len() as u64;
        stats.currently_pending = stats
            .currently_pending
            .saturating_sub(pending_ids.len() as u64);
    }

    /// Get aggregate statistics.
    pub fn stats(&self) -> QueryStats {
        self.stats.lock().unwrap().clone()
    }

    /// Get the total number of workflows with registered handlers.
    pub fn workflow_count(&self) -> usize {
        self.handlers.lock().unwrap().len()
    }

    /// Get the total number of active (pending/dispatched) queries.
    pub fn active_query_count(&self) -> usize {
        self.records
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.state == QueryState::Pending || r.state == QueryState::Dispatched)
            .count()
    }

    /// Purge completed/failed/timed_out/rejected query records older than the given age.
    pub fn purge_completed(&self, older_than: Duration) -> usize {
        let mut records = self.records.lock().unwrap();
        let before = records.len();
        records.retain(|_, r| {
            let is_terminal = matches!(
                r.state,
                QueryState::Completed
                    | QueryState::Failed
                    | QueryState::TimedOut
                    | QueryState::Rejected
            );
            if is_terminal {
                if let Some(completed_at) = r.completed_at {
                    return completed_at.elapsed() < older_than;
                }
            }
            true
        });
        before - records.len()
    }
}

impl Default for QueryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_execute() {
        let reg = QueryRegistry::new();
        reg.register_handler(
            42,
            1,
            Box::new(|input| {
                let mut r = input.to_vec();
                r.push(0xFF);
                r
            }),
        );
        let result = reg.execute_query(42, 1, &[1, 2, 3]).unwrap();
        assert_eq!(result, vec![1, 2, 3, 0xFF]);
    }

    #[test]
    fn test_no_handler() {
        let reg = QueryRegistry::new();
        assert!(reg.execute_query(42, 1, &[]).is_none());
    }

    #[test]
    fn test_typed_handler_with_definition() {
        let reg = QueryRegistry::new();
        let def = QueryDefinition::new("get_status")
            .with_consistency(QueryConsistency::Strong)
            .with_timeout(Duration::from_secs(5));

        reg.register_typed_handler(1, 100, def.clone(), Box::new(|_| b"running".to_vec()));

        let stored_def = reg.get_definition(1, 100).unwrap();
        assert_eq!(stored_def.name, "get_status");
        assert_eq!(stored_def.consistency, QueryConsistency::Strong);
        assert_eq!(stored_def.timeout, Duration::from_secs(5));

        let result = reg.execute_query(1, 100, &[]).unwrap();
        assert_eq!(result, b"running");
    }

    #[test]
    fn test_submit_and_execute_by_id() {
        let reg = QueryRegistry::new();
        reg.register_handler(42, 1, Box::new(|input| input.to_vec()));

        let query_id = reg.submit_query(42, 1, &[10, 20]);
        assert!(query_id > 0);

        let record = reg.get_record(query_id).unwrap();
        assert_eq!(record.state, QueryState::Pending);

        let result = reg.execute_query_by_id(query_id, &[10, 20]).unwrap();
        assert_eq!(result, vec![10, 20]);

        let record = reg.get_record(query_id).unwrap();
        assert_eq!(record.state, QueryState::Completed);
    }

    #[test]
    fn test_reject_query() {
        let reg = QueryRegistry::new();
        reg.register_handler(42, 1, Box::new(|_| vec![]));

        let query_id = reg.submit_query(42, 1, &[]);
        reg.reject_query(query_id, "workflow not running");

        let record = reg.get_record(query_id).unwrap();
        assert_eq!(record.state, QueryState::Rejected);
        assert_eq!(record.error.as_deref(), Some("workflow not running"));
    }

    #[test]
    fn test_timeout_query() {
        let reg = QueryRegistry::new();
        reg.register_handler(42, 1, Box::new(|_| vec![]));

        let query_id = reg.submit_query(42, 1, &[]);
        reg.timeout_query(query_id);

        let record = reg.get_record(query_id).unwrap();
        assert_eq!(record.state, QueryState::TimedOut);
    }

    #[test]
    fn test_buffered_queries() {
        let reg = QueryRegistry::new();
        let def = QueryDefinition::new("consistent_query").with_buffer_until_consistent(true);

        reg.register_typed_handler(42, 1, def, Box::new(|_| b"ok".to_vec()));

        let qid = reg.submit_query(42, 1, &[]);
        assert_eq!(reg.buffered_count(42), 1);

        // Flush buffered queries
        let flushed = reg.flush_buffered(42);
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].query_id, qid);
        assert_eq!(reg.buffered_count(42), 0);
    }

    #[test]
    fn test_list_queries() {
        let reg = QueryRegistry::new();
        reg.register_handler(42, 1, Box::new(|_| vec![]));
        reg.register_handler(42, 2, Box::new(|_| vec![]));
        reg.register_handler(42, 3, Box::new(|_| vec![]));

        let queries = reg.list_queries(42);
        assert_eq!(queries.len(), 3);
        assert!(queries.contains(&1));
        assert!(queries.contains(&2));
        assert!(queries.contains(&3));
    }

    #[test]
    fn test_unregister_workflow() {
        let reg = QueryRegistry::new();
        reg.register_handler(42, 1, Box::new(|_| vec![]));
        reg.register_handler(42, 2, Box::new(|_| vec![]));

        // Submit a pending query
        let qid = reg.submit_query(42, 1, &[]);

        reg.unregister_workflow(42);

        assert!(!reg.has_handler(42, 1));
        assert!(!reg.has_handler(42, 2));
        assert_eq!(reg.workflow_count(), 0);

        // Pending query should be rejected
        let record = reg.get_record(qid).unwrap();
        assert_eq!(record.state, QueryState::Rejected);
    }

    #[test]
    fn test_stats() {
        let reg = QueryRegistry::new();
        reg.register_handler(42, 1, Box::new(|_| b"ok".to_vec()));

        let qid1 = reg.submit_query(42, 1, &[]);
        let _qid2 = reg.submit_query(42, 1, &[]);

        reg.execute_query_by_id(qid1, &[]);
        reg.reject_query(qid1, "test"); // already completed, but tests the counter

        let stats = reg.stats();
        assert_eq!(stats.total_queries, 2);
        assert!(stats.total_completed >= 1);
    }

    #[test]
    fn test_purge_completed() {
        let reg = QueryRegistry::new();
        reg.register_handler(42, 1, Box::new(|_| vec![]));

        let qid = reg.submit_query(42, 1, &[]);
        reg.execute_query_by_id(qid, &[]);

        // Purge with very short age — should purge everything
        std::thread::sleep(Duration::from_millis(10));
        let purged = reg.purge_completed(Duration::from_millis(1));
        assert_eq!(purged, 1);
    }

    #[test]
    fn test_query_record_latency() {
        let reg = QueryRegistry::new();
        reg.register_handler(42, 1, Box::new(|_| vec![]));

        let qid = reg.submit_query(42, 1, &[]);
        std::thread::sleep(Duration::from_millis(5));
        reg.execute_query_by_id(qid, &[]);

        let record = reg.get_record(qid).unwrap();
        let latency = record.latency().unwrap();
        assert!(latency >= Duration::from_millis(5));
    }
}
