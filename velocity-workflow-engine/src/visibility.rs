//! Workflow visibility and search index. Provides O(1) workflow listing by status,
//! type, namespace, and time range. Supports custom search attributes, compound queries,
//! pagination, sort ordering, and aggregation for advanced visibility.

use std::collections::{BTreeMap, HashMap};
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
    /// Memo attached to this workflow (for visibility display).
    pub memo: HashMap<String, Vec<u8>>,
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

/// Sort order for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

/// Sort field for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortField {
    StartTime,
    CloseTime,
    WorkflowKey,
}

/// Pagination token for cursor-based pagination.
#[derive(Debug, Clone)]
pub struct PageToken {
    pub offset: usize,
}

/// A paginated query result.
#[derive(Debug, Clone)]
pub struct PaginatedResult {
    pub items: Vec<WorkflowExecutionInfo>,
    pub next_token: Option<PageToken>,
    pub total_count: usize,
}

/// A compound visibility query combining multiple filters.
#[derive(Debug, Clone)]
pub enum VisibilityFilter {
    Status(WorkflowStatus),
    Namespace(u64),
    WorkflowType(u64),
    TimeRange {
        start_ms: u64,
        end_ms: u64,
    },
    CloseTimeRange {
        start_ms: u64,
        end_ms: u64,
    },
    SearchAttribute {
        key: String,
        value: SearchAttributeValue,
    },
    Prefix(String),
    /// AND: all filters must match.
    And(Vec<VisibilityFilter>),
    /// OR: at least one filter must match.
    Or(Vec<VisibilityFilter>),
    /// NOT: negate a filter.
    Not(Box<VisibilityFilter>),
}

/// Visibility query combining filter, sort, and pagination.
#[derive(Debug, Clone)]
pub struct VisibilityQuery {
    pub filter: VisibilityFilter,
    pub sort_field: SortField,
    pub sort_order: SortOrder,
    pub page_size: usize,
    pub page_token: Option<PageToken>,
}

impl VisibilityQuery {
    pub fn new(filter: VisibilityFilter) -> Self {
        Self {
            filter,
            sort_field: SortField::StartTime,
            sort_order: SortOrder::Descending,
            page_size: 100,
            page_token: None,
        }
    }
    pub fn with_sort(mut self, field: SortField, order: SortOrder) -> Self {
        self.sort_field = field;
        self.sort_order = order;
        self
    }
    pub fn with_pagination(mut self, page_size: usize, token: Option<PageToken>) -> Self {
        self.page_size = page_size;
        self.page_token = token;
        self
    }
}

/// Aggregate counts by various dimensions.
#[derive(Debug, Clone, Default)]
pub struct VisibilityAggregation {
    pub by_status: HashMap<u8, usize>,
    pub by_namespace: HashMap<u64, usize>,
    pub by_type: HashMap<u64, usize>,
    pub total: usize,
}

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
    /// Index by close time (sorted): close_time_ms -> workflow_key.
    by_close_time: RwLock<BTreeMap<u64, Vec<u64>>>,
}

impl VisibilityIndex {
    pub fn new() -> Self {
        Self {
            executions: RwLock::new(HashMap::new()),
            by_status: RwLock::new(HashMap::new()),
            by_namespace: RwLock::new(HashMap::new()),
            by_type: RwLock::new(HashMap::new()),
            by_start_time: RwLock::new(BTreeMap::new()),
            by_close_time: RwLock::new(BTreeMap::new()),
        }
    }

    /// Register a new workflow execution in the index.
    pub fn register(&self, info: WorkflowExecutionInfo) {
        let key = info.workflow_key;

        // Main index
        self.executions.write().unwrap().insert(key, info.clone());

        // Status index
        self.by_status
            .write()
            .unwrap()
            .entry(info.status as u8)
            .or_default()
            .push(key);

        // Namespace index
        self.by_namespace
            .write()
            .unwrap()
            .entry(info.namespace_id)
            .or_default()
            .push(key);

        // Type index
        self.by_type
            .write()
            .unwrap()
            .entry(info.workflow_type_id)
            .or_default()
            .push(key);

        // Time index
        self.by_start_time
            .write()
            .unwrap()
            .entry(info.start_time_ms)
            .or_default()
            .push(key);

        // Close time index (if already closed)
        if let Some(ct) = info.close_time_ms {
            self.by_close_time
                .write()
                .unwrap()
                .entry(ct)
                .or_default()
                .push(key);
        }
    }

    /// Update the status of a workflow (e.g., Running -> Completed).
    pub fn update_status(
        &self,
        workflow_key: u64,
        new_status: WorkflowStatus,
        close_time_ms: Option<u64>,
    ) {
        let mut executions = self.executions.write().unwrap();
        if let Some(info) = executions.get_mut(&workflow_key) {
            let old_status = info.status as u8;
            info.status = new_status;
            info.close_time_ms = close_time_ms;

            // Update close time index
            if let Some(ct) = close_time_ms {
                self.by_close_time
                    .write()
                    .unwrap()
                    .entry(ct)
                    .or_default()
                    .push(workflow_key);
            }

            // Update status index
            let mut by_status = self.by_status.write().unwrap();
            if let Some(keys) = by_status.get_mut(&old_status) {
                keys.retain(|k| *k != workflow_key);
            }
            by_status
                .entry(new_status as u8)
                .or_default()
                .push(workflow_key);
        }
    }

    /// Set a custom search attribute on a workflow.
    pub fn set_search_attribute(
        &self,
        workflow_key: u64,
        key: String,
        value: SearchAttributeValue,
    ) {
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
        by_status
            .get(&(status as u8))
            .map(|keys| {
                keys.iter()
                    .filter_map(|k| executions.get(k).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List workflows by namespace.
    pub fn list_by_namespace(&self, namespace_id: u64) -> Vec<WorkflowExecutionInfo> {
        let by_ns = self.by_namespace.read().unwrap();
        let executions = self.executions.read().unwrap();
        by_ns
            .get(&namespace_id)
            .map(|keys| {
                keys.iter()
                    .filter_map(|k| executions.get(k).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List workflows by type.
    pub fn list_by_type(&self, type_id: u64) -> Vec<WorkflowExecutionInfo> {
        let by_type = self.by_type.read().unwrap();
        let executions = self.executions.read().unwrap();
        by_type
            .get(&type_id)
            .map(|keys| {
                keys.iter()
                    .filter_map(|k| executions.get(k).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List workflows started within a time range (inclusive).
    pub fn list_by_time_range(&self, start_ms: u64, end_ms: u64) -> Vec<WorkflowExecutionInfo> {
        let by_time = self.by_start_time.read().unwrap();
        let executions = self.executions.read().unwrap();
        by_time
            .range(start_ms..=end_ms)
            .flat_map(|(_, keys)| keys.iter().filter_map(|k| executions.get(k).cloned()))
            .collect()
    }

    /// List workflows matching a custom search attribute.
    pub fn list_by_search_attribute(
        &self,
        key: &str,
        value: &SearchAttributeValue,
    ) -> Vec<WorkflowExecutionInfo> {
        let executions = self.executions.read().unwrap();
        executions
            .values()
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
            let mut by_status = self.by_status.write().unwrap();
            if let Some(keys) = by_status.get_mut(&(info.status as u8)) {
                keys.retain(|k| *k != workflow_key);
            }
            let mut by_ns = self.by_namespace.write().unwrap();
            if let Some(keys) = by_ns.get_mut(&info.namespace_id) {
                keys.retain(|k| *k != workflow_key);
            }
            let mut by_type = self.by_type.write().unwrap();
            if let Some(keys) = by_type.get_mut(&info.workflow_type_id) {
                keys.retain(|k| *k != workflow_key);
            }
            let mut by_time = self.by_start_time.write().unwrap();
            if let Some(keys) = by_time.get_mut(&info.start_time_ms) {
                keys.retain(|k| *k != workflow_key);
            }
            if let Some(ct) = info.close_time_ms {
                let mut by_ct = self.by_close_time.write().unwrap();
                if let Some(keys) = by_ct.get_mut(&ct) {
                    keys.retain(|k| *k != workflow_key);
                }
            }
        }
    }

    // ─── Advanced Query Methods ─────────────────────────────────────────

    /// Execute a compound visibility query with filtering, sorting, and pagination.
    pub fn execute_query(&self, query: &VisibilityQuery) -> PaginatedResult {
        let executions = self.executions.read().unwrap();
        let mut matched: Vec<WorkflowExecutionInfo> = executions
            .values()
            .filter(|info| self.matches_filter(info, &query.filter))
            .cloned()
            .collect();

        // Sort
        matched.sort_by(|a, b| {
            let cmp = match query.sort_field {
                SortField::StartTime => a.start_time_ms.cmp(&b.start_time_ms),
                SortField::CloseTime => a
                    .close_time_ms
                    .unwrap_or(0)
                    .cmp(&b.close_time_ms.unwrap_or(0)),
                SortField::WorkflowKey => a.workflow_key.cmp(&b.workflow_key),
            };
            if query.sort_order == SortOrder::Descending {
                cmp.reverse()
            } else {
                cmp
            }
        });

        let total_count = matched.len();
        let offset = query.page_token.as_ref().map_or(0, |t| t.offset);
        let page: Vec<_> = matched
            .into_iter()
            .skip(offset)
            .take(query.page_size)
            .collect();
        let next_offset = offset + page.len();
        let next_token = if next_offset < total_count {
            Some(PageToken {
                offset: next_offset,
            })
        } else {
            None
        };

        PaginatedResult {
            items: page,
            next_token,
            total_count,
        }
    }

    /// Check if a workflow matches a filter.
    fn matches_filter(&self, info: &WorkflowExecutionInfo, filter: &VisibilityFilter) -> bool {
        match filter {
            VisibilityFilter::Status(s) => info.status == *s,
            VisibilityFilter::Namespace(ns) => info.namespace_id == *ns,
            VisibilityFilter::WorkflowType(wt) => info.workflow_type_id == *wt,
            VisibilityFilter::TimeRange { start_ms, end_ms } => {
                info.start_time_ms >= *start_ms && info.start_time_ms <= *end_ms
            }
            VisibilityFilter::CloseTimeRange { start_ms, end_ms } => info
                .close_time_ms
                .is_some_and(|ct| ct >= *start_ms && ct <= *end_ms),
            VisibilityFilter::SearchAttribute { key, value } => {
                info.search_attributes.get(key) == Some(value)
            }
            VisibilityFilter::Prefix(prefix) => {
                // Match against workflow_id as string
                let wid_str = info.workflow_id.to_string();
                wid_str.starts_with(prefix.as_str())
            }
            VisibilityFilter::And(filters) => filters.iter().all(|f| self.matches_filter(info, f)),
            VisibilityFilter::Or(filters) => filters.iter().any(|f| self.matches_filter(info, f)),
            VisibilityFilter::Not(inner) => !self.matches_filter(info, inner),
        }
    }

    /// List workflows by close time range.
    pub fn list_by_close_time_range(
        &self,
        start_ms: u64,
        end_ms: u64,
    ) -> Vec<WorkflowExecutionInfo> {
        let by_ct = self.by_close_time.read().unwrap();
        let executions = self.executions.read().unwrap();
        by_ct
            .range(start_ms..=end_ms)
            .flat_map(|(_, keys)| keys.iter().filter_map(|k| executions.get(k).cloned()))
            .collect()
    }

    /// Get aggregate counts across all dimensions.
    pub fn aggregate(&self) -> VisibilityAggregation {
        let executions = self.executions.read().unwrap();
        let mut agg = VisibilityAggregation {
            total: executions.len(),
            ..Default::default()
        };
        for info in executions.values() {
            *agg.by_status.entry(info.status as u8).or_insert(0) += 1;
            *agg.by_namespace.entry(info.namespace_id).or_insert(0) += 1;
            *agg.by_type.entry(info.workflow_type_id).or_insert(0) += 1;
        }
        agg
    }

    /// Set memo on a workflow execution info.
    pub fn set_memo(&self, workflow_key: u64, memo: HashMap<String, Vec<u8>>) {
        let mut executions = self.executions.write().unwrap();
        if let Some(info) = executions.get_mut(&workflow_key) {
            info.memo = memo;
        }
    }

    /// List workflows matching a prefix on their workflow ID.
    pub fn list_by_prefix(&self, prefix: &str) -> Vec<WorkflowExecutionInfo> {
        let executions = self.executions.read().unwrap();
        executions
            .values()
            .filter(|info| info.workflow_id.to_string().starts_with(prefix))
            .cloned()
            .collect()
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
            memo: HashMap::new(),
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

        index.set_search_attribute(
            1,
            "customer_id".into(),
            SearchAttributeValue::String("C123".into()),
        );

        let results = index
            .list_by_search_attribute("customer_id", &SearchAttributeValue::String("C123".into()));
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

    #[test]
    fn test_compound_query_and() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running));
        index.register(make_info(2, 0, 101, WorkflowStatus::Running));
        index.register(make_info(3, 1, 100, WorkflowStatus::Completed));

        let q = VisibilityQuery::new(VisibilityFilter::And(vec![
            VisibilityFilter::Namespace(0),
            VisibilityFilter::Status(WorkflowStatus::Running),
        ]));
        let result = index.execute_query(&q);
        assert_eq!(result.items.len(), 2);
        assert_eq!(result.total_count, 2);
    }

    #[test]
    fn test_compound_query_or() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running));
        index.register(make_info(2, 0, 101, WorkflowStatus::Completed));
        index.register(make_info(3, 1, 100, WorkflowStatus::Failed));

        let q = VisibilityQuery::new(VisibilityFilter::Or(vec![
            VisibilityFilter::Status(WorkflowStatus::Running),
            VisibilityFilter::Status(WorkflowStatus::Completed),
        ]));
        let result = index.execute_query(&q);
        assert_eq!(result.items.len(), 2);
    }

    #[test]
    fn test_pagination() {
        let index = VisibilityIndex::new();
        for i in 0..10 {
            index.register(make_info(i, 0, 100, WorkflowStatus::Running));
        }

        let q = VisibilityQuery::new(VisibilityFilter::Status(WorkflowStatus::Running))
            .with_pagination(3, None);
        let result = index.execute_query(&q);
        assert_eq!(result.items.len(), 3);
        assert_eq!(result.total_count, 10);
        assert!(result.next_token.is_some());

        // Second page
        let q2 = VisibilityQuery::new(VisibilityFilter::Status(WorkflowStatus::Running))
            .with_pagination(3, result.next_token);
        let result2 = index.execute_query(&q2);
        assert_eq!(result2.items.len(), 3);
    }

    #[test]
    fn test_sort_order() {
        let index = VisibilityIndex::new();
        index.register(make_info(3, 0, 100, WorkflowStatus::Running)); // start=3000
        index.register(make_info(1, 0, 100, WorkflowStatus::Running)); // start=1000
        index.register(make_info(2, 0, 100, WorkflowStatus::Running)); // start=2000

        let q_asc = VisibilityQuery::new(VisibilityFilter::Status(WorkflowStatus::Running))
            .with_sort(SortField::StartTime, SortOrder::Ascending);
        let asc = index.execute_query(&q_asc);
        assert_eq!(asc.items[0].start_time_ms, 1000);
        assert_eq!(asc.items[2].start_time_ms, 3000);

        let q_desc = VisibilityQuery::new(VisibilityFilter::Status(WorkflowStatus::Running))
            .with_sort(SortField::StartTime, SortOrder::Descending);
        let desc = index.execute_query(&q_desc);
        assert_eq!(desc.items[0].start_time_ms, 3000);
        assert_eq!(desc.items[2].start_time_ms, 1000);
    }

    #[test]
    fn test_aggregation() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running));
        index.register(make_info(2, 0, 100, WorkflowStatus::Completed));
        index.register(make_info(3, 1, 101, WorkflowStatus::Running));

        let agg = index.aggregate();
        assert_eq!(agg.total, 3);
        assert_eq!(agg.by_namespace.get(&0), Some(&2));
        assert_eq!(agg.by_namespace.get(&1), Some(&1));
    }

    #[test]
    fn test_close_time_range() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running));
        index.update_status(1, WorkflowStatus::Completed, Some(5000));
        index.register(make_info(2, 0, 100, WorkflowStatus::Running));
        index.update_status(2, WorkflowStatus::Completed, Some(8000));

        let results = index.list_by_close_time_range(4000, 6000);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].workflow_key, 1);
    }

    #[test]
    fn test_not_filter() {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 0, 100, WorkflowStatus::Running));
        index.register(make_info(2, 0, 100, WorkflowStatus::Completed));

        let q = VisibilityQuery::new(VisibilityFilter::Not(Box::new(VisibilityFilter::Status(
            WorkflowStatus::Running,
        ))));
        let result = index.execute_query(&q);
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].status, WorkflowStatus::Completed);
    }
}
