//! Search Query Executor — evaluates parsed SQL-like queries against the visibility index.
//!
//! Bridges the search_query parser with the visibility index to provide
//! Temporal-compatible query execution with field mapping, type coercion,
//! sorting, and pagination.
//!
//! Supported fields:
//! - WorkflowId, RunId, WorkflowType, NamespaceId
//! - ExecutionStatus (Running, Completed, Failed, Canceled, Terminated, TimedOut)
//! - StartTime, CloseTime (epoch millis)
//! - TaskQueue (hash)
//! - Any custom search attribute by name

use crate::engine::WorkflowStatus;
use crate::search_query::{CompareOp, QueryExpr, QueryValue};
use crate::visibility::{
    PaginatedResult, PageToken, SearchAttributeValue, SortField, SortOrder,
    VisibilityIndex, WorkflowExecutionInfo,
};

// ─── Query Executor ────────────────────────────────────────────────────────

/// Executes parsed search queries against a visibility index.
pub struct SearchQueryExecutor<'a> {
    index: &'a VisibilityIndex,
}

impl<'a> SearchQueryExecutor<'a> {
    pub fn new(index: &'a VisibilityIndex) -> Self {
        Self { index }
    }

    /// Execute a parsed query expression and return matching workflows.
    pub fn execute(&self, expr: &QueryExpr) -> Vec<WorkflowExecutionInfo> {
        let all = self.index.list_all();
        all.into_iter()
            .filter(|info| self.evaluate(expr, info))
            .collect()
    }

    /// Execute with sorting and pagination.
    pub fn execute_query(
        &self,
        expr: &QueryExpr,
        sort_field: SortField,
        sort_order: SortOrder,
        page_size: usize,
        page_token: Option<&PageToken>,
    ) -> PaginatedResult {
        let mut results = self.execute(expr);

        // Sort
        results.sort_by(|a, b| {
            let cmp = match sort_field {
                SortField::StartTime => a.start_time_ms.cmp(&b.start_time_ms),
                SortField::CloseTime => {
                    let a_ct = a.close_time_ms.unwrap_or(u64::MAX);
                    let b_ct = b.close_time_ms.unwrap_or(u64::MAX);
                    a_ct.cmp(&b_ct)
                }
                SortField::WorkflowKey => a.workflow_key.cmp(&b.workflow_key),
            };
            match sort_order {
                SortOrder::Ascending => cmp,
                SortOrder::Descending => cmp.reverse(),
            }
        });

        let total_count = results.len();

        // Paginate
        let offset = page_token.map_or(0, |t| t.offset);
        let page: Vec<WorkflowExecutionInfo> = results
            .into_iter()
            .skip(offset)
            .take(page_size)
            .collect();

        let next_token = if offset + page.len() < total_count {
            Some(PageToken {
                offset: offset + page.len(),
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

    /// Execute a query string (parse + evaluate).
    pub fn execute_string(&self, query: &str) -> Result<Vec<WorkflowExecutionInfo>, String> {
        let expr = crate::search_query::parse_query(query).map_err(|e| format!("{}", e))?;
        Ok(self.execute(&expr))
    }

    /// Execute a query string with sorting and pagination.
    pub fn execute_string_query(
        &self,
        query: &str,
        sort_field: SortField,
        sort_order: SortOrder,
        page_size: usize,
        page_token: Option<&PageToken>,
    ) -> Result<PaginatedResult, String> {
        let expr = crate::search_query::parse_query(query).map_err(|e| format!("{}", e))?;
        Ok(self.execute_query(&expr, sort_field, sort_order, page_size, page_token))
    }

    /// Evaluate a query expression against a single workflow execution.
    pub fn evaluate(&self, expr: &QueryExpr, info: &WorkflowExecutionInfo) -> bool {
        match expr {
            QueryExpr::Comparison { field, op, value } => {
                let field_val = self.resolve_field(field, info);
                match field_val {
                    Some(fv) => compare_values(&fv, op, value),
                    None => false,
                }
            }
            QueryExpr::And(left, right) => {
                self.evaluate(left, info) && self.evaluate(right, info)
            }
            QueryExpr::Or(left, right) => {
                self.evaluate(left, info) || self.evaluate(right, info)
            }
            QueryExpr::Not(inner) => !self.evaluate(inner, info),
            QueryExpr::Between { field, low, high } => {
                let field_val = self.resolve_field(field, info);
                match field_val {
                    Some(fv) => {
                        compare_values(&fv, &CompareOp::Ge, low)
                            && compare_values(&fv, &CompareOp::Le, high)
                    }
                    None => false,
                }
            }
            QueryExpr::In { field, values } => {
                let field_val = self.resolve_field(field, info);
                match field_val {
                    Some(fv) => values.iter().any(|v| compare_values(&fv, &CompareOp::Eq, v)),
                    None => false,
                }
            }
            QueryExpr::Like { field, pattern } => {
                let field_val = self.resolve_field(field, info);
                match field_val {
                    Some(ComparableValue::String(s)) => like_match(&s, pattern),
                    _ => false,
                }
            }
            QueryExpr::IsNull(field) => self.resolve_field(field, info).is_none(),
            QueryExpr::IsNotNull(field) => self.resolve_field(field, info).is_some(),
        }
    }

    /// Resolve a field name to a comparable value.
    fn resolve_field(
        &self,
        field: &str,
        info: &WorkflowExecutionInfo,
    ) -> Option<ComparableValue> {
        match field {
            "WorkflowId" | "WorkflowID" => {
                Some(ComparableValue::String(info.workflow_id.to_string()))
            }
            "RunId" | "RunID" => {
                Some(ComparableValue::String(info.run_id.to_string()))
            }
            "WorkflowType" | "WorkflowTypeId" | "WorkflowType_id" => {
                Some(ComparableValue::Integer(info.workflow_type_id as i64))
            }
            "NamespaceId" | "Namespace" => {
                Some(ComparableValue::Integer(info.namespace_id as i64))
            }
            "ExecutionStatus" | "Status" => {
                let status_str = status_to_string(info.status);
                Some(ComparableValue::String(status_str))
            }
            "StartTime" | "ExecutionTime" | "StartTimestamp" => {
                Some(ComparableValue::Integer(info.start_time_ms as i64))
            }
            "CloseTime" | "CloseTimestamp" => {
                info.close_time_ms.map(|t| ComparableValue::Integer(t as i64))
            }
            "TaskQueue" | "TaskQueueHash" => {
                Some(ComparableValue::Integer(info.task_queue_hash as i64))
            }
            _ => {
                // Look up in custom search attributes
                info.search_attributes.get(field).map(|v| match v {
                    SearchAttributeValue::String(s) => ComparableValue::String(s.clone()),
                    SearchAttributeValue::Keyword(s) => ComparableValue::String(s.clone()),
                    SearchAttributeValue::Integer(i) => ComparableValue::Integer(*i),
                    SearchAttributeValue::Double(d) => ComparableValue::Double(*d),
                    SearchAttributeValue::Bool(b) => ComparableValue::Bool(*b),
                    SearchAttributeValue::DateTime(t) => ComparableValue::Integer(*t as i64),
                })
            }
        }
    }
}

// ─── Comparable Values ────────────────────────────────────────────────────

/// Internal comparable value representation for query evaluation.
#[derive(Debug, Clone)]
enum ComparableValue {
    String(String),
    Integer(i64),
    Double(f64),
    Bool(bool),
}

// ─── Value Comparison ─────────────────────────────────────────────────────

fn compare_values(field: &ComparableValue, op: &CompareOp, query: &QueryValue) -> bool {
    let result = match (field, query) {
        // String comparisons
        (ComparableValue::String(fs), QueryValue::String(qs)) => {
            Some(fs.cmp(qs))
        }
        (ComparableValue::String(fs), QueryValue::Integer(qi)) => {
            // Try to parse string as integer for comparison
            fs.parse::<i64>().ok().map(|fi| fi.cmp(qi))
        }
        // Integer comparisons
        (ComparableValue::Integer(fi), QueryValue::Integer(qi)) => {
            Some(fi.cmp(qi))
        }
        (ComparableValue::Integer(fi), QueryValue::Double(qd)) => {
            Some((*fi as f64).partial_cmp(qd).unwrap_or(std::cmp::Ordering::Equal))
        }
        // Double comparisons
        (ComparableValue::Double(fd), QueryValue::Double(qd)) => {
            Some(fd.partial_cmp(qd).unwrap_or(std::cmp::Ordering::Equal))
        }
        (ComparableValue::Double(fd), QueryValue::Integer(qi)) => {
            Some(fd.partial_cmp(&(*qi as f64)).unwrap_or(std::cmp::Ordering::Equal))
        }
        // Bool comparisons
        (ComparableValue::Bool(fb), QueryValue::Bool(qb)) => {
            Some(fb.cmp(qb))
        }
        // Cross-type: integer field vs string query (e.g., status = 'Running')
        (ComparableValue::String(fs), QueryValue::Bool(qb)) => {
            let fs_lower = fs.to_lowercase();
            let qb_str = if *qb { "true" } else { "false" };
            Some(fs_lower.as_str().cmp(qb_str))
        }
        _ => None,
    };

    match result {
        Some(ordering) => match op {
            CompareOp::Eq => ordering == std::cmp::Ordering::Equal,
            CompareOp::Ne => ordering != std::cmp::Ordering::Equal,
            CompareOp::Lt => ordering == std::cmp::Ordering::Less,
            CompareOp::Gt => ordering == std::cmp::Ordering::Greater,
            CompareOp::Le => ordering != std::cmp::Ordering::Greater,
            CompareOp::Ge => ordering != std::cmp::Ordering::Less,
        },
        None => false,
    }
}

// ─── LIKE Pattern Matching ────────────────────────────────────────────────

fn like_match(value: &str, pattern: &str) -> bool {
    // Simple LIKE implementation: % = any sequence, _ = single char
    let mut vi = 0;
    let mut pi = 0;
    let vchars: Vec<char> = value.chars().collect();
    let pchars: Vec<char> = pattern.chars().collect();
    let mut star_pi: Option<usize> = None;
    let mut star_vi: Option<usize> = None;

    while vi < vchars.len() {
        if pi < pchars.len() && (pchars[pi] == '_' || pchars[pi] == vchars[vi]) {
            vi += 1;
            pi += 1;
        } else if pi < pchars.len() && pchars[pi] == '%' {
            star_pi = Some(pi);
            star_vi = Some(vi);
            pi += 1;
        } else if let (Some(spi), Some(svi)) = (star_pi, star_vi) {
            pi = spi + 1;
            let new_vi = svi + 1;
            vi = new_vi;
            star_vi = Some(new_vi);
        } else {
            return false;
        }
    }

    while pi < pchars.len() && pchars[pi] == '%' {
        pi += 1;
    }

    pi == pchars.len()
}

// ─── Helpers ──────────────────────────────────────────────────────────────

fn status_to_string(status: WorkflowStatus) -> String {
    match status {
        WorkflowStatus::Void => "Void".to_string(),
        WorkflowStatus::Running => "Running".to_string(),
        WorkflowStatus::Completed => "Completed".to_string(),
        WorkflowStatus::Failed => "Failed".to_string(),
        WorkflowStatus::Canceled => "Canceled".to_string(),
        WorkflowStatus::Terminated => "Terminated".to_string(),
        WorkflowStatus::ContinuedAsNew => "ContinuedAsNew".to_string(),
        WorkflowStatus::TimedOut => "TimedOut".to_string(),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::visibility::{WorkflowExecutionInfo, VisibilityIndex};
    use std::collections::HashMap;

    fn make_info(
        key: u64,
        wf_id: u64,
        type_id: u64,
        ns_id: u64,
        status: WorkflowStatus,
    ) -> WorkflowExecutionInfo {
        WorkflowExecutionInfo {
            workflow_key: key,
            workflow_id: wf_id,
            run_id: key * 100,
            workflow_type_id: type_id,
            namespace_id: ns_id,
            status,
            start_time_ms: key * 1000,
            close_time_ms: if status == WorkflowStatus::Running {
                None
            } else {
                Some(key * 1000 + 500)
            },
            task_queue_hash: 42,
            search_attributes: HashMap::new(),
            memo: HashMap::new(),
        }
    }

    fn make_index() -> VisibilityIndex {
        let index = VisibilityIndex::new();
        index.register(make_info(1, 100, 1, 10, WorkflowStatus::Running));
        index.register(make_info(2, 200, 1, 10, WorkflowStatus::Completed));
        index.register(make_info(3, 300, 2, 10, WorkflowStatus::Failed));
        index.register(make_info(4, 400, 2, 20, WorkflowStatus::Running));
        index.register(make_info(5, 500, 3, 20, WorkflowStatus::Terminated));
        index
    }

    #[test]
    fn test_equality_query() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("ExecutionStatus = 'Running'")
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.status == WorkflowStatus::Running));
    }

    #[test]
    fn test_not_equal_query() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("ExecutionStatus != 'Running'")
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_numeric_comparison() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("WorkflowType = 1")
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_and_query() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("ExecutionStatus = 'Running' AND NamespaceId = 10")
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].workflow_key, 1);
    }

    #[test]
    fn test_or_query() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("ExecutionStatus = 'Completed' OR ExecutionStatus = 'Failed'")
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_not_query() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("NOT ExecutionStatus = 'Running'")
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_between_query() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("WorkflowType BETWEEN 1 AND 2")
            .unwrap();
        assert_eq!(results.len(), 4); // type 1 (2) + type 2 (2)
    }

    #[test]
    fn test_in_query() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("ExecutionStatus IN ('Running', 'Completed')")
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_is_null_query() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("CloseTime IS NULL")
            .unwrap();
        assert_eq!(results.len(), 2); // Only running workflows have null close time
    }

    #[test]
    fn test_is_not_null_query() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("CloseTime IS NOT NULL")
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_sorting_ascending() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let result = executor
            .execute_string_query(
                "ExecutionStatus = 'Running'",
                SortField::StartTime,
                SortOrder::Ascending,
                100,
                None,
            )
            .unwrap();
        assert_eq!(result.items.len(), 2);
        assert!(result.items[0].start_time_ms <= result.items[1].start_time_ms);
    }

    #[test]
    fn test_pagination() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let page1 = executor
            .execute_string_query(
                "NamespaceId = 10",
                SortField::StartTime,
                SortOrder::Ascending,
                2,
                None,
            )
            .unwrap();
        assert_eq!(page1.items.len(), 2);
        assert_eq!(page1.total_count, 3);
        assert!(page1.next_token.is_some());

        let page2 = executor
            .execute_string_query(
                "NamespaceId = 10",
                SortField::StartTime,
                SortOrder::Ascending,
                2,
                page1.next_token.as_ref(),
            )
            .unwrap();
        assert_eq!(page2.items.len(), 1);
        assert!(page2.next_token.is_none());
    }

    #[test]
    fn test_custom_search_attributes() {
        let index = VisibilityIndex::new();
        let mut info = make_info(1, 100, 1, 10, WorkflowStatus::Running);
        info.search_attributes.insert(
            "OrderId".to_string(),
            SearchAttributeValue::String("ORD-12345".to_string()),
        );
        info.search_attributes.insert(
            "Amount".to_string(),
            SearchAttributeValue::Integer(5000),
        );
        index.register(info);

        let mut info2 = make_info(2, 200, 1, 10, WorkflowStatus::Running);
        info2.search_attributes.insert(
            "OrderId".to_string(),
            SearchAttributeValue::String("ORD-67890".to_string()),
        );
        info2.search_attributes.insert(
            "Amount".to_string(),
            SearchAttributeValue::Integer(3000),
        );
        index.register(info2);

        let executor = SearchQueryExecutor::new(&index);

        // String attribute
        let results = executor
            .execute_string("OrderId = 'ORD-12345'")
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].workflow_key, 1);

        // Numeric attribute
        let results = executor
            .execute_string("Amount > 4000")
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].workflow_key, 1);
    }

    #[test]
    fn test_like_pattern_matching() {
        let index = VisibilityIndex::new();
        let mut info = make_info(1, 100, 1, 10, WorkflowStatus::Running);
        info.search_attributes.insert(
            "CustomerId".to_string(),
            SearchAttributeValue::String("CUST-001-ABC".to_string()),
        );
        index.register(info);

        let mut info2 = make_info(2, 200, 1, 10, WorkflowStatus::Running);
        info2.search_attributes.insert(
            "CustomerId".to_string(),
            SearchAttributeValue::String("CUST-002-XYZ".to_string()),
        );
        index.register(info2);

        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("CustomerId LIKE 'CUST-001%'")
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].workflow_key, 1);
    }

    #[test]
    fn test_complex_nested_query() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string(
                "(ExecutionStatus = 'Running' AND NamespaceId = 10) OR ExecutionStatus = 'Failed'",
            )
            .unwrap();
        // Running + NS10 (1) + Failed (1) = 2
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_workflow_id_query() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("WorkflowId = '300'")
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].workflow_key, 3);
    }

    #[test]
    fn test_greater_than() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("WorkflowType > 1")
            .unwrap();
        assert_eq!(results.len(), 3); // types 2 and 3
    }

    #[test]
    fn test_less_than_or_equal() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("WorkflowType <= 2")
            .unwrap();
        assert_eq!(results.len(), 4); // types 1 and 2
    }

    #[test]
    fn test_like_match_function() {
        assert!(like_match("hello world", "hello%"));
        assert!(like_match("hello world", "%world"));
        assert!(like_match("hello world", "%lo wo%"));
        assert!(like_match("hello", "h_llo"));
        assert!(!like_match("hello", "h_lo"));
        assert!(like_match("abc", "%"));
        assert!(like_match("", "%"));
        assert!(!like_match("", "_"));
    }

    #[test]
    fn test_empty_result() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let results = executor
            .execute_string("WorkflowType = 999")
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_parse_error() {
        let index = make_index();
        let executor = SearchQueryExecutor::new(&index);

        let result = executor.execute_string("INVALID QUERY !!!");
        assert!(result.is_err());
    }
}
