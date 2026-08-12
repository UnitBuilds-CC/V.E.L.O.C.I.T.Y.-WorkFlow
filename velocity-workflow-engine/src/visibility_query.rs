//! SQL-like query parser for visibility index.
//! Supports simple WHERE clauses: `Field = 'Value' AND Field = 'Value'`
//! Fields: WorkflowType, Status, Namespace, TaskQueue, WorkflowId, ExecutionStatus

use std::collections::HashMap;
use crate::visibility::{VisibilityIndex, WorkflowExecutionInfo, SearchAttributeValue};
use crate::engine::WorkflowStatus;

/// A parsed visibility query with filter conditions.
#[derive(Debug, Clone, Default)]
pub struct VisibilityQuery {
    pub conditions: Vec<QueryCondition>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// A single condition in a visibility query.
#[derive(Debug, Clone)]
pub struct QueryCondition {
    pub field: QueryField,
    pub op: QueryOp,
    pub value: String,
}

/// Supported query fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryField {
    WorkflowType,
    Status,
    ExecutionStatus,
    Namespace,
    TaskQueue,
    WorkflowId,
    SearchAttribute(String),
}

/// Supported comparison operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryOp {
    Eq,
    Neq,
}

impl VisibilityQuery {
    /// Parse a SQL-like query string.
    /// Examples:
    /// - `"WorkflowType = 'OrderWorkflow'"`
    /// - `"Status = 'Running' AND Namespace = 'default'"`
    /// - `"ExecutionStatus = 'Completed' LIMIT 10"`
    pub fn parse(query: &str) -> Result<Self, QueryParseError> {
        let query = query.trim();
        if query.is_empty() {
            return Ok(Self::default());
        }

        let mut conditions = Vec::new();
        let mut limit = None;
        let mut offset = None;

        // Extract LIMIT clause from anywhere in the query
        let query = if let Some(idx) = query.to_uppercase().find(" LIMIT ") {
            let after = query[idx + 7..].trim().trim_matches('\'');
            limit = after.split_whitespace().next().and_then(|s| s.parse::<usize>().ok());
            query[..idx].trim()
        } else { query };

        // Extract OFFSET clause from anywhere in the query
        let query = if let Some(idx) = query.to_uppercase().find(" OFFSET ") {
            let after = query[idx + 8..].trim().trim_matches('\'');
            offset = after.split_whitespace().next().and_then(|s| s.parse::<usize>().ok());
            query[..idx].trim()
        } else { query };

        // Split by AND (case-insensitive)
        let parts = split_by_and(query);

        for part in parts {
            let part = part.trim();
            if part.is_empty() { continue; }

            // Parse condition: Field OP 'Value'
            let condition = parse_condition(part)?;
            conditions.push(condition);
        }

        Ok(Self { conditions, limit, offset })
    }

    /// Execute this query against a visibility index.
    pub fn execute(&self, index: &VisibilityIndex) -> Vec<WorkflowExecutionInfo> {
        // Start with all workflows (use the broadest filter available)
        let mut results = self.get_initial_set(index);

        // Apply remaining conditions as filters
        for condition in &self.conditions {
            results.retain(|info| matches_condition(info, condition));
        }

        // Apply offset
        if let Some(offset) = self.offset {
            if offset < results.len() {
                results = results.into_iter().skip(offset).collect();
            } else {
                return Vec::new();
            }
        }

        // Apply limit
        if let Some(limit) = self.limit {
            results.truncate(limit);
        }

        results
    }

    fn get_initial_set(&self, index: &VisibilityIndex) -> Vec<WorkflowExecutionInfo> {
        // Try to use an index-friendly initial set
        for condition in &self.conditions {
            if condition.op == QueryOp::Eq {
                match &condition.field {
                    QueryField::Status | QueryField::ExecutionStatus => {
                        if let Some(status) = parse_status(&condition.value) {
                            return index.list_by_status(status);
                        }
                    }
                    QueryField::Namespace => {
                        let ns_id = condition.value.parse::<u64>().unwrap_or(0);
                        return index.list_by_namespace(ns_id);
                    }
                    QueryField::WorkflowType => {
                        let type_id = condition.value.parse::<u64>().unwrap_or(0);
                        return index.list_by_type(type_id);
                    }
                    _ => {}
                }
            }
        }
        // Fallback: get all statuses
        let mut all = Vec::new();
        for status in [WorkflowStatus::Running, WorkflowStatus::Completed,
                       WorkflowStatus::Failed, WorkflowStatus::Canceled,
                       WorkflowStatus::Terminated, WorkflowStatus::ContinuedAsNew] {
            all.extend(index.list_by_status(status));
        }
        all
    }
}

fn parse_condition(s: &str) -> Result<QueryCondition, QueryParseError> {
    // Try to find operator
    let (field_str, op, value_str) = if let Some(pos) = s.find("!=") {
        (&s[..pos], QueryOp::Neq, &s[pos+2..])
    } else if let Some(pos) = s.find('=') {
        // Make sure it's not part of !=
        if pos > 0 && &s[pos-1..pos] == "!" {
            return Err(QueryParseError::InvalidOperator(s.to_string()));
        }
        (&s[..pos], QueryOp::Eq, &s[pos+1..])
    } else {
        return Err(QueryParseError::MissingOperator(s.to_string()));
    };

    let field_str = field_str.trim();
    let value_str = value_str.trim().trim_matches('\'').trim_matches('"');

    let field = match field_str.to_lowercase().as_str() {
        "workflowtype" | "workflow_type" => QueryField::WorkflowType,
        "status" | "executionstatus" | "execution_status" => QueryField::ExecutionStatus,
        "namespace" | "namespace_name" => QueryField::Namespace,
        "taskqueue" | "task_queue" => QueryField::TaskQueue,
        "workflowid" | "workflow_id" => QueryField::WorkflowId,
        other => QueryField::SearchAttribute(other.to_string()),
    };

    Ok(QueryCondition {
        field,
        op,
        value: value_str.to_string(),
    })
}

fn matches_condition(info: &WorkflowExecutionInfo, condition: &QueryCondition) -> bool {
    let result = match &condition.field {
        QueryField::WorkflowType => info.workflow_type_id.to_string() == condition.value,
        QueryField::Status | QueryField::ExecutionStatus => {
            if let Some(status) = parse_status(&condition.value) {
                info.status == status
            } else {
                false
            }
        }
        QueryField::Namespace => info.namespace_id.to_string() == condition.value,
        QueryField::TaskQueue => info.task_queue_hash.to_string() == condition.value,
        QueryField::WorkflowId => info.workflow_id.to_string() == condition.value,
        QueryField::SearchAttribute(key) => {
            info.search_attributes.get(key)
                .map(|v| match v {
                    SearchAttributeValue::String(s) => s == &condition.value,
                    SearchAttributeValue::Keyword(s) => s == &condition.value,
                    SearchAttributeValue::Integer(i) => i.to_string() == condition.value,
                    SearchAttributeValue::Double(d) => d.to_string() == condition.value,
                    SearchAttributeValue::Bool(b) => b.to_string() == condition.value,
                    SearchAttributeValue::DateTime(dt) => dt.to_string() == condition.value,
                })
                .unwrap_or(false)
        }
    };

    match condition.op {
        QueryOp::Eq => result,
        QueryOp::Neq => !result,
    }
}

fn parse_status(s: &str) -> Option<WorkflowStatus> {
    match s.to_lowercase().as_str() {
        "running" => Some(WorkflowStatus::Running),
        "completed" => Some(WorkflowStatus::Completed),
        "failed" => Some(WorkflowStatus::Failed),
        "canceled" | "cancelled" => Some(WorkflowStatus::Canceled),
        "terminated" => Some(WorkflowStatus::Terminated),
        "continuedasnew" | "continued_as_new" => Some(WorkflowStatus::ContinuedAsNew),
        "timedout" | "timed_out" => Some(WorkflowStatus::TimedOut),
        _ => s.parse::<u8>().ok().and_then(|n| match n {
            1 => Some(WorkflowStatus::Running),
            2 => Some(WorkflowStatus::Completed),
            3 => Some(WorkflowStatus::Failed),
            4 => Some(WorkflowStatus::Canceled),
            5 => Some(WorkflowStatus::Terminated),
            6 => Some(WorkflowStatus::ContinuedAsNew),
            7 => Some(WorkflowStatus::TimedOut),
            _ => None,
        }),
    }
}

/// Split a query string by AND keyword (case-insensitive), respecting quoted strings.
fn split_by_and(query: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let chars: Vec<char> = query.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '\'' || chars[i] == '"' {
            in_quote = !in_quote;
            current.push(chars[i]);
            i += 1;
        } else if !in_quote && i + 4 < chars.len() {
            let word: String = chars[i..i+5].iter().collect::<String>().to_uppercase();
            if word == " AND " {
                parts.push(current.trim().to_string());
                current = String::new();
                i += 5;
            } else {
                current.push(chars[i]);
                i += 1;
            }
        } else {
            current.push(chars[i]);
            i += 1;
        }
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        parts.push(trimmed);
    }
    parts
}

#[derive(Debug)]
pub enum QueryParseError {
    MissingOperator(String),
    InvalidOperator(String),
    InvalidValue(String),
}

impl std::fmt::Display for QueryParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingOperator(s) => write!(f, "Missing operator in: {}", s),
            Self::InvalidOperator(s) => write!(f, "Invalid operator in: {}", s),
            Self::InvalidValue(s) => write!(f, "Invalid value: {}", s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty() {
        let q = VisibilityQuery::parse("").unwrap();
        assert_eq!(q.conditions.len(), 0);
    }

    #[test]
    fn test_parse_single_condition() {
        let q = VisibilityQuery::parse("Status = 'Running'").unwrap();
        assert_eq!(q.conditions.len(), 1);
        assert_eq!(q.conditions[0].field, QueryField::ExecutionStatus);
        assert_eq!(q.conditions[0].op, QueryOp::Eq);
        assert_eq!(q.conditions[0].value, "Running");
    }

    #[test]
    fn test_parse_and_conditions() {
        let q = VisibilityQuery::parse("WorkflowType = '100' AND Status = 'Running'").unwrap();
        assert_eq!(q.conditions.len(), 2);
        assert_eq!(q.conditions[0].field, QueryField::WorkflowType);
        assert_eq!(q.conditions[1].field, QueryField::ExecutionStatus);
    }

    #[test]
    fn test_parse_with_limit() {
        let q = VisibilityQuery::parse("Status = 'Running' LIMIT 10").unwrap();
        assert_eq!(q.conditions.len(), 1);
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn test_execute_status_filter() {
        let index = VisibilityIndex::new();
        index.register(WorkflowExecutionInfo {
            workflow_key: 1, workflow_id: 1, run_id: 100, workflow_type_id: 10,
            namespace_id: 0, status: WorkflowStatus::Running, start_time_ms: 0,
            close_time_ms: None, task_queue_hash: 42, search_attributes: HashMap::new(),
        });
        index.register(WorkflowExecutionInfo {
            workflow_key: 2, workflow_id: 2, run_id: 200, workflow_type_id: 10,
            namespace_id: 0, status: WorkflowStatus::Completed, start_time_ms: 0,
            close_time_ms: None, task_queue_hash: 42, search_attributes: HashMap::new(),
        });

        let q = VisibilityQuery::parse("Status = 'Running'").unwrap();
        let results = q.execute(&index);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, WorkflowStatus::Running);
    }

    #[test]
    fn test_execute_with_limit() {
        let index = VisibilityIndex::new();
        for i in 0..5 {
            index.register(WorkflowExecutionInfo {
                workflow_key: i, workflow_id: i, run_id: i + 100, workflow_type_id: 10,
                namespace_id: 0, status: WorkflowStatus::Running, start_time_ms: 0,
                close_time_ms: None, task_queue_hash: 42, search_attributes: HashMap::new(),
            });
        }

        let q = VisibilityQuery::parse("Status = 'Running' LIMIT 3").unwrap();
        let results = q.execute(&index);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_parse_search_attribute() {
        let q = VisibilityQuery::parse("customer_id = 'C123'").unwrap();
        assert_eq!(q.conditions.len(), 1);
        match &q.conditions[0].field {
            QueryField::SearchAttribute(name) => assert_eq!(name, "customer_id"),
            _ => panic!("Expected SearchAttribute"),
        }
    }
}
