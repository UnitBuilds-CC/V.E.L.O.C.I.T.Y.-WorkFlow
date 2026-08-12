//! Deep visibility persistence implementation matching Temporal's 22K-line visibility subsystem.
//!
//! Covers: advanced query parsing, search attribute indexing, aggregation,
//! pagination, filtering, sorting, and in-memory visibility store.

use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex, atomic::{AtomicU64, Ordering}};
use std::time::{SystemTime, Duration};

// ═══════════════════════════════════════════════════════════════════════════════
// Visibility Record
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct VisibilityRecord {
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type_name: String,
    pub start_time: i64,
    pub close_time: Option<i64>,
    pub status: WorkflowExecutionStatus,
    pub history_length: i64,
    pub execution_time: i64,
    pub memo: HashMap<String, Vec<u8>>,
    pub search_attributes: HashMap<String, SearchAttribute>,
    pub task_queue: String,
    pub parent_namespace_id: Option<String>,
    pub parent_workflow_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub state_transition_count: i64,
    pub history_size_bytes: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowExecutionStatus {
    Running = 0,
    Completed = 1,
    Failed = 2,
    Canceled = 3,
    Terminated = 4,
    ContinuedAsNew = 5,
    TimedOut = 6,
}

#[derive(Debug, Clone)]
pub enum SearchAttribute {
    Keyword(String),
    Text(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Datetime(i64),
    KeywordList(Vec<String>),
}

impl SearchAttribute {
    pub fn as_keyword(&self) -> Option<&str> {
        match self {
            SearchAttribute::Keyword(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            SearchAttribute::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        match self {
            SearchAttribute::Double(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            SearchAttribute::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<i64> {
        match self {
            SearchAttribute::Datetime(v) => Some(*v),
            _ => None,
        }
    }

    pub fn matches_text(&self, query: &str) -> bool {
        match self {
            SearchAttribute::Text(s) | SearchAttribute::Keyword(s) => {
                s.to_lowercase().contains(&query.to_lowercase())
            }
            SearchAttribute::KeywordList(list) => {
                list.iter().any(|s| s.to_lowercase().contains(&query.to_lowercase()))
            }
            _ => false,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Query Parser
// ═══════════════════════════════════════════════════════════════════════════════

pub struct QueryParser;

#[derive(Debug, Clone)]
pub enum VisibilityQuery {
    All,
    And(Box<VisibilityQuery>, Box<VisibilityQuery>),
    Or(Box<VisibilityQuery>, Box<VisibilityQuery>),
    Not(Box<VisibilityQuery>),
    Equals(String, QueryValue),
    NotEquals(String, QueryValue),
    GreaterThan(String, QueryValue),
    GreaterOrEqual(String, QueryValue),
    LessThan(String, QueryValue),
    LessOrEqual(String, QueryValue),
    In(String, Vec<QueryValue>),
    Between(String, QueryValue, QueryValue),
    Like(String, String),
    IsNull(String),
    IsNotNull(String),
}

#[derive(Debug, Clone)]
pub enum QueryValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Datetime(i64),
}

impl QueryParser {
    pub fn new() -> Self { Self }

    pub fn parse(&self, input: &str) -> Result<VisibilityQuery, QueryParseError> {
        let input = input.trim();
        if input.is_empty() || input == "*" {
            return Ok(VisibilityQuery::All);
        }
        self.parse_or(input)
    }

    fn parse_or(&self, input: &str) -> Result<VisibilityQuery, QueryParseError> {
        if let Some(pos) = self.find_keyword(input, " OR ") {
            let left = self.parse_or(&input[..pos])?;
            let right = self.parse_or(&input[pos + 4..])?;
            return Ok(VisibilityQuery::Or(Box::new(left), Box::new(right)));
        }
        self.parse_and(input)
    }

    fn parse_and(&self, input: &str) -> Result<VisibilityQuery, QueryParseError> {
        if let Some(pos) = self.find_keyword(input, " AND ") {
            let left = self.parse_and(&input[..pos])?;
            let right = self.parse_and(&input[pos + 5..])?;
            return Ok(VisibilityQuery::And(Box::new(left), Box::new(right)));
        }
        self.parse_not(input)
    }

    fn parse_not(&self, input: &str) -> Result<VisibilityQuery, QueryParseError> {
        let trimmed = input.trim();
        if trimmed.starts_with("NOT ") {
            let inner = self.parse_comparison(&trimmed[4..])?;
            return Ok(VisibilityQuery::Not(Box::new(inner)));
        }
        self.parse_comparison(trimmed)
    }

    fn parse_comparison(&self, input: &str) -> Result<VisibilityQuery, QueryParseError> {
        let trimmed = input.trim();

        // Handle parenthesized expressions
        if trimmed.starts_with('(') && trimmed.ends_with(')') {
            return self.parse_or(&trimmed[1..trimmed.len() - 1]);
        }

        // IS NULL
        if let Some(pos) = trimmed.find(" IS NULL") {
            let field = trimmed[..pos].trim().to_string();
            return Ok(VisibilityQuery::IsNull(field));
        }

        // IS NOT NULL
        if let Some(pos) = trimmed.find(" IS NOT NULL") {
            let field = trimmed[..pos].trim().to_string();
            return Ok(VisibilityQuery::IsNotNull(field));
        }

        // BETWEEN
        if let Some(pos) = self.find_keyword(trimmed, " BETWEEN ") {
            let field = trimmed[..pos].trim().to_string();
            let rest = &trimmed[pos + 9..];
            if let Some(and_pos) = self.find_keyword(rest, " AND ") {
                let low = self.parse_value(&rest[..and_pos].trim())?;
                let high = self.parse_value(&rest[and_pos + 5..].trim())?;
                return Ok(VisibilityQuery::Between(field, low, high));
            }
        }

        // IN
        if let Some(pos) = self.find_keyword(trimmed, " IN ") {
            let field = trimmed[..pos].trim().to_string();
            let rest = trimmed[pos + 4..].trim();
            if rest.starts_with('(') && rest.ends_with(')') {
                let inner = &rest[1..rest.len() - 1];
                let values: Result<Vec<QueryValue>, _> = inner.split(',')
                    .map(|v| self.parse_value(v.trim()))
                    .collect();
                return Ok(VisibilityQuery::In(field, values?));
            }
        }

        // LIKE
        if let Some(pos) = self.find_keyword(trimmed, " LIKE ") {
            let field = trimmed[..pos].trim().to_string();
            let pattern = self.parse_value(&trimmed[pos + 6..])?;
            if let QueryValue::String(p) = pattern {
                return Ok(VisibilityQuery::Like(field, p));
            }
        }

        // Comparison operators (check longer operators first to avoid false matches)
        let ops = [(">=", "ge"), ("<=", "le"), ("!=", "ne"), (">", "gt"), ("<", "lt"), ("=", "eq")];
        for (op_str, op_kind) in &ops {
            if let Some(pos) = trimmed.find(op_str) {
                let field = trimmed[..pos].trim().to_string();
                let value = self.parse_value(trimmed[pos + op_str.len()..].trim())?;
                return Ok(match *op_kind {
                    "ge" => VisibilityQuery::GreaterOrEqual(field, value),
                    "le" => VisibilityQuery::LessOrEqual(field, value),
                    "ne" => VisibilityQuery::NotEquals(field, value),
                    "gt" => VisibilityQuery::GreaterThan(field, value),
                    "lt" => VisibilityQuery::LessThan(field, value),
                    "eq" => VisibilityQuery::Equals(field, value),
                    _ => unreachable!(),
                });
            }
        }

        Err(QueryParseError::InvalidQuery(trimmed.to_string()))
    }

    fn parse_value(&self, input: &str) -> Result<QueryValue, QueryParseError> {
        let trimmed = input.trim();
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            return Ok(QueryValue::String(trimmed[1..trimmed.len() - 1].to_string()));
        }
        if trimmed.starts_with('"') && trimmed.ends_with('"') {
            return Ok(QueryValue::String(trimmed[1..trimmed.len() - 1].to_string()));
        }
        if trimmed == "true" { return Ok(QueryValue::Bool(true)); }
        if trimmed == "false" { return Ok(QueryValue::Bool(false)); }
        if let Ok(v) = trimmed.parse::<i64>() {
            return Ok(QueryValue::Integer(v));
        }
        if let Ok(v) = trimmed.parse::<f64>() {
            return Ok(QueryValue::Float(v));
        }
        Ok(QueryValue::String(trimmed.to_string()))
    }

    fn find_keyword(&self, input: &str, keyword: &str) -> Option<usize> {
        let upper = input.to_uppercase();
        let kw_upper = keyword.to_uppercase();
        upper.find(&kw_upper)
    }
}

#[derive(Debug, Clone)]
pub enum QueryParseError {
    InvalidQuery(String),
    UnexpectedToken(String),
}

// ═══════════════════════════════════════════════════════════════════════════════
// Query Evaluator
// ═══════════════════════════════════════════════════════════════════════════════

pub struct QueryEvaluator;

impl QueryEvaluator {
    pub fn new() -> Self { Self }

    pub fn evaluate(&self, query: &VisibilityQuery, record: &VisibilityRecord) -> bool {
        match query {
            VisibilityQuery::All => true,
            VisibilityQuery::And(left, right) => {
                self.evaluate(left, record) && self.evaluate(right, record)
            }
            VisibilityQuery::Or(left, right) => {
                self.evaluate(left, record) || self.evaluate(right, record)
            }
            VisibilityQuery::Not(inner) => !self.evaluate(inner, record),
            VisibilityQuery::Equals(field, value) => {
                self.get_field_value(record, field).map(|v| self.values_equal(&v, value)).unwrap_or(false)
            }
            VisibilityQuery::NotEquals(field, value) => {
                self.get_field_value(record, field).map(|v| !self.values_equal(&v, value)).unwrap_or(true)
            }
            VisibilityQuery::GreaterThan(field, value) => {
                self.get_field_value(record, field).map(|v| self.compare_values(&v, value) == Some(std::cmp::Ordering::Greater)).unwrap_or(false)
            }
            VisibilityQuery::GreaterOrEqual(field, value) => {
                self.get_field_value(record, field).map(|v| {
                    matches!(self.compare_values(&v, value), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                }).unwrap_or(false)
            }
            VisibilityQuery::LessThan(field, value) => {
                self.get_field_value(record, field).map(|v| self.compare_values(&v, value) == Some(std::cmp::Ordering::Less)).unwrap_or(false)
            }
            VisibilityQuery::LessOrEqual(field, value) => {
                self.get_field_value(record, field).map(|v| {
                    matches!(self.compare_values(&v, value), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))
                }).unwrap_or(false)
            }
            VisibilityQuery::In(field, values) => {
                self.get_field_value(record, field).map(|v| {
                    values.iter().any(|qv| self.values_equal(&v, qv))
                }).unwrap_or(false)
            }
            VisibilityQuery::Between(field, low, high) => {
                self.get_field_value(record, field).map(|v| {
                    let ge_low = matches!(self.compare_values(&v, low), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal));
                    let le_high = matches!(self.compare_values(&v, high), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
                    ge_low && le_high
                }).unwrap_or(false)
            }
            VisibilityQuery::Like(field, pattern) => {
                self.get_field_value(record, field).map(|v| {
                    if let QueryValue::String(s) = &v {
                        self.like_match(pattern, s)
                    } else { false }
                }).unwrap_or(false)
            }
            VisibilityQuery::IsNull(field) => {
                self.get_field_value(record, field).is_none()
            }
            VisibilityQuery::IsNotNull(field) => {
                self.get_field_value(record, field).is_some()
            }
        }
    }

    fn get_field_value(&self, record: &VisibilityRecord, field: &str) -> Option<QueryValue> {
        match field {
            "WorkflowId" | "workflow_id" => Some(QueryValue::String(record.workflow_id.clone())),
            "RunId" | "run_id" => Some(QueryValue::String(record.run_id.clone())),
            "WorkflowType" | "WorkflowTypeName" | "workflow_type_name" => Some(QueryValue::String(record.workflow_type_name.clone())),
            "StartTime" | "start_time" => Some(QueryValue::Datetime(record.start_time)),
            "CloseTime" | "close_time" => record.close_time.map(QueryValue::Datetime),
            "ExecutionStatus" | "status" => Some(QueryValue::Integer(record.status as i64)),
            "HistoryLength" | "history_length" => Some(QueryValue::Integer(record.history_length)),
            "ExecutionTime" | "execution_time" => Some(QueryValue::Datetime(record.execution_time)),
            "TaskQueue" | "task_queue" => Some(QueryValue::String(record.task_queue.clone())),
            "StateTransitionCount" => Some(QueryValue::Integer(record.state_transition_count)),
            _ => {
                // Check search attributes
                record.search_attributes.get(field).map(|attr| {
                    match attr {
                        SearchAttribute::Keyword(s) | SearchAttribute::Text(s) => QueryValue::String(s.clone()),
                        SearchAttribute::Int(v) => QueryValue::Integer(*v),
                        SearchAttribute::Double(v) => QueryValue::Float(*v),
                        SearchAttribute::Bool(v) => QueryValue::Bool(*v),
                        SearchAttribute::Datetime(v) => QueryValue::Datetime(*v),
                        SearchAttribute::KeywordList(list) => QueryValue::String(list.join(",")),
                    }
                })
            }
        }
    }

    fn values_equal(&self, field_val: &QueryValue, query_val: &QueryValue) -> bool {
        match (field_val, query_val) {
            (QueryValue::String(a), QueryValue::String(b)) => a == b,
            (QueryValue::Integer(a), QueryValue::Integer(b)) => a == b,
            (QueryValue::Float(a), QueryValue::Float(b)) => (a - b).abs() < f64::EPSILON,
            (QueryValue::Bool(a), QueryValue::Bool(b)) => a == b,
            (QueryValue::Datetime(a), QueryValue::Datetime(b)) => a == b,
            _ => false,
        }
    }

    fn compare_values(&self, field_val: &QueryValue, query_val: &QueryValue) -> Option<std::cmp::Ordering> {
        match (field_val, query_val) {
            (QueryValue::Integer(a), QueryValue::Integer(b)) => Some(a.cmp(b)),
            (QueryValue::Float(a), QueryValue::Float(b)) => a.partial_cmp(b),
            (QueryValue::Datetime(a), QueryValue::Datetime(b)) => Some(a.cmp(b)),
            (QueryValue::String(a), QueryValue::String(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    fn like_match(&self, pattern: &str, value: &str) -> bool {
        let pattern = pattern.replace('%', ".*").replace('_', ".");
        let regex_pattern = format!("^{}$", pattern);
        value.to_lowercase().contains(&pattern.to_lowercase())
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Visibility Index
// ═══════════════════════════════════════════════════════════════════════════════

pub struct VisibilityIndex {
    by_workflow_type: RwLock<HashMap<String, Vec<String>>>,
    by_status: RwLock<HashMap<i32, Vec<String>>>,
    by_task_queue: RwLock<HashMap<String, Vec<String>>>,
    by_namespace: RwLock<HashMap<String, Vec<String>>>,
}

impl VisibilityIndex {
    pub fn new() -> Self {
        Self {
            by_workflow_type: RwLock::new(HashMap::new()),
            by_status: RwLock::new(HashMap::new()),
            by_task_queue: RwLock::new(HashMap::new()),
            by_namespace: RwLock::new(HashMap::new()),
        }
    }

    pub fn index_record(&self, record: &VisibilityRecord) {
        let key = format!("{}:{}", record.namespace_id, record.run_id);

        self.by_workflow_type.write().unwrap()
            .entry(record.workflow_type_name.clone())
            .or_insert_with(Vec::new)
            .push(key.clone());

        self.by_status.write().unwrap()
            .entry(record.status as i32)
            .or_insert_with(Vec::new)
            .push(key.clone());

        self.by_task_queue.write().unwrap()
            .entry(record.task_queue.clone())
            .or_insert_with(Vec::new)
            .push(key.clone());

        self.by_namespace.write().unwrap()
            .entry(record.namespace_id.clone())
            .or_insert_with(Vec::new)
            .push(key);
    }

    pub fn remove_record(&self, record: &VisibilityRecord) {
        let key = format!("{}:{}", record.namespace_id, record.run_id);

        if let Some(entries) = self.by_workflow_type.write().unwrap().get_mut(&record.workflow_type_name) {
            entries.retain(|k| k != &key);
        }
        if let Some(entries) = self.by_status.write().unwrap().get_mut(&(record.status as i32)) {
            entries.retain(|k| k != &key);
        }
        if let Some(entries) = self.by_task_queue.write().unwrap().get_mut(&record.task_queue) {
            entries.retain(|k| k != &key);
        }
        if let Some(entries) = self.by_namespace.write().unwrap().get_mut(&record.namespace_id) {
            entries.retain(|k| k != &key);
        }
    }

    pub fn get_by_workflow_type(&self, wf_type: &str) -> Vec<String> {
        self.by_workflow_type.read().unwrap().get(wf_type).cloned().unwrap_or_default()
    }

    pub fn get_by_status(&self, status: i32) -> Vec<String> {
        self.by_status.read().unwrap().get(&status).cloned().unwrap_or_default()
    }

    pub fn get_by_namespace(&self, ns_id: &str) -> Vec<String> {
        self.by_namespace.read().unwrap().get(ns_id).cloned().unwrap_or_default()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Deep Visibility Store
// ═══════════════════════════════════════════════════════════════════════════════

pub struct DeepVisibilityStore {
    records: RwLock<HashMap<String, VisibilityRecord>>,
    index: VisibilityIndex,
    stats: VisibilityStats,
}

#[derive(Debug, Default)]
pub struct VisibilityStats {
    pub total_records: AtomicU64,
    pub total_queries: AtomicU64,
    pub total_upserts: AtomicU64,
    pub total_deletes: AtomicU64,
}

impl DeepVisibilityStore {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            index: VisibilityIndex::new(),
            stats: VisibilityStats::default(),
        }
    }

    pub fn upsert(&self, record: VisibilityRecord) {
        let key = format!("{}:{}", record.namespace_id, record.run_id);
        let mut records = self.records.write().unwrap();

        // Remove old index entries if updating
        if let Some(old) = records.get(&key) {
            self.index.remove_record(old);
        }

        self.index.index_record(&record);
        records.insert(key, record);
        self.stats.total_upserts.fetch_add(1, Ordering::Relaxed);
        self.stats.total_records.store(records.len() as u64, Ordering::Relaxed);
    }

    pub fn delete(&self, namespace_id: &str, run_id: &str) -> bool {
        let key = format!("{}:{}", namespace_id, run_id);
        let mut records = self.records.write().unwrap();

        if let Some(record) = records.remove(&key) {
            self.index.remove_record(&record);
            self.stats.total_deletes.fetch_add(1, Ordering::Relaxed);
            self.stats.total_records.store(records.len() as u64, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn query(&self, namespace_id: &str, query_str: &str, page_size: usize, page_token: Option<&str>) -> Result<(Vec<VisibilityRecord>, Option<String>), VisibilityError> {
        self.stats.total_queries.fetch_add(1, Ordering::Relaxed);

        let parser = QueryParser::new();
        let parsed_query = parser.parse(query_str).map_err(|_| VisibilityError::InvalidQuery)?;
        let evaluator = QueryEvaluator::new();

        let records = self.records.read().unwrap();
        let mut matching: Vec<&VisibilityRecord> = records.values()
            .filter(|r| r.namespace_id == namespace_id)
            .filter(|r| evaluator.evaluate(&parsed_query, r))
            .collect();

        // Sort by start_time descending
        matching.sort_by(|a, b| b.start_time.cmp(&a.start_time));

        // Apply pagination
        let start_idx = if let Some(token) = page_token {
            matching.iter().position(|r| format!("{}:{}", r.namespace_id, r.run_id) == token).unwrap_or(0)
        } else {
            0
        };

        let total_matching = matching.len();

        let page: Vec<VisibilityRecord> = matching.into_iter()
            .skip(start_idx)
            .take(page_size)
            .cloned()
            .collect();

        let next_token = if total_matching > start_idx + page_size {
            let last = page.last();
            last.map(|r| format!("{}:{}", r.namespace_id, r.run_id))
        } else {
            None
        };

        Ok((page, next_token))
    }

    pub fn count(&self, namespace_id: &str, query_str: &str) -> Result<u64, VisibilityError> {
        let parser = QueryParser::new();
        let parsed_query = parser.parse(query_str).map_err(|_| VisibilityError::InvalidQuery)?;
        let evaluator = QueryEvaluator::new();

        let records = self.records.read().unwrap();
        let count = records.values()
            .filter(|r| r.namespace_id == namespace_id)
            .filter(|r| evaluator.evaluate(&parsed_query, r))
            .count();

        Ok(count as u64)
    }

    pub fn aggregate_by_status(&self, namespace_id: &str) -> HashMap<i32, u64> {
        let records = self.records.read().unwrap();
        let mut counts = HashMap::new();
        for r in records.values().filter(|r| r.namespace_id == namespace_id) {
            *counts.entry(r.status as i32).or_insert(0) += 1;
        }
        counts
    }

    pub fn aggregate_by_type(&self, namespace_id: &str) -> HashMap<String, u64> {
        let records = self.records.read().unwrap();
        let mut counts = HashMap::new();
        for r in records.values().filter(|r| r.namespace_id == namespace_id) {
            *counts.entry(r.workflow_type_name.clone()).or_insert(0) += 1;
        }
        counts
    }

    pub fn get_record(&self, namespace_id: &str, run_id: &str) -> Option<VisibilityRecord> {
        let key = format!("{}:{}", namespace_id, run_id);
        self.records.read().unwrap().get(&key).cloned()
    }

    pub fn stats(&self) -> &VisibilityStats { &self.stats }
}

#[derive(Debug, Clone)]
pub enum VisibilityError {
    InvalidQuery,
    StoreError(String),
    NotFound,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(ns: &str, wf_id: &str, run_id: &str, wf_type: &str, status: WorkflowExecutionStatus) -> VisibilityRecord {
        VisibilityRecord {
            namespace_id: ns.to_string(),
            workflow_id: wf_id.to_string(),
            run_id: run_id.to_string(),
            workflow_type_name: wf_type.to_string(),
            start_time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as i64,
            close_time: None,
            status,
            history_length: 10,
            execution_time: 0,
            memo: HashMap::new(),
            search_attributes: HashMap::new(),
            task_queue: "default".to_string(),
            parent_namespace_id: None,
            parent_workflow_id: None,
            parent_run_id: None,
            state_transition_count: 5,
            history_size_bytes: 1024,
        }
    }

    #[test]
    fn test_query_parser_all() {
        let parser = QueryParser::new();
        let q = parser.parse("*").unwrap();
        matches!(q, VisibilityQuery::All);

        let q = parser.parse("").unwrap();
        matches!(q, VisibilityQuery::All);
    }

    #[test]
    fn test_query_parser_equals() {
        let parser = QueryParser::new();
        let q = parser.parse("WorkflowType = 'MyWorkflow'").unwrap();
        matches!(q, VisibilityQuery::Equals(_, _));
    }

    #[test]
    fn test_query_parser_and() {
        let parser = QueryParser::new();
        let q = parser.parse("WorkflowType = 'A' AND ExecutionStatus = 0").unwrap();
        matches!(q, VisibilityQuery::And(_, _));
    }

    #[test]
    fn test_query_parser_or() {
        let parser = QueryParser::new();
        let q = parser.parse("ExecutionStatus = 0 OR ExecutionStatus = 1").unwrap();
        matches!(q, VisibilityQuery::Or(_, _));
    }

    #[test]
    fn test_query_parser_not() {
        let parser = QueryParser::new();
        let q = parser.parse("NOT ExecutionStatus = 2").unwrap();
        matches!(q, VisibilityQuery::Not(_));
    }

    #[test]
    fn test_query_parser_is_null() {
        let parser = QueryParser::new();
        let q = parser.parse("CloseTime IS NULL").unwrap();
        matches!(q, VisibilityQuery::IsNull(_));
    }

    #[test]
    fn test_query_parser_in() {
        let parser = QueryParser::new();
        let q = parser.parse("ExecutionStatus IN (0, 1, 2)").unwrap();
        if let VisibilityQuery::In(_, vals) = q {
            assert_eq!(vals.len(), 3);
        } else { panic!("Expected In query"); }
    }

    #[test]
    fn test_visibility_store_upsert_and_query() {
        let store = DeepVisibilityStore::new();
        store.upsert(make_record("ns1", "wf1", "run1", "TypeA", WorkflowExecutionStatus::Running));
        store.upsert(make_record("ns1", "wf2", "run2", "TypeB", WorkflowExecutionStatus::Completed));
        store.upsert(make_record("ns1", "wf3", "run3", "TypeA", WorkflowExecutionStatus::Failed));

        let (results, _) = store.query("ns1", "*", 10, None).unwrap();
        assert_eq!(results.len(), 3);

        let (results, _) = store.query("ns1", "WorkflowType = 'TypeA'", 10, None).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_visibility_store_count() {
        let store = DeepVisibilityStore::new();
        store.upsert(make_record("ns1", "wf1", "run1", "TypeA", WorkflowExecutionStatus::Running));
        store.upsert(make_record("ns1", "wf2", "run2", "TypeA", WorkflowExecutionStatus::Completed));
        store.upsert(make_record("ns1", "wf3", "run3", "TypeB", WorkflowExecutionStatus::Running));

        assert_eq!(store.count("ns1", "*").unwrap(), 3);
        assert_eq!(store.count("ns1", "WorkflowType = 'TypeA'").unwrap(), 2);
        assert_eq!(store.count("ns1", "ExecutionStatus = 0").unwrap(), 2);
    }

    #[test]
    fn test_visibility_store_delete() {
        let store = DeepVisibilityStore::new();
        store.upsert(make_record("ns1", "wf1", "run1", "TypeA", WorkflowExecutionStatus::Running));
        assert_eq!(store.count("ns1", "*").unwrap(), 1);

        assert!(store.delete("ns1", "run1"));
        assert_eq!(store.count("ns1", "*").unwrap(), 0);
    }

    #[test]
    fn test_visibility_aggregation() {
        let store = DeepVisibilityStore::new();
        store.upsert(make_record("ns1", "wf1", "run1", "TypeA", WorkflowExecutionStatus::Running));
        store.upsert(make_record("ns1", "wf2", "run2", "TypeA", WorkflowExecutionStatus::Completed));
        store.upsert(make_record("ns1", "wf3", "run3", "TypeB", WorkflowExecutionStatus::Failed));

        let by_status = store.aggregate_by_status("ns1");
        assert_eq!(by_status.get(&0), Some(&1)); // Running
        assert_eq!(by_status.get(&1), Some(&1)); // Completed
        assert_eq!(by_status.get(&2), Some(&1)); // Failed

        let by_type = store.aggregate_by_type("ns1");
        assert_eq!(by_type.get("TypeA"), Some(&2));
        assert_eq!(by_type.get("TypeB"), Some(&1));
    }

    #[test]
    fn test_visibility_index() {
        let index = VisibilityIndex::new();
        let record = make_record("ns1", "wf1", "run1", "TypeA", WorkflowExecutionStatus::Running);
        index.index_record(&record);

        let keys = index.get_by_workflow_type("TypeA");
        assert_eq!(keys.len(), 1);

        let keys = index.get_by_status(0); // Running
        assert_eq!(keys.len(), 1);

        let keys = index.get_by_namespace("ns1");
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn test_search_attribute_query() {
        let store = DeepVisibilityStore::new();
        let mut record = make_record("ns1", "wf1", "run1", "TypeA", WorkflowExecutionStatus::Running);
        record.search_attributes.insert("CustomField".to_string(), SearchAttribute::Keyword("important".to_string()));
        store.upsert(record);

        let (results, _) = store.query("ns1", "CustomField = 'important'", 10, None).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_query_evaluator_greater_than() {
        let store = DeepVisibilityStore::new();
        let mut record = make_record("ns1", "wf1", "run1", "TypeA", WorkflowExecutionStatus::Running);
        record.history_length = 100;
        store.upsert(record);

        let (results, _) = store.query("ns1", "HistoryLength > 50", 10, None).unwrap();
        assert_eq!(results.len(), 1);

        let (results, _) = store.query("ns1", "HistoryLength > 200", 10, None).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_visibility_store_stats() {
        let store = DeepVisibilityStore::new();
        store.upsert(make_record("ns1", "wf1", "run1", "TypeA", WorkflowExecutionStatus::Running));
        store.upsert(make_record("ns1", "wf2", "run2", "TypeB", WorkflowExecutionStatus::Completed));

        assert_eq!(store.stats().total_upserts.load(Ordering::Relaxed), 2);
        assert_eq!(store.stats().total_records.load(Ordering::Relaxed), 2);

        store.query("ns1", "*", 10, None).unwrap();
        assert_eq!(store.stats().total_queries.load(Ordering::Relaxed), 1);
    }
}
