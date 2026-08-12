//! B-tree indexed search attributes for O(log n) range queries.
//! Replaces the linear scan in `visibility.rs` with a sorted BTreeMap index
//! supporting exact match, range, prefix, and comparison queries.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::visibility::SearchAttributeValue;

// ─── Search Attribute Index ──────────────────────────────────────────────────

/// A sortable key for search attribute values, enabling BTreeMap ordering.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexedValue {
    String(String),
    Integer(i64),
    Double(u64), // IEEE 754 bits for ordering
    Bool(bool),
    DateTime(u64),
    Keyword(String),
}

impl From<&SearchAttributeValue> for IndexedValue {
    fn from(v: &SearchAttributeValue) -> Self {
        match v {
            SearchAttributeValue::String(s) => IndexedValue::String(s.clone()),
            SearchAttributeValue::Integer(i) => IndexedValue::Integer(*i),
            SearchAttributeValue::Double(f) => IndexedValue::Double(f.to_bits()),
            SearchAttributeValue::Bool(b) => IndexedValue::Bool(*b),
            SearchAttributeValue::DateTime(ms) => IndexedValue::DateTime(*ms),
            SearchAttributeValue::Keyword(k) => IndexedValue::Keyword(k.clone()),
        }
    }
}

impl IndexedValue {
    pub fn to_search_attribute_value(&self) -> SearchAttributeValue {
        match self {
            IndexedValue::String(s) => SearchAttributeValue::String(s.clone()),
            IndexedValue::Integer(i) => SearchAttributeValue::Integer(*i),
            IndexedValue::Double(bits) => SearchAttributeValue::Double(f64::from_bits(*bits)),
            IndexedValue::Bool(b) => SearchAttributeValue::Bool(*b),
            IndexedValue::DateTime(ms) => SearchAttributeValue::DateTime(*ms),
            IndexedValue::Keyword(k) => SearchAttributeValue::Keyword(k.clone()),
        }
    }
}

/// Composite index key: (attribute_name, attribute_value).
/// Enables efficient per-attribute range scans in the BTreeMap.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexKey {
    pub attr_name: String,
    pub attr_value: IndexedValue,
}

/// B-tree based search attribute index.
/// Provides O(log n) exact match, range, and prefix queries on custom search attributes.
pub struct SearchAttributeIndex {
    /// Main B-tree: (attr_name, attr_value) -> set of workflow_keys.
    index: RwLock<BTreeMap<IndexKey, HashSet<u64>>>,
    /// Reverse index: workflow_key -> set of (attr_name, attr_value) for cleanup.
    reverse: RwLock<HashMap<u64, HashSet<IndexKey>>>,
    /// Statistics.
    stats: RwLock<SearchIndexStats>,
}

/// Statistics for the search attribute index.
#[derive(Debug, Clone, Default)]
pub struct SearchIndexStats {
    pub total_entries: u64,
    pub unique_keys: u64,
    pub indexed_workflows: u64,
    pub range_queries: u64,
    pub exact_queries: u64,
}

impl SearchAttributeIndex {
    pub fn new() -> Self {
        Self {
            index: RwLock::new(BTreeMap::new()),
            reverse: RwLock::new(HashMap::new()),
            stats: RwLock::new(SearchIndexStats::default()),
        }
    }

    /// Index a search attribute for a workflow.
    pub fn index_attribute(
        &self,
        workflow_key: u64,
        attr_name: &str,
        attr_value: &SearchAttributeValue,
    ) {
        let indexed_value = IndexedValue::from(attr_value);
        let key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: indexed_value,
        };

        // Forward index
        {
            let mut index = self.index.write().unwrap();
            index.entry(key.clone()).or_default().insert(workflow_key);
        }

        // Reverse index
        {
            let mut reverse = self.reverse.write().unwrap();
            reverse.entry(workflow_key).or_default().insert(key);
        }

        // Update stats
        let mut stats = self.stats.write().unwrap();
        stats.total_entries += 1;
        stats.unique_keys = self.index.read().unwrap().len() as u64;
        stats.indexed_workflows = self.reverse.read().unwrap().len() as u64;
    }

    /// Remove all search attributes for a workflow.
    pub fn remove_workflow(&self, workflow_key: u64) {
        let keys_to_remove: Vec<IndexKey> = {
            let mut reverse = self.reverse.write().unwrap();
            if let Some(keys) = reverse.remove(&workflow_key) {
                keys.into_iter().collect()
            } else {
                return;
            }
        };

        let mut index = self.index.write().unwrap();
        let mut removed = 0u64;
        for key in keys_to_remove {
            if let Some(set) = index.get_mut(&key) {
                set.remove(&workflow_key);
                removed += 1;
                if set.is_empty() {
                    index.remove(&key);
                }
            }
        }

        let mut stats = self.stats.write().unwrap();
        stats.total_entries = stats.total_entries.saturating_sub(removed);
        stats.unique_keys = index.len() as u64;
        stats.indexed_workflows = self.reverse.read().unwrap().len() as u64;
    }

    /// Remove a specific attribute from a workflow.
    pub fn remove_attribute(
        &self,
        workflow_key: u64,
        attr_name: &str,
        attr_value: &SearchAttributeValue,
    ) {
        let indexed_value = IndexedValue::from(attr_value);
        let key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: indexed_value,
        };

        {
            let mut index = self.index.write().unwrap();
            if let Some(set) = index.get_mut(&key) {
                set.remove(&workflow_key);
                if set.is_empty() {
                    index.remove(&key);
                }
            }
        }

        {
            let mut reverse = self.reverse.write().unwrap();
            if let Some(keys) = reverse.get_mut(&workflow_key) {
                keys.remove(&key);
                if keys.is_empty() {
                    reverse.remove(&workflow_key);
                }
            }
        }
    }

    // ─── Query Methods ────────────────────────────────────────────────────

    /// Exact match: find all workflows with a specific attribute value.
    pub fn exact_match(&self, attr_name: &str, attr_value: &SearchAttributeValue) -> Vec<u64> {
        let indexed_value = IndexedValue::from(attr_value);
        let key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: indexed_value,
        };

        let index = self.index.read().unwrap();
        self.stats.write().unwrap().exact_queries += 1;
        index
            .get(&key)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Range query: find all workflows with an integer attribute in [low, high].
    pub fn range_integer(&self, attr_name: &str, low: i64, high: i64) -> Vec<u64> {
        let low_key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: IndexedValue::Integer(low),
        };
        let high_key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: IndexedValue::Integer(high),
        };

        let index = self.index.read().unwrap();
        self.stats.write().unwrap().range_queries += 1;
        index
            .range(low_key..=high_key)
            .flat_map(|(_, set)| set.iter().copied())
            .collect()
    }

    /// Range query: find all workflows with a DateTime attribute in [start_ms, end_ms].
    pub fn range_datetime(&self, attr_name: &str, start_ms: u64, end_ms: u64) -> Vec<u64> {
        let low_key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: IndexedValue::DateTime(start_ms),
        };
        let high_key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: IndexedValue::DateTime(end_ms),
        };

        let index = self.index.read().unwrap();
        self.stats.write().unwrap().range_queries += 1;
        index
            .range(low_key..=high_key)
            .flat_map(|(_, set)| set.iter().copied())
            .collect()
    }

    /// Prefix query: find all workflows with a string attribute starting with the given prefix.
    pub fn prefix_match(&self, attr_name: &str, prefix: &str) -> Vec<u64> {
        let low_key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: IndexedValue::String(prefix.to_string()),
        };
        // Upper bound: prefix with last char incremented
        let upper = prefix_increment(prefix);
        let high_key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: IndexedValue::String(upper),
        };

        let index = self.index.read().unwrap();
        self.stats.write().unwrap().range_queries += 1;
        index
            .range(low_key..high_key)
            .filter(|(key, _)| {
                key.attr_value
                    .to_search_attribute_value()
                    .matches_string_prefix(prefix)
            })
            .flat_map(|(_, set)| set.iter().copied())
            .collect()
    }

    /// Greater than: find all workflows with an integer attribute > value.
    pub fn greater_than_integer(&self, attr_name: &str, value: i64) -> Vec<u64> {
        let low_key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: IndexedValue::Integer(value + 1),
        };
        let high_key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: IndexedValue::Integer(i64::MAX),
        };

        let index = self.index.read().unwrap();
        self.stats.write().unwrap().range_queries += 1;
        index
            .range(low_key..=high_key)
            .flat_map(|(_, set)| set.iter().copied())
            .collect()
    }

    /// Less than: find all workflows with an integer attribute < value.
    pub fn less_than_integer(&self, attr_name: &str, value: i64) -> Vec<u64> {
        let low_key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: IndexedValue::Integer(i64::MIN),
        };
        let high_key = IndexKey {
            attr_name: attr_name.to_string(),
            attr_value: IndexedValue::Integer(value - 1),
        };

        let index = self.index.read().unwrap();
        self.stats.write().unwrap().range_queries += 1;
        index
            .range(low_key..=high_key)
            .flat_map(|(_, set)| set.iter().copied())
            .collect()
    }

    /// Get all indexed attribute names.
    pub fn indexed_attribute_names(&self) -> Vec<String> {
        let index = self.index.read().unwrap();
        let mut names: Vec<String> = index.keys().map(|k| k.attr_name.clone()).collect();
        names.sort();
        names.dedup();
        names
    }

    /// Get the total number of indexed entries.
    pub fn entry_count(&self) -> usize {
        self.index.read().unwrap().len()
    }

    /// Get the number of indexed workflows.
    pub fn workflow_count(&self) -> usize {
        self.reverse.read().unwrap().len()
    }

    /// Get index statistics.
    pub fn stats(&self) -> SearchIndexStats {
        self.stats.read().unwrap().clone()
    }
}

impl Default for SearchAttributeIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Increment the last character of a string for prefix range upper bound.
fn prefix_increment(s: &str) -> String {
    if s.is_empty() {
        return String::from("\u{10FFFF}");
    }
    let mut chars: Vec<char> = s.chars().collect();
    let last = chars.last_mut().unwrap();
    *last = char::from_u32((*last as u32) + 1).unwrap_or(*last);
    chars.into_iter().collect()
}

/// Extension trait for SearchAttributeValue to support prefix matching.
trait SearchAttributeValueExt {
    fn matches_string_prefix(&self, prefix: &str) -> bool;
}

impl SearchAttributeValueExt for SearchAttributeValue {
    fn matches_string_prefix(&self, prefix: &str) -> bool {
        match self {
            SearchAttributeValue::String(s) => s.starts_with(prefix),
            SearchAttributeValue::Keyword(k) => k.starts_with(prefix),
            _ => false,
        }
    }
}

// ─── Search Attribute Schema ─────────────────────────────────────────────────

/// Declared type for a custom search attribute (mirrors Temporal's searchattribute.Type).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchAttributeType {
    Text,
    Keyword,
    Int,
    Double,
    Bool,
    Datetime,
    KeywordList,
}

impl SearchAttributeType {
    /// Parse from a type name string (case-insensitive).
    pub fn from_type_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "text" => Some(Self::Text),
            "keyword" => Some(Self::Keyword),
            "int" | "integer" | "long" => Some(Self::Int),
            "double" | "float" => Some(Self::Double),
            "bool" | "boolean" => Some(Self::Bool),
            "datetime" | "timestamp" => Some(Self::Datetime),
            "keywordlist" | "keyword_list" => Some(Self::KeywordList),
            _ => None,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Text => "Text",
            Self::Keyword => "Keyword",
            Self::Int => "Int",
            Self::Double => "Double",
            Self::Bool => "Bool",
            Self::Datetime => "Datetime",
            Self::KeywordList => "KeywordList",
        }
    }

    /// Whether this type supports prefix queries.
    pub fn supports_prefix(&self) -> bool {
        matches!(self, Self::Text | Self::Keyword | Self::KeywordList)
    }

    /// Whether this type supports range queries.
    pub fn supports_range(&self) -> bool {
        matches!(self, Self::Int | Self::Double | Self::Datetime)
    }
}

/// A field definition in the search attribute schema.
#[derive(Debug, Clone)]
pub struct SearchAttributeField {
    pub name: String,
    pub attr_type: SearchAttributeType,
    pub is_system: bool,
    pub is_dynamic: bool,
    pub index_enabled: bool,
}

/// Schema manager for search attributes. Tracks declared fields and validates values.
pub struct SearchAttributeSchema {
    fields: RwLock<HashMap<String, SearchAttributeField>>,
    /// System fields that always exist.
    system_fields: HashMap<String, SearchAttributeField>,
}

impl SearchAttributeSchema {
    /// Create a new schema with Temporal-equivalent system fields pre-registered.
    pub fn new() -> Self {
        let mut system = HashMap::new();
        let system_defs = vec![
            ("WorkflowId", SearchAttributeType::Keyword),
            ("RunId", SearchAttributeType::Keyword),
            ("WorkflowType", SearchAttributeType::Keyword),
            ("StartTime", SearchAttributeType::Datetime),
            ("CloseTime", SearchAttributeType::Datetime),
            ("ExecutionStatus", SearchAttributeType::Keyword),
            ("ExecutionDuration", SearchAttributeType::Int),
            ("HistoryLength", SearchAttributeType::Int),
            ("HistorySizeBytes", SearchAttributeType::Int),
            ("TaskQueue", SearchAttributeType::Keyword),
            ("Namespace", SearchAttributeType::Keyword),
            ("ParentWorkflowId", SearchAttributeType::Keyword),
            ("ParentRunId", SearchAttributeType::Keyword),
            ("RootWorkflowId", SearchAttributeType::Keyword),
            ("RootRunId", SearchAttributeType::Keyword),
            ("StateTransitionCount", SearchAttributeType::Int),
            ("BatchOperationId", SearchAttributeType::Keyword),
        ];
        for (name, attr_type) in system_defs {
            system.insert(
                name.to_string(),
                SearchAttributeField {
                    name: name.to_string(),
                    attr_type,
                    is_system: true,
                    is_dynamic: false,
                    index_enabled: true,
                },
            );
        }
        Self {
            fields: RwLock::new(HashMap::new()),
            system_fields: system,
        }
    }

    /// Add a custom search attribute (idempotent — no-op if already exists with same type).
    pub fn add_search_attribute(
        &self,
        name: &str,
        attr_type: SearchAttributeType,
    ) -> Result<(), SchemaError> {
        if self.system_fields.contains_key(name) {
            return Err(SchemaError::Conflict(format!(
                "cannot add custom attribute '{}': conflicts with system attribute",
                name
            )));
        }
        let mut fields = self.fields.write().unwrap();
        if let Some(existing) = fields.get(name) {
            if existing.attr_type == attr_type {
                return Ok(()); // idempotent
            }
            return Err(SchemaError::TypeMismatch(format!(
                "attribute '{}' already registered as {:?}, cannot change to {:?}",
                name, existing.attr_type, attr_type
            )));
        }
        fields.insert(
            name.to_string(),
            SearchAttributeField {
                name: name.to_string(),
                attr_type,
                is_system: false,
                is_dynamic: true,
                index_enabled: true,
            },
        );
        Ok(())
    }

    /// Remove a custom search attribute. System attributes cannot be removed.
    pub fn remove_search_attribute(&self, name: &str) -> Result<(), SchemaError> {
        if self.system_fields.contains_key(name) {
            return Err(SchemaError::Forbidden(format!(
                "cannot remove system attribute '{}'",
                name
            )));
        }
        self.fields.write().unwrap().remove(name);
        Ok(())
    }

    /// Look up a field definition by name (checks custom first, then system).
    pub fn get_field(&self, name: &str) -> Option<SearchAttributeField> {
        self.fields
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .or_else(|| self.system_fields.get(name).cloned())
    }

    /// List all field names (system + custom).
    pub fn all_field_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.system_fields.keys().cloned().collect();
        names.extend(self.fields.read().unwrap().keys().cloned());
        names.sort();
        names.dedup();
        names
    }

    /// List only custom (non-system) fields.
    pub fn custom_field_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.fields.read().unwrap().keys().cloned().collect();
        names.sort();
        names
    }

    /// Validate that a value matches the declared type for a field.
    pub fn validate_value(
        &self,
        field_name: &str,
        value: &SearchAttributeValue,
    ) -> Result<(), SchemaError> {
        let field = self.get_field(field_name).ok_or_else(|| {
            SchemaError::UnknownField(format!("unknown search attribute '{}'", field_name))
        })?;
        let type_ok = match (&field.attr_type, value) {
            (SearchAttributeType::Text, SearchAttributeValue::String(_)) => true,
            (SearchAttributeType::Keyword, SearchAttributeValue::Keyword(_)) => true,
            (SearchAttributeType::Keyword, SearchAttributeValue::String(_)) => true, // allow coercion
            (SearchAttributeType::Int, SearchAttributeValue::Integer(_)) => true,
            (SearchAttributeType::Double, SearchAttributeValue::Double(_)) => true,
            (SearchAttributeType::Bool, SearchAttributeValue::Bool(_)) => true,
            (SearchAttributeType::Datetime, SearchAttributeValue::DateTime(_)) => true,
            (SearchAttributeType::KeywordList, SearchAttributeValue::Keyword(_)) => true,
            _ => false,
        };
        if type_ok {
            Ok(())
        } else {
            Err(SchemaError::TypeMismatch(format!(
                "field '{}' is {:?} but got {:?}",
                field_name, field.attr_type, value
            )))
        }
    }

    /// Get the count of custom (non-system) fields.
    pub fn custom_field_count(&self) -> usize {
        self.fields.read().unwrap().len()
    }
}

impl Default for SearchAttributeSchema {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors from schema operations.
#[derive(Debug, Clone)]
pub enum SchemaError {
    UnknownField(String),
    TypeMismatch(String),
    Conflict(String),
    Forbidden(String),
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownField(msg) => write!(f, "unknown field: {}", msg),
            Self::TypeMismatch(msg) => write!(f, "type mismatch: {}", msg),
            Self::Conflict(msg) => write!(f, "conflict: {}", msg),
            Self::Forbidden(msg) => write!(f, "forbidden: {}", msg),
        }
    }
}

// ─── Visibility Query Parser ─────────────────────────────────────────────────

/// Parsed query node for Temporal-style visibility queries.
/// Supports: `Field = "value"`, `Field != "value"`, `Field > N`, `Field < N`,
/// `Field >= N`, `Field <= N`, `Field BETWEEN a AND b`,
/// `Field LIKE "prefix%"`, `Field IN (v1, v2, ...)`,
/// combined with `AND`, `OR`, `NOT`, and parenthesized grouping.
#[derive(Debug, Clone)]
pub enum QueryNode {
    /// field = value
    Eq { field: String, value: QueryValue },
    /// field != value
    Neq { field: String, value: QueryValue },
    /// field > value
    Gt { field: String, value: QueryValue },
    /// field >= value
    Gte { field: String, value: QueryValue },
    /// field < value
    Lt { field: String, value: QueryValue },
    /// field <= value
    Lte { field: String, value: QueryValue },
    /// field BETWEEN low AND high
    Between {
        field: String,
        low: QueryValue,
        high: QueryValue,
    },
    /// field LIKE "prefix%"
    Like { field: String, pattern: String },
    /// field IN (v1, v2, ...)
    In {
        field: String,
        values: Vec<QueryValue>,
    },
    /// left AND right
    And(Box<QueryNode>, Box<QueryNode>),
    /// left OR right
    Or(Box<QueryNode>, Box<QueryNode>),
    /// NOT child
    Not(Box<QueryNode>),
}

/// A literal value in a query.
#[derive(Debug, Clone)]
pub enum QueryValue {
    Str(String),
    Num(i64),
    Float(f64),
    Bool(bool),
}

/// Tokenizer for the query language.
#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    StringLit(String),
    NumLit(i64),
    FloatLit(f64),
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
    Not,
    Between,
    Like,
    In,
    LParen,
    RParen,
    Comma,
    Eof,
}

struct Tokenizer {
    chars: Vec<char>,
    pos: usize,
}

impl Tokenizer {
    fn new(input: &str) -> Self {
        Self {
            chars: input.chars().collect(),
            pos: 0,
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn next_char(&mut self) -> Option<char> {
        let c = self.chars.get(self.pos).copied();
        self.pos += 1;
        c
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn read_string(&mut self) -> String {
        let quote = self.next_char().unwrap(); // consume opening quote
        let mut s = String::new();
        while let Some(c) = self.next_char() {
            if c == quote {
                break;
            }
            if c == '\\' {
                if let Some(esc) = self.next_char() {
                    s.push(esc);
                }
            } else {
                s.push(c);
            }
        }
        s
    }

    fn read_number(&mut self, first: char) -> Token {
        let mut s = String::new();
        s.push(first);
        let mut is_float = false;
        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() {
                s.push(c);
                self.pos += 1;
            } else if c == '.' && !is_float {
                is_float = true;
                s.push(c);
                self.pos += 1;
            } else {
                break;
            }
        }
        if is_float {
            Token::FloatLit(s.parse().unwrap_or(0.0))
        } else {
            Token::NumLit(s.parse().unwrap_or(0))
        }
    }

    fn read_ident(&mut self, first: char) -> Token {
        let mut s = String::new();
        s.push(first);
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '_' {
                s.push(c);
                self.pos += 1;
            } else {
                break;
            }
        }
        match s.to_uppercase().as_str() {
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "BETWEEN" => Token::Between,
            "LIKE" => Token::Like,
            "IN" => Token::In,
            "TRUE" => Token::Ident("true".into()),
            "FALSE" => Token::Ident("false".into()),
            _ => Token::Ident(s),
        }
    }

    fn tokenize(&mut self) -> Vec<Token> {
        let mut tokens = Vec::new();
        loop {
            self.skip_whitespace();
            match self.peek_char() {
                None => {
                    tokens.push(Token::Eof);
                    break;
                }
                Some(c) => match c {
                    '"' | '\'' => tokens.push(Token::StringLit(self.read_string())),
                    '(' => {
                        self.pos += 1;
                        tokens.push(Token::LParen);
                    }
                    ')' => {
                        self.pos += 1;
                        tokens.push(Token::RParen);
                    }
                    ',' => {
                        self.pos += 1;
                        tokens.push(Token::Comma);
                    }
                    '=' => {
                        self.pos += 1;
                        tokens.push(Token::Eq);
                    }
                    '!' if self.chars.get(self.pos + 1) == Some(&'=') => {
                        self.pos += 2;
                        tokens.push(Token::Neq);
                    }
                    '>' => {
                        self.pos += 1;
                        if self.peek_char() == Some('=') {
                            self.pos += 1;
                            tokens.push(Token::Gte);
                        } else {
                            tokens.push(Token::Gt);
                        }
                    }
                    '<' => {
                        self.pos += 1;
                        if self.peek_char() == Some('=') {
                            self.pos += 1;
                            tokens.push(Token::Lte);
                        } else {
                            tokens.push(Token::Lt);
                        }
                    }
                    _ if c.is_ascii_digit() || c == '-' => {
                        self.pos += 1;
                        tokens.push(self.read_number(c));
                    }
                    _ if c.is_alphabetic() || c == '_' => {
                        self.pos += 1;
                        tokens.push(self.read_ident(c));
                    }
                    _ => {
                        self.pos += 1;
                    } // skip unknown
                },
            }
        }
        tokens
    }
}

/// Parser for Temporal-style visibility query strings.
pub struct VisibilityQueryParser {
    tokens: Vec<Token>,
    pos: usize,
}

impl VisibilityQueryParser {
    /// Parse a query string into a QueryNode AST.
    pub fn parse(input: &str) -> Result<QueryNode, String> {
        let mut tokenizer = Tokenizer::new(input);
        let tokens = tokenizer.tokenize();
        let mut parser = Self { tokens, pos: 0 };
        let node = parser.parse_or()?;
        if parser.peek() != &Token::Eof {
            return Err(format!("unexpected token at position {}", parser.pos));
        }
        Ok(node)
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let t = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        t
    }

    fn parse_or(&mut self) -> Result<QueryNode, String> {
        let mut left = self.parse_and()?;
        while self.peek() == &Token::Or {
            self.advance();
            let right = self.parse_and()?;
            left = QueryNode::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<QueryNode, String> {
        let mut left = self.parse_unary()?;
        while self.peek() == &Token::And {
            self.advance();
            let right = self.parse_unary()?;
            left = QueryNode::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<QueryNode, String> {
        if self.peek() == &Token::Not {
            self.advance();
            let child = self.parse_primary()?;
            return Ok(QueryNode::Not(Box::new(child)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<QueryNode, String> {
        if self.peek() == &Token::LParen {
            self.advance();
            let node = self.parse_or()?;
            if self.peek() != &Token::RParen {
                return Err("expected closing ')'".into());
            }
            self.advance();
            return Ok(node);
        }

        let field = match self.advance() {
            Token::Ident(name) => name,
            other => return Err(format!("expected field name, got {:?}", other)),
        };

        match self.advance() {
            Token::Eq => {
                let value = self.parse_value()?;
                Ok(QueryNode::Eq { field, value })
            }
            Token::Neq => {
                let value = self.parse_value()?;
                Ok(QueryNode::Neq { field, value })
            }
            Token::Gt => {
                let value = self.parse_value()?;
                Ok(QueryNode::Gt { field, value })
            }
            Token::Gte => {
                let value = self.parse_value()?;
                Ok(QueryNode::Gte { field, value })
            }
            Token::Lt => {
                let value = self.parse_value()?;
                Ok(QueryNode::Lt { field, value })
            }
            Token::Lte => {
                let value = self.parse_value()?;
                Ok(QueryNode::Lte { field, value })
            }
            Token::Between => {
                let low = self.parse_value()?;
                if self.advance() != Token::And {
                    return Err("expected AND after BETWEEN low value".into());
                }
                let high = self.parse_value()?;
                Ok(QueryNode::Between { field, low, high })
            }
            Token::Like => {
                let pattern = match self.parse_value()? {
                    QueryValue::Str(s) => s,
                    _ => return Err("LIKE requires a string pattern".into()),
                };
                Ok(QueryNode::Like { field, pattern })
            }
            Token::In => {
                if self.advance() != Token::LParen {
                    return Err("expected '(' after IN".into());
                }
                let mut values = vec![self.parse_value()?];
                while self.peek() == &Token::Comma {
                    self.advance();
                    values.push(self.parse_value()?);
                }
                if self.advance() != Token::RParen {
                    return Err("expected ')' after IN values".into());
                }
                Ok(QueryNode::In { field, values })
            }
            other => Err(format!("expected operator, got {:?}", other)),
        }
    }

    fn parse_value(&mut self) -> Result<QueryValue, String> {
        match self.advance() {
            Token::StringLit(s) => Ok(QueryValue::Str(s)),
            Token::NumLit(n) => Ok(QueryValue::Num(n)),
            Token::FloatLit(f) => Ok(QueryValue::Float(f)),
            Token::Ident(s) => match s.as_str() {
                "true" => Ok(QueryValue::Bool(true)),
                "false" => Ok(QueryValue::Bool(false)),
                _ => Ok(QueryValue::Str(s)), // unquoted string
            },
            other => Err(format!("expected value, got {:?}", other)),
        }
    }
}

/// Execute a parsed QueryNode against the SearchAttributeIndex.
impl SearchAttributeIndex {
    /// Execute a parsed query and return matching workflow keys.
    pub fn execute_query(&self, query: &QueryNode) -> HashSet<u64> {
        match query {
            QueryNode::Eq { field, value } => {
                let sav = query_value_to_sav(value);
                self.exact_match(field, &sav).into_iter().collect()
            }
            QueryNode::Neq { field, value } => {
                let eq_set = {
                    let sav = query_value_to_sav(value);
                    let matched: HashSet<u64> = self.exact_match(field, &sav).into_iter().collect();
                    matched
                };
                // all workflows minus the eq set
                let all = self.all_indexed_workflow_keys();
                all.difference(&eq_set).copied().collect()
            }
            QueryNode::Gt { field, value } => match value {
                QueryValue::Num(n) => self.greater_than_integer(field, *n).into_iter().collect(),
                _ => HashSet::new(),
            },
            QueryNode::Gte { field, value } => match value {
                QueryValue::Num(n) => self
                    .range_integer(field, *n, i64::MAX)
                    .into_iter()
                    .collect(),
                _ => HashSet::new(),
            },
            QueryNode::Lt { field, value } => match value {
                QueryValue::Num(n) => self.less_than_integer(field, *n).into_iter().collect(),
                _ => HashSet::new(),
            },
            QueryNode::Lte { field, value } => match value {
                QueryValue::Num(n) => self
                    .range_integer(field, i64::MIN, *n)
                    .into_iter()
                    .collect(),
                _ => HashSet::new(),
            },
            QueryNode::Between { field, low, high } => match (low, high) {
                (QueryValue::Num(a), QueryValue::Num(b)) => {
                    self.range_integer(field, *a, *b).into_iter().collect()
                }
                _ => HashSet::new(),
            },
            QueryNode::Like { field, pattern } => {
                let prefix = pattern.trim_end_matches('%');
                self.prefix_match(field, prefix).into_iter().collect()
            }
            QueryNode::In { field, values } => {
                let mut result = HashSet::new();
                for v in values {
                    let sav = query_value_to_sav(v);
                    for key in self.exact_match(field, &sav) {
                        result.insert(key);
                    }
                }
                result
            }
            QueryNode::And(left, right) => {
                let l = self.execute_query(left);
                let r = self.execute_query(right);
                l.intersection(&r).copied().collect()
            }
            QueryNode::Or(left, right) => {
                let l = self.execute_query(left);
                let r = self.execute_query(right);
                l.union(&r).copied().collect()
            }
            QueryNode::Not(child) => {
                let matched = self.execute_query(child);
                let all = self.all_indexed_workflow_keys();
                all.difference(&matched).copied().collect()
            }
        }
    }

    /// Get all workflow keys that have at least one indexed attribute.
    pub fn all_indexed_workflow_keys(&self) -> HashSet<u64> {
        self.reverse.read().unwrap().keys().copied().collect()
    }
}

fn query_value_to_sav(v: &QueryValue) -> SearchAttributeValue {
    match v {
        QueryValue::Str(s) => SearchAttributeValue::Keyword(s.clone()),
        QueryValue::Num(n) => SearchAttributeValue::Integer(*n),
        QueryValue::Float(f) => SearchAttributeValue::Double(*f),
        QueryValue::Bool(b) => SearchAttributeValue::Bool(*b),
    }
}

// ─── Bulk Indexer ────────────────────────────────────────────────────────────

/// A buffered operation for the bulk indexer.
#[derive(Debug, Clone)]
pub enum BulkOperation {
    Index {
        workflow_key: u64,
        attr_name: String,
        attr_value: SearchAttributeValue,
    },
    Remove {
        workflow_key: u64,
    },
    RemoveAttribute {
        workflow_key: u64,
        attr_name: String,
        attr_value: SearchAttributeValue,
    },
}

/// Statistics for the bulk indexer.
#[derive(Debug, Clone, Default)]
pub struct BulkIndexerStats {
    pub operations_buffered: u64,
    pub operations_flushed: u64,
    pub flush_count: u64,
    pub errors: u64,
}

/// Bulk indexer that buffers index operations and flushes them in batches.
/// Mirrors Temporal's bulk indexer for Elasticsearch.
pub struct BulkIndexer {
    index: Arc<SearchAttributeIndex>,
    buffer: RwLock<Vec<BulkOperation>>,
    max_buffer_size: usize,
    stats: RwLock<BulkIndexerStats>,
}

impl BulkIndexer {
    pub fn new(index: Arc<SearchAttributeIndex>, max_buffer_size: usize) -> Self {
        Self {
            index,
            buffer: RwLock::new(Vec::new()),
            max_buffer_size: max_buffer_size.max(1),
            stats: RwLock::new(BulkIndexerStats::default()),
        }
    }

    /// Add an operation to the buffer. Auto-flushes when buffer is full.
    pub fn add(&self, op: BulkOperation) {
        let should_flush = {
            let mut buf = self.buffer.write().unwrap();
            buf.push(op);
            self.stats.write().unwrap().operations_buffered += 1;
            buf.len() >= self.max_buffer_size
        };
        if should_flush {
            self.flush();
        }
    }

    /// Flush all buffered operations to the index.
    pub fn flush(&self) {
        let ops: Vec<BulkOperation> = {
            let mut buf = self.buffer.write().unwrap();
            std::mem::take(&mut *buf)
        };
        let count = ops.len() as u64;
        for op in ops {
            match op {
                BulkOperation::Index {
                    workflow_key,
                    attr_name,
                    attr_value,
                } => {
                    self.index
                        .index_attribute(workflow_key, &attr_name, &attr_value);
                }
                BulkOperation::Remove { workflow_key } => {
                    self.index.remove_workflow(workflow_key);
                }
                BulkOperation::RemoveAttribute {
                    workflow_key,
                    attr_name,
                    attr_value,
                } => {
                    self.index
                        .remove_attribute(workflow_key, &attr_name, &attr_value);
                }
            }
        }
        let mut stats = self.stats.write().unwrap();
        stats.operations_flushed += count;
        stats.flush_count += 1;
    }

    /// Get bulk indexer statistics.
    pub fn stats(&self) -> BulkIndexerStats {
        self.stats.read().unwrap().clone()
    }

    /// Get the current buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer.read().unwrap().len()
    }
}

// ─── Index Lifecycle Manager ─────────────────────────────────────────────────

/// State of a search index in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    Creating,
    Active,
    ReadOnly,
    Deleting,
    Deleted,
}

/// Metadata for a managed search index.
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    pub name: String,
    pub alias: Option<String>,
    pub state: IndexState,
    pub created_at_ms: u64,
    pub doc_count: u64,
    pub size_bytes: u64,
    pub schema_version: u32,
    pub number_of_shards: u32,
    pub replicas: u32,
}

/// Manages the lifecycle of search indices (create, alias, rollover, delete).
/// Mirrors Temporal's Elasticsearch index management.
pub struct IndexLifecycleManager {
    indices: RwLock<HashMap<String, IndexMetadata>>,
    aliases: RwLock<HashMap<String, String>>, // alias -> index name
}

impl IndexLifecycleManager {
    pub fn new() -> Self {
        Self {
            indices: RwLock::new(HashMap::new()),
            aliases: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new index with the given configuration.
    pub fn create_index(
        &self,
        name: &str,
        shards: u32,
        replicas: u32,
        schema_version: u32,
    ) -> Result<(), SchemaError> {
        let mut indices = self.indices.write().unwrap();
        if indices.contains_key(name) {
            return Ok(()); // idempotent
        }
        indices.insert(
            name.to_string(),
            IndexMetadata {
                name: name.to_string(),
                alias: None,
                state: IndexState::Active,
                created_at_ms: 0, // would use real clock in production
                doc_count: 0,
                size_bytes: 0,
                schema_version,
                number_of_shards: shards,
                replicas,
            },
        );
        Ok(())
    }

    /// Delete an index.
    pub fn delete_index(&self, name: &str) -> Result<(), SchemaError> {
        let mut indices = self.indices.write().unwrap();
        if let Some(meta) = indices.get_mut(name) {
            meta.state = IndexState::Deleted;
            // Clean up aliases pointing to this index
            let mut aliases = self.aliases.write().unwrap();
            aliases.retain(|_, v| v != name);
            Ok(())
        } else {
            Err(SchemaError::UnknownField(format!(
                "index '{}' not found",
                name
            )))
        }
    }

    /// Create or update an alias pointing to an index.
    pub fn put_alias(&self, alias: &str, index_name: &str) -> Result<(), SchemaError> {
        let indices = self.indices.read().unwrap();
        if !indices.contains_key(index_name) {
            return Err(SchemaError::UnknownField(format!(
                "index '{}' not found",
                index_name
            )));
        }
        self.aliases
            .write()
            .unwrap()
            .insert(alias.to_string(), index_name.to_string());
        Ok(())
    }

    /// Resolve an alias to its target index name.
    pub fn resolve_alias(&self, alias: &str) -> Option<String> {
        self.aliases.read().unwrap().get(alias).cloned()
    }

    /// Rollover: create a new index version and update the alias to point to it.
    pub fn rollover(
        &self,
        alias: &str,
        shards: u32,
        replicas: u32,
        schema_version: u32,
    ) -> Result<String, SchemaError> {
        // Find the current index for this alias
        let current = self.aliases.read().unwrap().get(alias).cloned();
        if let Some(current_name) = current {
            // Mark old index as read-only
            if let Some(meta) = self.indices.write().unwrap().get_mut(&current_name) {
                meta.state = IndexState::ReadOnly;
            }
        }
        // Create new index with versioned name
        let version = self.indices.read().unwrap().len() as u64 + 1;
        let new_name = format!("{}-v{}", alias, version);
        self.create_index(&new_name, shards, replicas, schema_version)?;
        self.put_alias(alias, &new_name)?;
        Ok(new_name)
    }

    /// List all indices.
    pub fn list_indices(&self) -> Vec<IndexMetadata> {
        self.indices
            .read()
            .unwrap()
            .values()
            .filter(|m| m.state != IndexState::Deleted)
            .cloned()
            .collect()
    }

    /// Get metadata for a specific index.
    pub fn get_index(&self, name: &str) -> Option<IndexMetadata> {
        self.indices.read().unwrap().get(name).cloned()
    }

    /// Get the total number of active indices.
    pub fn active_index_count(&self) -> usize {
        self.indices
            .read()
            .unwrap()
            .values()
            .filter(|m| m.state == IndexState::Active)
            .count()
    }
}

impl Default for IndexLifecycleManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_exact_match() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(
            1,
            "customer_id",
            &SearchAttributeValue::String("C123".into()),
        );
        idx.index_attribute(
            2,
            "customer_id",
            &SearchAttributeValue::String("C456".into()),
        );
        idx.index_attribute(
            3,
            "customer_id",
            &SearchAttributeValue::String("C123".into()),
        );

        let results = idx.exact_match("customer_id", &SearchAttributeValue::String("C123".into()));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&3));
    }

    #[test]
    fn test_index_integer_range() {
        let idx = SearchAttributeIndex::new();
        for i in 0..10 {
            idx.index_attribute(i, "priority", &SearchAttributeValue::Integer(i as i64));
        }

        let results = idx.range_integer("priority", 3, 7);
        assert_eq!(results.len(), 5); // 3, 4, 5, 6, 7
    }

    #[test]
    fn test_index_datetime_range() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(1, "created_at", &SearchAttributeValue::DateTime(1000));
        idx.index_attribute(2, "created_at", &SearchAttributeValue::DateTime(2000));
        idx.index_attribute(3, "created_at", &SearchAttributeValue::DateTime(3000));
        idx.index_attribute(4, "created_at", &SearchAttributeValue::DateTime(4000));

        let results = idx.range_datetime("created_at", 1500, 3500);
        assert_eq!(results.len(), 2); // 2000, 3000
    }

    #[test]
    fn test_index_prefix_match() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(1, "name", &SearchAttributeValue::String("alice".into()));
        idx.index_attribute(2, "name", &SearchAttributeValue::String("albert".into()));
        idx.index_attribute(3, "name", &SearchAttributeValue::String("bob".into()));
        idx.index_attribute(
            4,
            "name",
            &SearchAttributeValue::String("alice_smith".into()),
        );

        let results = idx.prefix_match("name", "ali");
        assert_eq!(results.len(), 2); // alice, alice_smith
    }

    #[test]
    fn test_index_greater_than() {
        let idx = SearchAttributeIndex::new();
        for i in 1..=5 {
            idx.index_attribute(i, "score", &SearchAttributeValue::Integer(i as i64 * 10));
        }

        let results = idx.greater_than_integer("score", 30);
        assert_eq!(results.len(), 2); // 40, 50
    }

    #[test]
    fn test_index_less_than() {
        let idx = SearchAttributeIndex::new();
        for i in 1..=5 {
            idx.index_attribute(i, "score", &SearchAttributeValue::Integer(i as i64 * 10));
        }

        let results = idx.less_than_integer("score", 30);
        assert_eq!(results.len(), 2); // 10, 20
    }

    #[test]
    fn test_index_remove_workflow() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(1, "key", &SearchAttributeValue::String("val1".into()));
        idx.index_attribute(1, "key", &SearchAttributeValue::String("val2".into()));
        idx.index_attribute(2, "key", &SearchAttributeValue::String("val1".into()));

        assert_eq!(idx.workflow_count(), 2);

        idx.remove_workflow(1);
        assert_eq!(idx.workflow_count(), 1);

        let results = idx.exact_match("key", &SearchAttributeValue::String("val1".into()));
        assert_eq!(results.len(), 1);
        assert!(results.contains(&2));
    }

    #[test]
    fn test_index_remove_attribute() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(1, "color", &SearchAttributeValue::Keyword("red".into()));
        idx.index_attribute(1, "color", &SearchAttributeValue::Keyword("blue".into()));

        idx.remove_attribute(1, "color", &SearchAttributeValue::Keyword("red".into()));

        let reds = idx.exact_match("color", &SearchAttributeValue::Keyword("red".into()));
        assert!(reds.is_empty());

        let blues = idx.exact_match("color", &SearchAttributeValue::Keyword("blue".into()));
        assert_eq!(blues.len(), 1);
    }

    #[test]
    fn test_index_stats() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(1, "a", &SearchAttributeValue::Integer(1));
        idx.index_attribute(2, "b", &SearchAttributeValue::Integer(2));

        let stats = idx.stats();
        assert_eq!(stats.total_entries, 2);
        assert_eq!(stats.unique_keys, 2);
        assert_eq!(stats.indexed_workflows, 2);
    }

    #[test]
    fn test_indexed_attribute_names() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(1, "zebra", &SearchAttributeValue::Integer(1));
        idx.index_attribute(2, "alpha", &SearchAttributeValue::Integer(2));
        idx.index_attribute(3, "zebra", &SearchAttributeValue::Integer(3));

        let names = idx.indexed_attribute_names();
        assert_eq!(names, vec!["alpha", "zebra"]);
    }

    #[test]
    fn test_indexed_value_ordering() {
        // Verify that IndexedValue ordering is correct for range queries
        let a = IndexedValue::Integer(10);
        let b = IndexedValue::Integer(20);
        let c = IndexedValue::Integer(30);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn test_prefix_increment() {
        assert_eq!(prefix_increment("abc"), "abd");
        assert_eq!(prefix_increment("az"), "a{");
    }

    #[test]
    fn test_multiple_values_same_workflow() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(1, "status", &SearchAttributeValue::Keyword("active".into()));
        idx.index_attribute(
            1,
            "region",
            &SearchAttributeValue::Keyword("us-east".into()),
        );
        idx.index_attribute(1, "priority", &SearchAttributeValue::Integer(5));

        let active = idx.exact_match("status", &SearchAttributeValue::Keyword("active".into()));
        assert_eq!(active.len(), 1);
        assert!(active.contains(&1));

        let us_east = idx.exact_match("region", &SearchAttributeValue::Keyword("us-east".into()));
        assert_eq!(us_east.len(), 1);
    }

    #[test]
    fn test_empty_index_queries() {
        let idx = SearchAttributeIndex::new();
        assert!(idx
            .exact_match("key", &SearchAttributeValue::String("val".into()))
            .is_empty());
        assert!(idx.range_integer("key", 0, 100).is_empty());
        assert!(idx.prefix_match("key", "pre").is_empty());
    }

    // ─── Schema Tests ─────────────────────────────────────────────────────

    #[test]
    fn test_schema_system_fields() {
        let schema = SearchAttributeSchema::new();
        assert!(schema.get_field("WorkflowId").is_some());
        assert!(schema.get_field("WorkflowType").is_some());
        assert!(schema.get_field("StartTime").is_some());
        assert!(schema.get_field("NonExistent").is_none());
        assert!(schema.get_field("WorkflowId").unwrap().is_system);
    }

    #[test]
    fn test_schema_add_custom_field() {
        let schema = SearchAttributeSchema::new();
        assert!(schema
            .add_search_attribute("CustomerId", SearchAttributeType::Keyword)
            .is_ok());
        assert_eq!(schema.custom_field_count(), 1);
        assert!(schema.get_field("CustomerId").is_some());
        assert!(!schema.get_field("CustomerId").unwrap().is_system);
    }

    #[test]
    fn test_schema_add_idempotent() {
        let schema = SearchAttributeSchema::new();
        assert!(schema
            .add_search_attribute("CustomerId", SearchAttributeType::Keyword)
            .is_ok());
        assert!(schema
            .add_search_attribute("CustomerId", SearchAttributeType::Keyword)
            .is_ok()); // no-op
        assert_eq!(schema.custom_field_count(), 1);
    }

    #[test]
    fn test_schema_add_conflict_with_system() {
        let schema = SearchAttributeSchema::new();
        assert!(schema
            .add_search_attribute("WorkflowId", SearchAttributeType::Text)
            .is_err());
    }

    #[test]
    fn test_schema_type_mismatch() {
        let schema = SearchAttributeSchema::new();
        schema
            .add_search_attribute("Priority", SearchAttributeType::Int)
            .unwrap();
        let result = schema.add_search_attribute("Priority", SearchAttributeType::Keyword);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_validate_value() {
        let schema = SearchAttributeSchema::new();
        schema
            .add_search_attribute("Priority", SearchAttributeType::Int)
            .unwrap();
        assert!(schema
            .validate_value("Priority", &SearchAttributeValue::Integer(5))
            .is_ok());
        assert!(schema
            .validate_value("Priority", &SearchAttributeValue::String("x".into()))
            .is_err());
    }

    #[test]
    fn test_schema_remove_custom() {
        let schema = SearchAttributeSchema::new();
        schema
            .add_search_attribute("Temp", SearchAttributeType::Text)
            .unwrap();
        assert_eq!(schema.custom_field_count(), 1);
        schema.remove_search_attribute("Temp").unwrap();
        assert_eq!(schema.custom_field_count(), 0);
    }

    #[test]
    fn test_schema_remove_system_forbidden() {
        let schema = SearchAttributeSchema::new();
        assert!(schema.remove_search_attribute("WorkflowId").is_err());
    }

    // ─── Query Parser Tests ───────────────────────────────────────────────

    #[test]
    fn test_parse_simple_eq() {
        let q = VisibilityQueryParser::parse(r#"WorkflowType = "MyWorkflow""#).unwrap();
        match q {
            QueryNode::Eq {
                field,
                value: QueryValue::Str(v),
            } => {
                assert_eq!(field, "WorkflowType");
                assert_eq!(v, "MyWorkflow");
            }
            _ => panic!("expected Eq node"),
        }
    }

    #[test]
    fn test_parse_and_query() {
        let q = VisibilityQueryParser::parse(r#"Status = "Running" AND Priority > 5"#).unwrap();
        match q {
            QueryNode::And(left, right) => {
                assert!(matches!(*left, QueryNode::Eq { .. }));
                assert!(matches!(*right, QueryNode::Gt { .. }));
            }
            _ => panic!("expected And node"),
        }
    }

    #[test]
    fn test_parse_or_query() {
        let q =
            VisibilityQueryParser::parse(r#"Status = "Running" OR Status = "Completed""#).unwrap();
        assert!(matches!(q, QueryNode::Or(_, _)));
    }

    #[test]
    fn test_parse_not_query() {
        let q = VisibilityQueryParser::parse(r#"NOT Status = "Failed""#).unwrap();
        assert!(matches!(q, QueryNode::Not(_)));
    }

    #[test]
    fn test_parse_between() {
        let q = VisibilityQueryParser::parse("Priority BETWEEN 1 AND 10").unwrap();
        match q {
            QueryNode::Between { field, .. } => assert_eq!(field, "Priority"),
            _ => panic!("expected Between node"),
        }
    }

    #[test]
    fn test_parse_like() {
        let q = VisibilityQueryParser::parse(r#"WorkflowType LIKE "Payment%""#).unwrap();
        match q {
            QueryNode::Like { field, pattern } => {
                assert_eq!(field, "WorkflowType");
                assert_eq!(pattern, "Payment%");
            }
            _ => panic!("expected Like node"),
        }
    }

    #[test]
    fn test_parse_in() {
        let q = VisibilityQueryParser::parse(r#"Status IN ("Running", "Completed", "Failed")"#)
            .unwrap();
        match q {
            QueryNode::In { field, values } => {
                assert_eq!(field, "Status");
                assert_eq!(values.len(), 3);
            }
            _ => panic!("expected In node"),
        }
    }

    #[test]
    fn test_parse_parenthesized() {
        let q = VisibilityQueryParser::parse(
            r#"(Status = "Running" OR Status = "Completed") AND Priority > 5"#,
        )
        .unwrap();
        match q {
            QueryNode::And(left, right) => {
                assert!(matches!(*left, QueryNode::Or(_, _)));
                assert!(matches!(*right, QueryNode::Gt { .. }));
            }
            _ => panic!("expected And node"),
        }
    }

    // ─── Query Execution Tests ────────────────────────────────────────────

    #[test]
    fn test_execute_eq_query() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(
            1,
            "status",
            &SearchAttributeValue::Keyword("running".into()),
        );
        idx.index_attribute(
            2,
            "status",
            &SearchAttributeValue::Keyword("completed".into()),
        );
        idx.index_attribute(
            3,
            "status",
            &SearchAttributeValue::Keyword("running".into()),
        );

        let q = VisibilityQueryParser::parse(r#"status = "running""#).unwrap();
        let results = idx.execute_query(&q);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&3));
    }

    #[test]
    fn test_execute_and_query() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(
            1,
            "status",
            &SearchAttributeValue::Keyword("running".into()),
        );
        idx.index_attribute(1, "priority", &SearchAttributeValue::Integer(10));
        idx.index_attribute(
            2,
            "status",
            &SearchAttributeValue::Keyword("running".into()),
        );
        idx.index_attribute(2, "priority", &SearchAttributeValue::Integer(3));
        idx.index_attribute(
            3,
            "status",
            &SearchAttributeValue::Keyword("completed".into()),
        );
        idx.index_attribute(3, "priority", &SearchAttributeValue::Integer(8));

        let q = VisibilityQueryParser::parse(r#"status = "running" AND priority > 5"#).unwrap();
        let results = idx.execute_query(&q);
        assert_eq!(results.len(), 1);
        assert!(results.contains(&1));
    }

    #[test]
    fn test_execute_or_query() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(
            1,
            "status",
            &SearchAttributeValue::Keyword("running".into()),
        );
        idx.index_attribute(
            2,
            "status",
            &SearchAttributeValue::Keyword("completed".into()),
        );
        idx.index_attribute(3, "status", &SearchAttributeValue::Keyword("failed".into()));

        let q =
            VisibilityQueryParser::parse(r#"status = "running" OR status = "completed""#).unwrap();
        let results = idx.execute_query(&q);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
    }

    #[test]
    fn test_execute_between_query() {
        let idx = SearchAttributeIndex::new();
        for i in 1..=10 {
            idx.index_attribute(i, "score", &SearchAttributeValue::Integer(i as i64 * 10));
        }
        let q = VisibilityQueryParser::parse("score BETWEEN 30 AND 70").unwrap();
        let results = idx.execute_query(&q);
        assert_eq!(results.len(), 5); // 30, 40, 50, 60, 70
    }

    #[test]
    fn test_execute_like_query() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(
            1,
            "type",
            &SearchAttributeValue::String("PaymentWorkflow".into()),
        );
        idx.index_attribute(
            2,
            "type",
            &SearchAttributeValue::String("PaymentRefund".into()),
        );
        idx.index_attribute(
            3,
            "type",
            &SearchAttributeValue::String("OrderWorkflow".into()),
        );

        let q = VisibilityQueryParser::parse(r#"type LIKE "Payment%""#).unwrap();
        let results = idx.execute_query(&q);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_execute_in_query() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(
            1,
            "region",
            &SearchAttributeValue::Keyword("us-east".into()),
        );
        idx.index_attribute(
            2,
            "region",
            &SearchAttributeValue::Keyword("eu-west".into()),
        );
        idx.index_attribute(
            3,
            "region",
            &SearchAttributeValue::Keyword("ap-south".into()),
        );

        let q = VisibilityQueryParser::parse(r#"region IN ("us-east", "eu-west")"#).unwrap();
        let results = idx.execute_query(&q);
        assert_eq!(results.len(), 2);
    }

    // ─── Bulk Indexer Tests ───────────────────────────────────────────────

    #[test]
    fn test_bulk_indexer_buffer_and_flush() {
        let idx = Arc::new(SearchAttributeIndex::new());
        let bulk = BulkIndexer::new(idx.clone(), 10);

        bulk.add(BulkOperation::Index {
            workflow_key: 1,
            attr_name: "status".into(),
            attr_value: SearchAttributeValue::Keyword("running".into()),
        });
        bulk.add(BulkOperation::Index {
            workflow_key: 2,
            attr_name: "status".into(),
            attr_value: SearchAttributeValue::Keyword("completed".into()),
        });

        assert_eq!(bulk.buffer_size(), 2);
        assert_eq!(idx.workflow_count(), 0); // not yet flushed

        bulk.flush();
        assert_eq!(bulk.buffer_size(), 0);
        assert_eq!(idx.workflow_count(), 2);
    }

    #[test]
    fn test_bulk_indexer_auto_flush() {
        let idx = Arc::new(SearchAttributeIndex::new());
        let bulk = BulkIndexer::new(idx.clone(), 3);

        for i in 1..=3 {
            bulk.add(BulkOperation::Index {
                workflow_key: i,
                attr_name: "key".into(),
                attr_value: SearchAttributeValue::Integer(i as i64),
            });
        }
        // Should have auto-flushed at buffer size 3
        assert_eq!(idx.workflow_count(), 3);
    }

    #[test]
    fn test_bulk_indexer_remove() {
        let idx = Arc::new(SearchAttributeIndex::new());
        idx.index_attribute(1, "k", &SearchAttributeValue::Integer(1));
        idx.index_attribute(2, "k", &SearchAttributeValue::Integer(2));

        let bulk = BulkIndexer::new(idx.clone(), 10);
        bulk.add(BulkOperation::Remove { workflow_key: 1 });
        bulk.flush();
        assert_eq!(idx.workflow_count(), 1);
    }

    #[test]
    fn test_bulk_indexer_stats() {
        let idx = Arc::new(SearchAttributeIndex::new());
        let bulk = BulkIndexer::new(idx.clone(), 5);

        for i in 0..10 {
            bulk.add(BulkOperation::Index {
                workflow_key: i,
                attr_name: "x".into(),
                attr_value: SearchAttributeValue::Integer(i as i64),
            });
        }

        let stats = bulk.stats();
        assert_eq!(stats.operations_buffered, 10);
        assert!(stats.flush_count >= 1);
    }

    // ─── Index Lifecycle Tests ────────────────────────────────────────────

    #[test]
    fn test_lifecycle_create_index() {
        let mgr = IndexLifecycleManager::new();
        mgr.create_index("velocity-v1", 5, 1, 1).unwrap();
        assert_eq!(mgr.active_index_count(), 1);
        let meta = mgr.get_index("velocity-v1").unwrap();
        assert_eq!(meta.number_of_shards, 5);
        assert_eq!(meta.schema_version, 1);
    }

    #[test]
    fn test_lifecycle_create_idempotent() {
        let mgr = IndexLifecycleManager::new();
        mgr.create_index("idx", 1, 0, 1).unwrap();
        mgr.create_index("idx", 1, 0, 1).unwrap(); // no-op
        assert_eq!(mgr.active_index_count(), 1);
    }

    #[test]
    fn test_lifecycle_alias() {
        let mgr = IndexLifecycleManager::new();
        mgr.create_index("velocity-v1", 5, 1, 1).unwrap();
        mgr.put_alias("velocity", "velocity-v1").unwrap();
        assert_eq!(
            mgr.resolve_alias("velocity"),
            Some("velocity-v1".to_string())
        );
    }

    #[test]
    fn test_lifecycle_rollover() {
        let mgr = IndexLifecycleManager::new();
        mgr.create_index("velocity-v1", 5, 1, 1).unwrap();
        mgr.put_alias("velocity", "velocity-v1").unwrap();

        let new_name = mgr.rollover("velocity", 5, 1, 2).unwrap();
        // Old index should be read-only
        let old = mgr.get_index("velocity-v1").unwrap();
        assert_eq!(old.state, IndexState::ReadOnly);
        // New index should be active
        let new_idx = mgr.get_index(&new_name).unwrap();
        assert_eq!(new_idx.state, IndexState::Active);
        assert_eq!(new_idx.schema_version, 2);
        // Alias should point to new index
        assert_eq!(mgr.resolve_alias("velocity"), Some(new_name));
    }

    #[test]
    fn test_lifecycle_delete() {
        let mgr = IndexLifecycleManager::new();
        mgr.create_index("temp-idx", 1, 0, 1).unwrap();
        mgr.delete_index("temp-idx").unwrap();
        assert_eq!(mgr.active_index_count(), 0);
    }

    #[test]
    fn test_lifecycle_delete_cleans_aliases() {
        let mgr = IndexLifecycleManager::new();
        mgr.create_index("idx-1", 1, 0, 1).unwrap();
        mgr.put_alias("my-alias", "idx-1").unwrap();
        mgr.delete_index("idx-1").unwrap();
        assert_eq!(mgr.resolve_alias("my-alias"), None);
    }

    #[test]
    fn test_search_attribute_type_parsing() {
        assert_eq!(
            SearchAttributeType::from_type_name("keyword"),
            Some(SearchAttributeType::Keyword)
        );
        assert_eq!(
            SearchAttributeType::from_type_name("INT"),
            Some(SearchAttributeType::Int)
        );
        assert_eq!(
            SearchAttributeType::from_type_name("Boolean"),
            Some(SearchAttributeType::Bool)
        );
        assert_eq!(SearchAttributeType::from_type_name("unknown"), None);
    }

    #[test]
    fn test_search_attribute_type_capabilities() {
        assert!(SearchAttributeType::Keyword.supports_prefix());
        assert!(!SearchAttributeType::Int.supports_prefix());
        assert!(SearchAttributeType::Int.supports_range());
        assert!(SearchAttributeType::Datetime.supports_range());
        assert!(!SearchAttributeType::Bool.supports_range());
    }
}
