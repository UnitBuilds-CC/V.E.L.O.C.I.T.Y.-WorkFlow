//! Search attribute definitions matching Temporal's common/searchattribute (4,763 lines).
//!
//! Covers: search attribute types, field definitions, validation, mapping,
//! system vs custom attributes, aliases, and type conversion.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Search Attribute Types
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchAttributeType {
    Text = 1,
    Keyword = 2,
    Int = 3,
    Double = 4,
    Bool = 5,
    Datetime = 6,
    KeywordList = 7,
}

impl SearchAttributeType {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            1 => Some(Self::Text),
            2 => Some(Self::Keyword),
            3 => Some(Self::Int),
            4 => Some(Self::Double),
            5 => Some(Self::Bool),
            6 => Some(Self::Datetime),
            7 => Some(Self::KeywordList),
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

    pub fn is_valid_value(&self, value: &SearchAttributeValue) -> bool {
        match (self, value) {
            (Self::Text, SearchAttributeValue::Text(_)) => true,
            (Self::Keyword, SearchAttributeValue::Keyword(_)) => true,
            (Self::Int, SearchAttributeValue::Int(_)) => true,
            (Self::Double, SearchAttributeValue::Double(_)) => true,
            (Self::Bool, SearchAttributeValue::Bool(_)) => true,
            (Self::Datetime, SearchAttributeValue::Datetime(_)) => true,
            (Self::KeywordList, SearchAttributeValue::KeywordList(_)) => true,
            _ => false,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Search Attribute Values
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq)]
pub enum SearchAttributeValue {
    Text(String),
    Keyword(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Datetime(i64), // Unix nanos
    KeywordList(Vec<String>),
    Unspecified(Vec<u8>),
}

impl SearchAttributeValue {
    pub fn type_of(&self) -> SearchAttributeType {
        match self {
            Self::Text(_) => SearchAttributeType::Text,
            Self::Keyword(_) => SearchAttributeType::Keyword,
            Self::Int(_) => SearchAttributeType::Int,
            Self::Double(_) => SearchAttributeType::Double,
            Self::Bool(_) => SearchAttributeType::Bool,
            Self::Datetime(_) => SearchAttributeType::Datetime,
            Self::KeywordList(_) => SearchAttributeType::KeywordList,
            Self::Unspecified(_) => SearchAttributeType::Text, // fallback
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) | Self::Keyword(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        match self {
            Self::Double(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<i64> {
        match self {
            Self::Datetime(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_keyword_list(&self) -> Option<&[String]> {
        match self {
            Self::KeywordList(v) => Some(v),
            _ => None,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Search Attribute Field
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct SearchAttributeField {
    pub name: String,
    pub field_type: SearchAttributeType,
    pub is_system: bool,
    pub alias: Option<String>,
    pub description: String,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Search Attribute Definition / Schema
// ═══════════════════════════════════════════════════════════════════════════════

pub struct SearchAttributeDefinition {
    pub fields: RwLock<HashMap<String, SearchAttributeField>>,
    pub aliases: RwLock<HashMap<String, String>>,
    stats: SearchAttributeStats,
}

#[derive(Debug, Default)]
pub struct SearchAttributeStats {
    pub system_attributes: AtomicU64,
    pub custom_attributes: AtomicU64,
    pub validations_performed: AtomicU64,
    pub validation_failures: AtomicU64,
}

impl SearchAttributeDefinition {
    pub fn new() -> Self {
        let def = Self {
            fields: RwLock::new(HashMap::new()),
            aliases: RwLock::new(HashMap::new()),
            stats: SearchAttributeStats::default(),
        };
        def.register_system_attributes();
        def
    }

    fn register_system_attributes(&self) {
        let system_attrs = vec![
            ("WorkflowId", SearchAttributeType::Keyword, "Workflow ID"),
            ("RunId", SearchAttributeType::Keyword, "Run ID"),
            (
                "WorkflowType",
                SearchAttributeType::Keyword,
                "Workflow type name",
            ),
            (
                "StartTime",
                SearchAttributeType::Datetime,
                "Workflow start time",
            ),
            (
                "CloseTime",
                SearchAttributeType::Datetime,
                "Workflow close time",
            ),
            (
                "ExecutionStatus",
                SearchAttributeType::Keyword,
                "Workflow execution status",
            ),
            (
                "ExecutionDuration",
                SearchAttributeType::Int,
                "Workflow execution duration in nanos",
            ),
            (
                "StateTransitionCount",
                SearchAttributeType::Int,
                "State transition count",
            ),
            (
                "HistoryLength",
                SearchAttributeType::Int,
                "History event count",
            ),
            ("TaskQueue", SearchAttributeType::Keyword, "Task queue name"),
            ("Namespace", SearchAttributeType::Keyword, "Namespace name"),
            ("NamespaceId", SearchAttributeType::Keyword, "Namespace ID"),
            (
                "TemporalChangeVersion",
                SearchAttributeType::KeywordList,
                "Change version list",
            ),
            (
                "BatchOperationId",
                SearchAttributeType::Keyword,
                "Batch operation ID",
            ),
            (
                "ParentWorkflowId",
                SearchAttributeType::Keyword,
                "Parent workflow ID",
            ),
            ("ParentRunId", SearchAttributeType::Keyword, "Parent run ID"),
            (
                "RootWorkflowId",
                SearchAttributeType::Keyword,
                "Root workflow ID",
            ),
            ("RootRunId", SearchAttributeType::Keyword, "Root run ID"),
            (
                "BinaryChecksums",
                SearchAttributeType::KeywordList,
                "Binary checksums",
            ),
            (
                "HistorySizeBytes",
                SearchAttributeType::Int,
                "History size in bytes",
            ),
            ("ShardId", SearchAttributeType::Int, "Shard ID"),
        ];

        let mut fields = self.fields.write().unwrap();
        for (name, sa_type, desc) in system_attrs {
            fields.insert(
                name.to_string(),
                SearchAttributeField {
                    name: name.to_string(),
                    field_type: sa_type,
                    is_system: true,
                    alias: None,
                    description: desc.to_string(),
                },
            );
            self.stats.system_attributes.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn add_custom_attribute(
        &self,
        name: &str,
        sa_type: SearchAttributeType,
    ) -> Result<(), SearchAttributeError> {
        let mut fields = self.fields.write().unwrap();
        if fields.contains_key(name) {
            return Err(SearchAttributeError::AlreadyExists(name.to_string()));
        }
        if Self::is_reserved_name(name) {
            return Err(SearchAttributeError::ReservedName(name.to_string()));
        }
        fields.insert(
            name.to_string(),
            SearchAttributeField {
                name: name.to_string(),
                field_type: sa_type,
                is_system: false,
                alias: None,
                description: String::new(),
            },
        );
        self.stats.custom_attributes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn remove_custom_attribute(&self, name: &str) -> Result<(), SearchAttributeError> {
        let mut fields = self.fields.write().unwrap();
        let field = fields
            .get(name)
            .ok_or(SearchAttributeError::NotFound(name.to_string()))?;
        if field.is_system {
            return Err(SearchAttributeError::CannotRemoveSystem(name.to_string()));
        }
        fields.remove(name);
        Ok(())
    }

    pub fn get_field(&self, name: &str) -> Option<SearchAttributeField> {
        let fields = self.fields.read().unwrap();
        // Check aliases first
        let resolved = {
            let aliases = self.aliases.read().unwrap();
            aliases
                .get(name)
                .cloned()
                .unwrap_or_else(|| name.to_string())
        };
        fields.get(&resolved).cloned()
    }

    pub fn add_alias(&self, alias: &str, target: &str) -> Result<(), SearchAttributeError> {
        let fields = self.fields.read().unwrap();
        if !fields.contains_key(target) {
            return Err(SearchAttributeError::NotFound(target.to_string()));
        }
        self.aliases
            .write()
            .unwrap()
            .insert(alias.to_string(), target.to_string());
        Ok(())
    }

    pub fn validate(
        &self,
        name: &str,
        value: &SearchAttributeValue,
    ) -> Result<(), SearchAttributeError> {
        self.stats
            .validations_performed
            .fetch_add(1, Ordering::Relaxed);
        let fields = self.fields.read().unwrap();
        let resolved = {
            let aliases = self.aliases.read().unwrap();
            aliases
                .get(name)
                .cloned()
                .unwrap_or_else(|| name.to_string())
        };
        let field = fields
            .get(&resolved)
            .ok_or(SearchAttributeError::NotFound(name.to_string()))?;
        if !field.field_type.is_valid_value(value) {
            self.stats
                .validation_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(SearchAttributeError::TypeMismatch {
                field: name.to_string(),
                expected: field.field_type.type_name().to_string(),
                got: value.type_of().type_name().to_string(),
            });
        }
        Ok(())
    }

    pub fn validate_attributes(
        &self,
        attrs: &HashMap<String, SearchAttributeValue>,
    ) -> Result<(), SearchAttributeError> {
        for (name, value) in attrs {
            self.validate(name, value)?;
        }
        Ok(())
    }

    pub fn list_fields(&self) -> Vec<SearchAttributeField> {
        self.fields.read().unwrap().values().cloned().collect()
    }

    pub fn list_system_fields(&self) -> Vec<SearchAttributeField> {
        self.fields
            .read()
            .unwrap()
            .values()
            .filter(|f| f.is_system)
            .cloned()
            .collect()
    }

    pub fn list_custom_fields(&self) -> Vec<SearchAttributeField> {
        self.fields
            .read()
            .unwrap()
            .values()
            .filter(|f| !f.is_system)
            .cloned()
            .collect()
    }

    pub fn field_count(&self) -> usize {
        self.fields.read().unwrap().len()
    }

    pub fn is_reserved_name(name: &str) -> bool {
        name.starts_with("Temporal") || name.starts_with("_")
    }

    pub fn stats(&self) -> &SearchAttributeStats {
        &self.stats
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Search Attribute Mapper
// ═══════════════════════════════════════════════════════════════════════════════

pub struct SearchAttributeMapper {
    field_to_db_column: RwLock<HashMap<String, String>>,
    db_column_to_field: RwLock<HashMap<String, String>>,
}

impl SearchAttributeMapper {
    pub fn new() -> Self {
        Self {
            field_to_db_column: RwLock::new(HashMap::new()),
            db_column_to_field: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_mapping(&self, field_name: &str, db_column: &str) {
        self.field_to_db_column
            .write()
            .unwrap()
            .insert(field_name.to_string(), db_column.to_string());
        self.db_column_to_field
            .write()
            .unwrap()
            .insert(db_column.to_string(), field_name.to_string());
    }

    pub fn field_to_column(&self, field_name: &str) -> Option<String> {
        self.field_to_db_column
            .read()
            .unwrap()
            .get(field_name)
            .cloned()
    }

    pub fn column_to_field(&self, db_column: &str) -> Option<String> {
        self.db_column_to_field
            .read()
            .unwrap()
            .get(db_column)
            .cloned()
    }

    pub fn map_attributes_to_columns(
        &self,
        attrs: &HashMap<String, SearchAttributeValue>,
    ) -> HashMap<String, SearchAttributeValue> {
        let mapping = self.field_to_db_column.read().unwrap();
        attrs
            .iter()
            .map(|(k, v)| {
                let col = mapping.get(k).cloned().unwrap_or_else(|| k.clone());
                (col, v.clone())
            })
            .collect()
    }

    pub fn map_columns_to_attributes(
        &self,
        columns: &HashMap<String, SearchAttributeValue>,
    ) -> HashMap<String, SearchAttributeValue> {
        let mapping = self.db_column_to_field.read().unwrap();
        columns
            .iter()
            .map(|(k, v)| {
                let field = mapping.get(k).cloned().unwrap_or_else(|| k.clone());
                (field, v.clone())
            })
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Errors
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum SearchAttributeError {
    NotFound(String),
    AlreadyExists(String),
    ReservedName(String),
    CannotRemoveSystem(String),
    TypeMismatch {
        field: String,
        expected: String,
        got: String,
    },
    InvalidValue {
        field: String,
        message: String,
    },
}

impl std::fmt::Display for SearchAttributeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(n) => write!(f, "search attribute not found: {}", n),
            Self::AlreadyExists(n) => write!(f, "search attribute already exists: {}", n),
            Self::ReservedName(n) => write!(f, "reserved name: {}", n),
            Self::CannotRemoveSystem(n) => write!(f, "cannot remove system attribute: {}", n),
            Self::TypeMismatch {
                field,
                expected,
                got,
            } => write!(
                f,
                "type mismatch for {}: expected {}, got {}",
                field, expected, got
            ),
            Self::InvalidValue { field, message } => {
                write!(f, "invalid value for {}: {}", field, message)
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_attributes_registered() {
        let def = SearchAttributeDefinition::new();
        assert!(def.field_count() >= 20);
        let sys = def.list_system_fields();
        assert!(sys.len() >= 20);
    }

    #[test]
    fn test_get_system_field() {
        let def = SearchAttributeDefinition::new();
        let field = def.get_field("WorkflowId").unwrap();
        assert_eq!(field.field_type, SearchAttributeType::Keyword);
        assert!(field.is_system);
    }

    #[test]
    fn test_add_custom_attribute() {
        let def = SearchAttributeDefinition::new();
        def.add_custom_attribute("CustomField", SearchAttributeType::Text)
            .unwrap();
        let field = def.get_field("CustomField").unwrap();
        assert_eq!(field.field_type, SearchAttributeType::Text);
        assert!(!field.is_system);
    }

    #[test]
    fn test_add_duplicate_custom() {
        let def = SearchAttributeDefinition::new();
        def.add_custom_attribute("MyField", SearchAttributeType::Int)
            .unwrap();
        let err = def
            .add_custom_attribute("MyField", SearchAttributeType::Text)
            .unwrap_err();
        assert!(matches!(err, SearchAttributeError::AlreadyExists(_)));
    }

    #[test]
    fn test_reserved_name() {
        let def = SearchAttributeDefinition::new();
        let err = def
            .add_custom_attribute("TemporalCustom", SearchAttributeType::Text)
            .unwrap_err();
        assert!(matches!(err, SearchAttributeError::ReservedName(_)));
    }

    #[test]
    fn test_validate_correct_type() {
        let def = SearchAttributeDefinition::new();
        def.validate(
            "WorkflowId",
            &SearchAttributeValue::Keyword("wf-1".to_string()),
        )
        .unwrap();
    }

    #[test]
    fn test_validate_wrong_type() {
        let def = SearchAttributeDefinition::new();
        let err = def
            .validate("WorkflowId", &SearchAttributeValue::Int(42))
            .unwrap_err();
        assert!(matches!(err, SearchAttributeError::TypeMismatch { .. }));
    }

    #[test]
    fn test_validate_not_found() {
        let def = SearchAttributeDefinition::new();
        let err = def
            .validate("NonExistent", &SearchAttributeValue::Text("x".to_string()))
            .unwrap_err();
        assert!(matches!(err, SearchAttributeError::NotFound(_)));
    }

    #[test]
    fn test_remove_custom_attribute() {
        let def = SearchAttributeDefinition::new();
        def.add_custom_attribute("Temp", SearchAttributeType::Bool)
            .unwrap();
        def.remove_custom_attribute("Temp").unwrap();
        assert!(def.get_field("Temp").is_none());
    }

    #[test]
    fn test_cannot_remove_system() {
        let def = SearchAttributeDefinition::new();
        let err = def.remove_custom_attribute("WorkflowId").unwrap_err();
        assert!(matches!(err, SearchAttributeError::CannotRemoveSystem(_)));
    }

    #[test]
    fn test_alias() {
        let def = SearchAttributeDefinition::new();
        def.add_alias("wf_id", "WorkflowId").unwrap();
        let field = def.get_field("wf_id").unwrap();
        assert_eq!(field.name, "WorkflowId");
    }

    #[test]
    fn test_mapper() {
        let mapper = SearchAttributeMapper::new();
        mapper.add_mapping("WorkflowId", "workflow_id");
        mapper.add_mapping("RunId", "run_id");

        assert_eq!(
            mapper.field_to_column("WorkflowId"),
            Some("workflow_id".to_string())
        );
        assert_eq!(mapper.column_to_field("run_id"), Some("RunId".to_string()));
    }

    #[test]
    fn test_mapper_batch_convert() {
        let mapper = SearchAttributeMapper::new();
        mapper.add_mapping("WorkflowId", "workflow_id");

        let mut attrs = HashMap::new();
        attrs.insert(
            "WorkflowId".to_string(),
            SearchAttributeValue::Keyword("wf-1".to_string()),
        );
        attrs.insert(
            "CustomField".to_string(),
            SearchAttributeValue::Text("hello".to_string()),
        );

        let mapped = mapper.map_attributes_to_columns(&attrs);
        assert!(mapped.contains_key("workflow_id"));
        assert!(mapped.contains_key("CustomField")); // unmapped stays same
    }

    #[test]
    fn test_attribute_value_type_of() {
        assert_eq!(
            SearchAttributeValue::Text("x".into()).type_of(),
            SearchAttributeType::Text
        );
        assert_eq!(
            SearchAttributeValue::Int(42).type_of(),
            SearchAttributeType::Int
        );
        assert_eq!(
            SearchAttributeValue::Bool(true).type_of(),
            SearchAttributeType::Bool
        );
    }

    #[test]
    fn test_attribute_value_accessors() {
        assert_eq!(
            SearchAttributeValue::Text("hello".into()).as_text(),
            Some("hello")
        );
        assert_eq!(SearchAttributeValue::Int(42).as_int(), Some(42));
        assert_eq!(SearchAttributeValue::Double(3.14).as_double(), Some(3.14));
        assert_eq!(SearchAttributeValue::Bool(true).as_bool(), Some(true));
        assert_eq!(
            SearchAttributeValue::Datetime(12345).as_datetime(),
            Some(12345)
        );
    }

    #[test]
    fn test_validate_batch() {
        let def = SearchAttributeDefinition::new();
        let mut attrs = HashMap::new();
        attrs.insert(
            "WorkflowId".to_string(),
            SearchAttributeValue::Keyword("wf-1".to_string()),
        );
        attrs.insert(
            "RunId".to_string(),
            SearchAttributeValue::Keyword("run-1".to_string()),
        );
        def.validate_attributes(&attrs).unwrap();
    }

    #[test]
    fn test_search_attribute_type_from_i32() {
        assert_eq!(
            SearchAttributeType::from_i32(1),
            Some(SearchAttributeType::Text)
        );
        assert_eq!(
            SearchAttributeType::from_i32(7),
            Some(SearchAttributeType::KeywordList)
        );
        assert_eq!(SearchAttributeType::from_i32(99), None);
    }
}
