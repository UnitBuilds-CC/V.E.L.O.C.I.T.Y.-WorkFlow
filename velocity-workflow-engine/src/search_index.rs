//! B-tree indexed search attributes for O(log n) range queries.
//! Replaces the linear scan in `visibility.rs` with a sorted BTreeMap index
//! supporting exact match, range, prefix, and comparison queries.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::RwLock;

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
    pub fn index_attribute(&self, workflow_key: u64, attr_name: &str, attr_value: &SearchAttributeValue) {
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
    pub fn remove_attribute(&self, workflow_key: u64, attr_name: &str, attr_value: &SearchAttributeValue) {
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
        index.get(&key).map(|s| s.iter().copied().collect()).unwrap_or_default()
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
        index.range(low_key..=high_key)
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
        index.range(low_key..=high_key)
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
        index.range(low_key..high_key)
            .filter(|(key, _)| key.attr_value.to_search_attribute_value()
                .matches_string_prefix(prefix))
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
        index.range(low_key..=high_key)
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
        index.range(low_key..=high_key)
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

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_exact_match() {
        let idx = SearchAttributeIndex::new();
        idx.index_attribute(1, "customer_id", &SearchAttributeValue::String("C123".into()));
        idx.index_attribute(2, "customer_id", &SearchAttributeValue::String("C456".into()));
        idx.index_attribute(3, "customer_id", &SearchAttributeValue::String("C123".into()));

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
        idx.index_attribute(4, "name", &SearchAttributeValue::String("alice_smith".into()));

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
        idx.index_attribute(1, "region", &SearchAttributeValue::Keyword("us-east".into()));
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
        assert!(idx.exact_match("key", &SearchAttributeValue::String("val".into())).is_empty());
        assert!(idx.range_integer("key", 0, 100).is_empty());
        assert!(idx.prefix_match("key", "pre").is_empty());
    }
}
