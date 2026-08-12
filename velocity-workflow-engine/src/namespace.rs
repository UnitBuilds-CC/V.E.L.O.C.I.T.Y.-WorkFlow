//! Namespace isolation and multi-tenancy support.
//! Each namespace has its own configuration, retention policy, and search attributes.
//! Workflows in different namespaces are completely isolated.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    RwLock,
};
use std::time::Duration;

// ─── Namespace Config ─────────────────────────────────────────────────────────

/// Configuration for a namespace.
#[derive(Debug, Clone)]
pub struct NamespaceConfig {
    /// Human-readable namespace name (e.g., "production", "staging").
    pub name: String,
    /// Unique numeric ID for this namespace.
    pub id: u64,
    /// How long completed workflows are retained before deletion.
    pub retention_period: Duration,
    /// Maximum number of concurrent workflow executions (0 = unlimited).
    pub max_concurrent_workflows: u64,
    /// Whether this namespace is active (accepts new workflows).
    pub is_active: bool,
    /// Optional description.
    pub description: String,
    /// Custom key-value metadata for the namespace.
    pub metadata: HashMap<String, String>,
}

impl NamespaceConfig {
    pub fn new(id: u64, name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            id,
            retention_period: Duration::from_secs(7 * 24 * 3600), // 7 days default
            max_concurrent_workflows: 0,
            is_active: true,
            description: String::new(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention_period = retention;
        self
    }

    pub fn with_max_concurrent(mut self, max: u64) -> Self {
        self.max_concurrent_workflows = max;
        self
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }
}

// ─── Namespace Registry ──────────────────────────────────────────────────────

/// Thread-safe namespace registry. Manages namespace lifecycle and configuration.
pub struct NamespaceRegistry {
    /// Namespaces indexed by their numeric ID.
    by_id: RwLock<HashMap<u64, NamespaceConfig>>,
    /// Namespaces indexed by name (for lookup by name).
    by_name: RwLock<HashMap<String, u64>>,
    /// Per-namespace workflow count (for concurrency limits).
    workflow_counts: RwLock<HashMap<u64, AtomicU64>>,
    /// Next namespace ID.
    next_id: AtomicU64,
}

impl NamespaceRegistry {
    pub fn new() -> Self {
        let registry = Self {
            by_id: RwLock::new(HashMap::new()),
            by_name: RwLock::new(HashMap::new()),
            workflow_counts: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        };

        // Register the default namespace
        let default_ns = NamespaceConfig::new(0, "default");
        registry.register(default_ns).ok();

        registry
    }

    /// Register a new namespace. Returns the namespace ID.
    pub fn register(&self, config: NamespaceConfig) -> Result<u64, NamespaceError> {
        let mut by_name = self.by_name.write().unwrap();
        if by_name.contains_key(&config.name) {
            return Err(NamespaceError::AlreadyExists(config.name.clone()));
        }

        let id = config.id;
        let name = config.name.clone();

        let mut by_id = self.by_id.write().unwrap();
        by_id.insert(id, config);
        by_name.insert(name, id);

        let mut counts = self.workflow_counts.write().unwrap();
        counts.insert(id, AtomicU64::new(0));

        Ok(id)
    }

    /// Register a namespace with auto-generated ID.
    pub fn register_auto(&self, name: impl Into<String>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let config = NamespaceConfig::new(id, name);
        self.register(config).unwrap_or(id)
    }

    /// Get namespace config by ID.
    pub fn get(&self, id: u64) -> Option<NamespaceConfig> {
        let by_id = self.by_id.read().unwrap();
        by_id.get(&id).cloned()
    }

    /// Get namespace ID by name.
    pub fn get_by_name(&self, name: &str) -> Option<u64> {
        let by_name = self.by_name.read().unwrap();
        by_name.get(name).copied()
    }

    /// Check if a namespace exists and is active.
    pub fn is_active(&self, id: u64) -> bool {
        let by_id = self.by_id.read().unwrap();
        by_id.get(&id).is_some_and(|ns| ns.is_active)
    }

    /// Increment the workflow count for a namespace (called when a workflow starts).
    /// Returns false if the concurrency limit would be exceeded.
    pub fn increment_workflow_count(&self, namespace_id: u64) -> bool {
        let counts = self.workflow_counts.read().unwrap();
        if let Some(counter) = counts.get(&namespace_id) {
            let by_id = self.by_id.read().unwrap();
            if let Some(ns) = by_id.get(&namespace_id) {
                if ns.max_concurrent_workflows > 0 {
                    let current = counter.load(Ordering::Relaxed);
                    if current >= ns.max_concurrent_workflows {
                        return false;
                    }
                }
            }
            counter.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Decrement the workflow count for a namespace (called when a workflow completes).
    pub fn decrement_workflow_count(&self, namespace_id: u64) {
        let counts = self.workflow_counts.read().unwrap();
        if let Some(counter) = counts.get(&namespace_id) {
            counter.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Get the current workflow count for a namespace.
    pub fn workflow_count(&self, namespace_id: u64) -> u64 {
        let counts = self.workflow_counts.read().unwrap();
        counts
            .get(&namespace_id)
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }

    /// Deactivate a namespace (stop accepting new workflows).
    pub fn deactivate(&self, id: u64) -> bool {
        let mut by_id = self.by_id.write().unwrap();
        if let Some(ns) = by_id.get_mut(&id) {
            ns.is_active = false;
            true
        } else {
            false
        }
    }

    /// Activate a namespace.
    pub fn activate(&self, id: u64) -> bool {
        let mut by_id = self.by_id.write().unwrap();
        if let Some(ns) = by_id.get_mut(&id) {
            ns.is_active = true;
            true
        } else {
            false
        }
    }

    /// Delete a namespace.
    pub fn delete(&self, id: u64) -> Result<(), NamespaceError> {
        if id == 0 {
            return Err(NamespaceError::CannotDeleteDefault);
        }

        let mut by_id = self.by_id.write().unwrap();
        let ns = by_id.remove(&id).ok_or(NamespaceError::NotFound(id))?;

        let mut by_name = self.by_name.write().unwrap();
        by_name.remove(&ns.name);

        let mut counts = self.workflow_counts.write().unwrap();
        counts.remove(&id);

        Ok(())
    }

    /// List all registered namespaces.
    pub fn list(&self) -> Vec<NamespaceConfig> {
        let by_id = self.by_id.read().unwrap();
        by_id.values().cloned().collect()
    }

    /// Get the total number of registered namespaces.
    pub fn count(&self) -> usize {
        let by_id = self.by_id.read().unwrap();
        by_id.len()
    }
}

impl Default for NamespaceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Errors ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceError {
    AlreadyExists(String),
    NotFound(u64),
    CannotDeleteDefault,
    Inactive(u64),
    ConcurrencyLimitExceeded(u64),
}

impl std::fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists(name) => write!(f, "namespace '{}' already exists", name),
            Self::NotFound(id) => write!(f, "namespace {} not found", id),
            Self::CannotDeleteDefault => write!(f, "cannot delete the default namespace"),
            Self::Inactive(id) => write!(f, "namespace {} is not active", id),
            Self::ConcurrencyLimitExceeded(id) => {
                write!(f, "namespace {} concurrency limit exceeded", id)
            }
        }
    }
}

impl std::error::Error for NamespaceError {}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_namespace_exists() {
        let registry = NamespaceRegistry::new();
        assert!(registry.get(0).is_some());
        assert_eq!(registry.get(0).unwrap().name, "default");
        assert!(registry.is_active(0));
    }

    #[test]
    fn test_register_namespace() {
        let registry = NamespaceRegistry::new();
        let config = NamespaceConfig::new(1, "production")
            .with_retention(Duration::from_secs(30 * 24 * 3600))
            .with_description("Production environment");

        let id = registry.register(config).unwrap();
        assert_eq!(id, 1);

        let ns = registry.get(1).unwrap();
        assert_eq!(ns.name, "production");
        assert_eq!(ns.retention_period, Duration::from_secs(30 * 24 * 3600));
        assert_eq!(ns.description, "Production environment");
    }

    #[test]
    fn test_register_duplicate_name() {
        let registry = NamespaceRegistry::new();
        registry
            .register(NamespaceConfig::new(1, "staging"))
            .unwrap();
        let result = registry.register(NamespaceConfig::new(2, "staging"));
        assert_eq!(result, Err(NamespaceError::AlreadyExists("staging".into())));
    }

    #[test]
    fn test_lookup_by_name() {
        let registry = NamespaceRegistry::new();
        registry
            .register(NamespaceConfig::new(5, "test-ns"))
            .unwrap();
        assert_eq!(registry.get_by_name("test-ns"), Some(5));
        assert_eq!(registry.get_by_name("nonexistent"), None);
    }

    #[test]
    fn test_activate_deactivate() {
        let registry = NamespaceRegistry::new();
        registry.register(NamespaceConfig::new(1, "ns1")).unwrap();

        assert!(registry.is_active(1));
        registry.deactivate(1);
        assert!(!registry.is_active(1));
        registry.activate(1);
        assert!(registry.is_active(1));
    }

    #[test]
    fn test_concurrency_limit() {
        let registry = NamespaceRegistry::new();
        let config = NamespaceConfig::new(1, "limited").with_max_concurrent(2);
        registry.register(config).unwrap();

        assert!(registry.increment_workflow_count(1));
        assert!(registry.increment_workflow_count(1));
        // Third should fail — limit is 2
        assert!(!registry.increment_workflow_count(1));

        registry.decrement_workflow_count(1);
        // Now there's room
        assert!(registry.increment_workflow_count(1));
    }

    #[test]
    fn test_delete_namespace() {
        let registry = NamespaceRegistry::new();
        registry
            .register(NamespaceConfig::new(1, "to-delete"))
            .unwrap();
        assert_eq!(registry.count(), 2); // default + to-delete

        registry.delete(1).unwrap();
        assert_eq!(registry.count(), 1);
        assert!(registry.get(1).is_none());
    }

    #[test]
    fn test_cannot_delete_default() {
        let registry = NamespaceRegistry::new();
        assert_eq!(registry.delete(0), Err(NamespaceError::CannotDeleteDefault));
    }

    #[test]
    fn test_list_namespaces() {
        let registry = NamespaceRegistry::new();
        registry.register(NamespaceConfig::new(1, "ns1")).unwrap();
        registry.register(NamespaceConfig::new(2, "ns2")).unwrap();

        let list = registry.list();
        assert_eq!(list.len(), 3); // default + ns1 + ns2
    }
}
