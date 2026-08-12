//! Namespace manager matching Temporal's common/namespace (~29 files).
//!
//! Covers: namespace lifecycle management, replication config, failover,
//! namespace registry with caching, and namespace replication notifications.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::SystemTime;

// ═══════════════════════════════════════════════════════════════════════════════
// Namespace Registry — cached namespace lookup with change notification
// ═══════════════════════════════════════════════════════════════════════════════

pub struct NamespaceRegistry {
    pub namespaces_by_id: RwLock<HashMap<String, Arc<NamespaceEntry>>>,
    pub namespaces_by_name: RwLock<HashMap<String, String>>,
    pub notification_version: AtomicI64,
    pub listeners: RwLock<Vec<NamespaceChangeListener>>,
    pub stats: NamespaceRegistryStats,
}

pub type NamespaceChangeListener = Box<dyn Fn(&NamespaceChangeEvent) + Send + Sync>;

#[derive(Debug, Clone)]
pub enum NamespaceChangeEvent {
    Created {
        namespace_id: String,
        name: String,
    },
    Updated {
        namespace_id: String,
        name: String,
    },
    Deleted {
        namespace_id: String,
        name: String,
    },
    Failover {
        namespace_id: String,
        active_cluster: String,
    },
}

#[derive(Debug, Default)]
pub struct NamespaceRegistryStats {
    pub lookups_by_id: AtomicU64,
    pub lookups_by_name: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub notifications_sent: AtomicU64,
}

impl NamespaceRegistry {
    pub fn new() -> Self {
        Self {
            namespaces_by_id: RwLock::new(HashMap::new()),
            namespaces_by_name: RwLock::new(HashMap::new()),
            notification_version: AtomicI64::new(0),
            listeners: RwLock::new(Vec::new()),
            stats: NamespaceRegistryStats::default(),
        }
    }

    pub fn register(&self, entry: NamespaceEntry) -> Result<(), NamespaceError> {
        let arc = Arc::new(entry.clone());
        let mut by_id = self.namespaces_by_id.write().unwrap();
        let mut by_name = self.namespaces_by_name.write().unwrap();
        if by_id.contains_key(&entry.id) {
            return Err(NamespaceError::AlreadyExists(entry.id.clone()));
        }
        if by_name.contains_key(&entry.name) {
            return Err(NamespaceError::NameExists(entry.name.clone()));
        }
        by_name.insert(entry.name.clone(), entry.id.clone());
        by_id.insert(entry.id.clone(), arc);
        let _version = self.notification_version.fetch_add(1, Ordering::Relaxed);
        self.notify(NamespaceChangeEvent::Created {
            namespace_id: entry.id,
            name: entry.name,
        });
        Ok(())
    }

    pub fn get_by_id(&self, id: &str) -> Result<Arc<NamespaceEntry>, NamespaceError> {
        self.stats.lookups_by_id.fetch_add(1, Ordering::Relaxed);
        self.namespaces_by_id
            .read()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or(NamespaceError::NotFound(id.into()))
    }

    pub fn get_by_name(&self, name: &str) -> Result<Arc<NamespaceEntry>, NamespaceError> {
        self.stats.lookups_by_name.fetch_add(1, Ordering::Relaxed);
        let id = self
            .namespaces_by_name
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or(NamespaceError::NotFound(name.into()))?;
        self.namespaces_by_id
            .read()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(NamespaceError::NotFound(id))
    }

    pub fn update(
        &self,
        id: &str,
        updater: impl Fn(&mut NamespaceEntry),
    ) -> Result<(), NamespaceError> {
        let by_id = self.namespaces_by_id.read().unwrap();
        let entry = by_id.get(id).ok_or(NamespaceError::NotFound(id.into()))?;
        let mut new_entry = (**entry).clone();
        updater(&mut new_entry);
        new_entry.last_updated = now_millis();
        drop(by_id);
        let arc = Arc::new(new_entry.clone());
        self.namespaces_by_id
            .write()
            .unwrap()
            .insert(id.to_string(), arc);
        self.notification_version.fetch_add(1, Ordering::Relaxed);
        self.notify(NamespaceChangeEvent::Updated {
            namespace_id: id.into(),
            name: new_entry.name,
        });
        Ok(())
    }

    pub fn deprecate(&self, id: &str) -> Result<(), NamespaceError> {
        self.update(id, |e| e.state = NamespaceLifecycleState::Deprecated)
    }

    pub fn delete(&self, id: &str) -> Result<(), NamespaceError> {
        let mut by_id = self.namespaces_by_id.write().unwrap();
        let entry = by_id
            .remove(id)
            .ok_or(NamespaceError::NotFound(id.into()))?;
        let name = entry.name.clone();
        self.namespaces_by_name.write().unwrap().remove(&name);
        self.notification_version.fetch_add(1, Ordering::Relaxed);
        self.notify(NamespaceChangeEvent::Deleted {
            namespace_id: id.into(),
            name,
        });
        Ok(())
    }

    pub fn failover(&self, id: &str, new_active_cluster: &str) -> Result<(), NamespaceError> {
        let new_active = new_active_cluster.to_string();
        self.update(id, |e| {
            e.active_cluster = new_active.clone();
            e.config.replication_config.active_cluster_name = new_active.clone();
            e.failover_version += 1;
        })?;
        let _entry = self.get_by_id(id)?;
        self.notify(NamespaceChangeEvent::Failover {
            namespace_id: id.into(),
            active_cluster: new_active_cluster.into(),
        });
        Ok(())
    }

    pub fn add_listener(&self, listener: NamespaceChangeListener) {
        self.listeners.write().unwrap().push(listener);
    }

    fn notify(&self, event: NamespaceChangeEvent) {
        let listeners = self.listeners.read().unwrap();
        for listener in listeners.iter() {
            listener(&event);
        }
        self.stats
            .notifications_sent
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn namespace_count(&self) -> usize {
        self.namespaces_by_id.read().unwrap().len()
    }

    pub fn all_namespaces(&self) -> Vec<Arc<NamespaceEntry>> {
        self.namespaces_by_id
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub fn current_version(&self) -> i64 {
        self.notification_version.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Namespace Entry
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct NamespaceEntry {
    pub id: String,
    pub name: String,
    pub state: NamespaceLifecycleState,
    pub description: String,
    pub owner_email: String,
    pub data: HashMap<String, String>,
    pub retention_days: i32,
    pub active_cluster: String,
    pub clusters: Vec<String>,
    pub failover_version: i64,
    pub is_global: bool,
    pub history_archival_enabled: bool,
    pub visibility_archival_enabled: bool,
    pub created_at: i64,
    pub last_updated: i64,
    pub config: NamespaceEntryConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceLifecycleState {
    Registered,
    Deprecated,
    Deleted,
}

#[derive(Debug, Clone)]
pub struct NamespaceEntryConfig {
    pub replication_config: ReplicationNsConfig,
    pub bad_binaries: Vec<BadBinary>,
    pub custom_search_attributes: HashMap<String, SearchAttrType>,
}

#[derive(Debug, Clone)]
pub struct ReplicationNsConfig {
    pub active_cluster_name: String,
    pub clusters: Vec<String>,
    pub state: ReplicationState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationState {
    Handover,
    Registered,
}

#[derive(Debug, Clone)]
pub struct BadBinary {
    pub binary_checksum: String,
    pub reason: String,
    pub operator: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchAttrType {
    Text,
    Keyword,
    Int,
    Double,
    Bool,
    Datetime,
    KeywordList,
}

impl NamespaceEntry {
    pub fn new(id: &str, name: &str, active_cluster: &str) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            state: NamespaceLifecycleState::Registered,
            description: String::new(),
            owner_email: String::new(),
            data: HashMap::new(),
            retention_days: 7,
            active_cluster: active_cluster.into(),
            clusters: vec![active_cluster.into()],
            failover_version: 0,
            is_global: false,
            history_archival_enabled: false,
            visibility_archival_enabled: false,
            created_at: now_millis(),
            last_updated: now_millis(),
            config: NamespaceEntryConfig {
                replication_config: ReplicationNsConfig {
                    active_cluster_name: active_cluster.into(),
                    clusters: vec![active_cluster.into()],
                    state: ReplicationState::Registered,
                },
                bad_binaries: Vec::new(),
                custom_search_attributes: HashMap::new(),
            },
        }
    }

    pub fn is_active(&self) -> bool {
        self.state == NamespaceLifecycleState::Registered
    }
    pub fn is_global(&self) -> bool {
        self.is_global
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Namespace Failover Manager
// ═══════════════════════════════════════════════════════════════════════════════

pub struct FailoverManager {
    pub registry: Arc<NamespaceRegistry>,
    pub active_failovers: RwLock<HashMap<String, FailoverState>>,
    pub stats: FailoverManagerStats,
}

#[derive(Debug, Clone)]
pub struct FailoverState {
    pub namespace_id: String,
    pub from_cluster: String,
    pub to_cluster: String,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    pub phase: FailoverPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverPhase {
    Pending,
    Handover,
    PostHandover,
    Completed,
    Failed,
}

#[derive(Debug, Default)]
pub struct FailoverManagerStats {
    pub failovers_started: AtomicU64,
    pub failovers_completed: AtomicU64,
    pub failovers_failed: AtomicU64,
}

impl FailoverManager {
    pub fn new(registry: Arc<NamespaceRegistry>) -> Self {
        Self {
            registry,
            active_failovers: RwLock::new(HashMap::new()),
            stats: FailoverManagerStats::default(),
        }
    }

    pub fn initiate_failover(
        &self,
        namespace_id: &str,
        target_cluster: &str,
    ) -> Result<(), NamespaceError> {
        let ns = self.registry.get_by_id(namespace_id)?;
        let from = ns.active_cluster.clone();
        let _name = ns.name.clone();
        self.registry.failover(namespace_id, target_cluster)?;
        let state = FailoverState {
            namespace_id: namespace_id.into(),
            from_cluster: from,
            to_cluster: target_cluster.into(),
            started_at: now_millis(),
            completed_at: None,
            phase: FailoverPhase::Completed,
        };
        self.active_failovers
            .write()
            .unwrap()
            .insert(namespace_id.into(), state);
        self.stats.failovers_started.fetch_add(1, Ordering::Relaxed);
        self.stats
            .failovers_completed
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn get_failover_state(&self, namespace_id: &str) -> Option<FailoverState> {
        self.active_failovers
            .read()
            .unwrap()
            .get(namespace_id)
            .cloned()
    }

    pub fn active_failover_count(&self) -> usize {
        self.active_failovers.read().unwrap().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum NamespaceError {
    NotFound(String),
    AlreadyExists(String),
    NameExists(String),
    InvalidState(String),
    ReplicationError(String),
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_registry_lifecycle() {
        let reg = NamespaceRegistry::new();
        let ns = NamespaceEntry::new("ns-1", "test-ns", "cluster-0");
        reg.register(ns).unwrap();
        assert_eq!(reg.namespace_count(), 1);
        let entry = reg.get_by_id("ns-1").unwrap();
        assert_eq!(entry.name, "test-ns");
        let by_name = reg.get_by_name("test-ns").unwrap();
        assert_eq!(by_name.id, "ns-1");
    }

    #[test]
    fn test_namespace_duplicate() {
        let reg = NamespaceRegistry::new();
        reg.register(NamespaceEntry::new("ns-1", "test", "c"))
            .unwrap();
        assert!(reg
            .register(NamespaceEntry::new("ns-1", "test", "c"))
            .is_err());
    }

    #[test]
    fn test_namespace_duplicate_name() {
        let reg = NamespaceRegistry::new();
        reg.register(NamespaceEntry::new("ns-1", "test", "c"))
            .unwrap();
        assert!(reg
            .register(NamespaceEntry::new("ns-2", "test", "c"))
            .is_err());
    }

    #[test]
    fn test_namespace_update() {
        let reg = NamespaceRegistry::new();
        reg.register(NamespaceEntry::new("ns-1", "test", "c"))
            .unwrap();
        reg.update("ns-1", |e| e.description = "updated".into())
            .unwrap();
        let entry = reg.get_by_id("ns-1").unwrap();
        assert_eq!(entry.description, "updated");
    }

    #[test]
    fn test_namespace_deprecate() {
        let reg = NamespaceRegistry::new();
        reg.register(NamespaceEntry::new("ns-1", "test", "c"))
            .unwrap();
        reg.deprecate("ns-1").unwrap();
        let entry = reg.get_by_id("ns-1").unwrap();
        assert_eq!(entry.state, NamespaceLifecycleState::Deprecated);
    }

    #[test]
    fn test_namespace_delete() {
        let reg = NamespaceRegistry::new();
        reg.register(NamespaceEntry::new("ns-1", "test", "c"))
            .unwrap();
        reg.delete("ns-1").unwrap();
        assert_eq!(reg.namespace_count(), 0);
        assert!(reg.get_by_id("ns-1").is_err());
    }

    #[test]
    fn test_namespace_failover() {
        let reg = NamespaceRegistry::new();
        reg.register(NamespaceEntry::new("ns-1", "test", "cluster-0"))
            .unwrap();
        reg.failover("ns-1", "cluster-1").unwrap();
        let entry = reg.get_by_id("ns-1").unwrap();
        assert_eq!(entry.active_cluster, "cluster-1");
        assert_eq!(entry.failover_version, 1);
    }

    #[test]
    fn test_namespace_listener() {
        let reg = NamespaceRegistry::new();
        let events = Arc::new(RwLock::new(Vec::new()));
        let events_clone = events.clone();
        reg.add_listener(Box::new(move |e| {
            events_clone.write().unwrap().push(format!("{:?}", e));
        }));
        reg.register(NamespaceEntry::new("ns-1", "test", "c"))
            .unwrap();
        assert_eq!(events.read().unwrap().len(), 1);
    }

    #[test]
    fn test_namespace_version() {
        let reg = NamespaceRegistry::new();
        assert_eq!(reg.current_version(), 0);
        reg.register(NamespaceEntry::new("ns-1", "test", "c"))
            .unwrap();
        assert_eq!(reg.current_version(), 1);
        reg.update("ns-1", |e| e.description = "x".into()).unwrap();
        assert_eq!(reg.current_version(), 2);
    }

    #[test]
    fn test_failover_manager() {
        let reg = Arc::new(NamespaceRegistry::new());
        reg.register(NamespaceEntry::new("ns-1", "test", "cluster-0"))
            .unwrap();
        let fm = FailoverManager::new(reg.clone());
        fm.initiate_failover("ns-1", "cluster-1").unwrap();
        let entry = reg.get_by_id("ns-1").unwrap();
        assert_eq!(entry.active_cluster, "cluster-1");
        assert_eq!(fm.stats.failovers_completed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_namespace_entry_is_active() {
        let ns = NamespaceEntry::new("ns-1", "test", "c");
        assert!(ns.is_active());
        let mut deprecated = ns.clone();
        deprecated.state = NamespaceLifecycleState::Deprecated;
        assert!(!deprecated.is_active());
    }

    #[test]
    fn test_all_namespaces() {
        let reg = NamespaceRegistry::new();
        reg.register(NamespaceEntry::new("ns-1", "a", "c")).unwrap();
        reg.register(NamespaceEntry::new("ns-2", "b", "c")).unwrap();
        let all = reg.all_namespaces();
        assert_eq!(all.len(), 2);
    }
}
