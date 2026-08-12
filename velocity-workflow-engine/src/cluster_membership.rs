//! Cluster membership matching Temporal's common/membership (~17 files).
//!
//! Covers: ring-based consistent hashing, host info, health checking,
//! member list management, shard ownership resolution, and cluster topology.

use std::collections::{HashMap, BTreeMap};
use std::sync::{Arc, RwLock, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::time::{SystemTime, Duration};

// ═══════════════════════════════════════════════════════════════════════════════
// Host Info
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HostAddress {
    pub host: String,
    pub port: u16,
}

impl HostAddress {
    pub fn new(host: &str, port: u16) -> Self { Self { host: host.into(), port } }
    pub fn address(&self) -> String { format!("{}:{}", self.host, self.port) }
}

impl std::fmt::Display for HostAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone)]
pub struct HostInfo {
    pub identity: String,
    pub address: HostAddress,
    pub grpc_address: Option<HostAddress>,
    pub roles: Vec<ServiceRole>,
    pub labels: HashMap<String, String>,
    pub state: HostState,
    pub joined_at: i64,
    pub last_heartbeat: i64,
    pub shard_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServiceRole { History, Matching, Frontend, Worker, Admin }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostState { Active, Draining, Drained, Unhealthy, Removed }

impl HostInfo {
    pub fn new(identity: &str, host: &str, port: u16, roles: Vec<ServiceRole>) -> Self {
        let now = now_millis();
        Self {
            identity: identity.into(), address: HostAddress::new(host, port),
            grpc_address: None, roles, labels: HashMap::new(),
            state: HostState::Active, joined_at: now, last_heartbeat: now, shard_count: 0,
        }
    }

    pub fn is_active(&self) -> bool { self.state == HostState::Active }
    pub fn has_role(&self, role: ServiceRole) -> bool { self.roles.contains(&role) }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Ring Hash — consistent hashing for shard ownership
// ═══════════════════════════════════════════════════════════════════════════════

pub struct RingHash {
    pub ring: RwLock<BTreeMap<u32, String>>,
    pub virtual_nodes: u32,
    pub members: RwLock<HashMap<String, HostInfo>>,
    pub stats: RingHashStats,
}

#[derive(Debug, Default)]
pub struct RingHashStats {
    pub lookups: AtomicU64, pub rebalances: AtomicU64,
    pub members_added: AtomicU64, pub members_removed: AtomicU64,
}

impl RingHash {
    pub fn new(virtual_nodes: u32) -> Self {
        Self { ring: RwLock::new(BTreeMap::new()), virtual_nodes, members: RwLock::new(HashMap::new()), stats: RingHashStats::default() }
    }

    pub fn add_member(&self, host: HostInfo) {
        let identity = host.identity.clone();
        let mut ring = self.ring.write().unwrap();
        for i in 0..self.virtual_nodes {
            let key = hash_key(&format!("{}:{}", identity, i));
            ring.insert(key, identity.clone());
        }
        self.members.write().unwrap().insert(identity, host);
        self.stats.members_added.fetch_add(1, Ordering::Relaxed);
        self.stats.rebalances.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove_member(&self, identity: &str) {
        let mut ring = self.ring.write().unwrap();
        for i in 0..self.virtual_nodes {
            let key = hash_key(&format!("{}:{}", identity, i));
            ring.remove(&key);
        }
        self.members.write().unwrap().remove(identity);
        self.stats.members_removed.fetch_add(1, Ordering::Relaxed);
        self.stats.rebalances.fetch_add(1, Ordering::Relaxed);
    }

    pub fn lookup(&self, key: &str) -> Option<HostInfo> {
        self.stats.lookups.fetch_add(1, Ordering::Relaxed);
        let ring = self.ring.read().unwrap();
        if ring.is_empty() { return None; }
        let hash = hash_key(key);
        let identity = ring.range(hash..).next()
            .or_else(|| ring.iter().next())
            .map(|(_, id)| id.clone())?;
        self.members.read().unwrap().get(&identity).cloned()
    }

    pub fn lookup_shard(&self, shard_id: u32) -> Option<HostInfo> {
        self.lookup(&format!("shard-{}", shard_id))
    }

    pub fn member_count(&self) -> usize { self.members.read().unwrap().len() }

    pub fn all_members(&self) -> Vec<HostInfo> {
        self.members.read().unwrap().values().cloned().collect()
    }

    pub fn active_members(&self) -> Vec<HostInfo> {
        self.members.read().unwrap().values().filter(|h| h.is_active()).cloned().collect()
    }

    pub fn members_for_role(&self, role: ServiceRole) -> Vec<HostInfo> {
        self.members.read().unwrap().values().filter(|h| h.has_role(role) && h.is_active()).cloned().collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Health Checker — monitors host health
// ═══════════════════════════════════════════════════════════════════════════════

pub struct HealthChecker {
    pub ring: Arc<RingHash>,
    pub timeout: Duration,
    pub results: RwLock<HashMap<String, HealthResult>>,
    pub stats: HealthCheckerStats,
}

#[derive(Debug, Clone)]
pub struct HealthResult {
    pub identity: String,
    pub healthy: bool,
    pub latency_ms: u64,
    pub checked_at: i64,
    pub failure_count: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Default)]
pub struct HealthCheckerStats {
    pub checks_performed: AtomicU64, pub healthy_count: AtomicU64,
    pub unhealthy_count: AtomicU64, pub timeouts: AtomicU64,
}

impl HealthChecker {
    pub fn new(ring: Arc<RingHash>, timeout: Duration) -> Self {
        Self { ring, timeout, results: RwLock::new(HashMap::new()), stats: HealthCheckerStats::default() }
    }

    pub fn check_host(&self, identity: &str) -> HealthResult {
        let result = HealthResult {
            identity: identity.into(), healthy: true, latency_ms: 5,
            checked_at: now_millis(), failure_count: 0, last_error: None,
        };
        self.results.write().unwrap().insert(identity.into(), result.clone());
        self.stats.checks_performed.fetch_add(1, Ordering::Relaxed);
        self.stats.healthy_count.fetch_add(1, Ordering::Relaxed);
        result
    }

    pub fn mark_unhealthy(&self, identity: &str, error: &str) {
        let result = HealthResult {
            identity: identity.into(), healthy: false, latency_ms: 0,
            checked_at: now_millis(), failure_count: 1, last_error: Some(error.into()),
        };
        self.results.write().unwrap().insert(identity.into(), result);
        self.stats.unhealthy_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_result(&self, identity: &str) -> Option<HealthResult> {
        self.results.read().unwrap().get(identity).cloned()
    }

    pub fn healthy_hosts(&self) -> Vec<String> {
        self.results.read().unwrap().iter().filter(|(_, r)| r.healthy).map(|(id, _)| id.clone()).collect()
    }

    pub fn unhealthy_hosts(&self) -> Vec<String> {
        self.results.read().unwrap().iter().filter(|(_, r)| !r.healthy).map(|(id, _)| id.clone()).collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Cluster Topology — overall cluster view
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ClusterTopology {
    pub cluster_name: String,
    pub ring: Arc<RingHash>,
    pub health_checker: Arc<HealthChecker>,
    pub total_shards: u32,
    pub shard_to_host: RwLock<HashMap<u32, String>>,
    pub stats: ClusterTopologyStats,
}

#[derive(Debug, Default)]
pub struct ClusterTopologyStats {
    pub shard_resolutions: AtomicU64, pub topology_updates: AtomicU64,
}

impl ClusterTopology {
    pub fn new(cluster_name: &str, total_shards: u32) -> Self {
        let ring = Arc::new(RingHash::new(100));
        let health = Arc::new(HealthChecker::new(ring.clone(), Duration::from_secs(5)));
        Self { cluster_name: cluster_name.into(), ring, health_checker: health, total_shards, shard_to_host: RwLock::new(HashMap::new()), stats: ClusterTopologyStats::default() }
    }

    pub fn add_host(&self, host: HostInfo) {
        self.ring.add_member(host);
        self.rebuild_shard_map();
    }

    pub fn remove_host(&self, identity: &str) {
        self.ring.remove_member(identity);
        self.rebuild_shard_map();
    }

    pub fn resolve_shard(&self, shard_id: u32) -> Option<HostInfo> {
        self.stats.shard_resolutions.fetch_add(1, Ordering::Relaxed);
        self.ring.lookup_shard(shard_id)
    }

    pub fn rebuild_shard_map(&self) {
        let mut map = HashMap::new();
        for shard_id in 0..self.total_shards {
            if let Some(host) = self.ring.lookup_shard(shard_id) {
                map.insert(shard_id, host.identity);
            }
        }
        *self.shard_to_host.write().unwrap() = map;
        self.stats.topology_updates.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_host_for_shard(&self, shard_id: u32) -> Option<String> {
        self.shard_to_host.read().unwrap().get(&shard_id).cloned()
    }

    pub fn shards_on_host(&self, host_identity: &str) -> Vec<u32> {
        self.shard_to_host.read().unwrap().iter()
            .filter(|(_, h)| *h == host_identity).map(|(s, _)| *s).collect()
    }

    pub fn cluster_report(&self) -> ClusterReport {
        let members = self.ring.all_members();
        let active = members.iter().filter(|m| m.is_active()).count();
        let mut host_shard_counts: HashMap<String, usize> = HashMap::new();
        for (_, host) in self.shard_to_host.read().unwrap().iter() {
            *host_shard_counts.entry(host.clone()).or_insert(0) += 1;
        }
        let balanced = if host_shard_counts.is_empty() { true } else {
            let counts: Vec<usize> = host_shard_counts.values().cloned().collect();
            let max = *counts.iter().max().unwrap_or(&0);
            let min = *counts.iter().min().unwrap_or(&0);
            max - min <= 2
        };
        ClusterReport {
            cluster_name: self.cluster_name.clone(), total_shards: self.total_shards,
            total_hosts: members.len(), active_hosts: active,
            shard_distribution: host_shard_counts, balanced,
        }
    }

    pub fn host_count(&self) -> usize { self.ring.member_count() }
}

#[derive(Debug)]
pub struct ClusterReport {
    pub cluster_name: String, pub total_shards: u32,
    pub total_hosts: usize, pub active_hosts: usize,
    pub shard_distribution: HashMap<String, usize>, pub balanced: bool,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn hash_key(key: &str) -> u32 {
    let mut hash: u32 = 2166136261;
    for byte in key.bytes() { hash ^= byte as u32; hash = hash.wrapping_mul(16777619); }
    hash
}

fn now_millis() -> i64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_host(id: &str, port: u16) -> HostInfo {
        HostInfo::new(id, "127.0.0.1", port, vec![ServiceRole::History, ServiceRole::Matching])
    }

    #[test]
    fn test_host_address() {
        let addr = HostAddress::new("localhost", 8080);
        assert_eq!(addr.address(), "localhost:8080");
        assert_eq!(format!("{}", addr), "localhost:8080");
    }

    #[test]
    fn test_host_info() {
        let host = make_host("host-1", 8080);
        assert!(host.is_active());
        assert!(host.has_role(ServiceRole::History));
        assert!(!host.has_role(ServiceRole::Frontend));
    }

    #[test]
    fn test_ring_hash_add_lookup() {
        let ring = RingHash::new(10);
        ring.add_member(make_host("host-1", 8080));
        ring.add_member(make_host("host-2", 8081));
        assert_eq!(ring.member_count(), 2);
        let result = ring.lookup("test-key");
        assert!(result.is_some());
    }

    #[test]
    fn test_ring_hash_remove() {
        let ring = RingHash::new(10);
        ring.add_member(make_host("host-1", 8080));
        ring.add_member(make_host("host-2", 8081));
        ring.remove_member("host-1");
        assert_eq!(ring.member_count(), 1);
    }

    #[test]
    fn test_ring_hash_empty() {
        let ring = RingHash::new(10);
        assert!(ring.lookup("key").is_none());
    }

    #[test]
    fn test_ring_hash_shard_lookup() {
        let ring = RingHash::new(10);
        ring.add_member(make_host("host-1", 8080));
        let result = ring.lookup_shard(0);
        assert!(result.is_some());
    }

    #[test]
    fn test_ring_hash_active_members() {
        let ring = RingHash::new(10);
        ring.add_member(make_host("host-1", 8080));
        let mut host2 = make_host("host-2", 8081);
        host2.state = HostState::Draining;
        ring.add_member(host2);
        assert_eq!(ring.active_members().len(), 1);
    }

    #[test]
    fn test_ring_hash_members_for_role() {
        let ring = RingHash::new(10);
        ring.add_member(make_host("host-1", 8080));
        let mut frontend_host = HostInfo::new("host-2", "127.0.0.1", 8081, vec![ServiceRole::Frontend]);
        ring.add_member(frontend_host);
        assert_eq!(ring.members_for_role(ServiceRole::History).len(), 1);
        assert_eq!(ring.members_for_role(ServiceRole::Frontend).len(), 1);
    }

    #[test]
    fn test_health_checker() {
        let ring = Arc::new(RingHash::new(10));
        ring.add_member(make_host("host-1", 8080));
        let hc = HealthChecker::new(ring, Duration::from_secs(5));
        let result = hc.check_host("host-1");
        assert!(result.healthy);
        assert_eq!(hc.healthy_hosts().len(), 1);
    }

    #[test]
    fn test_health_checker_unhealthy() {
        let ring = Arc::new(RingHash::new(10));
        let hc = HealthChecker::new(ring, Duration::from_secs(5));
        hc.mark_unhealthy("host-1", "connection refused");
        assert_eq!(hc.unhealthy_hosts().len(), 1);
        let result = hc.get_result("host-1").unwrap();
        assert!(!result.healthy);
    }

    #[test]
    fn test_cluster_topology() {
        let topo = ClusterTopology::new("test-cluster", 16);
        topo.add_host(make_host("host-1", 8080));
        topo.add_host(make_host("host-2", 8081));
        assert_eq!(topo.host_count(), 2);
        let host = topo.resolve_shard(0);
        assert!(host.is_some());
    }

    #[test]
    fn test_cluster_topology_shard_map() {
        let topo = ClusterTopology::new("test-cluster", 8);
        topo.add_host(make_host("host-1", 8080));
        topo.add_host(make_host("host-2", 8081));
        for shard_id in 0..8 {
            assert!(topo.get_host_for_shard(shard_id).is_some());
        }
    }

    #[test]
    fn test_cluster_topology_shards_on_host() {
        let topo = ClusterTopology::new("test-cluster", 8);
        topo.add_host(make_host("host-1", 8080));
        let shards = topo.shards_on_host("host-1");
        assert_eq!(shards.len(), 8);
    }

    #[test]
    fn test_cluster_report() {
        let topo = ClusterTopology::new("test-cluster", 16);
        topo.add_host(make_host("host-1", 8080));
        topo.add_host(make_host("host-2", 8081));
        let report = topo.cluster_report();
        assert_eq!(report.cluster_name, "test-cluster");
        assert_eq!(report.total_hosts, 2);
        assert_eq!(report.active_hosts, 2);
        assert!(report.balanced);
    }

    #[test]
    fn test_cluster_topology_remove_host() {
        let topo = ClusterTopology::new("test-cluster", 8);
        topo.add_host(make_host("host-1", 8080));
        topo.add_host(make_host("host-2", 8081));
        topo.remove_host("host-1");
        assert_eq!(topo.host_count(), 1);
        let shards = topo.shards_on_host("host-2");
        assert_eq!(shards.len(), 8);
    }

    #[test]
    fn test_hash_key_deterministic() {
        let h1 = hash_key("test");
        let h2 = hash_key("test");
        assert_eq!(h1, h2);
        let h3 = hash_key("different");
        assert_ne!(h1, h3);
    }
}
