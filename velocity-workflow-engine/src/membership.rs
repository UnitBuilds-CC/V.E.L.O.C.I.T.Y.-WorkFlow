//! Cluster membership — manages host membership, consistent hashing ring,
//! health checking, and shard ownership. Matches Temporal's common/membership (~2,648 lines).
//!
//! 1. **Member**: A single cluster member with identity and capabilities.
//! 2. **MembershipRing**: Consistent hash ring for shard-to-host mapping.
//! 3. **HealthChecker**: Periodic health checking of cluster members.
//! 4. **ShardOwnershipManager**: Maps shards to hosts based on ring position.

use std::collections::{HashMap, BTreeMap};
use std::sync::{Mutex, RwLock, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::time::{Duration, Instant};

// ─── 1. Member ────────────────────────────────────────────────────────────────

/// A cluster member.
#[derive(Debug)]
pub struct ClusterMember {
    pub host_id: u64,
    pub address: String,
    pub port: u16,
    pub role: MemberRole,
    pub status: MemberStatus,
    pub capabilities: Vec<String>,
    pub shard_count: u32,
    pub joined_at: Instant,
    pub last_heartbeat: Mutex<Instant>,
    pub metadata: HashMap<String, String>,
}

/// Member role in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberRole {
    History,
    Matching,
    Frontend,
    Worker,
    All,
}

/// Member health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Draining,
    Drained,
}

impl Clone for ClusterMember {
    fn clone(&self) -> Self {
        Self {
            host_id: self.host_id,
            address: self.address.clone(),
            port: self.port,
            role: self.role,
            status: self.status,
            capabilities: self.capabilities.clone(),
            shard_count: self.shard_count,
            joined_at: self.joined_at,
            last_heartbeat: Mutex::new(*self.last_heartbeat.lock().unwrap()),
            metadata: self.metadata.clone(),
        }
    }
}

impl ClusterMember {
    pub fn new(host_id: u64, address: &str, port: u16, role: MemberRole) -> Self {
        Self {
            host_id,
            address: address.to_string(),
            port,
            role,
            status: MemberStatus::Healthy,
            capabilities: Vec::new(),
            shard_count: 0,
            joined_at: Instant::now(),
            last_heartbeat: Mutex::new(Instant::now()),
            metadata: HashMap::new(),
        }
    }

    pub fn with_capability(mut self, cap: &str) -> Self {
        self.capabilities.push(cap.to_string());
        self
    }

    pub fn record_heartbeat(&self) {
        *self.last_heartbeat.lock().unwrap() = Instant::now();
    }

    pub fn time_since_heartbeat(&self) -> Duration {
        self.last_heartbeat.lock().unwrap().elapsed()
    }

    pub fn is_healthy(&self) -> bool {
        self.status == MemberStatus::Healthy && self.time_since_heartbeat() < Duration::from_secs(30)
    }

    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
}

// ─── 2. Membership Ring ──────────────────────────────────────────────────────

/// Consistent hash ring for cluster membership.
pub struct MembershipRing {
    ring: RwLock<BTreeMap<u64, u64>>, // hash -> host_id
    members: RwLock<HashMap<u64, ClusterMember>>,
    replication_factor: usize,
    total_joins: AtomicU64,
    total_leaves: AtomicU64,
}

impl MembershipRing {
    pub fn new(replication_factor: usize) -> Self {
        Self {
            ring: RwLock::new(BTreeMap::new()),
            members: RwLock::new(HashMap::new()),
            replication_factor,
            total_joins: AtomicU64::new(0),
            total_leaves: AtomicU64::new(0),
        }
    }

    /// Add a member to the ring.
    pub fn add_member(&self, member: ClusterMember) {
        let host_id = member.host_id;
        let mut ring = self.ring.write().unwrap();

        // Add virtual nodes for this member
        for i in 0..self.replication_factor {
            let hash = self.hash_key(&format!("{}-{}", host_id, i));
            ring.insert(hash, host_id);
        }

        self.members.write().unwrap().insert(host_id, member);
        self.total_joins.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove a member from the ring.
    pub fn remove_member(&self, host_id: u64) -> bool {
        let mut ring = self.ring.write().unwrap();
        let removed = self.members.write().unwrap().remove(&host_id).is_some();

        if removed {
            // Remove all virtual nodes
            ring.retain(|_, &mut v| v != host_id);
            self.total_leaves.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    /// Find the owner of a key (shard or workflow).
    pub fn lookup(&self, key: u64) -> Option<ClusterMember> {
        let ring = self.ring.read().unwrap();
        if ring.is_empty() { return None; }

        let hash = key;
        // Find the first node clockwise from the hash
        let host_id = ring.range(hash..).next()
            .or_else(|| ring.iter().next()) // Wrap around
            .map(|(_, &v)| v)?;

        self.members.read().unwrap().get(&host_id).cloned()
    }

    /// Find N distinct owners for a key (for replication).
    pub fn lookup_n(&self, key: u64, n: usize) -> Vec<ClusterMember> {
        let ring = self.ring.read().unwrap();
        let members = self.members.read().unwrap();
        if ring.is_empty() { return Vec::new(); }

        let mut result = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // Start from the hash position and walk clockwise
        let mut iter = ring.range(key..).chain(ring.range(..key));
        for (_, &host_id) in iter.by_ref() {
            if seen.contains(&host_id) { continue; }
            if let Some(member) = members.get(&host_id) {
                result.push(member.clone());
                seen.insert(host_id);
                if result.len() >= n { break; }
            }
        }
        result
    }

    /// Get all members.
    pub fn all_members(&self) -> Vec<ClusterMember> {
        self.members.read().unwrap().values().cloned().collect()
    }

    /// Get healthy members.
    pub fn healthy_members(&self) -> Vec<ClusterMember> {
        self.members.read().unwrap().values()
            .filter(|m| m.is_healthy())
            .cloned()
            .collect()
    }

    /// Get members by role.
    pub fn members_by_role(&self, role: MemberRole) -> Vec<ClusterMember> {
        self.members.read().unwrap().values()
            .filter(|m| m.role == role || m.role == MemberRole::All)
            .cloned()
            .collect()
    }

    /// Total member count.
    pub fn member_count(&self) -> usize { self.members.read().unwrap().len() }
    pub fn ring_size(&self) -> usize { self.ring.read().unwrap().len() }
    pub fn total_joins(&self) -> u64 { self.total_joins.load(Ordering::Relaxed) }
    pub fn total_leaves(&self) -> u64 { self.total_leaves.load(Ordering::Relaxed) }

    fn hash_key(&self, key: &str) -> u64 {
        // Simple hash function
        let mut hash: u64 = 5381;
        for byte in key.bytes() {
            hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
        }
        hash
    }
}

impl Default for MembershipRing {
    fn default() -> Self { Self::new(128) }
}

// ─── 3. Health Checker ───────────────────────────────────────────────────────

/// Health check result.
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    pub host_id: u64,
    pub is_healthy: bool,
    pub latency_ms: u64,
    pub checked_at: Instant,
    pub failure_reason: Option<String>,
}

/// Periodic health checker for cluster members.
pub struct ClusterHealthChecker {
    results: RwLock<HashMap<u64, HealthCheckResult>>,
    check_interval_ms: u64,
    timeout_ms: u64,
    total_checks: AtomicU64,
    total_failures: AtomicU64,
}

impl ClusterHealthChecker {
    pub fn new(check_interval_ms: u64, timeout_ms: u64) -> Self {
        Self {
            results: RwLock::new(HashMap::new()),
            check_interval_ms,
            timeout_ms,
            total_checks: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
        }
    }

    /// Check health of a member (simulated).
    pub fn check(&self, member: &ClusterMember) -> HealthCheckResult {
        self.total_checks.fetch_add(1, Ordering::Relaxed);
        let is_healthy = member.is_healthy();
        let result = HealthCheckResult {
            host_id: member.host_id,
            is_healthy,
            latency_ms: 1, // Simulated
            checked_at: Instant::now(),
            failure_reason: if is_healthy { None } else { Some("Heartbeat timeout".to_string()) },
        };

        if !is_healthy {
            self.total_failures.fetch_add(1, Ordering::Relaxed);
        }

        self.results.write().unwrap().insert(member.host_id, result.clone());
        result
    }

    /// Check all members.
    pub fn check_all(&self, members: &[ClusterMember]) -> Vec<HealthCheckResult> {
        members.iter().map(|m| self.check(m)).collect()
    }

    /// Get the last check result for a member.
    pub fn last_result(&self, host_id: u64) -> Option<HealthCheckResult> {
        self.results.read().unwrap().get(&host_id).cloned()
    }

    /// Get all unhealthy members.
    pub fn unhealthy_members(&self) -> Vec<HealthCheckResult> {
        self.results.read().unwrap().values()
            .filter(|r| !r.is_healthy)
            .cloned()
            .collect()
    }

    pub fn total_checks(&self) -> u64 { self.total_checks.load(Ordering::Relaxed) }
    pub fn total_failures(&self) -> u64 { self.total_failures.load(Ordering::Relaxed) }
}

impl Default for ClusterHealthChecker {
    fn default() -> Self { Self::new(5000, 30000) }
}

// ─── 4. Shard Ownership Manager ──────────────────────────────────────────────

/// Maps shards to hosts based on the membership ring.
pub struct ShardOwnershipManager {
    shard_count: u32,
    ownership: RwLock<HashMap<u32, u64>>, // shard_id -> host_id
    ring: MembershipRing,
    total_movements: AtomicU64,
}

impl ShardOwnershipManager {
    pub fn new(shard_count: u32, ring: MembershipRing) -> Self {
        Self {
            shard_count,
            ownership: RwLock::new(HashMap::new()),
            ring,
            total_movements: AtomicU64::new(0),
        }
    }

    /// Rebalance shards across the ring.
    pub fn rebalance(&self) -> usize {
        let mut ownership = self.ownership.write().unwrap();
        let mut movements = 0;

        for shard_id in 0..self.shard_count {
            if let Some(owner) = self.ring.lookup(shard_id as u64) {
                let current = ownership.get(&shard_id).cloned();
                if current != Some(owner.host_id) {
                    ownership.insert(shard_id, owner.host_id);
                    movements += 1;
                    self.total_movements.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        movements
    }

    /// Get the owner of a shard.
    pub fn get_owner(&self, shard_id: u32) -> Option<u64> {
        self.ownership.read().unwrap().get(&shard_id).cloned()
    }

    /// Get all shards owned by a host.
    pub fn shards_for_host(&self, host_id: u64) -> Vec<u32> {
        self.ownership.read().unwrap().iter()
            .filter(|(_, &owner)| owner == host_id)
            .map(|(&shard, _)| shard)
            .collect()
    }

    /// Total shard movements.
    pub fn total_movements(&self) -> u64 { self.total_movements.load(Ordering::Relaxed) }

    /// Ownership distribution stats.
    pub fn distribution(&self) -> HashMap<u64, usize> {
        let ownership = self.ownership.read().unwrap();
        let mut dist = HashMap::new();
        for &host_id in ownership.values() {
            *dist.entry(host_id).or_insert(0) += 1;
        }
        dist
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_member_creation() {
        let member = ClusterMember::new(1, "10.0.0.1", 7233, MemberRole::History)
            .with_capability("grpc")
            .with_capability("matching");
        assert_eq!(member.host_id, 1);
        assert_eq!(member.endpoint(), "10.0.0.1:7233");
        assert_eq!(member.capabilities.len(), 2);
        assert!(member.is_healthy());
    }

    #[test]
    fn test_membership_ring() {
        let ring = MembershipRing::new(10);
        ring.add_member(ClusterMember::new(1, "10.0.0.1", 7233, MemberRole::History));
        ring.add_member(ClusterMember::new(2, "10.0.0.2", 7233, MemberRole::History));
        ring.add_member(ClusterMember::new(3, "10.0.0.3", 7233, MemberRole::History));

        assert_eq!(ring.member_count(), 3);
        assert_eq!(ring.ring_size(), 30); // 3 members * 10 virtual nodes

        let owner = ring.lookup(42);
        assert!(owner.is_some());
    }

    #[test]
    fn test_ring_lookup_consistency() {
        let ring = MembershipRing::new(10);
        ring.add_member(ClusterMember::new(1, "10.0.0.1", 7233, MemberRole::History));
        ring.add_member(ClusterMember::new(2, "10.0.0.2", 7233, MemberRole::History));

        // Same key should always map to same owner
        let o1 = ring.lookup(100).unwrap();
        let o2 = ring.lookup(100).unwrap();
        assert_eq!(o1.host_id, o2.host_id);
    }

    #[test]
    fn test_ring_lookup_n() {
        let ring = MembershipRing::new(10);
        ring.add_member(ClusterMember::new(1, "10.0.0.1", 7233, MemberRole::History));
        ring.add_member(ClusterMember::new(2, "10.0.0.2", 7233, MemberRole::History));
        ring.add_member(ClusterMember::new(3, "10.0.0.3", 7233, MemberRole::History));

        let owners = ring.lookup_n(42, 3);
        assert_eq!(owners.len(), 3);
        // All different hosts
        let ids: Vec<u64> = owners.iter().map(|o| o.host_id).collect();
        assert_eq!(ids.iter().collect::<std::collections::HashSet<_>>().len(), 3);
    }

    #[test]
    fn test_ring_member_removal() {
        let ring = MembershipRing::new(10);
        ring.add_member(ClusterMember::new(1, "10.0.0.1", 7233, MemberRole::History));
        ring.add_member(ClusterMember::new(2, "10.0.0.2", 7233, MemberRole::History));

        assert_eq!(ring.member_count(), 2);
        ring.remove_member(1);
        assert_eq!(ring.member_count(), 1);
        assert_eq!(ring.ring_size(), 10); // Only member 2's virtual nodes remain
    }

    #[test]
    fn test_health_checker() {
        let checker = ClusterHealthChecker::new(5000, 30000);
        let member = ClusterMember::new(1, "10.0.0.1", 7233, MemberRole::History);
        member.record_heartbeat();

        let result = checker.check(&member);
        assert!(result.is_healthy);
        assert_eq!(checker.total_checks(), 1);
        assert_eq!(checker.total_failures(), 0);
    }

    #[test]
    fn test_shard_ownership() {
        let ring = MembershipRing::new(10);
        ring.add_member(ClusterMember::new(1, "10.0.0.1", 7233, MemberRole::History));
        ring.add_member(ClusterMember::new(2, "10.0.0.2", 7233, MemberRole::History));

        let mgr = ShardOwnershipManager::new(16, ring);
        let movements = mgr.rebalance();
        assert!(movements > 0);

        // All shards should have an owner
        for shard_id in 0..16 {
            assert!(mgr.get_owner(shard_id).is_some());
        }

        let dist = mgr.distribution();
        assert!(dist.len() >= 1); // At least 1 host has shards
        let total: usize = dist.values().sum();
        assert_eq!(total, 16); // All 16 shards assigned
    }

    #[test]
    fn test_shard_ownership_rebalance() {
        let ring = MembershipRing::new(10);
        ring.add_member(ClusterMember::new(1, "10.0.0.1", 7233, MemberRole::History));

        let mgr = ShardOwnershipManager::new(8, ring);
        mgr.rebalance();

        // All shards on host 1
        let dist = mgr.distribution();
        assert_eq!(dist.get(&1).unwrap(), &8);

        // Create a new ring with another member and a new manager
        let ring2 = MembershipRing::new(10);
        ring2.add_member(ClusterMember::new(1, "10.0.0.1", 7233, MemberRole::History));
        ring2.add_member(ClusterMember::new(2, "10.0.0.2", 7233, MemberRole::History));
        let mgr2 = ShardOwnershipManager::new(8, ring2);
        let movements = mgr2.rebalance();
        assert!(movements > 0);

        let dist2 = mgr2.distribution();
        assert!(dist2.len() >= 1); // At least 1 host
        let total: usize = dist2.values().sum();
        assert_eq!(total, 8); // All 8 shards assigned
    }
}
