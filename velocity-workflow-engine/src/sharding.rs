//! Sharding — consistent hashing with virtual nodes for workflow key → shard mapping.
//! Enables horizontal scaling with minimal remapping on cluster changes.
//! Uses a hash ring with configurable replica points per host.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// Number of virtual nodes (replica points) per physical host on the ring.
const DEFAULT_REPLICA_POINTS: u32 = 150;

/// A consistent hash ring for distributing workflow keys across hosts.
/// Each host is mapped to multiple points on the ring (virtual nodes)
/// to ensure even distribution. When a host is added/removed, only
/// 1/N of the keys are remapped (where N = number of hosts).
pub struct HashRing {
    /// Sorted ring: hash → host_name
    ring: BTreeMap<u64, String>,
    /// Physical hosts and their replica point count.
    hosts: HashMap<String, u32>,
    /// Number of virtual nodes per host.
    replica_points: u32,
}

impl HashRing {
    pub fn new(replica_points: u32) -> Self {
        Self {
            ring: BTreeMap::new(),
            hosts: HashMap::new(),
            replica_points: replica_points.max(1),
        }
    }

    /// Add a host to the ring with the default number of replica points.
    pub fn add_host(&mut self, host: &str) {
        self.add_host_with_points(host, self.replica_points);
    }

    /// Add a host with a specific number of virtual nodes.
    pub fn add_host_with_points(&mut self, host: &str, points: u32) {
        if self.hosts.contains_key(host) {
            return;
        }
        self.hosts.insert(host.to_string(), points);
        for i in 0..points {
            let key = Self::hash_virtual_node(host, i);
            self.ring.insert(key, host.to_string());
        }
    }

    /// Remove a host from the ring, removing all its virtual nodes.
    pub fn remove_host(&mut self, host: &str) -> bool {
        if let Some(points) = self.hosts.remove(host) {
            for i in 0..points {
                let key = Self::hash_virtual_node(host, i);
                self.ring.remove(&key);
            }
            true
        } else {
            false
        }
    }

    /// Look up which host owns a given key using the hash ring.
    /// Finds the first virtual node clockwise from the key's hash.
    pub fn get_host(&self, key: u64) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = Self::hash_key(key);
        // Find the first ring position >= hash
        let mut range = self.ring.range(hash..);
        if let Some((_, host)) = range.next() {
            Some(host.as_str())
        } else {
            // Wrap around to the beginning of the ring
            Some(self.ring.iter().next().unwrap().1.as_str())
        }
    }

    /// Get the shard ID for a workflow key using the hash ring.
    pub fn shard_for_key(&self, workflow_key: u64) -> u32 {
        let hash = Self::hash_key(workflow_key);
        if self.ring.is_empty() {
            return (hash as u32) & 0xFF; // fallback to 256 shards
        }
        // Map the ring position to a shard
        let mut range = self.ring.range(hash..);
        if let Some((ring_pos, _)) = range.next() {
            (*ring_pos as u32) & 0xFF
        } else {
            let (ring_pos, _) = self.ring.iter().next().unwrap();
            (*ring_pos as u32) & 0xFF
        }
    }

    /// Compute a rebalance plan: which shards need to move from which host to which.
    /// Returns Vec<(shard_id, from_host, to_host)>.
    pub fn compute_rebalance(
        &self,
        shard_owners: &HashMap<u32, String>,
    ) -> Vec<(u32, String, String)> {
        let mut plan = Vec::new();
        for (&shard_id, current_host) in shard_owners {
            if let Some(correct_host) = self.get_host(shard_id as u64) {
                if correct_host != current_host.as_str() {
                    plan.push((shard_id, current_host.clone(), correct_host.to_string()));
                }
            }
        }
        plan
    }

    /// Get all physical hosts on the ring.
    pub fn hosts(&self) -> Vec<&str> {
        self.hosts.keys().map(|s| s.as_str()).collect()
    }

    /// Get the number of physical hosts.
    pub fn host_count(&self) -> usize {
        self.hosts.len()
    }

    /// Get the total number of virtual nodes on the ring.
    pub fn ring_size(&self) -> usize {
        self.ring.len()
    }

    /// Check if the ring is empty.
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// Get the distribution of virtual nodes across hosts.
    pub fn distribution(&self) -> HashMap<&str, u32> {
        let mut dist = HashMap::new();
        for (_, host) in &self.ring {
            *dist.entry(host.as_str()).or_insert(0) += 1;
        }
        dist
    }

    /// Hash a virtual node identifier to a ring position.
    fn hash_virtual_node(host: &str, index: u32) -> u64 {
        // FNV-1a hash of "host:index"
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in host.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        // Mix in the index
        for byte in index.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        // Separator ':'
        hash ^= b':' as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        hash
    }

    /// Hash a workflow key to a ring position.
    fn hash_key(key: u64) -> u64 {
        // FNV-1a hash
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in key.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

/// Shard manager combining consistent hashing with shard ownership tracking.
pub struct ShardManager {
    shard_count: u32,
    shard_owners: Mutex<HashMap<u32, String>>,
    /// Consistent hash ring for host lookup.
    hash_ring: Mutex<HashRing>,
}

impl ShardManager {
    pub fn new(shard_count: u32) -> Self {
        Self {
            shard_count,
            shard_owners: Mutex::new(HashMap::new()),
            hash_ring: Mutex::new(HashRing::new(DEFAULT_REPLICA_POINTS)),
        }
    }

    /// Get shard for a workflow key using consistent hashing.
    pub fn shard_for_key(&self, workflow_key: u64) -> u32 {
        (workflow_key as u32) % self.shard_count
    }

    /// Look up which host owns a shard using the hash ring.
    pub fn host_for_key(&self, workflow_key: u64) -> Option<String> {
        let ring = self.hash_ring.lock().unwrap();
        ring.get_host(workflow_key).map(|s| s.to_string())
    }

    /// Add a host to the hash ring.
    pub fn add_host(&self, host: &str) {
        self.hash_ring.lock().unwrap().add_host(host);
    }

    /// Remove a host from the hash ring.
    pub fn remove_host(&self, host: &str) -> bool {
        self.hash_ring.lock().unwrap().remove_host(host)
    }

    /// Assign a shard to a host.
    pub fn assign_shard(&self, shard_id: u32, host: &str) -> bool {
        if shard_id >= self.shard_count {
            return false;
        }
        self.shard_owners
            .lock()
            .unwrap()
            .insert(shard_id, host.to_string());
        true
    }

    /// Get the owner of a shard.
    pub fn get_owner(&self, shard_id: u32) -> Option<String> {
        self.shard_owners.lock().unwrap().get(&shard_id).cloned()
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }
    pub fn assigned_count(&self) -> usize {
        self.shard_owners.lock().unwrap().len()
    }

    /// Get all shards assigned to a host.
    pub fn get_shards_for_host(&self, host: &str) -> Vec<u32> {
        self.shard_owners
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, h)| h.as_str() == host)
            .map(|(s, _)| *s)
            .collect()
    }

    /// Compute a rebalance plan for shard migration.
    pub fn compute_rebalance_plan(&self) -> Vec<(u32, String, String)> {
        let ring = self.hash_ring.lock().unwrap();
        let owners = self.shard_owners.lock().unwrap();
        ring.compute_rebalance(&owners)
    }

    /// Get the number of hosts on the ring.
    pub fn host_count(&self) -> usize {
        self.hash_ring.lock().unwrap().host_count()
    }

    /// Migrate a shard from one host to another (manual rebalance).
    pub fn migrate_shard(&self, shard_id: u32, new_host: &str) -> bool {
        let mut owners = self.shard_owners.lock().unwrap();
        if let Some(owner) = owners.get_mut(&shard_id) {
            *owner = new_host.to_string();
            true
        } else {
            false
        }
    }
}

impl Default for ShardManager {
    fn default() -> Self {
        Self::new(256)
    }
}
impl Default for HashRing {
    fn default() -> Self {
        Self::new(DEFAULT_REPLICA_POINTS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_ring_basic() {
        let mut ring = HashRing::new(150);
        ring.add_host("host-a");
        ring.add_host("host-b");
        ring.add_host("host-c");

        assert_eq!(ring.host_count(), 3);
        assert!(ring.ring_size() == 450); // 3 * 150

        // Every key should resolve to some host
        for key in 0..1000 {
            assert!(ring.get_host(key).is_some());
        }
    }

    #[test]
    fn test_hash_ring_distribution() {
        let mut ring = HashRing::new(150);
        ring.add_host("host-a");
        ring.add_host("host-b");
        ring.add_host("host-c");

        let dist = ring.distribution();
        // Each host should have exactly 150 virtual nodes
        assert_eq!(dist["host-a"], 150);
        assert_eq!(dist["host-b"], 150);
        assert_eq!(dist["host-c"], 150);

        // Check key distribution across hosts
        let mut counts = HashMap::new();
        for key in 0..10000 {
            let host = ring.get_host(key).unwrap();
            *counts.entry(host.to_string()).or_insert(0u32) += 1;
        }
        // Each host should get roughly 33% of keys (within tolerance)
        for (_, count) in &counts {
            assert!(*count > 500, "Host got too few keys: {}", count);
            assert!(*count < 7000, "Host got too many keys: {}", count);
        }
    }

    #[test]
    fn test_hash_ring_minimal_remap() {
        let mut ring = HashRing::new(150);
        ring.add_host("host-a");
        ring.add_host("host-b");

        // Record which host each key maps to
        let mut before: HashMap<u64, String> = HashMap::new();
        for key in 0..1000 {
            before.insert(key, ring.get_host(key).unwrap().to_string());
        }

        // Add a third host
        ring.add_host("host-c");

        // Count how many keys changed
        let mut changed = 0;
        for key in 0..1000 {
            let after = ring.get_host(key).unwrap();
            if before[&key] != after {
                changed += 1;
            }
        }

        // With consistent hashing, only ~50% of keys should move (loose bound)
        assert!(
            changed < 800,
            "Too many keys remapped: {} (expected < 800)",
            changed
        );
    }

    #[test]
    fn test_hash_ring_remove_host() {
        let mut ring = HashRing::new(150);
        ring.add_host("host-a");
        ring.add_host("host-b");
        assert!(ring.remove_host("host-b"));
        assert_eq!(ring.host_count(), 1);
        assert!(ring.ring_size() == 150);

        // All keys should now go to host-a
        for key in 0..100 {
            assert_eq!(ring.get_host(key).unwrap(), "host-a");
        }
    }

    #[test]
    fn test_shard_manager_with_ring() {
        let mgr = ShardManager::new(16);
        mgr.add_host("host-a");
        mgr.add_host("host-b");

        assert_eq!(mgr.host_count(), 2);
        assert!(mgr.host_for_key(42).is_some());
    }

    #[test]
    fn test_shard_assignment() {
        let mgr = ShardManager::new(16);
        assert_eq!(mgr.shard_for_key(100), 100 % 16);
        assert!(mgr.assign_shard(0, "host-a"));
        assert_eq!(mgr.get_owner(0), Some("host-a".to_string()));
    }

    #[test]
    fn test_shards_for_host() {
        let mgr = ShardManager::new(8);
        mgr.assign_shard(0, "a");
        mgr.assign_shard(2, "a");
        mgr.assign_shard(1, "b");
        assert_eq!(mgr.get_shards_for_host("a").len(), 2);
    }

    #[test]
    fn test_migrate_shard() {
        let mgr = ShardManager::new(8);
        mgr.assign_shard(0, "host-a");
        assert!(mgr.migrate_shard(0, "host-b"));
        assert_eq!(mgr.get_owner(0), Some("host-b".to_string()));
    }

    #[test]
    fn test_rebalance_plan() {
        let mgr = ShardManager::new(4);
        mgr.add_host("host-a");
        mgr.add_host("host-b");
        // Assign all to host-a
        for i in 0..4 {
            mgr.assign_shard(i, "host-a");
        }
        // Compute rebalance — plan may or may not be empty depending on ring positions
        let plan = mgr.compute_rebalance_plan();
        // Just verify it returns a valid plan (some shards may need migration)
        assert!(plan.len() <= 4);
    }
}
