//! Distributed locking implementation matching Temporal's locking subsystem.
//!
//! Covers: shard ownership locks, leader election, fencing tokens,
//! distributed mutex, read-write locks, and lock manager.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    RwLock,
};
use std::time::{Duration, Instant};

// ═══════════════════════════════════════════════════════════════════════════════
// Distributed Lock
// ═══════════════════════════════════════════════════════════════════════════════

pub struct DistributedLock {
    pub resource_id: String,
    pub owner: String,
    pub fencing_token: u64,
    pub acquired_at: Instant,
    pub ttl: Duration,
    pub renewed_count: u64,
}

impl Clone for DistributedLock {
    fn clone(&self) -> Self {
        Self {
            resource_id: self.resource_id.clone(),
            owner: self.owner.clone(),
            fencing_token: self.fencing_token,
            acquired_at: self.acquired_at,
            ttl: self.ttl,
            renewed_count: self.renewed_count,
        }
    }
}

impl DistributedLock {
    pub fn is_expired(&self) -> bool {
        self.acquired_at.elapsed() > self.ttl
    }

    pub fn remaining_ttl(&self) -> Duration {
        self.ttl
            .checked_sub(self.acquired_at.elapsed())
            .unwrap_or(Duration::ZERO)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lock Manager
// ═══════════════════════════════════════════════════════════════════════════════

pub struct LockManager {
    locks: RwLock<HashMap<String, DistributedLock>>,
    next_token: AtomicU64,
    stats: LockManagerStats,
}

#[derive(Debug, Default)]
pub struct LockManagerStats {
    pub locks_acquired: AtomicU64,
    pub locks_released: AtomicU64,
    pub lock_contentions: AtomicU64,
    pub lock_expirations: AtomicU64,
    pub lock_renewals: AtomicU64,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            next_token: AtomicU64::new(1),
            stats: LockManagerStats::default(),
        }
    }

    pub fn acquire(
        &self,
        resource_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<DistributedLock, LockError> {
        let mut locks = self.locks.write().unwrap();

        // Check if lock exists and is not expired
        if let Some(existing) = locks.get(resource_id) {
            if !existing.is_expired() && existing.owner != owner {
                self.stats.lock_contentions.fetch_add(1, Ordering::Relaxed);
                return Err(LockError::AlreadyLocked(resource_id.to_string()));
            }
        }

        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let lock = DistributedLock {
            resource_id: resource_id.to_string(),
            owner: owner.to_string(),
            fencing_token: token,
            acquired_at: Instant::now(),
            ttl,
            renewed_count: 0,
        };

        locks.insert(resource_id.to_string(), lock.clone());
        self.stats.locks_acquired.fetch_add(1, Ordering::Relaxed);

        Ok(lock)
    }

    pub fn release(&self, resource_id: &str, owner: &str) -> Result<(), LockError> {
        let mut locks = self.locks.write().unwrap();

        let lock = locks
            .get(resource_id)
            .ok_or_else(|| LockError::NotFound(resource_id.to_string()))?;

        if lock.owner != owner {
            return Err(LockError::NotOwner(resource_id.to_string()));
        }

        locks.remove(resource_id);
        self.stats.locks_released.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn renew(
        &self,
        resource_id: &str,
        owner: &str,
        new_ttl: Duration,
    ) -> Result<u64, LockError> {
        let mut locks = self.locks.write().unwrap();

        let lock = locks
            .get_mut(resource_id)
            .ok_or_else(|| LockError::NotFound(resource_id.to_string()))?;

        if lock.owner != owner {
            return Err(LockError::NotOwner(resource_id.to_string()));
        }

        lock.acquired_at = Instant::now();
        lock.ttl = new_ttl;
        lock.renewed_count += 1;
        self.stats.lock_renewals.fetch_add(1, Ordering::Relaxed);

        Ok(lock.fencing_token)
    }

    pub fn is_locked(&self, resource_id: &str) -> bool {
        let locks = self.locks.read().unwrap();
        locks
            .get(resource_id)
            .map(|l| !l.is_expired())
            .unwrap_or(false)
    }

    pub fn get_owner(&self, resource_id: &str) -> Option<String> {
        let locks = self.locks.read().unwrap();
        locks
            .get(resource_id)
            .filter(|l| !l.is_expired())
            .map(|l| l.owner.clone())
    }

    pub fn cleanup_expired(&self) -> usize {
        let mut locks = self.locks.write().unwrap();
        let before = locks.len();
        locks.retain(|_, l| !l.is_expired());
        let expired = before - locks.len();
        if expired > 0 {
            self.stats
                .lock_expirations
                .fetch_add(expired as u64, Ordering::Relaxed);
        }
        expired
    }

    pub fn active_lock_count(&self) -> usize {
        self.locks
            .read()
            .unwrap()
            .values()
            .filter(|l| !l.is_expired())
            .count()
    }

    pub fn stats(&self) -> &LockManagerStats {
        &self.stats
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Shard Ownership
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ShardOwnershipManager {
    ownership: RwLock<HashMap<i32, ShardOwnershipInfo>>,
    lock_manager: LockManager,
    member_id: String,
}

#[derive(Debug, Clone)]
pub struct ShardOwnershipInfo {
    pub shard_id: i32,
    pub owner: String,
    pub fencing_token: u64,
    pub acquired_at: Instant,
    pub range_id: i64,
}

impl ShardOwnershipManager {
    pub fn new(member_id: &str) -> Self {
        Self {
            ownership: RwLock::new(HashMap::new()),
            lock_manager: LockManager::new(),
            member_id: member_id.to_string(),
        }
    }

    pub fn acquire_shard(
        &self,
        shard_id: i32,
        range_id: i64,
    ) -> Result<ShardOwnershipInfo, LockError> {
        let resource = format!("shard-{}", shard_id);
        let lock =
            self.lock_manager
                .acquire(&resource, &self.member_id, Duration::from_secs(60))?;

        let info = ShardOwnershipInfo {
            shard_id,
            owner: self.member_id.clone(),
            fencing_token: lock.fencing_token,
            acquired_at: Instant::now(),
            range_id,
        };

        self.ownership
            .write()
            .unwrap()
            .insert(shard_id, info.clone());
        Ok(info)
    }

    pub fn release_shard(&self, shard_id: i32) -> Result<(), LockError> {
        let resource = format!("shard-{}", shard_id);
        self.lock_manager.release(&resource, &self.member_id)?;
        self.ownership.write().unwrap().remove(&shard_id);
        Ok(())
    }

    pub fn assert_ownership(&self, shard_id: i32, fencing_token: u64) -> Result<(), LockError> {
        let ownership = self.ownership.read().unwrap();
        let info = ownership
            .get(&shard_id)
            .ok_or_else(|| LockError::NotOwner(format!("shard-{}", shard_id)))?;

        if info.fencing_token != fencing_token {
            return Err(LockError::StaleFencingToken(shard_id));
        }
        Ok(())
    }

    pub fn owned_shards(&self) -> Vec<i32> {
        self.ownership.read().unwrap().keys().cloned().collect()
    }

    pub fn get_ownership(&self, shard_id: i32) -> Option<ShardOwnershipInfo> {
        self.ownership.read().unwrap().get(&shard_id).cloned()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Leader Election
// ═══════════════════════════════════════════════════════════════════════════════

pub struct LeaderElection {
    pub election_id: String,
    pub leader_id: RwLock<Option<String>>,
    pub term: AtomicU64,
    pub lease_duration: Duration,
    pub last_heartbeat: RwLock<Option<Instant>>,
}

impl LeaderElection {
    pub fn new(election_id: &str, lease_duration: Duration) -> Self {
        Self {
            election_id: election_id.to_string(),
            leader_id: RwLock::new(None),
            term: AtomicU64::new(0),
            lease_duration,
            last_heartbeat: RwLock::new(None),
        }
    }

    pub fn try_become_leader(&self, candidate_id: &str) -> Result<u64, LockError> {
        let mut leader = self.leader_id.write().unwrap();

        // Check if current leader's lease has expired
        if let Some(_current_leader) = leader.as_ref() {
            let heartbeat = self.last_heartbeat.read().unwrap();
            if let Some(last) = *heartbeat {
                if last.elapsed() < self.lease_duration {
                    return Err(LockError::AlreadyLocked(self.election_id.clone()));
                }
            }
        }

        let new_term = self.term.fetch_add(1, Ordering::Relaxed) + 1;
        *leader = Some(candidate_id.to_string());
        *self.last_heartbeat.write().unwrap() = Some(Instant::now());

        Ok(new_term)
    }

    pub fn heartbeat(&self, leader_id: &str) -> Result<(), LockError> {
        let leader = self.leader_id.read().unwrap();
        if leader.as_deref() != Some(leader_id) {
            return Err(LockError::NotLeader(self.election_id.clone()));
        }
        *self.last_heartbeat.write().unwrap() = Some(Instant::now());
        Ok(())
    }

    pub fn resign(&self, leader_id: &str) -> Result<(), LockError> {
        let mut leader = self.leader_id.write().unwrap();
        if leader.as_deref() != Some(leader_id) {
            return Err(LockError::NotLeader(self.election_id.clone()));
        }
        *leader = None;
        Ok(())
    }

    pub fn get_leader(&self) -> Option<String> {
        self.leader_id.read().unwrap().clone()
    }

    pub fn current_term(&self) -> u64 {
        self.term.load(Ordering::Relaxed)
    }

    pub fn is_leader(&self, candidate_id: &str) -> bool {
        self.leader_id.read().unwrap().as_deref() == Some(candidate_id)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Error Types
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum LockError {
    AlreadyLocked(String),
    NotFound(String),
    NotOwner(String),
    StaleFencingToken(i32),
    NotLeader(String),
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_acquire_release() {
        let mgr = LockManager::new();
        let lock = mgr
            .acquire("resource-1", "owner-1", Duration::from_secs(60))
            .unwrap();
        assert_eq!(lock.owner, "owner-1");
        assert!(lock.fencing_token > 0);

        assert!(mgr.is_locked("resource-1"));
        assert_eq!(mgr.active_lock_count(), 1);

        mgr.release("resource-1", "owner-1").unwrap();
        assert!(!mgr.is_locked("resource-1"));
    }

    #[test]
    fn test_lock_contention() {
        let mgr = LockManager::new();
        mgr.acquire("resource-1", "owner-1", Duration::from_secs(60))
            .unwrap();
        assert!(mgr
            .acquire("resource-1", "owner-2", Duration::from_secs(60))
            .is_err());
    }

    #[test]
    fn test_lock_renew() {
        let mgr = LockManager::new();
        mgr.acquire("resource-1", "owner-1", Duration::from_secs(10))
            .unwrap();
        let token = mgr
            .renew("resource-1", "owner-1", Duration::from_secs(60))
            .unwrap();
        assert!(token > 0);
        assert_eq!(mgr.stats().lock_renewals.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_lock_wrong_owner() {
        let mgr = LockManager::new();
        mgr.acquire("resource-1", "owner-1", Duration::from_secs(60))
            .unwrap();
        assert!(mgr.release("resource-1", "owner-2").is_err());
    }

    #[test]
    fn test_shard_ownership() {
        let mgr = ShardOwnershipManager::new("member-1");
        let info = mgr.acquire_shard(1, 100).unwrap();
        assert_eq!(info.shard_id, 1);
        assert_eq!(info.range_id, 100);

        assert!(mgr.assert_ownership(1, info.fencing_token).is_ok());
        assert_eq!(mgr.owned_shards().len(), 1);

        mgr.release_shard(1).unwrap();
        assert!(mgr.owned_shards().is_empty());
    }

    #[test]
    fn test_leader_election() {
        let election = LeaderElection::new("election-1", Duration::from_secs(30));

        let term = election.try_become_leader("candidate-1").unwrap();
        assert_eq!(term, 1);
        assert!(election.is_leader("candidate-1"));
        assert_eq!(election.get_leader(), Some("candidate-1".to_string()));

        // Another candidate can't become leader
        assert!(election.try_become_leader("candidate-2").is_err());

        // Heartbeat
        election.heartbeat("candidate-1").unwrap();

        // Resign
        election.resign("candidate-1").unwrap();
        assert!(election.get_leader().is_none());

        // Now candidate-2 can become leader
        let term2 = election.try_become_leader("candidate-2").unwrap();
        assert_eq!(term2, 2);
    }

    #[test]
    fn test_lock_stats() {
        let mgr = LockManager::new();
        mgr.acquire("r1", "o1", Duration::from_secs(60)).unwrap();
        mgr.acquire("r2", "o1", Duration::from_secs(60)).unwrap();
        assert_eq!(mgr.stats().locks_acquired.load(Ordering::Relaxed), 2);

        mgr.release("r1", "o1").unwrap();
        assert_eq!(mgr.stats().locks_released.load(Ordering::Relaxed), 1);
    }
}
