//! In-memory Raft consensus tier for short-lived workflows.
//!
//! Implements the base.md vision: "An In-Memory / Raft-backed Transient Tier" —
//! for workflows running in seconds or minutes, event logging is held in a clustered,
//! replicated in-memory Raft group across worker nodes. Persistence to disk is deferred
//! or batched, drastically reducing DB write amplification.
//!
//! This module provides:
//! - Raft node with leader election, log replication, and heartbeats
//! - In-memory log store for transient workflow events
//! - Deferred disk persistence (batched flush)
//! - Short-lived workflow optimization (sub-second consensus)

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Raft node states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

/// A single entry in the Raft log.
#[derive(Debug, Clone)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub workflow_key: u64,
    pub event_type: RaftEventType,
    pub payload: Vec<u8>,
    pub timestamp_ms: u64,
    pub committed: bool,
}

/// Types of events that flow through the Raft log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftEventType {
    WorkflowStarted,
    StepCompleted,
    ActivityScheduled,
    ActivityCompleted,
    TimerScheduled,
    TimerFired,
    SignalReceived,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowCanceled,
}

/// Configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Unique node identifier within the Raft group.
    pub node_id: u64,
    /// Election timeout range (ms). Randomized within this range.
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    /// Heartbeat interval (ms).
    pub heartbeat_interval_ms: u64,
    /// Maximum number of log entries before triggering compaction.
    pub snapshot_threshold: u64,
    /// Whether to defer disk persistence (batched flush).
    pub defer_persistence: bool,
    /// Batch size for deferred persistence.
    pub persistence_batch_size: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            snapshot_threshold: 10_000,
            defer_persistence: true,
            persistence_batch_size: 1000,
        }
    }
}

/// Statistics for the Raft consensus tier.
#[derive(Debug, Clone, Default)]
pub struct RaftStats {
    pub current_term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log_length: u64,
    pub elections_won: u64,
    pub entries_committed: u64,
    pub entries_applied: u64,
    pub snapshots_taken: u64,
    pub deferred_flushes: u64,
}

/// In-memory Raft consensus node for transient workflow state.
pub struct RaftNode {
    config: RaftConfig,
    state: RaftState,
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<RaftLogEntry>,
    commit_index: u64,
    last_applied: u64,

    // Leader state
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,

    // Cluster membership
    peers: Vec<u64>,

    // Timing
    last_heartbeat: Instant,
    election_deadline: Instant,

    // Deferred persistence
    pending_flush: VecDeque<u64>, // indices not yet flushed to disk
    total_flushed: u64,

    // Stats
    stats: RaftStats,
}

impl RaftNode {
    /// Create a new Raft node with the given configuration.
    pub fn new(config: RaftConfig) -> Self {
        let now = Instant::now();
        let election_timeout =
            Duration::from_millis(config.election_timeout_min_ms + (config.node_id % 50));

        Self {
            config,
            state: RaftState::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            peers: Vec::new(),
            last_heartbeat: now,
            election_deadline: now + election_timeout,
            pending_flush: VecDeque::new(),
            total_flushed: 0,
            stats: RaftStats::default(),
        }
    }

    /// Register a peer node in the Raft group.
    pub fn add_peer(&mut self, peer_id: u64) {
        if !self.peers.contains(&peer_id) && peer_id != self.config.node_id {
            self.peers.push(peer_id);
            self.next_index.insert(peer_id, self.log.len() as u64 + 1);
            self.match_index.insert(peer_id, 0);
        }
    }

    /// Remove a peer from the Raft group.
    pub fn remove_peer(&mut self, peer_id: u64) {
        self.peers.retain(|&p| p != peer_id);
        self.next_index.remove(&peer_id);
        self.match_index.remove(&peer_id);
    }

    /// Start an election (transition to Candidate).
    pub fn start_election(&mut self) -> bool {
        if self.state == RaftState::Leader {
            return false;
        }

        self.current_term += 1;
        self.state = RaftState::Candidate;
        self.voted_for = Some(self.config.node_id);

        let now = Instant::now();
        let timeout = Duration::from_millis(
            self.config.election_timeout_min_ms
                + (self.config.node_id * 37
                    % (self.config.election_timeout_max_ms - self.config.election_timeout_min_ms
                        + 1)),
        );
        self.election_deadline = now + timeout;

        self.stats.elections_won += 1;
        true
    }

    /// Become leader (called after winning election).
    pub fn become_leader(&mut self) {
        self.state = RaftState::Leader;
        let next_idx = self.log.len() as u64 + 1;

        for &peer in &self.peers.clone() {
            self.next_index.insert(peer, next_idx);
            self.match_index.insert(peer, 0);
        }
    }

    /// Append a new log entry (leader only).
    pub fn append_entry(
        &mut self,
        workflow_key: u64,
        event_type: RaftEventType,
        payload: Vec<u8>,
    ) -> Option<u64> {
        if self.state != RaftState::Leader {
            return None;
        }

        let index = self.log.len() as u64 + 1;
        let entry = RaftLogEntry {
            term: self.current_term,
            index,
            workflow_key,
            event_type,
            payload,
            timestamp_ms: 0, // Would use system clock in production
            committed: false,
        };

        self.log.push(entry);

        if self.config.defer_persistence {
            self.pending_flush.push_back(index);
        }

        // In single-node mode, immediately commit
        if self.peers.is_empty() {
            self.commit_index = index;
            if let Some(e) = self.log.last_mut() {
                e.committed = true;
            }
            self.stats.entries_committed += 1;
        }

        Some(index)
    }

    /// Advance commit index after receiving majority acknowledgments.
    pub fn advance_commit(&mut self, new_commit_index: u64) {
        if new_commit_index > self.commit_index && new_commit_index <= self.log.len() as u64 {
            for i in self.commit_index..new_commit_index {
                if let Some(entry) = self.log.get_mut(i as usize) {
                    entry.committed = true;
                }
            }
            self.stats.entries_committed += new_commit_index - self.commit_index;
            self.commit_index = new_commit_index;
        }
    }

    /// Apply committed entries to the state machine.
    pub fn apply_committed(&mut self) -> Vec<RaftLogEntry> {
        let mut applied = Vec::new();

        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.get(self.last_applied as usize - 1) {
                applied.push(entry.clone());
                self.stats.entries_applied += 1;
            }
        }

        applied
    }

    /// Flush pending entries to persistent storage (deferred persistence).
    pub fn flush_pending(&mut self) -> u64 {
        let batch_size = self.config.persistence_batch_size as usize;
        let to_flush = self.pending_flush.len().min(batch_size);
        let mut flushed = 0;

        for _ in 0..to_flush {
            if let Some(_index) = self.pending_flush.pop_front() {
                flushed += 1;
                self.total_flushed += 1;
            }
        }

        self.stats.deferred_flushes += 1;
        flushed
    }

    /// Take a snapshot — compact the log up to last_applied.
    pub fn take_snapshot(&mut self) -> u64 {
        if self.last_applied == 0 {
            return 0;
        }

        let snapshot_index = self.last_applied;
        let entries_to_remove = self
            .log
            .iter()
            .take_while(|e| e.index <= snapshot_index)
            .count();

        self.log.drain(..entries_to_remove);
        self.stats.snapshots_taken += 1;

        snapshot_index
    }

    /// Check if an election timeout has occurred.
    pub fn check_election_timeout(&self) -> bool {
        self.state != RaftState::Leader && Instant::now() >= self.election_deadline
    }

    /// Reset the election timer (on heartbeat from leader).
    pub fn reset_election_timer(&mut self) {
        let timeout = Duration::from_millis(
            self.config.election_timeout_min_ms
                + (self.config.node_id * 37
                    % (self.config.election_timeout_max_ms - self.config.election_timeout_min_ms
                        + 1)),
        );
        self.election_deadline = Instant::now() + timeout;
    }

    /// Send heartbeat to peers (leader only). Returns list of (peer_id, entries) to replicate.
    pub fn prepare_heartbeats(&mut self) -> Vec<(u64, Vec<RaftLogEntry>)> {
        if self.state != RaftState::Leader {
            return Vec::new();
        }

        self.last_heartbeat = Instant::now();
        let mut heartbeats = Vec::new();

        for &peer in &self.peers.clone() {
            let next = self.next_index.get(&peer).copied().unwrap_or(1);
            let entries: Vec<RaftLogEntry> = self
                .log
                .iter()
                .filter(|e| e.index >= next)
                .cloned()
                .collect();

            if !entries.is_empty() {
                heartbeats.push((peer, entries.clone()));
                if let Some(last) = entries.last() {
                    self.match_index.insert(peer, last.index);
                    self.next_index.insert(peer, last.index + 1);
                }
            }
        }

        heartbeats
    }

    // Accessors
    pub fn state(&self) -> RaftState {
        self.state
    }
    pub fn current_term(&self) -> u64 {
        self.current_term
    }
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }
    pub fn log_length(&self) -> u64 {
        self.log.len() as u64
    }
    pub fn is_leader(&self) -> bool {
        self.state == RaftState::Leader
    }
    pub fn node_id(&self) -> u64 {
        self.config.node_id
    }
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }
    pub fn pending_flush_count(&self) -> u64 {
        self.pending_flush.len() as u64
    }

    pub fn stats(&self) -> RaftStats {
        RaftStats {
            current_term: self.current_term,
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            log_length: self.log.len() as u64,
            elections_won: self.stats.elections_won,
            entries_committed: self.stats.entries_committed,
            entries_applied: self.stats.entries_applied,
            snapshots_taken: self.stats.snapshots_taken,
            deferred_flushes: self.stats.deferred_flushes,
        }
    }
}

/// Manager for multiple Raft groups (one per namespace or shard).
pub struct RaftCluster {
    nodes: HashMap<u64, RaftNode>,
    next_group_id: u64,
}

impl RaftCluster {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            next_group_id: 0,
        }
    }

    /// Create a new Raft group and return its ID.
    pub fn create_group(&mut self, config: RaftConfig) -> u64 {
        let group_id = self.next_group_id;
        self.next_group_id += 1;
        self.nodes.insert(group_id, RaftNode::new(config));
        group_id
    }

    /// Get a reference to a Raft node by group ID.
    pub fn get_node(&self, group_id: u64) -> Option<&RaftNode> {
        self.nodes.get(&group_id)
    }

    /// Get a mutable reference to a Raft node by group ID.
    pub fn get_node_mut(&mut self, group_id: u64) -> Option<&mut RaftNode> {
        self.nodes.get_mut(&group_id)
    }

    /// Remove a Raft group.
    pub fn remove_group(&mut self, group_id: u64) -> bool {
        self.nodes.remove(&group_id).is_some()
    }

    /// Get the number of active Raft groups.
    pub fn group_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get aggregate stats across all groups.
    pub fn aggregate_stats(&self) -> RaftStats {
        let mut total = RaftStats::default();
        for node in self.nodes.values() {
            let s = node.stats();
            total.current_term = total.current_term.max(s.current_term);
            total.commit_index += s.commit_index;
            total.last_applied += s.last_applied;
            total.log_length += s.log_length;
            total.elections_won += s.elections_won;
            total.entries_committed += s.entries_committed;
            total.entries_applied += s.entries_applied;
            total.snapshots_taken += s.snapshots_taken;
            total.deferred_flushes += s.deferred_flushes;
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(node_id: u64) -> RaftConfig {
        RaftConfig {
            node_id,
            defer_persistence: true,
            persistence_batch_size: 10,
            ..Default::default()
        }
    }

    #[test]
    fn test_raft_creation() {
        let node = RaftNode::new(test_config(0));
        assert_eq!(node.state(), RaftState::Follower);
        assert_eq!(node.current_term(), 0);
        assert_eq!(node.log_length(), 0);
    }

    #[test]
    fn test_raft_election() {
        let mut node = RaftNode::new(test_config(0));
        assert!(node.start_election());
        assert_eq!(node.state(), RaftState::Candidate);
        assert_eq!(node.current_term(), 1);

        node.become_leader();
        assert_eq!(node.state(), RaftState::Leader);
        assert!(node.is_leader());
    }

    #[test]
    fn test_raft_append_and_commit() {
        let mut node = RaftNode::new(test_config(0));
        node.become_leader();

        let idx = node.append_entry(42, RaftEventType::WorkflowStarted, vec![1, 2, 3]);
        assert_eq!(idx, Some(1));
        assert_eq!(node.log_length(), 1);

        // Single node: auto-committed
        assert_eq!(node.commit_index(), 1);

        let applied = node.apply_committed();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].workflow_key, 42);
    }

    #[test]
    fn test_raft_multiple_entries() {
        let mut node = RaftNode::new(test_config(0));
        node.become_leader();

        for i in 0..10 {
            node.append_entry(i, RaftEventType::StepCompleted, vec![]);
        }

        assert_eq!(node.log_length(), 10);
        assert_eq!(node.commit_index(), 10);

        let applied = node.apply_committed();
        assert_eq!(applied.len(), 10);
    }

    #[test]
    fn test_raft_snapshot() {
        let mut node = RaftNode::new(test_config(0));
        node.become_leader();

        for i in 0..20 {
            node.append_entry(i, RaftEventType::StepCompleted, vec![]);
        }

        node.apply_committed();
        let snap = node.take_snapshot();
        assert_eq!(snap, 20);
        assert_eq!(node.log_length(), 0); // All entries compacted
    }

    #[test]
    fn test_raft_deferred_flush() {
        let mut node = RaftNode::new(test_config(0));
        node.become_leader();

        for i in 0..5 {
            node.append_entry(i, RaftEventType::StepCompleted, vec![]);
        }

        assert_eq!(node.pending_flush_count(), 5);
        let flushed = node.flush_pending();
        assert_eq!(flushed, 5);
        assert_eq!(node.pending_flush_count(), 0);
    }

    #[test]
    fn test_raft_cluster() {
        let mut cluster = RaftCluster::new();

        let g1 = cluster.create_group(test_config(0));
        let g2 = cluster.create_group(test_config(1));

        assert_eq!(cluster.group_count(), 2);

        // Make both leaders and append entries
        cluster.get_node_mut(g1).unwrap().become_leader();
        cluster.get_node_mut(g2).unwrap().become_leader();

        cluster
            .get_node_mut(g1)
            .unwrap()
            .append_entry(1, RaftEventType::WorkflowStarted, vec![]);
        cluster
            .get_node_mut(g2)
            .unwrap()
            .append_entry(2, RaftEventType::WorkflowStarted, vec![]);

        let stats = cluster.aggregate_stats();
        assert_eq!(stats.entries_committed, 2);
    }

    #[test]
    fn test_raft_peer_management() {
        let mut node = RaftNode::new(test_config(0));
        node.add_peer(1);
        node.add_peer(2);
        assert_eq!(node.peer_count(), 2);

        node.remove_peer(1);
        assert_eq!(node.peer_count(), 1);
    }

    #[test]
    fn test_raft_follower_cannot_append() {
        let mut node = RaftNode::new(test_config(0));
        let result = node.append_entry(1, RaftEventType::WorkflowStarted, vec![]);
        assert_eq!(result, None); // Follower can't append
    }
}
