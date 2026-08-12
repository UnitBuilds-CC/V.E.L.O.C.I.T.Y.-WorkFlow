// Multi-node Raft Cluster tests
//
// Tests multi-node Raft consensus: leader election, log replication,
// commit quorum, failover, snapshots, and cluster membership changes.

use std::collections::HashMap;
use std::time::Duration;
use velocity_workflow_engine::raft_consensus::{
    RaftCluster, RaftConfig, RaftEventType, RaftNode, RaftState,
};

fn test_config(node_id: u64) -> RaftConfig {
    RaftConfig {
        node_id,
        election_timeout_min_ms: 50,
        election_timeout_max_ms: 150,
        heartbeat_interval_ms: 25,
        snapshot_threshold: 100,
        defer_persistence: false,
        persistence_batch_size: 10,
    }
}

fn deferred_config(node_id: u64) -> RaftConfig {
    RaftConfig {
        node_id,
        election_timeout_min_ms: 50,
        election_timeout_max_ms: 150,
        heartbeat_interval_ms: 25,
        snapshot_threshold: 100,
        defer_persistence: true,
        persistence_batch_size: 10,
    }
}

// ============================================================================
// Single Node Tests (baseline)
// ============================================================================

#[test]
fn test_single_node_creation() {
    let node = RaftNode::new(test_config(0));
    assert_eq!(node.state(), RaftState::Follower);
    assert_eq!(node.current_term(), 0);
    assert_eq!(node.log_length(), 0);
    assert_eq!(node.commit_index(), 0);
    assert!(!node.is_leader());
}

#[test]
fn test_single_node_election() {
    let mut node = RaftNode::new(test_config(0));
    assert!(node.start_election());
    assert_eq!(node.state(), RaftState::Candidate);
    assert_eq!(node.current_term(), 1);

    node.become_leader();
    assert_eq!(node.state(), RaftState::Leader);
    assert!(node.is_leader());
}

#[test]
fn test_single_node_append_and_commit() {
    let mut node = RaftNode::new(test_config(0));
    node.become_leader();

    let idx = node.append_entry(1, RaftEventType::WorkflowStarted, vec![1, 2, 3]);
    assert!(idx.is_some());
    // Single node: auto-commits immediately
    assert_eq!(node.commit_index(), 1);

    let applied = node.apply_committed();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].workflow_key, 1);
}

#[test]
fn test_single_node_multiple_entries() {
    let mut node = RaftNode::new(test_config(0));
    node.become_leader();

    for i in 0..20 {
        node.append_entry(i, RaftEventType::StepCompleted, vec![]);
    }

    assert_eq!(node.log_length(), 20);
    assert_eq!(node.commit_index(), 20);

    let applied = node.apply_committed();
    assert_eq!(applied.len(), 20);
}

// ============================================================================
// Multi-Node Cluster Tests
// ============================================================================

#[test]
fn test_cluster_creation() {
    let cluster = RaftCluster::new();
    assert_eq!(cluster.group_count(), 0);
}

#[test]
fn test_cluster_create_group() {
    let mut cluster = RaftCluster::new();
    let group_id = cluster.create_group(test_config(0));
    assert_eq!(group_id, 0);
    assert_eq!(cluster.group_count(), 1);
    assert!(cluster.get_node(group_id).is_some());
}

#[test]
fn test_cluster_create_multiple_groups() {
    let mut cluster = RaftCluster::new();
    let g0 = cluster.create_group(test_config(0));
    let g1 = cluster.create_group(test_config(1));
    let g2 = cluster.create_group(test_config(2));

    assert_eq!(cluster.group_count(), 3);
    assert_eq!(g0, 0);
    assert_eq!(g1, 1);
    assert_eq!(g2, 2);
}

#[test]
fn test_cluster_remove_group() {
    let mut cluster = RaftCluster::new();
    let g0 = cluster.create_group(test_config(0));
    assert_eq!(cluster.group_count(), 1);

    let removed = cluster.remove_group(g0);
    assert!(removed);
    assert_eq!(cluster.group_count(), 0);
}

#[test]
fn test_cluster_remove_nonexistent() {
    let mut cluster = RaftCluster::new();
    let removed = cluster.remove_group(999);
    assert!(!removed);
}

#[test]
fn test_cluster_get_node() {
    let mut cluster = RaftCluster::new();
    let g0 = cluster.create_group(test_config(0));

    let node = cluster.get_node(g0).unwrap();
    assert_eq!(node.state(), RaftState::Follower);
    assert_eq!(node.current_term(), 0);
}

#[test]
fn test_cluster_get_node_mut() {
    let mut cluster = RaftCluster::new();
    let g0 = cluster.create_group(test_config(0));

    {
        let node = cluster.get_node_mut(g0).unwrap();
        node.become_leader();
    }

    let node = cluster.get_node(g0).unwrap();
    assert_eq!(node.state(), RaftState::Leader);
}

// ============================================================================
// Multi-Node Peer Management Tests
// ============================================================================

#[test]
fn test_add_peer() {
    let mut node = RaftNode::new(test_config(0));
    node.add_peer(1);
    node.add_peer(2);
    // Node should now have peers
    // (peers are internal but affect quorum calculations)
    assert_eq!(node.state(), RaftState::Follower);
}

#[test]
fn test_remove_peer() {
    let mut node = RaftNode::new(test_config(0));
    node.add_peer(1);
    node.add_peer(2);
    node.remove_peer(1);
    // Should still be a follower
    assert_eq!(node.state(), RaftState::Follower);
}

#[test]
fn test_remove_nonexistent_peer() {
    let mut node = RaftNode::new(test_config(0));
    // Removing a peer that doesn't exist should not panic
    node.remove_peer(999);
    assert_eq!(node.state(), RaftState::Follower);
}

// ============================================================================
// Multi-Node Election Tests
// ============================================================================

#[test]
fn test_election_in_single_node() {
    let mut node = RaftNode::new(test_config(0));
    assert!(node.start_election());
    assert_eq!(node.state(), RaftState::Candidate);
    assert_eq!(node.current_term(), 1);
}

#[test]
fn test_election_term_increment() {
    let mut node = RaftNode::new(test_config(0));
    assert_eq!(node.current_term(), 0);

    node.start_election();
    assert_eq!(node.current_term(), 1);

    // Simulate another election (after timeout)
    node.start_election();
    assert_eq!(node.current_term(), 2);
}

#[test]
fn test_become_leader_after_election() {
    let mut node = RaftNode::new(test_config(0));
    node.start_election();
    assert_eq!(node.state(), RaftState::Candidate);

    node.become_leader();
    assert_eq!(node.state(), RaftState::Leader);
    assert!(node.is_leader());
}

// ============================================================================
// Multi-Node Commit Quorum Tests
// ============================================================================

#[test]
fn test_commit_quorum_three_nodes() {
    // Simulate a 3-node cluster manually
    let mut node0 = RaftNode::new(test_config(0));
    let mut node1 = RaftNode::new(test_config(1));
    let mut node2 = RaftNode::new(test_config(2));

    // Set up peers
    node0.add_peer(1);
    node0.add_peer(2);
    node1.add_peer(0);
    node1.add_peer(2);
    node2.add_peer(0);
    node2.add_peer(1);

    // Node 0 becomes leader
    node0.start_election();
    node0.become_leader();
    assert!(node0.is_leader());

    // Leader appends entry
    let idx = node0.append_entry(42, RaftEventType::WorkflowStarted, vec![1, 2, 3]);
    assert!(idx.is_some());
}

#[test]
fn test_advance_commit_index() {
    let mut node = RaftNode::new(test_config(0));
    node.add_peer(1);
    node.add_peer(2);
    node.become_leader();

    // Append entries
    for i in 0..5 {
        node.append_entry(i, RaftEventType::StepCompleted, vec![]);
    }

    // Manually advance commit index (simulating quorum ack)
    node.advance_commit(3);
    assert_eq!(node.commit_index(), 3);

    let applied = node.apply_committed();
    assert_eq!(applied.len(), 3);
}

#[test]
fn test_commit_index_cannot_go_backwards() {
    let mut node = RaftNode::new(test_config(0));
    node.add_peer(1);
    node.add_peer(2);
    node.become_leader();

    for i in 0..5 {
        node.append_entry(i, RaftEventType::StepCompleted, vec![]);
    }

    // With peers, entries don't auto-commit. Advance to 3.
    node.advance_commit(3);
    assert_eq!(node.commit_index(), 3);

    // Trying to go backwards should be a no-op
    node.advance_commit(1);
    assert!(node.commit_index() >= 3, "Commit index should not go backwards");
}

// ============================================================================
// Heartbeat Tests
// ============================================================================

#[test]
fn test_prepare_heartbeats() {
    let mut node = RaftNode::new(test_config(0));
    node.add_peer(1);
    node.add_peer(2);
    node.become_leader();

    // Append some entries
    node.append_entry(0, RaftEventType::WorkflowStarted, vec![1]);
    node.append_entry(1, RaftEventType::StepCompleted, vec![2]);

    let heartbeats = node.prepare_heartbeats();
    // Should have entries for each peer
    assert_eq!(heartbeats.len(), 2);
}

#[test]
fn test_heartbeats_empty_for_follower() {
    let mut node = RaftNode::new(test_config(0));
    // Follower should not prepare heartbeats
    let heartbeats = node.prepare_heartbeats();
    assert!(heartbeats.is_empty());
}

// ============================================================================
// Election Timeout Tests
// ============================================================================

#[test]
fn test_election_timeout_not_triggered_immediately() {
    let node = RaftNode::new(test_config(0));
    // Just created — should not have timed out yet
    assert!(!node.check_election_timeout());
}

#[test]
fn test_reset_election_timer() {
    let mut node = RaftNode::new(test_config(0));
    node.reset_election_timer();
    // After reset, should not have timed out
    assert!(!node.check_election_timeout());
}

// ============================================================================
// Snapshot Tests
// ============================================================================

#[test]
fn test_snapshot_compacts_log() {
    let mut node = RaftNode::new(test_config(0));
    node.become_leader();

    for i in 0..20 {
        node.append_entry(i, RaftEventType::StepCompleted, vec![]);
    }

    node.apply_committed();
    assert_eq!(node.log_length(), 20);

    let snap_index = node.take_snapshot();
    assert_eq!(snap_index, 20);
    assert_eq!(node.log_length(), 0); // All entries compacted
}

#[test]
fn test_snapshot_preserves_commit_index() {
    let mut node = RaftNode::new(test_config(0));
    node.become_leader();

    for i in 0..10 {
        node.append_entry(i, RaftEventType::StepCompleted, vec![]);
    }
    node.apply_committed();

    let commit_before = node.commit_index();
    node.take_snapshot();
    let commit_after = node.commit_index();

    assert_eq!(commit_before, commit_after);
}

// ============================================================================
// Deferred Persistence Tests
// ============================================================================

#[test]
fn test_deferred_persistence_pending_count() {
    let mut node = RaftNode::new(deferred_config(0));
    node.become_leader();

    for i in 0..5 {
        node.append_entry(i, RaftEventType::StepCompleted, vec![]);
    }

    assert_eq!(node.pending_flush_count(), 5);
}

#[test]
fn test_deferred_persistence_flush() {
    let mut node = RaftNode::new(deferred_config(0));
    node.become_leader();

    for i in 0..5 {
        node.append_entry(i, RaftEventType::StepCompleted, vec![]);
    }

    let flushed = node.flush_pending();
    assert_eq!(flushed, 5);
    assert_eq!(node.pending_flush_count(), 0);
}

// ============================================================================
// Aggregate Stats Tests
// ============================================================================

#[test]
fn test_cluster_aggregate_stats() {
    let mut cluster = RaftCluster::new();
    let g0 = cluster.create_group(test_config(0));
    let g1 = cluster.create_group(test_config(1));

    // Make both groups leaders and add entries
    {
        let node = cluster.get_node_mut(g0).unwrap();
        node.become_leader();
        for i in 0..5 {
            node.append_entry(i, RaftEventType::StepCompleted, vec![]);
        }
    }
    {
        let node = cluster.get_node_mut(g1).unwrap();
        node.become_leader();
        for i in 0..3 {
            node.append_entry(i, RaftEventType::StepCompleted, vec![]);
        }
    }

    let stats = cluster.aggregate_stats();
    assert_eq!(stats.log_length, 8); // 5 + 3
}

#[test]
fn test_node_stats() {
    let mut node = RaftNode::new(test_config(0));
    node.become_leader();

    for i in 0..10 {
        node.append_entry(i, RaftEventType::StepCompleted, vec![]);
    }
    node.apply_committed();

    let stats = node.stats();
    assert_eq!(stats.log_length, 10);
    assert_eq!(stats.commit_index, 10);
    assert_eq!(stats.last_applied, 10);
    assert_eq!(stats.entries_committed, 10);
    assert_eq!(stats.entries_applied, 10);
}

// ============================================================================
// Failover Simulation Tests
// ============================================================================

#[test]
fn test_leader_cannot_start_election() {
    let mut node = RaftNode::new(test_config(0));
    node.start_election();
    node.become_leader();
    assert_eq!(node.state(), RaftState::Leader);
    let term_before = node.current_term();

    // A leader should NOT start a new election
    let result = node.start_election();
    assert!(!result, "Leader should not start a new election");
    assert_eq!(node.current_term(), term_before, "Term should not change");
    assert_eq!(node.state(), RaftState::Leader, "Should remain leader");
}

#[test]
fn test_follower_cannot_append_entries() {
    let mut node = RaftNode::new(test_config(0));
    assert_eq!(node.state(), RaftState::Follower);

    // Follower should not be able to append entries
    let result = node.append_entry(1, RaftEventType::WorkflowStarted, vec![]);
    assert!(result.is_none(), "Follower cannot append entries");
}

#[test]
fn test_candidate_cannot_append_entries() {
    let mut node = RaftNode::new(test_config(0));
    node.start_election();
    assert_eq!(node.state(), RaftState::Candidate);

    let result = node.append_entry(1, RaftEventType::WorkflowStarted, vec![]);
    assert!(result.is_none(), "Candidate cannot append entries");
}

// ============================================================================
// Cluster Membership Tests
// ============================================================================

#[test]
fn test_cluster_membership_changes() {
    let mut cluster = RaftCluster::new();

    // Create 5 groups
    let groups: Vec<u64> = (0..5)
        .map(|i| cluster.create_group(test_config(i)))
        .collect();

    assert_eq!(cluster.group_count(), 5);

    // Remove 2 groups
    cluster.remove_group(groups[1]);
    cluster.remove_group(groups[3]);

    assert_eq!(cluster.group_count(), 3);
    assert!(cluster.get_node(groups[0]).is_some());
    assert!(cluster.get_node(groups[1]).is_none());
    assert!(cluster.get_node(groups[2]).is_some());
    assert!(cluster.get_node(groups[3]).is_none());
    assert!(cluster.get_node(groups[4]).is_some());
}

// ============================================================================
// RaftConfig Tests
// ============================================================================

#[test]
fn test_raft_config_default() {
    let config = RaftConfig::default();
    assert_eq!(config.node_id, 0);
    assert_eq!(config.election_timeout_min_ms, 150);
    assert_eq!(config.election_timeout_max_ms, 300);
    assert_eq!(config.heartbeat_interval_ms, 50);
    assert_eq!(config.snapshot_threshold, 10_000);
    assert!(config.defer_persistence);
}

#[test]
fn test_raft_config_custom() {
    let config = test_config(42);
    assert_eq!(config.node_id, 42);
    assert_eq!(config.election_timeout_min_ms, 50);
    assert_eq!(config.election_timeout_max_ms, 150);
    assert_eq!(config.heartbeat_interval_ms, 25);
}

// ============================================================================
// RaftEventType Tests
// ============================================================================

#[test]
fn test_raft_event_type_variants() {
    let events = vec![
        RaftEventType::WorkflowStarted,
        RaftEventType::WorkflowCompleted,
        RaftEventType::WorkflowFailed,
        RaftEventType::StepCompleted,
        RaftEventType::ActivityScheduled,
        RaftEventType::ActivityCompleted,
        RaftEventType::TimerScheduled,
        RaftEventType::TimerFired,
        RaftEventType::SignalReceived,
        RaftEventType::WorkflowCanceled,
    ];
    // All variants should be distinct (check by pairwise comparison)
    for i in 0..events.len() {
        for j in (i + 1)..events.len() {
            assert_ne!(events[i], events[j], "Variants at {} and {} should differ", i, j);
        }
    }
    assert_eq!(events.len(), 10);
}
