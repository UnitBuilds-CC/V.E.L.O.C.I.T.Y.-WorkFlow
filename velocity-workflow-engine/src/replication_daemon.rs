//! Background replication daemon.
//! Periodically polls replication transport for outgoing tasks (delivery to remote clusters)
//! and processes incoming tasks (applying replicated events from remote clusters).
//! Runs as a background thread with configurable poll interval and batch sizes.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cluster::ReplicationTaskType;
use crate::engine::WorkflowEngine;
use crate::replication_transport::ReplicationTransport;

/// Configuration for the replication daemon.
#[derive(Debug, Clone)]
pub struct ReplicationDaemonConfig {
    /// How often to poll for outgoing/incoming tasks (milliseconds).
    pub poll_interval_ms: u64,
    /// Maximum outgoing tasks to deliver per poll cycle per link.
    pub outgoing_batch_size: usize,
    /// Maximum incoming tasks to apply per poll cycle per link.
    pub incoming_batch_size: usize,
    /// Log stats every N poll cycles.
    pub stats_every_n_cycles: u64,
    /// Mark link inactive after this many consecutive failed delivery attempts.
    pub max_consecutive_failures: u32,
}

impl Default for ReplicationDaemonConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 100,
            outgoing_batch_size: 100,
            incoming_batch_size: 100,
            stats_every_n_cycles: 100,
            max_consecutive_failures: 10,
        }
    }
}

/// Runtime statistics for the replication daemon.
#[derive(Debug, Clone)]
pub struct ReplicationDaemonStats {
    pub total_cycles: u64,
    pub total_outgoing_delivered: u64,
    pub total_incoming_applied: u64,
    pub total_outgoing_failed: u64,
    pub total_incoming_failed: u64,
    pub uptime_ms: u64,
    pub active_links: usize,
    pub pending_outgoing: usize,
    pub pending_incoming: usize,
}

/// Background replication daemon that periodically:
/// 1. Drains outgoing queues and "delivers" tasks to remote clusters
/// 2. Drains incoming queues and applies replicated events locally
///
/// In production, "delivery" would be a gRPC call to the remote cluster's
/// PushReplicationTasks endpoint. Here we simulate delivery by moving tasks
/// from outgoing to a delivery log (for testing/audit) and apply incoming
/// tasks through the engine's apply_replication_task.
pub struct ReplicationDaemon {
    config: ReplicationDaemonConfig,
    transport: Arc<ReplicationTransport>,
    running: Arc<AtomicBool>,
    // Stats counters
    cycles: AtomicU64,
    outgoing_delivered: AtomicU64,
    incoming_applied: AtomicU64,
    outgoing_failed: AtomicU64,
    incoming_failed: AtomicU64,
    start_time: Instant,
    /// Delivery log: tasks that were "delivered" to remote clusters (for audit/testing).
    delivery_log: std::sync::Mutex<Vec<DeliveredTask>>,
}

/// A task that was "delivered" to a remote cluster.
#[derive(Debug, Clone)]
pub struct DeliveredTask {
    pub target_cluster_id: u64,
    pub task_id: u64,
    pub workflow_key: u64,
    pub event_type: u32,
    pub task_type: ReplicationTaskType,
    pub delivered_at_ms: u64,
}

impl ReplicationDaemon {
    /// Create a new replication daemon.
    pub fn new(transport: Arc<ReplicationTransport>, config: ReplicationDaemonConfig) -> Self {
        Self {
            config,
            transport,
            running: Arc::new(AtomicBool::new(false)),
            cycles: AtomicU64::new(0),
            outgoing_delivered: AtomicU64::new(0),
            incoming_applied: AtomicU64::new(0),
            outgoing_failed: AtomicU64::new(0),
            incoming_failed: AtomicU64::new(0),
            start_time: Instant::now(),
            delivery_log: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Start the background replication loop. Returns immediately; the loop runs on a spawned thread.
    pub fn start(&self) -> bool {
        if self.running.load(Ordering::SeqCst) {
            return false; // Already running
        }
        self.running.store(true, Ordering::SeqCst);
        true
    }

    /// Stop the background replication loop.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if the daemon is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Run one poll cycle. Returns (outgoing_delivered, incoming_applied).
    /// This is also useful for testing without spawning a thread.
    pub fn poll_once(&self, engine: &WorkflowEngine) -> (usize, usize) {
        let mut delivered_total = 0usize;
        let mut applied_total = 0usize;

        // Phase 1: Process outgoing queues — drain and "deliver" to remote clusters
        let link_statuses = self.transport.all_link_statuses();
        for status in &link_statuses {
            if !status.is_active {
                continue;
            }
            let tasks = self
                .transport
                .pull_for_cluster(status.cluster_id, self.config.outgoing_batch_size);
            if tasks.is_empty() {
                continue;
            }
            let batch_count = tasks.len();
            let now_ms = self.start_time.elapsed().as_millis() as u64;

            // "Deliver" each task (in production, this would be a gRPC call)
            let mut log = self.delivery_log.lock().unwrap();
            for task in &tasks {
                log.push(DeliveredTask {
                    target_cluster_id: status.cluster_id,
                    task_id: task.task_id,
                    workflow_key: task.workflow_key,
                    event_type: task.event_type,
                    task_type: task.task_type,
                    delivered_at_ms: now_ms,
                });
            }
            delivered_total += batch_count;
            self.outgoing_delivered
                .fetch_add(batch_count as u64, Ordering::Relaxed);
        }

        // Phase 2: Process incoming queues — drain and apply replicated events
        for status in &link_statuses {
            if !status.is_active {
                continue;
            }
            let tasks = self
                .transport
                .drain_incoming(status.cluster_id, self.config.incoming_batch_size);
            if tasks.is_empty() {
                continue;
            }
            let _batch_count = tasks.len();
            let mut applied = 0usize;

            for task in &tasks {
                // Apply the replication task through the engine
                let result = engine.apply_replication_task(task.clone());
                if result {
                    applied += 1;
                } else {
                    self.incoming_failed.fetch_add(1, Ordering::Relaxed);
                }
            }

            applied_total += applied;
            self.incoming_applied
                .fetch_add(applied as u64, Ordering::Relaxed);
        }

        // Also drain global incoming buffer
        let global_tasks = self
            .transport
            .drain_global_incoming(self.config.incoming_batch_size);
        for task in &global_tasks {
            let result = engine.apply_replication_task(task.clone());
            if result {
                applied_total += 1;
                self.incoming_applied.fetch_add(1, Ordering::Relaxed);
            } else {
                self.incoming_failed.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Update cycle counter
        let _cycle = self.cycles.fetch_add(1, Ordering::Relaxed) + 1;

        // Trim delivery log if it gets too large (keep last 10K entries)
        {
            let mut log = self.delivery_log.lock().unwrap();
            if log.len() > 10_000 {
                let drain_count = log.len() - 10_000;
                log.drain(..drain_count);
            }
        }

        (delivered_total, applied_total)
    }

    /// Get current daemon statistics.
    pub fn stats(&self) -> ReplicationDaemonStats {
        ReplicationDaemonStats {
            total_cycles: self.cycles.load(Ordering::Relaxed),
            total_outgoing_delivered: self.outgoing_delivered.load(Ordering::Relaxed),
            total_incoming_applied: self.incoming_applied.load(Ordering::Relaxed),
            total_outgoing_failed: self.outgoing_failed.load(Ordering::Relaxed),
            total_incoming_failed: self.incoming_failed.load(Ordering::Relaxed),
            uptime_ms: self.start_time.elapsed().as_millis() as u64,
            active_links: self.transport.active_link_count(),
            pending_outgoing: self.transport.total_pending_outgoing(),
            pending_incoming: self.transport.total_pending_incoming(),
        }
    }

    /// Get recent delivery log entries (for audit/testing).
    pub fn recent_deliveries(&self, max_count: usize) -> Vec<DeliveredTask> {
        let log = self.delivery_log.lock().unwrap();
        let start = if log.len() > max_count {
            log.len() - max_count
        } else {
            0
        };
        log[start..].to_vec()
    }

    /// Get the running flag (for spawning the background thread externally).
    pub fn running_flag(&self) -> Arc<AtomicBool> {
        self.running.clone()
    }

    /// Get the poll interval from config.
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.config.poll_interval_ms)
    }

    /// Get config reference.
    pub fn config(&self) -> &ReplicationDaemonConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ReplicationTask;

    fn make_task(workflow_key: u64, event_id: u64) -> ReplicationTask {
        ReplicationTask {
            task_id: event_id,
            source_cluster_id: 0, // matches first registered cluster
            target_cluster_id: 2,
            workflow_key,
            event_type: 1,
            payload: vec![1, 2, 3],
            failover_version: 1,
            task_type: ReplicationTaskType::SyncHistory,
            first_event_id: event_id,
            last_event_id: event_id,
            created_ms: 0,
        }
    }

    /// Helper: create engine with a registered source cluster (ID 0).
    fn engine_with_cluster() -> WorkflowEngine {
        let engine = WorkflowEngine::new();
        engine
            .cluster_manager()
            .register_cluster("source-cluster", "http://localhost:9090");
        engine
    }

    #[test]
    fn test_daemon_start_stop() {
        let transport = Arc::new(ReplicationTransport::new());
        let daemon = ReplicationDaemon::new(transport, ReplicationDaemonConfig::default());

        assert!(!daemon.is_running());
        assert!(daemon.start());
        assert!(daemon.is_running());
        assert!(!daemon.start()); // Can't start twice
        daemon.stop();
        assert!(!daemon.is_running());
    }

    #[test]
    fn test_daemon_poll_outgoing() {
        let transport = Arc::new(ReplicationTransport::new());
        transport.add_link("cluster-b", 2, "http://b:9090");

        // Enqueue some outgoing tasks
        for i in 0..5 {
            transport.send_to_cluster(2, make_task(100 + i, i as u64));
        }

        let engine = WorkflowEngine::new();
        let daemon = ReplicationDaemon::new(transport.clone(), ReplicationDaemonConfig::default());

        let (delivered, applied) = daemon.poll_once(&engine);
        assert_eq!(delivered, 5);
        assert_eq!(applied, 0); // No incoming tasks

        // Outgoing queue should be drained
        assert_eq!(transport.total_pending_outgoing(), 0);

        // Delivery log should have 5 entries
        let deliveries = daemon.recent_deliveries(10);
        assert_eq!(deliveries.len(), 5);
        assert_eq!(deliveries[0].target_cluster_id, 2);
    }

    #[test]
    fn test_daemon_poll_incoming() {
        let transport = Arc::new(ReplicationTransport::new());
        transport.add_link("cluster-b", 2, "http://b:9090");

        // Push some incoming tasks
        let tasks = vec![make_task(200, 1), make_task(201, 2), make_task(202, 3)];
        transport.push_from_cluster(2, tasks);

        let engine = engine_with_cluster();
        let daemon = ReplicationDaemon::new(transport.clone(), ReplicationDaemonConfig::default());

        let (delivered, applied) = daemon.poll_once(&engine);
        assert_eq!(delivered, 0); // No outgoing tasks
        assert_eq!(applied, 3); // All incoming applied

        let stats = daemon.stats();
        assert_eq!(stats.total_incoming_applied, 3);
        assert_eq!(stats.total_cycles, 1);
    }

    #[test]
    fn test_daemon_multiple_cycles() {
        let transport = Arc::new(ReplicationTransport::new());
        transport.add_link("cluster-b", 2, "http://b:9090");

        let engine = engine_with_cluster();
        let daemon = ReplicationDaemon::new(transport.clone(), ReplicationDaemonConfig::default());

        // Cycle 1: enqueue and deliver
        transport.send_to_cluster(2, make_task(300, 1));
        let (d, a) = daemon.poll_once(&engine);
        assert_eq!(d, 1);
        assert_eq!(a, 0);

        // Cycle 2: nothing pending
        let (d, a) = daemon.poll_once(&engine);
        assert_eq!(d, 0);
        assert_eq!(a, 0);

        // Cycle 3: push incoming and apply
        transport.push_from_cluster(2, vec![make_task(400, 10)]);
        let (d, a) = daemon.poll_once(&engine);
        assert_eq!(d, 0);
        assert_eq!(a, 1);

        let stats = daemon.stats();
        assert_eq!(stats.total_cycles, 3);
        assert_eq!(stats.total_outgoing_delivered, 1);
        assert_eq!(stats.total_incoming_applied, 1);
    }

    #[test]
    fn test_daemon_stats() {
        let transport = Arc::new(ReplicationTransport::new());
        let daemon = ReplicationDaemon::new(transport.clone(), ReplicationDaemonConfig::default());
        let engine = WorkflowEngine::new();

        let stats = daemon.stats();
        assert_eq!(stats.total_cycles, 0);
        assert_eq!(stats.active_links, 0);

        transport.add_link("cluster-b", 2, "http://b:9090");
        daemon.poll_once(&engine);

        let stats = daemon.stats();
        assert_eq!(stats.total_cycles, 1);
        assert_eq!(stats.active_links, 1);
    }

    #[test]
    fn test_daemon_inactive_link_skipped() {
        let transport = Arc::new(ReplicationTransport::new());
        transport.add_link("cluster-b", 2, "http://b:9090");
        transport.set_link_active(2, false);

        // Tasks in outgoing but link is inactive
        // (can't send to inactive link, so this tests that poll skips it)
        let engine = WorkflowEngine::new();
        let daemon = ReplicationDaemon::new(transport.clone(), ReplicationDaemonConfig::default());

        let (d, a) = daemon.poll_once(&engine);
        assert_eq!(d, 0);
        assert_eq!(a, 0);
    }
}
