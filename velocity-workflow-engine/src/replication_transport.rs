//! Multi-cluster replication transport.
//! Manages outgoing replication queues per remote cluster and incoming task buffers.
//! Supports both push (enqueue for delivery) and pull (poll-based retrieval) models.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use crate::cluster::{ReplicationTask, ReplicationTaskType};

/// Status of a replication link to a remote cluster.
#[derive(Debug, Clone)]
pub struct ReplicationLinkStatus {
    pub cluster_name: String,
    pub cluster_id: u64,
    pub endpoint: String,
    pub pending_outgoing: usize,
    pub pending_incoming: usize,
    pub last_sent_task_id: u64,
    pub last_received_task_id: u64,
    pub is_active: bool,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub tasks_sent: u64,
    pub tasks_received: u64,
}

/// A replication link to a remote cluster.
/// Manages outgoing and incoming task queues.
#[derive(Debug)]
struct ReplicationLink {
    cluster_name: String,
    cluster_id: u64,
    endpoint: String,
    outgoing: VecDeque<ReplicationTask>,
    incoming: VecDeque<ReplicationTask>,
    last_sent_task_id: u64,
    last_received_task_id: u64,
    is_active: bool,
    bytes_sent: u64,
    bytes_received: u64,
    tasks_sent: u64,
    tasks_received: u64,
    max_queue_size: usize,
}

impl ReplicationLink {
    fn new(cluster_name: &str, cluster_id: u64, endpoint: &str) -> Self {
        Self {
            cluster_name: cluster_name.to_string(),
            cluster_id,
            endpoint: endpoint.to_string(),
            outgoing: VecDeque::new(),
            incoming: VecDeque::new(),
            last_sent_task_id: 0,
            last_received_task_id: 0,
            is_active: true,
            bytes_sent: 0,
            bytes_received: 0,
            tasks_sent: 0,
            tasks_received: 0,
            max_queue_size: 10_000,
        }
    }

    /// Enqueue a task for delivery to the remote cluster.
    fn enqueue_outgoing(&mut self, task: ReplicationTask) -> bool {
        if self.outgoing.len() >= self.max_queue_size {
            return false; // Queue full
        }
        self.outgoing.push_back(task);
        true
    }

    /// Drain up to `max_count` tasks from the outgoing queue for delivery.
    fn drain_outgoing(&mut self, max_count: usize) -> Vec<ReplicationTask> {
        let count = max_count.min(self.outgoing.len());
        let tasks: Vec<ReplicationTask> = self.outgoing.drain(..count).collect();
        for task in &tasks {
            self.last_sent_task_id = self.last_sent_task_id.max(task.last_event_id);
            self.tasks_sent += 1;
            // Estimate bytes (simplified)
            self.bytes_sent += task.payload.len() as u64 + 128;
        }
        tasks
    }

    /// Receive incoming tasks from the remote cluster.
    fn receive_incoming(&mut self, tasks: Vec<ReplicationTask>) -> usize {
        let count = tasks.len();
        for task in tasks {
            self.last_received_task_id = self.last_received_task_id.max(task.last_event_id);
            self.tasks_received += 1;
            self.bytes_received += task.payload.len() as u64 + 128;
            self.incoming.push_back(task);
        }
        count
    }

    /// Drain incoming tasks for local processing.
    fn drain_incoming(&mut self, max_count: usize) -> Vec<ReplicationTask> {
        let count = max_count.min(self.incoming.len());
        self.incoming.drain(..count).collect()
    }

    fn status(&self) -> ReplicationLinkStatus {
        ReplicationLinkStatus {
            cluster_name: self.cluster_name.clone(),
            cluster_id: self.cluster_id,
            endpoint: self.endpoint.clone(),
            pending_outgoing: self.outgoing.len(),
            pending_incoming: self.incoming.len(),
            last_sent_task_id: self.last_sent_task_id,
            last_received_task_id: self.last_received_task_id,
            is_active: self.is_active,
            bytes_sent: self.bytes_sent,
            bytes_received: self.bytes_received,
            tasks_sent: self.tasks_sent,
            tasks_received: self.tasks_received,
        }
    }
}

/// Manages replication links to all remote clusters.
pub struct ReplicationTransport {
    links: RwLock<HashMap<u64, ReplicationLink>>,
    /// Global incoming buffer for tasks that arrive before a link is established.
    global_incoming: RwLock<VecDeque<ReplicationTask>>,
}

impl ReplicationTransport {
    pub fn new() -> Self {
        Self {
            links: RwLock::new(HashMap::new()),
            global_incoming: RwLock::new(VecDeque::new()),
        }
    }

    /// Register a remote cluster endpoint for replication.
    pub fn add_link(&self, cluster_name: &str, cluster_id: u64, endpoint: &str) {
        let mut links = self.links.write().unwrap();
        links.insert(cluster_id, ReplicationLink::new(cluster_name, cluster_id, endpoint));
    }

    /// Remove a replication link.
    pub fn remove_link(&self, cluster_id: u64) -> bool {
        let mut links = self.links.write().unwrap();
        links.remove(&cluster_id).is_some()
    }

    /// Activate or deactivate a replication link.
    pub fn set_link_active(&self, cluster_id: u64, active: bool) -> bool {
        let mut links = self.links.write().unwrap();
        if let Some(link) = links.get_mut(&cluster_id) {
            link.is_active = active;
            true
        } else {
            false
        }
    }

    /// Enqueue a replication task for delivery to a specific remote cluster.
    pub fn send_to_cluster(&self, cluster_id: u64, task: ReplicationTask) -> bool {
        let mut links = self.links.write().unwrap();
        if let Some(link) = links.get_mut(&cluster_id) {
            if !link.is_active {
                return false;
            }
            link.enqueue_outgoing(task)
        } else {
            false
        }
    }

    /// Enqueue a replication task for delivery to ALL remote clusters.
    pub fn broadcast(&self, task: ReplicationTask) -> usize {
        let mut links = self.links.write().unwrap();
        let mut sent = 0;
        for (_, link) in links.iter_mut() {
            if link.is_active && link.enqueue_outgoing(task.clone()) {
                sent += 1;
            }
        }
        sent
    }

    /// Pull tasks destined for a specific remote cluster (poll-based transport).
    /// Called by the gRPC StreamReplicationTasks handler.
    pub fn pull_for_cluster(&self, cluster_id: u64, max_count: usize) -> Vec<ReplicationTask> {
        let mut links = self.links.write().unwrap();
        if let Some(link) = links.get_mut(&cluster_id) {
            link.drain_outgoing(max_count)
        } else {
            Vec::new()
        }
    }

    /// Push incoming tasks from a remote cluster.
    /// Called by the gRPC PushReplicationTasks handler.
    pub fn push_from_cluster(&self, cluster_id: u64, tasks: Vec<ReplicationTask>) -> usize {
        let mut links = self.links.write().unwrap();
        if let Some(link) = links.get_mut(&cluster_id) {
            link.receive_incoming(tasks)
        } else {
            // No link established — buffer in global incoming
            let mut global = self.global_incoming.write().unwrap();
            let count = tasks.len();
            for task in tasks {
                global.push_back(task);
            }
            count
        }
    }

    /// Drain incoming tasks from a specific cluster for local processing.
    pub fn drain_incoming(&self, cluster_id: u64, max_count: usize) -> Vec<ReplicationTask> {
        let mut links = self.links.write().unwrap();
        if let Some(link) = links.get_mut(&cluster_id) {
            link.drain_incoming(max_count)
        } else {
            Vec::new()
        }
    }

    /// Drain from the global incoming buffer (tasks received before link was established).
    pub fn drain_global_incoming(&self, max_count: usize) -> Vec<ReplicationTask> {
        let mut global = self.global_incoming.write().unwrap();
        let count = max_count.min(global.len());
        global.drain(..count).collect()
    }

    /// Get status of all replication links.
    pub fn all_link_statuses(&self) -> Vec<ReplicationLinkStatus> {
        let links = self.links.read().unwrap();
        links.values().map(|l| l.status()).collect()
    }

    /// Get status of a specific replication link.
    pub fn link_status(&self, cluster_id: u64) -> Option<ReplicationLinkStatus> {
        let links = self.links.read().unwrap();
        links.get(&cluster_id).map(|l| l.status())
    }

    /// Count of active links.
    pub fn active_link_count(&self) -> usize {
        let links = self.links.read().unwrap();
        links.values().filter(|l| l.is_active).count()
    }

    /// Total pending outgoing tasks across all links.
    pub fn total_pending_outgoing(&self) -> usize {
        let links = self.links.read().unwrap();
        links.values().map(|l| l.outgoing.len()).sum()
    }

    /// Total pending incoming tasks across all links.
    pub fn total_pending_incoming(&self) -> usize {
        let links = self.links.read().unwrap();
        let global = self.global_incoming.read().unwrap();
        links.values().map(|l| l.incoming.len()).sum::<usize>() + global.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(workflow_key: u64, event_id: u64) -> ReplicationTask {
        ReplicationTask {
            task_id: event_id,
            source_cluster_id: 1,
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

    #[test]
    fn test_add_remove_link() {
        let transport = ReplicationTransport::new();
        transport.add_link("cluster-b", 2, "http://cluster-b:9090");
        assert_eq!(transport.active_link_count(), 1);
        
        transport.remove_link(2);
        assert_eq!(transport.active_link_count(), 0);
    }

    #[test]
    fn test_send_and_pull() {
        let transport = ReplicationTransport::new();
        transport.add_link("cluster-b", 2, "http://cluster-b:9090");
        
        let task = make_task(100, 5);
        assert!(transport.send_to_cluster(2, task));
        
        let pulled = transport.pull_for_cluster(2, 10);
        assert_eq!(pulled.len(), 1);
        assert_eq!(pulled[0].workflow_key, 100);
        
        // Second pull should be empty
        let pulled2 = transport.pull_for_cluster(2, 10);
        assert!(pulled2.is_empty());
    }

    #[test]
    fn test_broadcast() {
        let transport = ReplicationTransport::new();
        transport.add_link("cluster-b", 2, "http://b:9090");
        transport.add_link("cluster-c", 3, "http://c:9090");
        
        let task = make_task(200, 10);
        let sent = transport.broadcast(task);
        assert_eq!(sent, 2);
        
        assert_eq!(transport.pull_for_cluster(2, 10).len(), 1);
        assert_eq!(transport.pull_for_cluster(3, 10).len(), 1);
    }

    #[test]
    fn test_push_and_drain() {
        let transport = ReplicationTransport::new();
        transport.add_link("cluster-b", 2, "http://b:9090");
        
        let tasks = vec![make_task(300, 1), make_task(301, 2)];
        let received = transport.push_from_cluster(2, tasks);
        assert_eq!(received, 2);
        
        let drained = transport.drain_incoming(2, 10);
        assert_eq!(drained.len(), 2);
    }

    #[test]
    fn test_push_without_link() {
        let transport = ReplicationTransport::new();
        
        let tasks = vec![make_task(400, 1)];
        let received = transport.push_from_cluster(99, tasks);
        assert_eq!(received, 1);
        
        // Should be in global buffer
        let drained = transport.drain_global_incoming(10);
        assert_eq!(drained.len(), 1);
    }

    #[test]
    fn test_deactivate_link() {
        let transport = ReplicationTransport::new();
        transport.add_link("cluster-b", 2, "http://b:9090");
        
        transport.set_link_active(2, false);
        assert_eq!(transport.active_link_count(), 0);
        
        // Can't send to inactive link
        assert!(!transport.send_to_cluster(2, make_task(500, 1)));
    }

    #[test]
    fn test_link_status() {
        let transport = ReplicationTransport::new();
        transport.add_link("cluster-b", 2, "http://b:9090");
        
        transport.send_to_cluster(2, make_task(600, 5));
        transport.pull_for_cluster(2, 10);
        
        let status = transport.link_status(2).unwrap();
        assert_eq!(status.cluster_name, "cluster-b");
        assert_eq!(status.tasks_sent, 1);
        assert_eq!(status.last_sent_task_id, 5);
    }

    #[test]
    fn test_queue_full() {
        let transport = ReplicationTransport::new();
        transport.add_link("cluster-b", 2, "http://b:9090");
        
        // Fill the queue (max is 10_000)
        for i in 0..10_000 {
            assert!(transport.send_to_cluster(2, make_task(i, i as u64)));
        }
        // Next one should fail
        assert!(!transport.send_to_cluster(2, make_task(99999, 99999)));
    }
}
