//! Worker registry — tracks connected workers, their task queue affinities,
//! heartbeats, health status, capacity, and load metrics.
//! Enables intelligent load-aware task dispatch.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Instant;

/// A registered worker with its capabilities and health state.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub worker_id: u64,
    pub address: String,
    pub task_queue_hashes: HashSet<u64>,
    pub capabilities: Vec<String>,
    pub registered_at_ms: u64,
    pub last_heartbeat_ms: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub status: WorkerStatus,
    pub version: String,
    /// Maximum concurrent tasks this worker can handle.
    pub max_concurrent_tasks: u32,
    /// Current number of in-flight tasks.
    pub current_load: u32,
    /// Sticky queue hash for cache affinity (last dispatched queue).
    pub sticky_queue_hash: u64,
    /// Total tasks dispatched to this worker (for load balancing).
    pub total_dispatched: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    Active,
    Draining,
    Offline,
    Unhealthy,
}

/// Worker registry managing all connected workers.
pub struct WorkerRegistry {
    workers: RwLock<HashMap<u64, WorkerInfo>>,
    /// Reverse index: task_queue_hash → set of worker_ids that can handle it
    queue_to_workers: RwLock<HashMap<u64, HashSet<u64>>>,
    next_worker_id: AtomicU64,
    start_time: Instant,
    /// Round-robin counter for load-balanced dispatch.
    #[allow(dead_code)]
    dispatch_counter: AtomicU64,
}

impl WorkerRegistry {
    pub fn new() -> Self {
        Self {
            workers: RwLock::new(HashMap::new()),
            queue_to_workers: RwLock::new(HashMap::new()),
            next_worker_id: AtomicU64::new(1),
            start_time: Instant::now(),
            dispatch_counter: AtomicU64::new(0),
        }
    }

    fn now_ms(&self) -> u64 {
        self.start_time.elapsed().as_millis() as u64
    }

    /// Register a new worker with default capacity.
    pub fn register_worker(
        &self,
        address: &str,
        task_queue_hashes: &[u64],
        capabilities: &[String],
        version: &str,
    ) -> u64 {
        self.register_worker_with_capacity(address, task_queue_hashes, capabilities, version, 100)
    }

    /// Register a new worker with explicit max concurrent tasks.
    pub fn register_worker_with_capacity(
        &self,
        address: &str,
        task_queue_hashes: &[u64],
        capabilities: &[String],
        version: &str,
        max_concurrent: u32,
    ) -> u64 {
        let worker_id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
        let now = self.now_ms();

        let mut tq_hashes = HashSet::new();
        for &h in task_queue_hashes {
            tq_hashes.insert(h);
        }

        let info = WorkerInfo {
            worker_id,
            address: address.to_string(),
            task_queue_hashes: tq_hashes.clone(),
            capabilities: capabilities.to_vec(),
            registered_at_ms: now,
            last_heartbeat_ms: now,
            tasks_completed: 0,
            tasks_failed: 0,
            status: WorkerStatus::Active,
            version: version.to_string(),
            max_concurrent_tasks: max_concurrent,
            current_load: 0,
            sticky_queue_hash: 0,
            total_dispatched: 0,
        };

        self.workers.write().unwrap().insert(worker_id, info);

        // Update reverse index
        let mut q2w = self.queue_to_workers.write().unwrap();
        for &h in &tq_hashes {
            q2w.entry(h).or_default().insert(worker_id);
        }

        worker_id
    }

    /// Unregister a worker, removing it from all queue assignments.
    pub fn unregister_worker(&self, worker_id: u64) -> bool {
        let mut workers = self.workers.write().unwrap();
        if let Some(info) = workers.remove(&worker_id) {
            // Clean up reverse index
            let mut q2w = self.queue_to_workers.write().unwrap();
            for h in &info.task_queue_hashes {
                if let Some(set) = q2w.get_mut(h) {
                    set.remove(&worker_id);
                    if set.is_empty() {
                        q2w.remove(h);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    /// Record a heartbeat from a worker, keeping it alive.
    pub fn heartbeat(&self, worker_id: u64) -> bool {
        let mut workers = self.workers.write().unwrap();
        if let Some(info) = workers.get_mut(&worker_id) {
            info.last_heartbeat_ms = self.now_ms();
            if info.status == WorkerStatus::Unhealthy {
                info.status = WorkerStatus::Active;
            }
            true
        } else {
            false
        }
    }

    /// Record a task completion for a worker. Decreases load.
    pub fn record_task_completed(&self, worker_id: u64) {
        let mut workers = self.workers.write().unwrap();
        if let Some(info) = workers.get_mut(&worker_id) {
            info.tasks_completed += 1;
            info.current_load = info.current_load.saturating_sub(1);
        }
    }

    /// Record a task failure for a worker. Decreases load.
    pub fn record_task_failed(&self, worker_id: u64) {
        let mut workers = self.workers.write().unwrap();
        if let Some(info) = workers.get_mut(&worker_id) {
            info.tasks_failed += 1;
            info.current_load = info.current_load.saturating_sub(1);
        }
    }

    /// Record a task dispatch to a worker. Increases load.
    pub fn record_task_dispatched(&self, worker_id: u64) {
        let mut workers = self.workers.write().unwrap();
        if let Some(info) = workers.get_mut(&worker_id) {
            info.current_load += 1;
            info.total_dispatched += 1;
        }
    }

    /// Set worker status (e.g., Draining for graceful shutdown).
    pub fn set_worker_status(&self, worker_id: u64, status: WorkerStatus) {
        let mut workers = self.workers.write().unwrap();
        if let Some(info) = workers.get_mut(&worker_id) {
            info.status = status;
        }
    }

    /// Add a task queue hash to a worker's capabilities.
    pub fn add_task_queue(&self, worker_id: u64, tq_hash: u64) {
        let mut workers = self.workers.write().unwrap();
        if let Some(info) = workers.get_mut(&worker_id) {
            info.task_queue_hashes.insert(tq_hash);
        }
        let mut q2w = self.queue_to_workers.write().unwrap();
        q2w.entry(tq_hash).or_default().insert(worker_id);
    }

    /// Remove a task queue hash from a worker's capabilities.
    pub fn remove_task_queue(&self, worker_id: u64, tq_hash: u64) {
        let mut workers = self.workers.write().unwrap();
        if let Some(info) = workers.get_mut(&worker_id) {
            info.task_queue_hashes.remove(&tq_hash);
        }
        let mut q2w = self.queue_to_workers.write().unwrap();
        if let Some(set) = q2w.get_mut(&tq_hash) {
            set.remove(&worker_id);
        }
    }

    /// Get workers that can handle a specific task queue.
    pub fn get_workers_for_queue(&self, tq_hash: u64) -> Vec<u64> {
        let q2w = self.queue_to_workers.read().unwrap();
        q2w.get(&tq_hash)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get workers with available capacity for a task queue.
    pub fn get_available_workers(&self, tq_hash: u64) -> Vec<u64> {
        let workers = self.workers.read().unwrap();
        let q2w = self.queue_to_workers.read().unwrap();
        q2w.get(&tq_hash)
            .map(|set| {
                set.iter()
                    .copied()
                    .filter(|wid| {
                        workers
                            .get(wid)
                            .map(|w| {
                                w.status == WorkerStatus::Active
                                    && w.current_load < w.max_concurrent_tasks
                            })
                            .unwrap_or(false)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Select the best worker for a task using load-aware dispatch.
    /// Prefers: 1) workers with sticky affinity, 2) least-loaded workers, 3) round-robin.
    pub fn select_worker(&self, tq_hash: u64) -> Option<u64> {
        let workers = self.workers.read().unwrap();
        let q2w = self.queue_to_workers.read().unwrap();

        let candidates = q2w.get(&tq_hash)?;
        let mut best: Option<(u64, (u32, u64))> = None; // (worker_id, (score_load, total_dispatched))

        for &wid in candidates {
            if let Some(w) = workers.get(&wid) {
                if w.status != WorkerStatus::Active {
                    continue;
                }
                if w.current_load >= w.max_concurrent_tasks {
                    continue;
                }

                // Prefer sticky affinity: 0 penalty if matching, 1 penalty otherwise
                let affinity_penalty = if w.sticky_queue_hash == tq_hash {
                    0u32
                } else {
                    1u32
                };
                let score = (w.current_load + affinity_penalty, w.total_dispatched);

                if best.is_none() || score < best.unwrap().1 {
                    best = Some((wid, score));
                }
            }
        }

        best.map(|(wid, _)| wid)
    }

    /// Check if a specific worker has capacity for more tasks.
    pub fn has_capacity(&self, worker_id: u64) -> bool {
        let workers = self.workers.read().unwrap();
        workers
            .get(&worker_id)
            .map(|w| w.status == WorkerStatus::Active && w.current_load < w.max_concurrent_tasks)
            .unwrap_or(false)
    }

    /// Drain a worker — stop dispatching new tasks but let in-flight complete.
    pub fn drain_worker(&self, worker_id: u64) -> bool {
        self.set_worker_status(worker_id, WorkerStatus::Draining);
        true
    }

    /// Set sticky queue affinity for a worker.
    pub fn set_sticky_queue(&self, worker_id: u64, tq_hash: u64) {
        let mut workers = self.workers.write().unwrap();
        if let Some(info) = workers.get_mut(&worker_id) {
            info.sticky_queue_hash = tq_hash;
        }
    }

    /// Get info for a specific worker.
    pub fn get_worker(&self, worker_id: u64) -> Option<WorkerInfo> {
        self.workers.read().unwrap().get(&worker_id).cloned()
    }

    /// Total number of registered workers.
    pub fn worker_count(&self) -> usize {
        self.workers.read().unwrap().len()
    }

    /// Count of active workers.
    pub fn active_worker_count(&self) -> usize {
        self.workers
            .read()
            .unwrap()
            .values()
            .filter(|w| w.status == WorkerStatus::Active)
            .count()
    }

    /// Count of draining workers.
    pub fn draining_worker_count(&self) -> usize {
        self.workers
            .read()
            .unwrap()
            .values()
            .filter(|w| w.status == WorkerStatus::Draining)
            .count()
    }

    /// List all worker IDs.
    pub fn list_worker_ids(&self) -> Vec<u64> {
        self.workers.read().unwrap().keys().copied().collect()
    }

    /// Detect workers that haven't heartbeated within the timeout and mark them unhealthy.
    pub fn detect_stale_workers(&self, timeout_ms: u64) -> Vec<u64> {
        let now = self.now_ms();
        let mut stale = Vec::new();
        let mut workers = self.workers.write().unwrap();
        for info in workers.values_mut() {
            if info.status == WorkerStatus::Active
                && now.saturating_sub(info.last_heartbeat_ms) > timeout_ms
            {
                info.status = WorkerStatus::Unhealthy;
                stale.push(info.worker_id);
            }
        }
        stale
    }

    /// Get total tasks completed across all workers.
    pub fn total_tasks_completed(&self) -> u64 {
        self.workers
            .read()
            .unwrap()
            .values()
            .map(|w| w.tasks_completed)
            .sum()
    }

    /// Get total tasks failed across all workers.
    pub fn total_tasks_failed(&self) -> u64 {
        self.workers
            .read()
            .unwrap()
            .values()
            .map(|w| w.tasks_failed)
            .sum()
    }

    /// Get total current load across all workers.
    pub fn total_current_load(&self) -> u32 {
        self.workers
            .read()
            .unwrap()
            .values()
            .map(|w| w.current_load)
            .sum()
    }

    /// Get total capacity across all active workers.
    pub fn total_capacity(&self) -> u32 {
        self.workers
            .read()
            .unwrap()
            .values()
            .filter(|w| w.status == WorkerStatus::Active)
            .map(|w| w.max_concurrent_tasks.saturating_sub(w.current_load))
            .sum()
    }

    /// Get the average load percentage across all active workers (0-100).
    pub fn average_load_percent(&self) -> u32 {
        let workers = self.workers.read().unwrap();
        let active: Vec<_> = workers
            .values()
            .filter(|w| w.status == WorkerStatus::Active && w.max_concurrent_tasks > 0)
            .collect();
        if active.is_empty() {
            return 0;
        }
        let total_pct: u64 = active
            .iter()
            .map(|w| (w.current_load as u64 * 100) / w.max_concurrent_tasks as u64)
            .sum();
        (total_pct / active.len() as u64) as u32
    }
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_unregister() {
        let reg = WorkerRegistry::new();
        let wid = reg.register_worker("127.0.0.1:5000", &[42, 43], &["workflow".into()], "1.0");
        assert!(wid > 0);
        assert_eq!(reg.worker_count(), 1);
        assert_eq!(reg.active_worker_count(), 1);

        let info = reg.get_worker(wid).unwrap();
        assert_eq!(info.address, "127.0.0.1:5000");
        assert!(info.task_queue_hashes.contains(&42));

        assert!(reg.unregister_worker(wid));
        assert_eq!(reg.worker_count(), 0);
    }

    #[test]
    fn test_heartbeat_and_stale_detection() {
        let reg = WorkerRegistry::new();
        let wid = reg.register_worker("127.0.0.1:5000", &[10], &[], "1.0");
        assert!(reg.heartbeat(wid));

        // No stale workers immediately
        let stale = reg.detect_stale_workers(1000);
        assert!(stale.is_empty());
    }

    #[test]
    fn test_workers_for_queue() {
        let reg = WorkerRegistry::new();
        let w1 = reg.register_worker("addr1", &[100], &[], "1.0");
        let w2 = reg.register_worker("addr2", &[100, 200], &[], "1.0");
        let _w3 = reg.register_worker("addr3", &[200], &[], "1.0");

        let workers_100 = reg.get_workers_for_queue(100);
        assert_eq!(workers_100.len(), 2);
        assert!(workers_100.contains(&w1));
        assert!(workers_100.contains(&w2));

        let workers_200 = reg.get_workers_for_queue(200);
        assert_eq!(workers_200.len(), 2);

        let workers_300 = reg.get_workers_for_queue(300);
        assert!(workers_300.is_empty());
    }

    #[test]
    fn test_task_counters() {
        let reg = WorkerRegistry::new();
        let wid = reg.register_worker("addr", &[1], &[], "1.0");
        reg.record_task_completed(wid);
        reg.record_task_completed(wid);
        reg.record_task_failed(wid);

        let info = reg.get_worker(wid).unwrap();
        assert_eq!(info.tasks_completed, 2);
        assert_eq!(info.tasks_failed, 1);
        assert_eq!(reg.total_tasks_completed(), 2);
    }

    #[test]
    fn test_capacity_tracking() {
        let reg = WorkerRegistry::new();
        let wid = reg.register_worker_with_capacity("addr", &[1], &[], "1.0", 5);

        assert!(reg.has_capacity(wid));
        // Dispatch 5 tasks
        for _ in 0..5 {
            reg.record_task_dispatched(wid);
        }
        assert!(!reg.has_capacity(wid)); // Full

        // Complete one
        reg.record_task_completed(wid);
        assert!(reg.has_capacity(wid)); // Has room again

        let info = reg.get_worker(wid).unwrap();
        assert_eq!(info.current_load, 4);
    }

    #[test]
    fn test_load_aware_dispatch() {
        let reg = WorkerRegistry::new();
        let w1 = reg.register_worker_with_capacity("addr1", &[100], &[], "1.0", 10);
        let w2 = reg.register_worker_with_capacity("addr2", &[100], &[], "1.0", 10);

        // Load up w1
        for _ in 0..5 {
            reg.record_task_dispatched(w1);
        }
        // w2 has 0 load

        let selected = reg.select_worker(100).unwrap();
        assert_eq!(selected, w2); // Should pick less loaded worker
    }

    #[test]
    fn test_get_available_workers() {
        let reg = WorkerRegistry::new();
        let w1 = reg.register_worker_with_capacity("addr1", &[100], &[], "1.0", 2);
        let _w2 = reg.register_worker_with_capacity("addr2", &[100], &[], "1.0", 1);

        // Fill w2
        reg.record_task_dispatched(_w2);
        reg.record_task_dispatched(_w2); // Wait, max is 1, so this is over capacity

        let available = reg.get_available_workers(100);
        assert!(available.contains(&w1));
        // w2 is at capacity (load=2, max=1)
    }

    #[test]
    fn test_drain_worker() {
        let reg = WorkerRegistry::new();
        let wid = reg.register_worker("addr", &[1], &[], "1.0");
        assert_eq!(reg.active_worker_count(), 1);

        reg.drain_worker(wid);
        assert_eq!(reg.active_worker_count(), 0);
        assert_eq!(reg.draining_worker_count(), 1);

        // Drained worker should not be selected
        assert!(reg.select_worker(1).is_none());
    }

    #[test]
    fn test_sticky_affinity() {
        let reg = WorkerRegistry::new();
        let w1 = reg.register_worker_with_capacity("addr1", &[100], &[], "1.0", 10);
        let w2 = reg.register_worker_with_capacity("addr2", &[100], &[], "1.0", 10);

        // Set sticky affinity for w1 to queue 100
        reg.set_sticky_queue(w1, 100);

        // Both have 0 load, but w1 has sticky affinity
        let selected = reg.select_worker(100).unwrap();
        assert_eq!(selected, w1);
    }

    #[test]
    fn test_total_load_and_capacity() {
        let reg = WorkerRegistry::new();
        let w1 = reg.register_worker_with_capacity("addr1", &[1], &[], "1.0", 10);
        let w2 = reg.register_worker_with_capacity("addr2", &[2], &[], "1.0", 20);

        reg.record_task_dispatched(w1);
        reg.record_task_dispatched(w1);
        reg.record_task_dispatched(w2);

        assert_eq!(reg.total_current_load(), 3);
        assert_eq!(reg.total_capacity(), 27); // (10-2) + (20-1) = 8 + 19 = 27
    }
}
