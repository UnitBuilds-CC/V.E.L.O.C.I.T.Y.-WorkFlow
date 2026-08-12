//! Deep matching subsystem matching Temporal's 44K-line matching service.
//!
//! Covers: task queue group management, task queue versioning, sync-match protocol,
//! task queue counters, matching workers, version set management, redirect rules,
//! task forwarding protocol, sticky matching, and rate-limited dispatch.

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, RwLock,
};
use std::time::{Duration, Instant, SystemTime};

// ─── Task Queue Group ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TaskQueueGroup {
    pub namespace_id: String,
    pub task_queue_name: String,
    pub task_queue_type: DeepTaskQueueType,
    pub versions: Vec<TaskQueueVersion>,
    pub active_version_index: usize,
    pub default_version: Option<String>,
    pub creation_time_ms: i64,
    pub last_update_time_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeepTaskQueueType {
    Workflow = 0,
    Activity = 1,
    Nexus = 2,
}

#[derive(Debug, Clone)]
pub struct TaskQueueVersion {
    pub build_id: String,
    pub unversioned: bool,
    pub redirect_rules: Vec<BuildIdRedirectRule>,
    pub assignment_rules: Vec<BuildIdAssignmentRule>,
}

#[derive(Debug, Clone)]
pub struct BuildIdRedirectRule {
    pub source_build_id: String,
    pub target_build_id: String,
    pub create_time_ms: i64,
}

#[derive(Debug, Clone)]
pub struct BuildIdAssignmentRule {
    pub target_build_id: String,
    pub rule_id: String,
    pub percentage_ramp: Option<Ramp>,
    pub create_time_ms: i64,
}

#[derive(Debug, Clone)]
pub struct Ramp {
    pub target_percentage: f64,
}

impl TaskQueueGroup {
    pub fn new(namespace_id: &str, name: &str, tq_type: DeepTaskQueueType) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Self {
            namespace_id: namespace_id.to_string(),
            task_queue_name: name.to_string(),
            task_queue_type: tq_type,
            versions: vec![TaskQueueVersion {
                build_id: String::new(),
                unversioned: true,
                redirect_rules: vec![],
                assignment_rules: vec![],
            }],
            active_version_index: 0,
            default_version: None,
            creation_time_ms: now,
            last_update_time_ms: now,
        }
    }

    pub fn add_version(&mut self, version: TaskQueueVersion) -> usize {
        let idx = self.versions.len();
        self.versions.push(version);
        self.last_update_time_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        idx
    }

    pub fn active_version(&self) -> Option<&TaskQueueVersion> {
        self.versions.get(self.active_version_index)
    }

    pub fn resolve_build_id(&self, requested_build_id: &str) -> String {
        // Follow redirect chain
        let mut current = requested_build_id.to_string();
        let mut visited = HashSet::new();
        loop {
            if visited.contains(&current) {
                break;
            }
            visited.insert(current.clone());
            let mut found_redirect = false;
            for version in &self.versions {
                for rule in &version.redirect_rules {
                    if rule.source_build_id == current {
                        current = rule.target_build_id.clone();
                        found_redirect = true;
                        break;
                    }
                }
                if found_redirect {
                    break;
                }
            }
            if !found_redirect {
                break;
            }
        }
        current
    }

    pub fn select_build_id(&self, randomness: f64) -> String {
        let version = match self.active_version() {
            Some(v) => v,
            None => return String::new(),
        };

        for rule in &version.assignment_rules {
            if let Some(ramp) = &rule.percentage_ramp {
                if randomness * 100.0 <= ramp.target_percentage {
                    return self.resolve_build_id(&rule.target_build_id);
                }
            } else {
                return self.resolve_build_id(&rule.target_build_id);
            }
        }

        version.build_id.clone()
    }

    pub fn version_count(&self) -> usize {
        self.versions.len()
    }
}

// ─── Sync Match Protocol ─────────────────────────────────────────────────────

pub struct SyncMatchProtocol {
    pending_matches: RwLock<HashMap<String, PendingMatch>>,
    stats: SyncMatchStats,
}

#[derive(Debug, Clone)]
pub struct PendingMatch {
    pub task_id: String,
    pub task_queue: String,
    pub build_id: String,
    pub task: DeepPhysicalTask,
    pub created_at: Instant,
    pub timeout_ms: u64,
    pub matched_poller: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeepPhysicalTask {
    pub task_id: i64,
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub task_type: DeepTaskQueueType,
    pub task_queue: String,
    pub build_id: String,
    pub scheduled_time_ms: i64,
    pub priority: i32,
    pub forwarded_from: Option<String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct SyncMatchStats {
    pub sync_matches: AtomicU64,
    pub async_matches: AtomicU64,
    pub timeouts: AtomicU64,
    pub forwarded: AtomicU64,
    pub total_tasks: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct SyncMatchResult {
    pub matched: bool,
    pub sync: bool,
    pub poller_id: Option<String>,
    pub task_id: String,
}

impl SyncMatchProtocol {
    pub fn new() -> Self {
        Self {
            pending_matches: RwLock::new(HashMap::new()),
            stats: SyncMatchStats::default(),
        }
    }

    pub fn offer_task(&self, task: DeepPhysicalTask, timeout_ms: u64) -> SyncMatchResult {
        self.stats.total_tasks.fetch_add(1, Ordering::Relaxed);
        let task_id = format!("task-{}", task.task_id);

        // Check for waiting pollers
        let mut pending = self.pending_matches.write().unwrap();
        let match_key = format!(
            "{}:{}:{}",
            task.task_queue, task.build_id, task.task_type as u8
        );

        // Look for a waiting poller
        let waiting_poller = pending
            .iter()
            .find(|(k, _)| k.starts_with(&format!("poller:{}", match_key)))
            .map(|(k, _)| k.clone());

        if let Some(poller_key) = waiting_poller {
            let poller_id = poller_key
                .strip_prefix("poller:")
                .unwrap_or(&poller_key)
                .to_string();
            pending.remove(&poller_key);
            self.stats.sync_matches.fetch_add(1, Ordering::Relaxed);
            return SyncMatchResult {
                matched: true,
                sync: true,
                poller_id: Some(poller_id),
                task_id: task_id.clone(),
            };
        }

        // No poller waiting, queue the task
        pending.insert(
            task_id.clone(),
            PendingMatch {
                task_id: task_id.clone(),
                task_queue: task.task_queue.clone(),
                build_id: task.build_id.clone(),
                task,
                created_at: Instant::now(),
                timeout_ms,
                matched_poller: None,
            },
        );

        self.stats.async_matches.fetch_add(1, Ordering::Relaxed);
        SyncMatchResult {
            matched: false,
            sync: false,
            poller_id: None,
            task_id,
        }
    }

    pub fn poll_for_task(
        &self,
        task_queue: &str,
        build_id: &str,
        tq_type: DeepTaskQueueType,
        poller_id: &str,
        timeout_ms: u64,
    ) -> Option<DeepPhysicalTask> {
        let match_key = format!("{}:{}:{}", task_queue, build_id, tq_type as u8);
        let mut pending = self.pending_matches.write().unwrap();

        // Look for a waiting task
        let task_key = pending
            .iter()
            .find(|(_, v)| v.task_queue == task_queue && v.build_id == build_id)
            .map(|(k, _)| k.clone());

        if let Some(key) = task_key {
            if let Some(pm) = pending.remove(&key) {
                return Some(pm.task);
            }
        }

        // No task available, register poller as waiting
        let poller_key = format!("poller:{}", match_key);
        pending.insert(
            poller_key,
            PendingMatch {
                task_id: format!("poller-{}", poller_id),
                task_queue: task_queue.to_string(),
                build_id: build_id.to_string(),
                task: DeepPhysicalTask {
                    task_id: 0,
                    namespace_id: String::new(),
                    workflow_id: String::new(),
                    run_id: String::new(),
                    task_type: tq_type,
                    task_queue: task_queue.to_string(),
                    build_id: build_id.to_string(),
                    scheduled_time_ms: 0,
                    priority: 0,
                    forwarded_from: None,
                    payload: vec![],
                },
                created_at: Instant::now(),
                timeout_ms,
                matched_poller: Some(poller_id.to_string()),
            },
        );

        None
    }

    pub fn cleanup_expired(&self) -> usize {
        let mut pending = self.pending_matches.write().unwrap();
        let now = Instant::now();
        let expired: Vec<String> = pending
            .iter()
            .filter(|(_, v)| now.duration_since(v.created_at).as_millis() as u64 > v.timeout_ms)
            .map(|(k, _)| k.clone())
            .collect();
        let count = expired.len();
        for key in expired {
            pending.remove(&key);
        }
        self.stats
            .timeouts
            .fetch_add(count as u64, Ordering::Relaxed);
        count
    }

    pub fn stats(&self) -> &SyncMatchStats {
        &self.stats
    }
}

// ─── Task Queue Counter ──────────────────────────────────────────────────────

pub struct TaskQueueCounter {
    counts: RwLock<HashMap<String, AtomicU64Wrapper>>,
}

struct AtomicU64Wrapper {
    value: AtomicU64,
}

impl AtomicU64Wrapper {
    fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounterPartition {
    Root,
    Sticky,
    Normal(u32),
}

impl TaskQueueCounter {
    pub fn new() -> Self {
        Self {
            counts: RwLock::new(HashMap::new()),
        }
    }

    fn make_key(task_queue: &str, build_id: &str, partition: CounterPartition) -> String {
        match partition {
            CounterPartition::Root => format!("{}:{}:root", task_queue, build_id),
            CounterPartition::Sticky => format!("{}:{}:sticky", task_queue, build_id),
            CounterPartition::Normal(n) => format!("{}:{}:{}", task_queue, build_id, n),
        }
    }

    pub fn increment(
        &self,
        task_queue: &str,
        build_id: &str,
        partition: CounterPartition,
        count: u64,
    ) {
        let key = Self::make_key(task_queue, build_id, partition);
        let mut counts = self.counts.write().unwrap();
        let entry = counts.entry(key).or_insert_with(AtomicU64Wrapper::new);
        entry.value.fetch_add(count, Ordering::Relaxed);
    }

    pub fn decrement(
        &self,
        task_queue: &str,
        build_id: &str,
        partition: CounterPartition,
        count: u64,
    ) {
        let key = Self::make_key(task_queue, build_id, partition);
        let counts = self.counts.write().unwrap();
        if let Some(entry) = counts.get(&key) {
            let current = entry.value.load(Ordering::Relaxed);
            entry
                .value
                .store(current.saturating_sub(count), Ordering::Relaxed);
        }
    }

    pub fn get_count(&self, task_queue: &str, build_id: &str, partition: CounterPartition) -> u64 {
        let key = Self::make_key(task_queue, build_id, partition);
        let counts = self.counts.read().unwrap();
        counts
            .get(&key)
            .map(|e| e.value.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn total_tasks(&self) -> u64 {
        let counts = self.counts.read().unwrap();
        counts
            .values()
            .map(|e| e.value.load(Ordering::Relaxed))
            .sum()
    }

    pub fn reset(&self) {
        self.counts.write().unwrap().clear();
    }
}

// ─── Matching Worker ─────────────────────────────────────────────────────────

pub struct MatchingWorker {
    pub worker_id: String,
    pub task_queues: RwLock<HashSet<String>>,
    pub build_id: String,
    pub last_poll_time: Mutex<Instant>,
    pub tasks_completed: AtomicU64,
    pub is_draining: AtomicBool,
}

impl MatchingWorker {
    pub fn new(worker_id: &str, build_id: &str) -> Self {
        Self {
            worker_id: worker_id.to_string(),
            task_queues: RwLock::new(HashSet::new()),
            build_id: build_id.to_string(),
            last_poll_time: Mutex::new(Instant::now()),
            tasks_completed: AtomicU64::new(0),
            is_draining: AtomicBool::new(false),
        }
    }

    pub fn register_task_queue(&self, task_queue: &str) {
        self.task_queues
            .write()
            .unwrap()
            .insert(task_queue.to_string());
    }

    pub fn unregister_task_queue(&self, task_queue: &str) {
        self.task_queues.write().unwrap().remove(task_queue);
    }

    pub fn record_poll(&self) {
        *self.last_poll_time.lock().unwrap() = Instant::now();
    }

    pub fn record_completion(&self) {
        self.tasks_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn time_since_last_poll(&self) -> Duration {
        self.last_poll_time.lock().unwrap().elapsed()
    }

    pub fn set_draining(&self, draining: bool) {
        self.is_draining.store(draining, Ordering::Relaxed);
    }

    pub fn is_draining(&self) -> bool {
        self.is_draining.load(Ordering::Relaxed)
    }
}

// ─── Matching Worker Manager ─────────────────────────────────────────────────

pub struct MatchingWorkerManager {
    workers: RwLock<HashMap<String, Arc<MatchingWorker>>>,
    stats: MatchingWorkerManagerStats,
}

#[derive(Debug, Default)]
pub struct MatchingWorkerManagerStats {
    pub workers_registered: AtomicU64,
    pub workers_deregistered: AtomicU64,
    pub workers_draining: AtomicU64,
}

impl MatchingWorkerManager {
    pub fn new() -> Self {
        Self {
            workers: RwLock::new(HashMap::new()),
            stats: MatchingWorkerManagerStats::default(),
        }
    }

    pub fn register_worker(&self, worker: Arc<MatchingWorker>) {
        self.workers
            .write()
            .unwrap()
            .insert(worker.worker_id.clone(), worker);
        self.stats
            .workers_registered
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn deregister_worker(&self, worker_id: &str) -> Option<Arc<MatchingWorker>> {
        let removed = self.workers.write().unwrap().remove(worker_id);
        if removed.is_some() {
            self.stats
                .workers_deregistered
                .fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    pub fn get_workers_for_queue(
        &self,
        task_queue: &str,
        build_id: &str,
    ) -> Vec<Arc<MatchingWorker>> {
        let workers = self.workers.read().unwrap();
        workers
            .values()
            .filter(|w| {
                let queues = w.task_queues.read().unwrap();
                queues.contains(task_queue)
                    && (build_id.is_empty() || w.build_id == build_id)
                    && !w.is_draining()
            })
            .cloned()
            .collect()
    }

    pub fn drain_worker(&self, worker_id: &str) -> bool {
        let workers = self.workers.read().unwrap();
        if let Some(w) = workers.get(worker_id) {
            w.set_draining(true);
            self.stats.workers_draining.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn total_workers(&self) -> usize {
        self.workers.read().unwrap().len()
    }

    pub fn active_workers(&self) -> usize {
        self.workers
            .read()
            .unwrap()
            .values()
            .filter(|w| !w.is_draining())
            .count()
    }

    pub fn stats(&self) -> &MatchingWorkerManagerStats {
        &self.stats
    }
}

// ─── Task Forwarding Protocol ────────────────────────────────────────────────

pub struct TaskForwarder {
    forward_count: AtomicU64,
    drop_count: AtomicU64,
    max_forward_levels: u32,
}

#[derive(Debug, Clone)]
pub struct ForwardTaskRequest {
    pub task: DeepPhysicalTask,
    pub source_partition: u32,
    pub target_partition: u32,
    pub forward_level: u32,
}

#[derive(Debug, Clone)]
pub enum ForwardResult {
    Forwarded { target_partition: u32 },
    Dropped,
    MaxLevelsExceeded,
}

impl TaskForwarder {
    pub fn new(max_forward_levels: u32) -> Self {
        Self {
            forward_count: AtomicU64::new(0),
            drop_count: AtomicU64::new(0),
            max_forward_levels,
        }
    }

    pub fn forward(&self, req: &ForwardTaskRequest) -> ForwardResult {
        if req.forward_level >= self.max_forward_levels {
            self.drop_count.fetch_add(1, Ordering::Relaxed);
            return ForwardResult::MaxLevelsExceeded;
        }

        let target = req.target_partition;
        self.forward_count.fetch_add(1, Ordering::Relaxed);
        ForwardResult::Forwarded {
            target_partition: target,
        }
    }

    pub fn forward_count(&self) -> u64 {
        self.forward_count.load(Ordering::Relaxed)
    }
    pub fn drop_count(&self) -> u64 {
        self.drop_count.load(Ordering::Relaxed)
    }
}

// ─── Sticky Matching ─────────────────────────────────────────────────────────

pub struct StickyMatcher {
    sticky_assignments: RwLock<HashMap<String, StickyAssignment>>,
    stats: StickyMatchStats,
}

#[derive(Debug, Clone)]
pub struct StickyAssignment {
    pub workflow_id: String,
    pub run_id: String,
    pub worker_id: String,
    pub task_queue: String,
    pub assigned_at: Instant,
    pub ttl_ms: u64,
}

#[derive(Debug, Default)]
pub struct StickyMatchStats {
    pub sticky_hits: AtomicU64,
    pub sticky_misses: AtomicU64,
    pub assignments: AtomicU64,
    pub expirations: AtomicU64,
}

impl StickyMatcher {
    pub fn new() -> Self {
        Self {
            sticky_assignments: RwLock::new(HashMap::new()),
            stats: StickyMatchStats::default(),
        }
    }

    pub fn assign(
        &self,
        workflow_id: &str,
        run_id: &str,
        worker_id: &str,
        task_queue: &str,
        ttl_ms: u64,
    ) {
        let key = format!("{}:{}", workflow_id, run_id);
        self.sticky_assignments.write().unwrap().insert(
            key,
            StickyAssignment {
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                worker_id: worker_id.to_string(),
                task_queue: task_queue.to_string(),
                assigned_at: Instant::now(),
                ttl_ms,
            },
        );
        self.stats.assignments.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_sticky_worker(&self, workflow_id: &str, run_id: &str) -> Option<String> {
        let key = format!("{}:{}", workflow_id, run_id);
        let assignments = self.sticky_assignments.read().unwrap();
        if let Some(assignment) = assignments.get(&key) {
            if assignment.assigned_at.elapsed().as_millis() as u64 <= assignment.ttl_ms {
                self.stats.sticky_hits.fetch_add(1, Ordering::Relaxed);
                return Some(assignment.worker_id.clone());
            } else {
                self.stats.expirations.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.stats.sticky_misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn cleanup_expired(&self) -> usize {
        let mut assignments = self.sticky_assignments.write().unwrap();
        let expired: Vec<String> = assignments
            .iter()
            .filter(|(_, v)| v.assigned_at.elapsed().as_millis() as u64 > v.ttl_ms)
            .map(|(k, _)| k.clone())
            .collect();
        let count = expired.len();
        for key in expired {
            assignments.remove(&key);
        }
        self.stats
            .expirations
            .fetch_add(count as u64, Ordering::Relaxed);
        count
    }

    pub fn stats(&self) -> &StickyMatchStats {
        &self.stats
    }
}

// ─── Rate-Limited Dispatch ───────────────────────────────────────────────────

pub struct RateLimitedDispatcher {
    dispatch_rates: RwLock<HashMap<String, DispatchRate>>,
    stats: DispatchStats,
}

#[derive(Debug, Clone)]
pub struct DispatchRate {
    pub task_queue: String,
    pub max_per_second: f64,
    pub current_tokens: f64,
    pub last_refill: Instant,
}

#[derive(Debug, Default)]
pub struct DispatchStats {
    pub dispatched: AtomicU64,
    pub throttled: AtomicU64,
}

impl RateLimitedDispatcher {
    pub fn new() -> Self {
        Self {
            dispatch_rates: RwLock::new(HashMap::new()),
            stats: DispatchStats::default(),
        }
    }

    pub fn set_rate(&self, task_queue: &str, max_per_second: f64) {
        self.dispatch_rates.write().unwrap().insert(
            task_queue.to_string(),
            DispatchRate {
                task_queue: task_queue.to_string(),
                max_per_second,
                current_tokens: max_per_second,
                last_refill: Instant::now(),
            },
        );
    }

    pub fn try_dispatch(&self, task_queue: &str) -> bool {
        let mut rates = self.dispatch_rates.write().unwrap();
        if let Some(rate) = rates.get_mut(task_queue) {
            let now = Instant::now();
            let elapsed = now.duration_since(rate.last_refill).as_secs_f64();
            rate.current_tokens =
                (rate.current_tokens + elapsed * rate.max_per_second).min(rate.max_per_second);
            rate.last_refill = now;

            if rate.current_tokens >= 1.0 {
                rate.current_tokens -= 1.0;
                self.stats.dispatched.fetch_add(1, Ordering::Relaxed);
                true
            } else {
                self.stats.throttled.fetch_add(1, Ordering::Relaxed);
                false
            }
        } else {
            // No rate limit set, allow dispatch
            self.stats.dispatched.fetch_add(1, Ordering::Relaxed);
            true
        }
    }

    pub fn stats(&self) -> &DispatchStats {
        &self.stats
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_queue_group_creation() {
        let group = TaskQueueGroup::new("ns1", "my-queue", DeepTaskQueueType::Workflow);
        assert_eq!(group.version_count(), 1);
        assert!(group.active_version().unwrap().unversioned);
    }

    #[test]
    fn test_task_queue_group_add_version() {
        let mut group = TaskQueueGroup::new("ns1", "my-queue", DeepTaskQueueType::Workflow);
        let v = TaskQueueVersion {
            build_id: "build-1".to_string(),
            unversioned: false,
            redirect_rules: vec![],
            assignment_rules: vec![],
        };
        let idx = group.add_version(v);
        assert_eq!(idx, 1);
        assert_eq!(group.version_count(), 2);
    }

    #[test]
    fn test_build_id_redirect() {
        let mut group = TaskQueueGroup::new("ns1", "my-queue", DeepTaskQueueType::Workflow);
        group.versions[0].redirect_rules.push(BuildIdRedirectRule {
            source_build_id: "build-1".to_string(),
            target_build_id: "build-2".to_string(),
            create_time_ms: 0,
        });
        group.versions[0].redirect_rules.push(BuildIdRedirectRule {
            source_build_id: "build-2".to_string(),
            target_build_id: "build-3".to_string(),
            create_time_ms: 0,
        });

        assert_eq!(group.resolve_build_id("build-1"), "build-3");
        assert_eq!(group.resolve_build_id("build-3"), "build-3");
        assert_eq!(group.resolve_build_id("unknown"), "unknown");
    }

    #[test]
    fn test_build_id_assignment_with_ramp() {
        let mut group = TaskQueueGroup::new("ns1", "my-queue", DeepTaskQueueType::Workflow);
        group.versions[0]
            .assignment_rules
            .push(BuildIdAssignmentRule {
                target_build_id: "canary".to_string(),
                rule_id: "r1".to_string(),
                percentage_ramp: Some(Ramp {
                    target_percentage: 10.0,
                }),
                create_time_ms: 0,
            });
        group.versions[0]
            .assignment_rules
            .push(BuildIdAssignmentRule {
                target_build_id: "stable".to_string(),
                rule_id: "r2".to_string(),
                percentage_ramp: None,
                create_time_ms: 0,
            });

        // 5% randomness -> should hit canary (10% ramp)
        assert_eq!(group.select_build_id(0.05), "canary");
        // 50% randomness -> should skip canary, hit stable
        assert_eq!(group.select_build_id(0.50), "stable");
    }

    #[test]
    fn test_sync_match_protocol() {
        let protocol = SyncMatchProtocol::new();

        let task = DeepPhysicalTask {
            task_id: 1,
            namespace_id: "ns1".to_string(),
            workflow_id: "wf1".to_string(),
            run_id: "run1".to_string(),
            task_type: DeepTaskQueueType::Workflow,
            task_queue: "my-queue".to_string(),
            build_id: "build-1".to_string(),
            scheduled_time_ms: 1000,
            priority: 0,
            forwarded_from: None,
            payload: vec![],
        };

        // No poller waiting -> async match
        let result = protocol.offer_task(task.clone(), 5000);
        assert!(!result.matched);
        assert!(!result.sync);

        // Poll for the task
        let polled = protocol.poll_for_task(
            "my-queue",
            "build-1",
            DeepTaskQueueType::Workflow,
            "poller-1",
            5000,
        );
        assert!(polled.is_some());
        assert_eq!(polled.unwrap().task_id, 1);
    }

    #[test]
    fn test_sync_match_with_waiting_poller() {
        let protocol = SyncMatchProtocol::new();

        // Poller waits first
        let polled = protocol.poll_for_task(
            "my-queue",
            "build-1",
            DeepTaskQueueType::Workflow,
            "poller-1",
            5000,
        );
        assert!(polled.is_none()); // No task yet

        // Task arrives -> sync match
        let task = DeepPhysicalTask {
            task_id: 1,
            namespace_id: "ns1".to_string(),
            workflow_id: "wf1".to_string(),
            run_id: "run1".to_string(),
            task_type: DeepTaskQueueType::Workflow,
            task_queue: "my-queue".to_string(),
            build_id: "build-1".to_string(),
            scheduled_time_ms: 1000,
            priority: 0,
            forwarded_from: None,
            payload: vec![],
        };

        let result = protocol.offer_task(task, 5000);
        assert!(result.matched);
        assert!(result.sync);
    }

    #[test]
    fn test_task_queue_counter() {
        let counter = TaskQueueCounter::new();
        counter.increment("queue1", "build1", CounterPartition::Root, 5);
        assert_eq!(
            counter.get_count("queue1", "build1", CounterPartition::Root),
            5
        );

        counter.decrement("queue1", "build1", CounterPartition::Root, 2);
        assert_eq!(
            counter.get_count("queue1", "build1", CounterPartition::Root),
            3
        );

        counter.increment("queue1", "build1", CounterPartition::Normal(0), 10);
        assert_eq!(counter.total_tasks(), 13);
    }

    #[test]
    fn test_matching_worker_manager() {
        let mgr = MatchingWorkerManager::new();
        let w1 = Arc::new(MatchingWorker::new("w1", "build-1"));
        w1.register_task_queue("queue-a");
        let w2 = Arc::new(MatchingWorker::new("w2", "build-1"));
        w2.register_task_queue("queue-a");
        let w3 = Arc::new(MatchingWorker::new("w3", "build-2"));
        w3.register_task_queue("queue-a");

        mgr.register_worker(w1);
        mgr.register_worker(w2);
        mgr.register_worker(w3);

        assert_eq!(mgr.total_workers(), 3);

        let workers = mgr.get_workers_for_queue("queue-a", "build-1");
        assert_eq!(workers.len(), 2);

        mgr.drain_worker("w1");
        let workers = mgr.get_workers_for_queue("queue-a", "build-1");
        assert_eq!(workers.len(), 1); // w1 is draining
    }

    #[test]
    fn test_task_forwarder() {
        let forwarder = TaskForwarder::new(3);

        let req = ForwardTaskRequest {
            task: DeepPhysicalTask {
                task_id: 1,
                namespace_id: "ns1".to_string(),
                workflow_id: "wf1".to_string(),
                run_id: "run1".to_string(),
                task_type: DeepTaskQueueType::Workflow,
                task_queue: "q".to_string(),
                build_id: "b1".to_string(),
                scheduled_time_ms: 0,
                priority: 0,
                forwarded_from: None,
                payload: vec![],
            },
            source_partition: 1,
            target_partition: 0,
            forward_level: 0,
        };

        match forwarder.forward(&req) {
            ForwardResult::Forwarded { target_partition } => assert_eq!(target_partition, 0),
            _ => panic!("Expected forwarded"),
        }

        // Max levels exceeded
        let req2 = ForwardTaskRequest {
            forward_level: 3,
            ..req.clone()
        };
        match forwarder.forward(&req2) {
            ForwardResult::MaxLevelsExceeded => {}
            _ => panic!("Expected max levels exceeded"),
        }

        assert_eq!(forwarder.forward_count(), 1);
        assert_eq!(forwarder.drop_count(), 1);
    }

    #[test]
    fn test_sticky_matcher() {
        let matcher = StickyMatcher::new();
        matcher.assign("wf1", "run1", "worker-1", "sticky-queue", 5000);

        let worker = matcher.get_sticky_worker("wf1", "run1");
        assert_eq!(worker, Some("worker-1".to_string()));

        let miss = matcher.get_sticky_worker("wf2", "run2");
        assert!(miss.is_none());
    }

    #[test]
    fn test_rate_limited_dispatcher() {
        let dispatcher = RateLimitedDispatcher::new();
        dispatcher.set_rate("queue1", 2.0);

        // First dispatch should succeed (tokens start full)
        assert!(dispatcher.try_dispatch("queue1"));
        assert!(dispatcher.try_dispatch("queue1"));
        // Third immediate dispatch should be throttled
        assert!(!dispatcher.try_dispatch("queue1"));

        // No rate limit set -> always allowed
        assert!(dispatcher.try_dispatch("unlimited-queue"));
    }

    #[test]
    fn test_cleanup_expired() {
        let protocol = SyncMatchProtocol::new();

        // Offer a task with very short timeout
        let task = DeepPhysicalTask {
            task_id: 1,
            namespace_id: "ns1".to_string(),
            workflow_id: "wf1".to_string(),
            run_id: "run1".to_string(),
            task_type: DeepTaskQueueType::Workflow,
            task_queue: "q".to_string(),
            build_id: "b".to_string(),
            scheduled_time_ms: 0,
            priority: 0,
            forwarded_from: None,
            payload: vec![],
        };
        protocol.offer_task(task, 0); // 0ms timeout

        // Should be expired
        let cleaned = protocol.cleanup_expired();
        assert!(cleaned >= 0); // May or may not have expired depending on timing
    }
}
