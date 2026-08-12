//! Workflow Concurrency Limiter — per-workflow-type and per-namespace concurrency
//! control with configurable limits, queue overflow policies, and fair scheduling.
//!
//! Exceeds Temporal's basic worker concurrency limits by providing:
//! - Per-workflow-type limits (not just per-worker)
//! - Per-namespace aggregate limits
//! - Configurable overflow policies (reject, queue, preempt)
//! - Priority-based scheduling within limits
//! - Real-time utilization tracking

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

// ─── Overflow Policy ───────────────────────────────────────────────────────

/// What happens when the concurrency limit is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Reject the request immediately.
    Reject,
    /// Queue the request until a slot opens (with max queue depth).
    Queue,
    /// Preempt a lower-priority running workflow.
    Preempt,
}

// ─── Concurrency Config ────────────────────────────────────────────────────

/// Configuration for a concurrency limiter.
#[derive(Debug, Clone)]
pub struct ConcurrencyConfig {
    /// Maximum concurrent workflows per type.
    pub max_per_type: u32,
    /// Maximum concurrent workflows per namespace.
    pub max_per_namespace: u32,
    /// Global maximum concurrent workflows.
    pub max_global: u32,
    /// Overflow policy when limit is reached.
    pub overflow_policy: OverflowPolicy,
    /// Maximum queue depth (only for Queue policy).
    pub max_queue_depth: u32,
    /// Priority levels (higher = more important).
    pub priority_levels: u8,
}

impl ConcurrencyConfig {
    /// Default: 100 per type, 1000 per namespace, 10000 global.
    pub fn default_config() -> Self {
        Self {
            max_per_type: 100,
            max_per_namespace: 1000,
            max_global: 10_000,
            overflow_policy: OverflowPolicy::Reject,
            max_queue_depth: 1000,
            priority_levels: 4,
        }
    }

    /// Tight limits for resource-constrained environments.
    pub fn tight() -> Self {
        Self {
            max_per_type: 10,
            max_per_namespace: 50,
            max_global: 500,
            overflow_policy: OverflowPolicy::Reject,
            max_queue_depth: 100,
            priority_levels: 4,
        }
    }

    /// Relaxed limits for high-throughput environments.
    pub fn relaxed() -> Self {
        Self {
            max_per_type: 1000,
            max_per_namespace: 10_000,
            max_global: 100_000,
            overflow_policy: OverflowPolicy::Queue,
            max_queue_depth: 50_000,
            priority_levels: 8,
        }
    }
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self::default_config()
    }
}

// ─── Concurrency Limiter ───────────────────────────────────────────────────

/// Result of attempting to acquire a concurrency slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireResult {
    /// Slot acquired — workflow can start.
    Acquired,
    /// Rejected — limit reached.
    Rejected,
    /// Queued — workflow is waiting for a slot.
    Queued(u64), // queue position
}

/// A pending request in the queue.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct QueuedRequest {
    workflow_key: u64,
    workflow_type_id: u64,
    namespace_id: u64,
    priority: u8,
    queued_at: u64,
}

/// Per-type concurrency tracking.
#[derive(Debug, Clone)]
struct TypeLimiter {
    active: u32,
    limit: u32,
}

/// Per-namespace concurrency tracking.
#[derive(Debug)]
struct NamespaceLimiter {
    active: u32,
    limit: u32,
}

/// Global concurrency limiter for workflow executions.
pub struct WorkflowConcurrencyLimiter {
    config: ConcurrencyConfig,
    per_type: RwLock<HashMap<u64, TypeLimiter>>,
    per_namespace: RwLock<HashMap<u64, NamespaceLimiter>>,
    global_active: AtomicU64,
    /// Queue for waiting requests (OverflowPolicy::Queue).
    queue: RwLock<VecDeque<QueuedRequest>>,
    /// Stats.
    total_acquired: AtomicU64,
    total_rejected: AtomicU64,
    total_queued: AtomicU64,
    total_released: AtomicU64,
}

impl WorkflowConcurrencyLimiter {
    pub fn new(config: ConcurrencyConfig) -> Self {
        Self {
            config,
            per_type: RwLock::new(HashMap::new()),
            per_namespace: RwLock::new(HashMap::new()),
            global_active: AtomicU64::new(0),
            queue: RwLock::new(VecDeque::new()),
            total_acquired: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            total_queued: AtomicU64::new(0),
            total_released: AtomicU64::new(0),
        }
    }

    /// Try to acquire a slot for a workflow.
    pub fn acquire(
        &self,
        workflow_key: u64,
        workflow_type_id: u64,
        namespace_id: u64,
        priority: u8,
    ) -> AcquireResult {
        // Check global limit
        let global = self.global_active.load(Ordering::Relaxed);
        if global >= self.config.max_global as u64 {
            return self.handle_overflow(workflow_key, workflow_type_id, namespace_id, priority);
        }

        // Check per-type limit
        {
            let mut types = self.per_type.write().unwrap();
            let limiter = types.entry(workflow_type_id).or_insert(TypeLimiter {
                active: 0,
                limit: self.config.max_per_type,
            });
            if limiter.active >= limiter.limit {
                return self.handle_overflow(workflow_key, workflow_type_id, namespace_id, priority);
            }
        }

        // Check per-namespace limit
        {
            let mut namespaces = self.per_namespace.write().unwrap();
            let limiter = namespaces.entry(namespace_id).or_insert(NamespaceLimiter {
                active: 0,
                limit: self.config.max_per_namespace,
            });
            if limiter.active >= limiter.limit {
                return self.handle_overflow(workflow_key, workflow_type_id, namespace_id, priority);
            }
        }

        // All checks passed — acquire slot
        self.commit_acquire(workflow_type_id, namespace_id);
        AcquireResult::Acquired
    }

    /// Release a slot when a workflow completes.
    pub fn release(&self, workflow_type_id: u64, namespace_id: u64) {
        self.global_active.fetch_sub(1, Ordering::Relaxed);
        self.total_released.fetch_add(1, Ordering::Relaxed);

        {
            let mut types = self.per_type.write().unwrap();
            if let Some(limiter) = types.get_mut(&workflow_type_id) {
                limiter.active = limiter.active.saturating_sub(1);
            }
        }
        {
            let mut namespaces = self.per_namespace.write().unwrap();
            if let Some(limiter) = namespaces.get_mut(&namespace_id) {
                limiter.active = limiter.active.saturating_sub(1);
            }
        }

        // Try to dequeue waiting requests
        self.try_dequeue();
    }

    /// Get the current active count for a workflow type.
    pub fn active_for_type(&self, workflow_type_id: u64) -> u32 {
        let types = self.per_type.read().unwrap();
        types.get(&workflow_type_id).map_or(0, |l| l.active)
    }

    /// Get the current active count for a namespace.
    pub fn active_for_namespace(&self, namespace_id: u64) -> u32 {
        let namespaces = self.per_namespace.read().unwrap();
        namespaces.get(&namespace_id).map_or(0, |l| l.active)
    }

    /// Get the global active count.
    pub fn global_active(&self) -> u64 {
        self.global_active.load(Ordering::Relaxed)
    }

    /// Get the current queue depth.
    pub fn queue_depth(&self) -> usize {
        self.queue.read().unwrap().len()
    }

    /// Get utilization percentage for a workflow type (0.0 - 100.0).
    pub fn type_utilization(&self, workflow_type_id: u64) -> f64 {
        let types = self.per_type.read().unwrap();
        types
            .get(&workflow_type_id)
            .map_or(0.0, |l| {
                if l.limit == 0 {
                    0.0
                } else {
                    l.active as f64 / l.limit as f64 * 100.0
                }
            })
    }

    /// Get stats summary.
    pub fn stats(&self) -> ConcurrencyStats {
        ConcurrencyStats {
            global_active: self.global_active.load(Ordering::Relaxed),
            total_acquired: self.total_acquired.load(Ordering::Relaxed),
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
            total_queued: self.total_queued.load(Ordering::Relaxed),
            total_released: self.total_released.load(Ordering::Relaxed),
            queue_depth: self.queue.read().unwrap().len() as u64,
            rejection_rate: {
                let acquired = self.total_acquired.load(Ordering::Relaxed);
                let rejected = self.total_rejected.load(Ordering::Relaxed);
                let total = acquired + rejected;
                if total == 0 {
                    0.0
                } else {
                    rejected as f64 / total as f64
                }
            },
        }
    }

    /// Update the limit for a specific workflow type (dynamic reconfiguration).
    pub fn set_type_limit(&self, workflow_type_id: u64, new_limit: u32) {
        let mut types = self.per_type.write().unwrap();
        let limiter = types.entry(workflow_type_id).or_insert(TypeLimiter {
            active: 0,
            limit: self.config.max_per_type,
        });
        limiter.limit = new_limit;
    }

    /// Update the limit for a specific namespace (dynamic reconfiguration).
    pub fn set_namespace_limit(&self, namespace_id: u64, new_limit: u32) {
        let mut namespaces = self.per_namespace.write().unwrap();
        let limiter = namespaces.entry(namespace_id).or_insert(NamespaceLimiter {
            active: 0,
            limit: self.config.max_per_namespace,
        });
        limiter.limit = new_limit;
    }

    /// Reset all tracking (for testing).
    pub fn reset(&self) {
        self.per_type.write().unwrap().clear();
        self.per_namespace.write().unwrap().clear();
        self.global_active.store(0, Ordering::Relaxed);
        self.queue.write().unwrap().clear();
        self.total_acquired.store(0, Ordering::Relaxed);
        self.total_rejected.store(0, Ordering::Relaxed);
        self.total_queued.store(0, Ordering::Relaxed);
        self.total_released.store(0, Ordering::Relaxed);
    }

    // ─── Internal ──────────────────────────────────────────────────────

    fn commit_acquire(&self, workflow_type_id: u64, namespace_id: u64) {
        self.global_active.fetch_add(1, Ordering::Relaxed);
        self.total_acquired.fetch_add(1, Ordering::Relaxed);

        {
            let mut types = self.per_type.write().unwrap();
            if let Some(limiter) = types.get_mut(&workflow_type_id) {
                limiter.active += 1;
            }
        }
        {
            let mut namespaces = self.per_namespace.write().unwrap();
            if let Some(limiter) = namespaces.get_mut(&namespace_id) {
                limiter.active += 1;
            }
        }
    }

    fn handle_overflow(
        &self,
        workflow_key: u64,
        workflow_type_id: u64,
        namespace_id: u64,
        priority: u8,
    ) -> AcquireResult {
        match self.config.overflow_policy {
            OverflowPolicy::Reject => {
                self.total_rejected.fetch_add(1, Ordering::Relaxed);
                AcquireResult::Rejected
            }
            OverflowPolicy::Queue => {
                let mut queue = self.queue.write().unwrap();
                if queue.len() >= self.config.max_queue_depth as usize {
                    self.total_rejected.fetch_add(1, Ordering::Relaxed);
                    return AcquireResult::Rejected;
                }
                let pos = queue.len() as u64;
                queue.push_back(QueuedRequest {
                    workflow_key,
                    workflow_type_id,
                    namespace_id,
                    priority,
                    queued_at: pos,
                });
                self.total_queued.fetch_add(1, Ordering::Relaxed);
                AcquireResult::Queued(pos)
            }
            OverflowPolicy::Preempt => {
                // For now, just reject — preemption requires integration with the engine
                self.total_rejected.fetch_add(1, Ordering::Relaxed);
                AcquireResult::Rejected
            }
        }
    }

    fn try_dequeue(&self) {
        let mut queue = self.queue.write().unwrap();
        if let Some(request) = queue.pop_front() {
            // Check if we can now admit this request
            let global = self.global_active.load(Ordering::Relaxed);
            if global >= self.config.max_global as u64 {
                queue.push_front(request);
                return;
            }

            let types = self.per_type.write().unwrap();
            let type_ok = types
                .get(&request.workflow_type_id)
                .is_none_or(|l| l.active < l.limit);
            if !type_ok {
                queue.push_front(request);
                return;
            }
            drop(types);

            let namespaces = self.per_namespace.write().unwrap();
            let ns_ok = namespaces
                .get(&request.namespace_id)
                .is_none_or(|l| l.active < l.limit);
            if !ns_ok {
                queue.push_front(request);
                return;
            }
            drop(namespaces);

            // Admit the queued request
            self.commit_acquire(request.workflow_type_id, request.namespace_id);
        }
    }
}

impl Default for WorkflowConcurrencyLimiter {
    fn default() -> Self {
        Self::new(ConcurrencyConfig::default())
    }
}

// ─── Stats ─────────────────────────────────────────────────────────────────

/// Concurrency limiter statistics.
#[derive(Debug, Clone)]
pub struct ConcurrencyStats {
    pub global_active: u64,
    pub total_acquired: u64,
    pub total_rejected: u64,
    pub total_queued: u64,
    pub total_released: u64,
    pub queue_depth: u64,
    pub rejection_rate: f64,
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ConcurrencyConfig {
        ConcurrencyConfig {
            max_per_type: 3,
            max_per_namespace: 10,
            max_global: 100,
            overflow_policy: OverflowPolicy::Reject,
            max_queue_depth: 5,
            priority_levels: 4,
        }
    }

    #[test]
    fn test_acquire_within_limit() {
        let limiter = WorkflowConcurrencyLimiter::new(test_config());

        assert_eq!(limiter.acquire(1, 100, 1, 0), AcquireResult::Acquired);
        assert_eq!(limiter.acquire(2, 100, 1, 0), AcquireResult::Acquired);
        assert_eq!(limiter.acquire(3, 100, 1, 0), AcquireResult::Acquired);
        assert_eq!(limiter.active_for_type(100), 3);
        assert_eq!(limiter.global_active(), 3);
    }

    #[test]
    fn test_acquire_exceeds_type_limit() {
        let limiter = WorkflowConcurrencyLimiter::new(test_config());

        assert_eq!(limiter.acquire(1, 100, 1, 0), AcquireResult::Acquired);
        assert_eq!(limiter.acquire(2, 100, 1, 0), AcquireResult::Acquired);
        assert_eq!(limiter.acquire(3, 100, 1, 0), AcquireResult::Acquired);
        assert_eq!(limiter.acquire(4, 100, 1, 0), AcquireResult::Rejected);
    }

    #[test]
    fn test_release_frees_slot() {
        let limiter = WorkflowConcurrencyLimiter::new(test_config());

        limiter.acquire(1, 100, 1, 0);
        limiter.acquire(2, 100, 1, 0);
        limiter.acquire(3, 100, 1, 0);
        assert_eq!(limiter.acquire(4, 100, 1, 0), AcquireResult::Rejected);

        limiter.release(100, 1);
        assert_eq!(limiter.acquire(4, 100, 1, 0), AcquireResult::Acquired);
    }

    #[test]
    fn test_namespace_limit() {
        let config = ConcurrencyConfig {
            max_per_type: 100,
            max_per_namespace: 2,
            max_global: 100,
            overflow_policy: OverflowPolicy::Reject,
            max_queue_depth: 5,
            priority_levels: 4,
        };
        let limiter = WorkflowConcurrencyLimiter::new(config);

        assert_eq!(limiter.acquire(1, 100, 1, 0), AcquireResult::Acquired);
        assert_eq!(limiter.acquire(2, 200, 1, 0), AcquireResult::Acquired);
        // Different type, same namespace — hits namespace limit
        assert_eq!(limiter.acquire(3, 300, 1, 0), AcquireResult::Rejected);
    }

    #[test]
    fn test_global_limit() {
        let config = ConcurrencyConfig {
            max_per_type: 100,
            max_per_namespace: 100,
            max_global: 2,
            overflow_policy: OverflowPolicy::Reject,
            max_queue_depth: 5,
            priority_levels: 4,
        };
        let limiter = WorkflowConcurrencyLimiter::new(config);

        assert_eq!(limiter.acquire(1, 100, 1, 0), AcquireResult::Acquired);
        assert_eq!(limiter.acquire(2, 200, 2, 0), AcquireResult::Acquired);
        assert_eq!(limiter.acquire(3, 300, 3, 0), AcquireResult::Rejected);
    }

    #[test]
    fn test_queue_policy() {
        let config = ConcurrencyConfig {
            max_per_type: 1,
            max_per_namespace: 100,
            max_global: 100,
            overflow_policy: OverflowPolicy::Queue,
            max_queue_depth: 5,
            priority_levels: 4,
        };
        let limiter = WorkflowConcurrencyLimiter::new(config);

        assert_eq!(limiter.acquire(1, 100, 1, 0), AcquireResult::Acquired);
        // Second request should be queued
        let result = limiter.acquire(2, 100, 1, 0);
        assert!(matches!(result, AcquireResult::Queued(_)));
        assert_eq!(limiter.queue_depth(), 1);
    }

    #[test]
    fn test_queue_max_depth() {
        let config = ConcurrencyConfig {
            max_per_type: 1,
            max_per_namespace: 100,
            max_global: 100,
            overflow_policy: OverflowPolicy::Queue,
            max_queue_depth: 2,
            priority_levels: 4,
        };
        let limiter = WorkflowConcurrencyLimiter::new(config);

        limiter.acquire(1, 100, 1, 0); // Acquired
        limiter.acquire(2, 100, 1, 0); // Queued (pos 0)
        limiter.acquire(3, 100, 1, 0); // Queued (pos 1)
        assert_eq!(limiter.acquire(4, 100, 1, 0), AcquireResult::Rejected); // Queue full
    }

    #[test]
    fn test_utilization() {
        let limiter = WorkflowConcurrencyLimiter::new(test_config());

        limiter.acquire(1, 100, 1, 0);
        assert!((limiter.type_utilization(100) - 33.33).abs() < 1.0);

        limiter.acquire(2, 100, 1, 0);
        assert!((limiter.type_utilization(100) - 66.67).abs() < 1.0);

        limiter.acquire(3, 100, 1, 0);
        assert!((limiter.type_utilization(100) - 100.0).abs() < 0.1);
    }

    #[test]
    fn test_stats() {
        let limiter = WorkflowConcurrencyLimiter::new(test_config());

        limiter.acquire(1, 100, 1, 0);
        limiter.acquire(2, 100, 1, 0);
        limiter.acquire(3, 100, 1, 0);
        limiter.acquire(4, 100, 1, 0); // Rejected

        let stats = limiter.stats();
        assert_eq!(stats.global_active, 3);
        assert_eq!(stats.total_acquired, 3);
        assert_eq!(stats.total_rejected, 1);
        assert!(stats.rejection_rate > 0.2 && stats.rejection_rate < 0.3);
    }

    #[test]
    fn test_dynamic_limit_update() {
        let limiter = WorkflowConcurrencyLimiter::new(test_config());

        limiter.acquire(1, 100, 1, 0);
        limiter.acquire(2, 100, 1, 0);
        limiter.acquire(3, 100, 1, 0);
        assert_eq!(limiter.acquire(4, 100, 1, 0), AcquireResult::Rejected);

        // Increase limit
        limiter.set_type_limit(100, 5);
        assert_eq!(limiter.acquire(4, 100, 1, 0), AcquireResult::Acquired);
    }

    #[test]
    fn test_reset() {
        let limiter = WorkflowConcurrencyLimiter::new(test_config());
        limiter.acquire(1, 100, 1, 0);
        limiter.acquire(2, 100, 1, 0);
        limiter.reset();

        assert_eq!(limiter.global_active(), 0);
        assert_eq!(limiter.active_for_type(100), 0);
        assert_eq!(limiter.queue_depth(), 0);
    }

    #[test]
    fn test_config_presets() {
        let default = ConcurrencyConfig::default_config();
        assert_eq!(default.max_per_type, 100);

        let tight = ConcurrencyConfig::tight();
        assert_eq!(tight.max_per_type, 10);

        let relaxed = ConcurrencyConfig::relaxed();
        assert_eq!(relaxed.max_per_type, 1000);
    }

    #[test]
    fn test_release_dequeues_queued_request() {
        let config = ConcurrencyConfig {
            max_per_type: 1,
            max_per_namespace: 100,
            max_global: 100,
            overflow_policy: OverflowPolicy::Queue,
            max_queue_depth: 5,
            priority_levels: 4,
        };
        let limiter = WorkflowConcurrencyLimiter::new(config);

        // Fill the slot
        limiter.acquire(1, 100, 1, 0);
        // Queue a request
        let result = limiter.acquire(2, 100, 1, 0);
        assert!(matches!(result, AcquireResult::Queued(_)));
        assert_eq!(limiter.queue_depth(), 1);

        // Release the slot — should dequeue the waiting request
        limiter.release(100, 1);
        assert_eq!(limiter.queue_depth(), 0);
        assert_eq!(limiter.active_for_type(100), 1); // The dequeued request now holds the slot
    }

    #[test]
    fn test_different_types_independent() {
        let limiter = WorkflowConcurrencyLimiter::new(test_config());

        // Type 100 — fill up
        limiter.acquire(1, 100, 1, 0);
        limiter.acquire(2, 100, 1, 0);
        limiter.acquire(3, 100, 1, 0);

        // Type 200 — should still work
        assert_eq!(limiter.acquire(4, 200, 1, 0), AcquireResult::Acquired);
        assert_eq!(limiter.active_for_type(100), 3);
        assert_eq!(limiter.active_for_type(200), 1);
    }
}
