//! Resource limits and tracking for the workflow engine.
//!
//! Enforces hard caps on active workflows, per-namespace concurrency, signal buffering,
//! payload sizes, step counts, and child workflows. The [`ResourceTracker`] maintains
//! live counters and rejects operations that would exceed configured limits.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

// ─── Resource Limits ──────────────────────────────────────────────────────────

/// Static configuration of resource ceilings.
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum number of concurrently active workflows across all namespaces.
    pub max_active_workflows: usize,
    /// Maximum number of concurrently active workflows per namespace.
    pub max_workflows_per_namespace: usize,
    /// Maximum number of pending signals buffered per workflow.
    pub max_signals_per_workflow: usize,
    /// Maximum payload size in bytes (start input, signal payload, query payload).
    pub max_payload_size_bytes: usize,
    /// Maximum number of steps in a single workflow definition.
    pub max_steps_per_workflow: u32,
    /// Maximum number of child workflows spawned from a single parent.
    pub max_child_workflows: usize,
}

impl ResourceLimits {
    /// Production defaults.
    pub fn production_defaults() -> Self {
        Self {
            max_active_workflows: 1_000_000,
            max_workflows_per_namespace: 100_000,
            max_signals_per_workflow: 1_000,
            max_payload_size_bytes: 10 * 1024 * 1024, // 10 MB
            max_steps_per_workflow: 100_000,
            max_child_workflows: 1_000,
        }
    }

    /// Small limits suitable for tests / embedded use.
    pub fn small() -> Self {
        Self {
            max_active_workflows: 1_000,
            max_workflows_per_namespace: 100,
            max_signals_per_workflow: 50,
            max_payload_size_bytes: 1 * 1024 * 1024, // 1 MB
            max_steps_per_workflow: 10_000,
            max_child_workflows: 50,
        }
    }
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self::production_defaults()
    }
}

// ─── Resource Exceeded Error ──────────────────────────────────────────────────

/// Error returned when a resource limit would be exceeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceExceeded {
    /// Global active-workflow cap reached.
    MaxActiveWorkflows { current: usize, limit: usize },
    /// Per-namespace workflow cap reached.
    MaxWorkflowsPerNamespace {
        namespace_id: u64,
        current: usize,
        limit: usize,
    },
    /// Signal buffer full.
    MaxSignalsPerWorkflow {
        workflow_key: u64,
        current: usize,
        limit: usize,
    },
    /// Payload too large.
    MaxPayloadSize { size: usize, limit: usize },
    /// Too many steps.
    MaxSteps { steps: u32, limit: u32 },
    /// Too many child workflows.
    MaxChildWorkflows {
        parent_key: u64,
        current: usize,
        limit: usize,
    },
}

impl std::fmt::Display for ResourceExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxActiveWorkflows { current, limit } => {
                write!(
                    f,
                    "max active workflows exceeded ({} >= {})",
                    current, limit
                )
            }
            Self::MaxWorkflowsPerNamespace {
                namespace_id,
                current,
                limit,
            } => {
                write!(
                    f,
                    "namespace {} workflow limit exceeded ({} >= {})",
                    namespace_id, current, limit
                )
            }
            Self::MaxSignalsPerWorkflow {
                workflow_key,
                current,
                limit,
            } => {
                write!(
                    f,
                    "workflow {} signal limit exceeded ({} >= {})",
                    workflow_key, current, limit
                )
            }
            Self::MaxPayloadSize { size, limit } => {
                write!(f, "payload size {} exceeds limit {}", size, limit)
            }
            Self::MaxSteps { steps, limit } => {
                write!(f, "step count {} exceeds limit {}", steps, limit)
            }
            Self::MaxChildWorkflows {
                parent_key,
                current,
                limit,
            } => {
                write!(
                    f,
                    "workflow {} child limit exceeded ({} >= {})",
                    parent_key, current, limit
                )
            }
        }
    }
}

impl std::error::Error for ResourceExceeded {}

// ─── Resource Usage Snapshot ──────────────────────────────────────────────────

/// Point-in-time snapshot of resource consumption.
#[derive(Debug, Clone)]
pub struct ResourceUsage {
    /// Total active workflows across all namespaces.
    pub active_workflows: usize,
    /// Per-namespace active workflow counts.
    pub per_namespace_counts: HashMap<u64, usize>,
    /// Peak (high-water mark) of concurrent active workflows.
    pub peak_workflows: usize,
}

// ─── Resource Tracker ─────────────────────────────────────────────────────────

/// Live resource tracker. Enforces [`ResourceLimits`] and maintains atomic counters.
pub struct ResourceTracker {
    limits: ResourceLimits,
    /// Total active workflows.
    active_workflows: AtomicUsize,
    /// Peak active workflows (high-water mark).
    peak_workflows: AtomicUsize,
    /// Per-namespace active workflow counts.
    namespace_counts: RwLock<HashMap<u64, AtomicUsize>>,
    /// Per-workflow pending signal counts.
    signal_counts: Mutex<HashMap<u64, usize>>,
    /// Per-workflow child workflow counts.
    child_counts: Mutex<HashMap<u64, usize>>,
}

impl ResourceTracker {
    /// Create a new tracker with the given limits.
    pub fn new(limits: ResourceLimits) -> Self {
        Self {
            limits,
            active_workflows: AtomicUsize::new(0),
            peak_workflows: AtomicUsize::new(0),
            namespace_counts: RwLock::new(HashMap::new()),
            signal_counts: Mutex::new(HashMap::new()),
            child_counts: Mutex::new(HashMap::new()),
        }
    }

    /// Create a tracker with production default limits.
    pub fn with_defaults() -> Self {
        Self::new(ResourceLimits::default())
    }

    /// Get a reference to the configured limits.
    pub fn limits(&self) -> &ResourceLimits {
        &self.limits
    }

    /// Check whether a new workflow can be started in the given namespace.
    pub fn check_can_start_workflow(&self, namespace_id: u64) -> Result<(), ResourceExceeded> {
        let current_total = self.active_workflows.load(Ordering::Acquire);
        if current_total >= self.limits.max_active_workflows {
            return Err(ResourceExceeded::MaxActiveWorkflows {
                current: current_total,
                limit: self.limits.max_active_workflows,
            });
        }

        let counts = self.namespace_counts.read().unwrap();
        let ns_count = counts
            .get(&namespace_id)
            .map(|c| c.load(Ordering::Acquire))
            .unwrap_or(0);
        if ns_count >= self.limits.max_workflows_per_namespace {
            return Err(ResourceExceeded::MaxWorkflowsPerNamespace {
                namespace_id,
                current: ns_count,
                limit: self.limits.max_workflows_per_namespace,
            });
        }

        Ok(())
    }

    /// Check whether a signal can be delivered to a workflow.
    pub fn check_can_signal(&self, workflow_key: u64) -> Result<(), ResourceExceeded> {
        let signals = self.signal_counts.lock().unwrap();
        let current = signals.get(&workflow_key).copied().unwrap_or(0);
        if current >= self.limits.max_signals_per_workflow {
            return Err(ResourceExceeded::MaxSignalsPerWorkflow {
                workflow_key,
                current,
                limit: self.limits.max_signals_per_workflow,
            });
        }
        Ok(())
    }

    /// Check whether a payload of the given size is within limits.
    pub fn check_payload_size(&self, size: usize) -> Result<(), ResourceExceeded> {
        if size > self.limits.max_payload_size_bytes {
            return Err(ResourceExceeded::MaxPayloadSize {
                size,
                limit: self.limits.max_payload_size_bytes,
            });
        }
        Ok(())
    }

    /// Check whether a step count is within limits.
    pub fn check_step_count(&self, steps: u32) -> Result<(), ResourceExceeded> {
        if steps > self.limits.max_steps_per_workflow {
            return Err(ResourceExceeded::MaxSteps {
                steps,
                limit: self.limits.max_steps_per_workflow,
            });
        }
        Ok(())
    }

    /// Check whether a parent workflow can spawn another child.
    pub fn check_can_spawn_child(&self, parent_key: u64) -> Result<(), ResourceExceeded> {
        let children = self.child_counts.lock().unwrap();
        let current = children.get(&parent_key).copied().unwrap_or(0);
        if current >= self.limits.max_child_workflows {
            return Err(ResourceExceeded::MaxChildWorkflows {
                parent_key,
                current,
                limit: self.limits.max_child_workflows,
            });
        }
        Ok(())
    }

    // ── Tracking mutations ────────────────────────────────────────────────

    /// Record that a workflow has started in the given namespace.
    pub fn track_workflow_started(&self, namespace_id: u64) {
        self.active_workflows.fetch_add(1, Ordering::AcqRel);
        self.update_peak();

        let counts = self.namespace_counts.read().unwrap();
        if let Some(counter) = counts.get(&namespace_id) {
            counter.fetch_add(1, Ordering::AcqRel);
        } else {
            // Need write lock to insert new namespace counter
            drop(counts);
            let mut counts = self.namespace_counts.write().unwrap();
            counts
                .entry(namespace_id)
                .or_insert_with(|| AtomicUsize::new(0))
                .fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Record that a workflow has completed (or been terminated/cancelled).
    pub fn track_workflow_completed(&self, namespace_id: u64) {
        self.active_workflows.fetch_sub(1, Ordering::AcqRel);

        let counts = self.namespace_counts.read().unwrap();
        if let Some(counter) = counts.get(&namespace_id) {
            counter.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Record that a signal was buffered for a workflow.
    pub fn track_signal_added(&self, workflow_key: u64) {
        let mut signals = self.signal_counts.lock().unwrap();
        let count = signals.entry(workflow_key).or_insert(0);
        *count += 1;
    }

    /// Record that a signal was consumed from a workflow's buffer.
    pub fn track_signal_consumed(&self, workflow_key: u64) {
        let mut signals = self.signal_counts.lock().unwrap();
        if let Some(count) = signals.get_mut(&workflow_key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                signals.remove(&workflow_key);
            }
        }
    }

    /// Record that a child workflow was spawned.
    pub fn track_child_spawned(&self, parent_key: u64) {
        let mut children = self.child_counts.lock().unwrap();
        let count = children.entry(parent_key).or_insert(0);
        *count += 1;
    }

    /// Record that a child workflow completed.
    pub fn track_child_completed(&self, parent_key: u64) {
        let mut children = self.child_counts.lock().unwrap();
        if let Some(count) = children.get_mut(&parent_key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                children.remove(&parent_key);
            }
        }
    }

    /// Clean up all tracking state for a completed workflow (signals, children).
    pub fn cleanup_workflow(&self, workflow_key: u64) {
        self.signal_counts.lock().unwrap().remove(&workflow_key);
        self.child_counts.lock().unwrap().remove(&workflow_key);
    }

    /// Get a snapshot of current resource usage.
    pub fn current_counts(&self) -> ResourceUsage {
        let counts = self.namespace_counts.read().unwrap();
        let per_ns: HashMap<u64, usize> = counts
            .iter()
            .map(|(&ns, counter)| (ns, counter.load(Ordering::Acquire)))
            .collect();

        ResourceUsage {
            active_workflows: self.active_workflows.load(Ordering::Acquire),
            per_namespace_counts: per_ns,
            peak_workflows: self.peak_workflows.load(Ordering::Acquire),
        }
    }

    // ── Internal ──────────────────────────────────────────────────────────

    fn update_peak(&self) {
        let current = self.active_workflows.load(Ordering::Acquire);
        let mut peak = self.peak_workflows.load(Ordering::Acquire);
        while current > peak {
            match self.peak_workflows.compare_exchange_weak(
                peak,
                current,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }
}

impl Default for ResourceTracker {
    fn default() -> Self {
        Self::with_defaults()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_workflow_limit() {
        let limits = ResourceLimits {
            max_active_workflows: 2,
            max_workflows_per_namespace: 100,
            ..ResourceLimits::small()
        };
        let tracker = ResourceTracker::new(limits);

        assert!(tracker.check_can_start_workflow(1).is_ok());
        tracker.track_workflow_started(1);
        assert!(tracker.check_can_start_workflow(1).is_ok());
        tracker.track_workflow_started(1);
        // Third should fail
        assert!(matches!(
            tracker.check_can_start_workflow(1),
            Err(ResourceExceeded::MaxActiveWorkflows { .. })
        ));
    }

    #[test]
    fn test_per_namespace_limit() {
        let limits = ResourceLimits {
            max_active_workflows: 1_000,
            max_workflows_per_namespace: 2,
            ..ResourceLimits::small()
        };
        let tracker = ResourceTracker::new(limits);

        tracker.track_workflow_started(1);
        tracker.track_workflow_started(1);
        // Namespace 1 is full
        assert!(matches!(
            tracker.check_can_start_workflow(1),
            Err(ResourceExceeded::MaxWorkflowsPerNamespace { .. })
        ));
        // Namespace 2 is fine
        assert!(tracker.check_can_start_workflow(2).is_ok());
    }

    #[test]
    fn test_signal_limit() {
        let limits = ResourceLimits {
            max_signals_per_workflow: 3,
            ..ResourceLimits::small()
        };
        let tracker = ResourceTracker::new(limits);

        for _ in 0..3 {
            tracker.track_signal_added(42);
        }
        assert!(matches!(
            tracker.check_can_signal(42),
            Err(ResourceExceeded::MaxSignalsPerWorkflow { .. })
        ));

        tracker.track_signal_consumed(42);
        assert!(tracker.check_can_signal(42).is_ok());
    }

    #[test]
    fn test_payload_size_check() {
        let limits = ResourceLimits {
            max_payload_size_bytes: 1024,
            ..ResourceLimits::small()
        };
        let tracker = ResourceTracker::new(limits);

        assert!(tracker.check_payload_size(512).is_ok());
        assert!(tracker.check_payload_size(1024).is_ok());
        assert!(matches!(
            tracker.check_payload_size(1025),
            Err(ResourceExceeded::MaxPayloadSize { .. })
        ));
    }

    #[test]
    fn test_workflow_lifecycle_tracking() {
        let tracker = ResourceTracker::with_defaults();

        tracker.track_workflow_started(1);
        tracker.track_workflow_started(1);
        tracker.track_workflow_started(2);

        let usage = tracker.current_counts();
        assert_eq!(usage.active_workflows, 3);
        assert_eq!(usage.per_namespace_counts.get(&1), Some(&2));
        assert_eq!(usage.per_namespace_counts.get(&2), Some(&1));
        assert_eq!(usage.peak_workflows, 3);

        tracker.track_workflow_completed(1);
        let usage = tracker.current_counts();
        assert_eq!(usage.active_workflows, 2);
        // Peak should still be 3
        assert_eq!(usage.peak_workflows, 3);
    }

    #[test]
    fn test_child_workflow_limit() {
        let limits = ResourceLimits {
            max_child_workflows: 2,
            ..ResourceLimits::small()
        };
        let tracker = ResourceTracker::new(limits);

        tracker.track_child_spawned(100);
        tracker.track_child_spawned(100);
        assert!(matches!(
            tracker.check_can_spawn_child(100),
            Err(ResourceExceeded::MaxChildWorkflows { .. })
        ));

        tracker.track_child_completed(100);
        assert!(tracker.check_can_spawn_child(100).is_ok());
    }

    #[test]
    fn test_step_count_check() {
        let limits = ResourceLimits {
            max_steps_per_workflow: 100,
            ..ResourceLimits::small()
        };
        let tracker = ResourceTracker::new(limits);

        assert!(tracker.check_step_count(50).is_ok());
        assert!(tracker.check_step_count(100).is_ok());
        assert!(matches!(
            tracker.check_step_count(101),
            Err(ResourceExceeded::MaxSteps { .. })
        ));
    }

    #[test]
    fn test_cleanup_workflow() {
        let tracker = ResourceTracker::with_defaults();

        tracker.track_signal_added(42);
        tracker.track_signal_added(42);
        tracker.track_child_spawned(42);

        tracker.cleanup_workflow(42);

        // After cleanup, signal and child counts should be reset
        assert!(tracker.check_can_signal(42).is_ok());
        assert!(tracker.check_can_spawn_child(42).is_ok());
    }
}
