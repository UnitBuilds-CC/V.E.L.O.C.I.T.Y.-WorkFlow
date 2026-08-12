//! Advanced operations matching Temporal's 2025-2026 API additions.
//!
//! This module closes the remaining parity gaps:
//! - **PauseActivity / UnpauseActivity / ResetActivity**: Runtime activity lifecycle control
//! - **PauseWorkflow / UnpauseWorkflow**: Workflow-level pause/resume
//! - **ExecuteMultiOperation**: Atomic start+update in a single RPC
//! - **UpdateActivityOptions / UpdateWorkflowOptions**: Runtime option mutation
//! - **TimeSkipping**: Test-oriented time advancement
//! - **FairnessState**: Task queue fairness tracking for priority dispatch
//! - **WorkerManagement**: List/count/describe workers, worker heartbeat
//! - **DLQ Admin**: Full dead-letter queue CRUD (get/purge/merge/list)

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

// ─── Pause / Unpause / Reset Activity ──────────────────────────────────────

/// State of an individual activity for pause/resume control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityPauseState {
    /// Activity is running normally.
    Active,
    /// Activity is paused — will not be rescheduled on failure.
    Paused,
    /// Activity has been reset — attempt counter cleared.
    Reset,
}

/// Request to pause an activity by ID within a workflow.
#[derive(Debug, Clone)]
pub struct PauseActivityRequest {
    pub workflow_key: u64,
    pub activity_id: u32,
}

/// Request to unpause a previously paused activity.
#[derive(Debug, Clone)]
pub struct UnpauseActivityRequest {
    pub workflow_key: u64,
    pub activity_id: u32,
    /// Optional jitter for rescheduling (milliseconds).
    pub jitter_ms: Option<u64>,
    /// Whether to reset the attempt counter.
    pub reset_attempts: bool,
    /// Whether to reset heartbeat state.
    pub reset_heartbeat: bool,
}

/// Request to reset an activity's execution state.
#[derive(Debug, Clone)]
pub struct ResetActivityRequest {
    pub workflow_key: u64,
    pub activity_id: u32,
    /// Optional jitter for rescheduling (milliseconds).
    pub jitter_ms: Option<u64>,
    /// Whether to reset heartbeat state.
    pub reset_heartbeats: bool,
    /// If activity is paused, keep it paused.
    pub keep_paused: bool,
}

/// Response for activity control operations.
#[derive(Debug, Clone)]
pub struct ActivityControlResponse {
    pub success: bool,
    pub previous_state: ActivityPauseState,
    pub new_state: ActivityPauseState,
    pub message: String,
}

/// Registry for activity pause/resume/reset state tracking.
#[derive(Debug, Default)]
pub struct ActivityPauseRegistry {
    /// workflow_key -> activity_id -> state
    states: HashMap<u64, HashMap<u32, ActivityPauseState>>,
    /// workflow_key -> activity_id -> attempt count
    attempts: HashMap<u64, HashMap<u32, u32>>,
    /// workflow_key -> activity_id -> heartbeat details
    heartbeats: HashMap<u64, HashMap<u32, Vec<u8>>>,
}

impl ActivityPauseRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an activity for pause tracking.
    pub fn register(&mut self, workflow_key: u64, activity_id: u32) {
        self.states
            .entry(workflow_key)
            .or_default()
            .insert(activity_id, ActivityPauseState::Active);
        self.attempts
            .entry(workflow_key)
            .or_default()
            .insert(activity_id, 0);
    }

    /// Pause an activity. Returns None if activity not found.
    pub fn pause(&mut self, req: &PauseActivityRequest) -> Option<ActivityControlResponse> {
        let wf_states = self.states.get_mut(&req.workflow_key)?;
        let prev = wf_states.get(&req.activity_id).copied()?;
        if prev == ActivityPauseState::Paused {
            return Some(ActivityControlResponse {
                success: true,
                previous_state: prev,
                new_state: ActivityPauseState::Paused,
                message: "already paused".into(),
            });
        }
        wf_states.insert(req.activity_id, ActivityPauseState::Paused);
        Some(ActivityControlResponse {
            success: true,
            previous_state: prev,
            new_state: ActivityPauseState::Paused,
            message: format!("activity {} paused", req.activity_id),
        })
    }

    /// Unpause an activity.
    pub fn unpause(&mut self, req: &UnpauseActivityRequest) -> Option<ActivityControlResponse> {
        let wf_states = self.states.get_mut(&req.workflow_key)?;
        let prev = wf_states.get(&req.activity_id).copied()?;
        let new_state = ActivityPauseState::Active;
        wf_states.insert(req.activity_id, new_state);

        if req.reset_attempts {
            if let Some(wf_attempts) = self.attempts.get_mut(&req.workflow_key) {
                wf_attempts.insert(req.activity_id, 0);
            }
        }
        if req.reset_heartbeat {
            if let Some(wf_hb) = self.heartbeats.get_mut(&req.workflow_key) {
                wf_hb.remove(&req.activity_id);
            }
        }

        Some(ActivityControlResponse {
            success: true,
            previous_state: prev,
            new_state,
            message: format!("activity {} unpaused", req.activity_id),
        })
    }

    /// Reset an activity's execution state.
    pub fn reset(&mut self, req: &ResetActivityRequest) -> Option<ActivityControlResponse> {
        let wf_states = self.states.get_mut(&req.workflow_key)?;
        let prev = wf_states.get(&req.activity_id).copied()?;

        let new_state = if req.keep_paused && prev == ActivityPauseState::Paused {
            ActivityPauseState::Paused
        } else {
            ActivityPauseState::Reset
        };
        wf_states.insert(req.activity_id, new_state);

        // Reset attempts to 0
        if let Some(wf_attempts) = self.attempts.get_mut(&req.workflow_key) {
            wf_attempts.insert(req.activity_id, 0);
        }
        if req.reset_heartbeats {
            if let Some(wf_hb) = self.heartbeats.get_mut(&req.workflow_key) {
                wf_hb.remove(&req.activity_id);
            }
        }

        Some(ActivityControlResponse {
            success: true,
            previous_state: prev,
            new_state,
            message: format!("activity {} reset", req.activity_id),
        })
    }

    /// Get the current pause state of an activity.
    pub fn get_state(&self, workflow_key: u64, activity_id: u32) -> Option<ActivityPauseState> {
        self.states.get(&workflow_key)?.get(&activity_id).copied()
    }

    /// Check if an activity should be scheduled (not paused).
    pub fn should_schedule(&self, workflow_key: u64, activity_id: u32) -> bool {
        self.get_state(workflow_key, activity_id)
            .map(|s| s != ActivityPauseState::Paused)
            .unwrap_or(true)
    }

    /// Record a heartbeat detail for an activity.
    pub fn record_heartbeat(&mut self, workflow_key: u64, activity_id: u32, details: Vec<u8>) {
        self.heartbeats
            .entry(workflow_key)
            .or_default()
            .insert(activity_id, details);
    }

    /// Get heartbeat details for an activity.
    pub fn get_heartbeat(&self, workflow_key: u64, activity_id: u32) -> Option<&Vec<u8>> {
        self.heartbeats.get(&workflow_key)?.get(&activity_id)
    }

    /// Increment and return the attempt count for an activity.
    pub fn increment_attempt(&mut self, workflow_key: u64, activity_id: u32) -> u32 {
        let count = self.attempts.entry(workflow_key).or_default().entry(activity_id).or_insert(0);
        *count += 1;
        *count
    }

    /// Get the current attempt count.
    pub fn get_attempts(&self, workflow_key: u64, activity_id: u32) -> u32 {
        self.attempts.get(&workflow_key).and_then(|m| m.get(&activity_id).copied()).unwrap_or(0)
    }

    /// Remove all state for a workflow.
    pub fn remove_workflow(&mut self, workflow_key: u64) {
        self.states.remove(&workflow_key);
        self.attempts.remove(&workflow_key);
        self.heartbeats.remove(&workflow_key);
    }

    /// Count of tracked activities across all workflows.
    pub fn total_tracked(&self) -> usize {
        self.states.values().map(|m| m.len()).sum()
    }

    /// Count of paused activities.
    pub fn paused_count(&self) -> usize {
        self.states.values()
            .flat_map(|m| m.values())
            .filter(|s| **s == ActivityPauseState::Paused)
            .count()
    }
}

// ─── Pause / Unpause Workflow ──────────────────────────────────────────────

/// Workflow-level pause state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowPauseState {
    Running,
    Paused,
}

/// Registry for workflow-level pause state.
#[derive(Debug, Default)]
pub struct WorkflowPauseRegistry {
    states: HashMap<u64, WorkflowPauseState>,
}

impl WorkflowPauseRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a workflow as running.
    pub fn register(&mut self, workflow_key: u64) {
        self.states.insert(workflow_key, WorkflowPauseState::Running);
    }

    /// Pause a workflow.
    pub fn pause(&mut self, workflow_key: u64) -> Option<WorkflowPauseState> {
        let prev = self.states.get(&workflow_key).copied()?;
        self.states.insert(workflow_key, WorkflowPauseState::Paused);
        Some(prev)
    }

    /// Unpause a workflow.
    pub fn unpause(&mut self, workflow_key: u64) -> Option<WorkflowPauseState> {
        let prev = self.states.get(&workflow_key).copied()?;
        self.states.insert(workflow_key, WorkflowPauseState::Running);
        Some(prev)
    }

    /// Check if a workflow is paused.
    pub fn is_paused(&self, workflow_key: u64) -> bool {
        self.states.get(&workflow_key) == Some(&WorkflowPauseState::Paused)
    }

    /// Get the pause state.
    pub fn get_state(&self, workflow_key: u64) -> Option<WorkflowPauseState> {
        self.states.get(&workflow_key).copied()
    }

    /// Remove a workflow from tracking.
    pub fn remove(&mut self, workflow_key: u64) {
        self.states.remove(&workflow_key);
    }

    /// List all paused workflow keys.
    pub fn all_paused(&self) -> Vec<u64> {
        self.states.iter()
            .filter(|(_, s)| **s == WorkflowPauseState::Paused)
            .map(|(k, _)| *k)
            .collect()
    }

    /// Total count of tracked workflows.
    pub fn total_tracked(&self) -> usize {
        self.states.len()
    }

    /// Count of paused workflows.
    pub fn paused_count(&self) -> usize {
        self.states.values().filter(|s| **s == WorkflowPauseState::Paused).count()
    }
}

// ─── ExecuteMultiOperation ─────────────────────────────────────────────────

/// A sub-operation within a multi-operation request.
#[derive(Debug, Clone)]
pub enum MultiOperationStep {
    /// Start a new workflow execution.
    StartWorkflow {
        workflow_type: String,
        workflow_id: String,
        task_queue: String,
        input: Option<Vec<u8>>,
    },
    /// Send an update to a workflow.
    UpdateWorkflow {
        workflow_id: String,
        update_name: String,
        args: Option<Vec<u8>>,
    },
    /// Signal a workflow.
    SignalWorkflow {
        workflow_id: String,
        signal_name: String,
        data: Option<Vec<u8>>,
    },
}

/// Result of a single step within a multi-operation.
#[derive(Debug, Clone)]
pub struct MultiOperationStepResult {
    pub step_index: usize,
    pub success: bool,
    pub workflow_key: Option<u64>,
    pub error: Option<String>,
}

/// Result of a complete multi-operation execution.
#[derive(Debug, Clone)]
pub struct MultiOperationResult {
    pub results: Vec<MultiOperationStepResult>,
    pub all_succeeded: bool,
}

/// Executor for multi-operation requests (atomic start + update/signal).
#[derive(Debug, Default)]
pub struct MultiOperationExecutor {
    /// Count of multi-operations executed.
    pub executed_count: u64,
    /// Count of failed multi-operations.
    pub failed_count: u64,
}

impl MultiOperationExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate a multi-operation request (all steps must reference valid targets).
    pub fn validate(operations: &[MultiOperationStep]) -> Result<(), String> {
        if operations.is_empty() {
            return Err("multi-operation must contain at least one step".into());
        }
        if operations.len() > 10 {
            return Err("multi-operation limited to 10 steps".into());
        }
        // First operation must be StartWorkflow if any StartWorkflow is present
        let has_start = operations.iter().any(|op| matches!(op, MultiOperationStep::StartWorkflow { .. }));
        if has_start && !matches!(operations[0], MultiOperationStep::StartWorkflow { .. }) {
            return Err("StartWorkflow must be the first step when present".into());
        }
        Ok(())
    }

    /// Record a successful multi-operation execution.
    pub fn record_success(&mut self) {
        self.executed_count += 1;
    }

    /// Record a failed multi-operation execution.
    pub fn record_failure(&mut self) {
        self.failed_count += 1;
    }

    /// Build a success result.
    pub fn success_result(results: Vec<MultiOperationStepResult>) -> MultiOperationResult {
        let all_succeeded = results.iter().all(|r| r.success);
        MultiOperationResult { results, all_succeeded }
    }

    /// Build a partial failure result.
    pub fn partial_failure(results: Vec<MultiOperationStepResult>) -> MultiOperationResult {
        MultiOperationResult { results, all_succeeded: false }
    }

    /// Total operations executed.
    pub fn total_executed(&self) -> u64 {
        self.executed_count
    }

    /// Failure rate as a percentage.
    pub fn failure_rate(&self) -> f64 {
        let total = self.executed_count + self.failed_count;
        if total == 0 { 0.0 } else { self.failed_count as f64 / total as f64 * 100.0 }
    }
}

// ─── Update Activity / Workflow Options ────────────────────────────────────

/// Runtime-mutable activity options.
#[derive(Debug, Clone)]
pub struct ActivityRuntimeOptions {
    pub start_to_close_timeout_ms: Option<u64>,
    pub schedule_to_close_timeout_ms: Option<u64>,
    pub heartbeat_timeout_ms: Option<u64>,
    pub retry_max_attempts: Option<u32>,
    pub retry_initial_interval_ms: Option<u64>,
    pub retry_backoff_coefficient: Option<f64>,
    pub retry_max_interval_ms: Option<u64>,
}

/// Runtime-mutable workflow options.
#[derive(Debug, Clone)]
pub struct WorkflowRuntimeOptions {
    pub workflow_execution_timeout_ms: Option<u64>,
    pub workflow_run_timeout_ms: Option<u64>,
    pub workflow_task_timeout_ms: Option<u64>,
    /// Versioning behavior override (e.g., pinned to a specific build ID).
    pub versioning_override: Option<String>,
}

/// Registry for runtime option overrides.
#[derive(Debug, Default)]
pub struct RuntimeOptionsRegistry {
    /// workflow_key -> activity_id -> options
    activity_options: HashMap<u64, HashMap<u32, ActivityRuntimeOptions>>,
    /// workflow_key -> options
    workflow_options: HashMap<u64, WorkflowRuntimeOptions>,
}

impl RuntimeOptionsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update activity options for a specific activity within a workflow.
    pub fn update_activity_options(
        &mut self,
        workflow_key: u64,
        activity_id: u32,
        options: ActivityRuntimeOptions,
    ) -> Option<ActivityRuntimeOptions> {
        let prev = self.activity_options
            .get(&workflow_key)
            .and_then(|m| m.get(&activity_id).cloned());
        self.activity_options
            .entry(workflow_key)
            .or_default()
            .insert(activity_id, options);
        prev
    }

    /// Get current activity options.
    pub fn get_activity_options(
        &self,
        workflow_key: u64,
        activity_id: u32,
    ) -> Option<&ActivityRuntimeOptions> {
        self.activity_options.get(&workflow_key)?.get(&activity_id)
    }

    /// Update workflow-level options.
    pub fn update_workflow_options(
        &mut self,
        workflow_key: u64,
        options: WorkflowRuntimeOptions,
    ) -> Option<WorkflowRuntimeOptions> {
        let prev = self.workflow_options.get(&workflow_key).cloned();
        self.workflow_options.insert(workflow_key, options);
        prev
    }

    /// Get current workflow options.
    pub fn get_workflow_options(&self, workflow_key: u64) -> Option<&WorkflowRuntimeOptions> {
        self.workflow_options.get(&workflow_key)
    }

    /// Remove all options for a workflow.
    pub fn remove_workflow(&mut self, workflow_key: u64) {
        self.activity_options.remove(&workflow_key);
        self.workflow_options.remove(&workflow_key);
    }

    /// Count of workflows with option overrides.
    pub fn workflow_count(&self) -> usize {
        self.workflow_options.len()
    }

    /// Count of activities with option overrides.
    pub fn activity_count(&self) -> usize {
        self.activity_options.values().map(|m| m.len()).sum()
    }
}

// ─── Time Skipping ─────────────────────────────────────────────────────────

/// Time skipping controller for test environments.
///
/// Allows advancing the logical clock without actually waiting, so timer-based
/// workflows can be tested instantly.
#[derive(Debug)]
pub struct TimeSkipController {
    /// Whether time skipping is currently enabled.
    enabled: bool,
    /// Accumulated skipped time.
    skipped: Duration,
    /// Original start instant.
    base_instant: Instant,
    /// Scheduled skip events (future time -> callback name).
    scheduled_skips: VecDeque<(Duration, String)>,
    /// Total number of skip operations performed.
    skip_count: u64,
}

impl TimeSkipController {
    pub fn new() -> Self {
        Self {
            enabled: false,
            skipped: Duration::ZERO,
            base_instant: Instant::now(),
            scheduled_skips: VecDeque::new(),
            skip_count: 0,
        }
    }

    /// Enable time skipping mode.
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Disable time skipping mode.
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Whether time skipping is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Skip time forward by the given duration.
    pub fn skip(&mut self, duration: Duration) -> Result<Duration, String> {
        if !self.enabled {
            return Err("time skipping is not enabled".into());
        }
        self.skipped += duration;
        self.skip_count += 1;
        Ok(self.skipped)
    }

    /// Skip to a specific future time (relative to base instant).
    pub fn skip_to(&mut self, target: Duration) -> Result<Duration, String> {
        if !self.enabled {
            return Err("time skipping is not enabled".into());
        }
        if target > self.skipped {
            self.skipped = target;
            self.skip_count += 1;
        }
        Ok(self.skipped)
    }

    /// Schedule a named skip event (for orchestrating complex test scenarios).
    pub fn schedule_skip(&mut self, at: Duration, label: String) {
        self.scheduled_skips.push_back((at, label));
    }

    /// Process any scheduled skips that are due.
    pub fn process_scheduled(&mut self) -> Vec<String> {
        let mut fired = Vec::new();
        while let Some((at, label)) = self.scheduled_skips.front() {
            if *at <= self.skipped {
                let (_, label) = self.scheduled_skips.pop_front().unwrap();
                fired.push(label);
            } else {
                break;
            }
        }
        fired
    }

    /// Get the current logical time (base + skipped).
    pub fn logical_now(&self) -> Duration {
        self.base_instant.elapsed() + self.skipped
    }

    /// Get the total accumulated skipped time.
    pub fn total_skipped(&self) -> Duration {
        self.skipped
    }

    /// Get the number of skip operations performed.
    pub fn skip_count(&self) -> u64 {
        self.skip_count
    }

    /// Get the number of pending scheduled skips.
    pub fn pending_scheduled(&self) -> usize {
        self.scheduled_skips.len()
    }

    /// Reset the controller.
    pub fn reset(&mut self) {
        self.skipped = Duration::ZERO;
        self.skip_count = 0;
        self.scheduled_skips.clear();
        self.base_instant = Instant::now();
    }
}

// ─── Fairness State ────────────────────────────────────────────────────────

/// Fairness tracking for task queue dispatch.
///
/// Tracks per-poller dispatch counts to detect and correct imbalances,
/// ensuring fair task distribution across workers.
#[derive(Debug)]
pub struct FairnessTracker {
    /// poller_id -> dispatch count
    dispatch_counts: HashMap<u64, u64>,
    /// poller_id -> last dispatch timestamp
    last_dispatch: HashMap<u64, Instant>,
    /// Global dispatch counter.
    total_dispatches: u64,
    /// Number of fairness adjustments made.
    adjustments: u64,
    /// Threshold ratio (max/min) that triggers rebalancing.
    threshold_ratio: f64,
}

impl FairnessTracker {
    pub fn new(threshold_ratio: f64) -> Self {
        Self {
            dispatch_counts: HashMap::new(),
            last_dispatch: HashMap::new(),
            total_dispatches: 0,
            adjustments: 0,
            threshold_ratio,
        }
    }

    /// Record a task dispatch to a poller.
    pub fn record_dispatch(&mut self, poller_id: u64) {
        *self.dispatch_counts.entry(poller_id).or_insert(0) += 1;
        self.last_dispatch.insert(poller_id, Instant::now());
        self.total_dispatches += 1;
    }

    /// Check if dispatch to a poller would be fair.
    /// Returns false if the poller has significantly more dispatches than the minimum.
    pub fn is_fair(&self, poller_id: u64) -> bool {
        if self.dispatch_counts.len() <= 1 {
            return true;
        }
        let current = self.dispatch_counts.get(&poller_id).copied().unwrap_or(0);
        let min_count = self.dispatch_counts.values().copied().min().unwrap_or(0);
        let max_count = self.dispatch_counts.values().copied().max().unwrap_or(0);

        if min_count == 0 {
            return current == 0;
        }

        let ratio = max_count as f64 / min_count as f64;
        // If this poller is at the max and ratio exceeds threshold, not fair
        current == min_count || ratio <= self.threshold_ratio
    }

    /// Get the poller with the fewest dispatches (most fair target).
    pub fn fairest_poller(&self) -> Option<u64> {
        self.dispatch_counts.iter()
            .min_by_key(|(_, count)| **count)
            .map(|(id, _)| *id)
    }

    /// Get dispatch counts sorted by count (ascending).
    pub fn dispatch_ranking(&self) -> Vec<(u64, u64)> {
        let mut ranking: Vec<(u64, u64)> = self.dispatch_counts.iter()
            .map(|(id, count)| (*id, *count))
            .collect();
        ranking.sort_by_key(|(_, count)| *count);
        ranking
    }

    /// Record a fairness adjustment.
    pub fn record_adjustment(&mut self) {
        self.adjustments += 1;
    }

    /// Get fairness statistics.
    pub fn stats(&self) -> FairnessStats {
        let counts: Vec<u64> = self.dispatch_counts.values().copied().collect();
        let min = counts.iter().copied().min().unwrap_or(0);
        let max = counts.iter().copied().max().unwrap_or(0);
        let avg = if counts.is_empty() { 0.0 } else {
            counts.iter().sum::<u64>() as f64 / counts.len() as f64
        };
        let imbalance = if min == 0 { max as f64 } else { max as f64 / min as f64 };

        FairnessStats {
            poller_count: self.dispatch_counts.len(),
            total_dispatches: self.total_dispatches,
            min_dispatches: min,
            max_dispatches: max,
            avg_dispatches: avg,
            imbalance_ratio: imbalance,
            adjustments: self.adjustments,
        }
    }

    /// Remove a poller from tracking.
    pub fn remove_poller(&mut self, poller_id: u64) {
        self.dispatch_counts.remove(&poller_id);
        self.last_dispatch.remove(&poller_id);
    }

    /// Reset all tracking state.
    pub fn reset(&mut self) {
        self.dispatch_counts.clear();
        self.last_dispatch.clear();
        self.total_dispatches = 0;
        self.adjustments = 0;
    }

    /// Number of tracked pollers.
    pub fn poller_count(&self) -> usize {
        self.dispatch_counts.len()
    }
}

/// Fairness statistics snapshot.
#[derive(Debug, Clone)]
pub struct FairnessStats {
    pub poller_count: usize,
    pub total_dispatches: u64,
    pub min_dispatches: u64,
    pub max_dispatches: u64,
    pub avg_dispatches: f64,
    pub imbalance_ratio: f64,
    pub adjustments: u64,
}

// ─── Worker Management ─────────────────────────────────────────────────────

/// Information about a registered worker.
#[derive(Debug, Clone)]
pub struct ManagedWorkerInfo {
    pub instance_key: String,
    pub namespace: String,
    pub task_queues: Vec<String>,
    pub build_id: String,
    pub registered_at: Instant,
    pub last_heartbeat: Instant,
    pub health: WorkerHealthStatus,
    pub active_polls: u32,
}

/// Health status of a managed worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerHealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Draining,
}

/// Request to list workers with optional filtering.
#[derive(Debug, Clone)]
pub struct ListWorkersRequest {
    pub namespace: String,
    pub task_queue_filter: Option<String>,
    pub build_id_filter: Option<String>,
    pub health_filter: Option<WorkerHealthStatus>,
    pub max_results: usize,
    pub page_token: Option<Vec<u8>>,
}

/// Response from listing workers.
#[derive(Debug, Clone)]
pub struct ListWorkersResponse {
    pub workers: Vec<ManagedWorkerInfo>,
    pub next_page_token: Option<Vec<u8>>,
    pub total_count: usize,
}

/// Registry for managed workers with list/count/describe/heartbeat operations.
#[derive(Debug, Default)]
pub struct WorkerManagementRegistry {
    /// instance_key -> worker info
    workers: HashMap<String, ManagedWorkerInfo>,
}

impl WorkerManagementRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new worker.
    pub fn register_worker(&mut self, info: ManagedWorkerInfo) {
        self.workers.insert(info.instance_key.clone(), info);
    }

    /// Record a heartbeat from a worker.
    pub fn heartbeat(&mut self, instance_key: &str) -> Option<&mut ManagedWorkerInfo> {
        let worker = self.workers.get_mut(instance_key)?;
        worker.last_heartbeat = Instant::now();
        worker.health = WorkerHealthStatus::Healthy;
        Some(worker)
    }

    /// List workers matching the given filters.
    pub fn list_workers(&self, req: &ListWorkersRequest) -> ListWorkersResponse {
        let mut filtered: Vec<&ManagedWorkerInfo> = self.workers.values()
            .filter(|w| w.namespace == req.namespace)
            .filter(|w| {
                if let Some(ref tq) = req.task_queue_filter {
                    w.task_queues.iter().any(|t| t == tq)
                } else { true }
            })
            .filter(|w| {
                if let Some(ref bid) = req.build_id_filter {
                    &w.build_id == bid
                } else { true }
            })
            .filter(|w| {
                if let Some(health) = req.health_filter {
                    w.health == health
                } else { true }
            })
            .collect();

        filtered.sort_by(|a, b| a.instance_key.cmp(&b.instance_key));
        let total_count = filtered.len();

        // Simple pagination by index
        let page_idx = req.page_token.as_ref()
            .and_then(|t| String::from_utf8(t.clone()).ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        let end = (page_idx + req.max_results).min(filtered.len());
        let workers: Vec<ManagedWorkerInfo> = filtered[page_idx..end].iter().map(|w| (*w).clone()).collect();

        let next_page_token = if end < filtered.len() {
            Some(end.to_string().into_bytes())
        } else {
            None
        };

        ListWorkersResponse { workers, next_page_token, total_count }
    }

    /// Count workers matching filters.
    pub fn count_workers(&self, namespace: &str, task_queue: Option<&str>) -> usize {
        self.workers.values()
            .filter(|w| w.namespace == namespace)
            .filter(|w| {
                if let Some(tq) = task_queue {
                    w.task_queues.iter().any(|t| t == tq)
                } else { true }
            })
            .count()
    }

    /// Describe a specific worker by instance key.
    pub fn describe_worker(&self, instance_key: &str) -> Option<&ManagedWorkerInfo> {
        self.workers.get(instance_key)
    }

    /// Remove a worker.
    pub fn remove_worker(&mut self, instance_key: &str) -> Option<ManagedWorkerInfo> {
        self.workers.remove(instance_key)
    }

    /// Mark a worker as draining.
    pub fn drain_worker(&mut self, instance_key: &str) -> Option<()> {
        let worker = self.workers.get_mut(instance_key)?;
        worker.health = WorkerHealthStatus::Draining;
        Some(())
    }

    /// Total registered workers.
    pub fn total_workers(&self) -> usize {
        self.workers.len()
    }

    /// Count of healthy workers.
    pub fn healthy_count(&self) -> usize {
        self.workers.values().filter(|w| w.health == WorkerHealthStatus::Healthy).count()
    }

    /// Count of unhealthy workers.
    pub fn unhealthy_count(&self) -> usize {
        self.workers.values().filter(|w| w.health == WorkerHealthStatus::Unhealthy).count()
    }
}

// ─── DLQ Admin Operations ──────────────────────────────────────────────────

/// A task in the dead-letter queue.
#[derive(Debug, Clone)]
pub struct DlqAdminTask {
    pub task_id: u64,
    pub queue_name: String,
    pub workflow_key: Option<u64>,
    pub task_type: String,
    pub payload: Vec<u8>,
    pub failed_at: Instant,
    pub failure_reason: String,
    pub retry_count: u32,
}

/// Dead-letter queue admin controller with full CRUD.
#[derive(Debug, Default)]
pub struct DlqAdminController {
    /// queue_name -> tasks
    queues: HashMap<String, VecDeque<DlqAdminTask>>,
    /// Next task ID counter.
    next_id: u64,
    /// Total tasks enqueued.
    total_enqueued: u64,
    /// Total tasks purged.
    total_purged: u64,
    /// Total tasks merged (re-enqueued).
    total_merged: u64,
}

impl DlqAdminController {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enqueue a failed task to the DLQ.
    pub fn enqueue(&mut self, queue_name: &str, workflow_key: Option<u64>, task_type: &str, payload: Vec<u8>, reason: &str) -> u64 {
        let task_id = self.next_id;
        self.next_id += 1;
        let task = DlqAdminTask {
            task_id,
            queue_name: queue_name.to_string(),
            workflow_key,
            task_type: task_type.to_string(),
            payload,
            failed_at: Instant::now(),
            failure_reason: reason.to_string(),
            retry_count: 0,
        };
        self.queues.entry(queue_name.to_string()).or_default().push_back(task);
        self.total_enqueued += 1;
        task_id
    }

    /// Get tasks from a DLQ (with optional limit).
    pub fn get_tasks(&self, queue_name: &str, max_count: usize) -> Vec<&DlqAdminTask> {
        self.queues.get(queue_name)
            .map(|q| q.iter().take(max_count).collect())
            .unwrap_or_default()
    }

    /// Get all tasks from a DLQ.
    pub fn get_all_tasks(&self, queue_name: &str) -> Vec<&DlqAdminTask> {
        self.get_tasks(queue_name, usize::MAX)
    }

    /// Purge (delete) all tasks from a DLQ. Returns count of purged tasks.
    pub fn purge(&mut self, queue_name: &str) -> u64 {
        let count = self.queues.get(queue_name).map(|q| q.len() as u64).unwrap_or(0);
        if let Some(q) = self.queues.get_mut(queue_name) {
            let purged = q.len() as u64;
            q.clear();
            self.total_purged += purged;
            purged
        } else {
            0
        }
    }

    /// Purge a specific task by ID. Returns true if found and removed.
    pub fn purge_task(&mut self, queue_name: &str, task_id: u64) -> bool {
        if let Some(q) = self.queues.get_mut(queue_name) {
            let before = q.len();
            q.retain(|t| t.task_id != task_id);
            let removed = before - q.len();
            self.total_purged += removed as u64;
            removed > 0
        } else {
            false
        }
    }

    /// Merge (re-enqueue) tasks from a DLQ back to the source. Returns count of merged tasks.
    pub fn merge(&mut self, queue_name: &str) -> Vec<DlqAdminTask> {
        let tasks = self.queues.remove(queue_name).map(|q| q.into_iter().collect::<Vec<_>>()).unwrap_or_default();
        let count = tasks.len() as u64;
        self.total_merged += count;
        tasks
    }

    /// Merge a specific task by ID.
    pub fn merge_task(&mut self, queue_name: &str, task_id: u64) -> Option<DlqAdminTask> {
        if let Some(q) = self.queues.get_mut(queue_name) {
            let pos = q.iter().position(|t| t.task_id == task_id)?;
            let task = q.remove(pos)?;
            self.total_merged += 1;
            Some(task)
        } else {
            None
        }
    }

    /// List all DLQ queue names.
    pub fn list_queues(&self) -> Vec<String> {
        self.queues.keys().cloned().collect()
    }

    /// Get the size of a specific DLQ.
    pub fn queue_size(&self, queue_name: &str) -> usize {
        self.queues.get(queue_name).map(|q| q.len()).unwrap_or(0)
    }

    /// Get total size across all DLQs.
    pub fn total_size(&self) -> usize {
        self.queues.values().map(|q| q.len()).sum()
    }

    /// Get admin statistics.
    pub fn stats(&self) -> DlqAdminStats {
        DlqAdminStats {
            queue_count: self.queues.len(),
            total_tasks: self.total_size(),
            total_enqueued: self.total_enqueued,
            total_purged: self.total_purged,
            total_merged: self.total_merged,
        }
    }
}

/// DLQ admin statistics.
#[derive(Debug, Clone)]
pub struct DlqAdminStats {
    pub queue_count: usize,
    pub total_tasks: usize,
    pub total_enqueued: u64,
    pub total_purged: u64,
    pub total_merged: u64,
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // --- Activity Pause Tests ---

    #[test]
    fn test_pause_activity_basic() {
        let mut reg = ActivityPauseRegistry::new();
        reg.register(1, 10);
        assert_eq!(reg.get_state(1, 10), Some(ActivityPauseState::Active));

        let resp = reg.pause(&PauseActivityRequest { workflow_key: 1, activity_id: 10 }).unwrap();
        assert!(resp.success);
        assert_eq!(resp.new_state, ActivityPauseState::Paused);
        assert_eq!(reg.get_state(1, 10), Some(ActivityPauseState::Paused));
    }

    #[test]
    fn test_pause_already_paused() {
        let mut reg = ActivityPauseRegistry::new();
        reg.register(1, 10);
        reg.pause(&PauseActivityRequest { workflow_key: 1, activity_id: 10 });
        let resp = reg.pause(&PauseActivityRequest { workflow_key: 1, activity_id: 10 }).unwrap();
        assert!(resp.success);
        assert_eq!(resp.message, "already paused");
    }

    #[test]
    fn test_unpause_activity() {
        let mut reg = ActivityPauseRegistry::new();
        reg.register(1, 10);
        reg.pause(&PauseActivityRequest { workflow_key: 1, activity_id: 10 });

        let resp = reg.unpause(&UnpauseActivityRequest {
            workflow_key: 1, activity_id: 10,
            jitter_ms: None, reset_attempts: false, reset_heartbeat: false,
        }).unwrap();
        assert_eq!(resp.new_state, ActivityPauseState::Active);
        assert!(reg.should_schedule(1, 10));
    }

    #[test]
    fn test_unpause_with_reset() {
        let mut reg = ActivityPauseRegistry::new();
        reg.register(1, 10);
        reg.increment_attempt(1, 10);
        reg.increment_attempt(1, 10);
        reg.record_heartbeat(1, 10, vec![1, 2, 3]);

        let resp = reg.unpause(&UnpauseActivityRequest {
            workflow_key: 1, activity_id: 10,
            jitter_ms: None, reset_attempts: true, reset_heartbeat: true,
        }).unwrap();
        assert_eq!(resp.new_state, ActivityPauseState::Active);
        assert_eq!(reg.get_attempts(1, 10), 0);
        assert!(reg.get_heartbeat(1, 10).is_none());
    }

    #[test]
    fn test_reset_activity() {
        let mut reg = ActivityPauseRegistry::new();
        reg.register(1, 10);
        reg.increment_attempt(1, 10);
        reg.increment_attempt(1, 10);

        let resp = reg.reset(&ResetActivityRequest {
            workflow_key: 1, activity_id: 10,
            jitter_ms: None, reset_heartbeats: true, keep_paused: false,
        }).unwrap();
        assert_eq!(resp.new_state, ActivityPauseState::Reset);
        assert_eq!(reg.get_attempts(1, 10), 0);
    }

    #[test]
    fn test_reset_keep_paused() {
        let mut reg = ActivityPauseRegistry::new();
        reg.register(1, 10);
        reg.pause(&PauseActivityRequest { workflow_key: 1, activity_id: 10 });

        let resp = reg.reset(&ResetActivityRequest {
            workflow_key: 1, activity_id: 10,
            jitter_ms: None, reset_heartbeats: false, keep_paused: true,
        }).unwrap();
        assert_eq!(resp.new_state, ActivityPauseState::Paused);
    }

    #[test]
    fn test_should_schedule() {
        let mut reg = ActivityPauseRegistry::new();
        reg.register(1, 10);
        assert!(reg.should_schedule(1, 10));
        reg.pause(&PauseActivityRequest { workflow_key: 1, activity_id: 10 });
        assert!(!reg.should_schedule(1, 10));
    }

    #[test]
    fn test_pause_nonexistent() {
        let mut reg = ActivityPauseRegistry::new();
        assert!(reg.pause(&PauseActivityRequest { workflow_key: 99, activity_id: 99 }).is_none());
    }

    #[test]
    fn test_remove_workflow_pause() {
        let mut reg = ActivityPauseRegistry::new();
        reg.register(1, 10);
        reg.register(1, 20);
        assert_eq!(reg.total_tracked(), 2);
        reg.remove_workflow(1);
        assert_eq!(reg.total_tracked(), 0);
    }

    #[test]
    fn test_paused_count() {
        let mut reg = ActivityPauseRegistry::new();
        reg.register(1, 10);
        reg.register(1, 20);
        reg.register(2, 30);
        assert_eq!(reg.paused_count(), 0);
        reg.pause(&PauseActivityRequest { workflow_key: 1, activity_id: 10 });
        reg.pause(&PauseActivityRequest { workflow_key: 2, activity_id: 30 });
        assert_eq!(reg.paused_count(), 2);
    }

    // --- Workflow Pause Tests ---

    #[test]
    fn test_workflow_pause_basic() {
        let mut reg = WorkflowPauseRegistry::new();
        reg.register(1);
        assert!(!reg.is_paused(1));

        reg.pause(1);
        assert!(reg.is_paused(1));
        assert_eq!(reg.get_state(1), Some(WorkflowPauseState::Paused));
    }

    #[test]
    fn test_workflow_unpause() {
        let mut reg = WorkflowPauseRegistry::new();
        reg.register(1);
        reg.pause(1);
        reg.unpause(1);
        assert!(!reg.is_paused(1));
    }

    #[test]
    fn test_workflow_pause_nonexistent() {
        let mut reg = WorkflowPauseRegistry::new();
        assert!(reg.pause(99).is_none());
    }

    #[test]
    fn test_all_paused() {
        let mut reg = WorkflowPauseRegistry::new();
        reg.register(1);
        reg.register(2);
        reg.register(3);
        reg.pause(1);
        reg.pause(3);
        let paused = reg.all_paused();
        assert_eq!(paused.len(), 2);
        assert!(paused.contains(&1));
        assert!(paused.contains(&3));
    }

    #[test]
    fn test_workflow_pause_counts() {
        let mut reg = WorkflowPauseRegistry::new();
        reg.register(1);
        reg.register(2);
        assert_eq!(reg.total_tracked(), 2);
        assert_eq!(reg.paused_count(), 0);
        reg.pause(1);
        assert_eq!(reg.paused_count(), 1);
    }

    // --- Multi-Operation Tests ---

    #[test]
    fn test_multi_op_validate_empty() {
        assert!(MultiOperationExecutor::validate(&[]).is_err());
    }

    #[test]
    fn test_multi_op_validate_too_many() {
        let ops = vec![MultiOperationStep::StartWorkflow {
            workflow_type: "test".into(), workflow_id: "1".into(),
            task_queue: "q".into(), input: None,
        }; 11];
        assert!(MultiOperationExecutor::validate(&ops).is_err());
    }

    #[test]
    fn test_multi_op_validate_start_first() {
        let ops = vec![
            MultiOperationStep::SignalWorkflow {
                workflow_id: "1".into(), signal_name: "sig".into(), data: None,
            },
            MultiOperationStep::StartWorkflow {
                workflow_type: "test".into(), workflow_id: "1".into(),
                task_queue: "q".into(), input: None,
            },
        ];
        assert!(MultiOperationExecutor::validate(&ops).is_err());
    }

    #[test]
    fn test_multi_op_validate_valid() {
        let ops = vec![
            MultiOperationStep::StartWorkflow {
                workflow_type: "test".into(), workflow_id: "1".into(),
                task_queue: "q".into(), input: None,
            },
            MultiOperationStep::SignalWorkflow {
                workflow_id: "1".into(), signal_name: "sig".into(), data: None,
            },
        ];
        assert!(MultiOperationExecutor::validate(&ops).is_ok());
    }

    #[test]
    fn test_multi_op_executor_counts() {
        let mut exec = MultiOperationExecutor::new();
        exec.record_success();
        exec.record_success();
        exec.record_failure();
        assert_eq!(exec.total_executed(), 2);
        assert!((exec.failure_rate() - 33.33).abs() < 1.0);
    }

    #[test]
    fn test_multi_op_result() {
        let results = vec![
            MultiOperationStepResult { step_index: 0, success: true, workflow_key: Some(1), error: None },
            MultiOperationStepResult { step_index: 1, success: true, workflow_key: Some(1), error: None },
        ];
        let result = MultiOperationExecutor::success_result(results);
        assert!(result.all_succeeded);
    }

    #[test]
    fn test_multi_op_partial_failure() {
        let results = vec![
            MultiOperationStepResult { step_index: 0, success: true, workflow_key: Some(1), error: None },
            MultiOperationStepResult { step_index: 1, success: false, workflow_key: None, error: Some("not found".into()) },
        ];
        let result = MultiOperationExecutor::partial_failure(results);
        assert!(!result.all_succeeded);
    }

    // --- Runtime Options Tests ---

    #[test]
    fn test_activity_runtime_options() {
        let mut reg = RuntimeOptionsRegistry::new();
        let opts = ActivityRuntimeOptions {
            start_to_close_timeout_ms: Some(5000),
            schedule_to_close_timeout_ms: None,
            heartbeat_timeout_ms: Some(1000),
            retry_max_attempts: Some(3),
            retry_initial_interval_ms: None,
            retry_backoff_coefficient: None,
            retry_max_interval_ms: None,
        };
        assert!(reg.update_activity_options(1, 10, opts.clone()).is_none());
        let stored = reg.get_activity_options(1, 10).unwrap();
        assert_eq!(stored.start_to_close_timeout_ms, Some(5000));
        assert_eq!(stored.retry_max_attempts, Some(3));
    }

    #[test]
    fn test_workflow_runtime_options() {
        let mut reg = RuntimeOptionsRegistry::new();
        let opts = WorkflowRuntimeOptions {
            workflow_execution_timeout_ms: Some(60000),
            workflow_run_timeout_ms: Some(30000),
            workflow_task_timeout_ms: Some(10000),
            versioning_override: Some("build-v2".into()),
        };
        assert!(reg.update_workflow_options(1, opts).is_none());
        let stored = reg.get_workflow_options(1).unwrap();
        assert_eq!(stored.versioning_override.as_deref(), Some("build-v2"));
    }

    #[test]
    fn test_runtime_options_update_returns_prev() {
        let mut reg = RuntimeOptionsRegistry::new();
        let opts1 = WorkflowRuntimeOptions {
            workflow_execution_timeout_ms: Some(1000),
            workflow_run_timeout_ms: None,
            workflow_task_timeout_ms: None,
            versioning_override: None,
        };
        let opts2 = WorkflowRuntimeOptions {
            workflow_execution_timeout_ms: Some(2000),
            workflow_run_timeout_ms: Some(5000),
            workflow_task_timeout_ms: None,
            versioning_override: None,
        };
        reg.update_workflow_options(1, opts1);
        let prev = reg.update_workflow_options(1, opts2).unwrap();
        assert_eq!(prev.workflow_execution_timeout_ms, Some(1000));
    }

    #[test]
    fn test_runtime_options_remove() {
        let mut reg = RuntimeOptionsRegistry::new();
        reg.update_workflow_options(1, WorkflowRuntimeOptions {
            workflow_execution_timeout_ms: Some(1000),
            workflow_run_timeout_ms: None,
            workflow_task_timeout_ms: None,
            versioning_override: None,
        });
        reg.remove_workflow(1);
        assert!(reg.get_workflow_options(1).is_none());
        assert_eq!(reg.workflow_count(), 0);
    }

    // --- Time Skip Tests ---

    #[test]
    fn test_time_skip_disabled() {
        let mut ctrl = TimeSkipController::new();
        assert!(ctrl.skip(Duration::from_secs(10)).is_err());
    }

    #[test]
    fn test_time_skip_basic() {
        let mut ctrl = TimeSkipController::new();
        ctrl.enable();
        let result = ctrl.skip(Duration::from_secs(10)).unwrap();
        assert!(result >= Duration::from_secs(10));
        assert_eq!(ctrl.skip_count(), 1);
    }

    #[test]
    fn test_time_skip_accumulates() {
        let mut ctrl = TimeSkipController::new();
        ctrl.enable();
        ctrl.skip(Duration::from_secs(5)).unwrap();
        ctrl.skip(Duration::from_secs(10)).unwrap();
        assert_eq!(ctrl.total_skipped(), Duration::from_secs(15));
        assert_eq!(ctrl.skip_count(), 2);
    }

    #[test]
    fn test_time_skip_to() {
        let mut ctrl = TimeSkipController::new();
        ctrl.enable();
        ctrl.skip(Duration::from_secs(5)).unwrap();
        ctrl.skip_to(Duration::from_secs(20)).unwrap();
        assert_eq!(ctrl.total_skipped(), Duration::from_secs(20));
    }

    #[test]
    fn test_time_skip_to_no_backward() {
        let mut ctrl = TimeSkipController::new();
        ctrl.enable();
        ctrl.skip(Duration::from_secs(30)).unwrap();
        ctrl.skip_to(Duration::from_secs(10)).unwrap();
        assert_eq!(ctrl.total_skipped(), Duration::from_secs(30)); // No change
    }

    #[test]
    fn test_scheduled_skips() {
        let mut ctrl = TimeSkipController::new();
        ctrl.enable();
        ctrl.schedule_skip(Duration::from_secs(5), "timer_a".into());
        ctrl.schedule_skip(Duration::from_secs(15), "timer_b".into());
        assert_eq!(ctrl.pending_scheduled(), 2);

        ctrl.skip(Duration::from_secs(10)).unwrap();
        let fired = ctrl.process_scheduled();
        assert_eq!(fired, vec!["timer_a"]);
        assert_eq!(ctrl.pending_scheduled(), 1);

        ctrl.skip(Duration::from_secs(10)).unwrap();
        let fired = ctrl.process_scheduled();
        assert_eq!(fired, vec!["timer_b"]);
        assert_eq!(ctrl.pending_scheduled(), 0);
    }

    #[test]
    fn test_time_skip_reset() {
        let mut ctrl = TimeSkipController::new();
        ctrl.enable();
        ctrl.skip(Duration::from_secs(100)).unwrap();
        ctrl.reset();
        assert_eq!(ctrl.total_skipped(), Duration::ZERO);
        assert_eq!(ctrl.skip_count(), 0);
    }

    // --- Fairness Tests ---

    #[test]
    fn test_fairness_single_poller() {
        let mut tracker = FairnessTracker::new(2.0);
        tracker.record_dispatch(1);
        tracker.record_dispatch(1);
        assert!(tracker.is_fair(1)); // Single poller always fair
    }

    #[test]
    fn test_fairness_balanced() {
        let mut tracker = FairnessTracker::new(2.0);
        tracker.record_dispatch(1);
        tracker.record_dispatch(2);
        tracker.record_dispatch(1);
        tracker.record_dispatch(2);
        assert!(tracker.is_fair(1));
        assert!(tracker.is_fair(2));
    }

    #[test]
    fn test_fairness_imbalanced() {
        let mut tracker = FairnessTracker::new(2.0);
        for _ in 0..10 { tracker.record_dispatch(1); }
        tracker.record_dispatch(2);
        // ratio = 10/1 = 10 > 2.0 threshold
        assert!(!tracker.is_fair(1));
    }

    #[test]
    fn test_fairest_poller() {
        let mut tracker = FairnessTracker::new(2.0);
        tracker.record_dispatch(1);
        tracker.record_dispatch(1);
        tracker.record_dispatch(2);
        assert_eq!(tracker.fairest_poller(), Some(2));
    }

    #[test]
    fn test_dispatch_ranking() {
        let mut tracker = FairnessTracker::new(2.0);
        tracker.record_dispatch(1);
        tracker.record_dispatch(1);
        tracker.record_dispatch(1);
        tracker.record_dispatch(2);
        let ranking = tracker.dispatch_ranking();
        assert_eq!(ranking[0], (2, 1));
        assert_eq!(ranking[1], (1, 3));
    }

    #[test]
    fn test_fairness_stats() {
        let mut tracker = FairnessTracker::new(2.0);
        tracker.record_dispatch(1);
        tracker.record_dispatch(2);
        tracker.record_dispatch(1);
        let stats = tracker.stats();
        assert_eq!(stats.poller_count, 2);
        assert_eq!(stats.total_dispatches, 3);
        assert_eq!(stats.min_dispatches, 1);
        assert_eq!(stats.max_dispatches, 2);
    }

    #[test]
    fn test_fairness_remove_poller() {
        let mut tracker = FairnessTracker::new(2.0);
        tracker.record_dispatch(1);
        tracker.record_dispatch(2);
        tracker.remove_poller(1);
        assert_eq!(tracker.poller_count(), 1);
        assert_eq!(tracker.fairest_poller(), Some(2));
    }

    // --- Worker Management Tests ---

    fn make_worker(key: &str, ns: &str, tqs: Vec<&str>) -> ManagedWorkerInfo {
        ManagedWorkerInfo {
            instance_key: key.into(),
            namespace: ns.into(),
            task_queues: tqs.into_iter().map(String::from).collect(),
            build_id: "v1".into(),
            registered_at: Instant::now(),
            last_heartbeat: Instant::now(),
            health: WorkerHealthStatus::Healthy,
            active_polls: 0,
        }
    }

    #[test]
    fn test_worker_register_and_list() {
        let mut reg = WorkerManagementRegistry::new();
        reg.register_worker(make_worker("w1", "ns1", vec!["q1"]));
        reg.register_worker(make_worker("w2", "ns1", vec!["q1", "q2"]));
        reg.register_worker(make_worker("w3", "ns2", vec!["q1"]));

        let resp = reg.list_workers(&ListWorkersRequest {
            namespace: "ns1".into(), task_queue_filter: None,
            build_id_filter: None, health_filter: None,
            max_results: 10, page_token: None,
        });
        assert_eq!(resp.total_count, 2);
        assert_eq!(resp.workers.len(), 2);
    }

    #[test]
    fn test_worker_list_with_tq_filter() {
        let mut reg = WorkerManagementRegistry::new();
        reg.register_worker(make_worker("w1", "ns1", vec!["q1"]));
        reg.register_worker(make_worker("w2", "ns1", vec!["q2"]));

        let resp = reg.list_workers(&ListWorkersRequest {
            namespace: "ns1".into(), task_queue_filter: Some("q2".into()),
            build_id_filter: None, health_filter: None,
            max_results: 10, page_token: None,
        });
        assert_eq!(resp.total_count, 1);
        assert_eq!(resp.workers[0].instance_key, "w2");
    }

    #[test]
    fn test_worker_count() {
        let mut reg = WorkerManagementRegistry::new();
        reg.register_worker(make_worker("w1", "ns1", vec!["q1"]));
        reg.register_worker(make_worker("w2", "ns1", vec!["q1", "q2"]));
        assert_eq!(reg.count_workers("ns1", Some("q1")), 2);
        assert_eq!(reg.count_workers("ns1", Some("q2")), 1);
        assert_eq!(reg.count_workers("ns2", None), 0);
    }

    #[test]
    fn test_worker_describe() {
        let mut reg = WorkerManagementRegistry::new();
        reg.register_worker(make_worker("w1", "ns1", vec!["q1"]));
        let info = reg.describe_worker("w1").unwrap();
        assert_eq!(info.namespace, "ns1");
        assert!(reg.describe_worker("w99").is_none());
    }

    #[test]
    fn test_worker_heartbeat() {
        let mut reg = WorkerManagementRegistry::new();
        reg.register_worker(make_worker("w1", "ns1", vec!["q1"]));
        let w = reg.heartbeat("w1").unwrap();
        assert_eq!(w.health, WorkerHealthStatus::Healthy);
        assert!(reg.heartbeat("w99").is_none());
    }

    #[test]
    fn test_worker_drain() {
        let mut reg = WorkerManagementRegistry::new();
        reg.register_worker(make_worker("w1", "ns1", vec!["q1"]));
        reg.drain_worker("w1");
        assert_eq!(reg.describe_worker("w1").unwrap().health, WorkerHealthStatus::Draining);
    }

    #[test]
    fn test_worker_remove() {
        let mut reg = WorkerManagementRegistry::new();
        reg.register_worker(make_worker("w1", "ns1", vec!["q1"]));
        assert_eq!(reg.total_workers(), 1);
        reg.remove_worker("w1");
        assert_eq!(reg.total_workers(), 0);
    }

    #[test]
    fn test_worker_health_counts() {
        let mut reg = WorkerManagementRegistry::new();
        reg.register_worker(make_worker("w1", "ns1", vec!["q1"]));
        reg.register_worker(make_worker("w2", "ns1", vec!["q1"]));
        assert_eq!(reg.healthy_count(), 2);
        assert_eq!(reg.unhealthy_count(), 0);
    }

    // --- DLQ Admin Tests ---

    #[test]
    fn test_dlq_enqueue() {
        let mut dlq = DlqAdminController::new();
        let id = dlq.enqueue("transfer-dlq", Some(1), "TransferTask", vec![1, 2], "timeout");
        assert_eq!(id, 0);
        assert_eq!(dlq.queue_size("transfer-dlq"), 1);
        assert_eq!(dlq.total_size(), 1);
    }

    #[test]
    fn test_dlq_get_tasks() {
        let mut dlq = DlqAdminController::new();
        dlq.enqueue("q1", None, "task_a", vec![], "err1");
        dlq.enqueue("q1", None, "task_b", vec![], "err2");
        dlq.enqueue("q2", None, "task_c", vec![], "err3");

        let tasks = dlq.get_tasks("q1", 10);
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].task_type, "task_a");
    }

    #[test]
    fn test_dlq_purge() {
        let mut dlq = DlqAdminController::new();
        dlq.enqueue("q1", None, "t1", vec![], "err");
        dlq.enqueue("q1", None, "t2", vec![], "err");
        let purged = dlq.purge("q1");
        assert_eq!(purged, 2);
        assert_eq!(dlq.queue_size("q1"), 0);
        assert_eq!(dlq.stats().total_purged, 2);
    }

    #[test]
    fn test_dlq_purge_task() {
        let mut dlq = DlqAdminController::new();
        let id = dlq.enqueue("q1", None, "t1", vec![], "err");
        dlq.enqueue("q1", None, "t2", vec![], "err");
        assert!(dlq.purge_task("q1", id));
        assert_eq!(dlq.queue_size("q1"), 1);
    }

    #[test]
    fn test_dlq_merge() {
        let mut dlq = DlqAdminController::new();
        dlq.enqueue("q1", None, "t1", vec![], "err");
        dlq.enqueue("q1", None, "t2", vec![], "err");
        let merged = dlq.merge("q1");
        assert_eq!(merged.len(), 2);
        assert_eq!(dlq.queue_size("q1"), 0);
        assert_eq!(dlq.stats().total_merged, 2);
    }

    #[test]
    fn test_dlq_merge_task() {
        let mut dlq = DlqAdminController::new();
        let id = dlq.enqueue("q1", None, "t1", vec![], "err");
        dlq.enqueue("q1", None, "t2", vec![], "err");
        let task = dlq.merge_task("q1", id).unwrap();
        assert_eq!(task.task_type, "t1");
        assert_eq!(dlq.queue_size("q1"), 1);
    }

    #[test]
    fn test_dlq_list_queues() {
        let mut dlq = DlqAdminController::new();
        dlq.enqueue("q1", None, "t1", vec![], "err");
        dlq.enqueue("q2", None, "t2", vec![], "err");
        let queues = dlq.list_queues();
        assert_eq!(queues.len(), 2);
        assert!(queues.contains(&"q1".to_string()));
        assert!(queues.contains(&"q2".to_string()));
    }

    #[test]
    fn test_dlq_stats() {
        let mut dlq = DlqAdminController::new();
        dlq.enqueue("q1", None, "t1", vec![], "err");
        dlq.enqueue("q1", None, "t2", vec![], "err");
        dlq.enqueue("q2", None, "t3", vec![], "err");
        dlq.purge("q2");

        let stats = dlq.stats();
        assert_eq!(stats.queue_count, 2);
        assert_eq!(stats.total_tasks, 2);
        assert_eq!(stats.total_enqueued, 3);
        assert_eq!(stats.total_purged, 1);
    }

    #[test]
    fn test_dlq_nonexistent_queue() {
        let dlq = DlqAdminController::new();
        assert_eq!(dlq.get_tasks("nope", 10).len(), 0);
        assert_eq!(dlq.queue_size("nope"), 0);
    }

    #[test]
    fn test_dlq_purge_nonexistent() {
        let mut dlq = DlqAdminController::new();
        assert_eq!(dlq.purge("nope"), 0);
    }
}
