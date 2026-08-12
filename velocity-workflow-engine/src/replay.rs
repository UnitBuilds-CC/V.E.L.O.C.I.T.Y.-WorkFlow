//! Deterministic replay engine for workflow state reconstruction.
//! Replays workflow event history to rebuild execution state, enabling:
//! - Workflow reset to any point in history
//! - Crash recovery by replaying from WAL/events
//! - Determinism verification (same events → same state)
//!
//! In Temporal, replay re-executes workflow code from history. The SDK intercepts
//! each command (schedule activity, start timer, etc.) and either returns the
//! recorded result from history or continues normal execution past the replay point.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use crate::engine::{WorkflowContext, WorkflowStatus};
use crate::event_history::{HistoryEvent, HistoryEventType, HistoryStore};

/// Result of replaying a workflow's event history.
#[derive(Debug, Clone)]
pub struct ReplayResult {
    /// The workflow key that was replayed.
    pub workflow_key: u64,
    /// The event ID we replayed up to.
    pub replayed_to_event_id: u64,
    /// Total events in the history.
    pub total_events: usize,
    /// Number of events successfully replayed.
    pub events_replayed: usize,
    /// Reconstructed workflow status at the replay point.
    pub status: WorkflowStatus,
    /// Reconstructed step results (step_index → result payload).
    pub step_results: HashMap<u32, Vec<u8>>,
    /// Reconstructed pending signals (signal_name_id → payloads).
    pub pending_signals: HashMap<u64, Vec<Vec<u8>>>,
    /// Reconstructed activity states.
    pub activity_states: Vec<ReplayActivityState>,
    /// Whether the replay completed successfully.
    pub success: bool,
    /// Error message if replay failed.
    pub error: Option<String>,
}

/// State of an activity reconstructed during replay.
#[derive(Debug, Clone)]
pub struct ReplayActivityState {
    pub step_index: u32,
    pub activity_name_id: u64,
    pub status: ReplayActivityStatus,
    pub result: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayActivityStatus {
    Scheduled,
    Started,
    Completed,
    Failed,
    TimedOut,
}

/// The deterministic replay engine.
/// Replays event histories to reconstruct workflow state.
pub struct ReplayEngine {
    /// Cache of replay results for quick access.
    replay_cache: RwLock<HashMap<u64, ReplayResult>>,
    /// Maximum number of events to replay in a single operation.
    max_replay_events: usize,
    /// Total replays performed.
    total_replays: AtomicU64,
}

impl ReplayEngine {
    pub fn new() -> Self {
        Self {
            replay_cache: RwLock::new(HashMap::new()),
            max_replay_events: 100_000,
            total_replays: AtomicU64::new(0),
        }
    }

    pub fn with_max_events(max_events: usize) -> Self {
        Self {
            replay_cache: RwLock::new(HashMap::new()),
            max_replay_events: max_events,
            total_replays: AtomicU64::new(0),
        }
    }

    /// Replay a workflow's event history from the beginning up to a specific event ID.
    /// If `up_to_event_id` is None, replays the entire history.
    pub fn replay(
        &self,
        workflow_key: u64,
        history: &[HistoryEvent],
        up_to_event_id: Option<u64>,
    ) -> ReplayResult {
        self.total_replays.fetch_add(1, Ordering::Relaxed);

        let cutoff = up_to_event_id.unwrap_or(u64::MAX);
        let mut step_results: HashMap<u32, Vec<u8>> = HashMap::new();
        let mut pending_signals: HashMap<u64, Vec<Vec<u8>>> = HashMap::new();
        let mut activity_states: Vec<ReplayActivityState> = Vec::new();
        let mut status = WorkflowStatus::Void;
        let mut events_replayed = 0usize;
        let mut replayed_to_event_id = 0u64;

        for event in history {
            if event.event_id > cutoff {
                break;
            }
            if events_replayed >= self.max_replay_events {
                break;
            }

            replayed_to_event_id = event.event_id;
            events_replayed += 1;

            match event.event_type {
                HistoryEventType::WorkflowStarted => {
                    status = WorkflowStatus::Running;
                }
                HistoryEventType::WorkflowCompleted => {
                    status = WorkflowStatus::Completed;
                }
                HistoryEventType::WorkflowFailed => {
                    status = WorkflowStatus::Failed;
                }
                HistoryEventType::WorkflowCanceled => {
                    status = WorkflowStatus::Canceled;
                }
                HistoryEventType::WorkflowTerminated => {
                    status = WorkflowStatus::Terminated;
                }
                HistoryEventType::WorkflowTimedOut => {
                    status = WorkflowStatus::TimedOut;
                }
                HistoryEventType::WorkflowContinuedAsNew => {
                    status = WorkflowStatus::ContinuedAsNew;
                }
                HistoryEventType::StepCompleted => {
                    // Parse step index from payload: first 4 bytes = step_index (LE)
                    if event.payload.len() >= 4 {
                        let step_index =
                            u32::from_le_bytes(event.payload[0..4].try_into().unwrap_or([0; 4]));
                        let result = if event.payload.len() > 4 {
                            event.payload[4..].to_vec()
                        } else {
                            vec![]
                        };
                        step_results.insert(step_index, result);
                    }
                }
                HistoryEventType::ActivityScheduled => {
                    if event.payload.len() >= 12 {
                        let step_index =
                            u32::from_le_bytes(event.payload[0..4].try_into().unwrap_or([0; 4]));
                        let activity_name_id =
                            u64::from_le_bytes(event.payload[4..12].try_into().unwrap_or([0; 8]));
                        activity_states.push(ReplayActivityState {
                            step_index,
                            activity_name_id,
                            status: ReplayActivityStatus::Scheduled,
                            result: None,
                        });
                    }
                }
                HistoryEventType::ActivityStarted => {
                    if event.payload.len() >= 4 {
                        let step_index =
                            u32::from_le_bytes(event.payload[0..4].try_into().unwrap_or([0; 4]));
                        if let Some(act) = activity_states
                            .iter_mut()
                            .rev()
                            .find(|a| a.step_index == step_index)
                        {
                            act.status = ReplayActivityStatus::Started;
                        }
                    }
                }
                HistoryEventType::ActivityCompleted => {
                    if event.payload.len() >= 4 {
                        let step_index =
                            u32::from_le_bytes(event.payload[0..4].try_into().unwrap_or([0; 4]));
                        let result = if event.payload.len() > 4 {
                            Some(event.payload[4..].to_vec())
                        } else {
                            None
                        };
                        if let Some(act) = activity_states
                            .iter_mut()
                            .rev()
                            .find(|a| a.step_index == step_index)
                        {
                            act.status = ReplayActivityStatus::Completed;
                            act.result = result;
                        }
                    }
                }
                HistoryEventType::ActivityFailed => {
                    if event.payload.len() >= 4 {
                        let step_index =
                            u32::from_le_bytes(event.payload[0..4].try_into().unwrap_or([0; 4]));
                        if let Some(act) = activity_states
                            .iter_mut()
                            .rev()
                            .find(|a| a.step_index == step_index)
                        {
                            act.status = ReplayActivityStatus::Failed;
                        }
                    }
                }
                HistoryEventType::ActivityTimedOut => {
                    if event.payload.len() >= 4 {
                        let step_index =
                            u32::from_le_bytes(event.payload[0..4].try_into().unwrap_or([0; 4]));
                        if let Some(act) = activity_states
                            .iter_mut()
                            .rev()
                            .find(|a| a.step_index == step_index)
                        {
                            act.status = ReplayActivityStatus::TimedOut;
                        }
                    }
                }
                HistoryEventType::SignalReceived => {
                    if event.payload.len() >= 8 {
                        let signal_name_id =
                            u64::from_le_bytes(event.payload[0..8].try_into().unwrap_or([0; 8]));
                        let signal_payload = if event.payload.len() > 8 {
                            event.payload[8..].to_vec()
                        } else {
                            vec![]
                        };
                        pending_signals
                            .entry(signal_name_id)
                            .or_default()
                            .push(signal_payload);
                    }
                }
                // Timer, child workflow, marker, update, query events are noted but
                // don't change the core reconstructed state in this implementation.
                _ => {}
            }
        }

        ReplayResult {
            workflow_key,
            replayed_to_event_id,
            total_events: history.len(),
            events_replayed,
            status,
            step_results,
            pending_signals,
            activity_states,
            success: true,
            error: None,
        }
    }

    /// Replay a workflow from the history store and cache the result.
    pub fn replay_from_store(
        &self,
        workflow_key: u64,
        history_store: &HistoryStore,
        up_to_event_id: Option<u64>,
    ) -> ReplayResult {
        let history = history_store.get_history(workflow_key).unwrap_or_default();
        let result = self.replay(workflow_key, &history, up_to_event_id);

        // Cache the result
        self.replay_cache
            .write()
            .unwrap()
            .insert(workflow_key, result.clone());

        result
    }

    /// Apply a replay result to reconstruct a workflow context.
    /// This creates a new WorkflowContext with the replayed state.
    pub fn apply_replay(&self, result: &ReplayResult) -> Option<WorkflowContext> {
        if !result.success {
            return None;
        }

        // Determine total steps from the max step_index in step_results
        let total_steps = result
            .step_results
            .keys()
            .max()
            .map(|&m| m + 1)
            .unwrap_or(0);

        let mut ctx = WorkflowContext::new(
            result.workflow_key & 0xFFFFFFFF, // workflow_id
            0,                                // run_id (will be reassigned)
            0,                                // workflow_type_id (from history)
            0,                                // task_queue_hash (from history)
            total_steps,
        );

        // Restore status
        ctx.status = result.status;

        // Restore step results
        for (&step, data) in &result.step_results {
            ctx.complete_step(step, data.clone());
        }

        // Restore signals
        for (&signal_name_id, payloads) in &result.pending_signals {
            for payload in payloads {
                ctx.signal(signal_name_id, payload.clone());
            }
        }

        Some(ctx)
    }

    /// Verify determinism: replay the same history twice and confirm identical results.
    pub fn verify_determinism(&self, workflow_key: u64, history: &[HistoryEvent]) -> bool {
        let result1 = self.replay(workflow_key, history, None);
        let result2 = self.replay(workflow_key, history, None);

        result1.status == result2.status
            && result1.step_results == result2.step_results
            && result1.events_replayed == result2.events_replayed
            && result1.replayed_to_event_id == result2.replayed_to_event_id
    }

    /// Get a cached replay result.
    pub fn get_cached(&self, workflow_key: u64) -> Option<ReplayResult> {
        self.replay_cache
            .read()
            .unwrap()
            .get(&workflow_key)
            .cloned()
    }

    /// Clear the replay cache for a specific workflow.
    pub fn invalidate_cache(&self, workflow_key: u64) {
        self.replay_cache.write().unwrap().remove(&workflow_key);
    }

    /// Clear the entire replay cache.
    pub fn clear_cache(&self) {
        self.replay_cache.write().unwrap().clear();
    }

    /// Get total number of replays performed.
    pub fn total_replays(&self) -> u64 {
        self.total_replays.load(Ordering::Relaxed)
    }

    /// Get the number of cached replay results.
    pub fn cache_size(&self) -> usize {
        self.replay_cache.read().unwrap().len()
    }
}

impl Default for ReplayEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_history::HistoryStore;

    fn make_history(workflow_key: u64) -> Vec<HistoryEvent> {
        let store = HistoryStore::new();
        store.record_event(workflow_key, HistoryEventType::WorkflowStarted, vec![]);
        store.record_event(workflow_key, HistoryEventType::StepCompleted, {
            let mut payload = Vec::new();
            payload.extend_from_slice(&0u32.to_le_bytes()); // step 0
            payload.extend_from_slice(&[10, 20, 30]); // result
            payload
        });
        store.record_event(workflow_key, HistoryEventType::StepCompleted, {
            let mut payload = Vec::new();
            payload.extend_from_slice(&1u32.to_le_bytes()); // step 1
            payload.extend_from_slice(&[40, 50]); // result
            payload
        });
        store.record_event(workflow_key, HistoryEventType::WorkflowCompleted, vec![]);
        store.get_history(workflow_key).unwrap()
    }

    #[test]
    fn test_full_replay() {
        let engine = ReplayEngine::new();
        let history = make_history(42);

        let result = engine.replay(42, &history, None);
        assert!(result.success);
        assert_eq!(result.events_replayed, 4);
        assert_eq!(result.status, WorkflowStatus::Completed);
        assert_eq!(result.step_results.len(), 2);
        assert_eq!(result.step_results[&0], vec![10, 20, 30]);
        assert_eq!(result.step_results[&1], vec![40, 50]);
    }

    #[test]
    fn test_partial_replay() {
        let engine = ReplayEngine::new();
        let history = make_history(42);

        // Replay only up to event 2 (the first StepCompleted)
        let result = engine.replay(42, &history, Some(2));
        assert!(result.success);
        assert_eq!(result.events_replayed, 2);
        assert_eq!(result.status, WorkflowStatus::Running); // Still running at event 2
        assert_eq!(result.step_results.len(), 1);
        assert_eq!(result.step_results[&0], vec![10, 20, 30]);
    }

    #[test]
    fn test_determinism_verification() {
        let engine = ReplayEngine::new();
        let history = make_history(42);

        assert!(engine.verify_determinism(42, &history));
    }

    #[test]
    fn test_replay_with_signals() {
        let store = HistoryStore::new();
        let key = 100u64;
        store.record_event(key, HistoryEventType::WorkflowStarted, vec![]);

        // Signal with name_id=999 and payload [1,2,3]
        let mut sig_payload = Vec::new();
        sig_payload.extend_from_slice(&999u64.to_le_bytes());
        sig_payload.extend_from_slice(&[1, 2, 3]);
        store.record_event(key, HistoryEventType::SignalReceived, sig_payload);

        let history = store.get_history(key).unwrap();
        let engine = ReplayEngine::new();
        let result = engine.replay(key, &history, None);

        assert!(result.success);
        assert_eq!(result.pending_signals.len(), 1);
        assert_eq!(result.pending_signals[&999], vec![vec![1, 2, 3]]);
    }

    #[test]
    fn test_replay_with_activities() {
        let store = HistoryStore::new();
        let key = 200u64;
        store.record_event(key, HistoryEventType::WorkflowStarted, vec![]);

        // Activity scheduled: step=0, name_id=55
        let mut sched_payload = Vec::new();
        sched_payload.extend_from_slice(&0u32.to_le_bytes());
        sched_payload.extend_from_slice(&55u64.to_le_bytes());
        store.record_event(key, HistoryEventType::ActivityScheduled, sched_payload);

        // Activity started: step=0
        let mut start_payload = Vec::new();
        start_payload.extend_from_slice(&0u32.to_le_bytes());
        store.record_event(key, HistoryEventType::ActivityStarted, start_payload);

        // Activity completed: step=0, result=[7,8,9]
        let mut comp_payload = Vec::new();
        comp_payload.extend_from_slice(&0u32.to_le_bytes());
        comp_payload.extend_from_slice(&[7, 8, 9]);
        store.record_event(key, HistoryEventType::ActivityCompleted, comp_payload);

        store.record_event(key, HistoryEventType::WorkflowCompleted, vec![]);

        let history = store.get_history(key).unwrap();
        let engine = ReplayEngine::new();
        let result = engine.replay(key, &history, None);

        assert!(result.success);
        assert_eq!(result.activity_states.len(), 1);
        assert_eq!(result.activity_states[0].step_index, 0);
        assert_eq!(result.activity_states[0].activity_name_id, 55);
        assert_eq!(
            result.activity_states[0].status,
            ReplayActivityStatus::Completed
        );
        assert_eq!(result.activity_states[0].result, Some(vec![7, 8, 9]));
    }

    #[test]
    fn test_replay_from_store() {
        let store = HistoryStore::new();
        let key = 300u64;
        store.record_event(key, HistoryEventType::WorkflowStarted, vec![]);
        store.record_event(key, HistoryEventType::WorkflowCompleted, vec![]);

        let engine = ReplayEngine::new();
        let result = engine.replay_from_store(key, &store, None);

        assert!(result.success);
        assert_eq!(result.status, WorkflowStatus::Completed);
        assert_eq!(engine.cache_size(), 1);
    }

    #[test]
    fn test_cache_invalidation() {
        let store = HistoryStore::new();
        let key = 400u64;
        store.record_event(key, HistoryEventType::WorkflowStarted, vec![]);

        let engine = ReplayEngine::new();
        engine.replay_from_store(key, &store, None);
        assert_eq!(engine.cache_size(), 1);

        engine.invalidate_cache(key);
        assert_eq!(engine.cache_size(), 0);
    }

    #[test]
    fn test_empty_history() {
        let engine = ReplayEngine::new();
        let result = engine.replay(999, &[], None);
        assert!(result.success);
        assert_eq!(result.events_replayed, 0);
        assert_eq!(result.status, WorkflowStatus::Void);
    }
}
