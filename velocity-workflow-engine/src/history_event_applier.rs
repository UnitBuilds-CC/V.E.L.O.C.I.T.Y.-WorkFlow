//! History Event Applier — applies history events to mutable state.
//!
//! This is the core of workflow replay and state reconstruction.
//! Every event type has a specific application logic that modifies
//! the mutable state accordingly.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    RwLock,
};
use std::time::SystemTime;

// ═══════════════════════════════════════════════════════════════════════════════
// Event Types — comprehensive history event enumeration
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum HistoryEventType {
    WorkflowExecutionStarted {
        workflow_type: String,
        task_queue: String,
        input: Vec<u8>,
        run_timeout: u64,
        task_timeout: u64,
    },
    WorkflowExecutionCompleted {
        result: Vec<u8>,
    },
    WorkflowExecutionFailed {
        reason: String,
        details: Vec<u8>,
    },
    WorkflowExecutionTimedOut,
    WorkflowExecutionCanceled {
        details: Vec<u8>,
    },
    WorkflowExecutionContinuedAsNew {
        new_run_id: String,
        workflow_type: String,
    },
    WorkflowExecutionTerminated {
        reason: String,
    },
    WorkflowTaskScheduled {
        task_queue: String,
        start_to_close_timeout: u64,
        attempt: u32,
    },
    WorkflowTaskStarted {
        identity: String,
        request_id: String,
    },
    WorkflowTaskCompleted {
        result: Vec<u8>,
    },
    WorkflowTaskTimedOut {
        timeout_type: TimeoutType,
    },
    WorkflowTaskFailed {
        reason: String,
    },
    ActivityTaskScheduled {
        activity_id: String,
        activity_type: String,
        task_queue: String,
    },
    ActivityTaskStarted {
        attempt: u32,
        identity: String,
    },
    ActivityTaskCompleted {
        result: Vec<u8>,
        scheduled_event_id: i64,
    },
    ActivityTaskFailed {
        reason: String,
        scheduled_event_id: i64,
    },
    ActivityTaskTimedOut {
        timeout_type: TimeoutType,
        scheduled_event_id: i64,
    },
    ActivityTaskCanceled {
        scheduled_event_id: i64,
    },
    TimerStarted {
        timer_id: String,
        start_to_fire_timeout: u64,
    },
    TimerFired {
        timer_id: String,
    },
    TimerCanceled {
        timer_id: String,
    },
    SignalExternalWorkflowExecutionInitiated {
        target_workflow_id: String,
        signal_name: String,
    },
    SignalExternalWorkflowExecutionFailed {
        cause: String,
    },
    ExternalWorkflowExecutionSignaled,
    WorkflowExecutionSignaled {
        signal_name: String,
        input: Vec<u8>,
    },
    MarkerRecorded {
        marker_name: String,
        details: Vec<u8>,
    },
    ChildWorkflowExecutionStarted {
        workflow_id: String,
        workflow_type: String,
    },
    ChildWorkflowExecutionCompleted {
        result: Vec<u8>,
        workflow_id: String,
    },
    ChildWorkflowExecutionFailed {
        reason: String,
        workflow_id: String,
    },
    ChildWorkflowExecutionCanceled {
        workflow_id: String,
    },
    ChildWorkflowExecutionTimedOut {
        workflow_id: String,
    },
    StartChildWorkflowExecutionInitiated {
        workflow_id: String,
        workflow_type: String,
    },
    StartChildWorkflowExecutionFailed {
        cause: String,
        workflow_id: String,
    },
    UpsertWorkflowSearchAttributes {
        attributes: HashMap<String, Vec<u8>>,
    },
    WorkflowPropertiesModified {
        build_id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutType {
    StartToClose,
    ScheduleToStart,
    ScheduleToClose,
    Heartbeat,
}

// ═══════════════════════════════════════════════════════════════════════════════
// History Event
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct HistoryEvent {
    pub event_id: i64,
    pub event_type: HistoryEventType,
    pub timestamp: i64,
    pub task_id: i64,
    pub version: i64,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Event Applier — applies events to mutable state
// ═══════════════════════════════════════════════════════════════════════════════

pub struct EventApplier {
    pub state: RwLock<AppliedState>,
    pub stats: EventApplierStats,
}

#[derive(Debug, Clone)]
pub struct AppliedState {
    pub workflow_started: bool,
    pub workflow_completed: bool,
    pub workflow_failed: bool,
    pub workflow_canceled: bool,
    pub workflow_terminated: bool,
    pub workflow_continued_as_new: bool,
    pub last_event_id: i64,
    pub activities: HashMap<String, AppliedActivity>,
    pub timers: HashMap<String, AppliedTimer>,
    pub child_workflows: HashMap<String, AppliedChildWorkflow>,
    pub signals_received: Vec<AppliedSignal>,
    pub markers: Vec<AppliedMarker>,
    pub search_attributes: HashMap<String, Vec<u8>>,
    pub event_count: u64,
}

#[derive(Debug, Clone)]
pub struct AppliedActivity {
    pub activity_id: String,
    pub activity_type: String,
    pub task_queue: String,
    pub state: AppliedActivityState,
    pub attempt: u32,
    pub result: Option<Vec<u8>>,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppliedActivityState {
    Scheduled,
    Started,
    Completed,
    Failed,
    TimedOut,
    Canceled,
}

#[derive(Debug, Clone)]
pub struct AppliedTimer {
    pub timer_id: String,
    pub fire_timeout: u64,
    pub state: AppliedTimerState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppliedTimerState {
    Started,
    Fired,
    Canceled,
}

#[derive(Debug, Clone)]
pub struct AppliedChildWorkflow {
    pub workflow_id: String,
    pub workflow_type: String,
    pub state: AppliedChildState,
    pub result: Option<Vec<u8>>,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppliedChildState {
    Initiated,
    Started,
    Completed,
    Failed,
    Canceled,
    TimedOut,
}

#[derive(Debug, Clone)]
pub struct AppliedSignal {
    pub signal_name: String,
    pub input: Vec<u8>,
    pub received_at: i64,
}

#[derive(Debug, Clone)]
pub struct AppliedMarker {
    pub name: String,
    pub details: Vec<u8>,
    pub recorded_at: i64,
}

#[derive(Debug, Default)]
pub struct EventApplierStats {
    pub events_applied: AtomicU64,
    pub replay_events_applied: AtomicU64,
    pub application_errors: AtomicU64,
}

impl AppliedState {
    pub fn new() -> Self {
        Self {
            workflow_started: false,
            workflow_completed: false,
            workflow_failed: false,
            workflow_canceled: false,
            workflow_terminated: false,
            workflow_continued_as_new: false,
            last_event_id: 0,
            activities: HashMap::new(),
            timers: HashMap::new(),
            child_workflows: HashMap::new(),
            signals_received: Vec::new(),
            markers: Vec::new(),
            search_attributes: HashMap::new(),
            event_count: 0,
        }
    }
}

impl EventApplier {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(AppliedState::new()),
            stats: EventApplierStats::default(),
        }
    }

    pub fn apply_event(&self, event: &HistoryEvent) -> Result<(), ApplyError> {
        let mut state = self.state.write().unwrap();
        state.last_event_id = event.event_id;
        state.event_count += 1;
        self.stats.events_applied.fetch_add(1, Ordering::Relaxed);

        match &event.event_type {
            HistoryEventType::WorkflowExecutionStarted { .. } => {
                state.workflow_started = true;
            }
            HistoryEventType::WorkflowExecutionCompleted { result: _ } => {
                state.workflow_completed = true;
            }
            HistoryEventType::WorkflowExecutionFailed { reason: _, .. } => {
                state.workflow_failed = true;
            }
            HistoryEventType::WorkflowExecutionTimedOut => {
                state.workflow_failed = true;
            }
            HistoryEventType::WorkflowExecutionCanceled { .. } => {
                state.workflow_canceled = true;
            }
            HistoryEventType::WorkflowExecutionTerminated { .. } => {
                state.workflow_terminated = true;
            }
            HistoryEventType::WorkflowExecutionContinuedAsNew { .. } => {
                state.workflow_continued_as_new = true;
            }
            HistoryEventType::ActivityTaskScheduled {
                activity_id,
                activity_type,
                task_queue,
            } => {
                state.activities.insert(
                    activity_id.clone(),
                    AppliedActivity {
                        activity_id: activity_id.clone(),
                        activity_type: activity_type.clone(),
                        task_queue: task_queue.clone(),
                        state: AppliedActivityState::Scheduled,
                        attempt: 0,
                        result: None,
                        failure_reason: None,
                    },
                );
            }
            HistoryEventType::ActivityTaskStarted { attempt, .. } => {
                let act_id = self.find_activity_by_event(&state, event.event_id);
                if let Some(act) = act_id.and_then(|id| state.activities.get_mut(&id)) {
                    act.state = AppliedActivityState::Started;
                    act.attempt = *attempt;
                }
            }
            HistoryEventType::ActivityTaskCompleted {
                result,
                scheduled_event_id: _,
            } => {
                if let Some(act) = state
                    .activities
                    .values_mut()
                    .find(|a| a.state == AppliedActivityState::Started)
                {
                    act.state = AppliedActivityState::Completed;
                    act.result = Some(result.clone());
                }
            }
            HistoryEventType::ActivityTaskFailed { reason, .. } => {
                if let Some(act) = state
                    .activities
                    .values_mut()
                    .find(|a| a.state == AppliedActivityState::Started)
                {
                    act.state = AppliedActivityState::Failed;
                    act.failure_reason = Some(reason.clone());
                }
            }
            HistoryEventType::ActivityTaskTimedOut { .. } => {
                if let Some(act) = state
                    .activities
                    .values_mut()
                    .find(|a| a.state == AppliedActivityState::Started)
                {
                    act.state = AppliedActivityState::TimedOut;
                }
            }
            HistoryEventType::ActivityTaskCanceled { .. } => {
                if let Some(act) = state.activities.values_mut().find(|a| {
                    a.state == AppliedActivityState::Started
                        || a.state == AppliedActivityState::Scheduled
                }) {
                    act.state = AppliedActivityState::Canceled;
                }
            }
            HistoryEventType::TimerStarted {
                timer_id,
                start_to_fire_timeout,
            } => {
                state.timers.insert(
                    timer_id.clone(),
                    AppliedTimer {
                        timer_id: timer_id.clone(),
                        fire_timeout: *start_to_fire_timeout,
                        state: AppliedTimerState::Started,
                    },
                );
            }
            HistoryEventType::TimerFired { timer_id } => {
                if let Some(t) = state.timers.get_mut(timer_id) {
                    t.state = AppliedTimerState::Fired;
                }
            }
            HistoryEventType::TimerCanceled { timer_id } => {
                if let Some(t) = state.timers.get_mut(timer_id) {
                    t.state = AppliedTimerState::Canceled;
                }
            }
            HistoryEventType::WorkflowExecutionSignaled { signal_name, input } => {
                state.signals_received.push(AppliedSignal {
                    signal_name: signal_name.clone(),
                    input: input.clone(),
                    received_at: event.timestamp,
                });
            }
            HistoryEventType::MarkerRecorded {
                marker_name,
                details,
            } => {
                state.markers.push(AppliedMarker {
                    name: marker_name.clone(),
                    details: details.clone(),
                    recorded_at: event.timestamp,
                });
            }
            HistoryEventType::StartChildWorkflowExecutionInitiated {
                workflow_id,
                workflow_type,
            } => {
                state.child_workflows.insert(
                    workflow_id.clone(),
                    AppliedChildWorkflow {
                        workflow_id: workflow_id.clone(),
                        workflow_type: workflow_type.clone(),
                        state: AppliedChildState::Initiated,
                        result: None,
                        failure_reason: None,
                    },
                );
            }
            HistoryEventType::ChildWorkflowExecutionStarted { workflow_id, .. } => {
                if let Some(cw) = state.child_workflows.get_mut(workflow_id) {
                    cw.state = AppliedChildState::Started;
                }
            }
            HistoryEventType::ChildWorkflowExecutionCompleted {
                result,
                workflow_id,
            } => {
                if let Some(cw) = state.child_workflows.get_mut(workflow_id) {
                    cw.state = AppliedChildState::Completed;
                    cw.result = Some(result.clone());
                }
            }
            HistoryEventType::ChildWorkflowExecutionFailed {
                reason,
                workflow_id,
            } => {
                if let Some(cw) = state.child_workflows.get_mut(workflow_id) {
                    cw.state = AppliedChildState::Failed;
                    cw.failure_reason = Some(reason.clone());
                }
            }
            HistoryEventType::UpsertWorkflowSearchAttributes { attributes } => {
                state.search_attributes.extend(attributes.clone());
            }
            _ => {}
        }
        Ok(())
    }

    fn find_activity_by_event(&self, state: &AppliedState, _event_id: i64) -> Option<String> {
        state.activities.keys().next().cloned()
    }

    pub fn apply_events(&self, events: &[HistoryEvent]) -> Result<u64, ApplyError> {
        for event in events {
            self.apply_event(event)?;
        }
        Ok(self.state.read().unwrap().event_count)
    }

    pub fn is_workflow_complete(&self) -> bool {
        let s = self.state.read().unwrap();
        s.workflow_completed
            || s.workflow_failed
            || s.workflow_canceled
            || s.workflow_terminated
            || s.workflow_continued_as_new
    }

    pub fn pending_activities(&self) -> Vec<String> {
        self.state
            .read()
            .unwrap()
            .activities
            .iter()
            .filter(|(_, a)| {
                a.state == AppliedActivityState::Scheduled
                    || a.state == AppliedActivityState::Started
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn pending_timers(&self) -> Vec<String> {
        self.state
            .read()
            .unwrap()
            .timers
            .iter()
            .filter(|(_, t)| t.state == AppliedTimerState::Started)
            .map(|(id, _)| id.clone())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct ApplyError {
    pub event_id: i64,
    pub message: String,
}

#[allow(dead_code)]
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(id: i64, event_type: HistoryEventType) -> HistoryEvent {
        HistoryEvent {
            event_id: id,
            event_type,
            timestamp: now_millis(),
            task_id: 1,
            version: 1,
        }
    }

    #[test]
    fn test_apply_workflow_started() {
        let applier = EventApplier::new();
        applier
            .apply_event(&make_event(
                1,
                HistoryEventType::WorkflowExecutionStarted {
                    workflow_type: "TestWF".into(),
                    task_queue: "q".into(),
                    input: vec![],
                    run_timeout: 60,
                    task_timeout: 10,
                },
            ))
            .unwrap();
        assert!(applier.state.read().unwrap().workflow_started);
    }

    #[test]
    fn test_apply_workflow_completed() {
        let applier = EventApplier::new();
        applier
            .apply_event(&make_event(
                1,
                HistoryEventType::WorkflowExecutionStarted {
                    workflow_type: "WF".into(),
                    task_queue: "q".into(),
                    input: vec![],
                    run_timeout: 60,
                    task_timeout: 10,
                },
            ))
            .unwrap();
        applier
            .apply_event(&make_event(
                2,
                HistoryEventType::WorkflowExecutionCompleted {
                    result: vec![1, 2, 3],
                },
            ))
            .unwrap();
        assert!(applier.is_workflow_complete());
    }

    #[test]
    fn test_apply_activity_lifecycle() {
        let applier = EventApplier::new();
        applier
            .apply_event(&make_event(
                1,
                HistoryEventType::ActivityTaskScheduled {
                    activity_id: "a1".into(),
                    activity_type: "SendEmail".into(),
                    task_queue: "emails".into(),
                },
            ))
            .unwrap();
        applier
            .apply_event(&make_event(
                2,
                HistoryEventType::ActivityTaskStarted {
                    attempt: 1,
                    identity: "worker-1".into(),
                },
            ))
            .unwrap();
        applier
            .apply_event(&make_event(
                3,
                HistoryEventType::ActivityTaskCompleted {
                    result: vec![],
                    scheduled_event_id: 1,
                },
            ))
            .unwrap();
        assert!(applier.pending_activities().is_empty());
    }

    #[test]
    fn test_apply_timer_lifecycle() {
        let applier = EventApplier::new();
        applier
            .apply_event(&make_event(
                1,
                HistoryEventType::TimerStarted {
                    timer_id: "t1".into(),
                    start_to_fire_timeout: 30,
                },
            ))
            .unwrap();
        assert_eq!(applier.pending_timers().len(), 1);
        applier
            .apply_event(&make_event(
                2,
                HistoryEventType::TimerFired {
                    timer_id: "t1".into(),
                },
            ))
            .unwrap();
        assert!(applier.pending_timers().is_empty());
    }

    #[test]
    fn test_apply_signal() {
        let applier = EventApplier::new();
        applier
            .apply_event(&make_event(
                1,
                HistoryEventType::WorkflowExecutionSignaled {
                    signal_name: "approve".into(),
                    input: vec![1],
                },
            ))
            .unwrap();
        assert_eq!(applier.state.read().unwrap().signals_received.len(), 1);
    }

    #[test]
    fn test_apply_child_workflow() {
        let applier = EventApplier::new();
        applier
            .apply_event(&make_event(
                1,
                HistoryEventType::StartChildWorkflowExecutionInitiated {
                    workflow_id: "child-1".into(),
                    workflow_type: "ChildWF".into(),
                },
            ))
            .unwrap();
        applier
            .apply_event(&make_event(
                2,
                HistoryEventType::ChildWorkflowExecutionStarted {
                    workflow_id: "child-1".into(),
                    workflow_type: "ChildWF".into(),
                },
            ))
            .unwrap();
        applier
            .apply_event(&make_event(
                3,
                HistoryEventType::ChildWorkflowExecutionCompleted {
                    result: vec![],
                    workflow_id: "child-1".into(),
                },
            ))
            .unwrap();
        let cw = applier
            .state
            .read()
            .unwrap()
            .child_workflows
            .get("child-1")
            .unwrap()
            .clone();
        assert_eq!(cw.state, AppliedChildState::Completed);
    }

    #[test]
    fn test_apply_search_attributes() {
        let applier = EventApplier::new();
        let mut attrs = HashMap::new();
        attrs.insert("env".into(), vec![116, 101, 115, 116]); // "test"
        applier
            .apply_event(&make_event(
                1,
                HistoryEventType::UpsertWorkflowSearchAttributes { attributes: attrs },
            ))
            .unwrap();
        assert!(applier
            .state
            .read()
            .unwrap()
            .search_attributes
            .contains_key("env"));
    }

    #[test]
    fn test_apply_batch() {
        let applier = EventApplier::new();
        let events = vec![
            make_event(
                1,
                HistoryEventType::WorkflowExecutionStarted {
                    workflow_type: "WF".into(),
                    task_queue: "q".into(),
                    input: vec![],
                    run_timeout: 60,
                    task_timeout: 10,
                },
            ),
            make_event(
                2,
                HistoryEventType::TimerStarted {
                    timer_id: "t1".into(),
                    start_to_fire_timeout: 5,
                },
            ),
            make_event(
                3,
                HistoryEventType::TimerFired {
                    timer_id: "t1".into(),
                },
            ),
            make_event(
                4,
                HistoryEventType::WorkflowExecutionCompleted { result: vec![] },
            ),
        ];
        let count = applier.apply_events(&events).unwrap();
        assert_eq!(count, 4);
        assert!(applier.is_workflow_complete());
    }

    #[test]
    fn test_apply_marker() {
        let applier = EventApplier::new();
        applier
            .apply_event(&make_event(
                1,
                HistoryEventType::MarkerRecorded {
                    marker_name: "checkpoint".into(),
                    details: vec![1, 2],
                },
            ))
            .unwrap();
        assert_eq!(applier.state.read().unwrap().markers.len(), 1);
    }
}
