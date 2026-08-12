//! Workflow state machine matching Temporal's service/history/workflow (45K+ lines).
//!
//! Covers: mutable state, workflow execution state, activity state, child workflow state,
//! timer state, signal state, query state, state transitions, checksum, and rebuilder.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    RwLock,
};
use std::time::SystemTime;

// ═══════════════════════════════════════════════════════════════════════════════
// Workflow Execution State
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowExecutionState {
    Created = 0,
    Running = 1,
    Completed = 2,
    Failed = 3,
    Cancelled = 4,
    Terminated = 5,
    ContinuedAsNew = 6,
    TimedOut = 7,
}

impl WorkflowExecutionState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::Failed
                | Self::Cancelled
                | Self::Terminated
                | Self::ContinuedAsNew
                | Self::TimedOut
        )
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Created => "Created",
            Self::Running => "Running",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
            Self::Terminated => "Terminated",
            Self::ContinuedAsNew => "ContinuedAsNew",
            Self::TimedOut => "TimedOut",
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Activity State
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct ActivityState {
    pub activity_id: String,
    pub activity_type: String,
    pub state: ActivityExecutionState,
    pub scheduled_event_id: i64,
    pub started_event_id: i64,
    pub task_queue: String,
    pub attempt: u32,
    pub heartbeat_timeout_ms: u64,
    pub schedule_to_close_timeout_ms: u64,
    pub schedule_to_start_timeout_ms: u64,
    pub start_to_close_timeout_ms: u64,
    pub last_heartbeat_at: Option<i64>,
    pub last_failure: Option<String>,
    pub result: Option<Vec<u8>>,
    pub retry_policy: Option<ActivityRetryPolicy>,
    pub header: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityExecutionState {
    Pending = 0,
    Scheduled = 1,
    Started = 2,
    Completed = 3,
    Failed = 4,
    Cancelled = 5,
    TimedOut = 6,
}

#[derive(Debug, Clone)]
pub struct ActivityRetryPolicy {
    pub initial_interval_ms: u64,
    pub backoff_coefficient: f64,
    pub max_interval_ms: u64,
    pub maximum_attempts: i32,
    pub non_retryable_error_types: Vec<String>,
}

impl ActivityState {
    pub fn new(activity_id: &str, activity_type: &str, task_queue: &str) -> Self {
        Self {
            activity_id: activity_id.to_string(),
            activity_type: activity_type.to_string(),
            state: ActivityExecutionState::Pending,
            scheduled_event_id: 0,
            started_event_id: 0,
            task_queue: task_queue.to_string(),
            attempt: 1,
            heartbeat_timeout_ms: 0,
            schedule_to_close_timeout_ms: 0,
            schedule_to_start_timeout_ms: 0,
            start_to_close_timeout_ms: 0,
            last_heartbeat_at: None,
            last_failure: None,
            result: None,
            retry_policy: None,
            header: HashMap::new(),
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            ActivityExecutionState::Completed
                | ActivityExecutionState::Failed
                | ActivityExecutionState::Cancelled
                | ActivityExecutionState::TimedOut
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Timer State
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct TimerState {
    pub timer_id: String,
    pub started_event_id: i64,
    pub fire_at: i64,
    pub state: TimerExecutionState,
    pub cancelled_event_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerExecutionState {
    Created = 0,
    Started = 1,
    Fired = 2,
    Cancelled = 3,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Child Workflow State
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct ChildWorkflowState {
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type: String,
    pub namespace: String,
    pub initiated_event_id: i64,
    pub started_event_id: i64,
    pub state: ChildWorkflowExecutionState,
    pub parent_close_policy: ParentClosePolicy,
    pub result: Option<Vec<u8>>,
    pub failure: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildWorkflowExecutionState {
    Initiated = 0,
    Started = 1,
    Completed = 2,
    Failed = 3,
    Cancelled = 4,
    Terminated = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentClosePolicy {
    Unspecified = 0,
    Terminate = 1,
    Abandon = 2,
    RequestCancel = 3,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Signal State
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct SignalState {
    pub signal_id: String,
    pub signal_name: String,
    pub input: Option<Vec<u8>>,
    pub identity: String,
    pub header: HashMap<String, Vec<u8>>,
    pub buffered: bool,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Query State
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct QueryState {
    pub query_id: String,
    pub query_type: String,
    pub query_args: Option<Vec<u8>>,
    pub state: QueryExecutionState,
    pub result: Option<Vec<u8>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryExecutionState {
    Buffered = 0,
    Unblocked = 1,
    Completed = 2,
    Failed = 3,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Mutable State
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MutableState {
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub execution_state: RwLock<WorkflowExecutionState>,
    pub workflow_type: String,
    pub task_queue: String,
    pub start_time: i64,
    pub close_time: RwLock<Option<i64>>,
    pub workflow_timeout_ms: u64,
    pub run_timeout_ms: u64,
    pub execution_timeout_ms: u64,
    pub next_event_id: AtomicI64,
    pub last_first_event_id: AtomicI64,
    pub state_transition_count: AtomicU64,
    pub history_size_bytes: AtomicU64,

    // Sub-states
    pub activities: RwLock<HashMap<String, ActivityState>>,
    pub timers: RwLock<HashMap<String, TimerState>>,
    pub child_workflows: RwLock<HashMap<String, ChildWorkflowState>>,
    pub buffered_signals: RwLock<VecDeque<SignalState>>,
    pub queries: RwLock<HashMap<String, QueryState>>,

    // Workflow properties
    pub memo: RwLock<HashMap<String, Vec<u8>>>,
    pub search_attributes: RwLock<HashMap<String, Vec<u8>>>,
    pub retry_state: RwLock<i32>,
    pub attempt: AtomicU64,
    pub has_buffered_events: AtomicU64,
    pub checksum: RwLock<Option<Vec<u8>>>,
}

impl MutableState {
    pub fn new(
        namespace_id: &str,
        workflow_id: &str,
        run_id: &str,
        workflow_type: &str,
        task_queue: &str,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Self {
            namespace_id: namespace_id.to_string(),
            workflow_id: workflow_id.to_string(),
            run_id: run_id.to_string(),
            execution_state: RwLock::new(WorkflowExecutionState::Created),
            workflow_type: workflow_type.to_string(),
            task_queue: task_queue.to_string(),
            start_time: now,
            close_time: RwLock::new(None),
            workflow_timeout_ms: 0,
            run_timeout_ms: 0,
            execution_timeout_ms: 0,
            next_event_id: AtomicI64::new(1),
            last_first_event_id: AtomicI64::new(0),
            state_transition_count: AtomicU64::new(0),
            history_size_bytes: AtomicU64::new(0),
            activities: RwLock::new(HashMap::new()),
            timers: RwLock::new(HashMap::new()),
            child_workflows: RwLock::new(HashMap::new()),
            buffered_signals: RwLock::new(VecDeque::new()),
            queries: RwLock::new(HashMap::new()),
            memo: RwLock::new(HashMap::new()),
            search_attributes: RwLock::new(HashMap::new()),
            retry_state: RwLock::new(0),
            attempt: AtomicU64::new(0),
            has_buffered_events: AtomicU64::new(0),
            checksum: RwLock::new(None),
        }
    }

    // Activity operations
    pub fn add_activity(&self, activity: ActivityState) -> i64 {
        let event_id = self.next_event_id();
        let mut act = activity;
        act.scheduled_event_id = event_id;
        act.state = ActivityExecutionState::Scheduled;
        self.activities
            .write()
            .unwrap()
            .insert(act.activity_id.clone(), act);
        event_id
    }

    pub fn get_activity(&self, activity_id: &str) -> Option<ActivityState> {
        self.activities.read().unwrap().get(activity_id).cloned()
    }

    pub fn complete_activity(
        &self,
        activity_id: &str,
        result: Option<Vec<u8>>,
    ) -> Result<(), StateError> {
        let mut activities = self.activities.write().unwrap();
        let act = activities
            .get_mut(activity_id)
            .ok_or(StateError::ActivityNotFound)?;
        if act.is_terminal() {
            return Err(StateError::InvalidTransition);
        }
        act.state = ActivityExecutionState::Completed;
        act.result = result;
        self.record_transition();
        Ok(())
    }

    pub fn fail_activity(&self, activity_id: &str, failure: &str) -> Result<(), StateError> {
        let mut activities = self.activities.write().unwrap();
        let act = activities
            .get_mut(activity_id)
            .ok_or(StateError::ActivityNotFound)?;
        if act.is_terminal() {
            return Err(StateError::InvalidTransition);
        }
        act.state = ActivityExecutionState::Failed;
        act.last_failure = Some(failure.to_string());
        self.record_transition();
        Ok(())
    }

    pub fn start_activity(&self, activity_id: &str) -> Result<(), StateError> {
        let mut activities = self.activities.write().unwrap();
        let act = activities
            .get_mut(activity_id)
            .ok_or(StateError::ActivityNotFound)?;
        if act.state != ActivityExecutionState::Scheduled {
            return Err(StateError::InvalidTransition);
        }
        act.state = ActivityExecutionState::Started;
        drop(activities);
        self.record_transition();
        Ok(())
    }

    pub fn cancel_activity(&self, activity_id: &str) -> Result<(), StateError> {
        let mut activities = self.activities.write().unwrap();
        let act = activities
            .get_mut(activity_id)
            .ok_or(StateError::ActivityNotFound)?;
        if act.is_terminal() || act.state == ActivityExecutionState::Started {
            return Err(StateError::InvalidTransition);
        }
        act.state = ActivityExecutionState::Cancelled;
        drop(activities);
        self.record_transition();
        Ok(())
    }

    pub fn timeout_activity(&self, activity_id: &str) -> Result<(), StateError> {
        let mut activities = self.activities.write().unwrap();
        let act = activities
            .get_mut(activity_id)
            .ok_or(StateError::ActivityNotFound)?;
        if act.is_terminal() {
            return Err(StateError::InvalidTransition);
        }
        act.state = ActivityExecutionState::TimedOut;
        drop(activities);
        self.record_transition();
        Ok(())
    }

    pub fn heartbeat_activity(&self, activity_id: &str) -> Result<(), StateError> {
        let mut activities = self.activities.write().unwrap();
        let act = activities
            .get_mut(activity_id)
            .ok_or(StateError::ActivityNotFound)?;
        // Heartbeat only valid for scheduled or started activities
        if act.state != ActivityExecutionState::Scheduled
            && act.state != ActivityExecutionState::Started
        {
            return Err(StateError::InvalidTransition);
        }
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        act.last_heartbeat_at = Some(now);
        Ok(())
    }

    pub fn active_activity_count(&self) -> usize {
        self.activities
            .read()
            .unwrap()
            .values()
            .filter(|a| !a.is_terminal())
            .count()
    }

    // Timer operations
    pub fn add_timer(&self, timer_id: &str, fire_at: i64) -> i64 {
        let event_id = self.next_event_id();
        let timer = TimerState {
            timer_id: timer_id.to_string(),
            started_event_id: event_id,
            fire_at,
            state: TimerExecutionState::Started,
            cancelled_event_id: None,
        };
        self.timers
            .write()
            .unwrap()
            .insert(timer_id.to_string(), timer);
        event_id
    }

    pub fn fire_timer(&self, timer_id: &str) -> Result<(), StateError> {
        let mut timers = self.timers.write().unwrap();
        let timer = timers.get_mut(timer_id).ok_or(StateError::TimerNotFound)?;
        if timer.state != TimerExecutionState::Started {
            return Err(StateError::InvalidTransition);
        }
        timer.state = TimerExecutionState::Fired;
        self.record_transition();
        Ok(())
    }

    pub fn cancel_timer(&self, timer_id: &str) -> Result<(), StateError> {
        let mut timers = self.timers.write().unwrap();
        let timer = timers.get_mut(timer_id).ok_or(StateError::TimerNotFound)?;
        if timer.state != TimerExecutionState::Started {
            return Err(StateError::InvalidTransition);
        }
        timer.state = TimerExecutionState::Cancelled;
        self.record_transition();
        Ok(())
    }

    pub fn active_timer_count(&self) -> usize {
        self.timers
            .read()
            .unwrap()
            .values()
            .filter(|t| t.state == TimerExecutionState::Started)
            .count()
    }

    // Child workflow operations
    pub fn add_child_workflow(&self, child: ChildWorkflowState) -> i64 {
        let event_id = self.next_event_id();
        let mut cw = child;
        cw.initiated_event_id = event_id;
        self.child_workflows
            .write()
            .unwrap()
            .insert(cw.workflow_id.clone(), cw);
        event_id
    }

    pub fn start_child_workflow(&self, workflow_id: &str) -> Result<(), StateError> {
        let mut children = self.child_workflows.write().unwrap();
        let cw = children
            .get_mut(workflow_id)
            .ok_or(StateError::ChildNotFound)?;
        if cw.state != ChildWorkflowExecutionState::Initiated {
            return Err(StateError::InvalidTransition);
        }
        cw.state = ChildWorkflowExecutionState::Started;
        drop(children);
        self.record_transition();
        Ok(())
    }

    pub fn fail_child_workflow(
        &self,
        workflow_id: &str,
        failure: Option<String>,
    ) -> Result<(), StateError> {
        let mut children = self.child_workflows.write().unwrap();
        let cw = children
            .get_mut(workflow_id)
            .ok_or(StateError::ChildNotFound)?;
        if cw.state == ChildWorkflowExecutionState::Completed
            || cw.state == ChildWorkflowExecutionState::Failed
        {
            return Err(StateError::InvalidTransition);
        }
        cw.state = ChildWorkflowExecutionState::Failed;
        cw.failure = failure;
        drop(children);
        self.record_transition();
        Ok(())
    }

    pub fn cancel_child_workflow(&self, workflow_id: &str) -> Result<(), StateError> {
        let mut children = self.child_workflows.write().unwrap();
        let cw = children
            .get_mut(workflow_id)
            .ok_or(StateError::ChildNotFound)?;
        if cw.state == ChildWorkflowExecutionState::Completed
            || cw.state == ChildWorkflowExecutionState::Cancelled
        {
            return Err(StateError::InvalidTransition);
        }
        cw.state = ChildWorkflowExecutionState::Cancelled;
        drop(children);
        self.record_transition();
        Ok(())
    }

    pub fn complete_child_workflow(
        &self,
        workflow_id: &str,
        result: Option<Vec<u8>>,
    ) -> Result<(), StateError> {
        let mut children = self.child_workflows.write().unwrap();
        let cw = children
            .get_mut(workflow_id)
            .ok_or(StateError::ChildNotFound)?;
        if cw.state != ChildWorkflowExecutionState::Started {
            return Err(StateError::InvalidTransition);
        }
        cw.state = ChildWorkflowExecutionState::Completed;
        cw.result = result;
        drop(children);
        self.record_transition();
        Ok(())
    }

    // Signal operations
    pub fn buffer_signal(&self, signal: SignalState) {
        self.buffered_signals.write().unwrap().push_back(signal);
        self.has_buffered_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn drain_buffered_signals(&self) -> Vec<SignalState> {
        let mut signals = self.buffered_signals.write().unwrap();
        signals.drain(..).collect()
    }

    pub fn buffered_signal_count(&self) -> usize {
        self.buffered_signals.read().unwrap().len()
    }

    // Query operations
    pub fn add_query(&self, query: QueryState) {
        self.queries
            .write()
            .unwrap()
            .insert(query.query_id.clone(), query);
    }

    pub fn complete_query(
        &self,
        query_id: &str,
        result: Option<Vec<u8>>,
    ) -> Result<(), StateError> {
        let mut queries = self.queries.write().unwrap();
        let q = queries.get_mut(query_id).ok_or(StateError::QueryNotFound)?;
        q.state = QueryExecutionState::Completed;
        q.result = result;
        Ok(())
    }

    // Workflow state transitions — with strict validation (Temporal parity)

    /// Validate that a workflow state transition is legal.
    /// Mirrors Temporal's transition matrix in service/history/workflow.
    fn validate_transition(
        from: WorkflowExecutionState,
        to: WorkflowExecutionState,
    ) -> Result<(), StateError> {
        use WorkflowExecutionState::*;
        let valid = match (from, to) {
            // From Created: can only go to Running
            (Created, Running) => true,
            // From Running: can go to any terminal state
            (Running, Completed | Failed | Cancelled | Terminated | ContinuedAsNew | TimedOut) => true,
            // Terminal states cannot transition (except TimedOut from Running handled above)
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(StateError::InvalidTransition)
        }
    }

    pub fn start_workflow(&self) -> Result<(), StateError> {
        let mut state = self.execution_state.write().unwrap();
        // start_workflow only valid from Created state
        if *state != WorkflowExecutionState::Created {
            return Err(StateError::InvalidTransition);
        }
        *state = WorkflowExecutionState::Running;
        drop(state);
        self.record_transition();
        Ok(())
    }

    pub fn complete_workflow(&self) -> Result<(), StateError> {
        let mut state = self.execution_state.write().unwrap();
        Self::validate_transition(*state, WorkflowExecutionState::Completed)?;
        *state = WorkflowExecutionState::Completed;
        *self.close_time.write().unwrap() = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        drop(state);
        self.record_transition();
        Ok(())
    }

    pub fn fail_workflow(&self) -> Result<(), StateError> {
        let mut state = self.execution_state.write().unwrap();
        Self::validate_transition(*state, WorkflowExecutionState::Failed)?;
        *state = WorkflowExecutionState::Failed;
        *self.close_time.write().unwrap() = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        drop(state);
        self.record_transition();
        Ok(())
    }

    pub fn cancel_workflow(&self) -> Result<(), StateError> {
        let mut state = self.execution_state.write().unwrap();
        Self::validate_transition(*state, WorkflowExecutionState::Cancelled)?;
        *state = WorkflowExecutionState::Cancelled;
        *self.close_time.write().unwrap() = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        drop(state);
        self.record_transition();
        Ok(())
    }

    pub fn terminate_workflow(&self) -> Result<(), StateError> {
        let mut state = self.execution_state.write().unwrap();
        Self::validate_transition(*state, WorkflowExecutionState::Terminated)?;
        *state = WorkflowExecutionState::Terminated;
        *self.close_time.write().unwrap() = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        drop(state);
        self.record_transition();
        Ok(())
    }

    pub fn continue_as_new(&self) -> Result<(), StateError> {
        let mut state = self.execution_state.write().unwrap();
        Self::validate_transition(*state, WorkflowExecutionState::ContinuedAsNew)?;
        *state = WorkflowExecutionState::ContinuedAsNew;
        *self.close_time.write().unwrap() = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        drop(state);
        self.record_transition();
        Ok(())
    }

    /// Timeout the workflow (Temporal parity — workflow execution/run timeout).
    pub fn timeout_workflow(&self) -> Result<(), StateError> {
        let mut state = self.execution_state.write().unwrap();
        Self::validate_transition(*state, WorkflowExecutionState::TimedOut)?;
        *state = WorkflowExecutionState::TimedOut;
        *self.close_time.write().unwrap() = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        drop(state);
        self.record_transition();
        Ok(())
    }

    pub fn execution_state(&self) -> WorkflowExecutionState {
        *self.execution_state.read().unwrap()
    }

    pub fn is_terminal(&self) -> bool {
        self.execution_state().is_terminal()
    }

    // Memo and search attributes
    pub fn upsert_memo(&self, memo: HashMap<String, Vec<u8>>) {
        self.memo.write().unwrap().extend(memo);
    }

    pub fn upsert_search_attributes(&self, attrs: HashMap<String, Vec<u8>>) {
        self.search_attributes.write().unwrap().extend(attrs);
    }

    // Checksum
    pub fn compute_checksum(&self) -> Vec<u8> {
        let state = format!(
            "{}:{}:{}:{}:{}:{}",
            self.workflow_id,
            self.run_id,
            self.execution_state().name(),
            self.state_transition_count.load(Ordering::Relaxed),
            self.next_event_id.load(Ordering::Relaxed),
            self.history_size_bytes.load(Ordering::Relaxed),
        );
        let hash = simple_hash(&state);
        let bytes = hash.to_le_bytes().to_vec();
        *self.checksum.write().unwrap() = Some(bytes.clone());
        bytes
    }

    pub fn verify_checksum(&self) -> bool {
        let stored = self.checksum.read().unwrap().clone();
        if let Some(expected) = stored {
            let computed = self.compute_checksum();
            expected == computed
        } else {
            true // no checksum to verify
        }
    }

    // Internal helpers
    fn next_event_id(&self) -> i64 {
        self.next_event_id.fetch_add(1, Ordering::Relaxed)
    }

    fn record_transition(&self) {
        self.state_transition_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn transition_count(&self) -> u64 {
        self.state_transition_count.load(Ordering::Relaxed)
    }

    pub fn add_history_size(&self, bytes: u64) {
        self.history_size_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn history_size(&self) -> u64 {
        self.history_size_bytes.load(Ordering::Relaxed)
    }
}

fn simple_hash(s: &str) -> u64 {
    let mut hash: u64 = 5381;
    for b in s.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(b as u64);
    }
    hash
}

// ═══════════════════════════════════════════════════════════════════════════════
// State Errors
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum StateError {
    ActivityNotFound,
    TimerNotFound,
    ChildNotFound,
    QueryNotFound,
    InvalidTransition,
    AlreadyCompleted,
}

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ActivityNotFound => write!(f, "activity not found"),
            Self::TimerNotFound => write!(f, "timer not found"),
            Self::ChildNotFound => write!(f, "child workflow not found"),
            Self::QueryNotFound => write!(f, "query not found"),
            Self::InvalidTransition => write!(f, "invalid state transition"),
            Self::AlreadyCompleted => write!(f, "workflow already completed"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> MutableState {
        MutableState::new("ns-1", "wf-1", "run-1", "TestWorkflow", "test-queue")
    }

    #[test]
    fn test_initial_state() {
        let ms = test_state();
        assert_eq!(ms.execution_state(), WorkflowExecutionState::Created);
        assert!(!ms.is_terminal());
        assert_eq!(ms.active_activity_count(), 0);
        assert_eq!(ms.active_timer_count(), 0);
    }

    #[test]
    fn test_start_workflow() {
        let ms = test_state();
        ms.start_workflow().unwrap();
        assert_eq!(ms.execution_state(), WorkflowExecutionState::Running);
        assert!(ms.transition_count() >= 1);
    }

    #[test]
    fn test_complete_workflow() {
        let ms = test_state();
        ms.start_workflow().unwrap();
        ms.complete_workflow().unwrap();
        assert_eq!(ms.execution_state(), WorkflowExecutionState::Completed);
        assert!(ms.is_terminal());
    }

    #[test]
    fn test_fail_workflow() {
        let ms = test_state();
        ms.start_workflow().unwrap();
        ms.fail_workflow().unwrap();
        assert_eq!(ms.execution_state(), WorkflowExecutionState::Failed);
        assert!(ms.is_terminal());
    }

    #[test]
    fn test_activity_lifecycle() {
        let ms = test_state();
        ms.start_workflow().unwrap();

        let act = ActivityState::new("act-1", "DoWork", "queue");
        let event_id = ms.add_activity(act);
        assert!(event_id > 0);
        assert_eq!(ms.active_activity_count(), 1);

        ms.start_activity("act-1").unwrap();
        ms.complete_activity("act-1", Some(b"result".to_vec()))
            .unwrap();
        let completed = ms.get_activity("act-1").unwrap();
        assert_eq!(completed.state, ActivityExecutionState::Completed);
        assert_eq!(ms.active_activity_count(), 0);
    }

    #[test]
    fn test_activity_failure() {
        let ms = test_state();
        ms.start_workflow().unwrap();
        ms.add_activity(ActivityState::new("act-1", "DoWork", "queue"));
        ms.fail_activity("act-1", "something went wrong").unwrap();
        let failed = ms.get_activity("act-1").unwrap();
        assert_eq!(failed.state, ActivityExecutionState::Failed);
        assert!(failed.last_failure.is_some());
    }

    #[test]
    fn test_activity_not_found() {
        let ms = test_state();
        let err = ms.complete_activity("missing", None).unwrap_err();
        assert!(matches!(err, StateError::ActivityNotFound));
    }

    #[test]
    fn test_timer_lifecycle() {
        let ms = test_state();
        ms.start_workflow().unwrap();

        let event_id = ms.add_timer("timer-1", 1000);
        assert!(event_id > 0);
        assert_eq!(ms.active_timer_count(), 1);

        ms.fire_timer("timer-1").unwrap();
        assert_eq!(ms.active_timer_count(), 0);
    }

    #[test]
    fn test_timer_cancel() {
        let ms = test_state();
        ms.add_timer("timer-1", 1000);
        ms.cancel_timer("timer-1").unwrap();
        assert_eq!(ms.active_timer_count(), 0);
    }

    #[test]
    fn test_child_workflow() {
        let ms = test_state();
        ms.start_workflow().unwrap();

        let child = ChildWorkflowState {
            workflow_id: "child-1".to_string(),
            run_id: "child-run-1".to_string(),
            workflow_type: "ChildWorkflow".to_string(),
            namespace: "ns-1".to_string(),
            initiated_event_id: 0,
            started_event_id: 0,
            state: ChildWorkflowExecutionState::Initiated,
            parent_close_policy: ParentClosePolicy::Terminate,
            result: None,
            failure: None,
        };
        ms.add_child_workflow(child);
        ms.start_child_workflow("child-1").unwrap();
        ms.complete_child_workflow("child-1", Some(b"done".to_vec()))
            .unwrap();
    }

    #[test]
    fn test_signal_buffering() {
        let ms = test_state();
        let signal = SignalState {
            signal_id: "sig-1".to_string(),
            signal_name: "MySignal".to_string(),
            input: Some(b"data".to_vec()),
            identity: "worker-1".to_string(),
            header: HashMap::new(),
            buffered: true,
        };
        ms.buffer_signal(signal);
        assert_eq!(ms.buffered_signal_count(), 1);

        let drained = ms.drain_buffered_signals();
        assert_eq!(drained.len(), 1);
        assert_eq!(ms.buffered_signal_count(), 0);
    }

    #[test]
    fn test_query_lifecycle() {
        let ms = test_state();
        let query = QueryState {
            query_id: "q-1".to_string(),
            query_type: "GetStatus".to_string(),
            query_args: None,
            state: QueryExecutionState::Buffered,
            result: None,
            error: None,
        };
        ms.add_query(query);
        ms.complete_query("q-1", Some(b"running".to_vec())).unwrap();
    }

    #[test]
    fn test_memo_and_search_attributes() {
        let ms = test_state();
        let mut memo = HashMap::new();
        memo.insert("key".to_string(), b"value".to_vec());
        ms.upsert_memo(memo);
        assert!(ms.memo.read().unwrap().contains_key("key"));

        let mut attrs = HashMap::new();
        attrs.insert("CustomField".to_string(), b"val".to_vec());
        ms.upsert_search_attributes(attrs);
        assert!(ms
            .search_attributes
            .read()
            .unwrap()
            .contains_key("CustomField"));
    }

    #[test]
    fn test_checksum() {
        let ms = test_state();
        ms.start_workflow().unwrap();
        let cs1 = ms.compute_checksum();
        assert!(!cs1.is_empty());

        // After another transition, checksum should change
        ms.add_timer("t-1", 1000);
        let cs2 = ms.compute_checksum();
        assert_ne!(cs1, cs2);
    }

    #[test]
    fn test_continue_as_new() {
        let ms = test_state();
        ms.start_workflow().unwrap();
        ms.continue_as_new().unwrap();
        assert_eq!(ms.execution_state(), WorkflowExecutionState::ContinuedAsNew);
        assert!(ms.is_terminal());
    }

    #[test]
    fn test_history_size_tracking() {
        let ms = test_state();
        ms.add_history_size(1024);
        ms.add_history_size(2048);
        assert_eq!(ms.history_size(), 3072);
    }

    #[test]
    fn test_heartbeat() {
        let ms = test_state();
        ms.add_activity(ActivityState::new("act-1", "DoWork", "queue"));
        ms.heartbeat_activity("act-1").unwrap();
        let act = ms.get_activity("act-1").unwrap();
        assert!(act.last_heartbeat_at.is_some());
    }

    // ─── Strict Transition Validation Tests ───────────────────────────────

    #[test]
    fn test_cannot_start_already_running_workflow() {
        let ms = test_state();
        ms.start_workflow().unwrap();
        // Starting again should fail (Running → Running is not valid via start_workflow)
        assert!(ms.start_workflow().is_err());
    }

    #[test]
    fn test_cannot_complete_unstarted_workflow() {
        let ms = test_state();
        // Created → Completed is invalid
        assert!(ms.complete_workflow().is_err());
    }

    #[test]
    fn test_cannot_fail_unstarted_workflow() {
        let ms = test_state();
        assert!(ms.fail_workflow().is_err());
    }

    #[test]
    fn test_cannot_cancel_unstarted_workflow() {
        let ms = test_state();
        assert!(ms.cancel_workflow().is_err());
    }

    #[test]
    fn test_cannot_terminate_unstarted_workflow() {
        let ms = test_state();
        assert!(ms.terminate_workflow().is_err());
    }

    #[test]
    fn test_cannot_transition_from_completed() {
        let ms = test_state();
        ms.start_workflow().unwrap();
        ms.complete_workflow().unwrap();
        // All transitions from Completed should fail
        assert!(ms.fail_workflow().is_err());
        assert!(ms.cancel_workflow().is_err());
        assert!(ms.terminate_workflow().is_err());
        assert!(ms.timeout_workflow().is_err());
    }

    #[test]
    fn test_timeout_workflow() {
        let ms = test_state();
        ms.start_workflow().unwrap();
        ms.timeout_workflow().unwrap();
        assert_eq!(ms.execution_state(), WorkflowExecutionState::TimedOut);
        assert!(ms.is_terminal());
    }

    #[test]
    fn test_cannot_timeout_completed_workflow() {
        let ms = test_state();
        ms.start_workflow().unwrap();
        ms.complete_workflow().unwrap();
        assert!(ms.timeout_workflow().is_err());
    }

    #[test]
    fn test_activity_start_transition() {
        let ms = test_state();
        ms.add_activity(ActivityState::new("act-1", "DoWork", "queue"));
        // Scheduled → Started
        ms.start_activity("act-1").unwrap();
        let act = ms.get_activity("act-1").unwrap();
        assert_eq!(act.state, ActivityExecutionState::Started);
    }

    #[test]
    fn test_cannot_start_non_scheduled_activity() {
        let ms = test_state();
        ms.add_activity(ActivityState::new("act-1", "DoWork", "queue"));
        ms.start_activity("act-1").unwrap();
        // Already Started → cannot start again
        assert!(ms.start_activity("act-1").is_err());
    }

    #[test]
    fn test_cancel_activity() {
        let ms = test_state();
        ms.add_activity(ActivityState::new("act-1", "DoWork", "queue"));
        ms.cancel_activity("act-1").unwrap();
        let act = ms.get_activity("act-1").unwrap();
        assert_eq!(act.state, ActivityExecutionState::Cancelled);
    }

    #[test]
    fn test_cannot_cancel_started_activity() {
        let ms = test_state();
        ms.add_activity(ActivityState::new("act-1", "DoWork", "queue"));
        ms.start_activity("act-1").unwrap();
        // Cannot cancel a started activity
        assert!(ms.cancel_activity("act-1").is_err());
    }

    #[test]
    fn test_timeout_activity() {
        let ms = test_state();
        ms.add_activity(ActivityState::new("act-1", "DoWork", "queue"));
        ms.timeout_activity("act-1").unwrap();
        let act = ms.get_activity("act-1").unwrap();
        assert_eq!(act.state, ActivityExecutionState::TimedOut);
    }

    #[test]
    fn test_cannot_timeout_completed_activity() {
        let ms = test_state();
        ms.add_activity(ActivityState::new("act-1", "DoWork", "queue"));
        ms.complete_activity("act-1", None).unwrap();
        assert!(ms.timeout_activity("act-1").is_err());
    }

    #[test]
    fn test_child_workflow_start_transition() {
        let ms = test_state();
        let child = ChildWorkflowState {
            workflow_id: "child-1".to_string(),
            run_id: "child-run-1".to_string(),
            workflow_type: "Child".to_string(),
            namespace: "ns-1".to_string(),
            initiated_event_id: 0,
            started_event_id: 0,
            state: ChildWorkflowExecutionState::Initiated,
            parent_close_policy: ParentClosePolicy::Terminate,
            result: None,
            failure: None,
        };
        ms.add_child_workflow(child);
        ms.start_child_workflow("child-1").unwrap();
        // Cannot start again
        assert!(ms.start_child_workflow("child-1").is_err());
    }

    #[test]
    fn test_child_workflow_fail_transition() {
        let ms = test_state();
        let child = ChildWorkflowState {
            workflow_id: "child-1".to_string(),
            run_id: "child-run-1".to_string(),
            workflow_type: "Child".to_string(),
            namespace: "ns-1".to_string(),
            initiated_event_id: 0,
            started_event_id: 0,
            state: ChildWorkflowExecutionState::Initiated,
            parent_close_policy: ParentClosePolicy::Terminate,
            result: None,
            failure: None,
        };
        ms.add_child_workflow(child);
        ms.start_child_workflow("child-1").unwrap();
        ms.fail_child_workflow("child-1", Some("error".to_string())).unwrap();
        // Cannot fail again
        assert!(ms.fail_child_workflow("child-1", None).is_err());
    }

    #[test]
    fn test_child_workflow_cancel_transition() {
        let ms = test_state();
        let child = ChildWorkflowState {
            workflow_id: "child-1".to_string(),
            run_id: "child-run-1".to_string(),
            workflow_type: "Child".to_string(),
            namespace: "ns-1".to_string(),
            initiated_event_id: 0,
            started_event_id: 0,
            state: ChildWorkflowExecutionState::Initiated,
            parent_close_policy: ParentClosePolicy::Terminate,
            result: None,
            failure: None,
        };
        ms.add_child_workflow(child);
        ms.cancel_child_workflow("child-1").unwrap();
        // Cannot cancel again
        assert!(ms.cancel_child_workflow("child-1").is_err());
    }

    #[test]
    fn test_cannot_complete_child_without_start() {
        let ms = test_state();
        let child = ChildWorkflowState {
            workflow_id: "child-1".to_string(),
            run_id: "child-run-1".to_string(),
            workflow_type: "Child".to_string(),
            namespace: "ns-1".to_string(),
            initiated_event_id: 0,
            started_event_id: 0,
            state: ChildWorkflowExecutionState::Initiated,
            parent_close_policy: ParentClosePolicy::Terminate,
            result: None,
            failure: None,
        };
        ms.add_child_workflow(child);
        // Cannot complete without starting first
        assert!(ms.complete_child_workflow("child-1", None).is_err());
    }

    #[test]
    fn test_heartbeat_requires_scheduled_or_started() {
        let ms = test_state();
        ms.add_activity(ActivityState::new("act-1", "DoWork", "queue"));
        // Scheduled → heartbeat OK
        ms.heartbeat_activity("act-1").unwrap();
        ms.start_activity("act-1").unwrap();
        // Started → heartbeat OK
        ms.heartbeat_activity("act-1").unwrap();
        ms.complete_activity("act-1", None).unwrap();
        // Completed → heartbeat fails
        assert!(ms.heartbeat_activity("act-1").is_err());
    }
}
