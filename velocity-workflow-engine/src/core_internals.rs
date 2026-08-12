//! Core internals — deep workflow state machine, command processing, task generation,
//! and transaction management. Matches Temporal's service/history/workflow depth.
//!
//! 1. **WorkflowMutableState**: Full mutable state with activity/timer/child/signal lifecycle.
//! 2. **CommandProcessor**: Processes workflow commands (ScheduleActivity, StartTimer, etc.).
//! 3. **TaskGenerator**: Generates transfer/timer/replication tasks from commands.
//! 4. **TransactionManager**: Snapshot/commit/rollback for mutable state mutations.
//! 5. **WorkflowTaskStateMachine**: Full workflow task scheduling/completion/failure cycle.
//! 6. **TaskRefresher**: Refreshes in-flight tasks after shard movement or replication.
//! 7. **TimerSequence**: Ordered timer management with sequence tracking.
//! 8. **MutableStateChecksum**: State integrity validation via checksumming.

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicU64, AtomicU8, Ordering},
    Mutex, RwLock,
};

// ─── 1. Workflow Mutable State ────────────────────────────────────────────────

/// Lifecycle state of an activity in mutable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityMutableState {
    None,
    Scheduled,
    Started,
    Completed,
    Failed,
    Canceled,
    TimedOut,
}

/// Full activity state in mutable state.
#[derive(Debug, Clone)]
pub struct ActivityMutableInfo {
    pub activity_id: u64,
    pub activity_type: String,
    pub state: ActivityMutableState,
    pub scheduled_event_id: u64,
    pub started_event_id: u64,
    pub attempt: u32,
    pub scheduled_time_ms: u64,
    pub started_time_ms: Option<u64>,
    pub scheduled_to_start_timeout_ms: Option<u64>,
    pub start_to_close_timeout_ms: Option<u64>,
    pub schedule_to_close_timeout_ms: Option<u64>,
    pub heartbeat_timeout_ms: Option<u64>,
    pub last_heartbeat_ms: Option<u64>,
    pub heartbeat_details: Option<Vec<u8>>,
    pub retry_policy: Option<ActivityRetryPolicyState>,
    pub task_queue: String,
    pub result: Option<Vec<u8>>,
    pub failure: Option<String>,
    /// Whether this activity is paused.
    pub is_paused: bool,
    /// Request ID for dedup.
    pub request_id: String,
}

/// Retry policy state for an activity.
#[derive(Debug, Clone)]
pub struct ActivityRetryPolicyState {
    pub max_attempts: u32,
    pub initial_interval_ms: u64,
    pub backoff_coefficient: f64,
    pub max_interval_ms: Option<u64>,
    pub non_retryable_error_types: Vec<String>,
}

impl ActivityRetryPolicyState {
    pub fn calculate_next_delay(&self, attempt: u32) -> Option<u64> {
        if attempt >= self.max_attempts {
            return None;
        }
        let delay = self.initial_interval_ms as f64 * self.backoff_coefficient.powi(attempt as i32);
        let delay_ms = delay as u64;
        Some(match self.max_interval_ms {
            Some(max) => delay_ms.min(max),
            None => delay_ms,
        })
    }
}

/// Lifecycle state of a timer in mutable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerMutableState {
    None,
    Started,
    Fired,
    Canceled,
}

/// Timer state in mutable state.
#[derive(Debug, Clone)]
pub struct TimerMutableInfo {
    pub timer_id: u64,
    pub state: TimerMutableState,
    pub started_event_id: u64,
    pub expiry_time_ms: u64,
    pub task_id: u64,
}

/// Lifecycle state of a child workflow in mutable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildWorkflowMutableState {
    None,
    Initiated,
    Started,
    Completed,
    Failed,
    Canceled,
    Terminated,
    TimedOut,
}

/// Child workflow state in mutable state.
#[derive(Debug, Clone)]
pub struct ChildWorkflowMutableInfo {
    pub workflow_key: u64,
    pub initiated_event_id: u64,
    pub started_event_id: u64,
    pub state: ChildWorkflowMutableState,
    pub namespace: String,
    pub workflow_type: String,
    pub parent_close_policy: ParentClosePolicyKind,
    pub result: Option<Vec<u8>>,
    pub failure: Option<String>,
}

/// Parent close policy kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentClosePolicyKind {
    Terminate,
    Cancel,
    Abandon,
    RequestCancel,
    TerminateIfRunning,
}

/// Pending external signal/cancel request.
#[derive(Debug, Clone)]
pub struct ExternalRequestInfo {
    pub request_id: String,
    pub target_workflow_key: u64,
    pub target_run_id: u64,
    pub request_type: ExternalRequestType,
    pub signal_name: Option<String>,
    pub signal_input: Option<Vec<u8>>,
}

/// Type of external request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalRequestType {
    Signal,
    Cancel,
}

/// The full mutable state for a workflow execution.
pub struct WorkflowMutableState {
    /// Workflow identity.
    pub workflow_key: u64,
    pub workflow_id: u64,
    pub run_id: u64,
    pub workflow_type: String,
    pub namespace_id: u64,
    pub task_queue: String,

    /// Execution state.
    pub status: AtomicU8, // 0=void, 1=running, 2=completed, etc.
    pub start_time_ms: u64,
    pub close_time_ms: Mutex<Option<u64>>,
    pub execution_time_ms: u64,

    /// Workflow task state.
    pub workflow_task_scheduled_event_id: u64,
    pub workflow_task_started_event_id: u64,
    pub workflow_task_attempt: u32,
    pub sticky_task_queue: Option<String>,
    pub sticky_schedule_to_start_timeout_ms: Option<u64>,

    /// Next event ID (monotonically increasing).
    pub next_event_id: AtomicU64,

    /// Last first transaction ID (for replication ordering).
    pub last_first_event_id: u64,

    /// Activities indexed by scheduled_event_id.
    pub activities: RwLock<HashMap<u64, ActivityMutableInfo>>,

    /// Timers indexed by timer_id.
    pub timers: RwLock<HashMap<u64, TimerMutableInfo>>,

    /// Child workflows indexed by initiated_event_id.
    pub child_workflows: RwLock<HashMap<u64, ChildWorkflowMutableInfo>>,

    /// Pending signal request IDs (for dedup).
    pub signal_request_ids: RwLock<HashSet<String>>,

    /// Pending external requests (signals/cancels to other workflows).
    pub external_requests: RwLock<Vec<ExternalRequestInfo>>,

    /// Search attributes.
    pub search_attributes: RwLock<HashMap<String, Vec<u8>>>,

    /// Memo.
    pub memo: RwLock<HashMap<String, Vec<u8>>>,

    /// Protocol messages pending completion.
    pub protocol_messages: RwLock<HashMap<String, Vec<u8>>>,

    /// Workflow execution timeout.
    pub workflow_execution_timeout_ms: Option<u64>,
    /// Workflow run timeout.
    pub workflow_run_timeout_ms: Option<u64>,
    /// Workflow task timeout.
    pub workflow_task_timeout_ms: Option<u64>,

    /// Continue-as-new info.
    pub continue_as_new_workflow_type: Option<String>,
    pub continue_as_new_task_queue: Option<String>,

    /// Stats.
    pub history_size_bytes: AtomicU64,
    pub mutation_count: AtomicU64,
}

impl WorkflowMutableState {
    pub fn new(
        workflow_key: u64,
        workflow_id: u64,
        run_id: u64,
        workflow_type: &str,
        namespace_id: u64,
        task_queue: &str,
    ) -> Self {
        Self {
            workflow_key,
            workflow_id,
            run_id,
            workflow_type: workflow_type.to_string(),
            namespace_id,
            task_queue: task_queue.to_string(),
            status: AtomicU8::new(1), // Running
            start_time_ms: now_ms(),
            close_time_ms: Mutex::new(None),
            execution_time_ms: 0,
            workflow_task_scheduled_event_id: 0,
            workflow_task_started_event_id: 0,
            workflow_task_attempt: 0,
            sticky_task_queue: None,
            sticky_schedule_to_start_timeout_ms: None,
            next_event_id: AtomicU64::new(1),
            last_first_event_id: 1,
            activities: RwLock::new(HashMap::new()),
            timers: RwLock::new(HashMap::new()),
            child_workflows: RwLock::new(HashMap::new()),
            signal_request_ids: RwLock::new(HashSet::new()),
            external_requests: RwLock::new(Vec::new()),
            search_attributes: RwLock::new(HashMap::new()),
            memo: RwLock::new(HashMap::new()),
            protocol_messages: RwLock::new(HashMap::new()),
            workflow_execution_timeout_ms: None,
            workflow_run_timeout_ms: None,
            workflow_task_timeout_ms: None,
            continue_as_new_workflow_type: None,
            continue_as_new_task_queue: None,
            history_size_bytes: AtomicU64::new(0),
            mutation_count: AtomicU64::new(0),
        }
    }

    // ─── Activity Operations ──────────────────────────────────────────────

    pub fn add_activity(&self, event_id: u64, info: ActivityMutableInfo) {
        self.activities.write().unwrap().insert(event_id, info);
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_activity(&self, event_id: u64) -> Option<ActivityMutableInfo> {
        self.activities.read().unwrap().get(&event_id).cloned()
    }

    pub fn update_activity_state(&self, event_id: u64, new_state: ActivityMutableState) -> bool {
        let mut acts = self.activities.write().unwrap();
        if let Some(act) = acts.get_mut(&event_id) {
            act.state = new_state;
            self.mutation_count.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn record_activity_heartbeat(&self, event_id: u64, details: Vec<u8>) -> bool {
        let mut acts = self.activities.write().unwrap();
        if let Some(act) = acts.get_mut(&event_id) {
            act.last_heartbeat_ms = Some(now_ms());
            act.heartbeat_details = Some(details);
            true
        } else {
            false
        }
    }

    pub fn pending_activities(&self) -> Vec<ActivityMutableInfo> {
        self.activities
            .read()
            .unwrap()
            .values()
            .filter(|a| {
                a.state == ActivityMutableState::Scheduled
                    || a.state == ActivityMutableState::Started
            })
            .cloned()
            .collect()
    }

    pub fn activity_count(&self) -> usize {
        self.activities.read().unwrap().len()
    }

    // ─── Timer Operations ─────────────────────────────────────────────────

    pub fn add_timer(&self, timer_id: u64, info: TimerMutableInfo) {
        self.timers.write().unwrap().insert(timer_id, info);
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_timer(&self, timer_id: u64) -> Option<TimerMutableInfo> {
        self.timers.read().unwrap().get(&timer_id).cloned()
    }

    pub fn fire_timer(&self, timer_id: u64) -> bool {
        let mut timers = self.timers.write().unwrap();
        if let Some(t) = timers.get_mut(&timer_id) {
            t.state = TimerMutableState::Fired;
            self.mutation_count.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn cancel_timer(&self, timer_id: u64) -> bool {
        let mut timers = self.timers.write().unwrap();
        if let Some(t) = timers.get_mut(&timer_id) {
            t.state = TimerMutableState::Canceled;
            self.mutation_count.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn pending_timers(&self) -> Vec<TimerMutableInfo> {
        self.timers
            .read()
            .unwrap()
            .values()
            .filter(|t| t.state == TimerMutableState::Started)
            .cloned()
            .collect()
    }

    // ─── Child Workflow Operations ────────────────────────────────────────

    pub fn add_child_workflow(&self, event_id: u64, info: ChildWorkflowMutableInfo) {
        self.child_workflows.write().unwrap().insert(event_id, info);
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_child_state(&self, event_id: u64, new_state: ChildWorkflowMutableState) -> bool {
        let mut children = self.child_workflows.write().unwrap();
        if let Some(c) = children.get_mut(&event_id) {
            c.state = new_state;
            self.mutation_count.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn pending_children(&self) -> Vec<ChildWorkflowMutableInfo> {
        self.child_workflows
            .read()
            .unwrap()
            .values()
            .filter(|c| {
                c.state == ChildWorkflowMutableState::Initiated
                    || c.state == ChildWorkflowMutableState::Started
            })
            .cloned()
            .collect()
    }

    // ─── Signal Dedup ─────────────────────────────────────────────────────

    pub fn add_signal_request_id(&self, request_id: &str) -> bool {
        self.signal_request_ids
            .write()
            .unwrap()
            .insert(request_id.to_string())
    }

    pub fn has_signal_request_id(&self, request_id: &str) -> bool {
        self.signal_request_ids.read().unwrap().contains(request_id)
    }

    pub fn remove_signal_request_id(&self, request_id: &str) -> bool {
        self.signal_request_ids.write().unwrap().remove(request_id)
    }

    // ─── External Requests ────────────────────────────────────────────────

    pub fn add_external_request(&self, req: ExternalRequestInfo) {
        self.external_requests.write().unwrap().push(req);
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove_external_request(&self, request_id: &str) -> bool {
        let mut reqs = self.external_requests.write().unwrap();
        let before = reqs.len();
        reqs.retain(|r| r.request_id != request_id);
        reqs.len() < before
    }

    // ─── Workflow Completion ──────────────────────────────────────────────

    pub fn complete_workflow(&self, _result: Option<Vec<u8>>) {
        self.status.store(2, Ordering::Relaxed); // Completed
        *self.close_time_ms.lock().unwrap() = Some(now_ms());
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn fail_workflow(&self, _failure: &str) {
        self.status.store(3, Ordering::Relaxed); // Failed
        *self.close_time_ms.lock().unwrap() = Some(now_ms());
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cancel_workflow(&self) {
        self.status.store(4, Ordering::Relaxed); // Canceled
        *self.close_time_ms.lock().unwrap() = Some(now_ms());
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn terminate_workflow(&self) {
        self.status.store(5, Ordering::Relaxed); // Terminated
        *self.close_time_ms.lock().unwrap() = Some(now_ms());
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn continue_as_new(&self, _new_type: &str, _new_task_queue: &str) {
        self.status.store(6, Ordering::Relaxed); // ContinuedAsNew
        *self.close_time_ms.lock().unwrap() = Some(now_ms());
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn is_running(&self) -> bool {
        self.status.load(Ordering::Relaxed) == 1
    }

    // ─── Event ID Management ──────────────────────────────────────────────

    pub fn next_event_id(&self) -> u64 {
        self.next_event_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn current_event_id(&self) -> u64 {
        self.next_event_id.load(Ordering::Relaxed)
    }

    // ─── State Summary ────────────────────────────────────────────────────

    pub fn state_summary(&self) -> MutableStateSummary {
        MutableStateSummary {
            workflow_key: self.workflow_key,
            status: self.status.load(Ordering::Relaxed),
            next_event_id: self.current_event_id(),
            activity_count: self.activity_count(),
            pending_activities: self.pending_activities().len(),
            timer_count: self.timers.read().unwrap().len(),
            pending_timers: self.pending_timers().len(),
            child_count: self.child_workflows.read().unwrap().len(),
            pending_children: self.pending_children().len(),
            signal_request_ids: self.signal_request_ids.read().unwrap().len(),
            external_requests: self.external_requests.read().unwrap().len(),
            history_size_bytes: self.history_size_bytes.load(Ordering::Relaxed),
            mutation_count: self.mutation_count.load(Ordering::Relaxed),
        }
    }
}

/// Summary snapshot of mutable state.
#[derive(Debug, Clone)]
pub struct MutableStateSummary {
    pub workflow_key: u64,
    pub status: u8,
    pub next_event_id: u64,
    pub activity_count: usize,
    pub pending_activities: usize,
    pub timer_count: usize,
    pub pending_timers: usize,
    pub child_count: usize,
    pub pending_children: usize,
    pub signal_request_ids: usize,
    pub external_requests: usize,
    pub history_size_bytes: u64,
    pub mutation_count: u64,
}

// ─── 2. Command Processor ─────────────────────────────────────────────────────

/// Workflow command types (matching Temporal's command enum).
#[derive(Debug, Clone)]
pub enum WorkflowCommand {
    ScheduleActivity(ScheduleActivityCommand),
    StartTimer(StartTimerCommand),
    CompleteWorkflow(CompleteWorkflowCommand),
    FailWorkflow(FailWorkflowCommand),
    CancelWorkflow(CancelWorkflowCommand),
    StartChildWorkflow(StartChildWorkflowCommand),
    CancelChildWorkflow(CancelChildWorkflowCommand),
    SignalExternalWorkflow(SignalExternalCommand),
    CancelExternalWorkflow(CancelExternalCommand),
    ContinueAsNew(ContinueAsNewCommand),
    CancelTimer(CancelTimerCommand),
    RequestCancelActivity(RequestCancelActivityCommand),
    ProtocolMessage(ProtocolMessageCommand),
    ModifyWorkflowProperties(ModifyPropertiesCommand),
    RecordMarker(RecordMarkerCommand),
}

#[derive(Debug, Clone)]
pub struct ScheduleActivityCommand {
    pub activity_id: u64,
    pub activity_type: String,
    pub task_queue: String,
    pub input: Option<Vec<u8>>,
    pub schedule_to_close_timeout_ms: Option<u64>,
    pub schedule_to_start_timeout_ms: Option<u64>,
    pub start_to_close_timeout_ms: Option<u64>,
    pub heartbeat_timeout_ms: Option<u64>,
    pub retry_policy: Option<ActivityRetryPolicyState>,
    pub request_id: String,
}

#[derive(Debug, Clone)]
pub struct StartTimerCommand {
    pub timer_id: u64,
    pub start_to_fire_timeout_ms: u64,
}

#[derive(Debug, Clone)]
pub struct CompleteWorkflowCommand {
    pub result: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct FailWorkflowCommand {
    pub failure: String,
    pub retry_state: u8,
}

#[derive(Debug, Clone)]
pub struct CancelWorkflowCommand {
    pub details: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct StartChildWorkflowCommand {
    pub namespace: String,
    pub workflow_type: String,
    pub workflow_id: u64,
    pub task_queue: String,
    pub input: Option<Vec<u8>>,
    pub parent_close_policy: ParentClosePolicyKind,
    pub request_id: String,
}

#[derive(Debug, Clone)]
pub struct CancelChildWorkflowCommand {
    pub child_workflow_key: u64,
}

#[derive(Debug, Clone)]
pub struct SignalExternalCommand {
    pub target_workflow_key: u64,
    pub target_run_id: u64,
    pub signal_name: String,
    pub input: Option<Vec<u8>>,
    pub request_id: String,
}

#[derive(Debug, Clone)]
pub struct CancelExternalCommand {
    pub target_workflow_key: u64,
    pub target_run_id: u64,
    pub request_id: String,
}

#[derive(Debug, Clone)]
pub struct ContinueAsNewCommand {
    pub workflow_type: String,
    pub task_queue: String,
    pub input: Option<Vec<u8>>,
    pub execution_timeout_ms: Option<u64>,
    pub run_timeout_ms: Option<u64>,
    pub task_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CancelTimerCommand {
    pub timer_id: u64,
}

#[derive(Debug, Clone)]
pub struct RequestCancelActivityCommand {
    pub activity_event_id: u64,
}

#[derive(Debug, Clone)]
pub struct ProtocolMessageCommand {
    pub protocol_instance_id: String,
    pub message_id: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ModifyPropertiesCommand {
    pub upserted_search_attributes: Option<HashMap<String, Vec<u8>>>,
    pub memo_delta: Option<HashMap<String, Vec<u8>>>,
}

#[derive(Debug, Clone)]
pub struct RecordMarkerCommand {
    pub marker_name: String,
    pub details: Option<Vec<u8>>,
}

/// Processes workflow commands against mutable state.
pub struct CommandProcessor {
    processed_commands: Mutex<Vec<ProcessedCommandRecord>>,
    total_processed: AtomicU64,
}

/// Record of a processed command.
#[derive(Debug, Clone)]
pub struct ProcessedCommandRecord {
    pub command_type: &'static str,
    pub event_id: u64,
    pub success: bool,
    pub error: Option<String>,
}

impl CommandProcessor {
    pub fn new() -> Self {
        Self {
            processed_commands: Mutex::new(Vec::new()),
            total_processed: AtomicU64::new(0),
        }
    }

    /// Process a batch of commands against mutable state. Returns generated tasks.
    pub fn process_commands(
        &self,
        state: &WorkflowMutableState,
        commands: &[WorkflowCommand],
    ) -> Vec<GeneratedTask> {
        let mut tasks = Vec::new();

        for cmd in commands {
            self.total_processed.fetch_add(1, Ordering::Relaxed);
            let event_id = state.next_event_id();

            match cmd {
                WorkflowCommand::ScheduleActivity(c) => {
                    let info = ActivityMutableInfo {
                        activity_id: c.activity_id,
                        activity_type: c.activity_type.clone(),
                        state: ActivityMutableState::Scheduled,
                        scheduled_event_id: event_id,
                        started_event_id: 0,
                        attempt: 1,
                        scheduled_time_ms: now_ms(),
                        started_time_ms: None,
                        scheduled_to_start_timeout_ms: c.schedule_to_start_timeout_ms,
                        start_to_close_timeout_ms: c.start_to_close_timeout_ms,
                        schedule_to_close_timeout_ms: c.schedule_to_close_timeout_ms,
                        heartbeat_timeout_ms: c.heartbeat_timeout_ms,
                        last_heartbeat_ms: None,
                        heartbeat_details: None,
                        retry_policy: c.retry_policy.clone(),
                        task_queue: c.task_queue.clone(),
                        result: None,
                        failure: None,
                        is_paused: false,
                        request_id: c.request_id.clone(),
                    };
                    state.add_activity(event_id, info);
                    tasks.push(GeneratedTask::Transfer(TransferTask {
                        task_type: TransferTaskType::ActivityTask,
                        workflow_key: state.workflow_key,
                        target_event_id: event_id,
                        target_task_queue: c.task_queue.clone(),
                        visibility_time_ms: now_ms(),
                    }));
                    self.log_command("ScheduleActivity", event_id, true, None);
                }

                WorkflowCommand::StartTimer(c) => {
                    let info = TimerMutableInfo {
                        timer_id: c.timer_id,
                        state: TimerMutableState::Started,
                        started_event_id: event_id,
                        expiry_time_ms: now_ms() + c.start_to_fire_timeout_ms,
                        task_id: 0,
                    };
                    state.add_timer(c.timer_id, info);
                    tasks.push(GeneratedTask::Timer(TimerTask {
                        task_type: TimerTaskType::UserTimer,
                        workflow_key: state.workflow_key,
                        timer_id: c.timer_id,
                        expiry_time_ms: now_ms() + c.start_to_fire_timeout_ms,
                    }));
                    self.log_command("StartTimer", event_id, true, None);
                }

                WorkflowCommand::CompleteWorkflow(c) => {
                    state.complete_workflow(c.result.clone());
                    tasks.push(GeneratedTask::Transfer(TransferTask {
                        task_type: TransferTaskType::CloseExecution,
                        workflow_key: state.workflow_key,
                        target_event_id: event_id,
                        target_task_queue: String::new(),
                        visibility_time_ms: now_ms(),
                    }));
                    self.log_command("CompleteWorkflow", event_id, true, None);
                }

                WorkflowCommand::FailWorkflow(c) => {
                    state.fail_workflow(&c.failure);
                    tasks.push(GeneratedTask::Transfer(TransferTask {
                        task_type: TransferTaskType::CloseExecution,
                        workflow_key: state.workflow_key,
                        target_event_id: event_id,
                        target_task_queue: String::new(),
                        visibility_time_ms: now_ms(),
                    }));
                    self.log_command("FailWorkflow", event_id, true, None);
                }

                WorkflowCommand::CancelWorkflow(_) => {
                    state.cancel_workflow();
                    tasks.push(GeneratedTask::Transfer(TransferTask {
                        task_type: TransferTaskType::CloseExecution,
                        workflow_key: state.workflow_key,
                        target_event_id: event_id,
                        target_task_queue: String::new(),
                        visibility_time_ms: now_ms(),
                    }));
                    self.log_command("CancelWorkflow", event_id, true, None);
                }

                WorkflowCommand::StartChildWorkflow(c) => {
                    let info = ChildWorkflowMutableInfo {
                        workflow_key: 0, // Assigned when child starts
                        initiated_event_id: event_id,
                        started_event_id: 0,
                        state: ChildWorkflowMutableState::Initiated,
                        namespace: c.namespace.clone(),
                        workflow_type: c.workflow_type.clone(),
                        parent_close_policy: c.parent_close_policy,
                        result: None,
                        failure: None,
                    };
                    state.add_child_workflow(event_id, info);
                    tasks.push(GeneratedTask::Transfer(TransferTask {
                        task_type: TransferTaskType::StartChildExecution,
                        workflow_key: state.workflow_key,
                        target_event_id: event_id,
                        target_task_queue: c.task_queue.clone(),
                        visibility_time_ms: now_ms(),
                    }));
                    self.log_command("StartChildWorkflow", event_id, true, None);
                }

                WorkflowCommand::CancelChildWorkflow(c) => {
                    state.update_child_state(
                        c.child_workflow_key,
                        ChildWorkflowMutableState::Canceled,
                    );
                    self.log_command("CancelChildWorkflow", event_id, true, None);
                }

                WorkflowCommand::SignalExternalWorkflow(c) => {
                    state.add_external_request(ExternalRequestInfo {
                        request_id: c.request_id.clone(),
                        target_workflow_key: c.target_workflow_key,
                        target_run_id: c.target_run_id,
                        request_type: ExternalRequestType::Signal,
                        signal_name: Some(c.signal_name.clone()),
                        signal_input: c.input.clone(),
                    });
                    tasks.push(GeneratedTask::Transfer(TransferTask {
                        task_type: TransferTaskType::SignalExternal,
                        workflow_key: state.workflow_key,
                        target_event_id: event_id,
                        target_task_queue: String::new(),
                        visibility_time_ms: now_ms(),
                    }));
                    self.log_command("SignalExternal", event_id, true, None);
                }

                WorkflowCommand::CancelExternalWorkflow(c) => {
                    state.add_external_request(ExternalRequestInfo {
                        request_id: c.request_id.clone(),
                        target_workflow_key: c.target_workflow_key,
                        target_run_id: c.target_run_id,
                        request_type: ExternalRequestType::Cancel,
                        signal_name: None,
                        signal_input: None,
                    });
                    tasks.push(GeneratedTask::Transfer(TransferTask {
                        task_type: TransferTaskType::CancelExternal,
                        workflow_key: state.workflow_key,
                        target_event_id: event_id,
                        target_task_queue: String::new(),
                        visibility_time_ms: now_ms(),
                    }));
                    self.log_command("CancelExternal", event_id, true, None);
                }

                WorkflowCommand::ContinueAsNew(c) => {
                    state.continue_as_new(&c.workflow_type, &c.task_queue);
                    tasks.push(GeneratedTask::Transfer(TransferTask {
                        task_type: TransferTaskType::ContinueAsNew,
                        workflow_key: state.workflow_key,
                        target_event_id: event_id,
                        target_task_queue: c.task_queue.clone(),
                        visibility_time_ms: now_ms(),
                    }));
                    self.log_command("ContinueAsNew", event_id, true, None);
                }

                WorkflowCommand::CancelTimer(c) => {
                    state.cancel_timer(c.timer_id);
                    self.log_command("CancelTimer", event_id, true, None);
                }

                WorkflowCommand::RequestCancelActivity(c) => {
                    state
                        .update_activity_state(c.activity_event_id, ActivityMutableState::Canceled);
                    self.log_command("RequestCancelActivity", event_id, true, None);
                }

                WorkflowCommand::ProtocolMessage(c) => {
                    state
                        .protocol_messages
                        .write()
                        .unwrap()
                        .insert(c.protocol_instance_id.clone(), c.payload.clone());
                    self.log_command("ProtocolMessage", event_id, true, None);
                }

                WorkflowCommand::ModifyWorkflowProperties(c) => {
                    if let Some(attrs) = &c.upserted_search_attributes {
                        let mut sa = state.search_attributes.write().unwrap();
                        for (k, v) in attrs {
                            sa.insert(k.clone(), v.clone());
                        }
                    }
                    if let Some(memo) = &c.memo_delta {
                        let mut m = state.memo.write().unwrap();
                        for (k, v) in memo {
                            m.insert(k.clone(), v.clone());
                        }
                    }
                    self.log_command("ModifyProperties", event_id, true, None);
                }

                WorkflowCommand::RecordMarker(_c) => {
                    // Markers are recorded in history but don't generate tasks
                    self.log_command("RecordMarker", event_id, true, None);
                }
            }
        }

        // Generate replication task for the entire batch
        if !commands.is_empty() {
            tasks.push(GeneratedTask::Replication(ReplicationTask {
                task_type: ReplicationTaskType::HistoryReplication,
                workflow_key: state.workflow_key,
                first_event_id: state.last_first_event_id,
                next_event_id: state.current_event_id(),
                branch_token: vec![],
            }));
        }

        tasks
    }

    fn log_command(
        &self,
        cmd_type: &'static str,
        event_id: u64,
        success: bool,
        error: Option<String>,
    ) {
        self.processed_commands
            .lock()
            .unwrap()
            .push(ProcessedCommandRecord {
                command_type: cmd_type,
                event_id,
                success,
                error,
            });
    }

    pub fn total_processed(&self) -> u64 {
        self.total_processed.load(Ordering::Relaxed)
    }

    pub fn command_log(&self) -> Vec<ProcessedCommandRecord> {
        self.processed_commands.lock().unwrap().clone()
    }
}

impl Default for CommandProcessor {
    fn default() -> Self {
        Self::new()
    }
}

// ─── 3. Task Generator ────────────────────────────────────────────────────────

/// A generated task from command processing.
#[derive(Debug, Clone)]
pub enum GeneratedTask {
    Transfer(TransferTask),
    Timer(TimerTask),
    Replication(ReplicationTask),
    Visibility(VisibilityTask),
}

/// Transfer task types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferTaskType {
    ActivityTask,
    StartChildExecution,
    SignalExternal,
    CancelExternal,
    CloseExecution,
    ContinueAsNew,
    RecordWorkflowStarted,
    DeleteExecution,
    UpsertSearchAttributes,
}

/// A transfer task (immediate processing).
#[derive(Debug, Clone)]
pub struct TransferTask {
    pub task_type: TransferTaskType,
    pub workflow_key: u64,
    pub target_event_id: u64,
    pub target_task_queue: String,
    pub visibility_time_ms: u64,
}

/// Timer task types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerTaskType {
    UserTimer,
    ActivityTimeout,
    WorkflowRunTimeout,
    WorkflowExecutionTimeout,
    WorkflowTaskTimeout,
    DeleteHistoryEvent,
}

/// A timer task (scheduled for future processing).
#[derive(Debug, Clone)]
pub struct TimerTask {
    pub task_type: TimerTaskType,
    pub workflow_key: u64,
    pub timer_id: u64,
    pub expiry_time_ms: u64,
}

/// Replication task types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationTaskType {
    HistoryReplication,
    SyncActivity,
    SyncWorkflowState,
}

/// A replication task.
#[derive(Debug, Clone)]
pub struct ReplicationTask {
    pub task_type: ReplicationTaskType,
    pub workflow_key: u64,
    pub first_event_id: u64,
    pub next_event_id: u64,
    pub branch_token: Vec<u8>,
}

/// Visibility task types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityTaskType {
    RecordStart,
    RecordClose,
    UpsertSearchAttributes,
    DeleteExecution,
}

/// A visibility task.
#[derive(Debug, Clone)]
pub struct VisibilityTask {
    pub task_type: VisibilityTaskType,
    pub workflow_key: u64,
    pub visibility_time_ms: u64,
}

/// Generates tasks from workflow commands and state transitions.
pub struct TaskGenerator {
    generated: Mutex<Vec<GeneratedTask>>,
    total_generated: AtomicU64,
}

impl TaskGenerator {
    pub fn new() -> Self {
        Self {
            generated: Mutex::new(Vec::new()),
            total_generated: AtomicU64::new(0),
        }
    }

    /// Generate tasks for a new workflow start.
    pub fn generate_workflow_start_tasks(
        &self,
        state: &WorkflowMutableState,
    ) -> Vec<GeneratedTask> {
        let mut tasks = Vec::new();
        self.total_generated.fetch_add(1, Ordering::Relaxed);

        // Record workflow started in visibility
        tasks.push(GeneratedTask::Visibility(VisibilityTask {
            task_type: VisibilityTaskType::RecordStart,
            workflow_key: state.workflow_key,
            visibility_time_ms: state.start_time_ms,
        }));

        // Transfer task for workflow started
        tasks.push(GeneratedTask::Transfer(TransferTask {
            task_type: TransferTaskType::RecordWorkflowStarted,
            workflow_key: state.workflow_key,
            target_event_id: 1,
            target_task_queue: state.task_queue.clone(),
            visibility_time_ms: state.start_time_ms,
        }));

        // Workflow execution timeout timer
        if let Some(timeout_ms) = state.workflow_execution_timeout_ms {
            tasks.push(GeneratedTask::Timer(TimerTask {
                task_type: TimerTaskType::WorkflowExecutionTimeout,
                workflow_key: state.workflow_key,
                timer_id: 0,
                expiry_time_ms: state.start_time_ms + timeout_ms,
            }));
        }

        // Workflow run timeout timer
        if let Some(timeout_ms) = state.workflow_run_timeout_ms {
            tasks.push(GeneratedTask::Timer(TimerTask {
                task_type: TimerTaskType::WorkflowRunTimeout,
                workflow_key: state.workflow_key,
                timer_id: 0,
                expiry_time_ms: state.start_time_ms + timeout_ms,
            }));
        }

        // Replication task
        tasks.push(GeneratedTask::Replication(ReplicationTask {
            task_type: ReplicationTaskType::HistoryReplication,
            workflow_key: state.workflow_key,
            first_event_id: 1,
            next_event_id: state.current_event_id(),
            branch_token: vec![],
        }));

        self.generated.lock().unwrap().extend(tasks.clone());
        tasks
    }

    /// Generate workflow task timeout timer.
    pub fn generate_workflow_task_timeout(
        &self,
        state: &WorkflowMutableState,
    ) -> Option<GeneratedTask> {
        let timeout_ms = state.workflow_task_timeout_ms?;
        let task = GeneratedTask::Timer(TimerTask {
            task_type: TimerTaskType::WorkflowTaskTimeout,
            workflow_key: state.workflow_key,
            timer_id: state.workflow_task_scheduled_event_id,
            expiry_time_ms: now_ms() + timeout_ms,
        });
        self.total_generated.fetch_add(1, Ordering::Relaxed);
        self.generated.lock().unwrap().push(task.clone());
        Some(task)
    }

    /// Generate activity timeout timers.
    pub fn generate_activity_timeouts(
        &self,
        activity: &ActivityMutableInfo,
        workflow_key: u64,
    ) -> Vec<GeneratedTask> {
        let mut tasks = Vec::new();
        let now = now_ms();

        if let Some(timeout) = activity.scheduled_to_start_timeout_ms {
            tasks.push(GeneratedTask::Timer(TimerTask {
                task_type: TimerTaskType::ActivityTimeout,
                workflow_key,
                timer_id: activity.scheduled_event_id,
                expiry_time_ms: activity.scheduled_time_ms + timeout,
            }));
        }

        if let Some(timeout) = activity.start_to_close_timeout_ms {
            tasks.push(GeneratedTask::Timer(TimerTask {
                task_type: TimerTaskType::ActivityTimeout,
                workflow_key,
                timer_id: activity.scheduled_event_id,
                expiry_time_ms: now + timeout, // from start time
            }));
        }

        if let Some(timeout) = activity.heartbeat_timeout_ms {
            tasks.push(GeneratedTask::Timer(TimerTask {
                task_type: TimerTaskType::ActivityTimeout,
                workflow_key,
                timer_id: activity.scheduled_event_id,
                expiry_time_ms: now + timeout,
            }));
        }

        self.total_generated.fetch_add(1, Ordering::Relaxed);
        self.generated.lock().unwrap().extend(tasks.clone());
        tasks
    }

    /// Total tasks generated.
    pub fn total_generated(&self) -> u64 {
        self.total_generated.load(Ordering::Relaxed)
    }

    /// Get all generated tasks.
    pub fn all_tasks(&self) -> Vec<GeneratedTask> {
        self.generated.lock().unwrap().clone()
    }
}

impl Default for TaskGenerator {
    fn default() -> Self {
        Self::new()
    }
}

// ─── 4. Transaction Manager ───────────────────────────────────────────────────

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Open,
    Committed,
    RolledBack,
}

/// A snapshot of mutable state for transaction rollback.
#[derive(Debug, Clone)]
pub struct MutableStateSnapshot {
    pub workflow_key: u64,
    pub status: u8,
    pub close_time_ms: Option<u64>,
    pub next_event_id: u64,
    pub activity_states: HashMap<u64, ActivityMutableState>,
    pub timer_states: HashMap<u64, TimerMutableState>,
    pub child_states: HashMap<u64, ChildWorkflowMutableState>,
    pub signal_ids: HashSet<String>,
    pub external_request_count: usize,
    pub mutation_count: u64,
}

/// Transaction manager for mutable state mutations.
pub struct TransactionManager {
    snapshots: Mutex<Vec<(u64, MutableStateSnapshot)>>,
    transactions: Mutex<HashMap<u64, TransactionInfo>>,
    next_tx_id: AtomicU64,
    total_committed: AtomicU64,
    total_rolled_back: AtomicU64,
}

/// Transaction info.
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub tx_id: u64,
    pub state: TransactionState,
    pub started_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    pub commands_processed: u32,
    pub tasks_generated: u32,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            snapshots: Mutex::new(Vec::new()),
            transactions: Mutex::new(HashMap::new()),
            next_tx_id: AtomicU64::new(1),
            total_committed: AtomicU64::new(0),
            total_rolled_back: AtomicU64::new(0),
        }
    }

    /// Begin a new transaction. Takes a snapshot of current state.
    pub fn begin(&self, state: &WorkflowMutableState) -> u64 {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::Relaxed);
        let snapshot = self.take_snapshot(state);

        self.snapshots.lock().unwrap().push((tx_id, snapshot));
        self.transactions.lock().unwrap().insert(
            tx_id,
            TransactionInfo {
                tx_id,
                state: TransactionState::Open,
                started_at_ms: now_ms(),
                completed_at_ms: None,
                commands_processed: 0,
                tasks_generated: 0,
            },
        );

        tx_id
    }

    /// Commit a transaction.
    pub fn commit(&self, tx_id: u64, commands: u32, tasks: u32) -> bool {
        let mut txns = self.transactions.lock().unwrap();
        if let Some(tx) = txns.get_mut(&tx_id) {
            if tx.state != TransactionState::Open {
                return false;
            }
            tx.state = TransactionState::Committed;
            tx.completed_at_ms = Some(now_ms());
            tx.commands_processed = commands;
            tx.tasks_generated = tasks;
            self.total_committed.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Rollback a transaction. Returns the snapshot for state restoration.
    pub fn rollback(&self, tx_id: u64) -> Option<MutableStateSnapshot> {
        let mut txns = self.transactions.lock().unwrap();
        if let Some(tx) = txns.get_mut(&tx_id) {
            if tx.state != TransactionState::Open {
                return None;
            }
            tx.state = TransactionState::RolledBack;
            tx.completed_at_ms = Some(now_ms());
            self.total_rolled_back.fetch_add(1, Ordering::Relaxed);

            // Find and return the snapshot
            let mut snapshots = self.snapshots.lock().unwrap();
            snapshots
                .iter()
                .position(|(id, _)| *id == tx_id)
                .map(|pos| snapshots.remove(pos).1)
        } else {
            None
        }
    }

    /// Apply a snapshot to restore mutable state.
    pub fn apply_snapshot(&self, snapshot: &MutableStateSnapshot, state: &WorkflowMutableState) {
        state.status.store(snapshot.status, Ordering::Relaxed);
        *state.close_time_ms.lock().unwrap() = snapshot.close_time_ms;

        // Restore activity states
        for (event_id, act_state) in &snapshot.activity_states {
            state.update_activity_state(*event_id, *act_state);
        }

        // Restore timer states
        for (timer_id, timer_state) in &snapshot.timer_states {
            match timer_state {
                TimerMutableState::Fired => {
                    state.fire_timer(*timer_id);
                }
                TimerMutableState::Canceled => {
                    state.cancel_timer(*timer_id);
                }
                _ => {}
            }
        }

        // Restore child states
        for (event_id, child_state) in &snapshot.child_states {
            state.update_child_state(*event_id, *child_state);
        }
    }

    fn take_snapshot(&self, state: &WorkflowMutableState) -> MutableStateSnapshot {
        let activity_states: HashMap<u64, ActivityMutableState> = state
            .activities
            .read()
            .unwrap()
            .iter()
            .map(|(&k, v)| (k, v.state))
            .collect();
        let timer_states: HashMap<u64, TimerMutableState> = state
            .timers
            .read()
            .unwrap()
            .iter()
            .map(|(&k, v)| (k, v.state))
            .collect();
        let child_states: HashMap<u64, ChildWorkflowMutableState> = state
            .child_workflows
            .read()
            .unwrap()
            .iter()
            .map(|(&k, v)| (k, v.state))
            .collect();
        let signal_ids: HashSet<String> = state.signal_request_ids.read().unwrap().clone();

        MutableStateSnapshot {
            workflow_key: state.workflow_key,
            status: state.status.load(Ordering::Relaxed),
            close_time_ms: *state.close_time_ms.lock().unwrap(),
            next_event_id: state.current_event_id(),
            activity_states,
            timer_states,
            child_states,
            signal_ids,
            external_request_count: state.external_requests.read().unwrap().len(),
            mutation_count: state.mutation_count.load(Ordering::Relaxed),
        }
    }

    /// Transaction stats.
    pub fn stats(&self) -> TransactionStats {
        TransactionStats {
            total_committed: self.total_committed.load(Ordering::Relaxed),
            total_rolled_back: self.total_rolled_back.load(Ordering::Relaxed),
            open_transactions: self
                .transactions
                .lock()
                .unwrap()
                .values()
                .filter(|t| t.state == TransactionState::Open)
                .count(),
        }
    }

    /// Get transaction info.
    pub fn get_transaction(&self, tx_id: u64) -> Option<TransactionInfo> {
        self.transactions.lock().unwrap().get(&tx_id).cloned()
    }
}

/// Transaction statistics.
#[derive(Debug, Clone)]
pub struct TransactionStats {
    pub total_committed: u64,
    pub total_rolled_back: u64,
    pub open_transactions: usize,
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─── 5. Workflow Task State Machine ───────────────────────────────────────────

/// Workflow task state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowTaskState {
    None,
    Scheduled,
    Started,
    Completed,
    Failed,
    TimedOut,
}

/// Full workflow task info.
#[derive(Debug, Clone)]
pub struct WorkflowTaskInfo {
    pub scheduled_event_id: u64,
    pub started_event_id: u64,
    pub state: WorkflowTaskState,
    pub attempt: u32,
    pub scheduled_time_ms: u64,
    pub started_time_ms: Option<u64>,
    pub request_id: String,
    pub task_queue: String,
    pub sticky_task_queue: Option<String>,
}

/// Manages the workflow task state machine lifecycle.
pub struct WorkflowTaskStateMachine {
    current_task: Mutex<Option<WorkflowTaskInfo>>,
    history: Mutex<Vec<WorkflowTaskInfo>>,
    total_scheduled: AtomicU64,
    total_completed: AtomicU64,
    total_failed: AtomicU64,
    total_timed_out: AtomicU64,
}

impl WorkflowTaskStateMachine {
    pub fn new() -> Self {
        Self {
            current_task: Mutex::new(None),
            history: Mutex::new(Vec::new()),
            total_scheduled: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
            total_failed: AtomicU64::new(0),
            total_timed_out: AtomicU64::new(0),
        }
    }

    /// Schedule a new workflow task.
    pub fn schedule(
        &self,
        event_id: u64,
        task_queue: &str,
        sticky: Option<&str>,
    ) -> WorkflowTaskInfo {
        let info = WorkflowTaskInfo {
            scheduled_event_id: event_id,
            started_event_id: 0,
            state: WorkflowTaskState::Scheduled,
            attempt: 1,
            scheduled_time_ms: now_ms(),
            started_time_ms: None,
            request_id: format!("wt-req-{}", event_id),
            task_queue: task_queue.to_string(),
            sticky_task_queue: sticky.map(|s| s.to_string()),
        };

        *self.current_task.lock().unwrap() = Some(info.clone());
        self.total_scheduled.fetch_add(1, Ordering::Relaxed);
        self.history.lock().unwrap().push(info.clone());
        info
    }

    /// Record that the task was started (worker picked it up).
    pub fn record_started(&self, started_event_id: u64) -> bool {
        let mut task = self.current_task.lock().unwrap();
        if let Some(ref mut t) = *task {
            if t.state != WorkflowTaskState::Scheduled {
                return false;
            }
            t.state = WorkflowTaskState::Started;
            t.started_event_id = started_event_id;
            t.started_time_ms = Some(now_ms());
            true
        } else {
            false
        }
    }

    /// Record task completion.
    pub fn record_completed(&self) -> bool {
        let mut task = self.current_task.lock().unwrap();
        if let Some(ref mut t) = *task {
            if t.state != WorkflowTaskState::Started {
                return false;
            }
            t.state = WorkflowTaskState::Completed;
            self.total_completed.fetch_add(1, Ordering::Relaxed);
            *task = None;
            true
        } else {
            false
        }
    }

    /// Record task failure.
    pub fn record_failed(&self) -> bool {
        let mut task = self.current_task.lock().unwrap();
        if let Some(ref mut t) = *task {
            t.state = WorkflowTaskState::Failed;
            self.total_failed.fetch_add(1, Ordering::Relaxed);
            *task = None;
            true
        } else {
            false
        }
    }

    /// Record task timeout.
    pub fn record_timed_out(&self) -> bool {
        let mut task = self.current_task.lock().unwrap();
        if let Some(ref mut t) = *task {
            t.state = WorkflowTaskState::TimedOut;
            self.total_timed_out.fetch_add(1, Ordering::Relaxed);
            *task = None;
            true
        } else {
            false
        }
    }

    /// Get the current workflow task.
    pub fn current(&self) -> Option<WorkflowTaskInfo> {
        self.current_task.lock().unwrap().clone()
    }

    /// Whether a workflow task is currently in-flight.
    pub fn has_pending_task(&self) -> bool {
        self.current_task.lock().unwrap().is_some()
    }

    /// Reset sticky task queue.
    pub fn reset_sticky(&self) {
        let mut task = self.current_task.lock().unwrap();
        if let Some(ref mut t) = *task {
            t.sticky_task_queue = None;
        }
    }

    /// Task history.
    pub fn task_history(&self) -> Vec<WorkflowTaskInfo> {
        self.history.lock().unwrap().clone()
    }

    /// Stats.
    pub fn stats(&self) -> WorkflowTaskStats {
        WorkflowTaskStats {
            total_scheduled: self.total_scheduled.load(Ordering::Relaxed),
            total_completed: self.total_completed.load(Ordering::Relaxed),
            total_failed: self.total_failed.load(Ordering::Relaxed),
            total_timed_out: self.total_timed_out.load(Ordering::Relaxed),
        }
    }
}

/// Workflow task state machine stats.
#[derive(Debug, Clone)]
pub struct WorkflowTaskStats {
    pub total_scheduled: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub total_timed_out: u64,
}

impl Default for WorkflowTaskStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

// ─── 6. Task Refresher ────────────────────────────────────────────────────────

/// Refreshes in-flight tasks after shard movement or replication.
pub struct TaskRefresher {
    total_refreshed: AtomicU64,
}

impl TaskRefresher {
    pub fn new() -> Self {
        Self {
            total_refreshed: AtomicU64::new(0),
        }
    }

    /// Refresh all pending activity tasks for a workflow.
    pub fn refresh_activities(&self, state: &WorkflowMutableState) -> Vec<TransferTask> {
        let pending = state.pending_activities();
        let mut tasks = Vec::new();

        for act in &pending {
            if act.state == ActivityMutableState::Scheduled {
                tasks.push(TransferTask {
                    task_type: TransferTaskType::ActivityTask,
                    workflow_key: state.workflow_key,
                    target_event_id: act.scheduled_event_id,
                    target_task_queue: act.task_queue.clone(),
                    visibility_time_ms: now_ms(),
                });
                self.total_refreshed.fetch_add(1, Ordering::Relaxed);
            }
        }

        tasks
    }

    /// Refresh pending child workflow tasks.
    pub fn refresh_children(&self, state: &WorkflowMutableState) -> Vec<TransferTask> {
        let pending = state.pending_children();
        let mut tasks = Vec::new();

        for child in &pending {
            if child.state == ChildWorkflowMutableState::Initiated {
                tasks.push(TransferTask {
                    task_type: TransferTaskType::StartChildExecution,
                    workflow_key: state.workflow_key,
                    target_event_id: child.initiated_event_id,
                    target_task_queue: String::new(),
                    visibility_time_ms: now_ms(),
                });
                self.total_refreshed.fetch_add(1, Ordering::Relaxed);
            }
        }

        tasks
    }

    /// Refresh pending timers.
    pub fn refresh_timers(&self, state: &WorkflowMutableState) -> Vec<TimerTask> {
        let pending = state.pending_timers();
        pending
            .iter()
            .map(|t| TimerTask {
                task_type: TimerTaskType::UserTimer,
                workflow_key: state.workflow_key,
                timer_id: t.timer_id,
                expiry_time_ms: t.expiry_time_ms,
            })
            .collect()
    }

    /// Refresh all tasks for a workflow.
    pub fn refresh_all(&self, state: &WorkflowMutableState) -> Vec<GeneratedTask> {
        let mut tasks: Vec<GeneratedTask> = Vec::new();
        tasks.extend(
            self.refresh_activities(state)
                .into_iter()
                .map(GeneratedTask::Transfer),
        );
        tasks.extend(
            self.refresh_children(state)
                .into_iter()
                .map(GeneratedTask::Transfer),
        );
        tasks.extend(
            self.refresh_timers(state)
                .into_iter()
                .map(GeneratedTask::Timer),
        );
        tasks
    }

    pub fn total_refreshed(&self) -> u64 {
        self.total_refreshed.load(Ordering::Relaxed)
    }
}

impl Default for TaskRefresher {
    fn default() -> Self {
        Self::new()
    }
}

// ─── 7. Timer Sequence ────────────────────────────────────────────────────────

/// Manages ordered timer sequences for a workflow.
pub struct TimerSequence {
    timers: Mutex<Vec<TimerSequenceEntry>>,
}

/// An entry in the timer sequence.
#[derive(Debug, Clone)]
pub struct TimerSequenceEntry {
    pub timer_id: u64,
    pub expiry_time_ms: u64,
    pub task_type: TimerTaskType,
    pub event_id: u64,
}

impl TimerSequence {
    pub fn new() -> Self {
        Self {
            timers: Mutex::new(Vec::new()),
        }
    }

    /// Add a timer to the sequence.
    pub fn add(&self, entry: TimerSequenceEntry) {
        let mut timers = self.timers.lock().unwrap();
        timers.push(entry);
        timers.sort_by_key(|e| e.expiry_time_ms);
    }

    /// Get the next timer to fire.
    pub fn next_to_fire(&self, now_ms: u64) -> Option<TimerSequenceEntry> {
        let timers = self.timers.lock().unwrap();
        timers
            .first()
            .filter(|e| e.expiry_time_ms <= now_ms)
            .cloned()
    }

    /// Remove a timer by ID.
    pub fn remove(&self, timer_id: u64) -> bool {
        let mut timers = self.timers.lock().unwrap();
        let before = timers.len();
        timers.retain(|e| e.timer_id != timer_id);
        timers.len() < before
    }

    /// Get all pending timers.
    pub fn pending(&self) -> Vec<TimerSequenceEntry> {
        self.timers.lock().unwrap().clone()
    }

    /// Count of pending timers.
    pub fn count(&self) -> usize {
        self.timers.lock().unwrap().len()
    }
}

impl Default for TimerSequence {
    fn default() -> Self {
        Self::new()
    }
}

// ─── 8. Mutable State Checksum ────────────────────────────────────────────────

/// Checksum for mutable state integrity validation.
pub struct MutableStateChecksum {
    last_checksum: Mutex<u64>,
    total_computed: AtomicU64,
}

impl MutableStateChecksum {
    pub fn new() -> Self {
        Self {
            last_checksum: Mutex::new(0),
            total_computed: AtomicU64::new(0),
        }
    }

    /// Compute a checksum of the mutable state.
    pub fn compute(&self, state: &WorkflowMutableState) -> u64 {
        let mut hash: u64 = state.workflow_key;
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(state.status.load(Ordering::Relaxed) as u64);
        hash = hash.wrapping_mul(31).wrapping_add(state.current_event_id());

        // Include activity states
        for (event_id, act) in state.activities.read().unwrap().iter() {
            hash = hash.wrapping_mul(31).wrapping_add(*event_id);
            hash = hash.wrapping_mul(31).wrapping_add(act.state as u64);
            hash = hash.wrapping_mul(31).wrapping_add(act.attempt as u64);
        }

        // Include timer states
        for (timer_id, timer) in state.timers.read().unwrap().iter() {
            hash = hash.wrapping_mul(31).wrapping_add(*timer_id);
            hash = hash.wrapping_mul(31).wrapping_add(timer.state as u64);
        }

        // Include child states
        for (event_id, child) in state.child_workflows.read().unwrap().iter() {
            hash = hash.wrapping_mul(31).wrapping_add(*event_id);
            hash = hash.wrapping_mul(31).wrapping_add(child.state as u64);
        }

        // Include signal count
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(state.signal_request_ids.read().unwrap().len() as u64);

        *self.last_checksum.lock().unwrap() = hash;
        self.total_computed.fetch_add(1, Ordering::Relaxed);
        hash
    }

    /// Verify state against a known checksum.
    pub fn verify(&self, state: &WorkflowMutableState, expected: u64) -> bool {
        self.compute(state) == expected
    }

    /// Get the last computed checksum.
    pub fn last(&self) -> u64 {
        *self.last_checksum.lock().unwrap()
    }

    /// Total checksums computed.
    pub fn total_computed(&self) -> u64 {
        self.total_computed.load(Ordering::Relaxed)
    }
}

impl Default for MutableStateChecksum {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Time Helper ──────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state() -> WorkflowMutableState {
        WorkflowMutableState::new(1, 100, 1000, "test-wf", 1, "test-queue")
    }

    // ─── Mutable State ────────────────────────────────────────────────────

    #[test]
    fn test_mutable_state_activity_lifecycle() {
        let state = make_state();
        let info = ActivityMutableInfo {
            activity_id: 1,
            activity_type: "greet".into(),
            state: ActivityMutableState::Scheduled,
            scheduled_event_id: 10,
            started_event_id: 0,
            attempt: 1,
            scheduled_time_ms: now_ms(),
            started_time_ms: None,
            scheduled_to_start_timeout_ms: Some(5000),
            start_to_close_timeout_ms: Some(10000),
            schedule_to_close_timeout_ms: None,
            heartbeat_timeout_ms: None,
            last_heartbeat_ms: None,
            heartbeat_details: None,
            retry_policy: None,
            task_queue: "test-queue".into(),
            result: None,
            failure: None,
            is_paused: false,
            request_id: "req-1".into(),
        };

        state.add_activity(10, info);
        assert_eq!(state.activity_count(), 1);
        assert_eq!(state.pending_activities().len(), 1);

        state.update_activity_state(10, ActivityMutableState::Started);
        assert_eq!(
            state.get_activity(10).unwrap().state,
            ActivityMutableState::Started
        );
        assert_eq!(state.pending_activities().len(), 1); // Still pending

        state.update_activity_state(10, ActivityMutableState::Completed);
        assert_eq!(state.pending_activities().len(), 0); // No longer pending
    }

    #[test]
    fn test_mutable_state_timer_lifecycle() {
        let state = make_state();
        let timer = TimerMutableInfo {
            timer_id: 1,
            state: TimerMutableState::Started,
            started_event_id: 5,
            expiry_time_ms: now_ms() + 5000,
            task_id: 0,
        };

        state.add_timer(1, timer);
        assert_eq!(state.pending_timers().len(), 1);

        state.fire_timer(1);
        assert_eq!(state.pending_timers().len(), 0);
        assert_eq!(state.get_timer(1).unwrap().state, TimerMutableState::Fired);
    }

    #[test]
    fn test_mutable_state_child_lifecycle() {
        let state = make_state();
        let child = ChildWorkflowMutableInfo {
            workflow_key: 500,
            initiated_event_id: 20,
            started_event_id: 0,
            state: ChildWorkflowMutableState::Initiated,
            namespace: "default".into(),
            workflow_type: "child-wf".into(),
            parent_close_policy: ParentClosePolicyKind::Terminate,
            result: None,
            failure: None,
        };

        state.add_child_workflow(20, child);
        assert_eq!(state.pending_children().len(), 1);

        state.update_child_state(20, ChildWorkflowMutableState::Started);
        assert_eq!(state.pending_children().len(), 1);

        state.update_child_state(20, ChildWorkflowMutableState::Completed);
        assert_eq!(state.pending_children().len(), 0);
    }

    #[test]
    fn test_signal_dedup() {
        let state = make_state();
        assert!(state.add_signal_request_id("sig-1"));
        assert!(state.has_signal_request_id("sig-1"));
        assert!(!state.add_signal_request_id("sig-1")); // Duplicate
        state.remove_signal_request_id("sig-1");
        assert!(!state.has_signal_request_id("sig-1"));
    }

    #[test]
    fn test_workflow_completion() {
        let state = make_state();
        assert!(state.is_running());
        state.complete_workflow(Some(b"ok".to_vec()));
        assert!(!state.is_running());
        assert_eq!(state.status.load(Ordering::Relaxed), 2);
        assert!(state.close_time_ms.lock().unwrap().is_some());
    }

    #[test]
    fn test_state_summary() {
        let state = make_state();
        state.add_activity(
            10,
            ActivityMutableInfo {
                activity_id: 1,
                activity_type: "a".into(),
                state: ActivityMutableState::Scheduled,
                scheduled_event_id: 10,
                started_event_id: 0,
                attempt: 1,
                scheduled_time_ms: 0,
                started_time_ms: None,
                scheduled_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                schedule_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
                last_heartbeat_ms: None,
                heartbeat_details: None,
                retry_policy: None,
                task_queue: "q".into(),
                result: None,
                failure: None,
                is_paused: false,
                request_id: "r".into(),
            },
        );

        let summary = state.state_summary();
        assert_eq!(summary.activity_count, 1);
        assert_eq!(summary.pending_activities, 1);
        assert_eq!(summary.status, 1);
    }

    // ─── Command Processor ────────────────────────────────────────────────

    #[test]
    fn test_command_schedule_activity() {
        let state = make_state();
        let proc = CommandProcessor::new();

        let commands = vec![WorkflowCommand::ScheduleActivity(ScheduleActivityCommand {
            activity_id: 1,
            activity_type: "greet".into(),
            task_queue: "activity-q".into(),
            input: None,
            schedule_to_close_timeout_ms: Some(30000),
            schedule_to_start_timeout_ms: None,
            start_to_close_timeout_ms: Some(10000),
            heartbeat_timeout_ms: None,
            retry_policy: None,
            request_id: "r1".into(),
        })];

        let tasks = proc.process_commands(&state, &commands);
        assert!(tasks.iter().any(|t| matches!(t, GeneratedTask::Transfer(tt) if tt.task_type == TransferTaskType::ActivityTask)));
        assert_eq!(state.activity_count(), 1);
        assert_eq!(proc.total_processed(), 1);
    }

    #[test]
    fn test_command_complete_workflow() {
        let state = make_state();
        let proc = CommandProcessor::new();

        let commands = vec![WorkflowCommand::CompleteWorkflow(CompleteWorkflowCommand {
            result: Some(b"done".to_vec()),
        })];

        let tasks = proc.process_commands(&state, &commands);
        assert!(tasks.iter().any(|t| matches!(t, GeneratedTask::Transfer(tt) if tt.task_type == TransferTaskType::CloseExecution)));
        assert!(!state.is_running());
    }

    #[test]
    fn test_command_start_timer() {
        let state = make_state();
        let proc = CommandProcessor::new();

        let commands = vec![WorkflowCommand::StartTimer(StartTimerCommand {
            timer_id: 42,
            start_to_fire_timeout_ms: 5000,
        })];

        let tasks = proc.process_commands(&state, &commands);
        assert!(tasks.iter().any(
            |t| matches!(t, GeneratedTask::Timer(tt) if tt.task_type == TimerTaskType::UserTimer)
        ));
        assert_eq!(state.pending_timers().len(), 1);
    }

    #[test]
    fn test_command_start_child() {
        let state = make_state();
        let proc = CommandProcessor::new();

        let commands = vec![WorkflowCommand::StartChildWorkflow(
            StartChildWorkflowCommand {
                namespace: "default".into(),
                workflow_type: "child".into(),
                workflow_id: 999,
                task_queue: "child-q".into(),
                input: None,
                parent_close_policy: ParentClosePolicyKind::Terminate,
                request_id: "cr1".into(),
            },
        )];

        let tasks = proc.process_commands(&state, &commands);
        assert!(tasks.iter().any(|t| matches!(t, GeneratedTask::Transfer(tt) if tt.task_type == TransferTaskType::StartChildExecution)));
        assert_eq!(state.pending_children().len(), 1);
    }

    #[test]
    fn test_command_modify_properties() {
        let state = make_state();
        let proc = CommandProcessor::new();

        let mut attrs = HashMap::new();
        attrs.insert("key".into(), b"value".to_vec());

        let commands = vec![WorkflowCommand::ModifyWorkflowProperties(
            ModifyPropertiesCommand {
                upserted_search_attributes: Some(attrs),
                memo_delta: None,
            },
        )];

        proc.process_commands(&state, &commands);
        assert!(state.search_attributes.read().unwrap().contains_key("key"));
    }

    // ─── Task Generator ───────────────────────────────────────────────────

    #[test]
    fn test_generate_start_tasks() {
        let state = make_state();
        let gen = TaskGenerator::new();

        let tasks = gen.generate_workflow_start_tasks(&state);
        assert!(tasks.len() >= 3); // visibility + transfer + replication
        assert!(tasks
            .iter()
            .any(|t| matches!(t, GeneratedTask::Visibility(_))));
        assert!(tasks
            .iter()
            .any(|t| matches!(t, GeneratedTask::Transfer(_))));
        assert!(tasks
            .iter()
            .any(|t| matches!(t, GeneratedTask::Replication(_))));
    }

    #[test]
    fn test_generate_timeout_timers() {
        let mut state = make_state();
        state.workflow_execution_timeout_ms = Some(60000);
        let gen = TaskGenerator::new();

        let tasks = gen.generate_workflow_start_tasks(&state);
        assert!(tasks.iter().any(|t| matches!(t, GeneratedTask::Timer(tt) if tt.task_type == TimerTaskType::WorkflowExecutionTimeout)));
    }

    // ─── Transaction Manager ──────────────────────────────────────────────

    #[test]
    fn test_transaction_commit() {
        let state = make_state();
        let mgr = TransactionManager::new();

        let tx_id = mgr.begin(&state);
        state.add_activity(
            10,
            ActivityMutableInfo {
                activity_id: 1,
                activity_type: "a".into(),
                state: ActivityMutableState::Scheduled,
                scheduled_event_id: 10,
                started_event_id: 0,
                attempt: 1,
                scheduled_time_ms: 0,
                started_time_ms: None,
                scheduled_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                schedule_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
                last_heartbeat_ms: None,
                heartbeat_details: None,
                retry_policy: None,
                task_queue: "q".into(),
                result: None,
                failure: None,
                is_paused: false,
                request_id: "r".into(),
            },
        );

        assert!(mgr.commit(tx_id, 1, 2));
        let stats = mgr.stats();
        assert_eq!(stats.total_committed, 1);
        assert_eq!(stats.total_rolled_back, 0);
    }

    #[test]
    fn test_transaction_rollback() {
        let state = make_state();
        let mgr = TransactionManager::new();

        let tx_id = mgr.begin(&state);
        state.complete_workflow(None);

        let snapshot = mgr.rollback(tx_id).unwrap();
        assert_eq!(snapshot.status, 1); // Was running before

        mgr.apply_snapshot(&snapshot, &state);
        assert!(state.is_running()); // Restored
    }

    // ─── Workflow Task State Machine ──────────────────────────────────────

    #[test]
    fn test_workflow_task_lifecycle() {
        let sm = WorkflowTaskStateMachine::new();

        let task = sm.schedule(5, "wf-queue", Some("sticky-queue"));
        assert_eq!(task.state, WorkflowTaskState::Scheduled);
        assert!(sm.has_pending_task());

        assert!(sm.record_started(6));
        assert_eq!(sm.current().unwrap().state, WorkflowTaskState::Started);

        assert!(sm.record_completed());
        assert!(!sm.has_pending_task());

        let stats = sm.stats();
        assert_eq!(stats.total_scheduled, 1);
        assert_eq!(stats.total_completed, 1);
    }

    #[test]
    fn test_workflow_task_failure() {
        let sm = WorkflowTaskStateMachine::new();
        sm.schedule(5, "q", None);
        sm.record_started(6);
        assert!(sm.record_failed());
        assert_eq!(sm.stats().total_failed, 1);
    }

    #[test]
    fn test_workflow_task_timeout() {
        let sm = WorkflowTaskStateMachine::new();
        sm.schedule(5, "q", None);
        assert!(sm.record_timed_out());
        assert_eq!(sm.stats().total_timed_out, 1);
    }

    // ─── Task Refresher ───────────────────────────────────────────────────

    #[test]
    fn test_refresh_activities() {
        let state = make_state();
        state.add_activity(
            10,
            ActivityMutableInfo {
                activity_id: 1,
                activity_type: "a".into(),
                state: ActivityMutableState::Scheduled,
                scheduled_event_id: 10,
                started_event_id: 0,
                attempt: 1,
                scheduled_time_ms: 0,
                started_time_ms: None,
                scheduled_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                schedule_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
                last_heartbeat_ms: None,
                heartbeat_details: None,
                retry_policy: None,
                task_queue: "q".into(),
                result: None,
                failure: None,
                is_paused: false,
                request_id: "r".into(),
            },
        );

        let refresher = TaskRefresher::new();
        let tasks = refresher.refresh_activities(&state);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].task_type, TransferTaskType::ActivityTask);
    }

    // ─── Timer Sequence ───────────────────────────────────────────────────

    #[test]
    fn test_timer_sequence_ordering() {
        let seq = TimerSequence::new();
        seq.add(TimerSequenceEntry {
            timer_id: 3,
            expiry_time_ms: 3000,
            task_type: TimerTaskType::UserTimer,
            event_id: 30,
        });
        seq.add(TimerSequenceEntry {
            timer_id: 1,
            expiry_time_ms: 1000,
            task_type: TimerTaskType::UserTimer,
            event_id: 10,
        });
        seq.add(TimerSequenceEntry {
            timer_id: 2,
            expiry_time_ms: 2000,
            task_type: TimerTaskType::UserTimer,
            event_id: 20,
        });

        let next = seq.next_to_fire(1500).unwrap();
        assert_eq!(next.timer_id, 1); // Earliest timer

        seq.remove(1);
        let next = seq.next_to_fire(1500);
        assert!(next.is_none()); // 2000 > 1500
    }

    // ─── Mutable State Checksum ───────────────────────────────────────────

    #[test]
    fn test_checksum_deterministic() {
        let state = make_state();
        let checksum = MutableStateChecksum::new();

        let c1 = checksum.compute(&state);
        let c2 = checksum.compute(&state);
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_checksum_changes_with_state() {
        let state = make_state();
        let checksum = MutableStateChecksum::new();

        let c1 = checksum.compute(&state);
        state.add_activity(
            10,
            ActivityMutableInfo {
                activity_id: 1,
                activity_type: "a".into(),
                state: ActivityMutableState::Scheduled,
                scheduled_event_id: 10,
                started_event_id: 0,
                attempt: 1,
                scheduled_time_ms: 0,
                started_time_ms: None,
                scheduled_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                schedule_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
                last_heartbeat_ms: None,
                heartbeat_details: None,
                retry_policy: None,
                task_queue: "q".into(),
                result: None,
                failure: None,
                is_paused: false,
                request_id: "r".into(),
            },
        );
        let c2 = checksum.compute(&state);
        assert_ne!(c1, c2);
    }

    #[test]
    fn test_checksum_verify() {
        let state = make_state();
        let checksum = MutableStateChecksum::new();
        let c = checksum.compute(&state);
        assert!(checksum.verify(&state, c));
        assert!(!checksum.verify(&state, c + 1));
    }
}
