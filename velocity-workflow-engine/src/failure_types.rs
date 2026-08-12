//! Structured failure types mirroring Temporal's failure model.
//!
//! Every failure in the system is classified by type (application, timeout, canceled, server, etc.)
//! with type-specific metadata. Failures form a cause chain for nested error reporting.
//! RetryState tracks the outcome of retry decisions. TimeoutType distinguishes the four
//! Temporal timeout categories. WorkflowIdReusePolicy controls start-workflow ID conflicts.

use std::fmt;

// ─── Failure Type ──────────────────────────────────────────────────────────

/// The kind of failure that occurred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureType {
    /// An application-level error returned by workflow or activity code.
    Application,
    /// A server-side internal error.
    Server,
    /// A timeout expired.
    Timeout,
    /// The operation was canceled.
    Canceled,
    /// A child workflow execution failed.
    ChildWorkflowExecution,
    /// A workflow reset occurred.
    ResetWorkflow,
    /// Activity task not found (worker died or network partition).
    ActivityTaskNotFound,
}

impl fmt::Display for FailureType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FailureType::Application => write!(f, "Application"),
            FailureType::Server => write!(f, "Server"),
            FailureType::Timeout => write!(f, "Timeout"),
            FailureType::Canceled => write!(f, "Canceled"),
            FailureType::ChildWorkflowExecution => write!(f, "ChildWorkflowExecution"),
            FailureType::ResetWorkflow => write!(f, "ResetWorkflow"),
            FailureType::ActivityTaskNotFound => write!(f, "ActivityTaskNotFound"),
        }
    }
}

// ─── Timeout Type ──────────────────────────────────────────────────────────

/// The four Temporal timeout categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum TimeoutType {
    /// Activity did not complete within the start-to-close window.
    StartToClose = 0,
    /// Activity was scheduled but a worker did not pick it up in time.
    ScheduleToStart = 1,
    /// Activity did not complete within the overall schedule-to-close window.
    ScheduleToClose = 2,
    /// Activity did not record a heartbeat in time.
    Heartbeat = 3,
}

impl fmt::Display for TimeoutType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeoutType::StartToClose => write!(f, "StartToClose"),
            TimeoutType::ScheduleToStart => write!(f, "ScheduleToStart"),
            TimeoutType::ScheduleToClose => write!(f, "ScheduleToClose"),
            TimeoutType::Heartbeat => write!(f, "Heartbeat"),
        }
    }
}

// ─── Retry State ───────────────────────────────────────────────────────────

/// Outcome of a retry decision, mirroring Temporal's RetryState enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum RetryState {
    /// Retry is in progress — another attempt will be made.
    InProgress = 0,
    /// The error is marked non-retryable.
    NonRetryable = 1,
    /// Maximum retry attempts have been exhausted.
    MaxAttemptsReached = 2,
    /// The retry timeout (schedule-to-close) expired.
    RetryTimeout = 3,
    /// The workflow/activity was canceled during retry.
    CancelRequested = 4,
}

impl fmt::Display for RetryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RetryState::InProgress => write!(f, "InProgress"),
            RetryState::NonRetryable => write!(f, "NonRetryable"),
            RetryState::MaxAttemptsReached => write!(f, "MaxAttemptsReached"),
            RetryState::RetryTimeout => write!(f, "RetryTimeout"),
            RetryState::CancelRequested => write!(f, "CancelRequested"),
        }
    }
}

// ─── Failure Info ──────────────────────────────────────────────────────────

/// Application-level failure metadata.
#[derive(Debug, Clone)]
pub struct ApplicationFailureInfo {
    /// Error type name (e.g., "NullPointerException", "InsufficientFundsError").
    pub error_type: String,
    /// Whether this error should NOT be retried.
    pub non_retryable: bool,
    /// Opaque details payload.
    pub details: Vec<u8>,
}

/// Timeout failure metadata.
#[derive(Debug, Clone)]
pub struct TimeoutFailureInfo {
    /// Which timeout expired.
    pub timeout_type: TimeoutType,
    /// Last heartbeat details (for heartbeat timeouts).
    pub last_heartbeat_details: Vec<u8>,
}

/// Canceled failure metadata.
#[derive(Debug, Clone)]
pub struct CanceledFailureInfo {
    /// Cancellation details.
    pub details: Vec<u8>,
}

/// Server failure metadata.
#[derive(Debug, Clone)]
pub struct ServerFailureInfo {
    /// Whether this server error should NOT be retried.
    pub non_retryable: bool,
}

/// Child workflow execution failure metadata.
#[derive(Debug, Clone)]
pub struct ChildWorkflowExecutionFailureInfo {
    /// The namespace of the child workflow.
    pub namespace: String,
    /// Child workflow execution ID.
    pub workflow_id: u64,
    /// Child workflow run ID.
    pub run_id: u64,
    /// Child workflow type name.
    pub workflow_type: String,
    /// The retry state of the child workflow.
    pub retry_state: RetryState,
}

/// Reset workflow failure metadata.
#[derive(Debug, Clone)]
pub struct ResetWorkflowFailureInfo {
    /// Last heartbeat details before reset.
    pub last_heartbeat_details: Vec<u8>,
}

/// Activity task not found metadata.
#[derive(Debug, Clone)]
pub struct ActivityTaskNotFoundInfo {
    /// The schedule event ID of the missing activity.
    pub schedule_event_id: u64,
}

/// Type-specific failure information.
#[derive(Debug, Clone)]
pub enum FailureInfo {
    Application(ApplicationFailureInfo),
    Server(ServerFailureInfo),
    Timeout(TimeoutFailureInfo),
    Canceled(CanceledFailureInfo),
    ChildWorkflowExecution(ChildWorkflowExecutionFailureInfo),
    ResetWorkflow(ResetWorkflowFailureInfo),
    ActivityTaskNotFound(ActivityTaskNotFoundInfo),
}

impl FailureInfo {
    /// Returns the failure type for this info.
    pub fn failure_type(&self) -> FailureType {
        match self {
            FailureInfo::Application(_) => FailureType::Application,
            FailureInfo::Server(_) => FailureType::Server,
            FailureInfo::Timeout(_) => FailureType::Timeout,
            FailureInfo::Canceled(_) => FailureType::Canceled,
            FailureInfo::ChildWorkflowExecution(_) => FailureType::ChildWorkflowExecution,
            FailureInfo::ResetWorkflow(_) => FailureType::ResetWorkflow,
            FailureInfo::ActivityTaskNotFound(_) => FailureType::ActivityTaskNotFound,
        }
    }

    /// Whether this failure is non-retryable.
    pub fn is_non_retryable(&self) -> bool {
        match self {
            FailureInfo::Application(info) => info.non_retryable,
            FailureInfo::Server(info) => info.non_retryable,
            FailureInfo::Timeout(_) => true,
            FailureInfo::Canceled(_) => true,
            FailureInfo::ChildWorkflowExecution(info) => info.retry_state == RetryState::NonRetryable,
            FailureInfo::ResetWorkflow(_) => true,
            FailureInfo::ActivityTaskNotFound(_) => false,
        }
    }
}

// ─── WorkflowFailure ───────────────────────────────────────────────────────

/// A structured failure in the workflow system, forming a cause chain.
#[derive(Debug, Clone)]
pub struct WorkflowFailure {
    /// Human-readable error message.
    pub message: String,
    /// Source of the failure (e.g., "Server", "SDK", worker identity).
    pub source: String,
    /// Stack trace if available.
    pub stack_trace: String,
    /// Type-specific failure information.
    pub info: FailureInfo,
    /// Underlying cause (forms a chain).
    pub cause: Option<Box<WorkflowFailure>>,
}

impl WorkflowFailure {
    /// Create a new application failure.
    pub fn application(message: impl Into<String>, error_type: impl Into<String>, non_retryable: bool) -> Self {
        Self {
            message: message.into(),
            source: String::new(),
            stack_trace: String::new(),
            info: FailureInfo::Application(ApplicationFailureInfo {
                error_type: error_type.into(),
                non_retryable,
                details: Vec::new(),
            }),
            cause: None,
        }
    }

    /// Create a new server failure.
    pub fn server(message: impl Into<String>, non_retryable: bool) -> Self {
        Self {
            message: message.into(),
            source: "Server".to_string(),
            stack_trace: String::new(),
            info: FailureInfo::Server(ServerFailureInfo { non_retryable }),
            cause: None,
        }
    }

    /// Create a new timeout failure.
    pub fn timeout(message: impl Into<String>, timeout_type: TimeoutType) -> Self {
        Self {
            message: message.into(),
            source: "Server".to_string(),
            stack_trace: String::new(),
            info: FailureInfo::Timeout(TimeoutFailureInfo {
                timeout_type,
                last_heartbeat_details: Vec::new(),
            }),
            cause: None,
        }
    }

    /// Create a new canceled failure.
    pub fn canceled(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: String::new(),
            stack_trace: String::new(),
            info: FailureInfo::Canceled(CanceledFailureInfo { details: Vec::new() }),
            cause: None,
        }
    }

    /// Create a child workflow execution failure.
    pub fn child_workflow(
        message: impl Into<String>,
        namespace: impl Into<String>,
        workflow_id: u64,
        run_id: u64,
        workflow_type: impl Into<String>,
        retry_state: RetryState,
    ) -> Self {
        Self {
            message: message.into(),
            source: String::new(),
            stack_trace: String::new(),
            info: FailureInfo::ChildWorkflowExecution(ChildWorkflowExecutionFailureInfo {
                namespace: namespace.into(),
                workflow_id,
                run_id,
                workflow_type: workflow_type.into(),
                retry_state,
            }),
            cause: None,
        }
    }

    /// Set the source.
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = source.into();
        self
    }

    /// Set the stack trace.
    pub fn with_stack_trace(mut self, trace: impl Into<String>) -> Self {
        self.stack_trace = trace.into();
        self
    }

    /// Set the cause chain.
    pub fn with_cause(mut self, cause: WorkflowFailure) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    /// Set application failure details.
    pub fn with_details(mut self, details: Vec<u8>) -> Self {
        if let FailureInfo::Application(ref mut info) = self.info {
            info.details = details;
        }
        self
    }

    /// Set timeout last heartbeat details.
    pub fn with_heartbeat_details(mut self, details: Vec<u8>) -> Self {
        if let FailureInfo::Timeout(ref mut info) = self.info {
            info.last_heartbeat_details = details;
        }
        self
    }

    /// Get the failure type.
    pub fn failure_type(&self) -> FailureType {
        self.info.failure_type()
    }

    /// Whether this failure is non-retryable.
    pub fn is_non_retryable(&self) -> bool {
        self.info.is_non_retryable()
    }

    /// Get the depth of the cause chain.
    pub fn cause_depth(&self) -> usize {
        let mut depth = 0;
        let mut current = &self.cause;
        while let Some(ref cause) = current {
            depth += 1;
            current = &cause.cause;
        }
        depth
    }

    /// Collect all messages in the cause chain.
    pub fn chain_messages(&self) -> Vec<&str> {
        let mut messages = vec![self.message.as_str()];
        let mut current = &self.cause;
        while let Some(ref cause) = current {
            messages.push(&cause.message);
            current = &cause.cause;
        }
        messages
    }

    /// Truncate the failure to fit within `max_size` bytes (for storage/transmission).
    pub fn truncate(self, max_size: usize) -> Self {
        self.truncate_with_depth(max_size, 20)
    }

    /// Truncate with a maximum cause chain depth.
    pub fn truncate_with_depth(mut self, max_size: usize, max_depth: usize) -> Self {
        let mut remaining = max_size;

        if self.message.len() > remaining {
            self.message.truncate(remaining);
            return self;
        }
        remaining -= self.message.len();

        if self.source.len() > remaining {
            self.source.truncate(remaining);
            return self;
        }
        remaining -= self.source.len();

        if self.stack_trace.len() > remaining {
            self.stack_trace.truncate(remaining);
            return self;
        }
        remaining -= self.stack_trace.len();

        if remaining > 4 && max_depth > 0 {
            if let Some(cause) = self.cause.take() {
                self.cause = Some(Box::new(cause.truncate_with_depth(remaining - 4, max_depth - 1)));
            }
        } else {
            self.cause = None;
        }

        self
    }

    /// Total approximate byte size of this failure (including cause chain).
    pub fn byte_size(&self) -> usize {
        let mut size = self.message.len() + self.source.len() + self.stack_trace.len() + 8;
        match &self.info {
            FailureInfo::Application(info) => {
                size += info.error_type.len() + info.details.len() + 4;
            }
            FailureInfo::Timeout(info) => {
                size += info.last_heartbeat_details.len() + 4;
            }
            FailureInfo::Canceled(info) => {
                size += info.details.len() + 2;
            }
            FailureInfo::Server(_) => size += 2,
            FailureInfo::ChildWorkflowExecution(info) => {
                size += info.namespace.len() + info.workflow_type.len() + 24;
            }
            FailureInfo::ResetWorkflow(info) => {
                size += info.last_heartbeat_details.len() + 2;
            }
            FailureInfo::ActivityTaskNotFound(_) => size += 8,
        }
        if let Some(ref cause) = self.cause {
            size += cause.byte_size();
        }
        size
    }
}

impl fmt::Display for WorkflowFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.failure_type(), self.message)?;
        if !self.source.is_empty() {
            write!(f, " (source: {})", self.source)?;
        }
        if let Some(ref cause) = self.cause {
            write!(f, "\n  Caused by: {}", cause)?;
        }
        Ok(())
    }
}

// ─── WorkflowIdReusePolicy ─────────────────────────────────────────────────

/// Policy for handling workflow ID conflicts when starting a new workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum WorkflowIdReusePolicy {
    /// Allow starting a workflow with the same ID regardless of previous state.
    AllowDuplicate = 0,
    /// Reject starting if a workflow with the same ID exists (in any state).
    RejectDuplicate = 1,
    /// Allow starting only if the previous workflow with the same ID has failed/terminated.
    AllowDuplicateFailedOnly = 2,
    /// If a workflow with the same ID is running, terminate it and start a new one.
    TerminateIfRunning = 3,
}

impl Default for WorkflowIdReusePolicy {
    fn default() -> Self {
        WorkflowIdReusePolicy::AllowDuplicate
    }
}

impl fmt::Display for WorkflowIdReusePolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkflowIdReusePolicy::AllowDuplicate => write!(f, "AllowDuplicate"),
            WorkflowIdReusePolicy::RejectDuplicate => write!(f, "RejectDuplicate"),
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly => write!(f, "AllowDuplicateFailedOnly"),
            WorkflowIdReusePolicy::TerminateIfRunning => write!(f, "TerminateIfRunning"),
        }
    }
}

/// Final status of a completed workflow execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum WorkflowFinalStatus {
    Completed = 0,
    Failed = 1,
    Canceled = 2,
    Terminated = 3,
    TimedOut = 4,
    ContinuedAsNew = 5,
}

impl WorkflowIdReusePolicy {
    /// Check if a new workflow start is allowed given the existing workflow state.
    pub fn allows_start(&self, existing_running: bool, existing_status: WorkflowFinalStatus) -> bool {
        match self {
            WorkflowIdReusePolicy::AllowDuplicate => true,
            WorkflowIdReusePolicy::RejectDuplicate => false,
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly => {
                !existing_running && matches!(existing_status,
                    WorkflowFinalStatus::Failed | WorkflowFinalStatus::Terminated | WorkflowFinalStatus::TimedOut)
            }
            WorkflowIdReusePolicy::TerminateIfRunning => true,
        }
    }

    /// Whether this policy requires terminating an existing running workflow.
    pub fn should_terminate_running(&self, existing_running: bool) -> bool {
        existing_running && *self == WorkflowIdReusePolicy::TerminateIfRunning
    }
}

// ─── Failure Builder ───────────────────────────────────────────────────────

/// Builder for constructing workflow failures fluently.
pub struct FailureBuilder {
    message: String,
    source: String,
    stack_trace: String,
}

impl FailureBuilder {
    pub fn new() -> Self {
        Self { message: String::new(), source: String::new(), stack_trace: String::new() }
    }

    pub fn message(mut self, msg: impl Into<String>) -> Self { self.message = msg.into(); self }
    pub fn source(mut self, src: impl Into<String>) -> Self { self.source = src.into(); self }
    pub fn stack_trace(mut self, trace: impl Into<String>) -> Self { self.stack_trace = trace.into(); self }

    pub fn application(self, error_type: impl Into<String>, non_retryable: bool) -> WorkflowFailure {
        WorkflowFailure {
            message: self.message, source: self.source, stack_trace: self.stack_trace,
            info: FailureInfo::Application(ApplicationFailureInfo {
                error_type: error_type.into(), non_retryable, details: Vec::new(),
            }),
            cause: None,
        }
    }

    pub fn server(self, non_retryable: bool) -> WorkflowFailure {
        WorkflowFailure {
            message: self.message,
            source: if self.source.is_empty() { "Server".to_string() } else { self.source },
            stack_trace: self.stack_trace,
            info: FailureInfo::Server(ServerFailureInfo { non_retryable }),
            cause: None,
        }
    }

    pub fn timeout(self, timeout_type: TimeoutType) -> WorkflowFailure {
        WorkflowFailure {
            message: self.message,
            source: if self.source.is_empty() { "Server".to_string() } else { self.source },
            stack_trace: self.stack_trace,
            info: FailureInfo::Timeout(TimeoutFailureInfo {
                timeout_type, last_heartbeat_details: Vec::new(),
            }),
            cause: None,
        }
    }

    pub fn canceled(self) -> WorkflowFailure {
        WorkflowFailure {
            message: self.message, source: self.source, stack_trace: self.stack_trace,
            info: FailureInfo::Canceled(CanceledFailureInfo { details: Vec::new() }),
            cause: None,
        }
    }
}

impl Default for FailureBuilder {
    fn default() -> Self { Self::new() }
}

// ─── Failure Stats ─────────────────────────────────────────────────────────

/// Aggregate statistics about failures.
#[derive(Debug, Clone, Default)]
pub struct FailureStats {
    pub total_failures: u64,
    pub application_failures: u64,
    pub timeout_failures: u64,
    pub canceled_failures: u64,
    pub server_failures: u64,
    pub child_workflow_failures: u64,
    pub non_retryable_count: u64,
    pub retryable_count: u64,
    pub max_cause_depth: usize,
}

impl FailureStats {
    /// Record a failure in the stats.
    pub fn record(&mut self, failure: &WorkflowFailure) {
        self.total_failures += 1;
        match failure.failure_type() {
            FailureType::Application => self.application_failures += 1,
            FailureType::Timeout => self.timeout_failures += 1,
            FailureType::Canceled => self.canceled_failures += 1,
            FailureType::Server => self.server_failures += 1,
            FailureType::ChildWorkflowExecution => self.child_workflow_failures += 1,
            _ => {}
        }
        if failure.is_non_retryable() {
            self.non_retryable_count += 1;
        } else {
            self.retryable_count += 1;
        }
        let depth = failure.cause_depth();
        if depth > self.max_cause_depth {
            self.max_cause_depth = depth;
        }
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_application_failure() {
        let f = WorkflowFailure::application("something broke", "NullPointer", false);
        assert_eq!(f.failure_type(), FailureType::Application);
        assert!(!f.is_non_retryable());
        assert_eq!(f.message, "something broke");
        assert!(f.cause.is_none());
        assert_eq!(f.cause_depth(), 0);
    }

    #[test]
    fn test_application_failure_non_retryable() {
        let f = WorkflowFailure::application("fatal", "ValidationError", true);
        assert!(f.is_non_retryable());
    }

    #[test]
    fn test_server_failure() {
        let f = WorkflowFailure::server("internal error", false);
        assert_eq!(f.failure_type(), FailureType::Server);
        assert_eq!(f.source, "Server");
        assert!(!f.is_non_retryable());
    }

    #[test]
    fn test_timeout_failure() {
        let f = WorkflowFailure::timeout("activity timed out", TimeoutType::StartToClose);
        assert_eq!(f.failure_type(), FailureType::Timeout);
        assert!(f.is_non_retryable());
    }

    #[test]
    fn test_canceled_failure() {
        let f = WorkflowFailure::canceled("workflow canceled");
        assert_eq!(f.failure_type(), FailureType::Canceled);
        assert!(f.is_non_retryable());
    }

    #[test]
    fn test_child_workflow_failure() {
        let f = WorkflowFailure::child_workflow("child failed", "default-ns", 42, 1, "ChildWorkflow", RetryState::MaxAttemptsReached);
        assert_eq!(f.failure_type(), FailureType::ChildWorkflowExecution);
    }

    #[test]
    fn test_cause_chain() {
        let root = WorkflowFailure::application("root cause", "DbError", true);
        let mid = WorkflowFailure::server("middleware error", false).with_cause(root);
        let top = WorkflowFailure::application("top level", "ServiceError", false).with_cause(mid);
        assert_eq!(top.cause_depth(), 2);
        let msgs = top.chain_messages();
        assert_eq!(msgs, vec!["top level", "middleware error", "root cause"]);
    }

    #[test]
    fn test_with_source_and_trace() {
        let f = WorkflowFailure::application("err", "TypeError", false)
            .with_source("worker-1").with_stack_trace("at line 42\nat line 10");
        assert_eq!(f.source, "worker-1");
        assert_eq!(f.stack_trace, "at line 42\nat line 10");
    }

    #[test]
    fn test_with_details() {
        let f = WorkflowFailure::application("err", "TypeError", false).with_details(vec![1, 2, 3]);
        if let FailureInfo::Application(info) = &f.info {
            assert_eq!(info.details, vec![1, 2, 3]);
        } else { panic!("expected Application info"); }
    }

    #[test]
    fn test_with_heartbeat_details() {
        let f = WorkflowFailure::timeout("hb timeout", TimeoutType::Heartbeat).with_heartbeat_details(vec![10, 20]);
        if let FailureInfo::Timeout(info) = &f.info {
            assert_eq!(info.last_heartbeat_details, vec![10, 20]);
        } else { panic!("expected Timeout info"); }
    }

    #[test]
    fn test_truncate_message() {
        let f = WorkflowFailure::application("a".repeat(1000), "Type", false);
        let truncated = f.truncate(100);
        assert!(truncated.message.len() <= 100);
    }

    #[test]
    fn test_truncate_cause_chain_depth() {
        let mut chain = WorkflowFailure::application("leaf", "T", false);
        for i in (0..25).rev() {
            chain = WorkflowFailure::application(format!("level {}", i), "T", false).with_cause(chain);
        }
        let truncated = chain.truncate_with_depth(10000, 5);
        assert!(truncated.cause_depth() <= 5);
    }

    #[test]
    fn test_byte_size() {
        let f = WorkflowFailure::application("msg", "Type", false).with_source("src").with_details(vec![0; 100]);
        assert!(f.byte_size() > 100);
    }

    #[test]
    fn test_display() {
        let f = WorkflowFailure::application("something broke", "NullPointer", false);
        let s = format!("{}", f);
        assert!(s.contains("Application"));
        assert!(s.contains("something broke"));
    }

    #[test]
    fn test_display_with_cause() {
        let root = WorkflowFailure::application("root", "T", true);
        let top = WorkflowFailure::server("top", false).with_cause(root);
        let s = format!("{}", top);
        assert!(s.contains("Caused by"));
    }

    #[test]
    fn test_builder_application() {
        let f = FailureBuilder::new().message("error occurred").source("worker-5")
            .stack_trace("at main.go:42").application("TypeError", true);
        assert_eq!(f.failure_type(), FailureType::Application);
        assert!(f.is_non_retryable());
        assert_eq!(f.source, "worker-5");
    }

    #[test]
    fn test_builder_server() {
        let f = FailureBuilder::new().message("internal").server(false);
        assert_eq!(f.failure_type(), FailureType::Server);
        assert_eq!(f.source, "Server");
    }

    #[test]
    fn test_builder_timeout() {
        let f = FailureBuilder::new().message("timed out").timeout(TimeoutType::Heartbeat);
        assert_eq!(f.failure_type(), FailureType::Timeout);
    }

    #[test]
    fn test_builder_canceled() {
        let f = FailureBuilder::new().message("canceled").canceled();
        assert_eq!(f.failure_type(), FailureType::Canceled);
    }

    #[test]
    fn test_reuse_policy_allow_duplicate() {
        let p = WorkflowIdReusePolicy::AllowDuplicate;
        assert!(p.allows_start(true, WorkflowFinalStatus::Completed));
        assert!(p.allows_start(false, WorkflowFinalStatus::Completed));
        assert!(p.allows_start(false, WorkflowFinalStatus::Failed));
    }

    #[test]
    fn test_reuse_policy_reject_duplicate() {
        let p = WorkflowIdReusePolicy::RejectDuplicate;
        assert!(!p.allows_start(true, WorkflowFinalStatus::Completed));
        assert!(!p.allows_start(false, WorkflowFinalStatus::Completed));
        assert!(!p.allows_start(false, WorkflowFinalStatus::Failed));
    }

    #[test]
    fn test_reuse_policy_allow_duplicate_failed_only() {
        let p = WorkflowIdReusePolicy::AllowDuplicateFailedOnly;
        assert!(!p.allows_start(true, WorkflowFinalStatus::Failed));
        assert!(!p.allows_start(false, WorkflowFinalStatus::Completed));
        assert!(p.allows_start(false, WorkflowFinalStatus::Failed));
        assert!(p.allows_start(false, WorkflowFinalStatus::Terminated));
        assert!(p.allows_start(false, WorkflowFinalStatus::TimedOut));
        assert!(!p.allows_start(false, WorkflowFinalStatus::Canceled));
    }

    #[test]
    fn test_reuse_policy_terminate_if_running() {
        let p = WorkflowIdReusePolicy::TerminateIfRunning;
        assert!(p.allows_start(true, WorkflowFinalStatus::Completed));
        assert!(p.should_terminate_running(true));
        assert!(!p.should_terminate_running(false));
    }

    #[test]
    fn test_reuse_policy_default() {
        assert_eq!(WorkflowIdReusePolicy::default(), WorkflowIdReusePolicy::AllowDuplicate);
    }

    #[test]
    fn test_retry_state_display() {
        assert_eq!(format!("{}", RetryState::InProgress), "InProgress");
        assert_eq!(format!("{}", RetryState::NonRetryable), "NonRetryable");
        assert_eq!(format!("{}", RetryState::MaxAttemptsReached), "MaxAttemptsReached");
    }

    #[test]
    fn test_timeout_type_display() {
        assert_eq!(format!("{}", TimeoutType::StartToClose), "StartToClose");
        assert_eq!(format!("{}", TimeoutType::Heartbeat), "Heartbeat");
    }

    #[test]
    fn test_failure_stats_record() {
        let mut stats = FailureStats::default();
        stats.record(&WorkflowFailure::application("err", "T", false));
        stats.record(&WorkflowFailure::timeout("timeout", TimeoutType::StartToClose));
        stats.record(&WorkflowFailure::server("server", true));
        assert_eq!(stats.total_failures, 3);
        assert_eq!(stats.application_failures, 1);
        assert_eq!(stats.timeout_failures, 1);
        assert_eq!(stats.server_failures, 1);
        assert_eq!(stats.non_retryable_count, 2);
        assert_eq!(stats.retryable_count, 1);
    }

    #[test]
    fn test_failure_stats_cause_depth() {
        let mut stats = FailureStats::default();
        let root = WorkflowFailure::application("root", "T", true);
        let top = WorkflowFailure::server("top", false).with_cause(root);
        stats.record(&top);
        assert_eq!(stats.max_cause_depth, 1);
    }

    #[test]
    fn test_failure_info_types() {
        assert_eq!(FailureInfo::Application(ApplicationFailureInfo { error_type: "T".into(), non_retryable: false, details: vec![] }).failure_type(), FailureType::Application);
        assert_eq!(FailureInfo::Server(ServerFailureInfo { non_retryable: false }).failure_type(), FailureType::Server);
        assert_eq!(FailureInfo::Timeout(TimeoutFailureInfo { timeout_type: TimeoutType::Heartbeat, last_heartbeat_details: vec![] }).failure_type(), FailureType::Timeout);
        assert_eq!(FailureInfo::Canceled(CanceledFailureInfo { details: vec![] }).failure_type(), FailureType::Canceled);
        assert_eq!(FailureInfo::ActivityTaskNotFound(ActivityTaskNotFoundInfo { schedule_event_id: 1 }).failure_type(), FailureType::ActivityTaskNotFound);
    }

    #[test]
    fn test_reset_workflow_failure() {
        let f = WorkflowFailure {
            message: "reset".into(), source: "Server".into(), stack_trace: String::new(),
            info: FailureInfo::ResetWorkflow(ResetWorkflowFailureInfo { last_heartbeat_details: vec![1, 2, 3] }),
            cause: None,
        };
        assert_eq!(f.failure_type(), FailureType::ResetWorkflow);
        assert!(f.is_non_retryable());
    }

    #[test]
    fn test_activity_task_not_found() {
        let f = WorkflowFailure {
            message: "not found".into(), source: String::new(), stack_trace: String::new(),
            info: FailureInfo::ActivityTaskNotFound(ActivityTaskNotFoundInfo { schedule_event_id: 42 }),
            cause: None,
        };
        assert_eq!(f.failure_type(), FailureType::ActivityTaskNotFound);
        assert!(!f.is_non_retryable());
    }

    #[test]
    fn test_display_all_types() {
        assert_eq!(format!("{}", WorkflowIdReusePolicy::AllowDuplicate), "AllowDuplicate");
        assert_eq!(format!("{}", WorkflowIdReusePolicy::RejectDuplicate), "RejectDuplicate");
        assert_eq!(format!("{}", WorkflowIdReusePolicy::AllowDuplicateFailedOnly), "AllowDuplicateFailedOnly");
        assert_eq!(format!("{}", WorkflowIdReusePolicy::TerminateIfRunning), "TerminateIfRunning");
        assert_eq!(format!("{}", FailureType::Application), "Application");
        assert_eq!(format!("{}", FailureType::Server), "Server");
        assert_eq!(format!("{}", FailureType::Timeout), "Timeout");
        assert_eq!(format!("{}", FailureType::Canceled), "Canceled");
        assert_eq!(format!("{}", FailureType::ChildWorkflowExecution), "ChildWorkflowExecution");
        assert_eq!(format!("{}", FailureType::ResetWorkflow), "ResetWorkflow");
        assert_eq!(format!("{}", FailureType::ActivityTaskNotFound), "ActivityTaskNotFound");
    }
}
