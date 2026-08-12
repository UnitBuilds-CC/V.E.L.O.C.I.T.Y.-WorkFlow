//! Comprehensive error handling framework for the VELOCITY-WorkFlow engine.
//!
//! Provides structured error types with rich context, gRPC status code mapping,
//! FFI error codes for the C# bridge, categorization, and retryability analysis.

use std::fmt;

// ─── Error Category ───────────────────────────────────────────────────────────

/// High-level error category for logging, metrics, and routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Workflow lifecycle errors (not found, already completed, invalid state).
    Workflow,
    /// Namespace and task-queue management errors.
    Namespace,
    /// Authentication, authorization, and rate-limit errors.
    Security,
    /// Database, serialization, and replication infrastructure errors.
    Infrastructure,
    /// Internal engine errors that indicate bugs or unexpected conditions.
    Internal,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCategory::Workflow => write!(f, "Workflow"),
            ErrorCategory::Namespace => write!(f, "Namespace"),
            ErrorCategory::Security => write!(f, "Security"),
            ErrorCategory::Infrastructure => write!(f, "Infrastructure"),
            ErrorCategory::Internal => write!(f, "Internal"),
        }
    }
}

// ─── Error Code (gRPC status mapping) ────────────────────────────────────────

/// Error codes aligned with gRPC standard status codes.
/// Used for API responses and inter-service communication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ErrorCode {
    /// 0 — Operation succeeded.
    Ok = 0,
    /// 1 — Operation cancelled.
    Cancelled = 1,
    /// 2 — Unknown error.
    Unknown = 2,
    /// 3 — Invalid argument.
    InvalidArgument = 3,
    /// 5 — Resource not found.
    NotFound = 5,
    /// 6 — Resource already exists.
    AlreadyExists = 6,
    /// 7 — Permission denied.
    PermissionDenied = 7,
    /// 8 — Resource exhausted (rate limit).
    ResourceExhausted = 8,
    /// 9 — Failed precondition.
    FailedPrecondition = 9,
    /// 10 — Operation aborted.
    Aborted = 10,
    /// 12 — Not implemented.
    Unimplemented = 12,
    /// 13 — Internal error.
    InternalError = 13,
    /// 14 — Service unavailable.
    Unavailable = 14,
    /// 15 — Data loss.
    DataLoss = 15,
    /// 16 — Unauthenticated.
    Unauthenticated = 16,
}

impl ErrorCode {
    /// Convert to the corresponding gRPC status code integer.
    pub fn to_grpc_code(self) -> i32 {
        self as i32
    }
}

// ─── FFI Error Codes ─────────────────────────────────────────────────────────

/// FFI error codes returned to the C# NativeBridge layer.
/// These are distinct from gRPC codes and form the engine's C-ABI contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum FfiErrorCode {
    /// 0 — Success.
    Success = 0,
    /// -1 — Generic / unknown error.
    GenericError = -1,
    /// -100 — Workflow not found.
    WorkflowNotFound = -100,
    /// -101 — Workflow already completed.
    WorkflowAlreadyCompleted = -101,
    /// -102 — Invalid workflow state transition.
    InvalidWorkflowState = -102,
    /// -103 — Step index out of range.
    StepOutOfRange = -103,
    /// -104 — Step already completed.
    StepAlreadyCompleted = -104,
    /// -200 — Namespace not found.
    NamespaceNotFound = -200,
    /// -201 — Namespace already exists.
    NamespaceAlreadyExists = -201,
    /// -202 — Task queue not found.
    TaskQueueNotFound = -202,
    /// -300 — Rate limit exceeded.
    RateLimitExceeded = -300,
    /// -301 — Authentication failed.
    AuthenticationFailed = -301,
    /// -302 — Permission denied.
    PermissionDenied = -302,
    /// -400 — Signal not found.
    SignalNotFound = -400,
    /// -401 — Query failed.
    QueryFailed = -401,
    /// -402 — Timer already cancelled.
    TimerAlreadyCancelled = -402,
    /// -500 — Saga compensation failed.
    SagaCompensationFailed = -500,
    /// -600 — Replication failed.
    ReplicationFailed = -600,
    /// -700 — Database error.
    DatabaseError = -700,
    /// -701 — Serialization error.
    SerializationError = -701,
    /// -800 — Internal error.
    InternalError = -800,
    /// -900 — Engine is shutting down.
    ShutdownInProgress = -900,
}

impl FfiErrorCode {
    pub fn to_i32(self) -> i32 {
        self as i32
    }
}

// ─── VelocityError ───────────────────────────────────────────────────────────

/// Comprehensive error type for all engine operations.
///
/// Each variant carries structured context for diagnostics, logging, and
/// client-facing error messages. The enum implements `Display`, `Error`,
/// and provides mapping to gRPC codes, FFI codes, categories, and
/// retryability.
#[derive(Debug, Clone)]
pub enum VelocityError {
    // ── Workflow errors ──────────────────────────────────────────────────
    /// The requested workflow was not found in the engine.
    WorkflowNotFound {
        workflow_key: u64,
    },
    /// The workflow has already reached a terminal state.
    WorkflowAlreadyCompleted {
        workflow_key: u64,
        status: String,
    },
    /// The workflow is not in the expected state for this operation.
    InvalidWorkflowState {
        workflow_key: u64,
        expected: String,
        actual: String,
    },
    /// The requested step index exceeds the workflow's total step count.
    StepOutOfRange {
        workflow_key: u64,
        step: u32,
        total_steps: u32,
    },
    /// The step has already been marked as completed.
    StepAlreadyCompleted {
        workflow_key: u64,
        step: u32,
    },

    // ── Namespace / task-queue errors ────────────────────────────────────
    /// The specified namespace does not exist.
    NamespaceNotFound {
        namespace: String,
    },
    /// A namespace with this name already exists.
    NamespaceAlreadyExists {
        namespace: String,
    },
    /// The specified task queue does not exist.
    TaskQueueNotFound {
        task_queue: String,
    },

    // ── Security errors ──────────────────────────────────────────────────
    /// The caller has exceeded the rate limit.
    RateLimitExceeded {
        limit: f64,
        current: f64,
    },
    /// Authentication failed.
    AuthenticationFailed {
        reason: String,
    },
    /// The caller lacks permission for the requested action.
    PermissionDenied {
        action: String,
        required_role: String,
    },

    // ── Signal / Query / Timer errors ────────────────────────────────────
    /// The specified signal was not found.
    SignalNotFound {
        signal_id: u64,
    },
    /// A query execution failed.
    QueryFailed {
        query_id: u64,
        reason: String,
    },
    /// Attempted to cancel a timer that was already cancelled.
    TimerAlreadyCancelled {
        timer_id: u64,
    },

    // ── Saga errors ──────────────────────────────────────────────────────
    /// A saga compensation step failed during rollback.
    SagaCompensationFailed {
        step: String,
        reason: String,
    },

    // ── Infrastructure errors ────────────────────────────────────────────
    /// Replication to a peer cluster failed.
    ReplicationFailed {
        reason: String,
    },
    /// A database operation failed.
    DatabaseError {
        operation: String,
        source: String,
    },
    /// Serialization or deserialization failed.
    SerializationError {
        context: String,
        source: String,
    },

    // ── Internal errors ──────────────────────────────────────────────────
    /// An unexpected internal error occurred.
    InternalError {
        context: String,
        source: String,
    },
    /// The engine is shutting down; new operations are rejected.
    ShutdownInProgress,
}

// ─── Display ─────────────────────────────────────────────────────────────────

impl fmt::Display for VelocityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VelocityError::WorkflowNotFound { workflow_key } => {
                write!(f, "workflow not found: key={workflow_key}")
            }
            VelocityError::WorkflowAlreadyCompleted { workflow_key, status } => {
                write!(f, "workflow already completed: key={workflow_key}, status={status}")
            }
            VelocityError::InvalidWorkflowState { workflow_key, expected, actual } => {
                write!(
                    f,
                    "invalid workflow state: key={workflow_key}, expected={expected}, actual={actual}"
                )
            }
            VelocityError::StepOutOfRange { workflow_key, step, total_steps } => {
                write!(
                    f,
                    "step out of range: key={workflow_key}, step={step}, total_steps={total_steps}"
                )
            }
            VelocityError::StepAlreadyCompleted { workflow_key, step } => {
                write!(f, "step already completed: key={workflow_key}, step={step}")
            }
            VelocityError::NamespaceNotFound { namespace } => {
                write!(f, "namespace not found: {namespace}")
            }
            VelocityError::NamespaceAlreadyExists { namespace } => {
                write!(f, "namespace already exists: {namespace}")
            }
            VelocityError::TaskQueueNotFound { task_queue } => {
                write!(f, "task queue not found: {task_queue}")
            }
            VelocityError::RateLimitExceeded { limit, current } => {
                write!(f, "rate limit exceeded: limit={limit}, current={current}")
            }
            VelocityError::AuthenticationFailed { reason } => {
                write!(f, "authentication failed: {reason}")
            }
            VelocityError::PermissionDenied { action, required_role } => {
                write!(f, "permission denied: action={action}, required_role={required_role}")
            }
            VelocityError::SignalNotFound { signal_id } => {
                write!(f, "signal not found: id={signal_id}")
            }
            VelocityError::QueryFailed { query_id, reason } => {
                write!(f, "query failed: id={query_id}, reason={reason}")
            }
            VelocityError::TimerAlreadyCancelled { timer_id } => {
                write!(f, "timer already cancelled: id={timer_id}")
            }
            VelocityError::SagaCompensationFailed { step, reason } => {
                write!(f, "saga compensation failed: step={step}, reason={reason}")
            }
            VelocityError::ReplicationFailed { reason } => {
                write!(f, "replication failed: {reason}")
            }
            VelocityError::DatabaseError { operation, source } => {
                write!(f, "database error: operation={operation}, source={source}")
            }
            VelocityError::SerializationError { context, source } => {
                write!(f, "serialization error: context={context}, source={source}")
            }
            VelocityError::InternalError { context, source } => {
                write!(f, "internal error: context={context}, source={source}")
            }
            VelocityError::ShutdownInProgress => {
                write!(f, "engine is shutting down")
            }
        }
    }
}

// ─── std::error::Error ───────────────────────────────────────────────────────

impl std::error::Error for VelocityError {}

// ─── Type alias ──────────────────────────────────────────────────────────────

/// Convenience result type for engine operations.
pub type VelocityResult<T> = Result<T, VelocityError>;

// ─── VelocityError methods ───────────────────────────────────────────────────

impl VelocityError {
    /// Map this error to its corresponding gRPC `ErrorCode`.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            VelocityError::WorkflowNotFound { .. } => ErrorCode::NotFound,
            VelocityError::WorkflowAlreadyCompleted { .. } => ErrorCode::FailedPrecondition,
            VelocityError::InvalidWorkflowState { .. } => ErrorCode::FailedPrecondition,
            VelocityError::StepOutOfRange { .. } => ErrorCode::InvalidArgument,
            VelocityError::StepAlreadyCompleted { .. } => ErrorCode::AlreadyExists,
            VelocityError::NamespaceNotFound { .. } => ErrorCode::NotFound,
            VelocityError::NamespaceAlreadyExists { .. } => ErrorCode::AlreadyExists,
            VelocityError::TaskQueueNotFound { .. } => ErrorCode::NotFound,
            VelocityError::RateLimitExceeded { .. } => ErrorCode::ResourceExhausted,
            VelocityError::AuthenticationFailed { .. } => ErrorCode::Unauthenticated,
            VelocityError::PermissionDenied { .. } => ErrorCode::PermissionDenied,
            VelocityError::SignalNotFound { .. } => ErrorCode::NotFound,
            VelocityError::QueryFailed { .. } => ErrorCode::InternalError,
            VelocityError::TimerAlreadyCancelled { .. } => ErrorCode::AlreadyExists,
            VelocityError::SagaCompensationFailed { .. } => ErrorCode::Aborted,
            VelocityError::ReplicationFailed { .. } => ErrorCode::Unavailable,
            VelocityError::DatabaseError { .. } => ErrorCode::InternalError,
            VelocityError::SerializationError { .. } => ErrorCode::InternalError,
            VelocityError::InternalError { .. } => ErrorCode::InternalError,
            VelocityError::ShutdownInProgress => ErrorCode::Unavailable,
        }
    }

    /// Return the gRPC status code integer for this error.
    pub fn to_grpc_status_code(&self) -> i32 {
        self.error_code().to_grpc_code()
    }

    /// Classify this error into a high-level `ErrorCategory`.
    pub fn category(&self) -> ErrorCategory {
        match self {
            VelocityError::WorkflowNotFound { .. }
            | VelocityError::WorkflowAlreadyCompleted { .. }
            | VelocityError::InvalidWorkflowState { .. }
            | VelocityError::StepOutOfRange { .. }
            | VelocityError::StepAlreadyCompleted { .. } => ErrorCategory::Workflow,

            VelocityError::NamespaceNotFound { .. }
            | VelocityError::NamespaceAlreadyExists { .. }
            | VelocityError::TaskQueueNotFound { .. } => ErrorCategory::Namespace,

            VelocityError::RateLimitExceeded { .. }
            | VelocityError::AuthenticationFailed { .. }
            | VelocityError::PermissionDenied { .. } => ErrorCategory::Security,

            VelocityError::SignalNotFound { .. }
            | VelocityError::QueryFailed { .. }
            | VelocityError::TimerAlreadyCancelled { .. }
            | VelocityError::SagaCompensationFailed { .. }
            | VelocityError::ReplicationFailed { .. }
            | VelocityError::DatabaseError { .. }
            | VelocityError::SerializationError { .. } => ErrorCategory::Infrastructure,

            VelocityError::InternalError { .. }
            | VelocityError::ShutdownInProgress => ErrorCategory::Internal,
        }
    }

    /// Whether the failed operation is safe to retry.
    ///
    /// Returns `true` for transient / infrastructure errors where retrying may
    /// succeed. Returns `false` for deterministic errors (not-found, invalid
    /// state, permission denied, etc.) where retrying would produce the same
    /// result.
    pub fn retryable(&self) -> bool {
        match self {
            // Deterministic — retrying won't help.
            VelocityError::WorkflowNotFound { .. }
            | VelocityError::WorkflowAlreadyCompleted { .. }
            | VelocityError::InvalidWorkflowState { .. }
            | VelocityError::StepOutOfRange { .. }
            | VelocityError::StepAlreadyCompleted { .. }
            | VelocityError::NamespaceNotFound { .. }
            | VelocityError::NamespaceAlreadyExists { .. }
            | VelocityError::TaskQueueNotFound { .. }
            | VelocityError::AuthenticationFailed { .. }
            | VelocityError::PermissionDenied { .. }
            | VelocityError::SignalNotFound { .. }
            | VelocityError::TimerAlreadyCancelled { .. }
            | VelocityError::SerializationError { .. }
            | VelocityError::ShutdownInProgress => false,

            // Transient — retry may succeed.
            VelocityError::RateLimitExceeded { .. }
            | VelocityError::QueryFailed { .. }
            | VelocityError::SagaCompensationFailed { .. }
            | VelocityError::ReplicationFailed { .. }
            | VelocityError::DatabaseError { .. }
            | VelocityError::InternalError { .. } => true,
        }
    }

    /// Map this error to the FFI error code consumed by the C# `NativeBridge`.
    pub fn to_ffi_code(&self) -> i32 {
        match self {
            VelocityError::WorkflowNotFound { .. } => FfiErrorCode::WorkflowNotFound.to_i32(),
            VelocityError::WorkflowAlreadyCompleted { .. } => FfiErrorCode::WorkflowAlreadyCompleted.to_i32(),
            VelocityError::InvalidWorkflowState { .. } => FfiErrorCode::InvalidWorkflowState.to_i32(),
            VelocityError::StepOutOfRange { .. } => FfiErrorCode::StepOutOfRange.to_i32(),
            VelocityError::StepAlreadyCompleted { .. } => FfiErrorCode::StepAlreadyCompleted.to_i32(),
            VelocityError::NamespaceNotFound { .. } => FfiErrorCode::NamespaceNotFound.to_i32(),
            VelocityError::NamespaceAlreadyExists { .. } => FfiErrorCode::NamespaceAlreadyExists.to_i32(),
            VelocityError::TaskQueueNotFound { .. } => FfiErrorCode::TaskQueueNotFound.to_i32(),
            VelocityError::RateLimitExceeded { .. } => FfiErrorCode::RateLimitExceeded.to_i32(),
            VelocityError::AuthenticationFailed { .. } => FfiErrorCode::AuthenticationFailed.to_i32(),
            VelocityError::PermissionDenied { .. } => FfiErrorCode::PermissionDenied.to_i32(),
            VelocityError::SignalNotFound { .. } => FfiErrorCode::SignalNotFound.to_i32(),
            VelocityError::QueryFailed { .. } => FfiErrorCode::QueryFailed.to_i32(),
            VelocityError::TimerAlreadyCancelled { .. } => FfiErrorCode::TimerAlreadyCancelled.to_i32(),
            VelocityError::SagaCompensationFailed { .. } => FfiErrorCode::SagaCompensationFailed.to_i32(),
            VelocityError::ReplicationFailed { .. } => FfiErrorCode::ReplicationFailed.to_i32(),
            VelocityError::DatabaseError { .. } => FfiErrorCode::DatabaseError.to_i32(),
            VelocityError::SerializationError { .. } => FfiErrorCode::SerializationError.to_i32(),
            VelocityError::InternalError { .. } => FfiErrorCode::InternalError.to_i32(),
            VelocityError::ShutdownInProgress => FfiErrorCode::ShutdownInProgress.to_i32(),
        }
    }

    /// Return a short machine-readable error name (useful for metrics labels).
    pub fn error_name(&self) -> &'static str {
        match self {
            VelocityError::WorkflowNotFound { .. } => "WorkflowNotFound",
            VelocityError::WorkflowAlreadyCompleted { .. } => "WorkflowAlreadyCompleted",
            VelocityError::InvalidWorkflowState { .. } => "InvalidWorkflowState",
            VelocityError::StepOutOfRange { .. } => "StepOutOfRange",
            VelocityError::StepAlreadyCompleted { .. } => "StepAlreadyCompleted",
            VelocityError::NamespaceNotFound { .. } => "NamespaceNotFound",
            VelocityError::NamespaceAlreadyExists { .. } => "NamespaceAlreadyExists",
            VelocityError::TaskQueueNotFound { .. } => "TaskQueueNotFound",
            VelocityError::RateLimitExceeded { .. } => "RateLimitExceeded",
            VelocityError::AuthenticationFailed { .. } => "AuthenticationFailed",
            VelocityError::PermissionDenied { .. } => "PermissionDenied",
            VelocityError::SignalNotFound { .. } => "SignalNotFound",
            VelocityError::QueryFailed { .. } => "QueryFailed",
            VelocityError::TimerAlreadyCancelled { .. } => "TimerAlreadyCancelled",
            VelocityError::SagaCompensationFailed { .. } => "SagaCompensationFailed",
            VelocityError::ReplicationFailed { .. } => "ReplicationFailed",
            VelocityError::DatabaseError { .. } => "DatabaseError",
            VelocityError::SerializationError { .. } => "SerializationError",
            VelocityError::InternalError { .. } => "InternalError",
            VelocityError::ShutdownInProgress => "ShutdownInProgress",
        }
    }
}

// ─── Conversions from common error shapes ────────────────────────────────────

impl From<std::io::Error> for VelocityError {
    fn from(err: std::io::Error) -> Self {
        VelocityError::DatabaseError {
            operation: "io".to_string(),
            source: err.to_string(),
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Display formatting ────────────────────────────────────────────────

    #[test]
    fn test_display_workflow_not_found() {
        let err = VelocityError::WorkflowNotFound { workflow_key: 42 };
        assert_eq!(err.to_string(), "workflow not found: key=42");
    }

    #[test]
    fn test_display_workflow_already_completed() {
        let err = VelocityError::WorkflowAlreadyCompleted {
            workflow_key: 7,
            status: "Completed".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "workflow already completed: key=7, status=Completed"
        );
    }

    #[test]
    fn test_display_invalid_workflow_state() {
        let err = VelocityError::InvalidWorkflowState {
            workflow_key: 10,
            expected: "Running".to_string(),
            actual: "Failed".to_string(),
        };
        assert!(err.to_string().contains("expected=Running"));
        assert!(err.to_string().contains("actual=Failed"));
    }

    #[test]
    fn test_display_step_out_of_range() {
        let err = VelocityError::StepOutOfRange {
            workflow_key: 1,
            step: 10,
            total_steps: 5,
        };
        let msg = err.to_string();
        assert!(msg.contains("step=10"));
        assert!(msg.contains("total_steps=5"));
    }

    #[test]
    fn test_display_rate_limit() {
        let err = VelocityError::RateLimitExceeded {
            limit: 100.0,
            current: 150.0,
        };
        let msg = err.to_string();
        assert!(msg.contains("limit=100"));
        assert!(msg.contains("current=150"));
    }

    #[test]
    fn test_display_shutdown() {
        let err = VelocityError::ShutdownInProgress;
        assert_eq!(err.to_string(), "engine is shutting down");
    }

    #[test]
    fn test_display_permission_denied() {
        let err = VelocityError::PermissionDenied {
            action: "delete_workflow".to_string(),
            required_role: "Admin".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("action=delete_workflow"));
        assert!(msg.contains("required_role=Admin"));
    }

    // ── Error code mapping ────────────────────────────────────────────────

    #[test]
    fn test_error_code_not_found_variants() {
        assert_eq!(
            VelocityError::WorkflowNotFound { workflow_key: 1 }.error_code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            VelocityError::NamespaceNotFound { namespace: "ns".into() }.error_code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            VelocityError::SignalNotFound { signal_id: 1 }.error_code(),
            ErrorCode::NotFound
        );
    }

    #[test]
    fn test_error_code_already_exists() {
        assert_eq!(
            VelocityError::NamespaceAlreadyExists { namespace: "ns".into() }.error_code(),
            ErrorCode::AlreadyExists
        );
        assert_eq!(
            VelocityError::StepAlreadyCompleted { workflow_key: 1, step: 0 }.error_code(),
            ErrorCode::AlreadyExists
        );
    }

    #[test]
    fn test_error_code_security() {
        assert_eq!(
            VelocityError::AuthenticationFailed { reason: "bad token".into() }.error_code(),
            ErrorCode::Unauthenticated
        );
        assert_eq!(
            VelocityError::PermissionDenied { action: "x".into(), required_role: "y".into() }.error_code(),
            ErrorCode::PermissionDenied
        );
        assert_eq!(
            VelocityError::RateLimitExceeded { limit: 1.0, current: 2.0 }.error_code(),
            ErrorCode::ResourceExhausted
        );
    }

    #[test]
    fn test_grpc_status_code_values() {
        assert_eq!(VelocityError::WorkflowNotFound { workflow_key: 0 }.to_grpc_status_code(), 5);
        assert_eq!(VelocityError::ShutdownInProgress.to_grpc_status_code(), 14);
        assert_eq!(VelocityError::InternalError { context: String::new(), source: String::new() }.to_grpc_status_code(), 13);
    }

    // ── Category ──────────────────────────────────────────────────────────

    #[test]
    fn test_category_workflow() {
        assert_eq!(
            VelocityError::WorkflowNotFound { workflow_key: 1 }.category(),
            ErrorCategory::Workflow
        );
        assert_eq!(
            VelocityError::StepOutOfRange { workflow_key: 1, step: 0, total_steps: 5 }.category(),
            ErrorCategory::Workflow
        );
    }

    #[test]
    fn test_category_namespace() {
        assert_eq!(
            VelocityError::NamespaceNotFound { namespace: "x".into() }.category(),
            ErrorCategory::Namespace
        );
    }

    #[test]
    fn test_category_security() {
        assert_eq!(
            VelocityError::AuthenticationFailed { reason: String::new() }.category(),
            ErrorCategory::Security
        );
    }

    #[test]
    fn test_category_infrastructure() {
        assert_eq!(
            VelocityError::DatabaseError { operation: String::new(), source: String::new() }.category(),
            ErrorCategory::Infrastructure
        );
    }

    #[test]
    fn test_category_internal() {
        assert_eq!(
            VelocityError::ShutdownInProgress.category(),
            ErrorCategory::Internal
        );
    }

    // ── Retryable ─────────────────────────────────────────────────────────

    #[test]
    fn test_retryable_transient_errors() {
        assert!(VelocityError::DatabaseError { operation: "write".into(), source: "timeout".into() }.retryable());
        assert!(VelocityError::ReplicationFailed { reason: "network".into() }.retryable());
        assert!(VelocityError::RateLimitExceeded { limit: 10.0, current: 20.0 }.retryable());
        assert!(VelocityError::InternalError { context: String::new(), source: String::new() }.retryable());
    }

    #[test]
    fn test_not_retryable_deterministic_errors() {
        assert!(!VelocityError::WorkflowNotFound { workflow_key: 1 }.retryable());
        assert!(!VelocityError::PermissionDenied { action: String::new(), required_role: String::new() }.retryable());
        assert!(!VelocityError::ShutdownInProgress.retryable());
        assert!(!VelocityError::SerializationError { context: String::new(), source: String::new() }.retryable());
        assert!(!VelocityError::NamespaceAlreadyExists { namespace: String::new() }.retryable());
    }

    // ── FFI code mapping ──────────────────────────────────────────────────

    #[test]
    fn test_ffi_code_workflow_errors() {
        assert_eq!(VelocityError::WorkflowNotFound { workflow_key: 0 }.to_ffi_code(), -100);
        assert_eq!(VelocityError::WorkflowAlreadyCompleted { workflow_key: 0, status: String::new() }.to_ffi_code(), -101);
        assert_eq!(VelocityError::InvalidWorkflowState { workflow_key: 0, expected: String::new(), actual: String::new() }.to_ffi_code(), -102);
        assert_eq!(VelocityError::StepOutOfRange { workflow_key: 0, step: 0, total_steps: 0 }.to_ffi_code(), -103);
        assert_eq!(VelocityError::StepAlreadyCompleted { workflow_key: 0, step: 0 }.to_ffi_code(), -104);
    }

    #[test]
    fn test_ffi_code_namespace_errors() {
        assert_eq!(VelocityError::NamespaceNotFound { namespace: String::new() }.to_ffi_code(), -200);
        assert_eq!(VelocityError::NamespaceAlreadyExists { namespace: String::new() }.to_ffi_code(), -201);
        assert_eq!(VelocityError::TaskQueueNotFound { task_queue: String::new() }.to_ffi_code(), -202);
    }

    #[test]
    fn test_ffi_code_security_errors() {
        assert_eq!(VelocityError::RateLimitExceeded { limit: 0.0, current: 0.0 }.to_ffi_code(), -300);
        assert_eq!(VelocityError::AuthenticationFailed { reason: String::new() }.to_ffi_code(), -301);
        assert_eq!(VelocityError::PermissionDenied { action: String::new(), required_role: String::new() }.to_ffi_code(), -302);
    }

    #[test]
    fn test_ffi_code_infra_and_internal() {
        assert_eq!(VelocityError::DatabaseError { operation: String::new(), source: String::new() }.to_ffi_code(), -700);
        assert_eq!(VelocityError::InternalError { context: String::new(), source: String::new() }.to_ffi_code(), -800);
        assert_eq!(VelocityError::ShutdownInProgress.to_ffi_code(), -900);
    }

    // ── Error name ────────────────────────────────────────────────────────

    #[test]
    fn test_error_name() {
        assert_eq!(VelocityError::WorkflowNotFound { workflow_key: 0 }.error_name(), "WorkflowNotFound");
        assert_eq!(VelocityError::ShutdownInProgress.error_name(), "ShutdownInProgress");
    }

    // ── From conversions ──────────────────────────────────────────────────

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let ve: VelocityError = io_err.into();
        assert!(matches!(ve, VelocityError::DatabaseError { .. }));
        assert!(ve.to_string().contains("file missing"));
    }
}
