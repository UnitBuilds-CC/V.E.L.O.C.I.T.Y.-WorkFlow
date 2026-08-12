//! Error types for the VELOCITY-WorkFlow Rust SDK.
//!
//! Error codes are consistent across all SDKs (Python, Go, TypeScript, Java, PHP, Ruby).
//! Each error carries a numeric code, a human-readable message, and a retryable flag.

use std::fmt;

// ─── Error Kind ──────────────────────────────────────────────────────────────

/// Classification of SDK errors, each mapped to a stable numeric code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ErrorKind {
    /// 0 — Unknown / uncategorised error.
    Unknown = 0,
    /// 1 — Workflow not found.
    WorkflowNotFound = 1,
    /// 2 — Workflow already completed.
    WorkflowAlreadyCompleted = 2,
    /// 3 — Connection to engine / server failed.
    ConnectionFailed = 3,
    /// 4 — Operation timed out.
    Timeout = 4,
    /// 5 — Rate limit exceeded.
    RateLimitExceeded = 5,
    /// 6 — Authentication / authorisation failure.
    AuthenticationFailed = 6,
    /// 7 — Internal engine error.
    Internal = 7,
}

impl ErrorKind {
    /// Whether this error category is typically retryable.
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::ConnectionFailed | Self::Timeout | Self::RateLimitExceeded | Self::Internal)
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::WorkflowNotFound => write!(f, "WorkflowNotFound"),
            Self::WorkflowAlreadyCompleted => write!(f, "WorkflowAlreadyCompleted"),
            Self::ConnectionFailed => write!(f, "ConnectionFailed"),
            Self::Timeout => write!(f, "Timeout"),
            Self::RateLimitExceeded => write!(f, "RateLimitExceeded"),
            Self::AuthenticationFailed => write!(f, "AuthenticationFailed"),
            Self::Internal => write!(f, "Internal"),
        }
    }
}

// ─── VelocityError ───────────────────────────────────────────────────────────

/// Base error type for all VELOCITY-WorkFlow SDK operations.
#[derive(Debug, Clone)]
pub struct VelocityError {
    /// Human-readable error message.
    pub message: String,
    /// Error classification.
    pub kind: ErrorKind,
    /// Whether the operation can be retried.
    pub retryable: bool,
    /// Optional structured details (e.g. workflow key, target address).
    pub details: Vec<(String, String)>,
}

impl VelocityError {
    /// Create a new error with the given kind and message.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        let retryable = kind.is_retryable();
        Self {
            message: message.into(),
            kind,
            retryable,
            details: Vec::new(),
        }
    }

    /// Attach a key-value detail to this error (builder pattern).
    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.push((key.into(), value.into()));
        self
    }

    /// Numeric error code (matches other SDKs).
    pub fn code(&self) -> i32 {
        self.kind as i32
    }
}

impl fmt::Display for VelocityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let retry = if self.retryable { " (retryable)" } else { "" };
        write!(f, "VelocityError[{}]: {}{}", self.kind, self.message, retry)
    }
}

impl std::error::Error for VelocityError {}

// ─── Convenience constructors ────────────────────────────────────────────────

/// Shorthand: workflow not found.
pub fn workflow_not_found(workflow_key: u64) -> VelocityError {
    VelocityError::new(ErrorKind::WorkflowNotFound, format!("Workflow not found: {workflow_key}"))
        .with_detail("workflow_key", workflow_key.to_string())
}

/// Shorthand: workflow already completed.
pub fn workflow_already_completed(workflow_key: u64) -> VelocityError {
    VelocityError::new(ErrorKind::WorkflowAlreadyCompleted, format!("Workflow already completed: {workflow_key}"))
        .with_detail("workflow_key", workflow_key.to_string())
}

/// Shorthand: connection failure.
pub fn connection_failed(target: &str) -> VelocityError {
    VelocityError::new(ErrorKind::ConnectionFailed, format!("Failed to connect to {target}"))
        .with_detail("target", target.to_string())
}

/// Shorthand: operation timeout.
pub fn timeout(operation: &str, timeout_ms: u64) -> VelocityError {
    VelocityError::new(ErrorKind::Timeout, format!("Operation '{operation}' timed out after {timeout_ms}ms"))
        .with_detail("operation", operation.to_string())
        .with_detail("timeout_ms", timeout_ms.to_string())
}

/// Shorthand: rate limit exceeded.
pub fn rate_limit_exceeded(retry_after_ms: u64) -> VelocityError {
    VelocityError::new(ErrorKind::RateLimitExceeded, "Rate limit exceeded")
        .with_detail("retry_after_ms", retry_after_ms.to_string())
}

/// Shorthand: authentication failure.
pub fn authentication_failed(reason: &str) -> VelocityError {
    VelocityError::new(ErrorKind::AuthenticationFailed, format!("Authentication failed: {reason}"))
}

/// Shorthand: internal error.
pub fn internal(message: &str) -> VelocityError {
    VelocityError::new(ErrorKind::Internal, message)
}
