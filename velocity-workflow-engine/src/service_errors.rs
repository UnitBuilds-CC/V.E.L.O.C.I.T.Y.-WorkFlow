//! Service errors matching Temporal's common/serviceerror (670 lines).
//!
//! Covers: typed error hierarchy for gRPC service errors, status codes,
//! retryable vs non-retryable, and error conversion.

use std::collections::HashMap;
use std::fmt;

// ═══════════════════════════════════════════════════════════════════════════════
// Service Error Status
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServiceErrorStatus {
    OK = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
    NamespaceNotFound = 100,
    NamespaceAlreadyExists = 101,
    WorkflowNotFound = 102,
    WorkflowAlreadyStarted = 103,
    WorkflowNotReady = 104,
    ShardOwnershipLost = 200,
    TaskAlreadyCompleted = 300,
    ActivityNotFound = 301,
    QueryFailed = 400,
    CancellationAlreadyRequested = 500,
}

impl ServiceErrorStatus {
    pub fn is_retryable(&self) -> bool {
        matches!(self,
            Self::Unavailable | Self::ResourceExhausted | Self::Aborted |
            Self::DeadlineExceeded | Self::ShardOwnershipLost
        )
    }

    pub fn is_client_error(&self) -> bool {
        matches!(self,
            Self::InvalidArgument | Self::NotFound | Self::AlreadyExists |
            Self::PermissionDenied | Self::OutOfRange | Self::Unauthenticated |
            Self::NamespaceNotFound | Self::NamespaceAlreadyExists |
            Self::WorkflowNotFound | Self::WorkflowAlreadyStarted |
            Self::ActivityNotFound | Self::CancellationAlreadyRequested
        )
    }

    pub fn is_server_error(&self) -> bool {
        matches!(self,
            Self::Internal | Self::Unimplemented | Self::DataLoss | Self::Unknown
        )
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::OK => "OK",
            Self::Cancelled => "Cancelled",
            Self::Unknown => "Unknown",
            Self::InvalidArgument => "InvalidArgument",
            Self::DeadlineExceeded => "DeadlineExceeded",
            Self::NotFound => "NotFound",
            Self::AlreadyExists => "AlreadyExists",
            Self::PermissionDenied => "PermissionDenied",
            Self::ResourceExhausted => "ResourceExhausted",
            Self::FailedPrecondition => "FailedPrecondition",
            Self::Aborted => "Aborted",
            Self::OutOfRange => "OutOfRange",
            Self::Unimplemented => "Unimplemented",
            Self::Internal => "Internal",
            Self::Unavailable => "Unavailable",
            Self::DataLoss => "DataLoss",
            Self::Unauthenticated => "Unauthenticated",
            Self::NamespaceNotFound => "NamespaceNotFound",
            Self::NamespaceAlreadyExists => "NamespaceAlreadyExists",
            Self::WorkflowNotFound => "WorkflowNotFound",
            Self::WorkflowAlreadyStarted => "WorkflowAlreadyStarted",
            Self::WorkflowNotReady => "WorkflowNotReady",
            Self::ShardOwnershipLost => "ShardOwnershipLost",
            Self::TaskAlreadyCompleted => "TaskAlreadyCompleted",
            Self::ActivityNotFound => "ActivityNotFound",
            Self::QueryFailed => "QueryFailed",
            Self::CancellationAlreadyRequested => "CancellationAlreadyRequested",
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Service Error
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct ServiceError {
    pub status: ServiceErrorStatus,
    pub message: String,
    pub details: HashMap<String, String>,
    pub cause: Option<Box<ServiceError>>,
}

impl ServiceError {
    pub fn new(status: ServiceErrorStatus, message: &str) -> Self {
        Self { status, message: message.to_string(), details: HashMap::new(), cause: None }
    }

    pub fn with_detail(mut self, key: &str, value: &str) -> Self {
        self.details.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_cause(mut self, cause: ServiceError) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    pub fn is_retryable(&self) -> bool {
        self.status.is_retryable()
    }

    // Convenience constructors
    pub fn not_found(msg: &str) -> Self { Self::new(ServiceErrorStatus::NotFound, msg) }
    pub fn already_exists(msg: &str) -> Self { Self::new(ServiceErrorStatus::AlreadyExists, msg) }
    pub fn invalid_argument(msg: &str) -> Self { Self::new(ServiceErrorStatus::InvalidArgument, msg) }
    pub fn internal(msg: &str) -> Self { Self::new(ServiceErrorStatus::Internal, msg) }
    pub fn unavailable(msg: &str) -> Self { Self::new(ServiceErrorStatus::Unavailable, msg) }
    pub fn permission_denied(msg: &str) -> Self { Self::new(ServiceErrorStatus::PermissionDenied, msg) }
    pub fn resource_exhausted(msg: &str) -> Self { Self::new(ServiceErrorStatus::ResourceExhausted, msg) }
    pub fn unauthenticated(msg: &str) -> Self { Self::new(ServiceErrorStatus::Unauthenticated, msg) }
    pub fn deadline_exceeded(msg: &str) -> Self { Self::new(ServiceErrorStatus::DeadlineExceeded, msg) }
    pub fn cancelled(msg: &str) -> Self { Self::new(ServiceErrorStatus::Cancelled, msg) }
    pub fn unimplemented(msg: &str) -> Self { Self::new(ServiceErrorStatus::Unimplemented, msg) }
    pub fn aborted(msg: &str) -> Self { Self::new(ServiceErrorStatus::Aborted, msg) }
    pub fn data_loss(msg: &str) -> Self { Self::new(ServiceErrorStatus::DataLoss, msg) }

    // Domain-specific constructors
    pub fn namespace_not_found(ns: &str) -> Self {
        Self::new(ServiceErrorStatus::NamespaceNotFound, &format!("namespace not found: {}", ns))
    }
    pub fn namespace_already_exists(ns: &str) -> Self {
        Self::new(ServiceErrorStatus::NamespaceAlreadyExists, &format!("namespace already exists: {}", ns))
    }
    pub fn workflow_not_found(wf_id: &str, run_id: &str) -> Self {
        Self::new(ServiceErrorStatus::WorkflowNotFound,
            &format!("workflow not found: {} / {}", wf_id, run_id))
    }
    pub fn workflow_already_started(wf_id: &str) -> Self {
        Self::new(ServiceErrorStatus::WorkflowAlreadyStarted,
            &format!("workflow already started: {}", wf_id))
    }
    pub fn shard_ownership_lost(shard_id: u32, owner: &str) -> Self {
        Self::new(ServiceErrorStatus::ShardOwnershipLost,
            &format!("shard {} ownership lost, current owner: {}", shard_id, owner))
    }
    pub fn activity_not_found(activity_id: &str) -> Self {
        Self::new(ServiceErrorStatus::ActivityNotFound,
            &format!("activity not found: {}", activity_id))
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.status.name(), self.message)?;
        if let Some(ref cause) = self.cause {
            write!(f, " (caused by: {})", cause)?;
        }
        Ok(())
    }
}

impl std::error::Error for ServiceError {}

// ═══════════════════════════════════════════════════════════════════════════════
// Error Counter
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ErrorCounter {
    counts: std::sync::RwLock<HashMap<ServiceErrorStatus, u64>>,
}

impl ErrorCounter {
    pub fn new() -> Self {
        Self { counts: std::sync::RwLock::new(HashMap::new()) }
    }

    pub fn record(&self, error: &ServiceError) {
        let mut counts = self.counts.write().unwrap();
        *counts.entry(error.status).or_insert(0) += 1;
    }

    pub fn count(&self, status: ServiceErrorStatus) -> u64 {
        self.counts.read().unwrap().get(&status).copied().unwrap_or(0)
    }

    pub fn total(&self) -> u64 {
        self.counts.read().unwrap().values().sum()
    }

    pub fn reset(&self) {
        self.counts.write().unwrap().clear();
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = ServiceError::not_found("resource missing");
        assert_eq!(err.status, ServiceErrorStatus::NotFound);
        assert_eq!(err.message, "resource missing");
    }

    #[test]
    fn test_retryable_errors() {
        assert!(ServiceErrorStatus::Unavailable.is_retryable());
        assert!(ServiceErrorStatus::ResourceExhausted.is_retryable());
        assert!(ServiceErrorStatus::Aborted.is_retryable());
        assert!(!ServiceErrorStatus::NotFound.is_retryable());
        assert!(!ServiceErrorStatus::InvalidArgument.is_retryable());
    }

    #[test]
    fn test_client_errors() {
        assert!(ServiceErrorStatus::NotFound.is_client_error());
        assert!(ServiceErrorStatus::InvalidArgument.is_client_error());
        assert!(ServiceErrorStatus::AlreadyExists.is_client_error());
        assert!(!ServiceErrorStatus::Internal.is_client_error());
    }

    #[test]
    fn test_server_errors() {
        assert!(ServiceErrorStatus::Internal.is_server_error());
        assert!(ServiceErrorStatus::Unimplemented.is_server_error());
        assert!(!ServiceErrorStatus::NotFound.is_server_error());
    }

    #[test]
    fn test_error_with_details() {
        let err = ServiceError::invalid_argument("bad input")
            .with_detail("field", "workflow_id")
            .with_detail("reason", "empty");
        assert_eq!(err.details.get("field"), Some(&"workflow_id".to_string()));
        assert_eq!(err.details.get("reason"), Some(&"empty".to_string()));
    }

    #[test]
    fn test_error_with_cause() {
        let cause = ServiceError::internal("db error");
        let err = ServiceError::unavailable("service degraded").with_cause(cause);
        assert!(err.cause.is_some());
        let display = format!("{}", err);
        assert!(display.contains("db error"));
    }

    #[test]
    fn test_domain_constructors() {
        let err = ServiceError::namespace_not_found("test-ns");
        assert_eq!(err.status, ServiceErrorStatus::NamespaceNotFound);
        assert!(err.message.contains("test-ns"));

        let err = ServiceError::workflow_already_started("wf-123");
        assert_eq!(err.status, ServiceErrorStatus::WorkflowAlreadyStarted);

        let err = ServiceError::shard_ownership_lost(42, "node-3");
        assert_eq!(err.status, ServiceErrorStatus::ShardOwnershipLost);
    }

    #[test]
    fn test_error_display() {
        let err = ServiceError::not_found("item");
        let s = format!("{}", err);
        assert!(s.contains("NotFound"));
        assert!(s.contains("item"));
    }

    #[test]
    fn test_error_counter() {
        let counter = ErrorCounter::new();
        counter.record(&ServiceError::not_found("a"));
        counter.record(&ServiceError::not_found("b"));
        counter.record(&ServiceError::internal("c"));

        assert_eq!(counter.count(ServiceErrorStatus::NotFound), 2);
        assert_eq!(counter.count(ServiceErrorStatus::Internal), 1);
        assert_eq!(counter.total(), 3);
    }

    #[test]
    fn test_error_counter_reset() {
        let counter = ErrorCounter::new();
        counter.record(&ServiceError::not_found("a"));
        counter.reset();
        assert_eq!(counter.total(), 0);
    }

    #[test]
    fn test_status_name() {
        assert_eq!(ServiceErrorStatus::NotFound.name(), "NotFound");
        assert_eq!(ServiceErrorStatus::Internal.name(), "Internal");
        assert_eq!(ServiceErrorStatus::ShardOwnershipLost.name(), "ShardOwnershipLost");
    }
}
