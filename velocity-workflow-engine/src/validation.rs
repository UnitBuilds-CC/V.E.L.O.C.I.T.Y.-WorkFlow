//! Workflow request validation.
//!
//! Validates all inbound API requests (start, signal, query, step completion) before they
//! reach the engine core. Provides clear, structured error messages for every failure mode.

use std::fmt;

// ─── Request Structs ──────────────────────────────────────────────────────────

/// Request to start a new workflow execution.
#[derive(Debug, Clone)]
pub struct StartWorkflowRequest {
    pub workflow_id: u64,
    pub namespace_id: u64,
    pub workflow_type_id: u64,
    pub task_queue_name: String,
    pub input_payload: Option<Vec<u8>>,
    pub cron_schedule: Option<String>,
    pub total_steps: u32,
}

/// Request to deliver a signal to a running workflow.
#[derive(Debug, Clone)]
pub struct SignalRequest {
    pub workflow_id: u64,
    pub namespace_id: u64,
    pub signal_name: String,
    pub payload: Option<Vec<u8>>,
    /// Number of pending signals already buffered for this workflow.
    pub pending_signal_count: u64,
}

/// Request to query a workflow's state.
#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub workflow_id: u64,
    pub namespace_id: u64,
    pub query_name: String,
    pub query_payload: Option<Vec<u8>>,
}

// ─── Validation Error ─────────────────────────────────────────────────────────

/// Structured validation error returned by all validator methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    /// `workflow_id` must be > 0.
    InvalidWorkflowId,
    /// `namespace_id` must be > 0.
    InvalidNamespace,
    /// Task queue name must not be empty.
    InvalidTaskQueue,
    /// `step` exceeds `total_steps` or total_steps exceeds the hard limit.
    StepOutOfRange {
        step: u32,
        total_steps: u32,
        max_allowed: u32,
    },
    /// The workflow has already reached a terminal state.
    WorkflowAlreadyCompleted,
    /// Payload exceeds the maximum allowed size.
    PayloadTooLarge { size: usize, max: usize },
    /// Too many pending signals buffered for this workflow.
    TooManySignals { count: u64, max: u64 },
    /// The supplied cron expression could not be parsed.
    InvalidCronExpression(String),
    /// Signal name must not be empty.
    InvalidSignalName,
    /// Query name must not be empty.
    InvalidQueryName,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWorkflowId => write!(f, "workflow_id must be greater than 0"),
            Self::InvalidNamespace => write!(f, "namespace_id must be greater than 0"),
            Self::InvalidTaskQueue => write!(f, "task_queue_name must not be empty"),
            Self::StepOutOfRange {
                step,
                total_steps,
                max_allowed,
            } => {
                write!(
                    f,
                    "step {} out of range (total_steps={}, max_allowed={})",
                    step, total_steps, max_allowed
                )
            }
            Self::WorkflowAlreadyCompleted => write!(f, "workflow has already completed"),
            Self::PayloadTooLarge { size, max } => {
                write!(
                    f,
                    "payload size {} exceeds maximum allowed size {}",
                    size, max
                )
            }
            Self::TooManySignals { count, max } => {
                write!(f, "too many pending signals ({} > max {})", count, max)
            }
            Self::InvalidCronExpression(expr) => {
                write!(f, "invalid cron expression: '{}'", expr)
            }
            Self::InvalidSignalName => write!(f, "signal_name must not be empty"),
            Self::InvalidQueryName => write!(f, "query_name must not be empty"),
        }
    }
}

impl std::error::Error for ValidationError {}

// ─── Validator ────────────────────────────────────────────────────────────────

/// Maximum payload size: 10 MB.
const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;

/// Maximum pending signals per workflow.
const MAX_PENDING_SIGNALS: u64 = 1_000;

/// Maximum total steps per workflow.
const MAX_TOTAL_STEPS: u32 = 100_000;

/// Validates workflow API requests before they reach the engine.
pub struct WorkflowValidator {
    max_payload_size: usize,
    max_pending_signals: u64,
    max_total_steps: u32,
}

impl WorkflowValidator {
    /// Create a validator with default limits.
    pub fn new() -> Self {
        Self {
            max_payload_size: MAX_PAYLOAD_SIZE,
            max_pending_signals: MAX_PENDING_SIGNALS,
            max_total_steps: MAX_TOTAL_STEPS,
        }
    }

    /// Create a validator with custom limits.
    pub fn with_limits(
        max_payload_size: usize,
        max_pending_signals: u64,
        max_total_steps: u32,
    ) -> Self {
        Self {
            max_payload_size,
            max_pending_signals,
            max_total_steps,
        }
    }

    /// Validate a start-workflow request.
    pub fn validate_start_request(
        &self,
        req: &StartWorkflowRequest,
    ) -> Result<(), ValidationError> {
        if req.workflow_id == 0 {
            return Err(ValidationError::InvalidWorkflowId);
        }
        if req.namespace_id == 0 {
            return Err(ValidationError::InvalidNamespace);
        }
        if req.task_queue_name.is_empty() {
            return Err(ValidationError::InvalidTaskQueue);
        }
        if req.total_steps == 0 || req.total_steps > self.max_total_steps {
            return Err(ValidationError::StepOutOfRange {
                step: 0,
                total_steps: req.total_steps,
                max_allowed: self.max_total_steps,
            });
        }
        if let Some(ref payload) = req.input_payload {
            if payload.len() > self.max_payload_size {
                return Err(ValidationError::PayloadTooLarge {
                    size: payload.len(),
                    max: self.max_payload_size,
                });
            }
        }
        if let Some(ref cron) = req.cron_schedule {
            if !cron.is_empty() && !Self::is_valid_cron_basic(cron) {
                return Err(ValidationError::InvalidCronExpression(cron.clone()));
            }
        }
        Ok(())
    }

    /// Validate a signal request.
    pub fn validate_signal_request(&self, req: &SignalRequest) -> Result<(), ValidationError> {
        if req.workflow_id == 0 {
            return Err(ValidationError::InvalidWorkflowId);
        }
        if req.namespace_id == 0 {
            return Err(ValidationError::InvalidNamespace);
        }
        if req.signal_name.is_empty() {
            return Err(ValidationError::InvalidSignalName);
        }
        if let Some(ref payload) = req.payload {
            if payload.len() > self.max_payload_size {
                return Err(ValidationError::PayloadTooLarge {
                    size: payload.len(),
                    max: self.max_payload_size,
                });
            }
        }
        if req.pending_signal_count >= self.max_pending_signals {
            return Err(ValidationError::TooManySignals {
                count: req.pending_signal_count,
                max: self.max_pending_signals,
            });
        }
        Ok(())
    }

    /// Validate a query request.
    pub fn validate_query_request(&self, req: &QueryRequest) -> Result<(), ValidationError> {
        if req.workflow_id == 0 {
            return Err(ValidationError::InvalidWorkflowId);
        }
        if req.namespace_id == 0 {
            return Err(ValidationError::InvalidNamespace);
        }
        if req.query_name.is_empty() {
            return Err(ValidationError::InvalidQueryName);
        }
        if let Some(ref payload) = req.query_payload {
            if payload.len() > self.max_payload_size {
                return Err(ValidationError::PayloadTooLarge {
                    size: payload.len(),
                    max: self.max_payload_size,
                });
            }
        }
        Ok(())
    }

    /// Validate a step completion.
    pub fn validate_step_completion(
        &self,
        _key: u64,
        step: u32,
        total_steps: u32,
    ) -> Result<(), ValidationError> {
        if total_steps == 0 || total_steps > self.max_total_steps {
            return Err(ValidationError::StepOutOfRange {
                step,
                total_steps,
                max_allowed: self.max_total_steps,
            });
        }
        if step >= total_steps {
            return Err(ValidationError::StepOutOfRange {
                step,
                total_steps,
                max_allowed: self.max_total_steps,
            });
        }
        Ok(())
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    /// Basic cron validation: must have 5 or 6 space-separated fields.
    fn is_valid_cron_basic(expr: &str) -> bool {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        parts.len() == 5 || parts.len() == 6
    }
}

impl Default for WorkflowValidator {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_start_request() -> StartWorkflowRequest {
        StartWorkflowRequest {
            workflow_id: 1,
            namespace_id: 1,
            workflow_type_id: 100,
            task_queue_name: "default-queue".into(),
            input_payload: None,
            cron_schedule: None,
            total_steps: 10,
        }
    }

    fn valid_signal_request() -> SignalRequest {
        SignalRequest {
            workflow_id: 1,
            namespace_id: 1,
            signal_name: "my-signal".into(),
            payload: None,
            pending_signal_count: 0,
        }
    }

    fn valid_query_request() -> QueryRequest {
        QueryRequest {
            workflow_id: 1,
            namespace_id: 1,
            query_name: "status".into(),
            query_payload: None,
        }
    }

    #[test]
    fn test_valid_start_request() {
        let v = WorkflowValidator::new();
        assert!(v.validate_start_request(&valid_start_request()).is_ok());
    }

    #[test]
    fn test_invalid_workflow_id() {
        let v = WorkflowValidator::new();
        let mut req = valid_start_request();
        req.workflow_id = 0;
        assert_eq!(
            v.validate_start_request(&req),
            Err(ValidationError::InvalidWorkflowId)
        );
    }

    #[test]
    fn test_invalid_namespace() {
        let v = WorkflowValidator::new();
        let mut req = valid_start_request();
        req.namespace_id = 0;
        assert_eq!(
            v.validate_start_request(&req),
            Err(ValidationError::InvalidNamespace)
        );
    }

    #[test]
    fn test_invalid_task_queue() {
        let v = WorkflowValidator::new();
        let mut req = valid_start_request();
        req.task_queue_name = String::new();
        assert_eq!(
            v.validate_start_request(&req),
            Err(ValidationError::InvalidTaskQueue)
        );
    }

    #[test]
    fn test_payload_too_large() {
        let v = WorkflowValidator::new();
        let mut req = valid_start_request();
        req.input_payload = Some(vec![0u8; MAX_PAYLOAD_SIZE + 1]);
        match v.validate_start_request(&req) {
            Err(ValidationError::PayloadTooLarge { size, max }) => {
                assert_eq!(size, MAX_PAYLOAD_SIZE + 1);
                assert_eq!(max, MAX_PAYLOAD_SIZE);
            }
            other => panic!("expected PayloadTooLarge, got {:?}", other),
        }
    }

    #[test]
    fn test_step_out_of_range() {
        let v = WorkflowValidator::new();
        // step >= total_steps
        assert!(v.validate_step_completion(1, 10, 10).is_err());
        // total_steps exceeds max
        assert!(v.validate_step_completion(1, 0, 200_000).is_err());
        // valid
        assert!(v.validate_step_completion(1, 5, 10).is_ok());
    }

    #[test]
    fn test_too_many_signals() {
        let v = WorkflowValidator::new();
        let mut req = valid_signal_request();
        req.pending_signal_count = MAX_PENDING_SIGNALS;
        match v.validate_signal_request(&req) {
            Err(ValidationError::TooManySignals { count, max }) => {
                assert_eq!(count, MAX_PENDING_SIGNALS);
                assert_eq!(max, MAX_PENDING_SIGNALS);
            }
            other => panic!("expected TooManySignals, got {:?}", other),
        }
    }

    #[test]
    fn test_invalid_cron_expression() {
        let v = WorkflowValidator::new();
        let mut req = valid_start_request();
        req.cron_schedule = Some("not-a-cron".into());
        assert!(matches!(
            v.validate_start_request(&req),
            Err(ValidationError::InvalidCronExpression(_))
        ));
    }

    #[test]
    fn test_valid_signal_and_query() {
        let v = WorkflowValidator::new();
        assert!(v.validate_signal_request(&valid_signal_request()).is_ok());
        assert!(v.validate_query_request(&valid_query_request()).is_ok());
    }

    #[test]
    fn test_empty_signal_name() {
        let v = WorkflowValidator::new();
        let mut req = valid_signal_request();
        req.signal_name = String::new();
        assert_eq!(
            v.validate_signal_request(&req),
            Err(ValidationError::InvalidSignalName)
        );
    }
}
