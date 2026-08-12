"""
VELOCITY-WorkFlow Python SDK - Exception hierarchy.

Defines error types that map to gRPC status codes and server error codes.
All exceptions include an error_code, message, and retryable flag.
"""

from typing import Optional


class VelocityError(Exception):
    """Base exception for all VELOCITY-WorkFlow errors."""

    def __init__(
        self,
        message: str,
        error_code: int = 0,
        retryable: bool = False,
        details: Optional[dict] = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.retryable = retryable
        self.details = details or {}

    def __str__(self) -> str:
        retry = " (retryable)" if self.retryable else ""
        return f"VelocityError[{self.error_code}]: {self.message}{retry}"


class WorkflowNotFoundError(VelocityError):
    """Raised when a workflow does not exist."""

    def __init__(self, workflow_key: int, message: Optional[str] = None):
        msg = message or f"Workflow not found: {workflow_key}"
        super().__init__(msg, error_code=1, retryable=False)
        self.workflow_key = workflow_key


class WorkflowAlreadyCompletedError(VelocityError):
    """Raised when attempting to modify a completed workflow."""

    def __init__(self, workflow_key: int, message: Optional[str] = None):
        msg = message or f"Workflow already completed: {workflow_key}"
        super().__init__(msg, error_code=2, retryable=False)
        self.workflow_key = workflow_key


class ConnectionError(VelocityError):
    """Raised when connection to the server fails."""

    def __init__(self, target: str, message: Optional[str] = None):
        msg = message or f"Failed to connect to {target}"
        super().__init__(msg, error_code=3, retryable=True)
        self.target = target


class TimeoutError(VelocityError):
    """Raised when an operation times out."""

    def __init__(self, operation: str, timeout_ms: int, message: Optional[str] = None):
        msg = message or f"Operation '{operation}' timed out after {timeout_ms}ms"
        super().__init__(msg, error_code=4, retryable=True)
        self.operation = operation
        self.timeout_ms = timeout_ms


class RateLimitError(VelocityError):
    """Raised when rate limit is exceeded."""

    def __init__(self, retry_after_ms: int = 0, message: Optional[str] = None):
        msg = message or "Rate limit exceeded"
        super().__init__(msg, error_code=5, retryable=True)
        self.retry_after_ms = retry_after_ms


class AuthenticationError(VelocityError):
    """Raised when authentication fails."""

    def __init__(self, message: Optional[str] = None):
        msg = message or "Authentication failed"
        super().__init__(msg, error_code=6, retryable=False)


class InternalError(VelocityError):
    """Raised for internal server errors."""

    def __init__(self, message: Optional[str] = None):
        msg = message or "Internal server error"
        super().__init__(msg, error_code=7, retryable=True)
