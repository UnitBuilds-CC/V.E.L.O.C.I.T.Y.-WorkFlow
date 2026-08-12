"""
VELOCITY-WorkFlow Python SDK

Cross-language worker SDK for the VELOCITY-WorkFlow gRPC server.
"""

from .client import VelocityClient, WorkflowHandle, WorkflowDescription, WorkflowStatus
from .transpiler import transpile_python, is_temporal_workflow, TranspilerConfig, TranspileResult, TranspileStats
from .exceptions import (
    VelocityError,
    WorkflowNotFoundError,
    WorkflowAlreadyCompletedError,
    ConnectionError,
    TimeoutError,
    RateLimitError,
    AuthenticationError,
    InternalError,
)
from .interceptors import (
    WorkflowInterceptor,
    ActivityInterceptor,
    LoggingInterceptor,
    MetricsInterceptor,
    TracingInterceptor,
    InterceptorChain,
)
from .testing import (
    WorkflowTestEnvironment,
    MockVelocityClient,
    assert_workflow_completed,
    assert_signal_received,
)

__all__ = [
    # Client
    "VelocityClient",
    "WorkflowHandle",
    "WorkflowDescription",
    "WorkflowStatus",
    # Transpiler
    "transpile_python",
    "is_temporal_workflow",
    "TranspilerConfig",
    "TranspileResult",
    "TranspileStats",
    # Exceptions
    "VelocityError",
    "WorkflowNotFoundError",
    "WorkflowAlreadyCompletedError",
    "ConnectionError",
    "TimeoutError",
    "RateLimitError",
    "AuthenticationError",
    "InternalError",
    # Interceptors
    "WorkflowInterceptor",
    "ActivityInterceptor",
    "LoggingInterceptor",
    "MetricsInterceptor",
    "TracingInterceptor",
    "InterceptorChain",
    # Testing
    "WorkflowTestEnvironment",
    "MockVelocityClient",
    "assert_workflow_completed",
    "assert_signal_received",
]

__version__ = "0.1.0"
