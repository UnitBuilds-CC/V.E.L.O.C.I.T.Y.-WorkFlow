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
from .retry import RetryPolicy, retry_with_policy, calculate_backoff, RetryableOperation
from .payload_codec import (
    PayloadCodec,
    JsonCodec,
    BinaryCodec,
    ProtobufCodec,
    CodecChain,
)
from .workflow_stub import WorkflowStub, WorkflowStubOptions
from .annotations import (
    workflow,
    activity,
    signal,
    query,
    update,
    get_registered_workflows,
    get_registered_activities,
    clear_registries,
    scan_module,
)
from .worker import Worker, WorkerOptions, WorkerStats, WorkflowContext

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
    # Retry
    "RetryPolicy",
    "retry_with_policy",
    "calculate_backoff",
    "RetryableOperation",
    # Payload Codec
    "PayloadCodec",
    "JsonCodec",
    "BinaryCodec",
    "ProtobufCodec",
    "CodecChain",
    # Workflow Stub
    "WorkflowStub",
    "WorkflowStubOptions",
    # Auto-Apply Decorators
    "workflow",
    "activity",
    "signal",
    "query",
    "update",
    "get_registered_workflows",
    "get_registered_activities",
    "clear_registries",
    "scan_module",
    # Worker
    "Worker",
    "WorkerOptions",
    "WorkerStats",
    "WorkflowContext",
]

__version__ = "1.0.0"
