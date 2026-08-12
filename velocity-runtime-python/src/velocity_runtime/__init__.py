"""
Velocity Runtime — Restate-compatible durable execution SDK.

Provides a durable runtime for building resilient services:
- VirtualObject: Actor-model keyed state (single-writer per key)
- Service: Stateless durable handlers
- Workflow: Long-running durable functions
- Context: Durable steps (ctx.run), state (ctx.get/set), promises, awakeables

Production features:
- Middleware pipeline (logging, metrics, timeout)
- Health checks (liveness, readiness)
- Metrics collection (Prometheus-compatible)
- Graceful shutdown with drain
- Retry policies with exponential backoff
- Config management (env vars, validation)
- Transport abstraction (HTTP, in-memory)

Example:
    from velocity_runtime import VirtualObject, ObjectContext

    chat = VirtualObject("ChatAgent")

    @chat.handler()
    async def message(ctx: ObjectContext, query: str):
        history = await ctx.get("history") or []
        history.append({"role": "user", "content": query})
        result = await ctx.run(lambda: call_llm(history))
        history.append({"role": "assistant", "content": result})
        await ctx.set("history", history)
        return result

    app = velocity_runtime.app(services=[chat])
"""

# Core types
from velocity_runtime.core import (
    VirtualObject,
    Service,
    Workflow,
    ObjectContext,
    Context,
    WorkflowContext,
    Awakeable,
    DurablePromise,
    HandlerKind,
    JournalEntry,
    HandlerRegistration,
)

# Server
from velocity_runtime.server import RuntimeServer, InvocationRecord, app

# Configuration
from velocity_runtime.config import ServerConfig

# Errors
from velocity_runtime.errors import (
    VelocityError,
    ServiceNotFoundError,
    HandlerNotFoundError,
    InvocationError,
    TimeoutError,
    IdempotencyConflictError,
    AwakeableNotFoundError,
    PromiseError,
    DoubleResolveError,
    ShutdownError,
    SerializationError,
    TransportError,
    ConnectionError,
)

# Middleware
from velocity_runtime.middleware import (
    MiddlewareChain,
    MiddlewareContext,
    MiddlewareFn,
    logging_middleware,
    metrics_middleware,
    timeout_middleware,
)

# Metrics
from velocity_runtime.metrics import (
    MetricsCollector,
    Counter,
    Histogram,
)

# Health
from velocity_runtime.health import (
    HealthChecker,
    HealthCheckResult,
    HealthStatus,
    HealthCheckFn,
    make_liveness_check,
    make_readiness_check,
    make_memory_check,
)

# Retry
from velocity_runtime.retry import (
    RetryPolicy,
    DEFAULT_RETRY_POLICY,
    NO_RETRY_POLICY,
    AGGRESSIVE_RETRY_POLICY,
    CONSERVATIVE_RETRY_POLICY,
    execute_with_retry,
)

# Transport
from velocity_runtime.transport import (
    Transport,
    HttpTransport,
    InMemoryTransport,
    TransportRequest,
    TransportResponse,
)

# Serialization
from velocity_runtime.serialization import (
    serialize,
    deserialize,
    to_json,
    from_json,
    deep_merge,
)

# Storage
from velocity_runtime.storage import (
    StorageBackend,
    InMemoryStorage,
    FileStorage,
    StoredJournal,
    StoredKeyState,
)

__all__ = [
    # Core
    "VirtualObject",
    "Service",
    "Workflow",
    "ObjectContext",
    "Context",
    "WorkflowContext",
    "Awakeable",
    "DurablePromise",
    "HandlerKind",
    "JournalEntry",
    "HandlerRegistration",
    # Server
    "RuntimeServer",
    "InvocationRecord",
    "app",
    # Config
    "ServerConfig",
    # Errors
    "VelocityError",
    "ServiceNotFoundError",
    "HandlerNotFoundError",
    "InvocationError",
    "TimeoutError",
    "IdempotencyConflictError",
    "AwakeableNotFoundError",
    "PromiseError",
    "DoubleResolveError",
    "ShutdownError",
    "SerializationError",
    "TransportError",
    "ConnectionError",
    # Middleware
    "MiddlewareChain",
    "MiddlewareContext",
    "MiddlewareFn",
    "logging_middleware",
    "metrics_middleware",
    "timeout_middleware",
    # Metrics
    "MetricsCollector",
    "Counter",
    "Histogram",
    # Health
    "HealthChecker",
    "HealthCheckResult",
    "HealthStatus",
    "HealthCheckFn",
    "make_liveness_check",
    "make_readiness_check",
    "make_memory_check",
    # Retry
    "RetryPolicy",
    "DEFAULT_RETRY_POLICY",
    "NO_RETRY_POLICY",
    "AGGRESSIVE_RETRY_POLICY",
    "CONSERVATIVE_RETRY_POLICY",
    "execute_with_retry",
    # Transport
    "Transport",
    "HttpTransport",
    "InMemoryTransport",
    "TransportRequest",
    "TransportResponse",
    # Serialization
    "serialize",
    "deserialize",
    "to_json",
    "from_json",
    "deep_merge",
    # Storage
    "StorageBackend",
    "InMemoryStorage",
    "FileStorage",
    "StoredJournal",
    "StoredKeyState",
]

__version__ = "0.1.0"
