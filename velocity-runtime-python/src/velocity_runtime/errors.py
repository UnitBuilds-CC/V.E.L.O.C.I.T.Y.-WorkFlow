"""
Velocity Runtime error hierarchy.

All errors derive from VelocityError for easy catch-all handling.
Each error has a code for programmatic handling and a message for humans.
"""

from typing import Any, Optional


class VelocityError(Exception):
    """Base error for all Velocity Runtime errors."""

    def __init__(self, message: str, code: str = "VELOCITY_ERROR", details: Optional[dict] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(code={self.code!r}, message={self.message!r})"


class ServiceNotFoundError(VelocityError):
    """Raised when a requested service is not registered."""

    def __init__(self, service_name: str):
        super().__init__(
            f"Service not found: {service_name}",
            code="SERVICE_NOT_FOUND",
            details={"service_name": service_name},
        )
        self.service_name = service_name


class HandlerNotFoundError(VelocityError):
    """Raised when a requested handler is not found on a service."""

    def __init__(self, service_name: str, handler_name: str):
        super().__init__(
            f"Handler not found: {service_name}/{handler_name}",
            code="HANDLER_NOT_FOUND",
            details={"service_name": service_name, "handler_name": handler_name},
        )
        self.service_name = service_name
        self.handler_name = handler_name


class InvocationError(VelocityError):
    """Raised when a handler invocation fails."""

    def __init__(self, invocation_id: str, cause: Optional[Exception] = None):
        msg = f"Invocation failed: {invocation_id}"
        if cause:
            msg += f" — {cause}"
        super().__init__(msg, code="INVOCATION_ERROR", details={"invocation_id": invocation_id})
        self.invocation_id = invocation_id
        self.cause = cause


class TimeoutError(VelocityError):
    """Raised when an invocation exceeds its timeout."""

    def __init__(self, invocation_id: str, timeout_ms: int):
        super().__init__(
            f"Invocation timed out after {timeout_ms}ms: {invocation_id}",
            code="TIMEOUT",
            details={"invocation_id": invocation_id, "timeout_ms": timeout_ms},
        )
        self.invocation_id = invocation_id
        self.timeout_ms = timeout_ms


class IdempotencyConflictError(VelocityError):
    """Raised when an idempotency key conflicts with a different request."""

    def __init__(self, idempotency_key: str):
        super().__init__(
            f"Idempotency key conflict: {idempotency_key}",
            code="IDEMPOTENCY_CONFLICT",
            details={"idempotency_key": idempotency_key},
        )
        self.idempotency_key = idempotency_key


class AwakeableNotFoundError(VelocityError):
    """Raised when an awakeable ID is not found."""

    def __init__(self, awakeable_id: str):
        super().__init__(
            f"Awakeable not found: {awakeable_id}",
            code="AWAKEABLE_NOT_FOUND",
            details={"awakeable_id": awakeable_id},
        )
        self.awakeable_id = awakeable_id


class PromiseError(VelocityError):
    """Raised for durable promise operations."""

    def __init__(self, message: str, promise_id: str = ""):
        super().__init__(message, code="PROMISE_ERROR", details={"promise_id": promise_id})
        self.promise_id = promise_id


class DoubleResolveError(PromiseError):
    """Raised when a promise or awakeable is resolved/rejected twice."""

    def __init__(self, entity_id: str, entity_type: str = "promise"):
        super().__init__(
            f"{entity_type.capitalize()} already resolved: {entity_id}",
            promise_id=entity_id,
        )
        self.entity_type = entity_type


class ShutdownError(VelocityError):
    """Raised when operations are attempted on a shut-down server."""

    def __init__(self):
        super().__init__("Server is shutting down", code="SHUTDOWN")


class SerializationError(VelocityError):
    """Raised when input/output cannot be serialized."""

    def __init__(self, message: str):
        super().__init__(message, code="SERIALIZATION_ERROR")


class TransportError(VelocityError):
    """Raised when a transport-level error occurs."""

    def __init__(self, message: str, endpoint: str = ""):
        super().__init__(message, code="TRANSPORT_ERROR", details={"endpoint": endpoint})
        self.endpoint = endpoint


class ConnectionError(TransportError):
    """Raised when a connection to the engine cannot be established."""

    def __init__(self, endpoint: str):
        super().__init__(f"Cannot connect to {endpoint}", endpoint=endpoint)
        self.code = "CONNECTION_ERROR"
