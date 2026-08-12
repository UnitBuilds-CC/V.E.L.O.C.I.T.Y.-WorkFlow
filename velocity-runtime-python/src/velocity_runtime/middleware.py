"""
Velocity Runtime middleware system.

Middleware wraps handler invocations with before/after/error hooks.
Supports global middleware and per-service middleware.
"""

import asyncio
import time
from typing import Any, Callable, Optional, Union

from velocity_runtime.core import VirtualObject, Service, Workflow


class MiddlewareContext:
    """Context passed to middleware functions."""

    __slots__ = (
        "invocation_id", "service_name", "handler_name", "key",
        "input_data", "metadata", "start_time",
    )

    def __init__(
        self,
        invocation_id: str,
        service_name: str,
        handler_name: str,
        key: str = "",
        input_data: Any = None,
    ):
        self.invocation_id = invocation_id
        self.service_name = service_name
        self.handler_name = handler_name
        self.key = key
        self.input_data = input_data
        self.metadata: dict = {}
        self.start_time: float = time.monotonic()

    @property
    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.start_time) * 1000


# Middleware signature: async fn(ctx, next) -> result
MiddlewareFn = Callable[
    [MiddlewareContext, Callable],
    Any,  # returns result or awaits next
]


class MiddlewareChain:
    """Ordered middleware pipeline with before/after/error hooks."""

    def __init__(self):
        self._global: list = []
        self._per_service: dict = {}

    def use(self, fn: MiddlewareFn) -> None:
        """Add a global middleware."""
        self._global.append(fn)

    def use_for(self, service_name: str, fn: MiddlewareFn) -> None:
        """Add middleware for a specific service."""
        self._per_service.setdefault(service_name, []).append(fn)

    def get_chain(self, service_name: str) -> list:
        """Get the full middleware chain for a service (global + per-service)."""
        chain = list(self._global)
        chain.extend(self._per_service.get(service_name, []))
        return chain

    def clear(self) -> None:
        """Remove all middleware."""
        self._global.clear()
        self._per_service.clear()


# ─── Built-in middleware ─────────────────────────────────────────────────────

def logging_middleware(logger: Optional[Callable] = None) -> MiddlewareFn:
    """Middleware that logs invocations with timing."""
    import logging as _logging
    _logger = logger or _logging.getLogger("velocity_runtime")

    async def middleware(ctx: MiddlewareContext, next_fn: Callable) -> Any:
        _logger.info(
            "invocation_start service=%s handler=%s key=%s invocation_id=%s",
            ctx.service_name, ctx.handler_name, ctx.key, ctx.invocation_id,
        )
        try:
            result = await next_fn() if asyncio.iscoroutinefunction(next_fn) else next_fn()
            elapsed = ctx.elapsed_ms
            _logger.info(
                "invocation_complete service=%s handler=%s invocation_id=%s elapsed_ms=%.1f",
                ctx.service_name, ctx.handler_name, ctx.invocation_id, elapsed,
            )
            return result
        except Exception as e:
            elapsed = ctx.elapsed_ms
            _logger.error(
                "invocation_error service=%s handler=%s invocation_id=%s elapsed_ms=%.1f error=%s",
                ctx.service_name, ctx.handler_name, ctx.invocation_id, elapsed, e,
            )
            raise

    return middleware


def metrics_middleware(metrics_collector: Any) -> MiddlewareFn:
    """Middleware that records invocation metrics."""

    async def middleware(ctx: MiddlewareContext, next_fn: Callable) -> Any:
        metrics_collector.record_invocation_start(ctx.service_name, ctx.handler_name)
        start = time.monotonic()
        try:
            result = await next_fn() if asyncio.iscoroutinefunction(next_fn) else next_fn()
            duration = (time.monotonic() - start) * 1000
            metrics_collector.record_invocation_complete(
                ctx.service_name, ctx.handler_name, duration, success=True,
            )
            return result
        except Exception as e:
            duration = (time.monotonic() - start) * 1000
            metrics_collector.record_invocation_complete(
                ctx.service_name, ctx.handler_name, duration, success=False,
            )
            raise

    return middleware


def timeout_middleware(default_timeout_ms: int = 30_000) -> MiddlewareFn:
    """Middleware that enforces invocation timeouts."""
    from velocity_runtime.errors import TimeoutError

    async def middleware(ctx: MiddlewareContext, next_fn: Callable) -> Any:
        timeout_ms = ctx.metadata.get("timeout_ms", default_timeout_ms)
        if timeout_ms <= 0:
            result = await next_fn() if asyncio.iscoroutinefunction(next_fn) else next_fn()
            return result

        timeout_sec = timeout_ms / 1000.0
        try:
            result = await asyncio.wait_for(
                next_fn() if asyncio.iscoroutinefunction(next_fn) else next_fn(),
                timeout=timeout_sec,
            )
            return result
        except asyncio.TimeoutError:
            raise TimeoutError(ctx.invocation_id, timeout_ms)

    return middleware
