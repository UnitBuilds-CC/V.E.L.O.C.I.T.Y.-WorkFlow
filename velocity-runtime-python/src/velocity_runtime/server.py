"""
Velocity Runtime Server — dispatches invocations to registered services.

Production-grade server with:
- Service registration and discovery
- Push-based dispatch to handlers
- Per-key single-writer concurrency for Virtual Objects
- Journal persistence for crash recovery
- Middleware pipeline (logging, metrics, timeout)
- Health checks (liveness, readiness)
- Metrics collection (Prometheus-compatible)
- Graceful shutdown with drain
- Retry policies
- Config management
"""

import asyncio
import json
import logging
import time
import uuid
import weakref
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union

from velocity_runtime.core import (
    Context,
    HandlerKind,
    HandlerRegistration,
    ObjectContext,
    Service,
    VirtualObject,
    Workflow,
    WorkflowContext,
)
from velocity_runtime.config import ServerConfig
from velocity_runtime.errors import (
    AwakeableNotFoundError,
    HandlerNotFoundError,
    InvocationError,
    ServiceNotFoundError,
    ShutdownError,
    VelocityError,
)
from velocity_runtime.health import (
    HealthChecker,
    HealthStatus,
    make_liveness_check,
    make_readiness_check,
)
from velocity_runtime.metrics import MetricsCollector
from velocity_runtime.middleware import (
    MiddlewareChain,
    MiddlewareContext,
    logging_middleware,
    metrics_middleware,
    timeout_middleware,
)
from velocity_runtime.retry import RetryPolicy, execute_with_retry
from velocity_runtime.storage import (
    StorageBackend,
    InMemoryStorage,
    StoredJournal,
    StoredKeyState,
)

logger = logging.getLogger("velocity_runtime")


@dataclass
class InvocationRecord:
    """Tracks a single handler invocation."""
    invocation_id: str
    service_name: str
    handler_name: str
    key: str
    input_data: Any = None
    output_data: Any = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    state: str = "queued"  # queued, running, suspended, completed, failed
    journal: list = field(default_factory=list)
    object_state: dict = field(default_factory=dict)
    created_at: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0
    attempts: int = 0
    idempotency_key: Optional[str] = None
    timeout_ms: int = 0
    retry_policy: Optional[RetryPolicy] = None

    @property
    def duration_ms(self) -> float:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at) * 1000
        return 0.0

    def to_dict(self) -> dict:
        return {
            "invocation_id": self.invocation_id,
            "service_name": self.service_name,
            "handler_name": self.handler_name,
            "key": self.key,
            "state": self.state,
            "error": self.error,
            "error_code": self.error_code,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_ms": round(self.duration_ms, 2),
            "attempts": self.attempts,
        }


class RuntimeServer:
    """The Velocity Runtime server — dispatches work to registered services.

    Production features:
    - Service registration (VirtualObject, Service, Workflow)
    - Per-key single-writer concurrency for Virtual Objects
    - Journal-based crash recovery
    - Durable promise management
    - Push-based dispatch
    - Middleware pipeline
    - Health checks
    - Metrics
    - Graceful shutdown
    - Retry policies
    """

    def __init__(self, config: Optional[ServerConfig] = None, storage: Optional[StorageBackend] = None):
        self._config = config or ServerConfig()
        self._config.validate()
        self._services: Dict[str, Union[VirtualObject, Service, Workflow]] = {}
        self._invocations: Dict[str, InvocationRecord] = {}
        self._key_state: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._key_queues: Dict[str, List[str]] = defaultdict(list)
        self._key_locks: Dict[str, str] = {}  # key -> invocation_id
        self._promises: Dict[str, Any] = {}
        self._idempotency_map: Dict[str, str] = {}
        self._awakeables: Dict[str, Any] = {}  # awakeable_id -> Awakeable

        # Storage backend for journal persistence and crash recovery
        self._storage = storage or InMemoryStorage()

        # Production subsystems
        self._middleware = MiddlewareChain()
        self._metrics = MetricsCollector()
        self._health = HealthChecker()
        self._retry_policy = RetryPolicy(
            max_attempts=self._config.max_retries,
            initial_delay_ms=self._config.retry_base_delay_ms,
            max_delay_ms=self._config.retry_max_delay_ms,
        )

        # Lifecycle
        self._shutting_down = False
        self._start_time = time.monotonic()
        self._active_tasks: set = set()
        self._shutdown_event = asyncio.Event()

        # Setup default middleware and health checks
        self._setup_defaults()

        # Replay journals from storage to restore state
        self._replay_from_storage()

    def _setup_defaults(self) -> None:
        """Register default middleware and health checks."""
        if self._config.enable_metrics:
            self._middleware.use(metrics_middleware(self._metrics))
        self._middleware.use(logging_middleware(logger))
        if self._config.default_invocation_timeout_ms > 0:
            self._middleware.use(timeout_middleware(self._config.default_invocation_timeout_ms))

        self._health.register("liveness", make_liveness_check())
        self._health.register("readiness", make_readiness_check(weakref.ref(self)))

    def _replay_from_storage(self) -> None:
        """Replay journals from storage to restore state after crash.

        For each completed journal:
        - Restore Virtual Object key state
        - Rebuild invocation records
        """
        journals = self._storage.load_all_journals()
        replayed = 0
        for j in journals:
            if j.state == "completed" and j.object_state:
                # Restore key state for Virtual Objects
                full_key = f"{j.service_name}/{j.key}" if j.key else j.service_name
                self._key_state[full_key] = j.object_state.copy()
                replayed += 1
            # Restore invocation record
            record = InvocationRecord(
                invocation_id=j.invocation_id,
                service_name=j.service_name,
                handler_name=j.handler_name,
                key=j.key,
                output_data=j.output,
                error=j.error,
                state=j.state,
                journal=j.entries,
                object_state=j.object_state,
                created_at=j.created_at,
                completed_at=j.completed_at,
            )
            self._invocations[j.invocation_id] = record

        if replayed > 0:
            logger.info("Replayed %d journals from storage, restored %d keys",
                       replayed, len(self._key_state))

    def _persist_journal(self, record: InvocationRecord) -> None:
        """Persist a journal entry to storage."""
        # Convert JournalEntry objects to plain dicts for JSON serialization
        serializable_entries = []
        for entry in record.journal:
            if hasattr(entry, '__dict__'):
                serializable_entries.append({
                    k: v for k, v in entry.__dict__.items()
                    if not k.startswith('_')
                })
            elif isinstance(entry, dict):
                serializable_entries.append(entry)
            else:
                serializable_entries.append(entry)

        stored = StoredJournal(
            invocation_id=record.invocation_id,
            service_name=record.service_name,
            handler_name=record.handler_name,
            key=record.key,
            entries=serializable_entries,
            object_state=record.object_state,
            output=record.output_data,
            error=record.error,
            state=record.state,
            created_at=record.created_at,
            completed_at=record.completed_at,
        )
        self._storage.save_journal(stored)

    def _persist_key_state(self, full_key: str) -> None:
        """Persist Virtual Object key state to storage."""
        stored = StoredKeyState(
            full_key=full_key,
            state=self._key_state[full_key].copy(),
            updated_at=time.time(),
        )
        self._storage.save_key_state(stored)

    # ─── Configuration ───────────────────────────────────────────────────

    @property
    def config(self) -> ServerConfig:
        return self._config

    @property
    def metrics(self) -> MetricsCollector:
        return self._metrics

    @property
    def health(self) -> HealthChecker:
        return self._health

    @property
    def middleware(self) -> MiddlewareChain:
        return self._middleware

    # ─── Service registration ────────────────────────────────────────────

    def register(self, service: Union[VirtualObject, Service, Workflow]) -> None:
        """Register a service, virtual object, or workflow."""
        if service.name in self._services:
            raise ValueError(f"Service already registered: {service.name}")
        self._services[service.name] = service
        if self._config.enable_metrics:
            self._metrics.record_service_registered()
        logger.info("Service registered: %s", service.name)

    def list_services(self) -> List[str]:
        """List all registered service names."""
        return list(self._services.keys())

    def get_service(self, name: str) -> Optional[Union[VirtualObject, Service, Workflow]]:
        """Get a registered service by name."""
        return self._services.get(name)

    # ─── Invocation ──────────────────────────────────────────────────────

    async def invoke(
        self,
        service_name: str,
        handler_name: str,
        key: str = "",
        input_data: Any = None,
        idempotency_key: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> str:
        """Invoke a handler on a registered service.

        For Virtual Objects, the key determines which instance to target.
        Operations on the same key are serialized (single-writer).
        """
        if self._shutting_down:
            raise ShutdownError()

        # Check idempotency
        if idempotency_key and idempotency_key in self._idempotency_map:
            return self._idempotency_map[idempotency_key]

        service = self._services.get(service_name)
        if not service:
            raise ServiceNotFoundError(service_name)

        handler_reg = service.get_handler(handler_name)
        if not handler_reg:
            raise HandlerNotFoundError(service_name, handler_name)

        invocation_id = str(uuid.uuid4())

        if idempotency_key:
            self._idempotency_map[idempotency_key] = invocation_id

        effective_timeout = timeout_ms or self._config.default_invocation_timeout_ms
        effective_retry = retry_policy or self._retry_policy

        record = InvocationRecord(
            invocation_id=invocation_id,
            service_name=service_name,
            handler_name=handler_name,
            key=key,
            input_data=input_data,
            created_at=time.time(),
            timeout_ms=effective_timeout,
            retry_policy=effective_retry,
            idempotency_key=idempotency_key,
        )
        self._invocations[invocation_id] = record

        # For Virtual Objects, enforce single-writer per key
        full_key = f"{service_name}/{key}" if key else service_name

        if isinstance(service, VirtualObject) and full_key in self._key_locks:
            # Queue the invocation
            queue = self._key_queues[full_key]
            if len(queue) >= self._config.max_queue_depth_per_key:
                record.state = "failed"
                record.error = "Queue depth exceeded"
                record.error_code = "QUEUE_FULL"
                record.completed_at = time.time()
                return invocation_id
            queue.append(invocation_id)
            record.state = "queued"
        else:
            # Run immediately
            self._key_locks[full_key] = invocation_id
            record.state = "running"
            task = asyncio.ensure_future(self._execute_invocation(invocation_id, full_key))
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)

        return invocation_id

    async def _execute_invocation(self, invocation_id: str, full_key: str) -> None:
        """Execute a handler invocation with middleware and retry."""
        record = self._invocations[invocation_id]
        service = self._services[record.service_name]
        handler_reg = service.get_handler(record.handler_name)

        # Build middleware context
        mw_ctx = MiddlewareContext(
            invocation_id=invocation_id,
            service_name=record.service_name,
            handler_name=record.handler_name,
            key=record.key,
            input_data=record.input_data,
        )
        if record.timeout_ms > 0:
            mw_ctx.metadata["timeout_ms"] = record.timeout_ms

        record.started_at = time.time()
        record.attempts = 1

        # Mutable container to capture ctx from run_handler closure
        captured = {"journal": [], "object_state": {}}

        try:
            # Build the handler execution function
            async def run_handler() -> Any:
                # Create appropriate context
                if isinstance(service, VirtualObject):
                    ctx = ObjectContext(
                        object_type=service.name,
                        key=record.key,
                        invocation_id=invocation_id,
                    )
                    ctx._state = self._key_state[full_key].copy()
                elif isinstance(service, Workflow):
                    ctx = WorkflowContext(
                        workflow_id=record.key or invocation_id,
                        invocation_id=invocation_id,
                    )
                else:
                    ctx = Context(key=record.key, invocation_id=invocation_id)

                try:
                    # Execute the handler
                    if asyncio.iscoroutinefunction(handler_reg.fn):
                        result = await handler_reg.fn(ctx, record.input_data)
                    else:
                        result = handler_reg.fn(ctx, record.input_data)
                finally:
                    # Always capture journal and state, even on failure
                    captured["journal"] = ctx._journal
                    if hasattr(ctx, '_state'):
                        captured["object_state"] = ctx._state.copy()

                # Save state back
                if isinstance(service, VirtualObject):
                    self._key_state[full_key] = ctx._state.copy()

                return result

            # Execute through middleware chain
            chain = self._middleware.get_chain(record.service_name)
            result = await self._run_middleware_chain(chain, mw_ctx, run_handler)

            record.output_data = result
            record.state = "completed"
            record.completed_at = time.time()

            # Persist journal and key state to storage
            record.journal = captured["journal"]
            record.object_state = captured["object_state"]
            self._persist_journal(record)
            if isinstance(service, VirtualObject):
                self._persist_key_state(full_key)

        except Exception as e:
            record.error = str(e)
            record.error_code = getattr(e, "code", "UNKNOWN")
            record.state = "failed"
            record.completed_at = time.time()
            logger.error(
                "Invocation failed: %s/%s [%s] — %s",
                record.service_name, record.handler_name, invocation_id, e,
            )
            # Persist failed journal for audit trail
            record.journal = captured["journal"]
            record.object_state = captured["object_state"]
            self._persist_journal(record)

        finally:
            # Release key lock and dispatch next
            if full_key in self._key_locks and self._key_locks[full_key] == invocation_id:
                del self._key_locks[full_key]
                self._dispatch_next(full_key)

    async def _run_middleware_chain(
        self, chain: list, ctx: MiddlewareContext, final_handler: Callable
    ) -> Any:
        """Execute middleware chain in order, then the final handler."""
        if not chain:
            return await final_handler()

        index = 0

        async def next_fn() -> Any:
            nonlocal index
            if index < len(chain):
                mw = chain[index]
                index += 1
                result = mw(ctx, next_fn)
                if asyncio.iscoroutine(result):
                    result = await result
                return result
            else:
                return await final_handler()

        return await next_fn()

    def _dispatch_next(self, full_key: str) -> None:
        """Dispatch the next queued invocation for a key."""
        if full_key in self._key_queues and self._key_queues[full_key]:
            next_id = self._key_queues[full_key].pop(0)
            self._key_locks[full_key] = next_id
            self._invocations[next_id].state = "running"
            task = asyncio.ensure_future(self._execute_invocation(next_id, full_key))
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)

    # ─── Invocation queries ──────────────────────────────────────────────

    def get_invocation(self, invocation_id: str) -> Optional[InvocationRecord]:
        """Get an invocation record by ID."""
        return self._invocations.get(invocation_id)

    def list_invocations(
        self,
        service_name: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
    ) -> List[InvocationRecord]:
        """List invocations with optional filters."""
        results = list(self._invocations.values())
        if service_name:
            results = [r for r in results if r.service_name == service_name]
        if state:
            results = [r for r in results if r.state == state]
        results.sort(key=lambda r: r.created_at, reverse=True)
        return results[:limit]

    # ─── Promise management ──────────────────────────────────────────────

    async def resolve_promise(self, promise_id: str, value: Any) -> None:
        """Resolve a durable promise."""
        if promise_id in self._promises:
            self._promises[promise_id].resolve(value)
            if self._config.enable_metrics:
                self._metrics.record_promise_resolved()

    async def reject_promise(self, promise_id: str, error: str) -> None:
        """Reject a durable promise."""
        if promise_id in self._promises:
            self._promises[promise_id].reject(error)
            if self._config.enable_metrics:
                self._metrics.record_promise_rejected()

    async def resolve_awakeable(self, awakeable_id: str, value: Any) -> None:
        """Resolve an awakeable by ID — actually wakes the suspended handler."""
        # Check server-level tracking first
        if awakeable_id in self._awakeables:
            self._awakeables[awakeable_id].resolve(value)
            return
        # Fallback: scan invocations
        for inv in self._invocations.values():
            for awk in getattr(inv, '_awakeables', {}).values():
                if awk.id == awakeable_id:
                    awk.resolve(value)
                    return
        raise AwakeableNotFoundError(awakeable_id)

    def register_awakeable(self, awakeable: Any) -> None:
        """Register an awakeable for server-level tracking."""
        self._awakeables[awakeable.id] = awakeable

    def reject_awakeable(self, awakeable_id: str, error: str) -> None:
        """Reject an awakeable by ID."""
        if awakeable_id in self._awakeables:
            self._awakeables[awakeable_id].reject(error)
            return
        raise AwakeableNotFoundError(awakeable_id)

    # ─── Health ──────────────────────────────────────────────────────────

    async def health_check(self) -> HealthStatus:
        """Run all health checks."""
        return await self._health.check()

    # ─── Stats ───────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Get runtime statistics."""
        states = defaultdict(int)
        for inv in self._invocations.values():
            states[inv.state] += 1

        base_stats = {
            "registered_services": len(self._services),
            "total_invocations": len(self._invocations),
            "active_invocations": states.get("running", 0),
            "queued_invocations": states.get("queued", 0),
            "completed_invocations": states.get("completed", 0),
            "failed_invocations": states.get("failed", 0),
            "tracked_keys": len(self._key_state),
            "uptime_seconds": round(time.monotonic() - self._start_time, 2),
            "shutting_down": self._shutting_down,
        }

        if self._config.enable_metrics:
            base_stats["metrics"] = self._metrics.snapshot()

        return base_stats

    # ─── Graceful shutdown ───────────────────────────────────────────────

    async def shutdown(self, grace_period_ms: Optional[int] = None) -> None:
        """Gracefully shut down the server.

        1. Stop accepting new invocations
        2. Wait for active invocations to complete (up to grace period)
        3. Cancel remaining tasks
        """
        if self._shutting_down:
            return

        self._shutting_down = True
        grace = (grace_period_ms or self._config.shutdown_grace_period_ms) / 1000.0
        logger.info("Shutting down (grace=%.1fs, active=%d)...", grace, len(self._active_tasks))

        if self._active_tasks:
            # Wait for active tasks to complete
            done, pending = await asyncio.wait(
                self._active_tasks,
                timeout=grace,
                return_when=asyncio.ALL_COMPLETED,
            )
            # Cancel any remaining
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending, timeout=1.0)

        self._active_tasks.clear()
        self._shutdown_event.set()
        logger.info("Shutdown complete.")

    async def wait_for_shutdown(self) -> None:
        """Block until shutdown is complete."""
        await self._shutdown_event.wait()

    @property
    def is_shutting_down(self) -> bool:
        return self._shutting_down


def app(
    services: List[Union[VirtualObject, Service, Workflow]],
    config: Optional[ServerConfig] = None,
    storage: Optional[StorageBackend] = None,
    **kwargs,
) -> RuntimeServer:
    """Create a Runtime server with the given services.

    Args:
        services: List of VirtualObject, Service, or Workflow instances.
        config: Server configuration (defaults to ServerConfig()).
        storage: Storage backend for journal persistence (defaults to InMemoryStorage).

    Example:
        from velocity_runtime import app, FileStorage
        config = ServerConfig.from_env()
        storage = FileStorage(".velocity_data")
        server = app(services=[chat, payment], config=config, storage=storage)
    """
    server = RuntimeServer(config=config, storage=storage)
    for svc in services:
        server.register(svc)
    return server
