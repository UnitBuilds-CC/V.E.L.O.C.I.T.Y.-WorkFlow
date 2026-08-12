"""
Core types for the Velocity Runtime SDK.

Implements Restate-compatible primitives:
- VirtualObject: Actor-model keyed state with single-writer per key
- Service: Stateless durable service handlers
- Workflow: Long-running durable functions
- Context: Durable execution context with run/get/set/promise/awakeable
"""

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Union


class HandlerKind(Enum):
    """Kind of handler registered on a service."""
    WORKFLOW = "workflow"
    SERVICE = "service"
    SHARED = "shared"


@dataclass
class JournalEntry:
    """A journal entry for crash recovery."""
    sequence: int
    entry_type: str
    input_data: Any = None
    output_data: Any = None
    completed: bool = False


@dataclass
class Awakeable:
    """An external resolution point.

    Create an awakeable, hand its ID to an external system,
    and await its resolution.
    """
    id: str
    _resolved: bool = False
    _value: Any = None
    _error: Optional[str] = None
    _event: Optional[asyncio.Event] = None

    def __post_init__(self):
        self._event = asyncio.Event()

    @property
    def resolved(self) -> bool:
        return self._resolved

    def resolve(self, value: Any) -> None:
        """Resolve this awakeable with a value."""
        self._resolved = True
        self._value = value
        if self._event:
            self._event.set()

    def reject(self, error: str) -> None:
        """Reject this awakeable with an error."""
        self._resolved = True
        self._error = error
        if self._event:
            self._event.set()

    async def wait(self) -> Any:
        """Await the resolution of this awakeable."""
        if self._event:
            await self._event.wait()
        if self._error:
            raise RuntimeError(f"Awakeable rejected: {self._error}")
        return self._value


@dataclass
class DurablePromise:
    """A durable promise — a named resolution point.

    Can be created, awaited, and resolved/rejected by any part of the system.
    """
    id: str
    _resolved: bool = False
    _value: Any = None
    _error: Optional[str] = None
    _event: Optional[asyncio.Event] = None

    def __post_init__(self):
        self._event = asyncio.Event()

    @property
    def resolved(self) -> bool:
        return self._resolved

    @property
    def pending(self) -> bool:
        return not self._resolved

    def resolve(self, value: Any) -> None:
        """Resolve this promise with a value."""
        if self._resolved:
            raise RuntimeError(f"Promise already resolved: {self.id}")
        self._resolved = True
        self._value = value
        if self._event:
            self._event.set()

    def reject(self, error: str) -> None:
        """Reject this promise with an error."""
        if self._resolved:
            raise RuntimeError(f"Promise already resolved: {self.id}")
        self._resolved = True
        self._error = error
        if self._event:
            self._event.set()

    async def await_value(self) -> Any:
        """Await the resolution of this promise."""
        if self._event:
            await self._event.wait()
        if self._error:
            raise RuntimeError(f"Promise rejected: {self._error}")
        return self._value


class Context:
    """Durable execution context for service handlers.

    Provides:
    - ctx.run(): Execute a durable step (survives crashes)
    - ctx.get()/ctx.set()/ctx.clear(): Keyed state operations
    - ctx.promise(): Create a durable promise
    - ctx.awakeable(): Create an external resolution point
    - ctx.sleep(): Durable sleep
    - ctx.key(): Get the current invocation key
    """

    def __init__(self, key: str = "", invocation_id: str = ""):
        self._key = key
        self._invocation_id = invocation_id or str(uuid.uuid4())
        self._state: Dict[str, Any] = {}
        self._journal: List[JournalEntry] = []
        self._journal_index: int = 0
        self._promises: Dict[str, DurablePromise] = {}
        self._awakeables: Dict[str, Awakeable] = {}
        self._replay_mode: bool = False

    @property
    def key(self) -> str:
        """Get the current invocation key."""
        return self._key

    @property
    def id(self) -> str:
        """Get the current invocation ID."""
        return self._invocation_id

    async def run(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute a durable step.

        The step is journaled. On crash recovery, the step is not re-executed;
        instead, the journaled result is returned.
        """
        seq = len(self._journal)

        # Check journal for replay
        if seq < self._journal_index:
            entry = self._journal[seq]
            if entry.completed:
                return entry.output_data

        # Execute the step
        if asyncio.iscoroutinefunction(fn):
            result = await fn(*args, **kwargs)
        else:
            result = fn(*args, **kwargs)

        # Journal the result
        self._journal.append(JournalEntry(
            sequence=seq,
            entry_type="durable_step",
            input_data={"args": args, "kwargs": kwargs},
            output_data=result,
            completed=True,
        ))

        return result

    async def get(self, state_key: str) -> Any:
        """Get a state value by key."""
        return self._state.get(state_key)

    async def set(self, state_key: str, value: Any) -> None:
        """Set a state value."""
        self._state[state_key] = value
        self._journal.append(JournalEntry(
            sequence=len(self._journal),
            entry_type="state_set",
            input_data={"key": state_key, "value": value},
            completed=True,
        ))

    async def clear(self, state_key: str) -> None:
        """Clear a state value."""
        self._state.pop(state_key, None)
        self._journal.append(JournalEntry(
            sequence=len(self._journal),
            entry_type="state_clear",
            input_data={"key": state_key},
            completed=True,
        ))

    def promise(self, promise_id: str) -> DurablePromise:
        """Create or get a durable promise."""
        if promise_id not in self._promises:
            self._promises[promise_id] = DurablePromise(id=promise_id)
        return self._promises[promise_id]

    def awakeable(self) -> Awakeable:
        """Create a new awakeable (external resolution point)."""
        awk_id = f"awk_{self._invocation_id}_{len(self._awakeables)}"
        awk = Awakeable(id=awk_id)
        self._awakeables[awk_id] = awk
        return awk

    async def sleep(self, duration_ms: int) -> None:
        """Durable sleep — survives crashes."""
        self._journal.append(JournalEntry(
            sequence=len(self._journal),
            entry_type="sleep",
            input_data={"duration_ms": duration_ms},
            completed=True,
        ))
        await asyncio.sleep(duration_ms / 1000.0)

    def _replay_journal(self, entries: List[JournalEntry], state: Dict[str, Any]) -> None:
        """Replay a journal for crash recovery."""
        self._journal = entries
        self._journal_index = len(entries)
        self._state = state.copy()
        self._replay_mode = True


class ObjectContext(Context):
    """Context for Virtual Object handlers.

    Extends Context with object-specific features:
    - Per-key isolated state
    - Single-writer concurrency (enforced by the runtime)
    """

    def __init__(self, object_type: str, key: str, invocation_id: str = ""):
        super().__init__(key=key, invocation_id=invocation_id)
        self._object_type = object_type

    @property
    def object_type(self) -> str:
        return self._object_type

    @property
    def full_key(self) -> str:
        return f"{self._object_type}/{self._key}"


class WorkflowContext(Context):
    """Context for Workflow handlers.

    Extends Context with workflow-specific features:
    - Long-running execution
    - Durable to completion
    """

    def __init__(self, workflow_id: str, invocation_id: str = ""):
        super().__init__(key=workflow_id, invocation_id=invocation_id)
        self._workflow_id = workflow_id

    @property
    def workflow_id(self) -> str:
        return self._workflow_id


@dataclass
class HandlerRegistration:
    """A registered handler on a service or virtual object."""
    name: str
    fn: Callable
    kind: HandlerKind
    service_name: str


class VirtualObject:
    """A stateful entity keyed by an ID (actor-model).

    Each key has isolated K/V state, single-writer concurrency
    (operations on the same key are serialized), and parallel
    execution across different keys.

    Example:
        chat = VirtualObject("ChatAgent")

        @chat.handler()
        async def message(ctx: ObjectContext, query: str):
            history = await ctx.get("history") or []
            history.append(query)
            await ctx.set("history", history)
            return "ok"
    """

    def __init__(self, name: str):
        self._name = name
        self._handlers: Dict[str, HandlerRegistration] = {}

    @property
    def name(self) -> str:
        return self._name

    def handler(self, kind: HandlerKind = HandlerKind.WORKFLOW):
        """Decorator to register a handler on this virtual object."""
        def decorator(fn: Callable) -> Callable:
            reg = HandlerRegistration(
                name=fn.__name__,
                fn=fn,
                kind=kind,
                service_name=self._name,
            )
            self._handlers[fn.__name__] = reg
            return fn
        return decorator

    @property
    def handlers(self) -> Dict[str, HandlerRegistration]:
        return self._handlers

    def get_handler(self, name: str) -> Optional[HandlerRegistration]:
        return self._handlers.get(name)


class Service:
    """A stateless durable service.

    Unlike VirtualObject, a Service has no keyed state.
    Handlers are independent and can run in parallel.

    Example:
        payment = Service("PaymentService")

        @payment.handler()
        async def charge(ctx: Context, order_id: str, amount: float):
            result = await ctx.run(lambda: stripe_charge(order_id, amount))
            return result
    """

    def __init__(self, name: str):
        self._name = name
        self._handlers: Dict[str, HandlerRegistration] = {}

    @property
    def name(self) -> str:
        return self._name

    def handler(self, kind: HandlerKind = HandlerKind.SERVICE):
        """Decorator to register a handler on this service."""
        def decorator(fn: Callable) -> Callable:
            reg = HandlerRegistration(
                name=fn.__name__,
                fn=fn,
                kind=kind,
                service_name=self._name,
            )
            self._handlers[fn.__name__] = reg
            return fn
        return decorator

    @property
    def handlers(self) -> Dict[str, HandlerRegistration]:
        return self._handlers

    def get_handler(self, name: str) -> Optional[HandlerRegistration]:
        return self._handlers.get(name)


class Workflow:
    """A long-running durable function.

    Workflows run to completion and are durable across crashes.

    Example:
        order_wf = Workflow("OrderWorkflow")

        @order_wf.handler()
        async def run(ctx: WorkflowContext, order_id: str):
            await ctx.run(lambda: charge(order_id))
            await ctx.run(lambda: ship(order_id))
            return "completed"
    """

    def __init__(self, name: str):
        self._name = name
        self._handlers: Dict[str, HandlerRegistration] = {}

    @property
    def name(self) -> str:
        return self._name

    def handler(self):
        """Decorator to register the workflow handler."""
        def decorator(fn: Callable) -> Callable:
            reg = HandlerRegistration(
                name=fn.__name__,
                fn=fn,
                kind=HandlerKind.WORKFLOW,
                service_name=self._name,
            )
            self._handlers[fn.__name__] = reg
            return fn
        return decorator

    @property
    def handlers(self) -> Dict[str, HandlerRegistration]:
        return self._handlers

    def get_handler(self, name: str) -> Optional[HandlerRegistration]:
        return self._handlers.get(name)
