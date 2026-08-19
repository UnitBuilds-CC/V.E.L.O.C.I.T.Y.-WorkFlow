"""
Auto-apply decorators for the VELOCITY-WorkFlow Python SDK.

These decorators enable annotation-driven workflow and activity registration.
When a class or function is decorated with @workflow or @activity, it is
automatically registered in a global registry. The Worker class scans this
registry at startup — no manual registration needed.

@example
```python
from velocity_sdk import workflow, activity, WorkflowContext

@activity
def process_payment(order_id: str) -> dict:
    return {"status": "charged", "order_id": order_id}

@workflow
class OrderWorkflow:
    async def run(self, ctx: WorkflowContext, order_id: str):
        result = await ctx.execute_activity("process_payment", order_id)
        return result
```
"""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Dict, Optional, Type

# ─── Global Registries ────────────────────────────────────────────────────────

_workflow_registry: Dict[str, Type] = {}
_activity_registry: Dict[str, Callable] = {}


def workflow(
    cls: Optional[Type] = None,
    *,
    name: Optional[str] = None,
    task_queue: Optional[str] = None,
):
    """
    Decorator that marks a class as a durable workflow.

    The decorated class is automatically registered in the workflow registry.
    The Worker scans this registry at startup and dispatches tasks to the
    matching class based on the workflow type name.

    Can be used with or without arguments:
        @workflow
        class MyWorkflow: ...

        @workflow(name="custom_name", task_queue="orders")
        class MyWorkflow: ...
    """
    def decorator(cls: Type) -> Type:
        wf_name = name or cls.__name__
        cls._velocity_workflow_type = wf_name
        cls._velocity_task_queue = task_queue
        cls._velocity_is_workflow = True
        _workflow_registry[wf_name] = cls
        return cls

    if cls is not None:
        # Used as @workflow without arguments
        return decorator(cls)
    # Used as @workflow(...) with arguments
    return decorator


def activity(
    fn: Optional[Callable] = None,
    *,
    name: Optional[str] = None,
    start_to_close_timeout_ms: Optional[int] = None,
    schedule_to_close_timeout_ms: Optional[int] = None,
    retry_max_attempts: Optional[int] = None,
):
    """
    Decorator that marks a function as a durable activity.

    The decorated function is automatically registered in the activity registry.
    The Worker scans this registry at startup and dispatches activity tasks to
    the matching function based on the activity type name.

    Can be used with or without arguments:
        @activity
        def my_activity(...): ...

        @activity(name="custom_name", retry_max_attempts=3)
        def my_activity(...): ...
    """
    def decorator(fn: Callable) -> Callable:
        act_name = name or fn.__name__
        fn._velocity_activity_type = act_name
        fn._velocity_activity_options = {
            "start_to_close_timeout_ms": start_to_close_timeout_ms,
            "schedule_to_close_timeout_ms": schedule_to_close_timeout_ms,
            "retry_max_attempts": retry_max_attempts,
        }
        fn._velocity_is_activity = True
        _activity_registry[act_name] = fn
        return fn

    if fn is not None:
        # Used as @activity without arguments
        return decorator(fn)
    # Used as @activity(...) with arguments
    return decorator


def signal(name: Optional[str] = None):
    """
    Decorator that marks a method as a signal handler within a workflow class.

    @signal("cancel_order")
    def handle_cancel(self, payload):
        ...
    """
    def decorator(fn: Callable) -> Callable:
        sig_name = name or fn.__name__
        fn._velocity_signal_name = sig_name
        fn._velocity_is_signal = True
        return fn
    return decorator


def query(name: Optional[str] = None):
    """
    Decorator that marks a method as a query handler within a workflow class.

    @query("get_status")
    def handle_status_query(self) -> str:
        return self._status
    """
    def decorator(fn: Callable) -> Callable:
        qry_name = name or fn.__name__
        fn._velocity_query_name = qry_name
        fn._velocity_is_query = True
        return fn
    return decorator


def update(name: Optional[str] = None):
    """
    Decorator that marks a method as an update handler within a workflow class.

    @update("change_address")
    def handle_address_update(self, payload):
        ...
    """
    def decorator(fn: Callable) -> Callable:
        upd_name = name or fn.__name__
        fn._velocity_update_name = upd_name
        fn._velocity_is_update = True
        return fn
    return decorator


# ─── Registry Access ──────────────────────────────────────────────────────────

def get_registered_workflows() -> Dict[str, Type]:
    """Return a copy of the global workflow registry."""
    return dict(_workflow_registry)


def get_registered_activities() -> Dict[str, Callable]:
    """Return a copy of the global activity registry."""
    return dict(_activity_registry)


def clear_registries():
    """Clear both registries (useful for testing)."""
    _workflow_registry.clear()
    _activity_registry.clear()


def scan_module(module: Any) -> None:
    """
    Scan a module for @workflow and @activity decorated classes/functions
    and register them. This is called automatically by the Worker when
    given a module path, but can also be called manually.
    """
    for _attr_name, obj in inspect.getmembers(module):
        if inspect.isclass(obj) and getattr(obj, "_velocity_is_workflow", False):
            wf_name = getattr(obj, "_velocity_workflow_type", obj.__name__)
            _workflow_registry[wf_name] = obj
        elif inspect.isfunction(obj) and getattr(obj, "_velocity_is_activity", False):
            act_name = getattr(obj, "_velocity_activity_type", obj.__name__)
            _activity_registry[act_name] = obj
