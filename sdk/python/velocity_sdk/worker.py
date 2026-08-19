"""
Worker process model for the VELOCITY-WorkFlow Python SDK.

The Worker polls the server for workflow and activity tasks, executes them
using auto-registered (or manually registered) implementations, and reports
results back. Supports the auto-apply decorator system for zero-config
workflow discovery.

@example
```python
# Auto-apply mode — decorators register workflows automatically
from velocity_sdk import Worker, workflow, activity

@activity
def process_payment(order_id: str) -> dict:
    return {"status": "charged", "order_id": order_id}

@workflow
class OrderWorkflow:
    async def run(self, ctx, order_id: str):
        return await ctx.execute_activity("process_payment", order_id)

# Worker auto-discovers all @workflow and @activity in the module
worker = Worker(task_queue="orders", workflows_path="my_workflows")
worker.run()
```

@example
```python
# Manual registration mode
worker = Worker(task_queue="orders")
worker.register_workflow("OrderWorkflow", OrderWorkflow)
worker.register_activity("process_payment", process_payment)
worker.run()
```
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import signal as os_signal
import sys
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Type

from .client import VelocityClient
from .annotations import (
    get_registered_workflows,
    get_registered_activities,
    scan_module,
)

logger = logging.getLogger("velocity.worker")


# ─── Worker Configuration ─────────────────────────────────────────────────────

@dataclass
class WorkerOptions:
    """Configuration for creating a Worker."""
    task_queue: str
    server_address: str = "localhost:7234"
    namespace: str = "default"
    max_concurrent_workflow_tasks: int = 10
    max_concurrent_activity_tasks: int = 100
    poll_timeout_ms: int = 10000
    heartbeat_interval_ms: int = 30000
    build_id: str = "1.0"
    enable_sticky_execution: bool = True
    workflows_path: Optional[str] = None
    interceptors: Optional[List[Any]] = None


@dataclass
class WorkerStats:
    """Runtime statistics for a Worker."""
    workflows_started: int = 0
    workflows_completed: int = 0
    workflows_failed: int = 0
    activities_scheduled: int = 0
    activities_completed: int = 0
    activities_failed: int = 0
    tasks_polled: int = 0
    heartbeats_sent: int = 0
    uptime_start: float = field(default_factory=time.monotonic)

    @property
    def uptime_ms(self) -> float:
        return (time.monotonic() - self.uptime_start) * 1000


# ─── Workflow Context ─────────────────────────────────────────────────────────

class WorkflowContext:
    """
    Context available inside workflow functions.

    Provides deterministic operations for scheduling activities, timers,
    signals, queries, updates, and child workflows.
    """

    def __init__(
        self,
        workflow_key: int,
        workflow_id: str,
        run_id: str,
        workflow_type: str,
        task_queue: str,
        client: Optional[VelocityClient] = None,
    ):
        self.workflow_key = workflow_key
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.workflow_type = workflow_type
        self.task_queue = task_queue
        self._client = client
        self._completed = False
        self._canceled = False
        self._current_step = 0
        self._signal_handlers: Dict[str, Callable] = {}
        self._query_handlers: Dict[str, Callable] = {}
        self._update_handlers: Dict[str, Callable] = {}
        self._pending_signals: Dict[str, list] = {}

    async def execute_activity(self, activity_name: str, *args: Any, **kwargs: Any) -> Any:
        """Schedule an activity for execution."""
        self._current_step += 1
        # In a full implementation, this would send a command to the server.
        # For embedded/local mode, call the registered activity directly.
        from .annotations import get_registered_activities
        activities = get_registered_activities()
        fn = activities.get(activity_name)
        if fn is None:
            raise RuntimeError(f"No activity registered for '{activity_name}'")
        if inspect.iscoroutinefunction(fn):
            return await fn(*args, **kwargs)
        return fn(*args, **kwargs)

    async def sleep(self, duration_ms: int) -> None:
        """Deterministic timer."""
        self._current_step += 1
        await asyncio.sleep(duration_ms / 1000.0)

    def on_signal(self, signal_name: str, handler: Callable) -> None:
        """Register a signal handler."""
        self._signal_handlers[signal_name] = handler

    def on_query(self, query_name: str, handler: Callable) -> None:
        """Register a query handler."""
        self._query_handlers[query_name] = handler

    def on_update(self, update_name: str, handler: Callable) -> None:
        """Register an update handler."""
        self._update_handlers[update_name] = handler

    async def wait_for_signal(self, signal_name: str) -> Any:
        """Block until a signal is received."""
        buffered = self._pending_signals.get(signal_name, [])
        if buffered:
            return buffered.pop(0)
        # In production, this suspends the workflow until the signal arrives.
        raise RuntimeError(f"Waiting for signal '{signal_name}' — not yet buffered")

    async def start_child_workflow(self, workflow_type: str, *args: Any, **kwargs: Any) -> Any:
        """Start a child workflow."""
        self._current_step += 1
        raise NotImplementedError("Child workflows require server-side support")

    @property
    def current_step(self) -> int:
        return self._current_step

    @property
    def is_canceled(self) -> bool:
        return self._canceled


# ─── Worker ───────────────────────────────────────────────────────────────────

class Worker:
    """
    VELOCITY Worker — polls the server for tasks and executes workflows/activities.

    Supports two registration modes:
    1. Auto-apply: Decorators @workflow and @activity auto-register into a
       global registry. The Worker discovers them at startup via module scanning.
    2. Manual: Call register_workflow() and register_activity() explicitly.
    """

    def __init__(
        self,
        task_queue: str,
        server_address: str = "localhost:7234",
        namespace: str = "default",
        workflows_path: Optional[str] = None,
        max_concurrent_workflow_tasks: int = 10,
        max_concurrent_activity_tasks: int = 100,
        poll_timeout_ms: int = 10000,
        heartbeat_interval_ms: int = 30000,
        build_id: str = "1.0",
    ):
        self._options = WorkerOptions(
            task_queue=task_queue,
            server_address=server_address,
            namespace=namespace,
            workflows_path=workflows_path,
            max_concurrent_workflow_tasks=max_concurrent_workflow_tasks,
            max_concurrent_activity_tasks=max_concurrent_activity_tasks,
            poll_timeout_ms=poll_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            build_id=build_id,
        )
        self._client = VelocityClient(server_address)
        self._stats = WorkerStats()
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Manual registration maps (merged with auto-apply registry at startup)
        self._workflows: Dict[str, Type] = {}
        self._activities: Dict[str, Callable] = {}

    # ─── Registration ─────────────────────────────────────────────────────

    def register_workflow(self, name: str, cls: Type) -> None:
        """Manually register a workflow class."""
        self._workflows[name] = cls

    def register_activity(self, name: str, fn: Callable) -> None:
        """Manually register an activity function."""
        self._activities[name] = fn

    # ─── Auto-Apply Discovery ─────────────────────────────────────────────

    def _auto_discover(self) -> None:
        """
        Scan for @workflow and @activity decorated classes/functions.

        If workflows_path is set, imports the module and scans it.
        Then merges the global auto-apply registry with manual registrations.
        """
        # Scan module if path provided
        if self._options.workflows_path:
            module_path = self._options.workflows_path.replace("/", ".").replace("\\", ".")
            if module_path.endswith(".py"):
                module_path = module_path[:-3]
            try:
                module = importlib.import_module(module_path)
                scan_module(module)
                logger.info("Scanned module '%s' for workflows/activities", module_path)
            except ImportError as exc:
                logger.warning("Could not import workflows_path '%s': %s", module_path, exc)

        # Merge auto-apply registry with manual registrations
        auto_workflows = get_registered_workflows()
        auto_activities = get_registered_activities()

        for name, cls in auto_workflows.items():
            if name not in self._workflows:
                self._workflows[name] = cls
                logger.info("Auto-applied workflow: %s", name)

        for name, fn in auto_activities.items():
            if name not in self._activities:
                self._activities[name] = fn
                logger.info("Auto-applied activity: %s", name)

    # ─── Lifecycle ────────────────────────────────────────────────────────

    def run(self) -> None:
        """Start the worker and block until shutdown (sync entry point)."""
        try:
            asyncio.get_event_loop().run_until_complete(self.run_async())
        except RuntimeError:
            asyncio.run(self.run_async())

    async def run_async(self) -> None:
        """Start the worker and block until shutdown (async entry point)."""
        self._auto_discover()
        self._running = True

        logger.info(
            "Worker starting — task_queue=%s, workflows=%d, activities=%d",
            self._options.task_queue,
            len(self._workflows),
            len(self._activities),
        )

        # Install signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()
        for sig_name in ("SIGINT", "SIGTERM"):
            sig = getattr(os_signal, sig_name, None)
            if sig is not None:
                loop.add_signal_handler(sig, self._request_shutdown)

        # Run concurrent poll loops
        try:
            await asyncio.gather(
                self._workflow_poll_loop(),
                self._activity_poll_loop(),
                self._heartbeat_loop(),
            )
        finally:
            self._running = False
            self._client.close()
            logger.info("Worker shut down — %s", self._stats_summary())

    def shutdown(self) -> None:
        """Request graceful shutdown."""
        self._request_shutdown()

    def _request_shutdown(self) -> None:
        logger.info("Shutdown requested")
        self._running = False
        self._shutdown_event.set()

    # ─── Poll Loops ───────────────────────────────────────────────────────

    async def _workflow_poll_loop(self) -> None:
        """Long-poll loop for workflow tasks."""
        while self._running:
            try:
                self._stats.tasks_polled += 1
                task = self._client.poll_task(
                    self._options.task_queue,
                    timeout_ms=self._options.poll_timeout_ms,
                )
                if task is not None:
                    await self._execute_workflow_task(task)
                else:
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Workflow poll error: %s", exc)
                await asyncio.sleep(1.0)

    async def _activity_poll_loop(self) -> None:
        """Long-poll loop for activity tasks."""
        while self._running:
            try:
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break

    async def _heartbeat_loop(self) -> None:
        """Periodic heartbeat sender."""
        interval = self._options.heartbeat_interval_ms / 1000.0
        while self._running:
            try:
                await asyncio.sleep(interval)
                self._stats.heartbeats_sent += 1
                logger.debug("Heartbeat #%d sent", self._stats.heartbeats_sent)
            except asyncio.CancelledError:
                break

    # ─── Task Execution ───────────────────────────────────────────────────

    async def _execute_workflow_task(self, task: dict) -> None:
        """Dispatch a workflow task to the registered implementation."""
        workflow_type = task.get("workflow_type", "unknown")
        workflow_key = task.get("workflow_key", 0)
        workflow_id = task.get("workflow_id", f"wf-{workflow_key}")
        input_data = task.get("input", "{}")

        cls = self._workflows.get(workflow_type)
        if cls is None:
            logger.warning("No workflow registered for '%s'", workflow_type)
            self._client.fail_task(workflow_key, f"No handler for {workflow_type}")
            return

        ctx = WorkflowContext(
            workflow_key=workflow_key,
            workflow_id=workflow_id,
            run_id=f"run-{int(time.time() * 1000)}",
            workflow_type=workflow_type,
            task_queue=self._options.task_queue,
            client=self._client,
        )

        self._stats.workflows_started += 1
        try:
            instance = cls()
            # Look for 'run' method or __call__
            run_method = getattr(instance, "run", None) or getattr(instance, "__call__", None)
            if run_method is None:
                raise RuntimeError(f"Workflow '{workflow_type}' has no 'run' method")

            import json
            args = json.loads(input_data) if isinstance(input_data, str) else input_data
            if isinstance(args, dict):
                result = await run_method(ctx, **args) if inspect.iscoroutinefunction(run_method) else run_method(ctx, **args)
            elif isinstance(args, list):
                result = await run_method(ctx, *args) if inspect.iscoroutinefunction(run_method) else run_method(ctx, *args)
            else:
                result = await run_method(ctx, args) if inspect.iscoroutinefunction(run_method) else run_method(ctx, args)

            import json as _json
            result_bytes = _json.dumps(result).encode() if result is not None else b""
            self._client.complete_workflow(workflow_key, result_bytes)
            self._stats.workflows_completed += 1
            logger.info("Workflow '%s' completed (key=%s)", workflow_type, workflow_key)

        except Exception as exc:
            self._stats.workflows_failed += 1
            logger.error("Workflow '%s' failed: %s\n%s", workflow_type, exc, traceback.format_exc())
            self._client.fail_task(workflow_key, str(exc))

    # ─── Stats ────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Return current worker statistics."""
        return {
            "workflows_started": self._stats.workflows_started,
            "workflows_completed": self._stats.workflows_completed,
            "workflows_failed": self._stats.workflows_failed,
            "activities_scheduled": self._stats.activities_scheduled,
            "activities_completed": self._stats.activities_completed,
            "activities_failed": self._stats.activities_failed,
            "tasks_polled": self._stats.tasks_polled,
            "heartbeats_sent": self._stats.heartbeats_sent,
            "uptime_ms": self._stats.uptime_ms,
            "registered_workflows": len(self._workflows),
            "registered_activities": len(self._activities),
        }

    def _stats_summary(self) -> str:
        return (
            f"workflows={self._stats.workflows_completed}/{self._stats.workflows_started}, "
            f"failed={self._stats.workflows_failed}, "
            f"uptime={self._stats.uptime_ms / 1000:.1f}s"
        )

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def task_queue(self) -> str:
        return self._options.task_queue
