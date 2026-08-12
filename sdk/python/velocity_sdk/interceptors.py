"""
VELOCITY-WorkFlow Python SDK - Interceptor framework.

Provides middleware pattern for workflow and activity lifecycle hooks.
Interceptors can be chained to compose logging, metrics, tracing, and custom logic.
"""

from typing import Any, Callable, List, Optional
from abc import ABC, abstractmethod
import time
import logging


class WorkflowInterceptor(ABC):
    """Base class for workflow interceptors."""

    def on_start(self, workflow_type: str, workflow_id: int, **kwargs) -> None:
        """Called before workflow starts."""
        pass

    def on_complete(self, workflow_id: int, result: Any, **kwargs) -> None:
        """Called after workflow completes successfully."""
        pass

    def on_fail(self, workflow_id: int, error: Exception, **kwargs) -> None:
        """Called when workflow fails."""
        pass

    def on_signal(self, workflow_id: int, signal_name: str, **kwargs) -> None:
        """Called when workflow receives a signal."""
        pass


class ActivityInterceptor(ABC):
    """Base class for activity interceptors."""

    def on_execute(self, activity_type: str, activity_id: str, **kwargs) -> None:
        """Called before activity executes."""
        pass

    def on_complete(self, activity_id: str, result: Any, **kwargs) -> None:
        """Called after activity completes."""
        pass

    def on_fail(self, activity_id: str, error: Exception, **kwargs) -> None:
        """Called when activity fails."""
        pass


class LoggingInterceptor(WorkflowInterceptor, ActivityInterceptor):
    """Logs workflow and activity lifecycle events."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("velocity_sdk")

    def on_start(self, workflow_type: str, workflow_id: int, **kwargs) -> None:
        self.logger.info(f"Workflow started: type={workflow_type}, id={workflow_id}")

    def on_complete(self, workflow_id: int, result: Any, **kwargs) -> None:
        self.logger.info(f"Workflow completed: id={workflow_id}")

    def on_fail(self, workflow_id: int, error: Exception, **kwargs) -> None:
        self.logger.error(f"Workflow failed: id={workflow_id}, error={error}")

    def on_signal(self, workflow_id: int, signal_name: str, **kwargs) -> None:
        self.logger.info(f"Workflow signal: id={workflow_id}, signal={signal_name}")

    def on_execute(self, activity_type: str, activity_id: str, **kwargs) -> None:
        self.logger.info(f"Activity executing: type={activity_type}, id={activity_id}")

    def on_complete(self, activity_id: str, result: Any, **kwargs) -> None:
        self.logger.info(f"Activity completed: id={activity_id}")

    def on_fail(self, activity_id: str, error: Exception, **kwargs) -> None:
        self.logger.error(f"Activity failed: id={activity_id}, error={error}")


class MetricsInterceptor(WorkflowInterceptor, ActivityInterceptor):
    """Tracks workflow and activity metrics."""

    def __init__(self):
        self.workflow_starts = 0
        self.workflow_completions = 0
        self.workflow_failures = 0
        self.activity_executions = 0
        self.activity_completions = 0
        self.activity_failures = 0
        self._start_times = {}

    def on_start(self, workflow_type: str, workflow_id: int, **kwargs) -> None:
        self.workflow_starts += 1
        self._start_times[workflow_id] = time.time()

    def on_complete(self, workflow_id: int, result: Any, **kwargs) -> None:
        self.workflow_completions += 1
        self._start_times.pop(workflow_id, None)

    def on_fail(self, workflow_id: int, error: Exception, **kwargs) -> None:
        self.workflow_failures += 1
        self._start_times.pop(workflow_id, None)

    def on_execute(self, activity_type: str, activity_id: str, **kwargs) -> None:
        self.activity_executions += 1

    def on_complete(self, activity_id: str, result: Any, **kwargs) -> None:
        self.activity_completions += 1

    def on_fail(self, activity_id: str, error: Exception, **kwargs) -> None:
        self.activity_failures += 1

    def get_metrics(self) -> dict:
        """Return current metrics snapshot."""
        return {
            "workflow_starts": self.workflow_starts,
            "workflow_completions": self.workflow_completions,
            "workflow_failures": self.workflow_failures,
            "activity_executions": self.activity_executions,
            "activity_completions": self.activity_completions,
            "activity_failures": self.activity_failures,
        }


class TracingInterceptor(WorkflowInterceptor, ActivityInterceptor):
    """Collects tracing spans for workflows and activities."""

    def __init__(self):
        self.spans = []

    def on_start(self, workflow_type: str, workflow_id: int, **kwargs) -> None:
        self.spans.append({
            "type": "workflow_start",
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "timestamp": time.time(),
        })

    def on_complete(self, workflow_id: int, result: Any, **kwargs) -> None:
        self.spans.append({
            "type": "workflow_complete",
            "workflow_id": workflow_id,
            "timestamp": time.time(),
        })

    def on_fail(self, workflow_id: int, error: Exception, **kwargs) -> None:
        self.spans.append({
            "type": "workflow_fail",
            "workflow_id": workflow_id,
            "error": str(error),
            "timestamp": time.time(),
        })

    def on_execute(self, activity_type: str, activity_id: str, **kwargs) -> None:
        self.spans.append({
            "type": "activity_execute",
            "activity_id": activity_id,
            "activity_type": activity_type,
            "timestamp": time.time(),
        })


class InterceptorChain:
    """Chain of interceptors that are invoked in order."""

    def __init__(self, interceptors: Optional[List[Any]] = None):
        self.interceptors = interceptors or []

    def add(self, interceptor: Any) -> None:
        """Add an interceptor to the chain."""
        self.interceptors.append(interceptor)

    def invoke_workflow_start(self, workflow_type: str, workflow_id: int, **kwargs) -> None:
        """Invoke all workflow interceptors for start event."""
        for interceptor in self.interceptors:
            if isinstance(interceptor, WorkflowInterceptor):
                interceptor.on_start(workflow_type, workflow_id, **kwargs)

    def invoke_workflow_complete(self, workflow_id: int, result: Any, **kwargs) -> None:
        """Invoke all workflow interceptors for complete event."""
        for interceptor in self.interceptors:
            if isinstance(interceptor, WorkflowInterceptor):
                interceptor.on_complete(workflow_id, result, **kwargs)

    def invoke_workflow_fail(self, workflow_id: int, error: Exception, **kwargs) -> None:
        """Invoke all workflow interceptors for fail event."""
        for interceptor in self.interceptors:
            if isinstance(interceptor, WorkflowInterceptor):
                interceptor.on_fail(workflow_id, error, **kwargs)

    def invoke_activity_execute(self, activity_type: str, activity_id: str, **kwargs) -> None:
        """Invoke all activity interceptors for execute event."""
        for interceptor in self.interceptors:
            if isinstance(interceptor, ActivityInterceptor):
                interceptor.on_execute(activity_type, activity_id, **kwargs)
