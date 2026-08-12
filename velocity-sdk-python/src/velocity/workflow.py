"""Workflow registration and execution for V.E.L.O.C.I.T.Y.-WorkFlow Python SDK"""

from typing import Any, Callable, Dict, Optional
from .types import WorkflowContext

# Global workflow registry
_workflow_registry: Dict[str, Callable] = {}


def register_workflow(name: str, func: Callable) -> None:
    """Register a workflow function"""
    _workflow_registry[name] = func


def get_workflow(name: str) -> Optional[Callable]:
    """Get a registered workflow function"""
    return _workflow_registry.get(name)


def has_workflow(name: str) -> bool:
    """Check if a workflow is registered"""
    return name in _workflow_registry


def list_workflows() -> Dict[str, Callable]:
    """List all registered workflows"""
    return _workflow_registry.copy()


class Workflow:
    """Workflow definition and registration"""

    def __init__(self, name: str, func: Callable):
        self.name = name
        self.func = func

    def execute(self, context: WorkflowContext, input: Any) -> Any:
        """Execute the workflow"""
        return self.func(context, input)


class WorkflowHelpers:
    """Helper functions for use within workflows"""

    @staticmethod
    def execute_activity(
        context: WorkflowContext,
        activity_type: str,
        input: Any = None,
        task_queue: Optional[str] = None,
    ) -> Any:
        """Execute an activity from within a workflow"""
        # In a real implementation, this would schedule the activity
        return None

    @staticmethod
    def sleep(context: WorkflowContext, duration_ms: int) -> None:
        """Sleep for a specified duration"""
        # In a real implementation, this would create a timer
        pass

    @staticmethod
    def execute_child_workflow(
        context: WorkflowContext,
        workflow_type: str,
        input: Any = None,
        workflow_id: Optional[str] = None,
    ) -> Any:
        """Execute a child workflow from within a workflow"""
        # In a real implementation, this would start a child workflow
        return None

    @staticmethod
    def get_info(context: WorkflowContext) -> Dict[str, Any]:
        """Get workflow execution info"""
        return {
            "workflow_id": context.workflow_id,
            "run_id": context.run_id,
            "workflow_type": context.workflow_type,
            "task_queue": context.task_queue,
            "attempt": context.attempt,
        }
