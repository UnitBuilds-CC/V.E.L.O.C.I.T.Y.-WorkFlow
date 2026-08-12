"""Activity registration and execution for V.E.L.O.C.I.T.Y.-WorkFlow Python SDK"""

from typing import Any, Callable, Dict, Optional
from .types import ActivityContext

# Global activity registry
_activity_registry: Dict[str, Callable] = {}


def register_activity(name: str, func: Callable) -> None:
    """Register an activity function"""
    _activity_registry[name] = func


def get_activity(name: str) -> Optional[Callable]:
    """Get a registered activity function"""
    return _activity_registry.get(name)


def has_activity(name: str) -> bool:
    """Check if an activity is registered"""
    return name in _activity_registry


def list_activities() -> Dict[str, Callable]:
    """List all registered activities"""
    return _activity_registry.copy()


class Activity:
    """Activity definition and registration"""

    def __init__(self, name: str, func: Callable):
        self.name = name
        self.func = func

    def execute(self, context: ActivityContext, input: Any) -> Any:
        """Execute the activity"""
        return self.func(context, input)


class ActivityHelpers:
    """Helper functions for use within activities"""

    @staticmethod
    def heartbeat(context: ActivityContext, details: Any = None) -> None:
        """Record activity heartbeat"""
        # In a real implementation, this would send heartbeat to server
        pass

    @staticmethod
    def get_info(context: ActivityContext) -> Dict[str, Any]:
        """Get activity execution info"""
        return {
            "activity_id": context.activity_id,
            "activity_type": context.activity_type,
            "task_queue": context.task_queue,
            "workflow_id": context.workflow_id,
            "run_id": context.run_id,
            "attempt": context.attempt,
        }
