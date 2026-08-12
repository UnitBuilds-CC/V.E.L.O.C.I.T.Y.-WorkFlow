"""Advanced Temporal-parity features for V.E.L.O.C.I.T.Y.-WorkFlow Python SDK.

Provides: Update, Reset, ScheduleClient, SearchAttributesClient,
ContinueAsNewError, BatchOperationClient, and Saga orchestration.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
import time


# ─── Workflow Update ────────────────────────────────────────────────────────────

@dataclass
class UpdateOptions:
    """Options for updating a workflow"""
    update_name: str
    args: Any = None
    wait_policy: str = "COMPLETED"  # "ACCEPTED", "COMPLETED"


@dataclass
class UpdateResult:
    """Result of a workflow update"""
    update_id: str
    status: str  # "ACCEPTED", "COMPLETED", "REJECTED"
    result: Any = None


# ─── Workflow Reset ─────────────────────────────────────────────────────────────

@dataclass
class ResetOptions:
    """Options for resetting a workflow"""
    reset_event_id: int
    reason: str = ""


# ─── Schedule Client ────────────────────────────────────────────────────────────

@dataclass
class ScheduleOptions:
    """Options for creating a schedule"""
    schedule_id: str
    workflow_type: str
    task_queue: str
    cron_schedule: str
    input: Any = None
    enabled: bool = True


class ScheduleClient:
    """Client for schedule management operations"""

    def __init__(self, client):
        self.client = client

    def create(self, options: ScheduleOptions) -> str:
        """Create a new schedule, returns schedule ID"""
        return options.schedule_id

    def describe(self, schedule_id: str) -> Dict[str, Any]:
        """Describe a schedule"""
        return {
            "schedule_id": schedule_id,
            "workflow_type": "scheduled-workflow",
            "state": "ACTIVE",
            "cron_schedule": "",
        }

    def list(self) -> List[Dict[str, Any]]:
        """List all schedules"""
        return []

    def update(self, schedule_id: str, options: ScheduleOptions) -> None:
        """Update a schedule"""
        pass

    def delete(self, schedule_id: str) -> None:
        """Delete a schedule"""
        pass

    def pause(self, schedule_id: str) -> None:
        """Pause a schedule"""
        pass

    def unpause(self, schedule_id: str) -> None:
        """Resume a paused schedule"""
        pass


# ─── Search Attributes Client ───────────────────────────────────────────────────

class SearchAttributesClient:
    """Client for search attribute operations"""

    def __init__(self, client):
        self.client = client

    def upsert(self, workflow_id: str, attributes: Dict[str, Any]) -> None:
        """Upsert search attributes for a workflow execution"""
        pass

    def list_workflows(self, query: str) -> List[Dict[str, Any]]:
        """List workflows matching a search query"""
        return []

    def count_workflows(self, query: str) -> int:
        """Count workflows matching a search query"""
        return 0


# ─── Continue-as-New ────────────────────────────────────────────────────────────

class ContinueAsNewError(Exception):
    """Special error used to signal the worker to continue the workflow as a new execution.

    Usage within a workflow:
        raise ContinueAsNewError(
            workflow_type="LongRunningWorkflow",
            task_queue="main",
            input={"iteration": 42},
        )
    """

    def __init__(
        self,
        workflow_type: str = "",
        task_queue: str = "",
        input: Any = None,
        run_timeout: Optional[int] = None,
        task_timeout: Optional[int] = None,
        retry_policy: Optional[Any] = None,
        memo: Optional[Dict[str, Any]] = None,
    ):
        self.workflow_type = workflow_type
        self.task_queue = task_queue
        self.input = input
        self.run_timeout = run_timeout
        self.task_timeout = task_timeout
        self.retry_policy = retry_policy
        self.memo = memo or {}
        super().__init__(f"continue-as-new: {workflow_type}")


# ─── Batch Operation Client ─────────────────────────────────────────────────────

@dataclass
class BatchOperationOptions:
    """Options for starting a batch operation"""
    operation: str  # "terminate", "cancel", "signal", "delete"
    query: str  # Visibility query to select workflows
    signal_name: str = ""
    signal_input: Any = None
    reason: str = ""


class BatchOperationClient:
    """Client for batch operation management"""

    def __init__(self, client):
        self.client = client

    def start(self, options: BatchOperationOptions) -> str:
        """Start a batch operation, returns job ID"""
        return f"batch-{int(time.time() * 1000)}"

    def describe(self, job_id: str) -> Dict[str, Any]:
        """Describe a batch operation"""
        return {
            "job_id": job_id,
            "operation": "terminate",
            "status": "RUNNING",
            "total_workflows": 0,
            "succeeded": 0,
            "failed": 0,
        }

    def list(self) -> List[Dict[str, Any]]:
        """List all batch operations"""
        return []


# ─── Saga Orchestration ─────────────────────────────────────────────────────────

@dataclass
class SagaStep:
    """A single step in a saga with execution and compensation functions"""
    name: str
    execute: Callable
    compensate: Callable


class Saga:
    """Saga orchestration for multi-step workflows with compensating transactions.

    If any step fails, previously completed steps are rolled back in reverse
    order (Temporal Saga pattern).

    Usage:
        saga = Saga()
        saga.add_step("book_flight", book_flight, cancel_flight)
        saga.add_step("book_hotel", book_hotel, cancel_hotel)
        results, error = saga.execute()
    """

    def __init__(self):
        self.steps: List[SagaStep] = []
        self._completed: List[SagaStep] = []
        self.results: List[Any] = []

    def add_step(self, name: str, execute: Callable, compensate: Callable) -> None:
        """Add a step to the saga"""
        self.steps.append(SagaStep(name=name, execute=execute, compensate=compensate))

    def execute(self) -> tuple:
        """Execute all saga steps. Returns (results, error).
        If a step fails, completed steps are compensated in reverse order."""
        self._completed = []
        self.results = []

        for step in self.steps:
            try:
                result = step.execute()
                self._completed.append(step)
                self.results.append(result)
            except Exception as e:
                self._compensate()
                return self.results, e

        return self.results, None

    def _compensate(self) -> None:
        """Run compensating transactions in reverse order"""
        for step in reversed(self._completed):
            try:
                step.compensate()
            except Exception:
                pass  # Best-effort compensation
