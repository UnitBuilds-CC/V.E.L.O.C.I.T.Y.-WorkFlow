"""
VELOCITY-WorkFlow Python SDK - Testing utilities.

Provides test environment and mock client for unit testing workflows
without requiring a running VELOCITY-WorkFlow server.
"""

from typing import Any, Dict, List, Optional
from .client import VelocityClient, WorkflowHandle, WorkflowDescription, WorkflowStatus
from .exceptions import WorkflowNotFoundError, WorkflowAlreadyCompletedError


class MockVelocityClient:
    """Mock client for testing workflows without a server."""

    def __init__(self):
        self._workflows: Dict[int, Dict[str, Any]] = {}
        self._signals: Dict[int, List[Dict[str, Any]]] = {}
        self._next_key = 1

    def start_workflow(
        self,
        workflow_type: str,
        namespace: str = "default",
        task_queue: str = "default",
        total_steps: int = 1,
        input_data: Optional[bytes] = None,
    ) -> WorkflowHandle:
        """Start a mock workflow."""
        key = self._next_key
        self._next_key += 1

        self._workflows[key] = {
            "workflow_type": workflow_type,
            "namespace": namespace,
            "task_queue": task_queue,
            "total_steps": total_steps,
            "current_step": 0,
            "status": WorkflowStatus.RUNNING,
            "result": None,
        }
        self._signals[key] = []

        return WorkflowHandle(workflow_key=key, workflow_id=key, run_id=key + 1000)

    def describe_workflow(self, workflow_key: int) -> WorkflowDescription:
        """Describe a mock workflow."""
        if workflow_key not in self._workflows:
            raise WorkflowNotFoundError(workflow_key)

        wf = self._workflows[workflow_key]
        return WorkflowDescription(
            workflow_key=workflow_key,
            status=wf["status"],
            current_step=wf["current_step"],
            total_steps=wf["total_steps"],
        )

    def signal_workflow(
        self,
        workflow_key: int,
        signal_name: str,
        payload: bytes = b"",
    ) -> bool:
        """Send a signal to a mock workflow."""
        if workflow_key not in self._workflows:
            raise WorkflowNotFoundError(workflow_key)

        self._signals[workflow_key].append({
            "signal_name": signal_name,
            "payload": payload,
        })
        return True

    def complete_workflow(self, workflow_key: int, result: bytes = b"") -> bool:
        """Complete a mock workflow."""
        if workflow_key not in self._workflows:
            raise WorkflowNotFoundError(workflow_key)

        wf = self._workflows[workflow_key]
        if wf["status"] != WorkflowStatus.RUNNING:
            raise WorkflowAlreadyCompletedError(workflow_key)

        wf["status"] = WorkflowStatus.COMPLETED
        wf["result"] = result
        return True

    def fail_workflow(self, workflow_key: int, reason: str = "") -> bool:
        """Fail a mock workflow."""
        if workflow_key not in self._workflows:
            raise WorkflowNotFoundError(workflow_key)

        wf = self._workflows[workflow_key]
        if wf["status"] != WorkflowStatus.RUNNING:
            raise WorkflowAlreadyCompletedError(workflow_key)

        wf["status"] = WorkflowStatus.FAILED
        return True

    def cancel_workflow(self, workflow_key: int) -> bool:
        """Cancel a mock workflow."""
        if workflow_key not in self._workflows:
            raise WorkflowNotFoundError(workflow_key)

        self._workflows[workflow_key]["status"] = WorkflowStatus.CANCELED
        return True

    def get_signals(self, workflow_key: int) -> List[Dict[str, Any]]:
        """Get all signals received by a workflow."""
        return self._signals.get(workflow_key, [])

    def close(self):
        """No-op for mock client."""
        pass


class WorkflowTestEnvironment:
    """Test environment for running workflows in isolation."""

    def __init__(self):
        self.client = MockVelocityClient()
        self._time_offset = 0

    def start_workflow(
        self,
        workflow_type: str,
        **kwargs,
    ) -> WorkflowHandle:
        """Start a workflow in the test environment."""
        return self.client.start_workflow(workflow_type, **kwargs)

    def complete_workflow(self, workflow_key: int, result: bytes = b"") -> bool:
        """Complete a workflow in the test environment."""
        return self.client.complete_workflow(workflow_key, result)

    def signal_workflow(
        self,
        workflow_key: int,
        signal_name: str,
        payload: bytes = b"",
    ) -> bool:
        """Signal a workflow in the test environment."""
        return self.client.signal_workflow(workflow_key, signal_name, payload)

    def time_skip(self, seconds: int) -> None:
        """Advance the test environment's clock."""
        self._time_offset += seconds

    def get_current_time(self) -> int:
        """Get the current test time (real time + offset)."""
        import time
        return int(time.time()) + self._time_offset

    def assert_workflow_completed(self, workflow_key: int) -> None:
        """Assert that a workflow has completed."""
        desc = self.client.describe_workflow(workflow_key)
        assert desc.status == WorkflowStatus.COMPLETED, (
            f"Expected workflow {workflow_key} to be completed, but status is {desc.status}"
        )

    def assert_signal_received(self, workflow_key: int, signal_name: str) -> None:
        """Assert that a workflow received a specific signal."""
        signals = self.client.get_signals(workflow_key)
        signal_names = [s["signal_name"] for s in signals]
        assert signal_name in signal_names, (
            f"Expected signal '{signal_name}' not found. Received: {signal_names}"
        )

    def reset(self) -> None:
        """Reset the test environment."""
        self.client = MockVelocityClient()
        self._time_offset = 0


def assert_workflow_completed(client: MockVelocityClient, workflow_key: int) -> None:
    """Assert that a workflow has completed."""
    desc = client.describe_workflow(workflow_key)
    assert desc.status == WorkflowStatus.COMPLETED, (
        f"Expected workflow {workflow_key} to be completed, but status is {desc.status}"
    )


def assert_signal_received(
    client: MockVelocityClient,
    workflow_key: int,
    signal_name: str,
) -> None:
    """Assert that a workflow received a specific signal."""
    signals = client.get_signals(workflow_key)
    signal_names = [s["signal_name"] for s in signals]
    assert signal_name in signal_names, (
        f"Expected signal '{signal_name}' not found. Received: {signal_names}"
    )
