"""
VELOCITY-WorkFlow Python SDK - Typed workflow stub.

Provides a high-level interface for workflow execution with type safety.
"""

from typing import Any, Optional, TypeVar, Generic
from dataclasses import dataclass

from .client import VelocityClient, WorkflowHandle
from .payload_codec import PayloadCodec, JsonCodec


T = TypeVar("T")


@dataclass
class WorkflowStubOptions:
    """Configuration for WorkflowStub."""

    workflow_type: str
    namespace: str = "default"
    task_queue: str = "default"
    execution_timeout: float = 60.0
    codec: Optional[PayloadCodec] = None


class WorkflowStub(Generic[T]):
    """
    Typed workflow execution stub.

    Provides a convenient interface for starting, signaling, querying,
    and waiting for workflow results.
    """

    def __init__(
        self,
        client: VelocityClient,
        options: WorkflowStubOptions,
    ):
        self.client = client
        self.options = options
        self.codec = options.codec or JsonCodec()
        self._handle: Optional[WorkflowHandle] = None

    def start(self, input_data: Any = None, **kwargs: Any) -> WorkflowHandle:
        """
        Start workflow execution.

        Args:
            input_data: Input data for the workflow (will be encoded)
            **kwargs: Additional arguments passed to start_workflow

        Returns:
            WorkflowHandle for the started workflow
        """
        payload = self.codec.encode(input_data) if input_data is not None else b""

        self._handle = self.client.start_workflow(
            self.options.workflow_type,
            namespace=self.options.namespace,
            task_queue=self.options.task_queue,
            input_data=payload,
            **kwargs,
        )

        return self._handle

    def signal(self, signal_name: str, data: Any = None) -> None:
        """
        Send a signal to the workflow.

        Args:
            signal_name: Name of the signal
            data: Signal payload (will be encoded)
        """
        if self._handle is None:
            raise RuntimeError("Workflow not started. Call start() first.")

        payload = self.codec.encode(data) if data is not None else b""
        self.client.signal_workflow(self._handle.workflow_key, signal_name, payload)

    def query(self, query_type: str, args: Any = None) -> Any:
        """
        Query the workflow state.

        Args:
            query_type: Type of query
            args: Query arguments (will be encoded)

        Returns:
            Query result (decoded)
        """
        if self._handle is None:
            raise RuntimeError("Workflow not started. Call start() first.")

        payload = self.codec.encode(args) if args is not None else b""
        result = self.client.query_workflow(
            self._handle.workflow_key,
            query_type,
            payload,
        )

        return self.codec.decode(result) if result else None

    def result(self, timeout: Optional[float] = None) -> T:
        """
        Wait for workflow completion and return the result.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Workflow result (decoded)
        """
        if self._handle is None:
            raise RuntimeError("Workflow not started. Call start() first.")

        result_data = self.client.wait_for_completion(
            self._handle.workflow_key,
            timeout=timeout,
        )

        return self.codec.decode(result_data) if result_data else None  # type: ignore[return-value]

    def cancel(self) -> None:
        """Cancel the workflow."""
        if self._handle is None:
            raise RuntimeError("Workflow not started. Call start() first.")

        self.client.cancel_workflow(self._handle.workflow_key)

    def terminate(self, reason: str = "") -> None:
        """
        Terminate the workflow.

        Args:
            reason: Termination reason
        """
        if self._handle is None:
            raise RuntimeError("Workflow not started. Call start() first.")

        self.client.terminate_workflow(self._handle.workflow_key, reason=reason)

    @property
    def handle(self) -> Optional[WorkflowHandle]:
        """Get the underlying workflow handle."""
        return self._handle

    @property
    def workflow_key(self) -> Optional[int]:
        """Get the workflow key."""
        return self._handle.workflow_key if self._handle else None
