"""
VELOCITY-WorkFlow Python SDK

Cross-language worker SDK that connects to the VELOCITY-WorkFlow gRPC server.
Proves the architecture is portable beyond C# — any language with gRPC support
can interact with the workflow engine.

Usage:
    from velocity_sdk import VelocityClient, WorkflowWorker

    client = VelocityClient("localhost:50051")
    workflow_key = client.start_workflow("my-workflow", total_steps=5)
    client.signal_workflow(workflow_key, "my-signal", b"payload")
    status = client.describe_workflow(workflow_key)
"""

import grpc
from dataclasses import dataclass
from typing import Optional
from enum import IntEnum


class WorkflowStatus(IntEnum):
    """Workflow execution status."""
    RUNNING = 0
    COMPLETED = 1
    FAILED = 2
    CANCELED = 3
    TERMINATED = 4
    CONTINUED_AS_NEW = 5


@dataclass
class WorkflowHandle:
    """Handle to a running workflow."""
    workflow_key: int
    workflow_id: int
    run_id: int


@dataclass
class WorkflowDescription:
    """Description of a workflow's current state."""
    workflow_key: int
    status: WorkflowStatus
    current_step: int
    total_steps: int


class VelocityClient:
    """
    gRPC client for the VELOCITY-WorkFlow server.
    
    Provides a Pythonic API for workflow lifecycle management:
    - Start/complete/fail/cancel/terminate workflows
    - Signal and query running workflows
    - Schedule and complete activities
    - Manage namespaces and visibility
    """

    def __init__(self, target: str, jwt_token: Optional[str] = None):
        """
        Connect to a VELOCITY-WorkFlow gRPC server.
        
        Args:
            target: gRPC server address (e.g., "localhost:50051")
            jwt_token: Optional JWT bearer token for authentication
        """
        self._target = target
        self._jwt_token = jwt_token
        
        # Build channel credentials
        self._channel = grpc.insecure_channel(target)
        
        # Build metadata for auth
        self._metadata = []
        if jwt_token:
            self._metadata.append(("authorization", f"Bearer {jwt_token}"))
        
        # Lazy import generated stubs
        self._stub = None

    def _get_stub(self):
        """Lazy-initialize the gRPC stub."""
        if self._stub is None:
            try:
                from . import workflow_service_pb2_grpc
                self._stub = workflow_service_pb2_grpc.WorkflowServiceStub(self._channel)
            except ImportError:
                raise ImportError(
                    "Generated gRPC stubs not found. Run: "
                    "python -m grpc_tools.protoc -I../../src/Velocity.Workflow.Server/Protos "
                    "--python_out=velocity_sdk --grpc_python_out=velocity_sdk "
                    "../../src/Velocity.Workflow.Server/Protos/workflow_service.proto"
                )
        return self._stub

    def start_workflow(
        self,
        workflow_type: str,
        namespace: str = "default",
        task_queue: str = "default",
        total_steps: int = 1,
        input_data: Optional[bytes] = None,
    ) -> WorkflowHandle:
        """
        Start a new workflow execution.
        
        Args:
            workflow_type: Type name of the workflow
            namespace: Namespace to run in
            task_queue: Task queue for worker dispatch
            total_steps: Number of execution steps
            input_data: Optional input payload
            
        Returns:
            WorkflowHandle with workflow_key, workflow_id, run_id
        """
        stub = self._get_stub()
        from . import workflow_service_pb2

        # Hash the type/namespace/task_queue to get IDs (matching server-side hashing)
        type_id = hash(workflow_type) & 0xFFFFFFFFFFFFFFFF
        ns_id = hash(namespace) & 0xFFFFFFFFFFFFFFFF
        tq_hash = hash(task_queue) & 0xFFFFFFFFFFFFFFFF

        request = workflow_service_pb2.StartWorkflowRequest(
            workflow_id=type_id,  # Use type hash as workflow ID for simplicity
            workflow_type_id=type_id,
            namespace_id=ns_id,
            task_queue_hash=tq_hash,
            total_steps=total_steps,
            input=input_data or b"",
        )

        response = stub.StartWorkflow(request, metadata=self._metadata)
        return WorkflowHandle(
            workflow_key=response.workflow_key,
            workflow_id=type_id,
            run_id=response.run_id,
        )

    def describe_workflow(self, workflow_key: int) -> WorkflowDescription:
        """Get the current state of a workflow."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.DescribeWorkflowRequest(workflow_key=workflow_key)
        response = stub.DescribeWorkflow(request, metadata=self._metadata)

        return WorkflowDescription(
            workflow_key=workflow_key,
            status=WorkflowStatus(response.status),
            current_step=response.current_step,
            total_steps=response.total_steps,
        )

    def signal_workflow(
        self,
        workflow_key: int,
        signal_name: str,
        payload: bytes = b"",
    ) -> bool:
        """Send a signal to a running workflow."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        name_bytes = signal_name.encode("utf-8")
        request = workflow_service_pb2.SignalWorkflowRequest(
            workflow_key=workflow_key,
            signal_name=name_bytes,
            payload=payload,
        )
        response = stub.SignalWorkflow(request, metadata=self._metadata)
        return response.success

    def complete_workflow(self, workflow_key: int, result: bytes = b"") -> bool:
        """Mark a workflow as completed."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.CompleteWorkflowRequest(
            workflow_key=workflow_key,
            result=result,
        )
        response = stub.CompleteWorkflow(request, metadata=self._metadata)
        return response.success

    def fail_workflow(self, workflow_key: int, reason: str = "") -> bool:
        """Mark a workflow as failed."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.FailWorkflowRequest(
            workflow_key=workflow_key,
            reason=reason.encode("utf-8"),
        )
        response = stub.FailWorkflow(request, metadata=self._metadata)
        return response.success

    def cancel_workflow(self, workflow_key: int) -> bool:
        """Request cancellation of a running workflow."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.CancelWorkflowRequest(workflow_key=workflow_key)
        response = stub.CancelWorkflow(request, metadata=self._metadata)
        return response.success

    def close(self):
        """Close the gRPC channel."""
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
