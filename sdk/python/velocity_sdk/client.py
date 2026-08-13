"""
VELOCITY-WorkFlow Python SDK

Cross-language worker SDK that connects to the VELOCITY-WorkFlow gRPC server.
Proves the architecture is portable beyond C# — any language with gRPC support
can interact with the workflow engine.

Usage:
    from velocity_sdk import VelocityClient, WorkflowWorker

    client = VelocityClient("localhost:7234")
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
            target: gRPC server address (e.g., "localhost:7234")
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

    def signal_with_start(
        self,
        workflow_type: str,
        signal_name: str,
        signal_payload: bytes = b"",
        task_queue: str = "default",
        input_data: bytes = b"",
        total_steps: int = 1,
    ) -> WorkflowHandle:
        """Signal an existing workflow or start a new one and signal it atomically."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.SignalWithStartRequest(
            namespace="default",
            workflow_type=workflow_type.encode("utf-8"),
            task_queue=task_queue.encode("utf-8"),
            input=input_data,
            signal_name=signal_name.encode("utf-8"),
            signal_payload=signal_payload,
            total_steps=total_steps,
        )
        response = stub.SignalWithStartWorkflow(request, metadata=self._metadata)
        return WorkflowHandle(
            key=response.workflow_key,
            workflow_id=response.workflow_id,
            status=WorkflowStatus.RUNNING,
            workflow_type=workflow_type,
            task_queue=task_queue,
            total_steps=total_steps,
        )

    def search_workflows(self, query: str) -> list:
        """Search workflows using a SQL-like visibility query."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.SearchWorkflowsRequest(
            namespace="default",
            query=query,
        )
        response = stub.SearchWorkflows(request, metadata=self._metadata)
        return [
            WorkflowDescription(
                key=wf.workflow_key,
                workflow_id=wf.workflow_id,
                status=WorkflowStatus(wf.status),
                workflow_type=wf.workflow_type,
                task_queue=wf.task_queue,
                total_steps=wf.total_steps,
            )
            for wf in response.workflows
        ]

    def list_workflows(self, page_size: int = 100) -> list:
        """List all workflows."""
        return self.search_workflows("")

    def reset_workflow(self, workflow_key: int, event_id: int = 0) -> bool:
        """Reset a workflow to a previous event for replay."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.ResetWorkflowRequest(
            workflow_key=workflow_key,
            event_id=event_id,
        )
        response = stub.ResetWorkflow(request, metadata=self._metadata)
        return response.success

    def update_workflow(
        self,
        workflow_key: int,
        update_name: str,
        input_data: bytes = b"",
    ) -> bytes:
        """Send a synchronous update to a running workflow and wait for the result."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.UpdateWorkflowRequest(
            workflow_key=workflow_key,
            update_name=update_name.encode("utf-8"),
            input=input_data,
        )
        response = stub.UpdateWorkflow(request, metadata=self._metadata)
        return response.result

    def continue_as_new(
        self,
        workflow_key: int,
        new_workflow_type: str = "",
        new_task_queue: str = "",
        new_input: bytes = b"",
    ) -> int:
        """Continue a workflow as a new execution with new input."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.ContinueAsNewRequest(
            workflow_key=workflow_key,
            new_workflow_type=new_workflow_type.encode("utf-8"),
            new_task_queue=new_task_queue.encode("utf-8"),
            new_input=new_input,
        )
        response = stub.ContinueAsNew(request, metadata=self._metadata)
        return response.new_workflow_key

    def set_memo(self, workflow_key: int, memo: dict) -> bool:
        """Set memo key-value pairs on a workflow."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.SetMemoRequest(
            workflow_key=workflow_key,
            memo={k: v if isinstance(v, bytes) else str(v).encode("utf-8") for k, v in memo.items()},
        )
        response = stub.SetMemo(request, metadata=self._metadata)
        return response.success

    def get_memo(self, workflow_key: int) -> dict:
        """Get all memo key-value pairs for a workflow."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.GetMemoRequest(workflow_key=workflow_key)
        response = stub.GetMemo(request, metadata=self._metadata)
        return dict(response.memo)

    def set_search_attributes(self, workflow_key: int, attributes: dict) -> bool:
        """Set search attributes on a workflow for visibility queries."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.SetSearchAttributesRequest(
            workflow_key=workflow_key,
            attributes={k: v if isinstance(v, bytes) else str(v).encode("utf-8") for k, v in attributes.items()},
        )
        response = stub.SetSearchAttributes(request, metadata=self._metadata)
        return response.success

    def get_search_attributes(self, workflow_key: int) -> dict:
        """Get all search attributes for a workflow."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.GetSearchAttributesRequest(workflow_key=workflow_key)
        response = stub.GetSearchAttributes(request, metadata=self._metadata)
        return dict(response.attributes)

    def create_schedule(
        self,
        schedule_id: str,
        cron_expression: str,
        workflow_type: str,
        input_data: bytes = b"",
        task_queue: str = "default",
    ) -> bool:
        """Create a recurring schedule for a workflow type."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.CreateScheduleRequest(
            namespace="default",
            schedule_id=schedule_id,
            cron_expression=cron_expression,
            workflow_type=workflow_type.encode("utf-8"),
            input=input_data,
            task_queue=task_queue.encode("utf-8"),
        )
        response = stub.CreateSchedule(request, metadata=self._metadata)
        return response.success

    def describe_schedule(self, schedule_id: str) -> dict:
        """Get information about a schedule."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.DescribeScheduleRequest(
            namespace="default",
            schedule_id=schedule_id,
        )
        response = stub.DescribeSchedule(request, metadata=self._metadata)
        return {
            "schedule_id": schedule_id,
            "cron": response.cron_expression,
            "workflow_type": response.workflow_type,
        }

    def list_schedules(self) -> list:
        """List all schedules in the namespace."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.ListSchedulesRequest(namespace="default")
        response = stub.ListSchedules(request, metadata=self._metadata)
        return list(response.schedules)

    def delete_schedule(self, schedule_id: str) -> bool:
        """Delete a schedule."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.DeleteScheduleRequest(
            namespace="default",
            schedule_id=schedule_id,
        )
        response = stub.DeleteSchedule(request, metadata=self._metadata)
        return response.success

    def batch_terminate(self, workflow_keys: list, reason: str = "") -> str:
        """Terminate multiple workflows in a single batch operation. Returns job ID."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.StartBatchOperationRequest(
            namespace="default",
            operation="terminate",
            workflow_keys=workflow_keys,
            reason=reason,
        )
        response = stub.StartBatchOperation(request, metadata=self._metadata)
        return response.job_id

    def batch_cancel(self, workflow_keys: list) -> str:
        """Cancel multiple workflows in a single batch operation. Returns job ID."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.StartBatchOperationRequest(
            namespace="default",
            operation="cancel",
            workflow_keys=workflow_keys,
        )
        response = stub.StartBatchOperation(request, metadata=self._metadata)
        return response.job_id

    def batch_signal(self, workflow_keys: list, signal_name: str, payload: bytes = b"") -> str:
        """Signal multiple workflows in a single batch operation. Returns job ID."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.StartBatchOperationRequest(
            namespace="default",
            operation="signal",
            workflow_keys=workflow_keys,
            signal_name=signal_name.encode("utf-8"),
            signal_payload=payload,
        )
        response = stub.StartBatchOperation(request, metadata=self._metadata)
        return response.job_id

    def describe_batch_operation(self, job_id: str) -> dict:
        """Get the status of a batch operation."""
        stub = self._get_stub()
        from . import workflow_service_pb2

        request = workflow_service_pb2.DescribeBatchOperationRequest(
            namespace="default",
            job_id=job_id,
        )
        response = stub.DescribeBatchOperation(request, metadata=self._metadata)
        return {
            "job_id": job_id,
            "operation": response.operation,
            "status": response.status,
            "total": response.total_workflows,
            "succeeded": response.succeeded,
            "failed": response.failed,
        }

    def close(self):
        """Close the gRPC channel."""
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
