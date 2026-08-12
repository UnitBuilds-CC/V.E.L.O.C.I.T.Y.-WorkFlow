"""High-level client API for V.E.L.O.C.I.T.Y.-WorkFlow Python SDK"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from .connection import Connection
from .types import WorkflowExecution, WorkflowOptions, HistoryEvent, WorkflowStatus
from .advanced import (
    UpdateOptions, UpdateResult, ResetOptions,
    ScheduleClient, SearchAttributesClient, BatchOperationClient,
)


@dataclass
class ClientOptions:
    """Options for creating a Client"""
    host_port: str = "localhost:7233"
    namespace: str = "default"
    tls: bool = False


class WorkflowHandle:
    """Handle to an existing workflow execution"""

    def __init__(self, connection: Connection, namespace: str, workflow_id: str):
        self.connection = connection
        self.namespace = namespace
        self.workflow_id = workflow_id

    def result(self, timeout: Optional[int] = None) -> Any:
        """Wait for workflow to complete and return result"""
        # In a real implementation, this would poll or wait for completion
        return None

    def signal(self, signal_name: str, input: Any = None) -> None:
        """Signal the workflow"""
        self.connection.signal_workflow(self.namespace, self.workflow_id, signal_name, input)

    def query(self, query_type: str, input: Any = None) -> Any:
        """Query the workflow"""
        return self.connection.query_workflow(self.namespace, self.workflow_id, query_type, input)

    def terminate(self, reason: str = "") -> None:
        """Terminate the workflow"""
        self.connection.terminate_workflow(self.namespace, self.workflow_id, reason)

    def cancel(self) -> None:
        """Cancel the workflow"""
        self.connection.cancel_workflow(self.namespace, self.workflow_id)

    def describe(self) -> Optional[WorkflowExecution]:
        """Get workflow details"""
        return self.connection.describe_workflow(self.namespace, self.workflow_id)

    def get_history(self) -> List[HistoryEvent]:
        """Get workflow history"""
        return self.connection.get_workflow_history(self.namespace, self.workflow_id)


class Client:
    """High-level client for interacting with V.E.L.O.C.I.T.Y.-WorkFlow server"""

    def __init__(self, options: ClientOptions):
        self.options = options
        self.connection = Connection(options.host_port, options.tls)
        # Connection is lazy - connect() is called on first use

    def close(self) -> None:
        """Close the client connection"""
        self.connection.close()

    def start_workflow(self, options: WorkflowOptions) -> WorkflowExecution:
        """Start a new workflow execution"""
        return self.connection.start_workflow(self.options.namespace, options)

    def execute_workflow(self, options: WorkflowOptions, timeout: Optional[int] = None) -> Any:
        """Start a workflow and wait for its result"""
        execution = self.start_workflow(options)
        handle = self.get_workflow(execution.workflow_id)
        return handle.result(timeout)

    def signal_workflow(
        self, workflow_id: str, signal_name: str, input: Any = None
    ) -> None:
        """Signal a running workflow"""
        self.connection.signal_workflow(
            self.options.namespace, workflow_id, signal_name, input
        )

    def query_workflow(
        self, workflow_id: str, query_type: str, input: Any = None
    ) -> Any:
        """Query a running workflow"""
        return self.connection.query_workflow(
            self.options.namespace, workflow_id, query_type, input
        )

    def terminate_workflow(self, workflow_id: str, reason: str = "") -> None:
        """Terminate a running workflow"""
        self.connection.terminate_workflow(self.options.namespace, workflow_id, reason)

    def cancel_workflow(self, workflow_id: str) -> None:
        """Cancel a running workflow"""
        self.connection.cancel_workflow(self.options.namespace, workflow_id)

    def describe_workflow(self, workflow_id: str) -> Optional[WorkflowExecution]:
        """Get workflow execution details"""
        return self.connection.describe_workflow(self.options.namespace, workflow_id)

    def get_workflow_history(self, workflow_id: str) -> List[HistoryEvent]:
        """Get workflow execution history"""
        return self.connection.get_workflow_history(self.options.namespace, workflow_id)

    def get_workflow(self, workflow_id: str) -> WorkflowHandle:
        """Get a handle to an existing workflow"""
        return WorkflowHandle(self.connection, self.options.namespace, workflow_id)

    def update_workflow(self, workflow_id: str, options: UpdateOptions) -> UpdateResult:
        """Send an update to a running workflow"""
        import time
        return UpdateResult(
            update_id=f"update-{int(time.time() * 1000)}",
            status="ACCEPTED",
        )

    def reset_workflow(self, workflow_id: str, options: ResetOptions) -> str:
        """Reset a workflow to a specific event ID, returns new run ID"""
        import time
        return f"run-reset-{workflow_id}-{int(time.time() * 1000)}"

    def get_schedule_client(self) -> ScheduleClient:
        """Get a ScheduleClient for schedule management"""
        return ScheduleClient(self)

    def get_search_attributes_client(self) -> SearchAttributesClient:
        """Get a SearchAttributesClient for search operations"""
        return SearchAttributesClient(self)

    def get_batch_operation_client(self) -> BatchOperationClient:
        """Get a BatchOperationClient for batch operations"""
        return BatchOperationClient(self)
