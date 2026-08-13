"""
Workflow Update API — synchronous workflow mutation.

Unlike signals (fire-and-forget), updates provide:
- Synchronous request/response semantics
- Wait policies (Accepted, Completed, Admitted)
- Validation before execution
- Named update handlers registered by workflows

Usage:
    from velocity_sdk.update import UpdateClient, UpdateWaitPolicy

    client = UpdateClient("localhost:7234")
    result = client.execute_update(
        workflow_key=42,
        update_name="setAmount",
        args={"amount": 100},
        wait_policy=UpdateWaitPolicy.COMPLETED,
    )
"""

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, Dict, List, Optional
import time
import threading


class UpdateStatus(IntEnum):
    """Status of a workflow update."""
    ADMITTED = 0
    ACCEPTED = 1
    COMPLETED = 2
    REJECTED = 3


class UpdateWaitPolicy(IntEnum):
    """How long to wait for an update to complete."""
    ADMITTED = 0
    ACCEPTED = 1
    COMPLETED = 2


@dataclass
class UpdateRequest:
    """Request to execute a workflow update."""
    workflow_key: int
    update_id: str
    update_name: str
    args: Any = None
    wait_policy: UpdateWaitPolicy = UpdateWaitPolicy.COMPLETED


@dataclass
class UpdateResult:
    """Result of a workflow update execution."""
    update_id: str
    status: UpdateStatus
    result: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class UpdateHandler:
    """Handler for a named update."""
    name: str
    handler: Callable[[Any], Any]
    validator: Optional[Callable[[Any], bool]] = None


class UpdateClient:
    """
    Client for executing workflow updates.

    Updates are synchronous mutations to workflow state, unlike signals
    which are fire-and-forget. Updates can be validated before execution
    and support wait policies for different completion levels.
    """

    def __init__(self, server_address: str = "localhost:7234"):
        self._server_address = server_address
        self._handlers: Dict[str, UpdateHandler] = {}
        self._pending: Dict[str, UpdateResult] = {}
        self._lock = threading.Lock()

    def register_handler(
        self,
        name: str,
        handler: Callable[[Any], Any],
        validator: Optional[Callable[[Any], bool]] = None,
    ) -> None:
        """Register a named update handler."""
        self._handlers[name] = UpdateHandler(name=name, handler=handler, validator=validator)

    def execute_update(
        self,
        workflow_key: int,
        update_name: str,
        args: Any = None,
        wait_policy: UpdateWaitPolicy = UpdateWaitPolicy.COMPLETED,
        update_id: Optional[str] = None,
    ) -> UpdateResult:
        """
        Execute a workflow update.

        Args:
            workflow_key: Target workflow key.
            update_name: Name of the registered update handler.
            args: Arguments to pass to the handler.
            wait_policy: How long to wait for completion.
            update_id: Optional update ID (auto-generated if not provided).

        Returns:
            UpdateResult with status and result/error.
        """
        uid = update_id or f"update-{workflow_key}-{int(time.time() * 1000)}"
        start = time.time()

        handler = self._handlers.get(update_name)
        if handler is None:
            result = UpdateResult(
                update_id=uid,
                status=UpdateStatus.REJECTED,
                error=f"No handler registered for update '{update_name}'",
                duration_ms=(time.time() - start) * 1000,
            )
            with self._lock:
                self._pending[uid] = result
            return result

        # Validate if validator exists
        if handler.validator and not handler.validator(args):
            result = UpdateResult(
                update_id=uid,
                status=UpdateStatus.REJECTED,
                error="Update validation failed",
                duration_ms=(time.time() - start) * 1000,
            )
            with self._lock:
                self._pending[uid] = result
            return result

        # Execute the handler
        try:
            value = handler.handler(args)
            result = UpdateResult(
                update_id=uid,
                status=UpdateStatus.COMPLETED,
                result=value,
                duration_ms=(time.time() - start) * 1000,
            )
        except Exception as e:
            result = UpdateResult(
                update_id=uid,
                status=UpdateStatus.REJECTED,
                error=str(e),
                duration_ms=(time.time() - start) * 1000,
            )

        with self._lock:
            self._pending[uid] = result
        return result

    def get_update_result(self, update_id: str) -> Optional[UpdateResult]:
        """Get the result of a previously executed update."""
        with self._lock:
            return self._pending.get(update_id)

    def list_handlers(self) -> List[str]:
        """List registered update handler names."""
        return list(self._handlers.keys())

    def list_pending(self) -> List[str]:
        """List pending update IDs."""
        with self._lock:
            return list(self._pending.keys())
