"""Type definitions for V.E.L.O.C.I.T.Y.-WorkFlow Python SDK"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum
import time


class WorkflowStatus(Enum):
    """Workflow execution status"""
    RUNNING = 0
    COMPLETED = 1
    FAILED = 2
    CANCELED = 3
    TERMINATED = 4
    CONTINUED_AS_NEW = 5
    TIMED_OUT = 6


@dataclass
class WorkflowExecution:
    """Represents a workflow execution"""
    workflow_id: str
    run_id: str
    workflow_type: str
    task_queue: str
    status: WorkflowStatus
    started_at: int
    closed_at: Optional[int] = None
    history_length: int = 0
    memo: Dict[str, Any] = field(default_factory=dict)
    search_attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowOptions:
    """Options for starting a workflow"""
    workflow_id: str
    workflow_type: str
    task_queue: str
    input: Any = None
    execution_timeout: Optional[int] = None
    run_timeout: Optional[int] = None
    task_timeout: Optional[int] = None
    memo: Dict[str, Any] = field(default_factory=dict)
    search_attributes: Dict[str, Any] = field(default_factory=dict)
    retry_policy: Optional['RetryPolicy'] = None


@dataclass
class RetryPolicy:
    """Retry policy for workflows and activities"""
    initial_interval: int = 1000  # milliseconds
    backoff_coefficient: float = 2.0
    maximum_interval: Optional[int] = None
    maximum_attempts: int = 0  # 0 = unlimited
    non_retryable_error_types: List[str] = field(default_factory=list)


@dataclass
class HistoryEvent:
    """Represents a history event"""
    event_id: int
    event_type: str
    event_time: int
    task_id: int = 0
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskQueue:
    """Represents a task queue"""
    name: str
    namespace: str
    task_type: str
    pollers: int = 0
    backlog_count: int = 0
    last_poll_at: Optional[int] = None


@dataclass
class Schedule:
    """Represents a schedule"""
    schedule_id: str
    workflow_type: str
    task_queue: str
    cron_schedule: str
    input: Any = None
    enabled: bool = True
    last_run_at: Optional[int] = None
    next_run_at: Optional[int] = None


@dataclass
class BatchOperation:
    """Represents a batch operation"""
    job_id: str
    operation: str
    status: str
    total_workflows: int = 0
    succeeded: int = 0
    failed: int = 0
    start_time: Optional[int] = None


@dataclass
class ActivityContext:
    """Context for activity execution"""
    activity_id: str
    activity_type: str
    task_queue: str
    workflow_id: str
    run_id: str
    attempt: int = 1
    heartbeat_timeout: Optional[int] = None


@dataclass
class WorkflowContext:
    """Context for workflow execution"""
    workflow_id: str
    run_id: str
    workflow_type: str
    task_queue: str
    attempt: int = 1
    memo: Dict[str, Any] = field(default_factory=dict)
    search_attributes: Dict[str, Any] = field(default_factory=dict)
