"""
Temporal Production Benchmark — Workflow and Activity Definitions.

Defines the same workload types as velocity-bench using Temporal's
native Python SDK. Each workflow uses Temporal's event-sourcing architecture
where every state mutation is persisted as a history event.

Architecture:
  [FastAPI service] ──► [Temporal Client] ──► [Temporal Server]
                                                    │
  [Worker] ◄────────────────────────────────────────┘
     └── executes workflows + activities
"""

import time
import json
import hashlib
from datetime import timedelta
from temporalio import workflow, activity
from temporalio.common import RetryPolicy

# ─── Activities ──────────────────────────────────────────────────────────────
# Activities are the unit of work in Temporal. Each activity execution
# is recorded as events in the workflow history.


def _compute_work(iterations: int = 2000) -> str:
    """SHA-256 hash chain — same CPU work across all engines."""
    h = hashlib.sha256(b"velocity-bench-seed").digest()
    for _ in range(iterations):
        h = hashlib.sha256(h).digest()
    return h.hex()


@activity.defn(name="execute_step")
async def execute_step(step_num: int) -> dict:
    """A single activity step — real SHA-256 compute work, recorded in workflow history."""
    result = _compute_work(2000)
    return {"step": step_num, "status": "ok", "hash": result}


@activity.defn(name="process_signal")
async def process_signal(signal_idx: int) -> dict:
    """Process a signal — recorded in workflow history."""
    return {"signal": signal_idx, "processed": True}


@activity.defn(name="echo_activity")
async def echo_activity(data: str) -> dict:
    """Echo activity — return input as-is."""
    return {"status": "ok", "data": data}


@activity.defn(name="payload_activity")
async def payload_activity(data: str) -> dict:
    """Payload roundtrip activity."""
    return {"status": "ok", "size": len(data)}


@activity.defn(name="stateful_activity")
async def stateful_activity(key: str, current_count: int) -> int:
    """Stateful activity — simulates state mutation."""
    return current_count + 1


@activity.defn(name="concurrent_activity")
async def concurrent_activity(workflow_id: int) -> dict:
    """Concurrent activity — simple computation."""
    return {"status": "ok", "id": workflow_id, "result": workflow_id * 2}


# No-retry policy for benchmarks (fail fast, don't retry)
NO_RETRY = RetryPolicy(maximum_attempts=1)

# ─── Workflows ───────────────────────────────────────────────────────────────
# Each workflow is a durable execution. Temporal records every activity
# completion, signal, query, and timer as history events.


@workflow.defn(name="SimpleWorkflow")
class SimpleWorkflow:
    """Simple workflow: 10 activity steps, each recorded in history."""

    def __init__(self) -> None:
        self._steps_completed = 0

    @workflow.run
    async def run(self) -> dict:
        for i in range(10):
            result = await workflow.execute_activity(
                execute_step,
                i,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=NO_RETRY,
            )
            self._steps_completed += 1
        return {"status": "completed", "steps": self._steps_completed}

    @workflow.signal(name="add_signal")
    async def add_signal(self) -> None:
        self._steps_completed += 1

    @workflow.query(name="get_progress")
    def get_progress(self) -> dict:
        return {"steps_completed": self._steps_completed}


@workflow.defn(name="SignalStormWorkflow")
class SignalStormWorkflow:
    """Signal storm: receive N signals, process each as an activity."""

    def __init__(self) -> None:
        self._signals_received = 0
        self._target_signals = 100
        self._signal_queue: list[int] = []

    @workflow.run
    async def run(self, num_signals: int = 100) -> dict:
        self._target_signals = num_signals
        # Wait until all signals are received
        await workflow.wait_condition(
            lambda: self._signals_received >= self._target_signals
        )
        return {
            "status": "completed",
            "signals_received": self._signals_received,
        }

    @workflow.signal(name="receive_signal")
    async def receive_signal(self, signal_idx: int) -> None:
        # Process each signal as an activity
        await workflow.execute_activity(
            process_signal,
            signal_idx,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=NO_RETRY,
        )
        self._signals_received += 1


@workflow.defn(name="ColdStartWorkflow")
class ColdStartWorkflow:
    """Cold start: single activity — measures first-execution overhead."""

    @workflow.run
    async def run(self) -> dict:
        result = await workflow.execute_activity(
            execute_step,
            0,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=NO_RETRY,
        )
        return {"status": "ok", "ts": workflow.now().timestamp()}


@workflow.defn(name="MultiStepWorkflow")
class MultiStepWorkflow:
    """Multi-step: N activity steps, each recorded in history."""

    @workflow.run
    async def run(self, num_steps: int = 100) -> dict:
        last = 0
        for i in range(num_steps):
            result = await workflow.execute_activity(
                execute_step,
                i,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=NO_RETRY,
            )
            last = i
        return {"status": "completed", "steps_completed": last + 1}


@workflow.defn(name="EchoWorkflow")
class EchoWorkflow:
    """Echo: single activity that returns input."""

    @workflow.run
    async def run(self, data: str = "") -> dict:
        result = await workflow.execute_activity(
            echo_activity,
            data,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=NO_RETRY,
        )
        return result


@workflow.defn(name="PayloadWorkflow")
class PayloadWorkflow:
    """Payload roundtrip: single activity with payload."""

    @workflow.run
    async def run(self, data: str = "") -> dict:
        result = await workflow.execute_activity(
            payload_activity,
            data,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=NO_RETRY,
        )
        return result


@workflow.defn(name="StatefulWorkflow")
class StatefulWorkflow:
    """Stateful: activities with state passed through workflow."""

    @workflow.run
    async def run(self, key: str = "default") -> dict:
        count = 0
        for _ in range(5):
            count = await workflow.execute_activity(
                stateful_activity,
                args=[key, count],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=NO_RETRY,
            )
        return {"status": "ok", "key": key, "count": count}


@workflow.defn(name="ConcurrentWorkflow")
class ConcurrentWorkflow:
    """Concurrent: simple single-activity workflow."""

    @workflow.run
    async def run(self, workflow_id: int = 0) -> dict:
        result = await workflow.execute_activity(
            concurrent_activity,
            workflow_id,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=NO_RETRY,
        )
        return result


@workflow.defn(name="DurablePromiseWorkflow")
class DurablePromiseWorkflow:
    """Durable promise: set state, read it back."""

    @workflow.run
    async def run(self) -> dict:
        # Simulate durable promise by using workflow state
        promise_value = {"resolved": True, "ts": workflow.now().timestamp()}
        # Execute an activity to persist the state
        result = await workflow.execute_activity(
            echo_activity,
            json.dumps(promise_value),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=NO_RETRY,
        )
        return json.loads(result["data"]) if isinstance(result.get("data"), str) else promise_value


# ─── Temporal-Specific Strength Workloads ────────────────────────────────────
# These highlight Temporal's unique activity scheduling and timer capabilities.


@activity.defn(name="schedulable_task")
async def schedulable_task(task_id: int) -> dict:
    """A schedulable activity — highlights Temporal's activity scheduling.

    Temporal's server manages activity scheduling, heartbeating, timeouts,
    and retries. This activity simulates real work that exercises the
    scheduler. Each execution is recorded as history events.
    """
    # Simulate variable-duration work (like real activities)
    result = {"task_id": task_id, "scheduled": True, "ts": time.time()}
    return result


@activity.defn(name="timer_checkpoint")
async def timer_checkpoint(checkpoint_id: int) -> dict:
    """Record a timer checkpoint — activity that persists timer state."""
    return {"checkpoint": checkpoint_id, "ts": time.time()}


@workflow.defn(name="ActivitySchedulingWorkflow")
class ActivitySchedulingWorkflow:
    """Activity scheduling: schedule N activities with Temporal's scheduler.

    Highlights Temporal's ability to manage large numbers of concurrent
    activities with automatic retry, timeout, and heartbeating.
    """

    @workflow.run
    async def run(self, num_activities: int = 10) -> dict:
        # Schedule all activities through Temporal's scheduler
        tasks = []
        for i in range(num_activities):
            result = await workflow.execute_activity(
                schedulable_task,
                i,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=NO_RETRY,
            )
            tasks.append(result)
        return {
            "status": "completed",
            "activities_scheduled": num_activities,
            "activities_completed": len(tasks),
        }


@workflow.defn(name="LongRunningWorkflow")
class LongRunningWorkflow:
    """Long-running workflow: timers + activity checkpoints.

    Highlights Temporal's durable timer support — timers survive process
    restarts because they're persisted as workflow history events.
    Uses short timers (100ms) for benchmarking while demonstrating the pattern.
    """

    @workflow.run
    async def run(self, num_stages: int = 3) -> dict:
        stages = []
        for i in range(num_stages):
            # Durable timer — survives process restarts
            await workflow.sleep(0.1)  # 100ms for benchmarking
            # Checkpoint after each timer
            checkpoint = await workflow.execute_activity(
                timer_checkpoint,
                i,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=NO_RETRY,
            )
            stages.append(checkpoint)
        return {
            "status": "completed",
            "stages": len(stages),
            "timers_fired": num_stages,
        }


# ─── All Activities List ─────────────────────────────────────────────────────

ALL_ACTIVITIES = [
    execute_step,
    process_signal,
    echo_activity,
    payload_activity,
    stateful_activity,
    concurrent_activity,
    schedulable_task,
    timer_checkpoint,
]

# ─── All Workflows List ──────────────────────────────────────────────────────

ALL_WORKFLOWS = [
    SimpleWorkflow,
    SignalStormWorkflow,
    ColdStartWorkflow,
    MultiStepWorkflow,
    EchoWorkflow,
    PayloadWorkflow,
    StatefulWorkflow,
    ConcurrentWorkflow,
    DurablePromiseWorkflow,
    ActivitySchedulingWorkflow,
    LongRunningWorkflow,
]
