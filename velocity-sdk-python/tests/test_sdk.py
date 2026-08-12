"""Tests for V.E.L.O.C.I.T.Y.-WorkFlow Python SDK"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from velocity.types import (
    WorkflowExecution,
    WorkflowOptions,
    WorkflowStatus,
    RetryPolicy,
    HistoryEvent,
    TaskQueue,
    Schedule,
    BatchOperation,
    WorkflowContext,
    ActivityContext,
)
from velocity.workflow import register_workflow, get_workflow, has_workflow, list_workflows
from velocity.activity import register_activity, get_activity, has_activity, list_activities
from velocity.client import Client, ClientOptions, WorkflowHandle
from velocity.worker import Worker, WorkerOptions
from velocity.connection import Connection
from velocity.advanced import (
    UpdateOptions, UpdateResult, ResetOptions,
    ContinueAsNewError, ScheduleClient, ScheduleOptions,
    SearchAttributesClient, BatchOperationClient, BatchOperationOptions,
    Saga,
)


def test_workflow_registration():
    """Test workflow registration and retrieval"""
    def test_workflow(ctx, input):
        return "test"

    register_workflow("test-workflow-reg", test_workflow)

    assert has_workflow("test-workflow-reg"), "Workflow should be registered"
    assert get_workflow("test-workflow-reg") == test_workflow, "Should retrieve same function"
    assert not has_workflow("non-existent-wf"), "Non-existent should not be registered"
    print("  test_workflow_registration: PASS")


def test_activity_registration():
    """Test activity registration and retrieval"""
    def test_activity(ctx, input):
        return "test"

    register_activity("test-activity-reg", test_activity)

    assert has_activity("test-activity-reg"), "Activity should be registered"
    assert get_activity("test-activity-reg") == test_activity, "Should retrieve same function"
    assert not has_activity("non-existent-act"), "Non-existent should not be registered"
    print("  test_activity_registration: PASS")


def test_workflow_context():
    """Test WorkflowContext creation"""
    ctx = WorkflowContext(
        workflow_id="test-workflow-id",
        run_id="test-run-id",
        workflow_type="test-type",
        task_queue="test-queue",
    )

    assert ctx.workflow_id == "test-workflow-id"
    assert ctx.run_id == "test-run-id"
    assert ctx.workflow_type == "test-type"
    assert ctx.task_queue == "test-queue"
    assert ctx.attempt == 1
    print("  test_workflow_context: PASS")


def test_activity_context():
    """Test ActivityContext creation"""
    ctx = ActivityContext(
        activity_id="test-activity-id",
        activity_type="test-type",
        task_queue="test-queue",
        workflow_id="test-workflow-id",
        run_id="test-run-id",
    )

    assert ctx.activity_id == "test-activity-id"
    assert ctx.activity_type == "test-type"
    assert ctx.task_queue == "test-queue"
    assert ctx.workflow_id == "test-workflow-id"
    assert ctx.run_id == "test-run-id"
    assert ctx.attempt == 1
    print("  test_activity_context: PASS")


def test_workflow_execution():
    """Test WorkflowExecution creation"""
    exec = WorkflowExecution(
        workflow_id="wf-1",
        run_id="run-1",
        workflow_type="test-type",
        task_queue="test-queue",
        status=WorkflowStatus.RUNNING,
        started_at=1000,
    )
    assert exec.workflow_id == "wf-1"
    assert exec.status == WorkflowStatus.RUNNING
    print("  test_workflow_execution: PASS")


def test_workflow_options():
    """Test WorkflowOptions creation"""
    opts = WorkflowOptions(
        workflow_id="wf-1",
        workflow_type="test-type",
        task_queue="test-queue",
        input={"key": "value"},
    )
    assert opts.workflow_id == "wf-1"
    assert opts.input == {"key": "value"}
    print("  test_workflow_options: PASS")


def test_retry_policy():
    """Test RetryPolicy creation"""
    policy = RetryPolicy(
        initial_interval=2000,
        backoff_coefficient=1.5,
        maximum_attempts=5,
    )
    assert policy.initial_interval == 2000
    assert policy.backoff_coefficient == 1.5
    assert policy.maximum_attempts == 5
    print("  test_retry_policy: PASS")


def test_connection():
    """Test Connection creation"""
    conn = Connection("localhost:7233", False)
    assert conn.host_port == "localhost:7233"
    assert not conn.is_connected()
    print("  test_connection: PASS")


def test_client_options():
    """Test ClientOptions creation"""
    opts = ClientOptions(host_port="localhost:7233", namespace="test-ns")
    assert opts.host_port == "localhost:7233"
    assert opts.namespace == "test-ns"
    print("  test_client_options: PASS")


def test_worker_options():
    """Test WorkerOptions creation"""
    opts = WorkerOptions(task_queue="test-queue", namespace="test-ns")
    assert opts.task_queue == "test-queue"
    assert opts.namespace == "test-ns"
    print("  test_worker_options: PASS")


def test_worker_requires_task_queue():
    """Test Worker requires task_queue"""
    try:
        Worker(WorkerOptions())
        assert False, "Should have raised ValueError"
    except ValueError:
        pass
    print("  test_worker_requires_task_queue: PASS")


def test_list_workflows():
    """Test listing workflows"""
    register_workflow("list-test-wf", lambda ctx, input: None)
    workflows = list_workflows()
    assert "list-test-wf" in workflows
    print("  test_list_workflows: PASS")


def test_list_activities():
    """Test listing activities"""
    register_activity("list-test-act", lambda ctx, input: None)
    activities = list_activities()
    assert "list-test-act" in activities
    print("  test_list_activities: PASS")


def test_update_workflow():
    """Test workflow update API"""
    client = Client(ClientOptions(host_port="localhost:7233"))
    result = client.update_workflow("wf-1", UpdateOptions(
        update_name="update-status",
        args={"status": "active"},
        wait_policy="COMPLETED",
    ))
    assert result.update_id != ""
    assert result.status == "ACCEPTED"
    client.close()
    print("  test_update_workflow: PASS")


def test_reset_workflow():
    """Test workflow reset API"""
    client = Client(ClientOptions(host_port="localhost:7233"))
    new_run_id = client.reset_workflow("wf-1", ResetOptions(
        reset_event_id=5,
        reason="testing reset",
    ))
    assert new_run_id != ""
    client.close()
    print("  test_reset_workflow: PASS")


def test_schedule_client():
    """Test schedule client operations"""
    client = Client(ClientOptions(host_port="localhost:7233"))
    sc = client.get_schedule_client()

    sid = sc.create(ScheduleOptions(
        schedule_id="daily-report",
        workflow_type="GenerateReport",
        task_queue="reports",
        cron_schedule="0 9 * * *",
    ))
    assert sid == "daily-report"

    desc = sc.describe("daily-report")
    assert desc["schedule_id"] == "daily-report"

    schedules = sc.list()
    assert isinstance(schedules, list)

    sc.delete("daily-report")
    client.close()
    print("  test_schedule_client: PASS")


def test_search_attributes_client():
    """Test search attributes client"""
    client = Client(ClientOptions(host_port="localhost:7233"))
    sac = client.get_search_attributes_client()

    sac.upsert("wf-1", {"CustomField": "value1"})
    workflows = sac.list_workflows("CustomField = 'value1'")
    assert isinstance(workflows, list)
    count = sac.count_workflows("CustomField = 'value1'")
    assert count >= 0
    client.close()
    print("  test_search_attributes_client: PASS")


def test_continue_as_new():
    """Test ContinueAsNewError"""
    err = ContinueAsNewError(
        workflow_type="LongRunningWorkflow",
        task_queue="main",
        input={"iteration": 42},
    )
    assert err.workflow_type == "LongRunningWorkflow"
    assert err.input == {"iteration": 42}
    assert "continue-as-new" in str(err)
    print("  test_continue_as_new: PASS")


def test_batch_operation_client():
    """Test batch operation client"""
    client = Client(ClientOptions(host_port="localhost:7233"))
    bc = client.get_batch_operation_client()

    job_id = bc.start(BatchOperationOptions(
        operation="terminate",
        query="WorkflowType = 'TestWorkflow'",
        reason="cleanup",
    ))
    assert job_id != ""

    desc = bc.describe(job_id)
    assert desc["job_id"] == job_id

    batches = bc.list()
    assert isinstance(batches, list)
    client.close()
    print("  test_batch_operation_client: PASS")


def test_saga_success():
    """Test saga execution with all steps succeeding"""
    saga = Saga()
    order = []

    saga.add_step("step1", lambda: (order.append("exec-1") or "r1"), lambda: order.append("comp-1"))
    saga.add_step("step2", lambda: (order.append("exec-2") or "r2"), lambda: order.append("comp-2"))

    results, error = saga.execute()
    assert error is None
    assert len(results) == 2
    assert order == ["exec-1", "exec-2"]
    print("  test_saga_success: PASS")


def test_saga_compensation():
    """Test saga compensation on failure"""
    saga = Saga()
    compensated = []

    saga.add_step("step1", lambda: "ok", lambda: compensated.append("step1"))

    def failing_step():
        raise ValueError("step2 failed")

    saga.add_step("step2", failing_step, lambda: compensated.append("step2"))

    results, error = saga.execute()
    assert error is not None
    assert compensated == ["step1"]  # Only step1 compensated (step2 never completed)
    print("  test_saga_compensation: PASS")


if __name__ == "__main__":
    print("Running V.E.L.O.C.I.T.Y.-WorkFlow Python SDK tests...")
    print()
    
    tests = [
        test_workflow_registration,
        test_activity_registration,
        test_workflow_context,
        test_activity_context,
        test_workflow_execution,
        test_workflow_options,
        test_retry_policy,
        test_connection,
        test_client_options,
        test_worker_options,
        test_worker_requires_task_queue,
        test_list_workflows,
        test_list_activities,
        test_update_workflow,
        test_reset_workflow,
        test_schedule_client,
        test_search_attributes_client,
        test_continue_as_new,
        test_batch_operation_client,
        test_saga_success,
        test_saga_compensation,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  {test.__name__}: FAIL - {e}")
            failed += 1
    
    print()
    print(f"Results: {passed} passed, {failed} failed, {passed + failed} total")
    
    if failed > 0:
        sys.exit(1)
