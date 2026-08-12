"""
VELOCITY-WorkFlow Python SDK - Depth tests.

Comprehensive tests for exception hierarchy, interceptors, mock client,
and testing utilities. These tests verify real behavior, not just mocking.
"""

import pytest
import logging
from io import StringIO
from velocity_sdk.exceptions import (
    VelocityError,
    WorkflowNotFoundError,
    WorkflowAlreadyCompletedError,
    ConnectionError,
    TimeoutError,
    RateLimitError,
    AuthenticationError,
    InternalError,
)
from velocity_sdk.interceptors import (
    WorkflowInterceptor,
    ActivityInterceptor,
    LoggingInterceptor,
    MetricsInterceptor,
    InterceptorChain,
)
from velocity_sdk.testing import (
    MockVelocityClient,
    WorkflowTestEnvironment,
)
from velocity_sdk.client import WorkflowStatus


# ─── Exception Hierarchy Tests ────────────────────────────────────────────────


class TestExceptionHierarchy:
    """Test exception error codes, messages, and retryable flags."""

    def test_velocity_error_base(self):
        """Test base VelocityError with all parameters."""
        error = VelocityError("Test error", error_code=99, retryable=True, details={"key": "value"})
        assert str(error) == "VelocityError[99]: Test error (retryable)"
        assert error.error_code == 99
        assert error.retryable is True
        assert error.details == {"key": "value"}
        assert error.message == "Test error"

    def test_workflow_not_found_error(self):
        """Test WorkflowNotFoundError with error code 1."""
        error = WorkflowNotFoundError(workflow_key=42)
        assert error.error_code == 1
        assert error.retryable is False
        assert error.workflow_key == 42
        assert "42" in str(error)

    def test_workflow_already_completed_error(self):
        """Test WorkflowAlreadyCompletedError with error code 2."""
        error = WorkflowAlreadyCompletedError(workflow_key=100)
        assert error.error_code == 2
        assert error.retryable is False
        assert error.workflow_key == 100
        assert "100" in str(error)

    def test_connection_error(self):
        """Test ConnectionError with error code 3."""
        error = ConnectionError(target="localhost:8080")
        assert error.error_code == 3
        assert error.retryable is True
        assert error.target == "localhost:8080"
        assert "localhost:8080" in str(error)

    def test_timeout_error(self):
        """Test TimeoutError with error code 4."""
        error = TimeoutError(operation="start_workflow", timeout_ms=5000)
        assert error.error_code == 4
        assert error.retryable is True
        assert error.operation == "start_workflow"
        assert error.timeout_ms == 5000
        assert "5000" in str(error)

    def test_rate_limit_error(self):
        """Test RateLimitError with error code 5."""
        error = RateLimitError(retry_after_ms=1000)
        assert error.error_code == 5
        assert error.retryable is True
        assert error.retry_after_ms == 1000

    def test_authentication_error(self):
        """Test AuthenticationError with error code 6."""
        error = AuthenticationError()
        assert error.error_code == 6
        assert error.retryable is False
        assert "Authentication" in str(error)

    def test_internal_error(self):
        """Test InternalError with error code 7."""
        error = InternalError()
        assert error.error_code == 7
        assert error.retryable is True
        assert "Internal" in str(error)

    def test_exception_inheritance(self):
        """Test all exceptions inherit from VelocityError."""
        assert issubclass(WorkflowNotFoundError, VelocityError)
        assert issubclass(WorkflowAlreadyCompletedError, VelocityError)
        assert issubclass(ConnectionError, VelocityError)
        assert issubclass(TimeoutError, VelocityError)
        assert issubclass(RateLimitError, VelocityError)
        assert issubclass(AuthenticationError, VelocityError)
        assert issubclass(InternalError, VelocityError)


# ─── Interceptor Tests ────────────────────────────────────────────────────────


class TestInterceptors:
    """Test interceptor chain execution and behavior."""

    def test_interceptor_chain_execution_order(self):
        """Test interceptors execute in the order they were added."""
        execution_order = []

        class TrackingInterceptor(WorkflowInterceptor):
            def __init__(self, name):
                self.name = name

            def on_start(self, workflow_type, workflow_id, **kwargs):
                execution_order.append(self.name)

        chain = InterceptorChain()
        chain.add(TrackingInterceptor("first"))
        chain.add(TrackingInterceptor("second"))
        chain.add(TrackingInterceptor("third"))

        chain.invoke_workflow_start("test_workflow", 1)

        assert execution_order == ["first", "second", "third"]

    def test_logging_interceptor_output(self):
        """Test LoggingInterceptor produces correct log messages."""
        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        logger = logging.getLogger("test_logger")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        interceptor = LoggingInterceptor(logger)

        interceptor.on_start("test_workflow", 42)
        interceptor.on_signal(42, "test_signal")

        log_output = log_stream.getvalue()
        assert "Workflow started: type=test_workflow, id=42" in log_output
        assert "Workflow signal: id=42, signal=test_signal" in log_output

    def test_metrics_interceptor_counters(self):
        """Test MetricsInterceptor tracks workflow and activity metrics."""
        interceptor = MetricsInterceptor()

        # Workflow metrics
        interceptor.on_start("workflow1", 1)
        interceptor.on_start("workflow2", 2)

        assert interceptor.workflow_starts == 2

        # Activity metrics
        interceptor.on_execute("activity1", "act_1")
        interceptor.on_execute("activity2", "act_2")

        assert interceptor.activity_executions == 2

    def test_interceptor_chain_with_multiple_types(self):
        """Test InterceptorChain with both workflow and activity interceptors."""
        chain = InterceptorChain()
        metrics = MetricsInterceptor()
        chain.add(metrics)

        # Workflow lifecycle
        chain.invoke_workflow_start("workflow", 1)

        # Activity lifecycle
        chain.invoke_activity_execute("activity", "act_1")

        assert metrics.workflow_starts == 1
        assert metrics.activity_executions == 1


# ─── Mock Client Tests ────────────────────────────────────────────────────────


class TestMockVelocityClient:
    """Test MockVelocityClient behavior."""

    def test_mock_client_start_workflow(self):
        """Test starting a workflow with mock client."""
        client = MockVelocityClient()
        handle = client.start_workflow("test_workflow", total_steps=3)

        assert handle.workflow_key > 0
        assert handle.workflow_id > 0
        assert handle.run_id > 0

    def test_mock_client_describe_workflow(self):
        """Test describing a workflow."""
        client = MockVelocityClient()
        handle = client.start_workflow("test_workflow", total_steps=5)

        description = client.describe_workflow(handle.workflow_key)
        assert description.workflow_key == handle.workflow_key
        assert description.status == WorkflowStatus.RUNNING
        assert description.total_steps == 5
        assert description.current_step == 0

    def test_mock_client_signal_workflow(self):
        """Test signaling a workflow."""
        client = MockVelocityClient()
        handle = client.start_workflow("test_workflow")

        result = client.signal_workflow(handle.workflow_key, "test_signal", b"data")
        assert result is True

    def test_mock_client_complete_workflow(self):
        """Test completing a workflow."""
        client = MockVelocityClient()
        handle = client.start_workflow("test_workflow")

        result = client.complete_workflow(handle.workflow_key, b"success")
        assert result is True

        description = client.describe_workflow(handle.workflow_key)
        assert description.status == WorkflowStatus.COMPLETED

    def test_mock_client_fail_workflow(self):
        """Test failing a workflow."""
        client = MockVelocityClient()
        handle = client.start_workflow("test_workflow")

        result = client.fail_workflow(handle.workflow_key, "error occurred")
        assert result is True

        description = client.describe_workflow(handle.workflow_key)
        assert description.status == WorkflowStatus.FAILED

    def test_mock_client_cancel_workflow(self):
        """Test canceling a workflow."""
        client = MockVelocityClient()
        handle = client.start_workflow("test_workflow")

        result = client.cancel_workflow(handle.workflow_key)
        assert result is True

        description = client.describe_workflow(handle.workflow_key)
        assert description.status == WorkflowStatus.CANCELED

    def test_mock_client_not_found_error(self):
        """Test WorkflowNotFoundError for non-existent workflow."""
        client = MockVelocityClient()

        with pytest.raises(WorkflowNotFoundError) as exc_info:
            client.describe_workflow(999)

        assert exc_info.value.workflow_key == 999
        assert exc_info.value.error_code == 1

    def test_mock_client_already_completed_error(self):
        """Test WorkflowAlreadyCompletedError when completing twice."""
        client = MockVelocityClient()
        handle = client.start_workflow("test_workflow")

        client.complete_workflow(handle.workflow_key)

        with pytest.raises(WorkflowAlreadyCompletedError) as exc_info:
            client.complete_workflow(handle.workflow_key)

        assert exc_info.value.workflow_key == handle.workflow_key
        assert exc_info.value.error_code == 2


# ─── Workflow Test Environment Tests ──────────────────────────────────────────


class TestWorkflowTestEnvironment:
    """Test WorkflowTestEnvironment assertions and utilities."""

    def test_test_environment_creation(self):
        """Test creating a test environment."""
        env = WorkflowTestEnvironment()
        assert env.client is not None

    def test_test_environment_start_workflow(self):
        """Test starting a workflow in test environment."""
        env = WorkflowTestEnvironment()
        handle = env.client.start_workflow("test_workflow", total_steps=2)

        assert handle.workflow_key > 0

    def test_test_environment_assert_workflow_running(self):
        """Test asserting workflow is running."""
        env = WorkflowTestEnvironment()
        handle = env.client.start_workflow("test_workflow")

        description = env.client.describe_workflow(handle.workflow_key)
        assert description.status == WorkflowStatus.RUNNING

    def test_test_environment_assert_workflow_completed(self):
        """Test asserting workflow is completed."""
        env = WorkflowTestEnvironment()
        handle = env.client.start_workflow("test_workflow")
        env.client.complete_workflow(handle.workflow_key)

        description = env.client.describe_workflow(handle.workflow_key)
        assert description.status == WorkflowStatus.COMPLETED

    def test_test_environment_multiple_workflows(self):
        """Test managing multiple workflows in test environment."""
        env = WorkflowTestEnvironment()

        handles = []
        for i in range(5):
            handle = env.client.start_workflow(f"workflow_{i}")
            handles.append(handle)

        # Complete some workflows
        env.client.complete_workflow(handles[0].workflow_key)
        env.client.complete_workflow(handles[1].workflow_key)
        env.client.fail_workflow(handles[2].workflow_key)

        # Verify states
        assert env.client.describe_workflow(handles[0].workflow_key).status == WorkflowStatus.COMPLETED
        assert env.client.describe_workflow(handles[1].workflow_key).status == WorkflowStatus.COMPLETED
        assert env.client.describe_workflow(handles[2].workflow_key).status == WorkflowStatus.FAILED
        assert env.client.describe_workflow(handles[3].workflow_key).status == WorkflowStatus.RUNNING
        assert env.client.describe_workflow(handles[4].workflow_key).status == WorkflowStatus.RUNNING


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
