//! Unit tests for the VELOCITY-WorkFlow Rust SDK.

use velocity_workflow_engine::engine::WorkflowStatus;

use crate::client::VelocityClient;
use crate::errors::ErrorKind;
use crate::interceptors::{LoggingInterceptor, MetricsInterceptor, InterceptorChain, WorkflowInterceptor};
use crate::testing::{MockClient, TestWorkflowEnvironment};

// ─── Client tests ────────────────────────────────────────────────────────────

#[test]
fn test_start_workflow_returns_nonzero_key() {
    let client = VelocityClient::new();
    let key = client.start_workflow(1, 1, 1, 3);
    assert_ne!(key, 0, "start_workflow should return a non-zero key");
    client.destroy();
}

#[test]
fn test_get_status_running_after_start() {
    let client = VelocityClient::new();
    let key = client.start_workflow(10, 1, 1, 5);
    assert_eq!(client.get_status(key), WorkflowStatus::Running);
    client.destroy();
}

#[test]
fn test_cancel_workflow_sets_canceled() {
    let client = VelocityClient::new();
    let key = client.start_workflow(20, 1, 1, 3);
    client.cancel_workflow(key);
    assert_eq!(client.get_status(key), WorkflowStatus::Canceled);
    client.destroy();
}

#[test]
fn test_complete_step_succeeds() {
    let client = VelocityClient::new();
    let key = client.start_workflow(30, 1, 1, 5);
    let result = client.complete_step(key, 0, b"step0".to_vec());
    assert!(result.is_ok());
    client.destroy();
}

#[test]
fn test_signal_workflow_does_not_panic() {
    let client = VelocityClient::new();
    let key = client.start_workflow(40, 1, 1, 3);
    client.signal_workflow(key, 1, b"hello".to_vec());
    // No panic = success (engine absorbs signals for running workflows).
    client.destroy();
}

#[test]
fn test_describe_nonexistent_workflow_returns_error() {
    let client = VelocityClient::new();
    let desc = client.describe_workflow(999_999);
    assert!(desc.is_err());
    assert_eq!(desc.unwrap_err().kind, ErrorKind::WorkflowNotFound);
    client.destroy();
}

#[test]
fn test_list_workflows_includes_started() {
    let client = VelocityClient::new();
    let key = client.start_workflow(50, 1, 1, 2);
    let keys = client.list_workflows();
    assert!(keys.contains(&key), "list_workflows should contain the started key");
    client.destroy();
}

// ─── MockClient tests ────────────────────────────────────────────────────────

#[test]
fn test_mock_client_start_and_complete() {
    let mut mock = MockClient::new();
    let key = mock.start_workflow(1, 1, 1, 3);
    assert_eq!(mock.get_status(key).unwrap(), WorkflowStatus::Running);
    mock.complete_workflow(key, b"done".to_vec()).unwrap();
    assert_eq!(mock.get_status(key).unwrap(), WorkflowStatus::Completed);
}

#[test]
fn test_mock_client_signal_not_found() {
    let mut mock = MockClient::new();
    let result = mock.signal_workflow(42, 1, b"x".to_vec());
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind, ErrorKind::WorkflowNotFound);
}

#[test]
fn test_mock_client_cancel() {
    let mut mock = MockClient::new();
    let key = mock.start_workflow(2, 1, 1, 1);
    mock.cancel_workflow(key).unwrap();
    assert_eq!(mock.get_status(key).unwrap(), WorkflowStatus::Canceled);
}

// ─── TestWorkflowEnvironment tests ───────────────────────────────────────────

#[test]
fn test_env_assert_workflow_completed() {
    let mut env = TestWorkflowEnvironment::new();
    let key = env.start_workflow(1, 1, 1, 2);
    env.client.complete_workflow(key, b"ok".to_vec()).unwrap();
    assert!(env.assert_workflow_completed(key).is_ok());
}

#[test]
fn test_env_assert_signal_received() {
    let mut env = TestWorkflowEnvironment::new();
    let key = env.start_workflow(1, 1, 1, 2);
    env.client.signal_workflow(key, 42, b"payload".to_vec()).unwrap();
    assert!(env.assert_signal_received(key, 42).is_ok());
    assert!(env.assert_signal_received(key, 99).is_err());
}

#[test]
fn test_env_reset_clears_state() {
    let mut env = TestWorkflowEnvironment::new();
    let _key = env.start_workflow(1, 1, 1, 1);
    env.reset();
    assert!(env.client.list_workflows().is_empty());
}
