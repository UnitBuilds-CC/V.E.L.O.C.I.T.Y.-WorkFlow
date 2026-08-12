//! gRPC server implementation for the VELOCITY-WorkFlow engine.
//!
//! This module provides a tonic-based gRPC server that wraps the `WorkflowEngine`,
//! exposing all workflow lifecycle operations as gRPC RPCs. SDKs in any language
//! can connect to this server to start workflows, signal them, poll for tasks, etc.
//!
//! # Feature Flag
//!
//! This module is only compiled when the `grpc` feature is enabled:
//! ```toml
//! [dependencies]
//! velocity-workflow-engine = { version = "0.1", features = ["grpc"] }
//! ```
//!
//! # Architecture
//!
//! ```text
//! [SDK Client] ──gRPC──► [WorkflowServiceImpl] ──► [WorkflowEngine]
//!                         (tonic service impl)     (zero-GC runtime)
//! ```
//!
//! The server holds an `Arc<WorkflowEngine>` and delegates all operations to it.
//! Error mapping converts engine-level errors (not found, already exists, etc.)
//! to appropriate gRPC status codes (NOT_FOUND, ALREADY_EXISTS, etc.).

#![cfg(feature = "grpc")]

use std::collections::HashMap;
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::engine::{WorkflowEngine, WorkflowStatus};
use crate::namespace::{NamespaceConfig, NamespaceError};
use crate::task_queue::TaskKind;
use crate::visibility::WorkflowExecutionInfo;

// Include the generated protobuf/gRPC code.
// The build.rs compiles protos into src/grpc/ when the grpc feature is enabled.
pub mod velocity_proto {
    tonic::include_proto!("velocity.v1");
}

use velocity_proto::workflow_service_server::{WorkflowService, WorkflowServiceServer};
use velocity_proto::*;

// ─── Error Mapping ─────────────────────────────────────────────────────────────

/// Convert a `WorkflowStatus` to its protobuf enum equivalent.
fn status_to_proto(status: WorkflowStatus) -> i32 {
    match status {
        WorkflowStatus::Void => WorkflowExecutionStatus::Unspecified as i32,
        WorkflowStatus::Running => WorkflowExecutionStatus::Running as i32,
        WorkflowStatus::Completed => WorkflowExecutionStatus::Completed as i32,
        WorkflowStatus::Failed => WorkflowExecutionStatus::Failed as i32,
        WorkflowStatus::Canceled => WorkflowExecutionStatus::Canceled as i32,
        WorkflowStatus::Terminated => WorkflowExecutionStatus::Terminated as i32,
        WorkflowStatus::ContinuedAsNew => WorkflowExecutionStatus::ContinuedAsNew as i32,
        WorkflowStatus::TimedOut => WorkflowExecutionStatus::TimedOut as i32,
    }
}

/// Convert a protobuf `WorkflowExecutionStatus` to the engine's `WorkflowStatus`.
fn status_from_proto(status: i32) -> WorkflowStatus {
    match WorkflowExecutionStatus::try_from(status) {
        Ok(WorkflowExecutionStatus::Running) => WorkflowStatus::Running,
        Ok(WorkflowExecutionStatus::Completed) => WorkflowStatus::Completed,
        Ok(WorkflowExecutionStatus::Failed) => WorkflowStatus::Failed,
        Ok(WorkflowExecutionStatus::Canceled) => WorkflowStatus::Canceled,
        Ok(WorkflowExecutionStatus::Terminated) => WorkflowStatus::Terminated,
        Ok(WorkflowExecutionStatus::ContinuedAsNew) => WorkflowStatus::ContinuedAsNew,
        Ok(WorkflowExecutionStatus::TimedOut) => WorkflowStatus::TimedOut,
        _ => WorkflowStatus::Void,
    }
}

/// Convert a `TaskKind` to its protobuf enum equivalent.
fn task_kind_to_proto(kind: TaskKind) -> i32 {
    match kind {
        TaskKind::WorkflowTask => velocity_proto::TaskKind::WorkflowTask as i32,
        TaskKind::ActivityTask => velocity_proto::TaskKind::ActivityTask as i32,
        TaskKind::TimerTask => velocity_proto::TaskKind::TimerTask as i32,
        TaskKind::SignalTask => velocity_proto::TaskKind::SignalTask as i32,
    }
}

/// Convert a `WorkflowExecutionInfo` to its protobuf representation.
fn execution_info_to_proto(info: &WorkflowExecutionInfo) -> WorkflowExecutionInfoProto {
    WorkflowExecutionInfoProto {
        execution: Some(velocity_proto::WorkflowExecution {
            workflow_id: info.workflow_id.to_string(),
            run_id: info.run_id.to_string(),
        }),
        r#type: Some(velocity_proto::WorkflowType {
            name: info.workflow_type_id.to_string(),
            type_id: info.workflow_type_id,
        }),
        start_time: None, // Instant doesn't convert to Timestamp directly
        close_time: info.close_time_ms.map(|ms| prost_types::Timestamp {
            seconds: (ms / 1000) as i64,
            nanos: ((ms % 1000) * 1_000_000) as i32,
        }),
        status: status_to_proto(info.status),
        history_length: 0,
        namespace: info.namespace_id.to_string(),
        namespace_id: info.namespace_id,
        task_queue: Some(velocity_proto::TaskQueue {
            name: info.task_queue_hash.to_string(),
            hash: info.task_queue_hash,
            kind: 0,
        }),
        search_attributes: None,
        memo: None,
        parent_execution: None,
        total_steps: 0,
    }
}

/// Map a `NamespaceError` to a gRPC `Status`.
fn namespace_error_to_status(err: &NamespaceError) -> Status {
    match err {
        NamespaceError::AlreadyExists(name) => Status::already_exists(format!(
            "namespace '{}' already exists", name
        )),
        NamespaceError::NotFound(id) => Status::not_found(format!(
            "namespace {} not found", id
        )),
        NamespaceError::CannotDeleteDefault => Status::failed_precondition(
            "cannot delete the default namespace"
        ),
        NamespaceError::Inactive(id) => Status::failed_precondition(format!(
            "namespace {} is not active", id
        )),
        NamespaceError::ConcurrencyLimitExceeded(id) => Status::resource_exhausted(format!(
            "namespace {} concurrency limit exceeded", id
        )),
    }
}

// ─── Type Aliases for Generated Proto Types ────────────────────────────────────
// (Disambiguate from engine types with the same names)

type WorkflowExecutionInfoProto = velocity_proto::WorkflowExecutionInfo;

// ─── Workflow Service Implementation ───────────────────────────────────────────

/// The gRPC service implementation wrapping the `WorkflowEngine`.
///
/// All state lives in the engine (Rust-owned, zero-GC). This struct is just
/// a thin adapter between the gRPC/protobuf world and the engine's native API.
pub struct WorkflowServiceImpl {
    engine: Arc<WorkflowEngine>,
}

impl WorkflowServiceImpl {
    /// Create a new gRPC service wrapping the given engine.
    pub fn new(engine: Arc<WorkflowEngine>) -> Self {
        Self { engine }
    }

    /// Create a tonic `Server` ready to be bound to a socket.
    pub fn into_server(self) -> WorkflowServiceServer<Self> {
        WorkflowServiceServer::new(self)
    }

    /// Resolve a workflow key from namespace_id and workflow_id.
    fn workflow_key(namespace_id: u64, workflow_id: u64) -> u64 {
        (namespace_id << 32) | workflow_id
    }

    /// Resolve namespace_id from a namespace name. Falls back to 0 (default) if empty.
    fn resolve_namespace(&self, namespace: &str) -> Result<u64, Status> {
        if namespace.is_empty() {
            return Ok(0);
        }
        self.engine
            .namespaces()
            .get_by_name(namespace)
            .ok_or_else(|| {
                Status::not_found(format!("namespace '{}' not found", namespace))
            })
    }
}

#[tonic::async_trait]
impl WorkflowService for WorkflowServiceImpl {
    // ─── Workflow Lifecycle ────────────────────────────────────────────────────

    async fn start_workflow_execution(
        &self,
        request: Request<StartWorkflowExecutionRequest>,
    ) -> Result<Response<StartWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = self.resolve_namespace(&req.namespace)?;
        let workflow_id = req
            .workflow_execution
            .as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let workflow_type_id = req
            .workflow_type
            .as_ref()
            .map(|t| t.type_id)
            .unwrap_or(0);
        let task_queue_hash = req
            .task_queue
            .as_ref()
            .map(|tq| tq.hash)
            .unwrap_or(0);
        let total_steps = req.total_steps;
        let input = req.input.map(|p| p.data);

        let key = self.engine.start_workflow(
            workflow_id,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            input,
        );

        let run_id = {
            let workflows = self.engine.workflows_write();
            workflows.get(&key).map(|ctx| ctx.run_id).unwrap_or(0)
        };

        Ok(Response::new(StartWorkflowExecutionResponse {
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
            }),
            workflow_key: key,
            started: true,
        }))
    }

    async fn signal_workflow_execution(
        &self,
        request: Request<SignalWorkflowExecutionRequest>,
    ) -> Result<Response<SignalWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = self.resolve_namespace(&req.namespace)?;
        let workflow_id = req
            .workflow_execution
            .as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(namespace_id, workflow_id);

        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!(
                "workflow execution {} not found", workflow_id
            )));
        }

        let signal_name_id = req.signal_name_id;
        let payload = req.input.map(|p| p.data).unwrap_or_default();

        self.engine.signal_workflow(key, signal_name_id, payload);

        Ok(Response::new(SignalWorkflowExecutionResponse {}))
    }

    async fn signal_with_start_workflow_execution(
        &self,
        request: Request<SignalWithStartWorkflowExecutionRequest>,
    ) -> Result<Response<SignalWithStartWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = self.resolve_namespace(&req.namespace)?;
        let workflow_id = req
            .workflow_execution
            .as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let workflow_type_id = req.workflow_type.as_ref().map(|t| t.type_id).unwrap_or(0);
        let task_queue_hash = req.task_queue.as_ref().map(|tq| tq.hash).unwrap_or(0);
        let total_steps = req.total_steps;
        let signal_name_id = req.signal_name_id;
        let payload = req.signal_input.map(|p| p.data).unwrap_or_default();

        let (key, was_started) = self.engine.signal_with_start(
            workflow_id,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            signal_name_id,
            payload,
        );

        let run_id = {
            let workflows = self.engine.workflows_write();
            workflows.get(&key).map(|ctx| ctx.run_id).unwrap_or(0)
        };

        Ok(Response::new(SignalWithStartWorkflowExecutionResponse {
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
            }),
            workflow_key: key,
            started: was_started,
        }))
    }

    async fn query_workflow(
        &self,
        request: Request<QueryWorkflowRequest>,
    ) -> Result<Response<QueryWorkflowResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = self.resolve_namespace(&req.namespace)?;
        let workflow_id = req
            .workflow_execution
            .as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(namespace_id, workflow_id);

        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!(
                "workflow execution {} not found", workflow_id
            )));
        }

        let query = req.query.ok_or_else(|| {
            Status::invalid_argument("query is required")
        })?;

        let query_name_id = query.query_name_id;
        let query_args = query.query_args.map(|p| p.data).unwrap_or_default();

        match self.engine.execute_query(key, query_name_id, &query_args) {
            Some(result) => Ok(Response::new(QueryWorkflowResponse {
                query_result: Some(velocity_proto::Payload {
                    data: result,
                    encoding: 0,
                    metadata: HashMap::new(),
                }),
            })),
            None => Err(Status::not_found(format!(
                "query handler for name_id {} not registered", query_name_id
            ))),
        }
    }

    async fn cancel_workflow_execution(
        &self,
        request: Request<CancelWorkflowExecutionRequest>,
    ) -> Result<Response<CancelWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = self.resolve_namespace(&req.namespace)?;
        let workflow_id = req
            .workflow_execution
            .as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(namespace_id, workflow_id);

        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!(
                "workflow execution {} not found", workflow_id
            )));
        }

        self.engine.cancel_workflow(key);

        Ok(Response::new(CancelWorkflowExecutionResponse {}))
    }

    async fn terminate_workflow_execution(
        &self,
        request: Request<TerminateWorkflowExecutionRequest>,
    ) -> Result<Response<TerminateWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = self.resolve_namespace(&req.namespace)?;
        let workflow_id = req
            .workflow_execution
            .as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(namespace_id, workflow_id);

        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!(
                "workflow execution {} not found", workflow_id
            )));
        }

        self.engine.terminate_workflow(key);

        Ok(Response::new(TerminateWorkflowExecutionResponse {}))
    }

    // ─── Workflow Visibility ───────────────────────────────────────────────────

    async fn describe_workflow_execution(
        &self,
        request: Request<DescribeWorkflowExecutionRequest>,
    ) -> Result<Response<DescribeWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = self.resolve_namespace(&req.namespace)?;
        let workflow_id = req
            .workflow_execution
            .as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(namespace_id, workflow_id);

        let info = self.engine.visibility().get(key).ok_or_else(|| {
            Status::not_found(format!("workflow execution {} not found", workflow_id))
        })?;

        let total_steps = self.engine.get_total_steps(key);
        let history_length = self.engine.get_event_sequence(key) as i64;

        let mut exec_info = execution_info_to_proto(&info);
        exec_info.history_length = history_length;
        exec_info.total_steps = total_steps;

        Ok(Response::new(DescribeWorkflowExecutionResponse {
            execution_info: Some(exec_info),
            pending_activities: vec![],
            pending_workflow_task: None,
        }))
    }

    async fn list_workflow_executions(
        &self,
        request: Request<ListWorkflowExecutionsRequest>,
    ) -> Result<Response<ListWorkflowExecutionsResponse>, Status> {
        let req = request.into_inner();
        let _namespace_id = self.resolve_namespace(&req.namespace)?;

        let infos = if req.status_filter != 0 {
            let status = status_from_proto(req.status_filter);
            self.engine.visibility().list_by_status(status)
        } else if req.namespace_id_filter != 0 {
            self.engine.visibility().list_by_namespace(req.namespace_id_filter)
        } else if let Some(type_filter) = &req.type_filter {
            self.engine.visibility().list_by_type(type_filter.type_id)
        } else {
            // Return all workflows (by listing Running status as default)
            self.engine.visibility().list_by_status(WorkflowStatus::Running)
        };

        let page_size = if req.page_size > 0 { req.page_size as usize } else { 100 };
        let executions: Vec<WorkflowExecutionInfoProto> = infos
            .iter()
            .take(page_size)
            .map(|info| execution_info_to_proto(info))
            .collect();

        Ok(Response::new(ListWorkflowExecutionsResponse {
            executions,
            next_page_token: vec![],
        }))
    }

    async fn get_workflow_execution_history(
        &self,
        request: Request<GetWorkflowExecutionHistoryRequest>,
    ) -> Result<Response<GetWorkflowExecutionHistoryResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = self.resolve_namespace(&req.namespace)?;
        let workflow_id = req
            .workflow_execution
            .as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(namespace_id, workflow_id);

        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!(
                "workflow execution {} not found", workflow_id
            )));
        }

        // Return empty history for now (event history retrieval requires callback-based FFI)
        Ok(Response::new(GetWorkflowExecutionHistoryResponse {
            history: Some(velocity_proto::History { events: vec![] }),
            next_page_token: vec![],
            archived: false,
        }))
    }

    // ─── Task Dispatch ─────────────────────────────────────────────────────────

    async fn poll_workflow_task_queue(
        &self,
        request: Request<PollWorkflowTaskQueueRequest>,
    ) -> Result<Response<PollWorkflowTaskQueueResponse>, Status> {
        let req = request.into_inner();

        let task_queue_hash = req.task_queue.as_ref().map(|tq| tq.hash).unwrap_or(0);

        match self.engine.task_queue().try_poll(task_queue_hash) {
            Some(task) if task.kind == TaskKind::WorkflowTask => {
                let workflow_id = task.workflow_key & 0xFFFFFFFF;
                let run_id = {
                    let workflows = self.engine.workflows_write();
                    workflows.get(&task.workflow_key).map(|ctx| ctx.run_id).unwrap_or(0)
                };

                Ok(Response::new(PollWorkflowTaskQueueResponse {
                    task_token: task.task_id,
                    workflow_execution: Some(velocity_proto::WorkflowExecution {
                        workflow_id: workflow_id.to_string(),
                        run_id: run_id.to_string(),
                    }),
                    workflow_type: None,
                    history: None,
                    workflow_key: task.workflow_key,
                    step_index: task.step_index,
                    attempt: task.attempt as i32,
                }))
            }
            _ => {
                // No task available — return empty response (long-poll would block here)
                Ok(Response::new(PollWorkflowTaskQueueResponse::default()))
            }
        }
    }

    async fn poll_activity_task_queue(
        &self,
        request: Request<PollActivityTaskQueueRequest>,
    ) -> Result<Response<PollActivityTaskQueueResponse>, Status> {
        let req = request.into_inner();

        let task_queue_hash = req.task_queue.as_ref().map(|tq| tq.hash).unwrap_or(0);

        match self.engine.task_queue().try_poll(task_queue_hash) {
            Some(task) if task.kind == TaskKind::ActivityTask => {
                let workflow_id = task.workflow_key & 0xFFFFFFFF;
                let run_id = {
                    let workflows = self.engine.workflows_write();
                    workflows.get(&task.workflow_key).map(|ctx| ctx.run_id).unwrap_or(0)
                };

                Ok(Response::new(PollActivityTaskQueueResponse {
                    task_token: task.task_id,
                    workflow_execution: Some(velocity_proto::WorkflowExecution {
                        workflow_id: workflow_id.to_string(),
                        run_id: run_id.to_string(),
                    }),
                    activity_type: Some(velocity_proto::ActivityType {
                        name: task.activity_name_id.to_string(),
                        type_id: task.activity_name_id,
                    }),
                    input: None,
                    workflow_key: task.workflow_key,
                    step_index: task.step_index,
                    attempt: task.attempt as i32,
                    scheduled_time: None,
                    started_time: None,
                    retry_policy: None,
                }))
            }
            _ => {
                Ok(Response::new(PollActivityTaskQueueResponse::default()))
            }
        }
    }

    async fn respond_workflow_task_completed(
        &self,
        request: Request<RespondWorkflowTaskCompletedRequest>,
    ) -> Result<Response<RespondWorkflowTaskCompletedResponse>, Status> {
        let req = request.into_inner();

        // Process commands from the workflow task completion
        for cmd in &req.commands {
            if let Some(ref attrs) = cmd.attributes {
                match attrs {
                    velocity_proto::command::Attributes::CompleteWorkflow(c) => {
                        // Find workflow_key from task_token (simplified — real impl tracks mapping)
                        // For now, this is a placeholder for command processing
                        let _ = c;
                    }
                    velocity_proto::command::Attributes::FailWorkflow(c) => {
                        let _ = c;
                    }
                    velocity_proto::command::Attributes::ScheduleActivity(c) => {
                        let _ = c;
                    }
                    velocity_proto::command::Attributes::StartTimer(c) => {
                        let _ = c;
                    }
                    velocity_proto::command::Attributes::SignalExternal(c) => {
                        let _ = c;
                    }
                    velocity_proto::command::Attributes::StartChildWorkflow(c) => {
                        let _ = c;
                    }
                    velocity_proto::command::Attributes::CancelWorkflow(c) => {
                        let _ = c;
                    }
                    velocity_proto::command::Attributes::ContinueAsNew(c) => {
                        let _ = c;
                    }
                }
            }
        }

        Ok(Response::new(RespondWorkflowTaskCompletedResponse {}))
    }

    async fn respond_activity_task_completed(
        &self,
        request: Request<RespondActivityTaskCompletedRequest>,
    ) -> Result<Response<RespondActivityTaskCompletedResponse>, Status> {
        let req = request.into_inner();

        let workflow_key = req.workflow_key;
        let step = req.step_index;
        let result = req.result.map(|p| p.data).unwrap_or_default();

        let status = self.engine.get_status(workflow_key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found("workflow not found"));
        }

        self.engine.complete_activity(workflow_key, step, result);

        Ok(Response::new(RespondActivityTaskCompletedResponse {}))
    }

    async fn respond_activity_task_failed(
        &self,
        request: Request<RespondActivityTaskFailedRequest>,
    ) -> Result<Response<RespondActivityTaskFailedResponse>, Status> {
        let req = request.into_inner();

        let workflow_key = req.workflow_key;
        let step = req.step_index;

        let status = self.engine.get_status(workflow_key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found("workflow not found"));
        }

        // Attempt retry; if permanent failure, fail the workflow
        let retried = self.engine.fail_activity_with_retry(workflow_key, step);
        if !retried {
            self.engine.fail_workflow(workflow_key);
        }

        Ok(Response::new(RespondActivityTaskFailedResponse {}))
    }

    async fn respond_query_task_completed(
        &self,
        request: Request<RespondQueryTaskCompletedRequest>,
    ) -> Result<Response<RespondQueryTaskCompletedResponse>, Status> {
        let _req = request.into_inner();
        // Query results are handled synchronously via QueryWorkflow RPC
        Ok(Response::new(RespondQueryTaskCompletedResponse {}))
    }

    // ─── Namespace Management ──────────────────────────────────────────────────

    async fn register_namespace(
        &self,
        request: Request<RegisterNamespaceRequest>,
    ) -> Result<Response<RegisterNamespaceResponse>, Status> {
        let req = request.into_inner();

        let ns_id = self.engine.namespaces().register_auto(&req.namespace);

        // Update description and metadata if provided
        if let Some(ns_config) = self.engine.namespaces().get(ns_id) {
            let mut config = ns_config;
            if !req.description.is_empty() {
                config.description = req.description;
            }
            if !req.metadata.is_empty() {
                config.metadata = req.metadata.into_iter().collect();
            }
            if req.max_concurrent_workflows > 0 {
                config.max_concurrent_workflows = req.max_concurrent_workflows;
            }
            // Re-register with updated config (delete + re-register)
            let _ = self.engine.namespaces().delete(ns_id);
            let _ = self.engine.namespaces().register(config);
        }

        Ok(Response::new(RegisterNamespaceResponse {
            namespace_id: ns_id,
        }))
    }

    async fn describe_namespace(
        &self,
        request: Request<DescribeNamespaceRequest>,
    ) -> Result<Response<DescribeNamespaceResponse>, Status> {
        let req = request.into_inner();

        let ns_id = if req.namespace_id != 0 {
            req.namespace_id
        } else {
            self.resolve_namespace(&req.namespace)?
        };

        let config = self.engine.namespaces().get(ns_id).ok_or_else(|| {
            Status::not_found(format!("namespace '{}' not found", req.namespace))
        })?;

        Ok(Response::new(DescribeNamespaceResponse {
            namespace_info: Some(velocity_proto::NamespaceInfo {
                name: config.name,
                namespace_id: config.id,
                description: config.description,
                is_active: config.is_active,
                retention_period: Some(prost_types::Duration {
                    seconds: config.retention_period.as_secs() as i64,
                    nanos: config.retention_period.subsec_nanos() as i32,
                }),
                max_concurrent_workflows: config.max_concurrent_workflows,
                metadata: config.metadata.into_iter().collect(),
            }),
            config: None,
        }))
    }

    async fn list_namespaces(
        &self,
        _request: Request<ListNamespacesRequest>,
    ) -> Result<Response<ListNamespacesResponse>, Status> {
        let namespaces = self.engine.namespaces().list();

        let ns_infos: Vec<velocity_proto::NamespaceInfo> = namespaces
            .iter()
            .map(|config| velocity_proto::NamespaceInfo {
                name: config.name.clone(),
                namespace_id: config.id,
                description: config.description.clone(),
                is_active: config.is_active,
                retention_period: Some(prost_types::Duration {
                    seconds: config.retention_period.as_secs() as i64,
                    nanos: config.retention_period.subsec_nanos() as i32,
                }),
                max_concurrent_workflows: config.max_concurrent_workflows,
                metadata: config.metadata.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            })
            .collect();

        Ok(Response::new(ListNamespacesResponse {
            namespaces: ns_infos,
            next_page_token: vec![],
        }))
    }

    async fn update_namespace(
        &self,
        request: Request<UpdateNamespaceRequest>,
    ) -> Result<Response<UpdateNamespaceResponse>, Status> {
        let req = request.into_inner();

        let ns_id = self.resolve_namespace(&req.namespace)?;

        // Delete and re-register with updated config
        let mut config = self.engine.namespaces().get(ns_id).ok_or_else(|| {
            Status::not_found(format!("namespace '{}' not found", req.namespace))
        })?;

        if let Some(update) = &req.update {
            if let Some(retention) = &update.workflow_execution_retention_period {
                config.retention_period = std::time::Duration::new(
                    retention.seconds as u64,
                    retention.nanos as u32,
                );
            }
        }

        let _ = self.engine.namespaces().delete(ns_id);
        self.engine.namespaces().register(config.clone()).map_err(|e| {
            namespace_error_to_status(&e)
        })?;

        Ok(Response::new(UpdateNamespaceResponse {
            namespace_info: Some(velocity_proto::NamespaceInfo {
                name: config.name,
                namespace_id: config.id,
                description: config.description,
                is_active: config.is_active,
                retention_period: Some(prost_types::Duration {
                    seconds: config.retention_period.as_secs() as i64,
                    nanos: config.retention_period.subsec_nanos() as i32,
                }),
                max_concurrent_workflows: config.max_concurrent_workflows,
                metadata: config.metadata.into_iter().collect(),
            }),
            config: None,
        }))
    }

    // ─── System ────────────────────────────────────────────────────────────────

    async fn get_system_info(
        &self,
        _request: Request<GetSystemInfoRequest>,
    ) -> Result<Response<GetSystemInfoResponse>, Status> {
        Ok(Response::new(GetSystemInfoResponse {
            system_info: Some(velocity_proto::SystemInfo {
                server: Some(velocity_proto::ServerInfo {
                    server_version: env!("CARGO_PKG_VERSION").to_string(),
                    supported_features: vec![
                        "signal_with_start".to_string(),
                        "query".to_string(),
                        "update".to_string(),
                        "child_workflows".to_string(),
                        "cron".to_string(),
                        "batch_operations".to_string(),
                        "nexus".to_string(),
                        "saga".to_string(),
                    ],
                }),
                capabilities: Some(velocity_proto::Capabilities {
                    signal_and_query_header: true,
                    internal_error_differentiation: true,
                    signal_with_start_as_new: true,
                    upsert_memo: true,
                    eager_workflow_start: false,
                    nexus: true,
                }),
            }),
        }))
    }
}

// ─── Server Builder ────────────────────────────────────────────────────────────

/// Convenience function to create and run the gRPC server.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use velocity_workflow_engine::WorkflowEngine;
/// use velocity_workflow_engine::grpc_server;
///
/// #[tokio::main]
/// async fn main() {
///     let engine = Arc::new(WorkflowEngine::new());
///     let addr = "[::1]:50051".parse().unwrap();
///     grpc_server::run_server(engine, addr).await.unwrap();
/// }
/// ```
pub async fn run_server(
    engine: Arc<WorkflowEngine>,
    addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = WorkflowServiceImpl::new(engine);

    println!("VELOCITY-WorkFlow gRPC server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════════
// UNIT TESTS
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a service wrapping a fresh engine.
    fn test_service() -> WorkflowServiceImpl {
        WorkflowServiceImpl::new(Arc::new(WorkflowEngine::new()))
    }

    /// Helper: create a tonic Request from a message.
    fn req<T>(msg: T) -> Request<T> {
        Request::new(msg)
    }

    // ─── Test 1: Start a workflow and verify the response ──────────────────────

    #[tokio::test]
    async fn test_start_workflow_execution() {
        let svc = test_service();

        let request = req(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "1001".to_string(),
                run_id: String::new(),
            }),
            workflow_type: Some(velocity_proto::WorkflowType {
                name: "TestWorkflow".to_string(),
                type_id: 1,
            }),
            task_queue: Some(velocity_proto::TaskQueue {
                name: "test-queue".to_string(),
                hash: 42,
                kind: 0,
            }),
            input: Some(velocity_proto::Payload {
                data: vec![1, 2, 3],
                encoding: 0,
                metadata: HashMap::new(),
            }),
            total_steps: 3,
            ..Default::default()
        });

        let response = svc.start_workflow_execution(request).await.unwrap();
        let resp = response.into_inner();

        assert!(resp.started);
        assert!(resp.workflow_key > 0);
        assert_eq!(resp.workflow_execution.unwrap().workflow_id, "1001");
    }

    // ─── Test 2: Signal a running workflow ─────────────────────────────────────

    #[tokio::test]
    async fn test_signal_workflow_execution() {
        let svc = test_service();

        // Start a workflow first
        let key = svc.engine.start_workflow(2001, 1, 0, 42, 1, None);

        let request = req(SignalWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "2001".to_string(),
                run_id: String::new(),
            }),
            signal_name: "my_signal".to_string(),
            signal_name_id: 100,
            input: Some(velocity_proto::Payload {
                data: vec![7, 8, 9],
                encoding: 0,
                metadata: HashMap::new(),
            }),
            ..Default::default()
        });

        let result = svc.signal_workflow_execution(request).await;
        assert!(result.is_ok());

        // Verify the signal was delivered
        assert!(svc.engine.has_signal(key, 100));
    }

    // ─── Test 3: Signal a non-existent workflow returns NOT_FOUND ──────────────

    #[tokio::test]
    async fn test_signal_nonexistent_workflow_returns_not_found() {
        let svc = test_service();

        let request = req(SignalWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "9999".to_string(),
                run_id: String::new(),
            }),
            signal_name: "sig".to_string(),
            signal_name_id: 1,
            ..Default::default()
        });

        let result = svc.signal_workflow_execution(request).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    // ─── Test 4: Cancel a running workflow ─────────────────────────────────────

    #[tokio::test]
    async fn test_cancel_workflow_execution() {
        let svc = test_service();
        let key = svc.engine.start_workflow(3001, 1, 0, 42, 1, None);

        let request = req(CancelWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "3001".to_string(),
                run_id: String::new(),
            }),
            ..Default::default()
        });

        let result = svc.cancel_workflow_execution(request).await;
        assert!(result.is_ok());
        assert_eq!(svc.engine.get_status(key), WorkflowStatus::Canceled);
    }

    // ─── Test 5: Terminate a running workflow ──────────────────────────────────

    #[tokio::test]
    async fn test_terminate_workflow_execution() {
        let svc = test_service();
        let key = svc.engine.start_workflow(4001, 1, 0, 42, 1, None);

        let request = req(TerminateWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "4001".to_string(),
                run_id: String::new(),
            }),
            reason: "test termination".to_string(),
            ..Default::default()
        });

        let result = svc.terminate_workflow_execution(request).await;
        assert!(result.is_ok());
        assert_eq!(svc.engine.get_status(key), WorkflowStatus::Terminated);
    }

    // ─── Test 6: Describe a workflow execution ─────────────────────────────────

    #[tokio::test]
    async fn test_describe_workflow_execution() {
        let svc = test_service();
        let _key = svc.engine.start_workflow(5001, 1, 0, 42, 5, None);

        let request = req(DescribeWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "5001".to_string(),
                run_id: String::new(),
            }),
        });

        let response = svc.describe_workflow_execution(request).await.unwrap();
        let resp = response.into_inner();

        let info = resp.execution_info.unwrap();
        assert_eq!(info.status, WorkflowExecutionStatus::Running as i32);
        assert_eq!(info.total_steps, 5);
    }

    // ─── Test 7: List workflow executions ──────────────────────────────────────

    #[tokio::test]
    async fn test_list_workflow_executions() {
        let svc = test_service();

        // Start a few workflows
        svc.engine.start_workflow(6001, 1, 0, 42, 1, None);
        svc.engine.start_workflow(6002, 1, 0, 42, 1, None);
        svc.engine.start_workflow(6003, 2, 0, 42, 1, None);

        let request = req(ListWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            status_filter: WorkflowExecutionStatus::Running as i32,
            ..Default::default()
        });

        let response = svc.list_workflow_executions(request).await.unwrap();
        let resp = response.into_inner();

        assert_eq!(resp.executions.len(), 3);
    }

    // ─── Test 8: Register and describe a namespace ─────────────────────────────

    #[tokio::test]
    async fn test_register_and_describe_namespace() {
        let svc = test_service();

        // Register a namespace
        let reg_request = req(RegisterNamespaceRequest {
            namespace: "test-ns".to_string(),
            description: "Test namespace".to_string(),
            max_concurrent_workflows: 100,
            ..Default::default()
        });

        let reg_response = svc.register_namespace(reg_request).await.unwrap();
        let ns_id = reg_response.into_inner().namespace_id;
        assert!(ns_id > 0);

        // Describe the namespace
        let desc_request = req(DescribeNamespaceRequest {
            namespace: "test-ns".to_string(),
            namespace_id: ns_id,
        });

        let desc_response = svc.describe_namespace(desc_request).await.unwrap();
        let info = desc_response.into_inner().namespace_info.unwrap();

        assert_eq!(info.name, "test-ns");
        assert_eq!(info.description, "Test namespace");
        assert!(info.is_active);
    }

    // ─── Test 9: Get system info ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_system_info() {
        let svc = test_service();

        let request = req(GetSystemInfoRequest {});
        let response = svc.get_system_info(request).await.unwrap();
        let resp = response.into_inner();

        let info = resp.system_info.unwrap();
        let server = info.server.unwrap();
        assert!(!server.server_version.is_empty());
        assert!(server.supported_features.contains(&"signal_with_start".to_string()));

        let caps = info.capabilities.unwrap();
        assert!(caps.signal_and_query_header);
        assert!(caps.nexus);
    }

    // ─── Test 10: SignalWithStart starts a new workflow ────────────────────────

    #[tokio::test]
    async fn test_signal_with_start_creates_new_workflow() {
        let svc = test_service();

        let request = req(SignalWithStartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "10001".to_string(),
                run_id: String::new(),
            }),
            workflow_type: Some(velocity_proto::WorkflowType {
                name: "TestWorkflow".to_string(),
                type_id: 1,
            }),
            task_queue: Some(velocity_proto::TaskQueue {
                name: "test-queue".to_string(),
                hash: 42,
                kind: 0,
            }),
            signal_name: "init_signal".to_string(),
            signal_name_id: 200,
            signal_input: Some(velocity_proto::Payload {
                data: vec![10, 20],
                encoding: 0,
                metadata: HashMap::new(),
            }),
            total_steps: 2,
            ..Default::default()
        });

        let response = svc.signal_with_start_workflow_execution(request).await.unwrap();
        let resp = response.into_inner();

        assert!(resp.started); // Should be started since workflow didn't exist
        assert!(resp.workflow_key > 0);

        // Verify signal was delivered
        assert!(svc.engine.has_signal(resp.workflow_key, 200));
    }

    // ─── Test 11: RespondActivityTaskCompleted completes the step ──────────────

    #[tokio::test]
    async fn test_respond_activity_task_completed() {
        let svc = test_service();
        let key = svc.engine.start_workflow(11001, 1, 0, 42, 3, None);

        let request = req(RespondActivityTaskCompletedRequest {
            task_token: 1,
            result: Some(velocity_proto::Payload {
                data: vec![42, 43, 44],
                encoding: 0,
                metadata: HashMap::new(),
            }),
            identity: "test-worker".to_string(),
            namespace: "default".to_string(),
            workflow_key: key,
            step_index: 0,
        });

        let result = svc.respond_activity_task_completed(request).await;
        assert!(result.is_ok());

        // Verify the step was completed
        assert!(svc.engine.is_step_completed(key, 0));
        assert_eq!(svc.engine.get_step_result(key, 0), Some(vec![42, 43, 44]));
    }

    // ─── Test 12: List namespaces includes default ─────────────────────────────

    #[tokio::test]
    async fn test_list_namespaces_includes_default() {
        let svc = test_service();

        let request = req(ListNamespacesRequest::default());
        let response = svc.list_namespaces(request).await.unwrap();
        let resp = response.into_inner();

        assert!(!resp.namespaces.is_empty());
        assert!(resp.namespaces.iter().any(|ns| ns.name == "default"));
    }

    // ─── Test 13: Status conversion round-trip ─────────────────────────────────

    #[test]
    fn test_status_conversion_round_trip() {
        let statuses = vec![
            WorkflowStatus::Running,
            WorkflowStatus::Completed,
            WorkflowStatus::Failed,
            WorkflowStatus::Canceled,
            WorkflowStatus::Terminated,
            WorkflowStatus::ContinuedAsNew,
            WorkflowStatus::TimedOut,
        ];

        for status in statuses {
            let proto_val = status_to_proto(status);
            let back = status_from_proto(proto_val);
            assert_eq!(status, back, "round-trip failed for {:?}", status);
        }
    }

    // ─── Test 14: PollWorkflowTaskQueue returns empty when no tasks ────────────

    #[tokio::test]
    async fn test_poll_workflow_task_queue_empty() {
        let svc = test_service();

        let request = req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue {
                name: "empty-queue".to_string(),
                hash: 999,
                kind: 0,
            }),
            identity: "test-worker".to_string(),
            build_id: String::new(),
        });

        let response = svc.poll_workflow_task_queue(request).await.unwrap();
        let resp = response.into_inner();

        // No tasks in this queue — should return default (empty) response
        assert_eq!(resp.task_token, 0);
    }

    // ─── Test 15: Workflow key computation ─────────────────────────────────────

    #[test]
    fn test_workflow_key_computation() {
        // namespace_id=1, workflow_id=42 → key = (1 << 32) | 42
        let key = WorkflowServiceImpl::workflow_key(1, 42);
        assert_eq!(key, (1u64 << 32) | 42);

        // namespace_id=0, workflow_id=0 → key = 0
        let key = WorkflowServiceImpl::workflow_key(0, 0);
        assert_eq!(key, 0);
    }
}
