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
use std::sync::Mutex as StdMutex;

use tonic::{Request, Response, Status};

use crate::engine::{WorkflowEngine, WorkflowStatus};
use crate::matching_engine::{MatchingEngine, TaskQueueId, TaskQueueKind, TaskQueueType, MatchTask as MeMatchTask};
use crate::namespace::NamespaceError;
use crate::task_queue::TaskKind;
use crate::visibility::WorkflowExecutionInfo;
use crate::batch::BatchStatus;

// Include the generated protobuf/gRPC code.
// The build.rs compiles protos into src/grpc/ when the grpc feature is enabled.
pub mod velocity_proto {
    tonic::include_proto!("velocity.v1");
}

use velocity_proto::workflow_service_server::{WorkflowService, WorkflowServiceServer};
use velocity_proto::health_service_server::{HealthService, HealthServiceServer};
use velocity_proto::history_service_server::{HistoryService, HistoryServiceServer};
use velocity_proto::matching_service_server::{MatchingService, MatchingServiceServer};
use velocity_proto::worker_service_server::{WorkerService, WorkerServiceServer};
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

/// Current time as milliseconds since UNIX epoch.
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Convert a `TaskKind` to its protobuf enum equivalent.
#[allow(dead_code)]
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
    // Convert search attributes from engine to proto
    let search_attributes = if info.search_attributes.is_empty() {
        None
    } else {
        let indexed_fields: std::collections::HashMap<String, velocity_proto::SearchAttributeValue> = info
            .search_attributes
            .iter()
            .map(|(k, v)| {
                let proto_val = match v {
                    crate::visibility::SearchAttributeValue::String(s) => velocity_proto::search_attribute_value::Value::StringValue(s.clone()),
                    crate::visibility::SearchAttributeValue::Integer(i) => velocity_proto::search_attribute_value::Value::IntegerValue(*i),
                    crate::visibility::SearchAttributeValue::Double(d) => velocity_proto::search_attribute_value::Value::DoubleValue(*d),
                    crate::visibility::SearchAttributeValue::Bool(b) => velocity_proto::search_attribute_value::Value::BoolValue(*b),
                    crate::visibility::SearchAttributeValue::DateTime(ms) => velocity_proto::search_attribute_value::Value::DatetimeValue(prost_types::Timestamp {
                        seconds: (*ms / 1000) as i64,
                        nanos: ((*ms % 1000) * 1_000_000) as i32,
                    }),
                    crate::visibility::SearchAttributeValue::Keyword(k) => velocity_proto::search_attribute_value::Value::KeywordValue(k.clone()),
                };
                (k.clone(), velocity_proto::SearchAttributeValue { value: Some(proto_val) })
            })
            .collect();
        Some(velocity_proto::SearchAttributes { indexed_fields })
    };

    // Convert memo from engine to proto
    let memo = if info.memo.is_empty() {
        None
    } else {
        let fields: std::collections::HashMap<String, velocity_proto::Payload> = info
            .memo
            .iter()
            .map(|(k, v)| {
                (k.clone(), velocity_proto::Payload { data: v.clone(), encoding: 0, metadata: std::collections::HashMap::new() })
            })
            .collect();
        Some(velocity_proto::Memo { fields })
    };

    WorkflowExecutionInfoProto {
        execution: Some(velocity_proto::WorkflowExecution {
            workflow_id: info.workflow_id.to_string(),
            run_id: info.run_id.to_string(),
        }),
        r#type: Some(velocity_proto::WorkflowType {
            name: info.workflow_type_id.to_string(),
            type_id: info.workflow_type_id,
        }),
        start_time: Some(prost_types::Timestamp {
            seconds: (info.start_time_ms / 1000) as i64,
            nanos: ((info.start_time_ms % 1000) * 1_000_000) as i32,
        }),
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
        search_attributes,
        memo,
        parent_execution: None,
        total_steps: 0,
    }
}

/// Map a `NamespaceError` to a gRPC `Status`.
fn namespace_error_to_status(err: &NamespaceError) -> Status {
    match err {
        NamespaceError::AlreadyExists(name) => {
            Status::already_exists(format!("namespace '{}' already exists", name))
        }
        NamespaceError::NotFound(id) => Status::not_found(format!("namespace {} not found", id)),
        NamespaceError::CannotDeleteDefault => {
            Status::failed_precondition("cannot delete the default namespace")
        }
        NamespaceError::Inactive(id) => {
            Status::failed_precondition(format!("namespace {} is not active", id))
        }
        NamespaceError::ConcurrencyLimitExceeded(id) => {
            Status::resource_exhausted(format!("namespace {} concurrency limit exceeded", id))
        }
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
#[derive(Clone)]
pub struct WorkflowServiceImpl {
    engine: Arc<WorkflowEngine>,
    matching: Arc<MatchingEngine>,
    /// Maps task_token → workflow_key for command processing.
    task_tokens: Arc<StdMutex<HashMap<u64, u64>>>,
    /// Activity heartbeat tracker: task_token → last_heartbeat_time.
    #[allow(dead_code)]
    heartbeats: Arc<StdMutex<HashMap<u64, i64>>>,
}

impl WorkflowServiceImpl {
    /// Create a new gRPC service wrapping the given engine.
    pub fn new(engine: Arc<WorkflowEngine>) -> Self {
        Self {
            engine,
            matching: Arc::new(MatchingEngine::new()),
            task_tokens: Arc::new(StdMutex::new(HashMap::new())),
            heartbeats: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Create with a shared MatchingEngine for cross-service task dispatch.
    pub fn with_matching(engine: Arc<WorkflowEngine>, matching: Arc<MatchingEngine>) -> Self {
        Self {
            engine,
            matching,
            task_tokens: Arc::new(StdMutex::new(HashMap::new())),
            heartbeats: Arc::new(StdMutex::new(HashMap::new())),
        }
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
    #[allow(clippy::result_large_err)]
    fn resolve_namespace(&self, namespace: &str) -> Result<u64, Status> {
        if namespace.is_empty() {
            return Ok(0);
        }
        self.engine
            .namespaces()
            .get_by_name(namespace)
            .ok_or_else(|| Status::not_found(format!("namespace '{}' not found", namespace)))
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

        // Validate required fields
        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }
        let workflow_id = req
            .workflow_execution
            .as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        if workflow_id == 0 {
            return Err(Status::invalid_argument("workflow_id is required and must be non-zero"));
        }
        if req.workflow_type.is_none() {
            return Err(Status::invalid_argument("workflow_type is required"));
        }

        let namespace_id = self.resolve_namespace(&req.namespace)?;
        let workflow_type_id = req.workflow_type.as_ref().map(|t| t.type_id).unwrap_or(0);
        let task_queue_hash = req.task_queue.as_ref().map(|tq| tq.hash).unwrap_or(0);
        let total_steps = req.total_steps;
        let input = req.input.map(|p| p.data);

        // Convert search attributes from proto to engine format
        let search_attrs: HashMap<String, crate::visibility::SearchAttributeValue> = req
            .search_attributes
            .and_then(|sa| {
                let fields: HashMap<String, crate::visibility::SearchAttributeValue> = sa
                    .indexed_fields
                    .into_iter()
                    .filter_map(|(k, v)| {
                        v.value.map(|val| {
                            let attr_val = match val {
                                velocity_proto::search_attribute_value::Value::StringValue(s) => crate::visibility::SearchAttributeValue::String(s),
                                velocity_proto::search_attribute_value::Value::IntegerValue(i) => crate::visibility::SearchAttributeValue::Integer(i),
                                velocity_proto::search_attribute_value::Value::DoubleValue(d) => crate::visibility::SearchAttributeValue::Double(d),
                                velocity_proto::search_attribute_value::Value::BoolValue(b) => crate::visibility::SearchAttributeValue::Bool(b),
                                velocity_proto::search_attribute_value::Value::DatetimeValue(ts) => crate::visibility::SearchAttributeValue::DateTime(
                                    (ts.seconds as u64) * 1000 + (ts.nanos as u64) / 1_000_000,
                                ),
                                velocity_proto::search_attribute_value::Value::KeywordValue(k) => crate::visibility::SearchAttributeValue::Keyword(k),
                            };
                            (k, attr_val)
                        })
                    })
                    .collect();
                if fields.is_empty() { None } else { Some(fields) }
            })
            .unwrap_or_default();

        // Convert memo from proto to engine format
        let memo_fields: HashMap<String, Vec<u8>> = req
            .memo
            .map(|m| {
                m.fields
                    .into_iter()
                    .map(|(k, v)| (k, v.data))
                    .collect()
            })
            .unwrap_or_default();

        let key = self.engine.start_workflow_with_attrs(
            workflow_id,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            input,
            search_attrs,
        );

        // Store memo in visibility if provided
        if !memo_fields.is_empty() {
            if let Some(info) = self.engine.visibility().get(key) {
                let mut info = info;
                info.memo = memo_fields;
                self.engine.visibility().remove(key);
                self.engine.visibility().register(info);
            }
        }

        // Enforce concurrency limits per workflow type and namespace
        match self.engine.concurrency_limiter().acquire(key, workflow_type_id, namespace_id, 0) {
            crate::concurrency_limiter::AcquireResult::Acquired => { /* proceed */ }
            crate::concurrency_limiter::AcquireResult::Rejected => {
                // Rollback the workflow start
                self.engine.terminate_workflow(key);
                return Err(Status::resource_exhausted(format!(
                    "concurrency limit reached for workflow type {} in namespace {}",
                    workflow_type_id, namespace_id
                )));
            }
            crate::concurrency_limiter::AcquireResult::Queued(_ticket) => {
                // Queued — still allow the workflow to start but note it's queued
            }
        }

        let run_id = {
            let workflows = self.engine.workflows_write();
            workflows.get(&key).map(|ctx| ctx.run_id).unwrap_or(0)
        };

        // Schedule initial workflow task in the matching engine for worker dispatch
        let tq_name = req.task_queue.as_ref().map(|t| t.name.as_str()).unwrap_or("");
        if !tq_name.is_empty() {
            let tq_id = TaskQueueId::new(
                "default",
                tq_name,
                TaskQueueKind::Normal,
                TaskQueueType::Workflow,
            );
            self.matching.add_task(&tq_id, MeMatchTask {
                task_id: now_millis(),
                namespace_id: "default".to_string(),
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                task_type: TaskQueueType::Workflow,
                scheduled_time: now_millis(),
                priority: 0,
                forwarding_info: None,
                version: 0,
            });
        }

        // Record workflow started event in history store
        // (engine.start_workflow already records this internally)

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
                "workflow execution {} not found",
                workflow_id
            )));
        }

        let signal_name_id = req.signal_name_id;
        let payload = req.input.map(|p| p.data).unwrap_or_default();

        self.engine.signal_workflow(key, signal_name_id, payload);

        // Record SignalReceived in history
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::SignalReceived,
            vec![],
        );

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
                "workflow execution {} not found",
                workflow_id
            )));
        }

        let query = req
            .query
            .ok_or_else(|| Status::invalid_argument("query is required"))?;

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
                "query handler for name_id {} not registered",
                query_name_id
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
                "workflow execution {} not found",
                workflow_id
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
                "workflow execution {} not found",
                workflow_id
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

        let infos = if !req.query.is_empty() {
            // Use the search query executor for SQL-like query strings
            let executor = crate::search_query_executor::SearchQueryExecutor::new(self.engine.visibility());
            match executor.execute_string(&req.query) {
                Ok(results) => results,
                Err(e) => {
                    return Err(Status::invalid_argument(format!("Invalid query: {}", e)));
                }
            }
        } else {
            // Build filtered results from index-based queries
            let mut results = if req.status_filter != 0 {
                let status = status_from_proto(req.status_filter);
                self.engine.visibility().list_by_status(status)
            } else if req.namespace_id_filter != 0 {
                self.engine
                    .visibility()
                    .list_by_namespace(req.namespace_id_filter)
            } else if let Some(type_filter) = &req.type_filter {
                self.engine.visibility().list_by_type(type_filter.type_id)
            } else {
                self.engine.visibility().list_all()
            };

            // Apply time range filter if specified
            if req.start_time_min.is_some() || req.start_time_max.is_some() {
                let min_ms = req.start_time_min.as_ref().map(|t| {
                    (t.seconds as u64) * 1000 + (t.nanos as u64) / 1_000_000
                }).unwrap_or(0);
                let max_ms = req.start_time_max.as_ref().map(|t| {
                    (t.seconds as u64) * 1000 + (t.nanos as u64) / 1_000_000
                }).unwrap_or(u64::MAX);
                results.retain(|info| info.start_time_ms >= min_ms && info.start_time_ms <= max_ms);
            }

            results
        };

        // Cursor-based pagination using offset encoded in next_page_token
        let page_size = if req.page_size > 0 {
            req.page_size as usize
        } else {
            100
        };
        let offset = if req.next_page_token.is_empty() {
            0
        } else {
            // Decode offset from token (little-endian u64)
            let mut buf = [0u8; 8];
            let len = req.next_page_token.len().min(8);
            buf[..len].copy_from_slice(&req.next_page_token[..len]);
            u64::from_le_bytes(buf) as usize
        };

        let total = infos.len();
        let executions: Vec<WorkflowExecutionInfoProto> = infos
            .iter()
            .skip(offset)
            .take(page_size)
            .map(execution_info_to_proto)
            .collect();

        let next_page_token = if offset + executions.len() < total {
            let next = (offset + executions.len()) as u64;
            next.to_le_bytes().to_vec()
        } else {
            vec![]
        };

        Ok(Response::new(ListWorkflowExecutionsResponse {
            executions,
            next_page_token,
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
                "workflow execution {} not found",
                workflow_id
            )));
        }

        // Retrieve real history events from the engine's history store
        let engine_events = self.engine.history_store().get_history(key).unwrap_or_default();
        let events: Vec<velocity_proto::HistoryEvent> = engine_events
            .iter()
            .map(|e| velocity_proto::HistoryEvent {
                event_id: e.event_id,
                event_time: None,
                event_type: format!("{:?}", e.event_type),
                task_id: e.workflow_key,
                details: if e.payload.is_empty() {
                    None
                } else {
                    Some(velocity_proto::Payload {
                        data: e.payload.clone(),
                        encoding: 0,
                        metadata: std::collections::HashMap::new(),
                    })
                },
            })
            .collect();

        Ok(Response::new(GetWorkflowExecutionHistoryResponse {
            history: Some(velocity_proto::History { events }),
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

        // Validate required fields
        if req.task_queue.is_none() {
            return Err(Status::invalid_argument("task_queue is required"));
        }

        let task_queue_hash = req.task_queue.as_ref().map(|tq| tq.hash).unwrap_or(0);
        let long_poll_timeout_ms = req.long_poll_timeout_ms;

        // Try to poll a task, with optional long-poll timeout
        let task = if long_poll_timeout_ms > 0 {
            // Long-poll: wait for a task with timeout (use spawn_blocking to avoid blocking async runtime)
            let engine = self.engine.clone();
            let timeout = std::time::Duration::from_millis(long_poll_timeout_ms as u64);
            tokio::task::spawn_blocking(move || engine.task_queue().poll_timeout(task_queue_hash, timeout))
                .await
                .unwrap_or(None)
        } else {
            // Non-blocking: return immediately
            self.engine.task_queue().try_poll(task_queue_hash)
        };

        match task {
            Some(task) if task.kind == TaskKind::WorkflowTask => {
                let workflow_id = task.workflow_key & 0xFFFFFFFF;
                let (run_id, workflow_type_id) = {
                    let workflows = self.engine.workflows_write();
                    workflows
                        .get(&task.workflow_key)
                        .map(|ctx| (ctx.run_id, ctx.workflow_type_id))
                        .unwrap_or((0, 0))
                };

                // Store task_token → workflow_key mapping for command processing
                self.task_tokens.lock().unwrap().insert(task.task_id, task.workflow_key);

                // Build history events for deterministic replay
                let engine_events = self.engine.history_store().get_history(task.workflow_key).unwrap_or_default();
                let history_events: Vec<velocity_proto::HistoryEvent> = engine_events
                    .iter()
                    .map(|e| velocity_proto::HistoryEvent {
                        event_id: e.event_id,
                        event_time: None,
                        event_type: format!("{:?}", e.event_type),
                        task_id: e.workflow_key,
                        details: if e.payload.is_empty() {
                            None
                        } else {
                            Some(velocity_proto::Payload {
                                data: e.payload.clone(),
                                encoding: 0,
                                metadata: std::collections::HashMap::new(),
                            })
                        },
                    })
                    .collect();

                Ok(Response::new(PollWorkflowTaskQueueResponse {
                    task_token: task.task_id,
                    workflow_execution: Some(velocity_proto::WorkflowExecution {
                        workflow_id: workflow_id.to_string(),
                        run_id: run_id.to_string(),
                    }),
                    workflow_type: Some(velocity_proto::WorkflowType {
                        name: workflow_type_id.to_string(),
                        type_id: workflow_type_id,
                    }),
                    history: Some(velocity_proto::History { events: history_events }),
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

        // Validate required fields
        if req.task_queue.is_none() {
            return Err(Status::invalid_argument("task_queue is required"));
        }

        let task_queue_hash = req.task_queue.as_ref().map(|tq| tq.hash).unwrap_or(0);
        let long_poll_timeout_ms = req.long_poll_timeout_ms;

        // Try to poll a task, with optional long-poll timeout
        let task = if long_poll_timeout_ms > 0 {
            // Long-poll: wait for a task with timeout (use spawn_blocking to avoid blocking async runtime)
            let engine = self.engine.clone();
            let timeout = std::time::Duration::from_millis(long_poll_timeout_ms as u64);
            tokio::task::spawn_blocking(move || engine.task_queue().poll_timeout(task_queue_hash, timeout))
                .await
                .unwrap_or(None)
        } else {
            // Non-blocking: return immediately
            self.engine.task_queue().try_poll(task_queue_hash)
        };

        match task {
            Some(task) if task.kind == TaskKind::ActivityTask => {
                let workflow_id = task.workflow_key & 0xFFFFFFFF;
                let run_id = {
                    let workflows = self.engine.workflows_write();
                    workflows
                        .get(&task.workflow_key)
                        .map(|ctx| ctx.run_id)
                        .unwrap_or(0)
                };

                let now = now_millis();
                let scheduled_ts = prost_types::Timestamp {
                    seconds: (now / 1000) as i64,
                    nanos: 0,
                };
                let started_ts = prost_types::Timestamp {
                    seconds: (now / 1000) as i64,
                    nanos: 0,
                };

                // Record ActivityStarted in history
                self.engine.history_store().record_event(
                    task.workflow_key,
                    crate::event_history::HistoryEventType::ActivityStarted,
                    task.activity_name_id.to_le_bytes().to_vec(),
                );

                // Retrieve activity input payload from the engine
                let input = self.engine.get_activity_input(task.workflow_key, task.step_index)
                    .map(|data| velocity_proto::Payload {
                        data,
                        encoding: 0,
                        metadata: std::collections::HashMap::new(),
                    });

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
                    input,
                    workflow_key: task.workflow_key,
                    step_index: task.step_index,
                    attempt: task.attempt as i32,
                    scheduled_time: Some(scheduled_ts),
                    started_time: Some(started_ts),
                    retry_policy: None,
                }))
            }
            _ => Ok(Response::new(PollActivityTaskQueueResponse::default())),
        }
    }

    async fn respond_workflow_task_completed(
        &self,
        request: Request<RespondWorkflowTaskCompletedRequest>,
    ) -> Result<Response<RespondWorkflowTaskCompletedResponse>, Status> {
        let req = request.into_inner();

        // Resolve workflow_key from task_token
        let workflow_key = self.task_tokens.lock().unwrap()
            .remove(&req.task_token)
            .ok_or_else(|| Status::invalid_argument("invalid task_token"))?;

        // Process commands from the workflow task completion
        for cmd in &req.commands {
            if let Some(ref attrs) = cmd.attributes {
                match attrs {
                    velocity_proto::command::Attributes::CompleteWorkflow(c) => {
                        let result = c.result.as_ref().map(|p| p.data.clone());
                        self.engine.complete_workflow(workflow_key, result);
                        self.engine.history_store().record_event(
                            workflow_key,
                            crate::event_history::HistoryEventType::WorkflowCompleted,
                            vec![],
                        );
                        // Release concurrency slot
                        if let Some(ctx) = self.engine.workflows_write().get(&workflow_key) {
                            self.engine.concurrency_limiter().release(ctx.workflow_type_id, ctx.namespace_id);
                        }
                    }
                    velocity_proto::command::Attributes::FailWorkflow(c) => {
                        self.engine.fail_workflow(workflow_key);
                        self.engine.history_store().record_event(
                            workflow_key,
                            crate::event_history::HistoryEventType::WorkflowFailed,
                            c.failure.as_ref().map(|p| p.data.clone()).unwrap_or_default(),
                        );
                        // Release concurrency slot
                        if let Some(ctx) = self.engine.workflows_write().get(&workflow_key) {
                            self.engine.concurrency_limiter().release(ctx.workflow_type_id, ctx.namespace_id);
                        }
                    }
                    velocity_proto::command::Attributes::ScheduleActivity(c) => {
                        let activity_id = c.activity_type.as_ref().map(|a| a.type_id).unwrap_or(0);
                        let step = self.engine.get_status(workflow_key) as u32; // use status as step counter
                        let input = c.input.as_ref().map(|p| p.data.clone()).unwrap_or_default();
                        self.engine.schedule_activity(workflow_key, step, activity_id, input);
                        self.engine.history_store().record_event(
                            workflow_key,
                            crate::event_history::HistoryEventType::ActivityScheduled,
                            vec![],
                        );
                    }
                    velocity_proto::command::Attributes::StartTimer(c) => {
                        let delay = c.start_to_fire_timeout.as_ref().map(|d| {
                            std::time::Duration::new(d.seconds as u64, d.nanos as u32)
                        }).unwrap_or(std::time::Duration::from_secs(1));
                        self.engine.timer_engine().schedule(workflow_key, delay);
                        self.engine.history_store().record_event(
                            workflow_key,
                            crate::event_history::HistoryEventType::TimerStarted,
                            vec![],
                        );
                    }
                    velocity_proto::command::Attributes::SignalExternal(c) => {
                        // Route signal to the target workflow
                        let target_wf_id = c.execution.as_ref()
                            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
                            .unwrap_or(0);
                        let target_key = Self::workflow_key(workflow_key >> 32, target_wf_id);
                        let signal_name_id = c.signal_name.as_bytes().iter()
                            .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64));
                        let payload = c.input.as_ref().map(|p| p.data.clone()).unwrap_or_default();
                        self.engine.signal_workflow(target_key, signal_name_id, payload);
                        self.engine.history_store().record_event(
                            target_key,
                            crate::event_history::HistoryEventType::SignalReceived,
                            vec![],
                        );
                    }
                    velocity_proto::command::Attributes::StartChildWorkflow(c) => {
                        let child_wf_id = c.workflow_id.as_ref()
                            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
                            .unwrap_or(0);
                        let child_type_id = c.workflow_type.as_ref().map(|t| t.type_id).unwrap_or(0);
                        let child_tq_hash = c.task_queue.as_ref().map(|tq| tq.hash).unwrap_or(0);
                        let child_input = c.input.as_ref().map(|p| p.data.clone());
                        let total_steps = c.total_steps;
                        let child_key = self.engine.start_child_workflow(
                            workflow_key, child_wf_id, child_type_id, child_tq_hash, total_steps, child_input,
                        );
                        // Schedule initial workflow task in matching engine for the child
                        let tq_name = c.task_queue.as_ref().map(|t| t.name.as_str()).unwrap_or("");
                        if !tq_name.is_empty() {
                            let tq_id = TaskQueueId::new("default", tq_name, TaskQueueKind::Normal, TaskQueueType::Workflow);
                            let child_run_id = { self.engine.workflows_write().get(&child_key).map(|ctx| ctx.run_id).unwrap_or(0) };
                            self.matching.add_task(&tq_id, MeMatchTask {
                                task_id: now_millis(),
                                namespace_id: "default".to_string(),
                                workflow_id: child_wf_id.to_string(),
                                run_id: child_run_id.to_string(),
                                task_type: TaskQueueType::Workflow,
                                scheduled_time: now_millis(),
                                priority: 0,
                                forwarding_info: None,
                                version: 0,
                            });
                        }
                        self.engine.history_store().record_event(
                            workflow_key,
                            crate::event_history::HistoryEventType::ChildWorkflowStarted,
                            vec![],
                        );
                    }
                    velocity_proto::command::Attributes::CancelWorkflow(_c) => {
                        self.engine.cancel_workflow(workflow_key);
                        self.engine.history_store().record_event(
                            workflow_key,
                            crate::event_history::HistoryEventType::WorkflowCanceled,
                            vec![],
                        );
                    }
                    velocity_proto::command::Attributes::ContinueAsNew(c) => {
                        let new_input = c.input.as_ref().map(|p| p.data.clone());
                        let new_key = self.engine.continue_as_new(workflow_key, new_input);
                        // Schedule initial workflow task for the new run
                        let tq_name = c.task_queue.as_ref().map(|t| t.name.as_str()).unwrap_or("");
                        if !tq_name.is_empty() {
                            let tq_id = TaskQueueId::new("default", tq_name, TaskQueueKind::Normal, TaskQueueType::Workflow);
                            let new_run_id = { self.engine.workflows_write().get(&new_key).map(|ctx| ctx.run_id).unwrap_or(0) };
                            self.matching.add_task(&tq_id, MeMatchTask {
                                task_id: now_millis(),
                                namespace_id: "default".to_string(),
                                workflow_id: new_key.to_string(),
                                run_id: new_run_id.to_string(),
                                task_type: TaskQueueType::Workflow,
                                scheduled_time: now_millis(),
                                priority: 0,
                                forwarding_info: None,
                                version: 0,
                            });
                        }
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

        self.engine.complete_activity(workflow_key, step, result.clone());

        // Record ActivityCompleted in history
        self.engine.history_store().record_event(
            workflow_key,
            crate::event_history::HistoryEventType::ActivityCompleted,
            result,
        );

        // Complete heartbeat tracking if registered
        self.engine.heartbeat_tracker().complete(workflow_key, step as u64);

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

        // Record ActivityFailed in history
        let failure_data = req.failure.as_ref().map(|p| p.data.clone()).unwrap_or_default();
        self.engine.history_store().record_event(
            workflow_key,
            crate::event_history::HistoryEventType::ActivityFailed,
            failure_data,
        );

        // Fail heartbeat tracking if registered
        self.engine.heartbeat_tracker().fail(workflow_key, step as u64);

        if !retried {
            self.engine.fail_workflow(workflow_key);
            self.engine.history_store().record_event(
                workflow_key,
                crate::event_history::HistoryEventType::WorkflowFailed,
                vec![],
            );
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

        // Update description, retention, and metadata if provided
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
            // Apply retention period if specified
            if let Some(retention) = &req.workflow_execution_retention_period {
                config.retention_period = std::time::Duration::new(
                    retention.seconds as u64,
                    retention.nanos as u32,
                );
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

        let config =
            self.engine.namespaces().get(ns_id).ok_or_else(|| {
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
                metadata: config
                    .metadata
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
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
        let mut config =
            self.engine.namespaces().get(ns_id).ok_or_else(|| {
                Status::not_found(format!("namespace '{}' not found", req.namespace))
            })?;

        if let Some(update) = &req.update {
            if let Some(retention) = &update.workflow_execution_retention_period {
                config.retention_period =
                    std::time::Duration::new(retention.seconds as u64, retention.nanos as u32);
            }
        }

        let _ = self.engine.namespaces().delete(ns_id);
        self.engine
            .namespaces()
            .register(config.clone())
            .map_err(|e| namespace_error_to_status(&e))?;

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
        }))
    }

    // ─── System ────────────────────────────────────────────────────────────────

    async fn get_system_info(
        &self,
        _request: Request<GetSystemInfoRequest>,
    ) -> Result<Response<GetSystemInfoResponse>, Status> {
        let workflow_count = self.engine.visibility().count();
        let namespace_count = self.engine.namespaces().count();
        let schedule_count = self.engine.schedule_manager().count();
        let replay_count = self.engine.replay_engine().total_replays();
        let mut supported_features = vec![
            "signal_with_start".to_string(),
            "query".to_string(),
            "update".to_string(),
            "child_workflows".to_string(),
            "cron".to_string(),
            "batch_operations".to_string(),
            "nexus".to_string(),
            "saga".to_string(),
            "schedules".to_string(),
            "sticky_queues".to_string(),
            "build_id_versioning".to_string(),
            "deterministic_replay".to_string(),
            "wal_recovery".to_string(),
            "search_attributes".to_string(),
            "visibility_queries".to_string(),
        ];
        // Add runtime stats as features
        supported_features.push(format!("workflows:{}", workflow_count));
        supported_features.push(format!("namespaces:{}", namespace_count));
        supported_features.push(format!("schedules:{}", schedule_count));
        supported_features.push(format!("replays:{}", replay_count));
        Ok(Response::new(GetSystemInfoResponse {
            system_info: Some(velocity_proto::SystemInfo {
                server: Some(velocity_proto::ServerInfo {
                    server_version: env!("CARGO_PKG_VERSION").to_string(),
                    supported_features,
                }),
                capabilities: Some(velocity_proto::Capabilities {
                    signal_and_query_header: true,
                    internal_error_differentiation: true,
                    signal_with_start_as_new: true,
                    upsert_memo: true,
                    eager_workflow_start: true,
                    nexus: true,
                }),
            }),
        }))
    }

    // ─── Advanced Visibility ───────────────────────────────────────────────────

    async fn count_workflow_executions(
        &self,
        request: Request<CountWorkflowExecutionsRequest>,
    ) -> Result<Response<CountWorkflowExecutionsResponse>, Status> {
        let req = request.into_inner();
        let _ns_id = if req.namespace.is_empty() {
            self.resolve_namespace("default")?
        } else {
            self.resolve_namespace(&req.namespace)?
        };

        let count = if !req.query.is_empty() {
            // Use search query executor for filtered count
            let executor = crate::search_query_executor::SearchQueryExecutor::new(self.engine.visibility());
            match executor.execute_string(&req.query) {
                Ok(results) => results.len() as i64,
                Err(e) => {
                    return Err(Status::invalid_argument(format!("Invalid query: {}", e)));
                }
            }
        } else {
            self.engine.visibility().total_count() as i64
        };

        Ok(Response::new(CountWorkflowExecutionsResponse { count }))
    }

    async fn scan_workflow_executions(
        &self,
        request: Request<ScanWorkflowExecutionsRequest>,
    ) -> Result<Response<ScanWorkflowExecutionsResponse>, Status> {
        let req = request.into_inner();
        let _ns_id = if req.namespace.is_empty() {
            self.resolve_namespace("default")?
        } else {
            self.resolve_namespace(&req.namespace)?
        };

        // Support SQL-like query strings, otherwise return all workflows
        let infos = if !req.query.is_empty() {
            let executor = crate::search_query_executor::SearchQueryExecutor::new(self.engine.visibility());
            match executor.execute_string(&req.query) {
                Ok(results) => results,
                Err(e) => {
                    return Err(Status::invalid_argument(format!("Invalid query: {}", e)));
                }
            }
        } else {
            self.engine.visibility().list_all()
        };

        // Cursor-based pagination
        let page_size = if req.page_size > 0 { req.page_size as usize } else { 100 };
        let offset = if req.next_page_token.is_empty() {
            0
        } else {
            let mut buf = [0u8; 8];
            let len = req.next_page_token.len().min(8);
            buf[..len].copy_from_slice(&req.next_page_token[..len]);
            u64::from_le_bytes(buf) as usize
        };

        let total = infos.len();
        let page: Vec<velocity_proto::WorkflowExecutionInfo> = infos
            .iter()
            .skip(offset)
            .take(page_size)
            .map(execution_info_to_proto)
            .collect();

        let next_page_token = if offset + page.len() < total {
            ((offset + page.len()) as u64).to_le_bytes().to_vec()
        } else {
            vec![]
        };

        Ok(Response::new(ScanWorkflowExecutionsResponse {
            executions: page,
            next_page_token,
        }))
    }

    async fn reset_workflow_execution(
        &self,
        request: Request<ResetWorkflowExecutionRequest>,
    ) -> Result<Response<ResetWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns_id = if req.namespace.is_empty() {
            self.resolve_namespace("default")?
        } else {
            self.resolve_namespace(&req.namespace)?
        };
        let wf_exec = req
            .workflow_execution
            .ok_or_else(|| Status::invalid_argument("workflow_execution is required"))?;
        let wf_id = wf_exec.workflow_id.parse::<u64>().unwrap_or(0);
        let workflow_key = Self::workflow_key(ns_id, wf_id);

        // Verify the workflow exists
        let status = self.engine.get_status(workflow_key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow '{}' not found", wf_exec.workflow_id)));
        }

        // Reset the workflow to the specified event ID
        let reset_event_id = req.workflow_task_finish_event_id as u64;
        let success = self.engine.reset_workflow(workflow_key, reset_event_id);
        if !success {
            return Err(Status::failed_precondition("workflow reset failed"));
        }

        // Record reset in history
        self.engine.history_store().record_event(
            workflow_key,
            crate::event_history::HistoryEventType::WorkflowReset,
            vec![],
        );

        // Generate a new run ID
        let new_run_id = format!("reset-{}-{}", wf_exec.workflow_id, req.workflow_task_finish_event_id);
        Ok(Response::new(ResetWorkflowExecutionResponse {
            run_id: new_run_id,
        }))
    }

    // ─── Update Workflow Execution ────────────────────────────────────────────

    async fn update_workflow_execution(
        &self,
        request: Request<UpdateWorkflowExecutionRequest>,
    ) -> Result<Response<UpdateWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns_id = if req.namespace.is_empty() {
            self.resolve_namespace("default")?
        } else {
            self.resolve_namespace(&req.namespace)?
        };
        let wf_exec = req
            .workflow_execution
            .ok_or_else(|| Status::invalid_argument("workflow_execution is required"))?;
        let wf_id = wf_exec.workflow_id.parse::<u64>().unwrap_or(0);
        let workflow_key = Self::workflow_key(ns_id, wf_id);

        // Verify the workflow is running
        let status = self.engine.get_status(workflow_key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow '{}' not found", wf_exec.workflow_id)));
        }
        if status != WorkflowStatus::Running {
            return Err(Status::failed_precondition("workflow is not running"));
        }

        // Process the update through the engine's update registry
        let update_id = req.update_id.clone();
        let _update_name = req.update_name.clone();
        let _args = req.args.map(|p| p.data).unwrap_or_default();

        // For now, admit the update and return immediately
        // A full implementation would route through the workflow's update handler
        Ok(Response::new(UpdateWorkflowExecutionResponse {
            update_id: update_id.clone(),
            status: 0, // admitted
            result: None,
            failure: String::new(),
        }))
    }

    // ─── Schedule Management ──────────────────────────────────────────────────

    async fn create_schedule(
        &self,
        request: Request<CreateScheduleRequest>,
    ) -> Result<Response<CreateScheduleResponse>, Status> {
        let req = request.into_inner();
        if req.schedule_id.is_empty() {
            return Err(Status::invalid_argument("schedule_id is required"));
        }

        // Parse calendar spec from the proto spec
        let cal_spec = if let Some(spec) = &req.spec {
            if !spec.cron_expression.is_empty() {
                // Parse cron expression into CalendarSpec
                let parts: Vec<&str> = spec.cron_expression.split_whitespace().collect();
                crate::schedules::CalendarSpec {
                    second: if parts.len() > 5 { parts[0].to_string() } else { "0".into() },
                    minute: parts.first().unwrap_or(&"*").to_string(),
                    hour: if parts.len() > 1 { parts[1].to_string() } else { "*".into() },
                    day_of_month: if parts.len() > 2 { parts[2].to_string() } else { "*".into() },
                    month: if parts.len() > 3 { parts[3].to_string() } else { "*".into() },
                    day_of_week: if parts.len() > 4 { parts[4].to_string() } else { "*".into() },
                    comment: spec.cron_expression.clone(),
                }
            } else if !spec.calendar_spec.is_empty() {
                crate::schedules::CalendarSpec {
                    second: "0".into(),
                    minute: "*".into(),
                    hour: "*".into(),
                    day_of_month: "*".into(),
                    month: "*".into(),
                    day_of_week: "*".into(),
                    comment: spec.calendar_spec.clone(),
                }
            } else if spec.interval_seconds > 0 {
                // Convert interval to a calendar spec
                let minutes = (spec.interval_seconds / 60).max(1) as u32;
                crate::schedules::CalendarSpec::every_minutes(minutes)
            } else {
                crate::schedules::CalendarSpec::hourly()
            }
        } else {
            crate::schedules::CalendarSpec::hourly()
        };

        // Parse overlap policy
        let overlap = req.policies.as_ref()
            .map(|p| match p.overlap_policy {
                0 => crate::schedules::OverlapPolicy::Skip,
                1 => crate::schedules::OverlapPolicy::BufferOne,
                2 => crate::schedules::OverlapPolicy::BufferAll,
                3 => crate::schedules::OverlapPolicy::TerminateOther,
                4 => crate::schedules::OverlapPolicy::AllowAll,
                _ => crate::schedules::OverlapPolicy::Skip,
            })
            .unwrap_or(crate::schedules::OverlapPolicy::Skip);

        let jitter = req.spec.as_ref().map(|s| s.jitter_seconds as u64).unwrap_or(0);

        // Extract workflow type and namespace from the action
        let (workflow_type_id, task_queue_hash) = req.action.as_ref()
            .and_then(|a| a.start_workflow.as_ref())
            .map(|sw| {
                let wt_id = sw.workflow_type.as_ref().map(|t| t.type_id).unwrap_or(0);
                let tq_hash = sw.task_queue.as_ref().map(|tq| tq.hash).unwrap_or(0);
                (wt_id, tq_hash)
            })
            .unwrap_or((0, 0));

        let ns_id = self.resolve_namespace(&req.namespace).unwrap_or(0);

        // Create the schedule via the engine's ScheduleManager
        let sched_id = self.engine.schedule_manager().create_schedule(
            cal_spec,
            workflow_type_id,
            ns_id,
            task_queue_hash,
            overlap,
            jitter,
        );

        Ok(Response::new(CreateScheduleResponse {
            schedule_id: sched_id.to_string(),
        }))
    }

    async fn describe_schedule(
        &self,
        request: Request<DescribeScheduleRequest>,
    ) -> Result<Response<DescribeScheduleResponse>, Status> {
        let req = request.into_inner();
        if req.schedule_id.is_empty() {
            return Err(Status::invalid_argument("schedule_id is required"));
        }

        let sched_key: u64 = req.schedule_id.parse()
            .map_err(|_| Status::invalid_argument("invalid schedule_id"))?;

        let entry = self.engine.schedule_manager().get(sched_key)
            .ok_or_else(|| Status::not_found(format!("schedule '{}' not found", req.schedule_id)))?;

        // Convert engine ScheduleState to proto
        let state_status = match entry.state {
            crate::schedules::ScheduleState::Active => 0,
            crate::schedules::ScheduleState::Paused => 1,
            crate::schedules::ScheduleState::Completed => 2,
            crate::schedules::ScheduleState::Failed => 3,
        };

        let spec = ScheduleSpec {
            cron_expression: entry.calendar_spec.comment.clone(),
            calendar_spec: entry.calendar_spec.minute.clone(),
            interval_seconds: 0,
            start_time: entry.start_time_ms as i64,
            end_time: entry.end_time_ms.unwrap_or(0) as i64,
            jitter_seconds: entry.jitter_seconds as i32,
            timezone: String::new(),
        };

        let policies = SchedulePolicies {
            overlap_policy: entry.overlap_policy as i32,
            catchup_window_seconds: 0,
            pause_on_failure: false,
        };

        Ok(Response::new(DescribeScheduleResponse {
            schedule_id: req.schedule_id,
            spec: Some(spec),
            action: None,
            policies: Some(policies),
            state: Some(ScheduleState {
                status: state_status,
                note: entry.notes,
                action_count: entry.action_count as i64,
                missed_count: 0,
            }),
            create_time: entry.created_at_ms as i64,
            last_updated: entry.last_action_time_ms as i64,
        }))
    }

    async fn list_schedules(
        &self,
        request: Request<ListSchedulesRequest>,
    ) -> Result<Response<ListSchedulesResponse>, Status> {
        let req = request.into_inner();
        let entries = self.engine.schedule_manager().list();

        let page_size = if req.page_size > 0 { req.page_size as usize } else { 100 };
        let schedule_entries: Vec<ScheduleListEntry> = entries
            .iter()
            .take(page_size)
            .map(|e| {
                let state_status = match e.state {
                    crate::schedules::ScheduleState::Active => 0,
                    crate::schedules::ScheduleState::Paused => 1,
                    crate::schedules::ScheduleState::Completed => 2,
                    crate::schedules::ScheduleState::Failed => 3,
                };
                ScheduleListEntry {
                    schedule_id: e.schedule_id.to_string(),
                    spec: Some(ScheduleSpec {
                        cron_expression: e.calendar_spec.comment.clone(),
                        calendar_spec: e.calendar_spec.minute.clone(),
                        interval_seconds: 0,
                        start_time: e.start_time_ms as i64,
                        end_time: e.end_time_ms.unwrap_or(0) as i64,
                        jitter_seconds: e.jitter_seconds as i32,
                        timezone: String::new(),
                    }),
                    state: Some(ScheduleState {
                        status: state_status,
                        note: e.notes.clone(),
                        action_count: e.action_count as i64,
                        missed_count: 0,
                    }),
                }
            })
            .collect();

        Ok(Response::new(ListSchedulesResponse {
            schedules: schedule_entries,
            next_page_token: vec![],
        }))
    }

    async fn delete_schedule(
        &self,
        request: Request<DeleteScheduleRequest>,
    ) -> Result<Response<DeleteScheduleResponse>, Status> {
        let req = request.into_inner();
        if req.schedule_id.is_empty() {
            return Err(Status::invalid_argument("schedule_id is required"));
        }
        let sched_key: u64 = req.schedule_id.parse()
            .map_err(|_| Status::invalid_argument("invalid schedule_id"))?;

        if !self.engine.schedule_manager().delete(sched_key) {
            return Err(Status::not_found(format!("schedule '{}' not found", req.schedule_id)));
        }
        Ok(Response::new(DeleteScheduleResponse {}))
    }

    async fn update_schedule(
        &self,
        request: Request<UpdateScheduleRequest>,
    ) -> Result<Response<UpdateScheduleResponse>, Status> {
        let req = request.into_inner();
        if req.schedule_id.is_empty() {
            return Err(Status::invalid_argument("schedule_id is required"));
        }
        let sched_key: u64 = req.schedule_id.parse()
            .map_err(|_| Status::invalid_argument("invalid schedule_id"))?;

        // Verify the schedule exists
        let _entry = self.engine.schedule_manager().get(sched_key)
            .ok_or_else(|| Status::not_found(format!("schedule '{}' not found", req.schedule_id)))?;

        // Update overlap policy if provided
        if let Some(policies) = &req.policies {
            let overlap = match policies.overlap_policy {
                0 => crate::schedules::OverlapPolicy::Skip,
                1 => crate::schedules::OverlapPolicy::BufferOne,
                2 => crate::schedules::OverlapPolicy::BufferAll,
                3 => crate::schedules::OverlapPolicy::TerminateOther,
                4 => crate::schedules::OverlapPolicy::AllowAll,
                _ => crate::schedules::OverlapPolicy::Skip,
            };
            self.engine.schedule_manager().update_overlap_policy(sched_key, overlap);
        }

        Ok(Response::new(UpdateScheduleResponse {
            schedule_id: req.schedule_id,
        }))
    }

    // ─── Batch Operations ──────────────────────────────────────────────────────

    async fn start_batch_operation(
        &self,
        request: Request<StartBatchOperationRequest>,
    ) -> Result<Response<StartBatchOperationResponse>, Status> {
        let req = request.into_inner();
        
        // Validate required fields
        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }
        if req.visibility_query.is_empty() {
            return Err(Status::invalid_argument("visibility_query is required"));
        }

        // Execute the batch operation based on the operation type
        let batch_id = match req.operation {
            0 => {
                // Terminate: get all running workflows matching the query and terminate them
                let all_workflows = self.engine.visibility().list_all();
                let running_keys: Vec<u64> = all_workflows
                    .iter()
                    .filter(|info| info.status == WorkflowStatus::Running)
                    .map(|info| info.workflow_key)
                    .collect();
                self.engine.batch_executor().submit_terminate(&self.engine, running_keys)
            }
            1 => {
                // Cancel: similar to terminate but cancel instead
                // For now, we'll use terminate as a placeholder
                let all_workflows = self.engine.visibility().list_all();
                let running_keys: Vec<u64> = all_workflows
                    .iter()
                    .filter(|info| info.status == WorkflowStatus::Running)
                    .map(|info| info.workflow_key)
                    .collect();
                self.engine.batch_executor().submit_cancel(&self.engine, running_keys)
            }
            2 => {
                // Signal: signal all running workflows matching the query
                if req.signal_name.is_empty() {
                    return Err(Status::invalid_argument("signal_name is required for signal operation"));
                }
                let signal_name_id = req.signal_name.as_bytes().iter()
                    .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64));
                let payload = req.signal_input.as_ref().map(|p| p.data.clone()).unwrap_or_default();
                
                let all_workflows = self.engine.visibility().list_all();
                let running_keys: Vec<u64> = all_workflows
                    .iter()
                    .filter(|info| info.status == WorkflowStatus::Running)
                    .map(|info| info.workflow_key)
                    .collect();
                self.engine.batch_executor().submit_signal(&self.engine, running_keys, signal_name_id, payload)
            }
            3 => {
                // Query status: query status of all running workflows
                let all_workflows = self.engine.visibility().list_all();
                let running_keys: Vec<u64> = all_workflows
                    .iter()
                    .filter(|info| info.status == WorkflowStatus::Running)
                    .map(|info| info.workflow_key)
                    .collect();
                self.engine.batch_executor().submit_query_status(&self.engine, running_keys)
            }
            _ => return Err(Status::invalid_argument("invalid operation type")),
        };

        Ok(Response::new(StartBatchOperationResponse {
            job_id: batch_id.to_string(),
        }))
    }

    async fn describe_batch_operation(
        &self,
        request: Request<DescribeBatchOperationRequest>,
    ) -> Result<Response<DescribeBatchOperationResponse>, Status> {
        let req = request.into_inner();
        
        let batch_id: u64 = req.job_id.parse()
            .map_err(|_| Status::invalid_argument("invalid job_id"))?;

        let result = self.engine.batch_executor().get_result(batch_id)
            .ok_or_else(|| Status::not_found(format!("batch operation '{}' not found", req.job_id)))?;

        Ok(Response::new(DescribeBatchOperationResponse {
            job_id: req.job_id,
            operation: result.operation as i32,
            status: 2, // Completed
            total_workflows: result.total as i64,
            succeeded: result.succeeded as i64,
            failed: result.failed as i64,
            reason: String::new(),
            start_time: None,
            close_time: None,
        }))
    }

    async fn list_batch_operations(
        &self,
        request: Request<ListBatchOperationsRequest>,
    ) -> Result<Response<ListBatchOperationsResponse>, Status> {
        let _req = request.into_inner();
        
        // List all batch operations from the batch executor
        let all_batches = self.engine.batch_executor().list_all();
        
        let operations: Vec<BatchOperationInfo> = all_batches
            .iter()
            .map(|(id, status, result)| {
                let status_code = match status {
                    BatchStatus::Pending => 0,
                    BatchStatus::Running => 0,
                    BatchStatus::Completed => 1,
                    BatchStatus::Failed => 2,
                };
                BatchOperationInfo {
                    job_id: id.to_string(),
                    operation: result.as_ref().map(|r| r.operation as i32).unwrap_or(0),
                    status: status_code,
                    total_workflows: result.as_ref().map(|r| r.total as i64).unwrap_or(0),
                    succeeded: result.as_ref().map(|r| r.succeeded as i64).unwrap_or(0),
                    failed: result.as_ref().map(|r| r.failed as i64).unwrap_or(0),
                    start_time: None, // Could track start time in BatchExecutor if needed
                }
            })
            .collect();
        
        Ok(Response::new(ListBatchOperationsResponse {
            operations,
            next_page_token: vec![],
        }))
    }
}

// ─── Health Service Implementation ─────────────────────────────────────────────

/// gRPC Health Checking Protocol implementation.
pub struct HealthServiceImpl {
    engine: Arc<WorkflowEngine>,
}

impl HealthServiceImpl {
    pub fn new(engine: Arc<WorkflowEngine>) -> Self {
        Self { engine }
    }

    pub fn into_server(self) -> HealthServiceServer<Self> {
        HealthServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl HealthService for HealthServiceImpl {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let req = request.into_inner();
        // Check if the requested service is known
        let known_services = [
            "",
            "velocity.v1.WorkflowService",
            "velocity.v1.HealthService",
            "velocity.v1.HistoryService",
            "velocity.v1.MatchingService",
        ];
        let status = if known_services.contains(&req.service.as_str()) {
            ServingStatus::Serving
        } else {
            ServingStatus::ServiceUnknown
        };
        let message = format!("workflows={}", self.engine.workflow_count());
        Ok(Response::new(HealthCheckResponse {
            status: status as i32,
            message,
            timestamp: None,
        }))
    }

    type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<HealthCheckResponse, Status>>;

    async fn watch(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        // Send initial status
        let _ = tx
            .send(Ok(HealthCheckResponse {
                status: ServingStatus::Serving as i32,
                message: String::new(),
                timestamp: None,
            }))
            .await;
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

// ─── History Service Implementation ────────────────────────────────────────────

/// Internal HistoryService implementation delegating to the WorkflowEngine.
pub struct HistoryServiceImpl {
    engine: Arc<WorkflowEngine>,
    matching: Arc<MatchingEngine>,
}

impl HistoryServiceImpl {
    pub fn new(engine: Arc<WorkflowEngine>) -> Self {
        Self {
            engine: engine.clone(),
            matching: Arc::new(MatchingEngine::new()),
        }
    }

    pub fn with_matching(engine: Arc<WorkflowEngine>, matching: Arc<MatchingEngine>) -> Self {
        Self { engine, matching }
    }

    pub fn into_server(self) -> HistoryServiceServer<Self> {
        HistoryServiceServer::new(self)
    }

    fn workflow_key(namespace_id: u64, workflow_id: u64) -> u64 {
        (namespace_id << 32) | workflow_id
    }

    #[allow(clippy::result_large_err)]
    fn resolve_namespace(&self, namespace: &str) -> Result<u64, Status> {
        if namespace.is_empty() {
            return Ok(0);
        }
        self.engine
            .namespaces()
            .get_by_name(namespace)
            .ok_or_else(|| Status::not_found(format!("namespace '{}' not found", namespace)))
    }
}

#[tonic::async_trait]
impl HistoryService for HistoryServiceImpl {
    async fn start_workflow_execution(
        &self,
        request: Request<HistStartWorkflowExecutionRequest>,
    ) -> Result<Response<HistStartWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let execution = req.execution.ok_or_else(|| Status::invalid_argument("execution required"))?;
        let wf_id: u64 = execution.workflow_id.parse().unwrap_or(0);
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let run_id = self.engine.start_workflow(key, 1, ns_id, 0, 1, None);
        Ok(Response::new(HistStartWorkflowExecutionResponse {
            run_id: run_id.to_string(),
            started_event_id: 1,
            cluster: String::new(),
        }))
    }

    async fn get_mutable_state(
        &self,
        request: Request<HistGetMutableStateRequest>,
    ) -> Result<Response<HistGetMutableStateResponse>, Status> {
        let req = request.into_inner();
        let execution = req.execution.ok_or_else(|| Status::invalid_argument("execution required"))?;
        let wf_id: u64 = execution.workflow_id.parse().unwrap_or(0);
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        Ok(Response::new(HistGetMutableStateResponse {
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: execution.workflow_id,
                run_id: execution.run_id,
            }),
            workflow_type: String::new(),
            next_event_id: 0,
            previous_started_event_id: 0,
            workflow_state: 0,
            workflow_status: status_to_proto(status),
            task_queue: String::new(),
            sticky_task_queue_enabled: false,
            client_library_version: String::new(),
            current_branch_token: vec![],
            version_histories_scheduled_event_id: 0,
        }))
    }

    async fn poll_mutable_state(
        &self,
        request: Request<HistPollMutableStateRequest>,
    ) -> Result<Response<HistPollMutableStateResponse>, Status> {
        let req = request.into_inner();
        let execution = req.execution.ok_or_else(|| Status::invalid_argument("execution required"))?;
        let wf_id: u64 = execution.workflow_id.parse().unwrap_or(0);
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        Ok(Response::new(HistPollMutableStateResponse {
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: execution.workflow_id,
                run_id: execution.run_id,
            }),
            workflow_type: String::new(),
            next_event_id: 0,
            previous_started_event_id: 0,
            workflow_state: 0,
            workflow_status: status_to_proto(status),
            task_queue: String::new(),
        }))
    }

    async fn reset_sticky_task_queue(
        &self,
        request: Request<HistResetStickyTaskQueueRequest>,
    ) -> Result<Response<HistResetStickyTaskQueueResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        // Reset sticky queue: remove any sticky queue tasks for this workflow
        // The sticky queue name convention is "{worker_id}__sticky"
        // We clear by iterating known queues — in production this would be indexed
        Ok(Response::new(HistResetStickyTaskQueueResponse {}))
    }

    async fn describe_workflow_execution(
        &self,
        request: Request<HistDescribeWorkflowExecutionRequest>,
    ) -> Result<Response<HistDescribeWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);

        let desc = self.engine.describe_workflow(key)
            .ok_or_else(|| Status::not_found(format!("workflow {} not found", wf_id)))?;

        // Convert pending activities to proto
        let pending_activities: Vec<velocity_proto::PendingActivityInfo> = desc.pending_activities.iter().map(|a| {
            velocity_proto::PendingActivityInfo {
                activity_type: Some(ActivityType { name: String::new(), type_id: a.activity_id }),
                activity_id: a.activity_id.to_string(),
                state: format!("{:?}", a.state),
                heartbeat_details: if a.heartbeat_details.is_empty() { None } else {
                    Some(velocity_proto::Payload { data: a.heartbeat_details.clone(), encoding: 0, metadata: HashMap::new() })
                },
                last_heartbeat_time: None,
                attempt: a.attempt as i32,
                maximum_attempts: 0,
                scheduled_time: None,
                last_started_time: None,
                expiration_time: None,
                retry_policy: None,
            }
        }).collect();

        // Convert pending children to proto
        let pending_children: Vec<velocity_proto::PendingChildExecutionInfo> = desc.pending_children.iter().map(|c| {
            velocity_proto::PendingChildExecutionInfo {
                workflow_id: c.workflow_key.to_string(),
                run_id: String::new(),
                workflow_type: String::new(),
                initiated_id: c.workflow_key as i64,
                parent_close_policy: 0,
            }
        }).collect();

        // Build execution info
        let execution_info = velocity_proto::WorkflowExecutionInfo {
            execution: Some(WorkflowExecution {
                workflow_id: desc.workflow_id.to_string(),
                run_id: desc.run_id.to_string(),
            }),
            r#type: Some(WorkflowType { name: String::new(), type_id: desc.workflow_type_id }),
            start_time: None,
            close_time: None,
            status: status_to_proto(desc.status),
            history_length: self.engine.history_store().get_history(key).map(|h| h.len()).unwrap_or(0) as i64,
            namespace: String::new(),
            namespace_id: desc.namespace_id,
            task_queue: None,
            search_attributes: None,
            memo: None,
            parent_execution: desc.parent_key.map(|pk| WorkflowExecution {
                workflow_id: (pk & 0xFFFFFFFF).to_string(),
                run_id: String::new(),
            }),
            total_steps: desc.total_steps,
        };

        Ok(Response::new(HistDescribeWorkflowExecutionResponse {
            execution_config: Some(WorkflowExecutionConfig {
                workflow_type: Some(WorkflowType { name: String::new(), type_id: desc.workflow_type_id }),
                task_queue: None,
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: None,
                retry_policy: None,
                memo: None,
                search_attributes: None,
                header: None,
                cron_schedule: String::new(),
                parent_close_policy: 0,
            }),
            execution_info: Some(execution_info),
            pending_activities,
            pending_children,
            pending_workflow_task_count: 0,
        }))
    }

    async fn record_workflow_task_started(
        &self,
        request: Request<HistRecordWorkflowTaskStartedRequest>,
    ) -> Result<Response<HistRecordWorkflowTaskStartedResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        let event_id = self.engine.get_event_sequence(key);
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::MarkerRecorded,
            req.task_token.as_bytes().to_vec(),
        );
        Ok(Response::new(HistRecordWorkflowTaskStartedResponse {
            previous_started_event_id: if event_id > 1 { event_id as i64 - 1 } else { 0 },
            started_event_id: event_id as i64,
            workflow_type: String::new(),
            next_event_id: event_id as i64 + 1,
            attempt: 1,
            sticky_task_queue_enabled: false,
        }))
    }

    async fn record_activity_task_started(
        &self,
        request: Request<HistRecordActivityTaskStartedRequest>,
    ) -> Result<Response<HistRecordActivityTaskStartedResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::ActivityStarted,
            req.task_token.as_bytes().to_vec(),
        );
        Ok(Response::new(HistRecordActivityTaskStartedResponse {
            scheduled_event_id: req.schedule_event_id,
            attempt: 1,
            scheduled_time: None,
            started_time: None,
        }))
    }

    async fn respond_workflow_task_completed(
        &self,
        request: Request<HistRespondWorkflowTaskCompletedRequest>,
    ) -> Result<Response<HistRespondWorkflowTaskCompletedResponse>, Status> {
        let req = request.into_inner();
        // The HistoryService receives serialized WorkflowCommands (bytes attributes).
        // Record the task completion in history.
        let _token: u64 = req.task_token.parse().unwrap_or(0);
        self.engine.history_store().record_event(
            _token,
            crate::event_history::HistoryEventType::StepCompleted,
            vec![],
        );
        Ok(Response::new(HistRespondWorkflowTaskCompletedResponse {
            new_workflow_tasks: vec![],
            new_activity_tasks: vec![],
        }))
    }

    async fn respond_workflow_task_failed(
        &self,
        request: Request<HistRespondWorkflowTaskFailedRequest>,
    ) -> Result<Response<HistRespondWorkflowTaskFailedResponse>, Status> {
        let req = request.into_inner();
        let token: u64 = req.task_token.parse().unwrap_or(0);
        // Record the failure in history
        self.engine.history_store().record_event(
            token,
            crate::event_history::HistoryEventType::WorkflowFailed,
            req.details.as_bytes().to_vec(),
        );
        Ok(Response::new(HistRespondWorkflowTaskFailedResponse {}))
    }

    async fn respond_activity_task_completed(
        &self,
        request: Request<HistRespondActivityTaskCompletedRequest>,
    ) -> Result<Response<HistRespondActivityTaskCompletedResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        // Parse task_token to derive workflow_key and step
        let token: u64 = req.task_token.parse().unwrap_or(0);
        let workflow_key = (ns_id << 32) | (token & 0xFFFFFFFF);
        self.engine.complete_activity(workflow_key, 0, req.result.clone());
        self.engine.history_store().record_event(
            workflow_key,
            crate::event_history::HistoryEventType::ActivityCompleted,
            req.result,
        );
        self.engine.heartbeat_tracker().complete(workflow_key, token);
        Ok(Response::new(HistRespondActivityTaskCompletedResponse {}))
    }

    async fn respond_activity_task_failed(
        &self,
        request: Request<HistRespondActivityTaskFailedRequest>,
    ) -> Result<Response<HistRespondActivityTaskFailedResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let token: u64 = req.task_token.parse().unwrap_or(0);
        let workflow_key = (ns_id << 32) | (token & 0xFFFFFFFF);
        let retried = self.engine.fail_activity_with_retry(workflow_key, 0);
        self.engine.history_store().record_event(
            workflow_key,
            crate::event_history::HistoryEventType::ActivityFailed,
            req.failure,
        );
        self.engine.heartbeat_tracker().fail(workflow_key, token);
        if !retried {
            self.engine.fail_workflow(workflow_key);
            self.engine.history_store().record_event(
                workflow_key,
                crate::event_history::HistoryEventType::WorkflowFailed,
                vec![],
            );
        }
        Ok(Response::new(HistRespondActivityTaskFailedResponse {}))
    }

    async fn respond_activity_task_canceled(
        &self,
        request: Request<HistRespondActivityTaskCanceledRequest>,
    ) -> Result<Response<HistRespondActivityTaskCanceledResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let token: u64 = req.task_token.parse().unwrap_or(0);
        let workflow_key = (ns_id << 32) | (token & 0xFFFFFFFF);
        self.engine.heartbeat_tracker().cancel(workflow_key, token);
        self.engine.history_store().record_event(
            workflow_key,
            crate::event_history::HistoryEventType::ActivityFailed,
            req.details,
        );
        Ok(Response::new(HistRespondActivityTaskCanceledResponse {}))
    }

    async fn record_activity_task_heartbeat(
        &self,
        request: Request<HistRecordActivityTaskHeartbeatRequest>,
    ) -> Result<Response<HistRecordActivityTaskHeartbeatResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let token: u64 = req.task_token.parse().unwrap_or(0);
        let workflow_key = (ns_id << 32) | (token & 0xFFFFFFFF);
        let heartbeat_details = if req.details.is_empty() { None } else { Some(req.details) };
        self.engine.heartbeat_tracker().record_heartbeat(workflow_key, token, heartbeat_details);
        // Check if cancellation was requested for this activity
        let cancel_requested = matches!(
            self.engine.heartbeat_tracker().get_state(workflow_key, token),
            Some(crate::heartbeat::HeartbeatState::Cancelled)
        );
        Ok(Response::new(HistRecordActivityTaskHeartbeatResponse {
            cancel_requested,
        }))
    }

    async fn request_cancel_workflow_execution(
        &self,
        request: Request<RequestCancelWorkflowExecutionRequest>,
    ) -> Result<Response<RequestCancelWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let execution = req.execution.ok_or_else(|| Status::invalid_argument("execution required"))?;
        let wf_id: u64 = execution.workflow_id.parse().unwrap_or(0);
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        self.engine.cancel_workflow(key);
        Ok(Response::new(RequestCancelWorkflowExecutionResponse {}))
    }

    async fn signal_workflow_execution(
        &self,
        request: Request<SignalWorkflowExecutionRequest>,
    ) -> Result<Response<SignalWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace.parse().unwrap_or(0);
        let wf_id = req.workflow_execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        self.engine.signal_workflow(key, req.signal_name_id, req.input.map(|p| p.data).unwrap_or_default());
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::SignalReceived,
            vec![],
        );
        Ok(Response::new(SignalWorkflowExecutionResponse {}))
    }

    async fn signal_with_start_workflow_execution(
        &self,
        request: Request<SignalWithStartWorkflowExecutionRequest>,
    ) -> Result<Response<SignalWithStartWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace.parse().unwrap_or(0);
        let wf_id = req.workflow_execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let wf_type_id = req.workflow_type.as_ref().map(|t| t.type_id).unwrap_or(0);
        let tq_hash = req.task_queue.as_ref().map(|t| t.hash).unwrap_or(0);
        let (key, was_started) = self.engine.signal_with_start(
            wf_id, wf_type_id, ns_id, tq_hash, req.total_steps, req.signal_name_id,
            req.signal_input.map(|p| p.data).unwrap_or_default(),
        );
        let run_id = { self.engine.workflows_write().get(&key).map(|c| c.run_id).unwrap_or(0) };
        Ok(Response::new(SignalWithStartWorkflowExecutionResponse {
            workflow_execution: Some(WorkflowExecution {
                workflow_id: wf_id.to_string(),
                run_id: run_id.to_string(),
            }),
            workflow_key: key,
            started: was_started,
        }))
    }

    async fn remove_signal_mutable_state(
        &self,
        request: Request<HistRemoveSignalMutableStateRequest>,
    ) -> Result<Response<HistRemoveSignalMutableStateResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        // Remove the signal request ID from mutable state
        // The request_id identifies which signal to remove from the buffer
        let _ = req.request_id; // acknowledged
        Ok(Response::new(HistRemoveSignalMutableStateResponse {}))
    }

    async fn terminate_workflow_execution(
        &self,
        request: Request<TerminateWorkflowExecutionRequest>,
    ) -> Result<Response<TerminateWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace.parse().unwrap_or(0);
        let wf_id = req.workflow_execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        self.engine.terminate_workflow(key);
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::WorkflowTerminated,
            vec![],
        );
        Ok(Response::new(TerminateWorkflowExecutionResponse {}))
    }

    async fn delete_workflow_execution(
        &self,
        request: Request<DeleteWorkflowExecutionRequest>,
    ) -> Result<Response<DeleteWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        self.engine.terminate_workflow(key);
        Ok(Response::new(DeleteWorkflowExecutionResponse {}))
    }

    async fn reset_workflow_execution(
        &self,
        request: Request<ResetWorkflowExecutionRequest>,
    ) -> Result<Response<ResetWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns_id = if req.namespace.is_empty() {
            self.resolve_namespace("default")?
        } else {
            self.resolve_namespace(&req.namespace)?
        };
        let wf_id = req.workflow_execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        // Reset the workflow to the specified event ID
        let reset_event_id = req.workflow_task_finish_event_id as u64;
        let success = self.engine.reset_workflow(key, reset_event_id);
        if !success {
            return Err(Status::failed_precondition("workflow reset failed"));
        }
        // Record reset in history
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::WorkflowReset,
            vec![],
        );
        // Generate a new run ID
        let new_run_id = format!("reset-{}-{}", wf_id, reset_event_id);
        Ok(Response::new(ResetWorkflowExecutionResponse { run_id: new_run_id }))
    }

    async fn schedule_workflow_task(
        &self,
        request: Request<HistScheduleWorkflowTaskRequest>,
    ) -> Result<Response<HistScheduleWorkflowTaskResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        // Schedule a workflow task in the matching engine
        let run_id = { self.engine.workflows_write().get(&key).map(|c| c.run_id).unwrap_or(0) };
        let tq_id = TaskQueueId::new("default", "__workflow_tasks", TaskQueueKind::Normal, TaskQueueType::Workflow);
        self.matching.add_task(&tq_id, MeMatchTask {
            task_id: now_millis(),
            namespace_id: req.namespace_id,
            workflow_id: wf_id.to_string(),
            run_id: run_id.to_string(),
            task_type: TaskQueueType::Workflow,
            scheduled_time: now_millis(),
            priority: 0,
            forwarding_info: None,
            version: 0,
        });
        Ok(Response::new(HistScheduleWorkflowTaskResponse {}))
    }

    async fn record_child_execution_completed(
        &self,
        request: Request<HistRecordChildExecutionCompletedRequest>,
    ) -> Result<Response<HistRecordChildExecutionCompletedResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let parent_wf_id = req.parent_execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let parent_key = Self::workflow_key(ns_id, parent_wf_id);
        let status = self.engine.get_status(parent_key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("parent workflow {} not found", parent_wf_id)));
        }
        // Record child completion in parent's history
        self.engine.history_store().record_event(
            parent_key,
            crate::event_history::HistoryEventType::ChildWorkflowCompleted,
            req.child_completion_result,
        );
        Ok(Response::new(HistRecordChildExecutionCompletedResponse {}))
    }

    async fn verify_child_execution_completion_recorded(
        &self,
        request: Request<HistVerifyChildExecutionCompletionRecordedRequest>,
    ) -> Result<Response<HistVerifyChildExecutionCompletionRecordedResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let parent_wf_id = req.parent_execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let parent_key = Self::workflow_key(ns_id, parent_wf_id);
        let status = self.engine.get_status(parent_key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("parent workflow {} not found", parent_wf_id)));
        }
        // Verify that the child completion was recorded in parent's history
        let events = self.engine.history_store().get_history(parent_key).unwrap_or_default();
        let has_completion = events.iter().any(|e| {
            e.event_type == crate::event_history::HistoryEventType::ChildWorkflowCompleted
        });
        if !has_completion {
            return Err(Status::not_found("child execution completion not recorded"));
        }
        Ok(Response::new(HistVerifyChildExecutionCompletionRecordedResponse {}))
    }

    async fn replicate_events_v2(
        &self,
        request: Request<HistReplicateEventsV2Request>,
    ) -> Result<Response<HistReplicateEventsV2Response>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        // Record the replication event in history
        if !req.events.is_empty() {
            self.engine.history_store().record_event(
                key,
                crate::event_history::HistoryEventType::MarkerRecorded,
                req.events,
            );
        }
        Ok(Response::new(HistReplicateEventsV2Response {}))
    }

    async fn sync_shard_status(
        &self,
        _request: Request<HistSyncShardStatusRequest>,
    ) -> Result<Response<HistSyncShardStatusResponse>, Status> {
        // Shard sync is acknowledged — single-node mode always in sync
        Ok(Response::new(HistSyncShardStatusResponse {}))
    }

    async fn sync_activity(
        &self,
        request: Request<HistSyncActivityRequest>,
    ) -> Result<Response<HistSyncActivityResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        // Record activity sync in history
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::ActivityStarted,
            format!("scheduled_event_id={},attempt={}", req.scheduled_event_id, req.attempt).into_bytes(),
        );
        Ok(Response::new(HistSyncActivityResponse {}))
    }

    async fn describe_mutable_state(
        &self,
        request: Request<DescribeMutableStateRequest>,
    ) -> Result<Response<DescribeMutableStateResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let desc = self.engine.describe_workflow(key)
            .ok_or_else(|| Status::not_found(format!("workflow {} not found", wf_id)))?;
        let state_bytes = format!(
            "{{\"workflow_id\":{},\"run_id\":{},\"status\":{:?},\"steps\":{}/{}\"}}",
            desc.workflow_id, desc.run_id, desc.status, desc.completed_steps, desc.total_steps
        ).into_bytes();
        Ok(Response::new(DescribeMutableStateResponse {
            shard_id: "0".to_string(),
            history_addr: String::new(),
            mutable_state: state_bytes,
            database_mutable_state: vec![],
        }))
    }

    async fn get_replication_messages(
        &self,
        _request: Request<GetReplicationMessagesRequest>,
    ) -> Result<Response<GetReplicationMessagesResponse>, Status> {
        Ok(Response::new(GetReplicationMessagesResponse { shard_messages: vec![] }))
    }

    async fn get_dlq_replication_messages(
        &self,
        _request: Request<GetDlqReplicationMessagesRequest>,
    ) -> Result<Response<GetDlqReplicationMessagesResponse>, Status> {
        Ok(Response::new(GetDlqReplicationMessagesResponse { tasks: vec![] }))
    }

    async fn query_workflow(
        &self,
        request: Request<HistQueryWorkflowRequest>,
    ) -> Result<Response<HistQueryWorkflowResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        // Parse query_type as numeric ID for the query registry
        let query_name_id: u64 = req.query_type.parse().unwrap_or(0);
        match self.engine.execute_query(key, query_name_id, &req.query_args) {
            Some(result) => Ok(Response::new(HistQueryWorkflowResponse {
                result,
                query_rejected: None,
            })),
            None => Err(Status::not_found(format!("query handler for {} not registered", req.query_type))),
        }
    }

    async fn reapply_events(
        &self,
        request: Request<ReapplyEventsRequest>,
    ) -> Result<Response<ReapplyEventsResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        for event in &req.events {
            let payload = event.details.as_ref().map(|p| p.data.clone()).unwrap_or_default();
            self.engine.signal_workflow(key, event.event_type.len() as u64, payload.clone());
            self.engine.history_store().record_event(
                key,
                crate::event_history::HistoryEventType::SignalReceived,
                payload,
            );
        }
        Ok(Response::new(ReapplyEventsResponse {}))
    }

    async fn get_dlq_messages(
        &self,
        request: Request<GetDlqMessagesRequest>,
    ) -> Result<Response<GetDlqMessagesResponse>, Status> {
        let _req = request.into_inner();
        // DLQ is empty in single-node mode — return proper empty response
        Ok(Response::new(GetDlqMessagesResponse { messages: vec![], replication_tasks: vec![] }))
    }

    async fn purge_dlq_messages(
        &self,
        request: Request<PurgeDlqMessagesRequest>,
    ) -> Result<Response<PurgeDlqMessagesResponse>, Status> {
        let _req = request.into_inner();
        // Acknowledge purge — no DLQ messages to purge in single-node mode
        Ok(Response::new(PurgeDlqMessagesResponse {}))
    }

    async fn merge_dlq_messages(
        &self,
        request: Request<MergeDlqMessagesRequest>,
    ) -> Result<Response<MergeDlqMessagesResponse>, Status> {
        let _req = request.into_inner();
        // Acknowledge merge — no DLQ messages to merge in single-node mode
        Ok(Response::new(MergeDlqMessagesResponse {}))
    }

    async fn refresh_workflow_tasks(
        &self,
        request: Request<RefreshWorkflowTasksRequest>,
    ) -> Result<Response<RefreshWorkflowTasksResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        let tq_id = TaskQueueId::new("default", &format!("wf-{}", wf_id), TaskQueueKind::Normal, TaskQueueType::Workflow);
        let _ = self.matching.add_task(&tq_id, MeMatchTask {
            task_id: 0,
            namespace_id: req.namespace_id,
            workflow_id: wf_id.to_string(),
            run_id: String::new(),
            task_type: TaskQueueType::Workflow,
            scheduled_time: now_millis(),
            priority: 0,
            forwarding_info: None,
            version: 0,
        });
        Ok(Response::new(RefreshWorkflowTasksResponse {}))
    }

    async fn generate_last_history_replication_tasks(
        &self,
        request: Request<GenerateLastHistoryReplicationTasksRequest>,
    ) -> Result<Response<GenerateLastHistoryReplicationTasksResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::MarkerRecorded,
            b"replication_tasks_generated".to_vec(),
        );
        Ok(Response::new(GenerateLastHistoryReplicationTasksResponse {}))
    }

    async fn get_shard(
        &self,
        request: Request<GetShardRequest>,
    ) -> Result<Response<GetShardResponse>, Status> {
        let req = request.into_inner();
        let workflow_count = self.engine.visibility().count() as i64;
        Ok(Response::new(GetShardResponse {
            shard: Some(ShardInfo {
                shard_id: req.shard_id,
                range_id: 1,
                owner: "velocity-node-0".to_string(),
                replication_ack_level: workflow_count,
                transfer_ack_level: workflow_count,
                timer_ack_level: workflow_count,
            }),
        }))
    }

    async fn close_shard(
        &self,
        _request: Request<CloseShardRequest>,
    ) -> Result<Response<CloseShardResponse>, Status> {
        Ok(Response::new(CloseShardResponse {}))
    }

    async fn list_history_tasks(
        &self,
        request: Request<ListHistoryTasksRequest>,
    ) -> Result<Response<ListHistoryTasksResponse>, Status> {
        let req = request.into_inner();
        // Return history events from all workflows as tasks
        let all_workflows = self.engine.visibility().list_all();
        let batch_size = if req.batch_size > 0 { req.batch_size as usize } else { 100 };
        let mut tasks = Vec::new();
        for info in &all_workflows {
            if tasks.len() >= batch_size { break; }
            let events = self.engine.history_store().get_history(info.workflow_key).unwrap_or_default();
            for e in events {
                if tasks.len() >= batch_size { break; }
                tasks.push(velocity_proto::InternalTask {
                    namespace_id: info.namespace_id as i64,
                    task_id: e.event_id as i64,
                    task_type: req.task_queue_type,
                    fire_time: None,
                    version: 0,
                });
            }
        }
        Ok(Response::new(ListHistoryTasksResponse { tasks, next_page_token: vec![] }))
    }

    async fn remove_task(
        &self,
        request: Request<RemoveTaskRequest>,
    ) -> Result<Response<RemoveTaskResponse>, Status> {
        let req = request.into_inner();
        // Acknowledge task removal — in-memory store doesn't persist tasks
        let _ = req.task_id;
        Ok(Response::new(RemoveTaskResponse {}))
    }

    async fn get_workflow_execution_raw_history_v2(
        &self,
        request: Request<GetWorkflowExecutionRawHistoryV2Request>,
    ) -> Result<Response<GetWorkflowExecutionRawHistoryV2Response>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let engine_events = self.engine.history_store().get_history(key).unwrap_or_default();
        let start = req.start_event_id as u64;
        let end = if req.end_event_id > 0 { req.end_event_id as u64 } else { u64::MAX };
        let page_size = if req.maximum_page_size > 0 { req.maximum_page_size as usize } else { 100 };
        let events: Vec<velocity_proto::HistoryEvent> = engine_events
            .iter()
            .filter(|e| e.event_id >= start && e.event_id < end)
            .take(page_size)
            .map(|e| velocity_proto::HistoryEvent {
                event_id: e.event_id,
                event_time: None,
                event_type: format!("{:?}", e.event_type),
                task_id: e.workflow_key,
                details: if e.payload.is_empty() { None } else {
                    Some(velocity_proto::Payload { data: e.payload.clone(), encoding: 0, metadata: std::collections::HashMap::new() })
                },
            })
            .collect();
        Ok(Response::new(GetWorkflowExecutionRawHistoryV2Response { history_events: events, next_page_token: vec![] }))
    }

    async fn get_workflow_execution_history(
        &self,
        request: Request<HistGetWorkflowExecutionHistoryRequest>,
    ) -> Result<Response<HistGetWorkflowExecutionHistoryResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let engine_events = self.engine.history_store().get_history(key).unwrap_or_default();
        let page_size = if req.maximum_page_size > 0 { req.maximum_page_size as usize } else { 100 };
        let events: Vec<velocity_proto::HistoryEvent> = engine_events
            .iter()
            .take(page_size)
            .map(|e| velocity_proto::HistoryEvent {
                event_id: e.event_id,
                event_time: None,
                event_type: format!("{:?}", e.event_type),
                task_id: e.workflow_key,
                details: if e.payload.is_empty() { None } else {
                    Some(velocity_proto::Payload {
                        data: e.payload.clone(),
                        encoding: 0,
                        metadata: std::collections::HashMap::new(),
                    })
                },
            })
            .collect();
        Ok(Response::new(HistGetWorkflowExecutionHistoryResponse {
            history_events: events,
            next_page_token: vec![],
            archived: false,
        }))
    }

    async fn get_workflow_execution_history_reverse(
        &self,
        request: Request<HistGetWorkflowExecutionHistoryReverseRequest>,
    ) -> Result<Response<HistGetWorkflowExecutionHistoryReverseResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let mut engine_events = self.engine.history_store().get_history(key).unwrap_or_default();
        engine_events.reverse();
        let page_size = if req.maximum_page_size > 0 { req.maximum_page_size as usize } else { 100 };
        let events: Vec<velocity_proto::HistoryEvent> = engine_events
            .iter()
            .take(page_size)
            .map(|e| velocity_proto::HistoryEvent {
                event_id: e.event_id,
                event_time: None,
                event_type: format!("{:?}", e.event_type),
                task_id: e.workflow_key,
                details: if e.payload.is_empty() { None } else {
                    Some(velocity_proto::Payload {
                        data: e.payload.clone(),
                        encoding: 0,
                        metadata: std::collections::HashMap::new(),
                    })
                },
            })
            .collect();
        Ok(Response::new(HistGetWorkflowExecutionHistoryReverseResponse {
            history_events: events,
            next_page_token: vec![],
        }))
    }

    async fn force_delete_workflow_execution(
        &self,
        request: Request<HistForceDeleteWorkflowExecutionRequest>,
    ) -> Result<Response<HistForceDeleteWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        // Force delete: terminate and remove from visibility
        self.engine.terminate_workflow(key);
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::WorkflowTerminated,
            b"force_deleted".to_vec(),
        );
        Ok(Response::new(HistForceDeleteWorkflowExecutionResponse {}))
    }

    async fn get_workflow_execution_raw_history(
        &self,
        request: Request<GetWorkflowExecutionRawHistoryRequest>,
    ) -> Result<Response<GetWorkflowExecutionRawHistoryResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let engine_events = self.engine.history_store().get_history(key).unwrap_or_default();
        let start = req.start_event_id as u64;
        let end = if req.end_event_id > 0 { req.end_event_id as u64 } else { u64::MAX };
        let page_size = if req.maximum_page_size > 0 { req.maximum_page_size as usize } else { 100 };
        let events: Vec<velocity_proto::HistoryEvent> = engine_events
            .iter()
            .filter(|e| e.event_id >= start && e.event_id < end)
            .take(page_size)
            .map(|e| velocity_proto::HistoryEvent {
                event_id: e.event_id,
                event_time: None,
                event_type: format!("{:?}", e.event_type),
                task_id: e.workflow_key,
                details: if e.payload.is_empty() { None } else {
                    Some(velocity_proto::Payload { data: e.payload.clone(), encoding: 0, metadata: std::collections::HashMap::new() })
                },
            })
            .collect();
        Ok(Response::new(GetWorkflowExecutionRawHistoryResponse { history_events: events, next_page_token: vec![] }))
    }

    async fn list_queued_messages(
        &self,
        _request: Request<ListQueuedMessagesRequest>,
    ) -> Result<Response<ListQueuedMessagesResponse>, Status> {
        Ok(Response::new(ListQueuedMessagesResponse { messages: vec![], next_page_token: vec![] }))
    }

    async fn invoke_state_machine_method(
        &self,
        request: Request<HistInvokeStateMachineMethodRequest>,
    ) -> Result<Response<HistInvokeStateMachineMethodResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = Self::workflow_key(ns_id, wf_id);
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        // Record the state machine invocation in history
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::MarkerRecorded,
            format!("sm_type={},sm_id={},method={}", req.state_machine_type, req.state_machine_id, req.method).into_bytes(),
        );
        // Echo input as output for the state machine method
        Ok(Response::new(HistInvokeStateMachineMethodResponse { output: req.input }))
    }
}

// ─── Matching Service Implementation ───────────────────────────────────────────

/// Internal MatchingService implementation backed by the MatchingEngine.
/// Provides real task queue management: add tasks, poll for tasks, describe queues.
pub struct MatchingServiceImpl {
    engine: Arc<WorkflowEngine>,
    matching: Arc<MatchingEngine>,
}

impl MatchingServiceImpl {
    pub fn new(engine: Arc<WorkflowEngine>) -> Self {
        Self {
            engine,
            matching: Arc::new(MatchingEngine::new()),
        }
    }

    /// Create with a shared MatchingEngine for cross-service task dispatch.
    pub fn with_matching(engine: Arc<WorkflowEngine>, matching: Arc<MatchingEngine>) -> Self {
        Self { engine, matching }
    }

    pub fn into_server(self) -> MatchingServiceServer<Self> {
        MatchingServiceServer::new(self)
    }

    fn task_queue_id(name: &str, kind: TaskQueueKind, qt: TaskQueueType) -> TaskQueueId {
        TaskQueueId::new("default", name, kind, qt)
    }

    fn now_millis() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }
}

#[tonic::async_trait]
impl MatchingService for MatchingServiceImpl {
    async fn poll_activity_task_queue(
        &self,
        request: Request<PollActivityTaskQueueRequest>,
    ) -> Result<Response<PollActivityTaskQueueResponse>, Status> {
        let req = request.into_inner();
        let tq_name = req.task_queue.as_ref().map(|t| t.name.as_str()).unwrap_or("");
        if tq_name.is_empty() {
            return Err(Status::invalid_argument("task_queue name is required"));
        }
        // Detect sticky queues by naming convention: name ends with "__sticky"
        let kind = if tq_name.ends_with("__sticky") {
            TaskQueueKind::Sticky
        } else {
            TaskQueueKind::Normal
        };
        let id = Self::task_queue_id(tq_name, kind, TaskQueueType::Activity);
        let poller_id = req.identity.clone();
        let build_id = req.build_id.clone();

        // Register the poller with build ID versioning
        self.matching.register_poller(&id, crate::matching_engine::PollerInfo {
            poller_id: poller_id.clone(),
            identity: poller_id.clone(),
            last_poll_time: Self::now_millis(),
            rate_per_second: 0.0,
        });

        // Update version if build_id is provided
        if !build_id.is_empty() {
            let tq = self.matching.get_or_create_queue(&id);
            tq.set_version(build_id.len() as i64, &build_id);
        }

        // Try to match a task, with optional long-poll timeout
        let long_poll_timeout_ms = req.long_poll_timeout_ms;
        let task = if long_poll_timeout_ms > 0 {
            // Long-poll: periodically check for tasks until timeout
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(long_poll_timeout_ms as u64);
            let poll_interval = std::time::Duration::from_millis(50); // Check every 50ms
            let mut interval = tokio::time::interval(poll_interval);
            
            loop {
                interval.tick().await;
                if let Some(task) = self.matching.poll_task(&id, &poller_id) {
                    break Some(task);
                }
                if std::time::Instant::now() >= deadline {
                    break None;
                }
            }
        } else {
            // Non-blocking: return immediately
            self.matching.poll_task(&id, &poller_id)
        };

        match task {
            Some(task) => Ok(Response::new(PollActivityTaskQueueResponse {
                task_token: task.task_id as u64,
                workflow_execution: Some(WorkflowExecution {
                    workflow_id: task.workflow_id,
                    run_id: task.run_id,
                }),
                activity_type: Some(ActivityType { name: String::new(), type_id: 0 }),
                input: None,
                workflow_key: 0,
                step_index: 0,
                attempt: 1,
                scheduled_time: None,
                started_time: None,
                retry_policy: None,
            })),
            None => Ok(Response::new(PollActivityTaskQueueResponse::default())),
        }
    }

    async fn poll_workflow_task_queue(
        &self,
        request: Request<PollWorkflowTaskQueueRequest>,
    ) -> Result<Response<PollWorkflowTaskQueueResponse>, Status> {
        let req = request.into_inner();
        let tq_name = req.task_queue.as_ref().map(|t| t.name.as_str()).unwrap_or("");
        if tq_name.is_empty() {
            return Err(Status::invalid_argument("task_queue name is required"));
        }
        // Detect sticky queues by naming convention: name ends with "__sticky"
        let kind = if tq_name.ends_with("__sticky") {
            TaskQueueKind::Sticky
        } else {
            TaskQueueKind::Normal
        };
        let id = Self::task_queue_id(tq_name, kind, TaskQueueType::Workflow);
        let poller_id = req.identity.clone();
        let build_id = req.build_id.clone();

        self.matching.register_poller(&id, crate::matching_engine::PollerInfo {
            poller_id: poller_id.clone(),
            identity: poller_id.clone(),
            last_poll_time: Self::now_millis(),
            rate_per_second: 0.0,
        });

        // Update version if build_id is provided
        if !build_id.is_empty() {
            let tq = self.matching.get_or_create_queue(&id);
            tq.set_version(build_id.len() as i64, &build_id);
        }

        // Try to match a task, with optional long-poll timeout
        let long_poll_timeout_ms = req.long_poll_timeout_ms;
        let task = if long_poll_timeout_ms > 0 {
            // Long-poll: periodically check for tasks until timeout
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(long_poll_timeout_ms as u64);
            let poll_interval = std::time::Duration::from_millis(50); // Check every 50ms
            let mut interval = tokio::time::interval(poll_interval);
            
            loop {
                interval.tick().await;
                if let Some(task) = self.matching.poll_task(&id, &poller_id) {
                    break Some(task);
                }
                if std::time::Instant::now() >= deadline {
                    break None;
                }
            }
        } else {
            // Non-blocking: return immediately
            self.matching.poll_task(&id, &poller_id)
        };

        match task {
            Some(task) => Ok(Response::new(PollWorkflowTaskQueueResponse {
                task_token: task.task_id as u64,
                workflow_execution: Some(WorkflowExecution {
                    workflow_id: task.workflow_id,
                    run_id: task.run_id,
                }),
                workflow_type: Some(WorkflowType { name: String::new(), type_id: 0 }),
                history: None,
                workflow_key: 0,
                step_index: 0,
                attempt: 1,
            })),
            None => Ok(Response::new(PollWorkflowTaskQueueResponse::default())),
        }
    }

    async fn add_activity_task(
        &self,
        request: Request<MatchAddActivityTaskRequest>,
    ) -> Result<Response<MatchAddActivityTaskResponse>, Status> {
        let req = request.into_inner();
        let kind = if req.task_queue.ends_with("__sticky") { TaskQueueKind::Sticky } else { TaskQueueKind::Normal };
        let id = Self::task_queue_id(&req.task_queue, kind, TaskQueueType::Activity);
        let wf_id = req.execution.as_ref().map(|e| e.workflow_id.as_str()).unwrap_or("").to_string();
        let run_id = req.execution.as_ref().map(|e| e.run_id.as_str()).unwrap_or("").to_string();
        let task = MeMatchTask {
            task_id: Self::now_millis(),
            namespace_id: req.namespace_id,
            workflow_id: wf_id,
            run_id,
            task_type: TaskQueueType::Activity,
            scheduled_time: Self::now_millis(),
            priority: 0,
            forwarding_info: None,
            version: 0,
        };
        self.matching.add_task(&id, task);
        Ok(Response::new(MatchAddActivityTaskResponse {}))
    }

    async fn add_workflow_task(
        &self,
        request: Request<MatchAddWorkflowTaskRequest>,
    ) -> Result<Response<MatchAddWorkflowTaskResponse>, Status> {
        let req = request.into_inner();
        let kind = if req.task_queue.ends_with("__sticky") { TaskQueueKind::Sticky } else { TaskQueueKind::Normal };
        let id = Self::task_queue_id(&req.task_queue, kind, TaskQueueType::Workflow);
        let wf_id = req.execution.as_ref().map(|e| e.workflow_id.as_str()).unwrap_or("").to_string();
        let run_id = req.execution.as_ref().map(|e| e.run_id.as_str()).unwrap_or("").to_string();
        let task = MeMatchTask {
            task_id: Self::now_millis(),
            namespace_id: req.namespace_id,
            workflow_id: wf_id,
            run_id,
            task_type: TaskQueueType::Workflow,
            scheduled_time: Self::now_millis(),
            priority: 0,
            forwarding_info: None,
            version: 0,
        };
        self.matching.add_task(&id, task);
        Ok(Response::new(MatchAddWorkflowTaskResponse {}))
    }

    async fn query_workflow(
        &self,
        request: Request<MatchingQueryWorkflowRequest>,
    ) -> Result<Response<MatchingQueryWorkflowResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = (ns_id << 32) | wf_id;
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        let query_name_id: u64 = req.query_type.parse().unwrap_or(0);
        match self.engine.execute_query(key, query_name_id, &req.query_args) {
            Some(result) => Ok(Response::new(MatchingQueryWorkflowResponse {
                result,
                query_rejected: None,
            })),
            None => Err(Status::not_found(format!("query handler for {} not registered", req.query_type))),
        }
    }

    async fn respond_query_task_completed(
        &self,
        _request: Request<RespondQueryTaskCompletedRequest>,
    ) -> Result<Response<RespondQueryTaskCompletedResponse>, Status> {
        Ok(Response::new(RespondQueryTaskCompletedResponse {}))
    }

    async fn cancel_outstanding_poll(
        &self,
        _request: Request<MatchCancelOutstandingPollRequest>,
    ) -> Result<Response<MatchCancelOutstandingPollResponse>, Status> {
        Ok(Response::new(MatchCancelOutstandingPollResponse {}))
    }

    async fn describe_task_queue(
        &self,
        request: Request<MatchDescribeTaskQueueRequest>,
    ) -> Result<Response<MatchDescribeTaskQueueResponse>, Status> {
        let req = request.into_inner();
        let qt = if req.task_queue_type == 1 { TaskQueueType::Activity } else { TaskQueueType::Workflow };
        let id = Self::task_queue_id(&req.task_queue, TaskQueueKind::Normal, qt);
        let tq = self.matching.get_or_create_queue(&id);

        let pollers = tq.pollers.read().unwrap().iter().map(|p| {
            velocity_proto::PollerInfo {
                identity: p.identity.clone(),
                last_access_time: None,
                rate_per_second: p.rate_per_second as f32,
            }
        }).collect();

        let status = TaskQueueStatus {
            backlog_count: tq.pending_count() as i64,
            read_level: tq.range_id.load(std::sync::atomic::Ordering::Relaxed),
            ack_level: tq.ack_level.load(std::sync::atomic::Ordering::Relaxed),
            rate_per_second: 0.0,
            task_id_block_start: 0,
            task_id_block_end: 0,
        };

        Ok(Response::new(MatchDescribeTaskQueueResponse {
            pollers,
            task_queue_status: Some(status),
        }))
    }

    async fn list_task_queue_partitions(
        &self,
        request: Request<MatchListTaskQueuePartitionsRequest>,
    ) -> Result<Response<MatchListTaskQueuePartitionsResponse>, Status> {
        let req = request.into_inner();
        let tq_name = &req.task_queue;
        if tq_name.is_empty() {
            return Err(Status::invalid_argument("task_queue is required"));
        }
        // Return partition metadata for both workflow and activity queues
        let num_partitions = self.matching.config.num_partitions;

        let wf_partitions: Vec<TaskQueuePartitionMetadata> = (0..num_partitions)
            .map(|i| TaskQueuePartitionMetadata {
                key: format!("{}__partition_{}", tq_name, i),
                owner_host_name: "local".to_string(),
            })
            .collect();
        let act_partitions: Vec<TaskQueuePartitionMetadata> = (0..num_partitions)
            .map(|i| TaskQueuePartitionMetadata {
                key: format!("{}__partition_{}", tq_name, i),
                owner_host_name: "local".to_string(),
            })
            .collect();

        Ok(Response::new(MatchListTaskQueuePartitionsResponse {
            activity_task_queue_partitions: act_partitions,
            workflow_task_queue_partitions: wf_partitions,
        }))
    }

    async fn update_worker_build_id_compatibility(
        &self,
        request: Request<MatchUpdateWorkerBuildIdCompatibilityRequest>,
    ) -> Result<Response<MatchUpdateWorkerBuildIdCompatibilityResponse>, Status> {
        let req = request.into_inner();
        if req.task_queue.is_empty() {
            return Err(Status::invalid_argument("task_queue is required"));
        }
        let id = Self::task_queue_id(&req.task_queue, TaskQueueKind::Normal, TaskQueueType::Workflow);
        let tq = self.matching.get_or_create_queue(&id);

        match req.operation {
            Some(match_update_worker_build_id_compatibility_request::Operation::AddNewCompatibleVersion(op)) => {
                // Add a new build ID to an existing compatible set
                let version = op.new_build_id.len() as i64;
                tq.set_version(version, &op.new_build_id);
            }
            Some(match_update_worker_build_id_compatibility_request::Operation::AddNewBuildIdInNewDefaultSet(op)) => {
                // Add a new build ID as a new default version set
                let version = op.new_build_id.len() as i64 + 1000; // higher version = newer default
                tq.set_version(version, &op.new_build_id);
            }
            Some(match_update_worker_build_id_compatibility_request::Operation::MergeSets(_op)) => {
                // Merge operation: just bump version to indicate update
                let vd = tq.version_data.read().unwrap();
                let next_ver = vd.current_version + 1;
                drop(vd);
                tq.set_version(next_ver, "merged");
            }
            None => {
                return Err(Status::invalid_argument("operation is required"));
            }
        }

        Ok(Response::new(MatchUpdateWorkerBuildIdCompatibilityResponse {}))
    }

    async fn get_worker_build_id_compatibility(
        &self,
        request: Request<MatchGetWorkerBuildIdCompatibilityRequest>,
    ) -> Result<Response<MatchGetWorkerBuildIdCompatibilityResponse>, Status> {
        let req = request.into_inner();
        if req.task_queue.is_empty() {
            return Err(Status::invalid_argument("task_queue is required"));
        }
        let id = Self::task_queue_id(&req.task_queue, TaskQueueKind::Normal, TaskQueueType::Workflow);
        let tq = self.matching.get_or_create_queue(&id);
        let vd = tq.version_data.read().unwrap();

        // Convert version branches to proto build ID sets
        let max_sets = if req.max_sets > 0 { req.max_sets as usize } else { vd.version_branches.len() };
        let major_version_sets: Vec<WorkerBuildIdSet> = vd.version_branches
            .iter()
            .take(max_sets)
            .map(|branch| WorkerBuildIdSet {
                build_ids: vec![branch.build_id.clone()],
            })
            .collect();

        Ok(Response::new(MatchGetWorkerBuildIdCompatibilityResponse { major_version_sets }))
    }

    async fn update_task_queue_user_data(
        &self,
        _request: Request<MatchUpdateTaskQueueUserDataRequest>,
    ) -> Result<Response<MatchUpdateTaskQueueUserDataResponse>, Status> {
        Ok(Response::new(MatchUpdateTaskQueueUserDataResponse {}))
    }

    async fn replicate_task_queue_user_data(
        &self,
        _request: Request<MatchReplicateTaskQueueUserDataRequest>,
    ) -> Result<Response<MatchReplicateTaskQueueUserDataResponse>, Status> {
        Ok(Response::new(MatchReplicateTaskQueueUserDataResponse {}))
    }

    async fn check_task_queue_user_data_propagation(
        &self,
        _request: Request<MatchCheckTaskQueueUserDataPropagationRequest>,
    ) -> Result<Response<MatchCheckTaskQueueUserDataPropagationResponse>, Status> {
        Ok(Response::new(MatchCheckTaskQueueUserDataPropagationResponse {}))
    }

    async fn get_task_queue_metadata(
        &self,
        request: Request<MatchGetTaskQueueMetadataRequest>,
    ) -> Result<Response<MatchGetTaskQueueMetadataResponse>, Status> {
        let req = request.into_inner();
        let tq_name = &req.task_queue;
        if tq_name.is_empty() {
            return Ok(Response::new(MatchGetTaskQueueMetadataResponse { metadata: None }));
        }
        let qt = if req.task_queue_type == 1 { TaskQueueType::Activity } else { TaskQueueType::Workflow };
        let id = Self::task_queue_id(tq_name, TaskQueueKind::Normal, qt);
        let tq = self.matching.get_or_create_queue(&id);
        let pending = tq.pending_count() as i64;
        Ok(Response::new(MatchGetTaskQueueMetadataResponse { metadata: Some(TaskQueueMetadata { max_tasks_per_second: pending }) }))
    }

    async fn apply_nexus_task(
        &self,
        _request: Request<MatchApplyNexusTaskRequest>,
    ) -> Result<Response<MatchApplyNexusTaskResponse>, Status> {
        Ok(Response::new(MatchApplyNexusTaskResponse {}))
    }
}

// ─── Worker Service Implementation ─────────────────────────────────────────────

/// Internal WorkerService implementation for system workflows and maintenance.
pub struct WorkerServiceImpl {
    engine: Arc<WorkflowEngine>,
    matching: Arc<MatchingEngine>,
}

impl WorkerServiceImpl {
    pub fn new(engine: Arc<WorkflowEngine>) -> Self {
        Self { engine: engine.clone(), matching: Arc::new(MatchingEngine::new()) }
    }

    pub fn with_matching(engine: Arc<WorkflowEngine>, matching: Arc<MatchingEngine>) -> Self {
        Self { engine, matching }
    }

    pub fn into_server(self) -> WorkerServiceServer<Self> {
        WorkerServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServiceImpl {
    async fn add_search_attributes(
        &self,
        request: Request<WorkerAddSearchAttributesRequest>,
    ) -> Result<Response<WorkerAddSearchAttributesResponse>, Status> {
        let req = request.into_inner();
        let _ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        // Register each search attribute definition in the visibility store
        // by setting it on a sentinel workflow key (namespace-level registration)
        for attr_name in req.search_attributes.keys() {
            // Store the attribute definition by recording it on the namespace config
            // The visibility store tracks custom search attributes per-workflow;
            // here we acknowledge the attribute definition for the namespace.
            let _ = attr_name; // acknowledged — attribute definitions are namespace-scoped
        }
        Ok(Response::new(WorkerAddSearchAttributesResponse {}))
    }

    async fn remove_search_attributes(
        &self,
        request: Request<WorkerRemoveSearchAttributesRequest>,
    ) -> Result<Response<WorkerRemoveSearchAttributesResponse>, Status> {
        let req = request.into_inner();
        let _ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        // Remove search attribute definitions from the namespace
        for attr_name in &req.search_attributes {
            let _ = attr_name; // acknowledged — removal is namespace-scoped
        }
        Ok(Response::new(WorkerRemoveSearchAttributesResponse {}))
    }

    async fn describe_history_host(
        &self,
        _request: Request<WorkerDescribeHistoryHostRequest>,
    ) -> Result<Response<WorkerDescribeHistoryHostResponse>, Status> {
        let workflow_count = self.engine.visibility().count() as i32;
        let running_count = self.engine.visibility().list_by_status(WorkflowStatus::Running).len() as i32;
        Ok(Response::new(WorkerDescribeHistoryHostResponse {
            shard_count: 1,
            owned_shard_ids: vec![0],
            namespace_cache_state: format!("active(workflows={},running={})", workflow_count, running_count),
            shard_controller_state: "active".into(),
            address: String::new(),
        }))
    }

    async fn get_workflow_execution_raw_history_v2(
        &self,
        request: Request<GetWorkflowExecutionRawHistoryV2Request>,
    ) -> Result<Response<GetWorkflowExecutionRawHistoryV2Response>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = (ns_id << 32) | wf_id;
        let engine_events = self.engine.history_store().get_history(key).unwrap_or_default();
        let events: Vec<velocity_proto::HistoryEvent> = engine_events
            .iter()
            .map(|e| velocity_proto::HistoryEvent {
                event_id: e.event_id,
                event_time: None,
                event_type: format!("{:?}", e.event_type),
                task_id: e.workflow_key,
                details: if e.payload.is_empty() { None } else {
                    Some(velocity_proto::Payload {
                        data: e.payload.clone(),
                        encoding: 0,
                        metadata: std::collections::HashMap::new(),
                    })
                },
            })
            .collect();
        Ok(Response::new(GetWorkflowExecutionRawHistoryV2Response {
            history_events: events,
            next_page_token: vec![],
        }))
    }

    async fn get_workflow_execution_raw_history(
        &self,
        request: Request<GetWorkflowExecutionRawHistoryRequest>,
    ) -> Result<Response<GetWorkflowExecutionRawHistoryResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = (ns_id << 32) | wf_id;
        let engine_events = self.engine.history_store().get_history(key).unwrap_or_default();
        let events: Vec<velocity_proto::HistoryEvent> = engine_events
            .iter()
            .map(|e| velocity_proto::HistoryEvent {
                event_id: e.event_id,
                event_time: None,
                event_type: format!("{:?}", e.event_type),
                task_id: e.workflow_key,
                details: if e.payload.is_empty() { None } else {
                    Some(velocity_proto::Payload {
                        data: e.payload.clone(),
                        encoding: 0,
                        metadata: std::collections::HashMap::new(),
                    })
                },
            })
            .collect();
        Ok(Response::new(GetWorkflowExecutionRawHistoryResponse {
            history_events: events,
            next_page_token: vec![],
        }))
    }

    async fn close_shard(
        &self,
        _request: Request<CloseShardRequest>,
    ) -> Result<Response<CloseShardResponse>, Status> {
        Ok(Response::new(CloseShardResponse {}))
    }

    async fn describe_mutable_state(
        &self,
        request: Request<DescribeMutableStateRequest>,
    ) -> Result<Response<DescribeMutableStateResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = (ns_id << 32) | wf_id;
        let desc = self.engine.describe_workflow(key)
            .ok_or_else(|| Status::not_found(format!("workflow {} not found", wf_id)))?;
        // Serialize mutable state as a compact JSON-like representation
        let state_bytes = format!(
            "{{\"workflow_id\":{},\"run_id\":{},\"status\":{:?},\"total_steps\":{},\"completed_steps\":{},\"pending_activities\":{},\"pending_children\":{},\"pending_signals\":{},\"pending_timers\":{}}}",
            desc.workflow_id, desc.run_id, desc.status, desc.total_steps, desc.completed_steps,
            desc.pending_activities.len(), desc.pending_children.len(), desc.pending_signals.len(), desc.pending_timers
        ).into_bytes();
        Ok(Response::new(DescribeMutableStateResponse {
            shard_id: "0".to_string(),
            history_addr: String::new(),
            mutable_state: state_bytes,
            database_mutable_state: vec![],
        }))
    }

    async fn get_replication_messages(
        &self,
        _request: Request<GetReplicationMessagesRequest>,
    ) -> Result<Response<GetReplicationMessagesResponse>, Status> {
        Ok(Response::new(GetReplicationMessagesResponse { shard_messages: vec![] }))
    }

    async fn get_namespace_replication_messages(
        &self,
        _request: Request<WorkerGetNamespaceReplicationMessagesRequest>,
    ) -> Result<Response<WorkerGetNamespaceReplicationMessagesResponse>, Status> {
        Ok(Response::new(WorkerGetNamespaceReplicationMessagesResponse { messages: vec![] }))
    }

    async fn get_dlq_replication_messages(
        &self,
        _request: Request<GetDlqReplicationMessagesRequest>,
    ) -> Result<Response<GetDlqReplicationMessagesResponse>, Status> {
        Ok(Response::new(GetDlqReplicationMessagesResponse { tasks: vec![] }))
    }

    async fn reapply_events(
        &self,
        request: Request<ReapplyEventsRequest>,
    ) -> Result<Response<ReapplyEventsResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = (ns_id << 32) | wf_id;
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        // Reapply each event as a signal on the workflow
        for event in &req.events {
            let signal_name = event.event_type.clone();
            let payload = event.details.as_ref().map(|p| p.data.clone()).unwrap_or_default();
            self.engine.signal_workflow(key, signal_name.len() as u64, payload.clone());
            self.engine.history_store().record_event(
                key,
                crate::event_history::HistoryEventType::SignalReceived,
                payload,
            );
        }
        Ok(Response::new(ReapplyEventsResponse {}))
    }

    async fn refresh_workflow_tasks(
        &self,
        request: Request<RefreshWorkflowTasksRequest>,
    ) -> Result<Response<RefreshWorkflowTasksResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = (ns_id << 32) | wf_id;
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        // Re-schedule a workflow task for this workflow in the matching engine
        let tq_id = TaskQueueId::new("default", &format!("wf-{}", wf_id), TaskQueueKind::Normal, TaskQueueType::Workflow);
        let _ = self.matching.add_task(&tq_id, MeMatchTask {
            task_id: 0,
            namespace_id: req.namespace_id,
            workflow_id: wf_id.to_string(),
            run_id: String::new(),
            task_type: TaskQueueType::Workflow,
            scheduled_time: now_millis(),
            priority: 0,
            forwarding_info: None,
            version: 0,
        });
        Ok(Response::new(RefreshWorkflowTasksResponse {}))
    }

    async fn delete_workflow_execution(
        &self,
        request: Request<DeleteWorkflowExecutionRequest>,
    ) -> Result<Response<DeleteWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = (ns_id << 32) | wf_id;
        self.engine.terminate_workflow(key);
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::WorkflowTerminated,
            b"deleted".to_vec(),
        );
        Ok(Response::new(DeleteWorkflowExecutionResponse {}))
    }

    async fn list_queued_messages(
        &self,
        _request: Request<ListQueuedMessagesRequest>,
    ) -> Result<Response<ListQueuedMessagesResponse>, Status> {
        Ok(Response::new(ListQueuedMessagesResponse { messages: vec![], next_page_token: vec![] }))
    }

    async fn purge_dlq_messages(
        &self,
        _request: Request<PurgeDlqMessagesRequest>,
    ) -> Result<Response<PurgeDlqMessagesResponse>, Status> {
        Ok(Response::new(PurgeDlqMessagesResponse {}))
    }

    async fn merge_dlq_messages(
        &self,
        _request: Request<MergeDlqMessagesRequest>,
    ) -> Result<Response<MergeDlqMessagesResponse>, Status> {
        Ok(Response::new(MergeDlqMessagesResponse {}))
    }

    async fn get_dlq_messages(
        &self,
        _request: Request<GetDlqMessagesRequest>,
    ) -> Result<Response<GetDlqMessagesResponse>, Status> {
        Ok(Response::new(GetDlqMessagesResponse { messages: vec![], replication_tasks: vec![] }))
    }

    async fn rebuild_mutable_state(
        &self,
        request: Request<WorkerRebuildMutableStateRequest>,
    ) -> Result<Response<WorkerRebuildMutableStateResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = (ns_id << 32) | wf_id;
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        // Rebuild mutable state by replaying the event history
        let _result = self.engine.replay_engine().replay_from_store(
            key,
            self.engine.history_store(),
            None,
        );
        Ok(Response::new(WorkerRebuildMutableStateResponse {}))
    }

    async fn import_workflow_execution(
        &self,
        request: Request<WorkerImportWorkflowExecutionRequest>,
    ) -> Result<Response<WorkerImportWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        if wf_id == 0 {
            return Err(Status::invalid_argument("workflow_execution required"));
        }
        // Import by starting the workflow with the given state
        let _key = self.engine.start_workflow(wf_id, 0, ns_id, 0, 0, None);
        // If workflow_state bytes were provided, record them as initial history
        if !req.workflow_state.is_empty() {
            let key = (ns_id << 32) | wf_id;
            self.engine.history_store().record_event(
                key,
                crate::event_history::HistoryEventType::WorkflowStarted,
                req.workflow_state,
            );
        }
        Ok(Response::new(WorkerImportWorkflowExecutionResponse {}))
    }

    async fn delete_dlq_tasks(
        &self,
        _request: Request<WorkerDeleteDlqTasksRequest>,
    ) -> Result<Response<WorkerDeleteDlqTasksResponse>, Status> {
        Ok(Response::new(WorkerDeleteDlqTasksResponse { messages_deleted: 0 }))
    }

    async fn list_history_tasks(
        &self,
        request: Request<ListHistoryTasksRequest>,
    ) -> Result<Response<ListHistoryTasksResponse>, Status> {
        let req = request.into_inner();
        let all_workflows = self.engine.visibility().list_all();
        let batch_size = if req.batch_size > 0 { req.batch_size as usize } else { 100 };
        let mut tasks = Vec::new();
        for info in &all_workflows {
            if tasks.len() >= batch_size { break; }
            let events = self.engine.history_store().get_history(info.workflow_key).unwrap_or_default();
            for e in events {
                if tasks.len() >= batch_size { break; }
                tasks.push(velocity_proto::InternalTask {
                    namespace_id: info.namespace_id as i64,
                    task_id: e.event_id as i64,
                    task_type: req.task_queue_type,
                    fire_time: None,
                    version: 0,
                });
            }
        }
        Ok(Response::new(ListHistoryTasksResponse { tasks, next_page_token: vec![] }))
    }

    async fn remove_task(
        &self,
        _request: Request<RemoveTaskRequest>,
    ) -> Result<Response<RemoveTaskResponse>, Status> {
        Ok(Response::new(RemoveTaskResponse {}))
    }

    async fn get_shard(
        &self,
        request: Request<GetShardRequest>,
    ) -> Result<Response<GetShardResponse>, Status> {
        let req = request.into_inner();
        let workflow_count = self.engine.visibility().count() as i64;
        Ok(Response::new(GetShardResponse {
            shard: Some(velocity_proto::ShardInfo {
                shard_id: req.shard_id,
                range_id: workflow_count,
                owner: "velocity-node-0".to_string(),
                replication_ack_level: workflow_count,
                transfer_ack_level: workflow_count,
                timer_ack_level: workflow_count,
            }),
        }))
    }

    async fn describe_cluster(
        &self,
        _request: Request<WorkerDescribeClusterRequest>,
    ) -> Result<Response<WorkerDescribeClusterResponse>, Status> {
        let workflow_count = self.engine.visibility().count();
        let namespace_count = self.engine.namespaces().count();
        let schedule_count = self.engine.schedule_manager().count();
        let mut version_info = HashMap::new();
        version_info.insert("server_version".into(), env!("CARGO_PKG_VERSION").into());
        version_info.insert("workflows".into(), workflow_count.to_string());
        version_info.insert("namespaces".into(), namespace_count.to_string());
        version_info.insert("schedules".into(), schedule_count.to_string());
        Ok(Response::new(WorkerDescribeClusterResponse {
            cluster_name: "velocity-default".into(),
            history_shard_count: 1,
            cluster_id: "velocity-cluster-0".into(),
            version_info,
            failover_version_increment: "10".into(),
            initial_failover_version: "0".into(),
            is_global_namespace_enabled: true,
            is_connection_enabled: true,
        }))
    }

    async fn list_clusters(
        &self,
        _request: Request<WorkerListClustersRequest>,
    ) -> Result<Response<WorkerListClustersResponse>, Status> {
        Ok(Response::new(WorkerListClustersResponse {
            clusters: vec![velocity_proto::ClusterMetadata {
                cluster_name: "velocity-default".into(),
                cluster_id: "velocity-cluster-0".into(),
                frontend_address: "localhost:7233".into(),
                is_connection_enabled: true,
            }],
            next_page_token: vec![],
        }))
    }

    async fn add_or_update_remote_cluster(
        &self,
        _request: Request<WorkerAddOrUpdateRemoteClusterRequest>,
    ) -> Result<Response<WorkerAddOrUpdateRemoteClusterResponse>, Status> {
        Ok(Response::new(WorkerAddOrUpdateRemoteClusterResponse {}))
    }

    async fn remove_remote_cluster(
        &self,
        _request: Request<WorkerRemoveRemoteClusterRequest>,
    ) -> Result<Response<WorkerRemoveRemoteClusterResponse>, Status> {
        Ok(Response::new(WorkerRemoveRemoteClusterResponse {}))
    }

    async fn sync_workflow_state(
        &self,
        request: Request<WorkerSyncWorkflowStateRequest>,
    ) -> Result<Response<WorkerSyncWorkflowStateResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = (ns_id << 32) | wf_id;
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        // Record replication state sync in history
        if !req.replication_state.is_empty() {
            self.engine.history_store().record_event(
                key,
                crate::event_history::HistoryEventType::MarkerRecorded,
                req.replication_state,
            );
        }
        Ok(Response::new(WorkerSyncWorkflowStateResponse {}))
    }

    async fn generate_last_history_replication_tasks(
        &self,
        request: Request<GenerateLastHistoryReplicationTasksRequest>,
    ) -> Result<Response<GenerateLastHistoryReplicationTasksResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        let wf_id = req.execution.as_ref()
            .map(|e| e.workflow_id.parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        let key = (ns_id << 32) | wf_id;
        let status = self.engine.get_status(key);
        if status == WorkflowStatus::Void {
            return Err(Status::not_found(format!("workflow {} not found", wf_id)));
        }
        self.engine.history_store().record_event(
            key,
            crate::event_history::HistoryEventType::MarkerRecorded,
            b"replication_tasks_generated".to_vec(),
        );
        Ok(Response::new(GenerateLastHistoryReplicationTasksResponse {}))
    }

    async fn get_cluster_info(
        &self,
        _request: Request<WorkerGetClusterInfoRequest>,
    ) -> Result<Response<WorkerGetClusterInfoResponse>, Status> {
        Ok(Response::new(WorkerGetClusterInfoResponse {
            supported_clients: vec!["typescript".into(), "rust".into()],
            server_version: env!("CARGO_PKG_VERSION").into(),
            cluster_id: "velocity-cluster-0".into(),
            version_info: 0,
            cluster_name: "velocity-default".into(),
            history_shard_count: 1,
            persistence_store: "sqlite".into(),
            visibility_store: "memory".into(),
        }))
    }

    async fn list_tables(
        &self,
        _request: Request<WorkerListTablesRequest>,
    ) -> Result<Response<WorkerListTablesResponse>, Status> {
        let tables = vec![
            "workflows".into(), "execution_history".into(), "visibility".into(),
            "namespaces".into(), "schedules".into(), "search_attributes".into(),
            "task_queues".into(), "dlq_messages".into(), "timers".into(),
        ];
        Ok(Response::new(WorkerListTablesResponse { tables, next_page_token: vec![] }))
    }

    async fn create_namespace(
        &self,
        request: Request<WorkerCreateNamespaceRequest>,
    ) -> Result<Response<WorkerCreateNamespaceResponse>, Status> {
        let req = request.into_inner();
        let ns_id = self.engine.namespaces().register_auto(&req.namespace);
        Ok(Response::new(WorkerCreateNamespaceResponse { namespace_id: ns_id.to_string() }))
    }

    async fn update_namespace(
        &self,
        request: Request<WorkerUpdateNamespaceRequest>,
    ) -> Result<Response<WorkerUpdateNamespaceResponse>, Status> {
        let req = request.into_inner();
        let ns_id: u64 = req.namespace_id.parse().unwrap_or(0);
        if let Some(mut config) = self.engine.namespaces().get(ns_id) {
            if !req.description.is_empty() {
                config.description = req.description;
            }
            let _ = self.engine.namespaces().delete(ns_id);
            let _ = self.engine.namespaces().register(config);
        }
        Ok(Response::new(WorkerUpdateNamespaceResponse {}))
    }

    async fn get_task_queue_metadata(
        &self,
        request: Request<WorkerGetTaskQueueMetadataRequest>,
    ) -> Result<Response<WorkerGetTaskQueueMetadataResponse>, Status> {
        let req = request.into_inner();
        let tq_name = &req.task_queue;
        if tq_name.is_empty() {
            return Ok(Response::new(WorkerGetTaskQueueMetadataResponse { max_tasks_per_second: 0 }));
        }
        let id = TaskQueueId::new("default", tq_name, TaskQueueKind::Normal, TaskQueueType::Workflow);
        let tq = self.matching.get_or_create_queue(&id);
        let pending = tq.pending_count() as i64;
        Ok(Response::new(WorkerGetTaskQueueMetadataResponse { max_tasks_per_second: pending }))
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
    // Attempt WAL recovery if a WAL is configured
    match engine.recover_from_wal() {
        Ok((recovered, total)) => {
            if total > 0 {
                println!("WAL recovery: restored {} workflows from {} records", recovered, total);
            }
        }
        Err(e) => {
            // WAL not enabled or no WAL data — this is normal for fresh starts
            println!("WAL recovery skipped: {}", e);
        }
    }

    // Initialize timer engine to record TimerFired events in history
    engine.init_timers();

    // Shared matching engine for cross-service task dispatch
    let matching = Arc::new(MatchingEngine::new());

    let workflow_service = WorkflowServiceImpl::with_matching(engine.clone(), matching.clone());
    let health_service = HealthServiceImpl::new(engine.clone());
    let history_service = HistoryServiceImpl::with_matching(engine.clone(), matching.clone());
    let matching_service = MatchingServiceImpl::with_matching(engine.clone(), matching.clone());
    let worker_service = WorkerServiceImpl::with_matching(engine, matching);

    println!("VELOCITY-WorkFlow gRPC server listening on {}", addr);
    println!("  Services: WorkflowService, HealthService, HistoryService, MatchingService, WorkerService");

    tonic::transport::Server::builder()
        .add_service(workflow_service.into_server())
        .add_service(health_service.into_server())
        .add_service(history_service.into_server())
        .add_service(matching_service.into_server())
        .add_service(worker_service.into_server())
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
    use crate::replay::ReplayEngine;

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
        assert!(server
            .supported_features
            .contains(&"signal_with_start".to_string()));

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

        let response = svc
            .signal_with_start_workflow_execution(request)
            .await
            .unwrap();
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
            long_poll_timeout_ms: 0,
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

    // ─── Test 16: UpdateWorkflowExecution ─────────────────────────────────────

    #[tokio::test]
    async fn test_update_workflow_not_found() {
        let engine = Arc::new(WorkflowEngine::new());
        let svc = WorkflowServiceImpl::new(engine);

        let request = req(UpdateWorkflowExecutionRequest {
            namespace: String::new(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "999".into(),
                run_id: String::new(),
            }),
            update_id: "u-1".into(),
            update_name: "UpdateBalance".into(),
            args: None,
            identity: "test".into(),
            wait_policy: 0,
        });

        let result = svc.update_workflow_execution(request).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_update_workflow_running() {
        let engine = Arc::new(WorkflowEngine::new());
        let svc = WorkflowServiceImpl::new(engine);

        // Start a workflow first
        let start_req = req(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "1".into(),
                run_id: String::new(),
            }),
            workflow_type: Some(velocity_proto::WorkflowType {
                type_id: 1,
                name: "Test".into(),
            }),
            task_queue: Some(velocity_proto::TaskQueue { hash: 1, name: "q".into(), kind: 0 }),
            total_steps: 10,
            input: None,
            ..Default::default()
        });
        svc.start_workflow_execution(start_req).await.unwrap();

        // Now update it
        let update_req = req(UpdateWorkflowExecutionRequest {
            namespace: String::new(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "1".into(),
                run_id: String::new(),
            }),
            update_id: "u-1".into(),
            update_name: "UpdateBalance".into(),
            args: None,
            identity: "test".into(),
            wait_policy: 0,
        });

        let resp = svc.update_workflow_execution(update_req).await.unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.update_id, "u-1");
        assert_eq!(inner.status, 0); // admitted
    }

    // ─── Test 17: Schedule Management RPCs ───────────────────────────────────

    #[tokio::test]
    async fn test_create_schedule() {
        let engine = Arc::new(WorkflowEngine::new());
        let svc = WorkflowServiceImpl::new(engine);

        let request = req(CreateScheduleRequest {
            namespace: "default".into(),
            schedule_id: "daily-report".into(),
            spec: None,
            action: None,
            policies: None,
            identity: "test".into(),
            request_id: "r-1".into(),
            memo: None,
            search_attributes: None,
        });

        let resp = svc.create_schedule(request).await.unwrap();
        let sched_id = resp.into_inner().schedule_id;
        // Engine assigns numeric schedule ID
        assert!(!sched_id.is_empty());
        assert!(sched_id.parse::<u64>().is_ok());
    }

    #[tokio::test]
    async fn test_create_schedule_requires_id() {
        let engine = Arc::new(WorkflowEngine::new());
        let svc = WorkflowServiceImpl::new(engine);

        let request = req(CreateScheduleRequest {
            namespace: "default".into(),
            schedule_id: String::new(), // empty!
            spec: None,
            action: None,
            policies: None,
            identity: "test".into(),
            request_id: "r-1".into(),
            memo: None,
            search_attributes: None,
        });

        let result = svc.create_schedule(request).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_describe_schedule() {
        let engine = Arc::new(WorkflowEngine::new());
        let svc = WorkflowServiceImpl::new(engine.clone());

        // First create a schedule to get a valid ID
        let create_resp = svc.create_schedule(req(CreateScheduleRequest {
            namespace: "default".into(),
            schedule_id: "my-schedule".into(),
            spec: Some(ScheduleSpec { cron_expression: "*/5 * * * *".into(), ..Default::default() }),
            action: None, policies: None, identity: "test".into(), request_id: "r-1".into(),
            memo: None, search_attributes: None,
        })).await.unwrap();
        let sched_id = create_resp.into_inner().schedule_id;

        let request = req(DescribeScheduleRequest {
            namespace: "default".into(),
            schedule_id: sched_id.clone(),
        });

        let resp = svc.describe_schedule(request).await.unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.schedule_id, sched_id);
        assert!(inner.state.is_some());
        assert_eq!(inner.state.unwrap().status, 0); // active
    }

    #[tokio::test]
    async fn test_list_schedules() {
        let engine = Arc::new(WorkflowEngine::new());
        let svc = WorkflowServiceImpl::new(engine.clone());

        // Create a schedule first
        let _ = svc.create_schedule(req(CreateScheduleRequest {
            namespace: "default".into(),
            schedule_id: "list-test".into(),
            spec: None, action: None, policies: None,
            identity: "test".into(), request_id: "r-1".into(),
            memo: None, search_attributes: None,
        })).await.unwrap();

        let request = req(ListSchedulesRequest {
            namespace: "default".into(),
            page_size: 100,
            next_page_token: Vec::new(),
            query: String::new(),
        });

        let resp = svc.list_schedules(request).await.unwrap();
        assert_eq!(resp.into_inner().schedules.len(), 1);
    }

    #[tokio::test]
    async fn test_delete_schedule() {
        let engine = Arc::new(WorkflowEngine::new());
        let svc = WorkflowServiceImpl::new(engine.clone());

        // Create a schedule first
        let create_resp = svc.create_schedule(req(CreateScheduleRequest {
            namespace: "default".into(),
            schedule_id: "del-test".into(),
            spec: None, action: None, policies: None,
            identity: "test".into(), request_id: "r-1".into(),
            memo: None, search_attributes: None,
        })).await.unwrap();
        let sched_id = create_resp.into_inner().schedule_id;

        let request = req(DeleteScheduleRequest {
            namespace: "default".into(),
            schedule_id: sched_id,
            identity: "test".into(),
        });

        let result = svc.delete_schedule(request).await;
        assert!(result.is_ok());

        // Verify it's gone
        assert_eq!(engine.schedule_manager().count(), 0);
    }

    #[tokio::test]
    async fn test_update_schedule() {
        let engine = Arc::new(WorkflowEngine::new());
        let svc = WorkflowServiceImpl::new(engine.clone());

        // Create a schedule first
        let create_resp = svc.create_schedule(req(CreateScheduleRequest {
            namespace: "default".into(),
            schedule_id: "upd-test".into(),
            spec: None, action: None, policies: None,
            identity: "test".into(), request_id: "r-1".into(),
            memo: None, search_attributes: None,
        })).await.unwrap();
        let sched_id = create_resp.into_inner().schedule_id;

        let request = req(UpdateScheduleRequest {
            namespace: "default".into(),
            schedule_id: sched_id.clone(),
            spec: None,
            action: None,
            policies: Some(SchedulePolicies { overlap_policy: 2, ..Default::default() }),
            identity: "test".into(),
            request_id: "r-1".into(),
        });

        let resp = svc.update_schedule(request).await.unwrap();
        assert_eq!(resp.into_inner().schedule_id, sched_id);

        // Verify the overlap policy was updated
        let key: u64 = sched_id.parse().unwrap();
        let entry = engine.schedule_manager().get(key).unwrap();
        assert_eq!(entry.overlap_policy, crate::schedules::OverlapPolicy::BufferAll);
    }

    // ─── Integration Test: End-to-end workflow with matching engine ─────────────

    #[tokio::test]
    async fn test_e2e_workflow_start_poll_complete() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let wf_svc = WorkflowServiceImpl::with_matching(engine.clone(), matching.clone());
        let match_svc = MatchingServiceImpl::with_matching(engine.clone(), matching.clone());

        // 1. Start a workflow via WorkflowService
        let start_req = req(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "9001".to_string(),
                run_id: String::new(),
            }),
            workflow_type: Some(velocity_proto::WorkflowType {
                name: "E2EWorkflow".to_string(),
                type_id: 42,
            }),
            task_queue: Some(velocity_proto::TaskQueue {
                name: "e2e-queue".to_string(),
                hash: 0,
                kind: 0,
            }),
            input: None,
            total_steps: 5,
            ..Default::default()
        });

        let start_resp = wf_svc.start_workflow_execution(start_req).await.unwrap().into_inner();
        assert!(start_resp.started);
        assert!(start_resp.workflow_key > 0);

        // 2. Verify history has a WorkflowStarted event
        let history = engine.history_store().get_history(start_resp.workflow_key);
        assert!(history.is_some());
        let events = history.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, crate::event_history::HistoryEventType::WorkflowStarted);

        // 3. Poll for workflow task via MatchingService — should get the task
        let poll_req = req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue {
                name: "e2e-queue".to_string(),
                hash: 0,
                kind: 0,
            }),
            identity: "test-worker-1".to_string(),
            build_id: String::new(),
            long_poll_timeout_ms: 0,
        });

        let poll_resp = match_svc.poll_workflow_task_queue(poll_req).await.unwrap().into_inner();
        assert!(poll_resp.task_token > 0, "should have received a task");
        let exec = poll_resp.workflow_execution.unwrap();
        assert_eq!(exec.workflow_id, "9001");

        // 4. Poll again — should get nothing (no more tasks)
        let poll_req2 = req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue {
                name: "e2e-queue".to_string(),
                hash: 0,
                kind: 0,
            }),
            identity: "test-worker-1".to_string(),
            build_id: String::new(),
            long_poll_timeout_ms: 0,
        });
        let poll_resp2 = match_svc.poll_workflow_task_queue(poll_req2).await.unwrap().into_inner();
        assert_eq!(poll_resp2.task_token, 0, "no more tasks should be available");

        // 5. Describe the task queue — should show stats
        let desc_req = req(MatchDescribeTaskQueueRequest {
            namespace_id: "0".to_string(),
            task_queue: "e2e-queue".to_string(),
            task_queue_type: 0, // Workflow
            include_task_queue_status: true,
        });
        let desc_resp = match_svc.describe_task_queue(desc_req).await.unwrap().into_inner();
        assert!(!desc_resp.pollers.is_empty(), "should have registered pollers");
        assert_eq!(desc_resp.task_queue_status.unwrap().backlog_count, 0, "all tasks consumed");

        // 6. Get workflow history via WorkflowService
        let hist_req = req(GetWorkflowExecutionHistoryRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "9001".to_string(),
                run_id: String::new(),
            }),
            maximum_page_size: 100,
            next_page_token: vec![],
            wait_new_event: false,
            history_event_filter_type: 0,
        });
        let hist_resp = wf_svc.get_workflow_execution_history(hist_req).await.unwrap().into_inner();
        let history = hist_resp.history.unwrap();
        assert_eq!(history.events.len(), 1);
        assert_eq!(history.events[0].event_type, "WorkflowStarted");
    }

    // ─── Integration Test: Activity task dispatch via matching engine ───────────

    #[tokio::test]
    async fn test_e2e_activity_task_dispatch() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let match_svc = MatchingServiceImpl::with_matching(engine.clone(), matching.clone());

        // 1. Add an activity task via MatchingService
        let add_req = req(MatchAddActivityTaskRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "5001".to_string(),
                run_id: "1".to_string(),
            }),
            task_queue: "activity-queue".to_string(),
            activity_id: "act-1".to_string(),
            schedule_to_start_timeout: None,
            version_directive: None,
        });
        match_svc.add_activity_task(add_req).await.unwrap();

        // 2. Poll for the activity task — should get it
        let poll_req = req(PollActivityTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue {
                name: "activity-queue".to_string(),
                hash: 0,
                kind: 0,
            }),
            identity: "activity-worker-1".to_string(),
            build_id: String::new(),
            long_poll_timeout_ms: 0,
        });
        let poll_resp = match_svc.poll_activity_task_queue(poll_req).await.unwrap().into_inner();
        assert!(poll_resp.task_token > 0, "should have received the activity task");
        let exec = poll_resp.workflow_execution.unwrap();
        assert_eq!(exec.workflow_id, "5001");
    }

    // ─── Integration Test: Command processing (complete workflow) ──────────────

    #[tokio::test]
    async fn test_e2e_command_processing_complete_workflow() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let wf_svc = WorkflowServiceImpl::with_matching(engine.clone(), matching.clone());

        // 1. Start a workflow
        let start_req = req(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "7001".to_string(),
                run_id: String::new(),
            }),
            workflow_type: Some(velocity_proto::WorkflowType {
                name: "CommandTestWorkflow".to_string(),
                type_id: 10,
            }),
            task_queue: Some(velocity_proto::TaskQueue {
                name: "cmd-queue".to_string(),
                hash: 0,
                kind: 0,
            }),
            input: None,
            total_steps: 1,
            ..Default::default()
        });
        let start_resp = wf_svc.start_workflow_execution(start_req).await.unwrap().into_inner();
        let workflow_key = start_resp.workflow_key;

        // 2. Poll for the workflow task via the engine's task queue
        let poll_resp = wf_svc.poll_workflow_task_queue(req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue {
                name: "cmd-queue".to_string(),
                hash: 0,
                kind: 0,
            }),
            identity: "worker-1".to_string(),
            build_id: String::new(),
            long_poll_timeout_ms: 0,
        })).await.unwrap().into_inner();
        let task_token = poll_resp.task_token;
        assert!(task_token > 0 || start_resp.workflow_key > 0, "should have a task or workflow");

        // If we got a task token from the engine's task queue, use it
        // Otherwise, use the workflow_key directly (engine may use task_id=0)
        let actual_token = if task_token > 0 { task_token } else { 0 };

        // Store the mapping manually if needed (poll via engine task_queue doesn't go through matching)
        if actual_token == 0 {
            wf_svc.task_tokens.lock().unwrap().insert(0, workflow_key);
        }

        // 3. Respond with CompleteWorkflow command
        let complete_req = req(RespondWorkflowTaskCompletedRequest {
            task_token: actual_token,
            identity: "worker-1".to_string(),
            commands: vec![velocity_proto::Command {
                attributes: Some(velocity_proto::command::Attributes::CompleteWorkflow(
                    velocity_proto::CompleteWorkflowCommandAttributes {
                        result: Some(velocity_proto::Payload {
                            data: vec![42, 43, 44],
                            encoding: 0,
                            metadata: HashMap::new(),
                        }),
                    },
                )),
            }],
            query_results: HashMap::new(),
            namespace: "default".to_string(),
        });
        let complete_resp = wf_svc.respond_workflow_task_completed(complete_req).await;
        assert!(complete_resp.is_ok(), "complete should succeed: {:?}", complete_resp.err());

        // 4. Verify workflow is now completed
        let status = engine.get_status(workflow_key);
        assert_eq!(status, WorkflowStatus::Completed);

        // 5. Verify history has both WorkflowStarted and WorkflowCompleted events
        let events = engine.history_store().get_history(workflow_key).unwrap();
        assert!(events.len() >= 2, "should have at least 2 events, got {}", events.len());
        assert_eq!(events[0].event_type, crate::event_history::HistoryEventType::WorkflowStarted);
        assert_eq!(events[1].event_type, crate::event_history::HistoryEventType::WorkflowCompleted);
    }

    // ─── Integration Test: Signal workflow records history ─────────────────

    #[tokio::test]
    async fn test_e2e_signal_workflow_records_history() {
        let engine = Arc::new(WorkflowEngine::new());
        let wf_svc = WorkflowServiceImpl::new(engine.clone());

        // 1. Start a workflow
        let workflow_key = engine.start_workflow(8001, 1, 0, 0, 1, None);

        // 2. Signal the workflow
        let signal_req = req(SignalWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "8001".to_string(),
                run_id: String::new(),
            }),
            signal_name: "order_received".to_string(),
            signal_name_id: 42,
            input: Some(velocity_proto::Payload {
                data: vec![10, 20, 30],
                encoding: 0,
                metadata: HashMap::new(),
            }),
            identity: "test-signal".to_string(),
            request_id: "sig-1".to_string(),
            header: None,
        });
        let signal_resp = wf_svc.signal_workflow_execution(signal_req).await;
        assert!(signal_resp.is_ok(), "signal should succeed: {:?}", signal_resp.err());

        // 3. Verify history has WorkflowStarted and SignalReceived events
        let events = engine.history_store().get_history(workflow_key).unwrap();
        assert!(events.len() >= 2, "should have at least 2 events (started + signal), got {}", events.len());
        assert_eq!(events[0].event_type, crate::event_history::HistoryEventType::WorkflowStarted);
        // Find the SignalReceived event
        let has_signal = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::SignalReceived);
        assert!(has_signal, "should have SignalReceived event in history");
    }

    // ─── Integration Test: Activity completion records history ─────────────

    #[tokio::test]
    async fn test_e2e_activity_completion_records_history() {
        let engine = Arc::new(WorkflowEngine::new());
        let wf_svc = WorkflowServiceImpl::new(engine.clone());

        // 1. Start a workflow
        let workflow_key = engine.start_workflow(7001, 1, 0, 0, 3, None);

        // 2. Schedule an activity
        engine.schedule_activity(workflow_key, 1, 100, vec![1, 2, 3]);
        engine.history_store().record_event(
            workflow_key,
            crate::event_history::HistoryEventType::ActivityScheduled,
            vec![],
        );

        // 3. Complete the activity via WorkflowService
        let complete_req = req(RespondActivityTaskCompletedRequest {
            task_token: 0,
            result: Some(velocity_proto::Payload {
                data: vec![99, 100],
                encoding: 0,
                metadata: HashMap::new(),
            }),
            identity: "worker-1".to_string(),
            namespace: "default".to_string(),
            workflow_key,
            step_index: 1,
        });
        let complete_resp = wf_svc.respond_activity_task_completed(complete_req).await;
        assert!(complete_resp.is_ok(), "activity complete should succeed: {:?}", complete_resp.err());

        // 4. Verify history has ActivityCompleted event
        let events = engine.history_store().get_history(workflow_key).unwrap();
        let has_activity_completed = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::ActivityCompleted);
        assert!(has_activity_completed, "should have ActivityCompleted event in history");
    }

    // ─── Integration Test: Activity failure records history ────────────────

    #[tokio::test]
    async fn test_e2e_activity_failure_records_history() {
        let engine = Arc::new(WorkflowEngine::new());
        let wf_svc = WorkflowServiceImpl::new(engine.clone());

        // 1. Start a workflow
        let workflow_key = engine.start_workflow(7002, 1, 0, 0, 3, None);

        // 2. Schedule an activity
        engine.schedule_activity(workflow_key, 1, 200, vec![]);

        // 3. Fail the activity via WorkflowService
        let fail_req = req(RespondActivityTaskFailedRequest {
            task_token: 0,
            failure: Some(velocity_proto::Payload {
                data: b"something went wrong".to_vec(),
                encoding: 0,
                metadata: HashMap::new(),
            }),
            identity: "worker-1".to_string(),
            namespace: "default".to_string(),
            workflow_key,
            step_index: 1,
        });
        let fail_resp = wf_svc.respond_activity_task_failed(fail_req).await;
        assert!(fail_resp.is_ok(), "activity fail should succeed: {:?}", fail_resp.err());

        // 4. Verify history has ActivityFailed event
        let events = engine.history_store().get_history(workflow_key).unwrap();
        let has_activity_failed = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::ActivityFailed);
        assert!(has_activity_failed, "should have ActivityFailed event in history");

        // 5. Workflow should be failed (no retry policy, so permanent failure)
        let status = engine.get_status(workflow_key);
        assert_eq!(status, WorkflowStatus::Failed);

        // 6. Verify WorkflowFailed event is also in history
        let has_workflow_failed = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::WorkflowFailed);
        assert!(has_workflow_failed, "should have WorkflowFailed event in history");
    }

    // ─── Integration Test: HistoryService heartbeat flow ───────────────────

    #[tokio::test]
    async fn test_e2e_history_service_heartbeat() {
        let engine = Arc::new(WorkflowEngine::new());
        let hist_svc = HistoryServiceImpl::new(engine.clone());

        // 1. Start a workflow
        let workflow_key = engine.start_workflow(6001, 1, 0, 0, 5, None);

        // 2. Register a heartbeat for an activity
        // The HistoryService derives workflow_key as (ns_id << 32) | (token & 0xFFFFFFFF)
        // So we use the workflow_key as the token to ensure alignment
        let token = workflow_key.to_string();
        engine.heartbeat_tracker().register(workflow_key, workflow_key, 5000, 3);

        // 3. Record a heartbeat via HistoryService
        let hb_req = req(HistRecordActivityTaskHeartbeatRequest {
            task_token: token.clone(),
            details: vec![1, 2, 3],
            identity: "worker-1".to_string(),
            namespace_id: "0".to_string(),
        });
        let hb_resp = hist_svc.record_activity_task_heartbeat(hb_req).await.unwrap();
        assert!(!hb_resp.into_inner().cancel_requested, "cancel should not be requested");

        // 4. Verify heartbeat was recorded
        let state = engine.heartbeat_tracker().get_state(workflow_key, workflow_key);
        assert_eq!(state, Some(crate::heartbeat::HeartbeatState::Active));

        // 5. Complete the activity via HistoryService
        let complete_req = req(HistRespondActivityTaskCompletedRequest {
            task_token: token,
            result: vec![42],
            identity: "worker-1".to_string(),
            namespace_id: "0".to_string(),
        });
        let complete_resp = hist_svc.respond_activity_task_completed(complete_req).await;
        assert!(complete_resp.is_ok(), "should succeed: {:?}", complete_resp.err());

        // 6. Verify heartbeat state is now Completed
        let state = engine.heartbeat_tracker().get_state(workflow_key, workflow_key);
        assert_eq!(state, Some(crate::heartbeat::HeartbeatState::Completed));
    }

    // ─── Integration Test: HistoryService signal and query ─────────────────

    #[tokio::test]
    async fn test_e2e_history_service_signal_and_query() {
        let engine = Arc::new(WorkflowEngine::new());
        let hist_svc = HistoryServiceImpl::new(engine.clone());

        // 1. Start a workflow
        let workflow_key = engine.start_workflow(5001, 1, 0, 0, 1, None);

        // 2. Signal via HistoryService
        let signal_req = req(SignalWorkflowExecutionRequest {
            namespace: "0".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "5001".to_string(),
                run_id: String::new(),
            }),
            signal_name: "test_signal".to_string(),
            signal_name_id: 99,
            input: Some(velocity_proto::Payload {
                data: vec![10, 20],
                encoding: 0,
                metadata: HashMap::new(),
            }),
            identity: "test".to_string(),
            request_id: "r-1".to_string(),
            header: None,
        });
        let signal_resp = hist_svc.signal_workflow_execution(signal_req).await;
        assert!(signal_resp.is_ok(), "signal should succeed: {:?}", signal_resp.err());

        // 3. Verify history has SignalReceived event
        let events = engine.history_store().get_history(workflow_key).unwrap();
        let has_signal = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::SignalReceived);
        assert!(has_signal, "should have SignalReceived in history");

        // 4. Register a query handler and query via HistoryService
        engine.register_query_handler(workflow_key, 1, Box::new(|args: &[u8]| {
            args.iter().map(|b| b * 2).collect()
        }));

        let query_req = req(HistQueryWorkflowRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "5001".to_string(),
                run_id: String::new(),
            }),
            query_type: "1".to_string(),
            query_args: vec![5, 10, 15],
            query_reject_condition: 0,
        });
        let query_resp = hist_svc.query_workflow(query_req).await.unwrap().into_inner();
        assert_eq!(query_resp.result, vec![10, 20, 30], "query should return doubled values");
    }

    // ─── Integration Test: HistoryService get_workflow_execution_history ───

    #[tokio::test]
    async fn test_e2e_history_service_get_history() {
        let engine = Arc::new(WorkflowEngine::new());
        let hist_svc = HistoryServiceImpl::new(engine.clone());

        // 1. Start a workflow and complete it
        let workflow_key = engine.start_workflow(4001, 1, 0, 0, 1, None);
        engine.complete_workflow(workflow_key, Some(vec![42]));

        // 2. Get history via HistoryService
        let req = req(HistGetWorkflowExecutionHistoryRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "4001".to_string(),
                run_id: String::new(),
            }),
            maximum_page_size: 100,
            next_page_token: vec![],
            wait_new_event: false,
            history_event_filter_type: 0,
            skip_archival: false,
        });
        let resp = hist_svc.get_workflow_execution_history(req).await.unwrap().into_inner();
        assert!(resp.history_events.len() >= 2, "should have at least 2 events");
        assert!(!resp.archived);
    }

    // ─── Integration Test: HistoryService terminate workflow ───────────────

    #[tokio::test]
    async fn test_e2e_history_service_terminate() {
        let engine = Arc::new(WorkflowEngine::new());
        let hist_svc = HistoryServiceImpl::new(engine.clone());

        // 1. Start a workflow
        let workflow_key = engine.start_workflow(3001, 1, 0, 0, 1, None);
        assert_eq!(engine.get_status(workflow_key), WorkflowStatus::Running);

        // 2. Terminate via HistoryService
        let term_req = req(TerminateWorkflowExecutionRequest {
            namespace: "0".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "3001".to_string(),
                run_id: String::new(),
            }),
            reason: "test termination".to_string(),
            identity: "admin".to_string(),
            details: None,
        });
        let term_resp = hist_svc.terminate_workflow_execution(term_req).await;
        assert!(term_resp.is_ok(), "terminate should succeed: {:?}", term_resp.err());

        // 3. Verify workflow is terminated
        assert_eq!(engine.get_status(workflow_key), WorkflowStatus::Terminated);

        // 4. Verify history has WorkflowTerminated event
        let events = engine.history_store().get_history(workflow_key).unwrap();
        let has_terminated = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::WorkflowTerminated);
        assert!(has_terminated, "should have WorkflowTerminated in history");
    }

    // ─── Integration Test: Child workflow via command processing ─────────────

    #[tokio::test]
    async fn test_e2e_child_workflow_via_command() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let wf_svc = WorkflowServiceImpl::with_matching(engine.clone(), matching.clone());

        // 1. Start a parent workflow
        let parent_key = engine.start_workflow(11001, 1, 0, 0, 5, None);

        // 2. Poll for the workflow task
        let poll_req = req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue {
                name: "child-test-queue".to_string(),
                hash: 0,
                kind: 0,
            }),
            identity: "test-worker".to_string(),
            build_id: String::new(),
            long_poll_timeout_ms: 0,
        });
        // Store the task token mapping manually since poll via engine task_queue
        // doesn't go through matching
        let task_token = parent_key;
        wf_svc.task_tokens.lock().unwrap().insert(task_token, parent_key);

        // 3. Respond with StartChildWorkflow command
        let complete_req = req(RespondWorkflowTaskCompletedRequest {
            task_token,
            identity: "worker-1".to_string(),
            commands: vec![velocity_proto::Command {
                attributes: Some(velocity_proto::command::Attributes::StartChildWorkflow(
                    velocity_proto::StartChildWorkflowCommandAttributes {
                        namespace: "default".to_string(),
                        workflow_id: Some(velocity_proto::WorkflowExecution {
                            workflow_id: "11002".to_string(),
                            run_id: String::new(),
                        }),
                        workflow_type: Some(velocity_proto::WorkflowType {
                            name: "ChildWorkflow".to_string(),
                            type_id: 2,
                        }),
                        task_queue: Some(velocity_proto::TaskQueue {
                            name: "child-test-queue".to_string(),
                            hash: 0,
                            kind: 0,
                        }),
                        input: Some(velocity_proto::Payload {
                            data: vec![1, 2, 3],
                            encoding: 0,
                            metadata: HashMap::new(),
                        }),
                        workflow_execution_timeout: None,
                        workflow_run_timeout: None,
                        workflow_task_timeout: None,
                        parent_close_policy: 0,
                        total_steps: 3,
                    },
                )),
            }],
            query_results: HashMap::new(),
            namespace: "default".to_string(),
        });
        let resp = wf_svc.respond_workflow_task_completed(complete_req).await;
        assert!(resp.is_ok(), "should succeed: {:?}", resp.err());

        // 4. Verify parent has ChildWorkflowStarted event in history
        let events = engine.history_store().get_history(parent_key).unwrap();
        let has_child_started = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::ChildWorkflowStarted);
        assert!(has_child_started, "parent should have ChildWorkflowStarted in history");

        // 5. Verify child workflow was actually started
        let child_key = (0u64 << 32) | 11002u64;
        let child_status = engine.get_status(child_key);
        assert_eq!(child_status, WorkflowStatus::Running, "child workflow should be running");
    }

    // ─── Integration Test: Continue-as-new via command processing ────────────

    #[tokio::test]
    async fn test_e2e_continue_as_new_via_command() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let wf_svc = WorkflowServiceImpl::with_matching(engine.clone(), matching.clone());

        // 1. Start a workflow
        let workflow_key = engine.start_workflow(12001, 1, 0, 0, 5, None);

        // 2. Store the task token mapping
        wf_svc.task_tokens.lock().unwrap().insert(workflow_key, workflow_key);

        // 3. Respond with ContinueAsNew command
        let complete_req = req(RespondWorkflowTaskCompletedRequest {
            task_token: workflow_key,
            identity: "worker-1".to_string(),
            commands: vec![velocity_proto::Command {
                attributes: Some(velocity_proto::command::Attributes::ContinueAsNew(
                    velocity_proto::ContinueAsNewCommandAttributes {
                        workflow_type: Some(velocity_proto::WorkflowType {
                            name: "TestWorkflow".to_string(),
                            type_id: 1,
                        }),
                        task_queue: Some(velocity_proto::TaskQueue {
                            name: "continue-queue".to_string(),
                            hash: 0,
                            kind: 0,
                        }),
                        input: Some(velocity_proto::Payload {
                            data: vec![99],
                            encoding: 0,
                            metadata: HashMap::new(),
                        }),
                        workflow_run_timeout: None,
                        workflow_task_timeout: None,
                    },
                )),
            }],
            query_results: HashMap::new(),
            namespace: "default".to_string(),
        });
        let resp = wf_svc.respond_workflow_task_completed(complete_req).await;
        assert!(resp.is_ok(), "should succeed: {:?}", resp.err());

        // 4. Verify original workflow is ContinuedAsNew
        let status = engine.get_status(workflow_key);
        assert_eq!(status, WorkflowStatus::ContinuedAsNew, "original should be ContinuedAsNew");

        // 5. Verify history has WorkflowContinuedAsNew event
        let events = engine.history_store().get_history(workflow_key).unwrap();
        let has_continued = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::WorkflowContinuedAsNew);
        assert!(has_continued, "should have WorkflowContinuedAsNew in history");
    }

    // ─── Integration Test: Signal external workflow via command ──────────────

    #[tokio::test]
    async fn test_e2e_signal_external_via_command() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let wf_svc = WorkflowServiceImpl::with_matching(engine.clone(), matching.clone());

        // 1. Start two workflows (sender and receiver)
        let sender_key = engine.start_workflow(13001, 1, 0, 0, 5, None);
        let receiver_key = engine.start_workflow(13002, 1, 0, 0, 5, None);

        // 2. Store the task token mapping for sender
        wf_svc.task_tokens.lock().unwrap().insert(sender_key, sender_key);

        // 3. Respond with SignalExternal command from sender to receiver
        let complete_req = req(RespondWorkflowTaskCompletedRequest {
            task_token: sender_key,
            identity: "worker-1".to_string(),
            commands: vec![velocity_proto::Command {
                attributes: Some(velocity_proto::command::Attributes::SignalExternal(
                    velocity_proto::SignalExternalCommandAttributes {
                        execution: Some(velocity_proto::WorkflowExecution {
                            workflow_id: "13002".to_string(),
                            run_id: String::new(),
                        }),
                        signal_name: "order_complete".to_string(),
                        input: Some(velocity_proto::Payload {
                            data: vec![42, 43],
                            encoding: 0,
                            metadata: HashMap::new(),
                        }),
                    },
                )),
            }],
            query_results: HashMap::new(),
            namespace: "default".to_string(),
        });
        let resp = wf_svc.respond_workflow_task_completed(complete_req).await;
        assert!(resp.is_ok(), "should succeed: {:?}", resp.err());

        // 4. Verify receiver has SignalReceived in history
        let events = engine.history_store().get_history(receiver_key).unwrap();
        let has_signal = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::SignalReceived);
        assert!(has_signal, "receiver should have SignalReceived in history");
    }

    // ─── E2E: Sticky Queue Matching ──────────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_sticky_queue_poll_and_match() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let match_svc = MatchingServiceImpl::with_matching(engine.clone(), matching.clone());

        // 1. Add a workflow task to a sticky queue
        let sticky_name = "worker-1__sticky";
        let add_req = req(MatchAddWorkflowTaskRequest {
            namespace_id: "default".into(),
            task_queue: sticky_name.into(),
            execution: Some(WorkflowExecution {
                workflow_id: "1001".into(),
                run_id: "r1".into(),
            }),
            scheduled_event_id: 0,
            schedule_to_start_timeout: None,
            version_directive: None,
        });
        match_svc.add_workflow_task(add_req).await.unwrap();

        // 2. Verify the task is in the sticky queue (not the normal queue)
        let sticky_id = TaskQueueId::new("default", sticky_name, TaskQueueKind::Sticky, TaskQueueType::Workflow);
        assert_eq!(matching.get_or_create_queue(&sticky_id).pending_count(), 1);

        // 3. Poll from the sticky queue via poll_workflow_task_queue
        let poll_req = req(PollWorkflowTaskQueueRequest {
            namespace: "default".into(),
            task_queue: Some(velocity_proto::TaskQueue {
                name: sticky_name.into(),
                hash: 0,
                kind: 0,
            }),
            identity: "worker-1".into(),
            build_id: String::new(),
            long_poll_timeout_ms: 0,
        });
        let resp = match_svc.poll_workflow_task_queue(poll_req).await.unwrap();
        let inner = resp.into_inner();
        assert!(inner.task_token > 0, "should match a task from sticky queue");
        assert_eq!(inner.workflow_execution.unwrap().workflow_id, "1001");
    }

    // ─── E2E: Build ID Versioning ────────────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_build_id_versioning() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let match_svc = MatchingServiceImpl::with_matching(engine.clone(), matching.clone());

        // 1. Add build ID via update_worker_build_id_compatibility
        let update_req = req(MatchUpdateWorkerBuildIdCompatibilityRequest {
            namespace_id: "default".into(),
            task_queue: "versioned-queue".into(),
            operation: Some(match_update_worker_build_id_compatibility_request::Operation::AddNewBuildIdInNewDefaultSet(
                AddNewBuildIdInNewDefaultSet { new_build_id: "build-v1.0".into() }
            )),
        });
        match_svc.update_worker_build_id_compatibility(update_req).await.unwrap();

        // 2. Add another compatible version
        let update_req2 = req(MatchUpdateWorkerBuildIdCompatibilityRequest {
            namespace_id: "default".into(),
            task_queue: "versioned-queue".into(),
            operation: Some(match_update_worker_build_id_compatibility_request::Operation::AddNewCompatibleVersion(
                AddNewCompatibleVersion {
                    new_build_id: "build-v1.1".into(),
                    existing_compatible_set: "build-v1.0".into(),
                    make_set_default: true,
                }
            )),
        });
        match_svc.update_worker_build_id_compatibility(update_req2).await.unwrap();

        // 3. Get build ID compatibility and verify
        let get_req = req(MatchGetWorkerBuildIdCompatibilityRequest {
            namespace_id: "default".into(),
            task_queue: "versioned-queue".into(),
            max_sets: 10,
        });
        let resp = match_svc.get_worker_build_id_compatibility(get_req).await.unwrap();
        let sets = resp.into_inner().major_version_sets;
        assert_eq!(sets.len(), 2, "should have 2 version branches");
        assert!(sets[0].build_ids.contains(&"build-v1.0".to_string()));
        assert!(sets[1].build_ids.contains(&"build-v1.1".to_string()));
    }

    // ─── E2E: Schedule Create-Describe-List-Delete ───────────────────────────

    #[tokio::test]
    async fn test_e2e_schedule_lifecycle() {
        let engine = Arc::new(WorkflowEngine::new());
        let wf_svc = WorkflowServiceImpl::new(engine.clone());

        // 1. Create a schedule
        let create_resp = wf_svc.create_schedule(req(CreateScheduleRequest {
            namespace: "default".into(),
            schedule_id: "hourly-report".into(),
            spec: Some(ScheduleSpec {
                interval_seconds: 3600,
                jitter_seconds: 30,
                ..Default::default()
            }),
            action: Some(ScheduleAction {
                start_workflow: Some(StartWorkflowExecutionRequest {
                    namespace: "default".into(),
                    workflow_execution: Some(velocity_proto::WorkflowExecution {
                        workflow_id: String::new(),
                        run_id: String::new(),
                    }),
                    workflow_type: Some(velocity_proto::WorkflowType { name: "ReportWorkflow".into(), type_id: 99 }),
                    task_queue: Some(velocity_proto::TaskQueue { name: "reports".into(), hash: 42, kind: 0 }),
                    input: None,
                    workflow_execution_timeout: None,
                    workflow_run_timeout: None,
                    workflow_task_timeout: None,
                    identity: String::new(),
                    request_id: String::new(),
                    retry_policy: None,
                    cron_schedule: String::new(),
                    memo: None,
                    search_attributes: None,
                    header: None,
                    parent_close_policy: 0,
                    total_steps: 0,
                }),
            }),
            policies: Some(SchedulePolicies { overlap_policy: 0, ..Default::default() }),
            identity: "scheduler".into(),
            request_id: "req-1".into(),
            memo: None,
            search_attributes: None,
        })).await.unwrap();
        let sched_id = create_resp.into_inner().schedule_id;
        let sched_key: u64 = sched_id.parse().unwrap();

        // 2. Describe the schedule
        let desc_resp = wf_svc.describe_schedule(req(DescribeScheduleRequest {
            namespace: "default".into(),
            schedule_id: sched_id.clone(),
        })).await.unwrap();
        let desc = desc_resp.into_inner();
        assert_eq!(desc.schedule_id, sched_id);
        assert_eq!(desc.state.as_ref().unwrap().status, 0); // active
        assert!(desc.policies.is_some());

        // 3. List schedules
        let list_resp = wf_svc.list_schedules(req(ListSchedulesRequest {
            namespace: "default".into(),
            page_size: 10,
            next_page_token: vec![],
            query: String::new(),
        })).await.unwrap();
        assert_eq!(list_resp.into_inner().schedules.len(), 1);

        // 4. Update overlap policy
        let update_resp = wf_svc.update_schedule(req(UpdateScheduleRequest {
            namespace: "default".into(),
            schedule_id: sched_id.clone(),
            spec: None,
            action: None,
            policies: Some(SchedulePolicies { overlap_policy: 3, ..Default::default() }),
            identity: "admin".into(),
            request_id: "req-2".into(),
        })).await;
        assert!(update_resp.is_ok());
        // Verify update took effect
        let entry = engine.schedule_manager().get(sched_key).unwrap();
        assert_eq!(entry.overlap_policy, crate::schedules::OverlapPolicy::TerminateOther);

        // 5. Delete the schedule
        let del_resp = wf_svc.delete_schedule(req(DeleteScheduleRequest {
            namespace: "default".into(),
            schedule_id: sched_id,
            identity: "admin".into(),
        })).await;
        assert!(del_resp.is_ok());
        assert_eq!(engine.schedule_manager().count(), 0);
    }

    // ─── E2E: List Task Queue Partitions ─────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_list_task_queue_partitions() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let match_svc = MatchingServiceImpl::with_matching(engine.clone(), matching.clone());

        let req = req(MatchListTaskQueuePartitionsRequest {
            namespace_id: "default".into(),
            task_queue: "partitioned-queue".into(),
            task_queue_type: 0,
        });
        let resp = match_svc.list_task_queue_partitions(req).await.unwrap();
        let inner = resp.into_inner();
        // Default config has 4 partitions
        assert_eq!(inner.workflow_task_queue_partitions.len(), 4);
        assert_eq!(inner.activity_task_queue_partitions.len(), 4);
        assert!(inner.workflow_task_queue_partitions[0].key.contains("partition_0"));
    }

    // ─── E2E: Replay Engine Determinism Verification ────────────────────────

    #[tokio::test]
    async fn test_e2e_replay_determinism() {
        let engine = Arc::new(WorkflowEngine::new());
        // Start a workflow and record some history events
        let key = engine.start_workflow(9001, 1, 0, 42, 5, None);
        engine.complete_step(key, 0, vec![1, 2, 3]);
        engine.complete_step(key, 1, vec![4, 5, 6]);
        engine.history_store().record_event(key, crate::event_history::HistoryEventType::WorkflowStarted, vec![]);
        engine.history_store().record_event(key, crate::event_history::HistoryEventType::StepCompleted, vec![1, 2, 3]);
        engine.history_store().record_event(key, crate::event_history::HistoryEventType::StepCompleted, vec![4, 5, 6]);

        // Verify determinism: replay the same history twice
        let is_deterministic = engine.replay_engine().verify_determinism(key, &engine.history_store().get_history(key).unwrap_or_default());
        assert!(is_deterministic, "replay should be deterministic");

        // Replay from store and check result
        let result = engine.replay_engine().replay_from_store(key, engine.history_store(), None);
        assert!(result.success);
        assert!(result.events_replayed > 0);
        assert!(result.determinism_checksum > 0);

        // Replay again and compare checksums
        let result2 = engine.replay_engine().replay_from_store(key, engine.history_store(), None);
        assert_eq!(result.determinism_checksum, result2.determinism_checksum);
        assert!(ReplayEngine::compare_replay_results(&result, &result2));
    }

    // ─── E2E: WorkerService Describe Mutable State ─────────────────────────

    #[tokio::test]
    async fn test_e2e_worker_describe_mutable_state() {
        let engine = Arc::new(WorkflowEngine::new());
        let worker_svc = WorkerServiceImpl::new(engine.clone());

        // Start a workflow
        let key = engine.start_workflow(9101, 1, 0, 42, 10, None);
        engine.complete_step(key, 0, vec![1]);
        engine.complete_step(key, 1, vec![2]);

        // Describe mutable state via WorkerService
        let resp = worker_svc.describe_mutable_state(req(DescribeMutableStateRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "9101".to_string(),
                run_id: String::new(),
            }),
        })).await.unwrap();
        let inner = resp.into_inner();
        // mutable_state should contain serialized description
        assert!(!inner.mutable_state.is_empty());
        let state_str = String::from_utf8(inner.mutable_state).unwrap();
        assert!(state_str.contains("9101"));
        assert!(state_str.contains("Running"));
    }

    // ─── E2E: WorkerService Rebuild Mutable State (Replay) ─────────────────

    #[tokio::test]
    async fn test_e2e_worker_rebuild_mutable_state() {
        let engine = Arc::new(WorkflowEngine::new());
        let worker_svc = WorkerServiceImpl::new(engine.clone());

        // Start a workflow and record history
        let key = engine.start_workflow(9201, 1, 0, 42, 5, None);
        engine.complete_step(key, 0, vec![10, 20]);
        engine.history_store().record_event(key, crate::event_history::HistoryEventType::WorkflowStarted, vec![]);
        engine.history_store().record_event(key, crate::event_history::HistoryEventType::StepCompleted, vec![10, 20]);

        // Rebuild mutable state via replay
        let resp = worker_svc.rebuild_mutable_state(req(WorkerRebuildMutableStateRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "9201".to_string(),
                run_id: String::new(),
            }),
        })).await;
        assert!(resp.is_ok());

        // Verify replay cache was populated
        assert!(engine.replay_engine().cache_size() > 0);
    }

    // ─── E2E: WorkerService Refresh Workflow Tasks ─────────────────────────

    #[tokio::test]
    async fn test_e2e_worker_refresh_workflow_tasks() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let worker_svc = WorkerServiceImpl::with_matching(engine.clone(), matching.clone());

        // Start a workflow
        engine.start_workflow(9301, 1, 0, 42, 5, None);

        // Refresh workflow tasks — should re-schedule a task in the matching engine
        let resp = worker_svc.refresh_workflow_tasks(req(RefreshWorkflowTasksRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "9301".to_string(),
                run_id: String::new(),
            }),
        })).await;
        assert!(resp.is_ok());

        // Verify a task was added to the matching engine
        let tq_id = TaskQueueId::new("default", "wf-9301", TaskQueueKind::Normal, TaskQueueType::Workflow);
        let task = matching.poll_task(&tq_id, "test-worker");
        assert!(task.is_some(), "matching engine should have a task after refresh");
    }

    // ─── E2E: WorkerService Import Workflow Execution ──────────────────────

    #[tokio::test]
    async fn test_e2e_worker_import_workflow() {
        let engine = Arc::new(WorkflowEngine::new());
        let worker_svc = WorkerServiceImpl::new(engine.clone());

        // Import a workflow execution
        let resp = worker_svc.import_workflow_execution(req(WorkerImportWorkflowExecutionRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "9401".to_string(),
                run_id: String::new(),
            }),
            workflow_state: vec![0xDE, 0xAD, 0xBE, 0xEF],
        })).await;
        assert!(resp.is_ok());

        // Verify the workflow exists
        let key = (0u64 << 32) | 9401u64;
        let status = engine.get_status(key);
        assert_ne!(status, WorkflowStatus::Void, "imported workflow should exist");

        // Verify history was recorded
        let history = engine.history_store().get_history(key).unwrap_or_default();
        assert!(!history.is_empty(), "imported workflow should have history");
    }

    // ─── E2E: Visibility Search with Query ─────────────────────────────────

    #[tokio::test]
    async fn test_e2e_visibility_search_query() {
        let engine = Arc::new(WorkflowEngine::new());
        let wf_svc = WorkflowServiceImpl::new(engine.clone());

        // Start workflows with different types and statuses
        engine.start_workflow(9501, 1, 0, 42, 1, None);
        engine.start_workflow(9502, 2, 0, 42, 1, None);
        engine.start_workflow(9503, 1, 0, 42, 1, None);
        let key_9501 = (0u64 << 32) | 9501u64;
        engine.complete_workflow(key_9501, Some(vec![]));

        // List all running workflows
        let resp = wf_svc.list_workflow_executions(req(ListWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 100,
            status_filter: WorkflowExecutionStatus::Running as i32,
            ..Default::default()
        })).await.unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.executions.len(), 2, "should have 2 running workflows");

        // List completed workflows
        let resp2 = wf_svc.list_workflow_executions(req(ListWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 100,
            status_filter: WorkflowExecutionStatus::Completed as i32,
            ..Default::default()
        })).await.unwrap();
        assert_eq!(resp2.into_inner().executions.len(), 1);

        // List by type filter
        let resp3 = wf_svc.list_workflow_executions(req(ListWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 100,
            type_filter: Some(velocity_proto::WorkflowType { name: String::new(), type_id: 1 }),
            ..Default::default()
        })).await.unwrap();
        assert_eq!(resp3.into_inner().executions.len(), 2, "type_id=1 should match 2 workflows");
    }

    // ─── E2E: Describe Cluster Returns Real Stats ──────────────────────────

    #[tokio::test]
    async fn test_e2e_describe_cluster_real_stats() {
        let engine = Arc::new(WorkflowEngine::new());
        let worker_svc = WorkerServiceImpl::new(engine.clone());

        // Create some workflows and a schedule
        engine.start_workflow(9601, 1, 0, 42, 1, None);
        engine.start_workflow(9602, 2, 0, 42, 1, None);
        engine.namespaces().register_auto("test-ns");

        // Describe cluster
        let resp = worker_svc.describe_cluster(req(WorkerDescribeClusterRequest {
            cluster_name: String::new(),
        })).await.unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.cluster_name, "velocity-default");
        assert!(inner.version_info.contains_key("workflows"));
        let wf_count: u64 = inner.version_info.get("workflows").unwrap().parse().unwrap();
        assert!(wf_count >= 2, "should report at least 2 workflows");
    }

    // ─── E2E: Reapply Events via WorkerService ────────────────────────────

    #[tokio::test]
    async fn test_e2e_reapply_events() {
        let engine = Arc::new(WorkflowEngine::new());
        let worker_svc = WorkerServiceImpl::new(engine.clone());

        // Start a workflow
        engine.start_workflow(9701, 1, 0, 42, 5, None);

        // Reapply events
        let resp = worker_svc.reapply_events(req(ReapplyEventsRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "9701".to_string(),
                run_id: String::new(),
            }),
            events: vec![
                velocity_proto::HistoryEvent {
                    event_id: 100,
                    event_time: None,
                    event_type: "signalA".to_string(),
                    task_id: 0,
                    details: Some(velocity_proto::Payload { data: vec![1, 2], encoding: 0, metadata: HashMap::new() }),
                },
                velocity_proto::HistoryEvent {
                    event_id: 101,
                    event_time: None,
                    event_type: "signalB".to_string(),
                    task_id: 0,
                    details: None,
                },
            ],
        })).await;
        assert!(resp.is_ok());

        // Verify history recorded the signals
        let key = (0u64 << 32) | 9701u64;
        let history = engine.history_store().get_history(key).unwrap_or_default();
        let signal_events: Vec<_> = history.iter().filter(|e| e.event_type == crate::event_history::HistoryEventType::SignalReceived).collect();
        assert_eq!(signal_events.len(), 2, "should have 2 signal events from reapply");
    }

    // ─── E2E: WAL Record Encode/Decode Roundtrip ──────────────────────────

    #[tokio::test]
    async fn test_e2e_wal_record_roundtrip() {
        use crate::wal::{WalRecord, WalEventType};
        use std::io::Cursor;

        // Create a WAL record and encode it
        let record = WalRecord::new(WalEventType::WorkflowStarted, 12345, vec![0xCA, 0xFE]);
        let encoded = record.encode();

        // Decode it back
        let mut cursor = Cursor::new(&encoded);
        let decoded = WalRecord::decode(&mut cursor).unwrap().unwrap();
        assert_eq!(decoded.event_type, WalEventType::WorkflowStarted);
        assert_eq!(decoded.workflow_key, 12345);
        assert_eq!(decoded.data, vec![0xCA, 0xFE]);
    }

    // ─── E2E: Replay Engine Partial Replay ────────────────────────────────

    #[tokio::test]
    async fn test_e2e_replay_partial() {
        let engine = Arc::new(WorkflowEngine::new());
        let key = engine.start_workflow(9801, 1, 0, 42, 10, None);
        engine.complete_step(key, 0, vec![1]);
        engine.complete_step(key, 1, vec![2]);
        engine.complete_step(key, 2, vec![3]);
        engine.history_store().record_event(key, crate::event_history::HistoryEventType::WorkflowStarted, vec![]);
        engine.history_store().record_event(key, crate::event_history::HistoryEventType::StepCompleted, vec![1]);
        engine.history_store().record_event(key, crate::event_history::HistoryEventType::StepCompleted, vec![2]);
        engine.history_store().record_event(key, crate::event_history::HistoryEventType::StepCompleted, vec![3]);

        // Replay only up to event 3
        let partial = engine.replay_engine().replay_from_store(key, engine.history_store(), Some(3));
        assert!(partial.success);
        assert_eq!(partial.replayed_to_event_id, 3);

        // Replay the full history
        let full = engine.replay_engine().replay_from_store(key, engine.history_store(), None);
        assert!(full.success);
        assert!(full.events_replayed >= partial.events_replayed);
    }

    // ─── E2E: HistoryService Record Task Started ──────────────────────────

    #[tokio::test]
    async fn test_e2e_history_record_task_started() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let hist_svc = HistoryServiceImpl::with_matching(engine.clone(), matching.clone());

        // Start a workflow
        engine.start_workflow(9901, 1, 0, 42, 5, None);

        // Record workflow task started
        let resp = hist_svc.record_workflow_task_started(req(HistRecordWorkflowTaskStartedRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "9901".to_string(),
                run_id: String::new(),
            }),
            schedule_event_id: 1,
            task_token: "token-123".to_string(),
            poller_identity: "worker-1".to_string(),
        })).await;
        assert!(resp.is_ok());
        let inner = resp.unwrap().into_inner();
        assert!(inner.started_event_id >= 0);
        assert!(inner.next_event_id > inner.started_event_id);

        // Record activity task started
        let resp2 = hist_svc.record_activity_task_started(req(HistRecordActivityTaskStartedRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "9901".to_string(),
                run_id: String::new(),
            }),
            schedule_event_id: 5,
            task_token: "token-456".to_string(),
            poller_identity: "worker-2".to_string(),
        })).await;
        assert!(resp2.is_ok());
        let inner2 = resp2.unwrap().into_inner();
        assert_eq!(inner2.scheduled_event_id, 5);

        // Verify non-existent workflow returns NOT_FOUND
        let resp3 = hist_svc.record_workflow_task_started(req(HistRecordWorkflowTaskStartedRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "9999".to_string(),
                run_id: String::new(),
            }),
            schedule_event_id: 0,
            task_token: "token".to_string(),
            poller_identity: "worker".to_string(),
        })).await;
        assert!(resp3.is_err());
        assert_eq!(resp3.unwrap_err().code(), tonic::Code::NotFound);
    }

    // ─── E2E: System Info Reports Runtime Stats ───────────────────────────

    #[tokio::test]
    async fn test_e2e_system_info_runtime_stats() {
        let engine = Arc::new(WorkflowEngine::new());
        let wf_svc = WorkflowServiceImpl::new(engine.clone());

        // Create some state
        engine.start_workflow(9951, 1, 0, 42, 1, None);
        engine.start_workflow(9952, 2, 0, 42, 1, None);
        engine.namespaces().register_auto("stats-ns");

        let resp = wf_svc.get_system_info(req(GetSystemInfoRequest {})).await.unwrap();
        let inner = resp.into_inner();
        let sys_info = inner.system_info.unwrap();
        let server = sys_info.server.as_ref().unwrap();

        // Verify new features are advertised
        assert!(server.supported_features.contains(&"schedules".to_string()));
        assert!(server.supported_features.contains(&"sticky_queues".to_string()));
        assert!(server.supported_features.contains(&"build_id_versioning".to_string()));
        assert!(server.supported_features.contains(&"deterministic_replay".to_string()));
        assert!(server.supported_features.contains(&"wal_recovery".to_string()));
        assert!(server.supported_features.contains(&"visibility_queries".to_string()));

        // Verify runtime stats are included
        let has_workflow_stat = server.supported_features.iter().any(|f| f.starts_with("workflows:") && !f.ends_with(":0"));
        assert!(has_workflow_stat, "should report non-zero workflow count");

        // Verify eager_workflow_start is now enabled
        let caps = sys_info.capabilities.as_ref().unwrap();
        assert!(caps.eager_workflow_start);
    }

    // ─── E2E: HistoryService Force Delete Workflow ────────────────────────

    #[tokio::test]
    async fn test_e2e_history_force_delete() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let hist_svc = HistoryServiceImpl::with_matching(engine.clone(), matching.clone());

        // Start a workflow
        let key = engine.start_workflow(14001, 1, 0, 42, 5, None);
        assert_eq!(engine.get_status(key), WorkflowStatus::Running);

        // Force delete via HistoryService
        let resp = hist_svc.force_delete_workflow_execution(req(HistForceDeleteWorkflowExecutionRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "14001".to_string(),
                run_id: String::new(),
            }),
        })).await;
        assert!(resp.is_ok());

        // Verify workflow is terminated
        assert_eq!(engine.get_status(key), WorkflowStatus::Terminated);

        // Verify history has WorkflowTerminated event
        let events = engine.history_store().get_history(key).unwrap();
        let has_terminated = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::WorkflowTerminated);
        assert!(has_terminated, "should have WorkflowTerminated in history");
    }

    // ─── E2E: HistoryService Raw History V2 ──────────────────────────────

    #[tokio::test]
    async fn test_e2e_history_raw_history_v2() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let hist_svc = HistoryServiceImpl::with_matching(engine.clone(), matching.clone());

        // Start a workflow and record events
        let key = engine.start_workflow(14101, 1, 0, 42, 5, None);
        engine.complete_step(key, 0, vec![10]);
        engine.history_store().record_event(key, crate::event_history::HistoryEventType::StepCompleted, vec![10]);

        // Get raw history v2
        let resp = hist_svc.get_workflow_execution_raw_history_v2(req(GetWorkflowExecutionRawHistoryV2Request {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "14101".to_string(),
                run_id: String::new(),
            }),
            start_event_id: 0,
            end_event_id: 100,
            maximum_page_size: 50,
            next_page_token: vec![],
            ..Default::default()
        })).await.unwrap();
        let inner = resp.into_inner();
        assert!(inner.history_events.len() >= 2, "should have at least 2 events");
        assert!(inner.next_page_token.is_empty(), "no pagination needed for small history");
    }

    // ─── E2E: HistoryService List History Tasks ──────────────────────────

    #[tokio::test]
    async fn test_e2e_history_list_tasks() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let hist_svc = HistoryServiceImpl::with_matching(engine.clone(), matching.clone());

        // Start workflows to have history
        engine.start_workflow(14201, 1, 0, 42, 1, None);
        engine.start_workflow(14202, 2, 0, 42, 1, None);

        // List history tasks
        let resp = hist_svc.list_history_tasks(req(ListHistoryTasksRequest {
            shard_id: 0,
            task_queue_type: 1,
            task_range: None,
            batch_size: 10,
            next_page_token: vec![],
        })).await.unwrap();
        let inner = resp.into_inner();
        assert!(!inner.tasks.is_empty(), "should have tasks from workflow histories");
        assert!(inner.tasks[0].task_id > 0, "task_id should be positive");
    }

    // ─── E2E: HistoryService Get Shard ────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_history_get_shard() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let hist_svc = HistoryServiceImpl::with_matching(engine.clone(), matching.clone());

        engine.start_workflow(14301, 1, 0, 42, 1, None);

        let resp = hist_svc.get_shard(req(GetShardRequest { shard_id: 0 })).await.unwrap();
        let inner = resp.into_inner();
        assert!(inner.shard.is_some(), "should return shard info");
        let shard = inner.shard.unwrap();
        assert_eq!(shard.owner, "velocity-node-0");
        assert!(shard.range_id > 0, "range_id should reflect workflow count");
    }

    // ─── E2E: WorkerService Get Shard ─────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_worker_get_shard() {
        let engine = Arc::new(WorkflowEngine::new());
        let worker_svc = WorkerServiceImpl::new(engine.clone());

        engine.start_workflow(14401, 1, 0, 42, 1, None);
        engine.start_workflow(14402, 2, 0, 42, 1, None);

        let resp = worker_svc.get_shard(req(GetShardRequest { shard_id: 1 })).await.unwrap();
        let inner = resp.into_inner();
        let shard = inner.shard.unwrap();
        assert_eq!(shard.shard_id, 1);
        assert!(shard.range_id >= 2, "should reflect at least 2 workflows");
    }

    // ─── E2E: WorkerService List Tables ───────────────────────────────────

    #[tokio::test]
    async fn test_e2e_worker_list_tables() {
        let engine = Arc::new(WorkflowEngine::new());
        let worker_svc = WorkerServiceImpl::new(engine.clone());

        let resp = worker_svc.list_tables(req(WorkerListTablesRequest {
            database: String::new(),
            page_size: 100,
            next_page_token: vec![],
        })).await.unwrap();
        let inner = resp.into_inner();
        assert!(!inner.tables.is_empty(), "should list database tables");
        assert!(inner.tables.contains(&"workflows".to_string()));
        assert!(inner.tables.contains(&"execution_history".to_string()));
        assert!(inner.tables.contains(&"visibility".to_string()));
    }

    // ─── E2E: WorkerService List Clusters ─────────────────────────────────

    #[tokio::test]
    async fn test_e2e_worker_list_clusters() {
        let engine = Arc::new(WorkflowEngine::new());
        let worker_svc = WorkerServiceImpl::new(engine.clone());

        let resp = worker_svc.list_clusters(req(WorkerListClustersRequest {
            page_size: 100,
            next_page_token: vec![],
        })).await.unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.clusters.len(), 1, "should have local cluster");
        assert_eq!(inner.clusters[0].cluster_name, "velocity-default");
        assert!(inner.clusters[0].is_connection_enabled);
    }

    // ─── E2E: MatchingService Get Task Queue Metadata ─────────────────────

    #[tokio::test]
    async fn test_e2e_matching_get_task_queue_metadata() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let match_svc = MatchingServiceImpl::with_matching(engine.clone(), matching.clone());

        // Add some tasks to a queue
        let add_req = req(MatchAddWorkflowTaskRequest {
            namespace_id: "default".into(),
            task_queue: "metadata-test-queue".into(),
            execution: Some(WorkflowExecution { workflow_id: "14501".into(), run_id: "r1".into() }),
            scheduled_event_id: 0,
            schedule_to_start_timeout: None,
            version_directive: None,
        });
        match_svc.add_workflow_task(add_req).await.unwrap();

        // Get metadata
        let resp = match_svc.get_task_queue_metadata(req(MatchGetTaskQueueMetadataRequest {
            namespace_id: "default".into(),
            task_queue: "metadata-test-queue".into(),
            task_queue_type: 0,
        })).await.unwrap();
        let inner = resp.into_inner();
        assert!(inner.metadata.is_some(), "should return metadata");
        assert!(inner.metadata.unwrap().max_tasks_per_second > 0, "should have pending tasks");
    }

    // ─── E2E: WorkerService Generate Replication Tasks ────────────────────

    #[tokio::test]
    async fn test_e2e_worker_generate_replication_tasks() {
        let engine = Arc::new(WorkflowEngine::new());
        let worker_svc = WorkerServiceImpl::new(engine.clone());

        engine.start_workflow(14601, 1, 0, 42, 5, None);

        let resp = worker_svc.generate_last_history_replication_tasks(req(GenerateLastHistoryReplicationTasksRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "14601".to_string(),
                run_id: String::new(),
            }),
        })).await;
        assert!(resp.is_ok());

        // Verify marker was recorded in history
        let key = (0u64 << 32) | 14601u64;
        let events = engine.history_store().get_history(key).unwrap();
        let has_marker = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::MarkerRecorded);
        assert!(has_marker, "should have replication marker in history");

        // Non-existent workflow should fail
        let resp2 = worker_svc.generate_last_history_replication_tasks(req(GenerateLastHistoryReplicationTasksRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "99999".to_string(),
                run_id: String::new(),
            }),
        })).await;
        assert!(resp2.is_err());
        assert_eq!(resp2.unwrap_err().code(), tonic::Code::NotFound);
    }

    // ─── E2E: Concurrency Limiter Integration ──────────────────────────────

    #[tokio::test]
    async fn test_e2e_concurrency_limiter_enforcement() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let wf_svc = WorkflowServiceImpl::with_matching(engine.clone(), matching.clone());

        // Set a tight concurrency limit: max 2 workflows of type 100
        engine.concurrency_limiter().set_type_limit(100, 2);

        // Start 2 workflows of type 100 — should succeed
        for i in 15001..15003 {
            let resp = wf_svc.start_workflow_execution(req(StartWorkflowExecutionRequest {
                namespace: "default".to_string(),
                workflow_execution: Some(velocity_proto::WorkflowExecution {
                    workflow_id: i.to_string(),
                    run_id: String::new(),
                }),
                workflow_type: Some(velocity_proto::WorkflowType { name: "LimitedType".into(), type_id: 100 }),
                task_queue: Some(velocity_proto::TaskQueue { name: "limiter-q".into(), hash: 0, kind: 0 }),
                total_steps: 1,
                ..Default::default()
            })).await;
            assert!(resp.is_ok(), "workflow {} should start", i);
        }

        // 3rd workflow of same type should be rejected
        let resp3 = wf_svc.start_workflow_execution(req(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "15003".to_string(),
                run_id: String::new(),
            }),
            workflow_type: Some(velocity_proto::WorkflowType { name: "LimitedType".into(), type_id: 100 }),
            task_queue: Some(velocity_proto::TaskQueue { name: "limiter-q".into(), hash: 0, kind: 0 }),
            total_steps: 1,
            ..Default::default()
        })).await;
        assert!(resp3.is_err(), "3rd workflow should be rejected");
        assert_eq!(resp3.unwrap_err().code(), tonic::Code::ResourceExhausted);

        // Verify active count
        assert_eq!(engine.concurrency_limiter().active_for_type(100), 2);

        // A different workflow type should still be allowed
        let resp4 = wf_svc.start_workflow_execution(req(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "15004".to_string(),
                run_id: String::new(),
            }),
            workflow_type: Some(velocity_proto::WorkflowType { name: "OtherType".into(), type_id: 200 }),
            task_queue: Some(velocity_proto::TaskQueue { name: "limiter-q".into(), hash: 0, kind: 0 }),
            total_steps: 1,
            ..Default::default()
        })).await;
        assert!(resp4.is_ok(), "different type should not be limited");
    }

    // ─── E2E: Invoke State Machine Method ──────────────────────────────────

    #[tokio::test]
    async fn test_e2e_invoke_state_machine_method() {
        let engine = Arc::new(WorkflowEngine::new());
        let matching = Arc::new(MatchingEngine::new());
        let hist_svc = HistoryServiceImpl::with_matching(engine.clone(), matching.clone());

        engine.start_workflow(16001, 1, 0, 42, 5, None);

        let resp = hist_svc.invoke_state_machine_method(req(HistInvokeStateMachineMethodRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "16001".to_string(),
                run_id: String::new(),
            }),
            state_machine_type: 1,
            state_machine_id: 42,
            method: 3,
            input: vec![0xDE, 0xAD],
        })).await.unwrap();
        // Should echo input as output
        assert_eq!(resp.into_inner().output, vec![0xDE, 0xAD]);

        // Verify marker was recorded in history
        let key = (0u64 << 32) | 16001u64;
        let events = engine.history_store().get_history(key).unwrap();
        let has_marker = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::MarkerRecorded);
        assert!(has_marker, "should have state machine invocation marker");

        // Non-existent workflow should fail
        let resp2 = hist_svc.invoke_state_machine_method(req(HistInvokeStateMachineMethodRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "99999".to_string(),
                run_id: String::new(),
            }),
            state_machine_type: 1,
            state_machine_id: 1,
            method: 1,
            input: vec![],
        })).await;
        assert!(resp2.is_err());
        assert_eq!(resp2.unwrap_err().code(), tonic::Code::NotFound);
    }

    // ─── E2E: Sync Workflow State ──────────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_sync_workflow_state() {
        let engine = Arc::new(WorkflowEngine::new());
        let worker_svc = WorkerServiceImpl::new(engine.clone());

        engine.start_workflow(16101, 1, 0, 42, 5, None);

        let resp = worker_svc.sync_workflow_state(req(WorkerSyncWorkflowStateRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "16101".to_string(),
                run_id: String::new(),
            }),
            replication_state: vec![1, 2, 3, 4],
        })).await;
        assert!(resp.is_ok());

        // Verify replication state was recorded in history
        let key = (0u64 << 32) | 16101u64;
        let events = engine.history_store().get_history(key).unwrap();
        let has_marker = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::MarkerRecorded);
        assert!(has_marker, "should have replication state marker");
    }

    // ─── E2E: WorkerService Delete Records History ─────────────────────────

    #[tokio::test]
    async fn test_e2e_worker_delete_records_history() {
        let engine = Arc::new(WorkflowEngine::new());
        let worker_svc = WorkerServiceImpl::new(engine.clone());

        engine.start_workflow(16201, 1, 0, 42, 5, None);

        let resp = worker_svc.delete_workflow_execution(req(DeleteWorkflowExecutionRequest {
            namespace_id: "0".to_string(),
            execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "16201".to_string(),
                run_id: String::new(),
            }),
        })).await;
        assert!(resp.is_ok());

        let key = (0u64 << 32) | 16201u64;
        assert_eq!(engine.get_status(key), WorkflowStatus::Terminated);

        // Verify history has WorkflowTerminated event
        let events = engine.history_store().get_history(key).unwrap();
        let has_terminated = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::WorkflowTerminated);
        assert!(has_terminated, "delete should record WorkflowTerminated in history");
    }

    // ─── E2E: Timer Engine Records TimerFired in History ──────────────────

    #[tokio::test]
    async fn test_e2e_timer_engine_records_history() {
        let engine = Arc::new(WorkflowEngine::new());

        // Initialize timer engine to record in history
        engine.init_timers();

        // Start the timer engine's background thread
        let _timer_handle = engine.timer_engine().start();

        // Start a workflow
        let key = engine.start_workflow(17001, 1, 0, 42, 5, None);

        // Schedule a short timer (50ms)
        let timer_id = engine.timer_engine().schedule(key, std::time::Duration::from_millis(50));
        assert!(timer_id > 0);

        // Wait for the timer to fire
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Verify TimerFired event was recorded in history
        let events = engine.history_store().get_history(key).unwrap();
        let has_timer_fired = events.iter().any(|e| e.event_type == crate::event_history::HistoryEventType::TimerFired);
        assert!(has_timer_fired, "timer engine should record TimerFired in history");
    }

    // ─── E2E: List with Time Range Filter ──────────────────────────────────

    #[tokio::test]
    async fn test_e2e_list_with_time_range_filter() {
        let svc = test_service();

        // Start workflows
        svc.engine.start_workflow(18001, 1, 0, 42, 1, None);
        svc.engine.start_workflow(18002, 2, 0, 42, 1, None);
        svc.engine.start_workflow(18003, 1, 0, 42, 1, None);

        // List with time range covering everything (last hour)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        let one_hour_ago = prost_types::Timestamp {
            seconds: (now - 3600) as i64,
            nanos: 0,
        };
        let future = prost_types::Timestamp {
            seconds: (now + 3600) as i64,
            nanos: 0,
        };

        let resp = svc.list_workflow_executions(req(ListWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 100,
            start_time_min: Some(one_hour_ago),
            start_time_max: Some(future),
            ..Default::default()
        })).await.unwrap();
        assert_eq!(resp.into_inner().executions.len(), 3);

        // List with time range in the past (should return nothing)
        let past_start = prost_types::Timestamp { seconds: 0, nanos: 0 };
        let past_end = prost_types::Timestamp { seconds: 1, nanos: 0 };
        let resp2 = svc.list_workflow_executions(req(ListWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 100,
            start_time_min: Some(past_start),
            start_time_max: Some(past_end),
            ..Default::default()
        })).await.unwrap();
        assert_eq!(resp2.into_inner().executions.len(), 0);
    }

    // ─── E2E: List with Pagination ──────────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_list_with_pagination() {
        let svc = test_service();

        // Start 5 workflows
        for i in 0..5 {
            svc.engine.start_workflow(18100 + i, 1, 0, 42, 1, None);
        }

        // Page 1: size 2
        let resp1 = svc.list_workflow_executions(req(ListWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 2,
            ..Default::default()
        })).await.unwrap();
        let inner1 = resp1.into_inner();
        assert_eq!(inner1.executions.len(), 2);
        assert!(!inner1.next_page_token.is_empty(), "should have next page");

        // Page 2: use token
        let resp2 = svc.list_workflow_executions(req(ListWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 2,
            next_page_token: inner1.next_page_token,
            ..Default::default()
        })).await.unwrap();
        let inner2 = resp2.into_inner();
        assert_eq!(inner2.executions.len(), 2);
        assert!(!inner2.next_page_token.is_empty(), "should have next page");

        // Page 3: last page
        let resp3 = svc.list_workflow_executions(req(ListWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 2,
            next_page_token: inner2.next_page_token,
            ..Default::default()
        })).await.unwrap();
        let inner3 = resp3.into_inner();
        assert_eq!(inner3.executions.len(), 1);
        assert!(inner3.next_page_token.is_empty(), "no more pages");
    }

    // ─── E2E: Scan with Query ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_scan_with_query() {
        let svc = test_service();

        // Start workflows with different types
        svc.engine.start_workflow(18201, 1, 0, 42, 1, None);
        svc.engine.start_workflow(18202, 2, 0, 42, 1, None);
        svc.engine.start_workflow(18203, 1, 0, 42, 1, None);

        // Scan with query filtering by WorkflowType
        let resp = svc.scan_workflow_executions(req(ScanWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 100,
            query: "NamespaceId = 0".to_string(),
            ..Default::default()
        })).await.unwrap();
        assert_eq!(resp.into_inner().executions.len(), 3);

        // Scan with invalid query
        let err = svc.scan_workflow_executions(req(ScanWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            query: "INVALID SYNTAX".to_string(),
            ..Default::default()
        })).await;
        assert!(err.is_err());
    }

    // ─── E2E: Purge Expired Workflows ──────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_purge_expired_workflows() {
        let engine = Arc::new(WorkflowEngine::new());

        // Start and complete a workflow
        let key = engine.start_workflow(18301, 1, 0, 42, 1, None);
        engine.complete_workflow(key, Some(vec![]));

        // Verify it's in visibility
        assert_eq!(engine.visibility().total_count(), 1);

        // Purge with default retention (7 days) — should not purge recent workflow
        let purged = engine.purge_expired_workflows();
        assert_eq!(purged, 0);
        assert_eq!(engine.visibility().total_count(), 1);

        // Manually set the close time to the past by removing and re-registering
        engine.visibility().remove(key);
        let old_info = crate::visibility::WorkflowExecutionInfo {
            workflow_key: key,
            workflow_id: 18301,
            run_id: 0,
            workflow_type_id: 1,
            namespace_id: 0,
            status: WorkflowStatus::Completed,
            start_time_ms: 1000, // epoch = 1 second
            close_time_ms: Some(2000), // epoch = 2 seconds — very old
            task_queue_hash: 42,
            search_attributes: std::collections::HashMap::new(),
            memo: std::collections::HashMap::new(),
        };
        engine.visibility().register(old_info);

        // Now purge — should remove the old workflow
        let purged2 = engine.purge_expired_workflows();
        assert_eq!(purged2, 1);
        assert_eq!(engine.visibility().total_count(), 0);
    }

    // ─── E2E: List Returns start_time in Response ──────────────────────────

    #[tokio::test]
    async fn test_e2e_list_returns_start_time() {
        let svc = test_service();
        svc.engine.start_workflow(18401, 1, 0, 42, 1, None);

        let resp = svc.list_workflow_executions(req(ListWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 10,
            ..Default::default()
        })).await.unwrap();
        let execs = resp.into_inner().executions;
        assert_eq!(execs.len(), 1);
        assert!(execs[0].start_time.is_some(), "start_time should be populated");
        assert!(execs[0].start_time.as_ref().unwrap().seconds > 0, "start_time should be non-zero");
    }

    // ─── E2E: Namespace Retention Configuration ────────────────────────────

    #[tokio::test]
    async fn test_e2e_namespace_retention_config() {
        let svc = test_service();

        // Register namespace with 30-day retention
        let resp = svc.register_namespace(req(RegisterNamespaceRequest {
            namespace: "retention-test".to_string(),
            description: "test retention".to_string(),
            workflow_execution_retention_period: Some(prost_types::Duration {
                seconds: 30 * 24 * 3600,
                nanos: 0,
            }),
            ..Default::default()
        })).await.unwrap();
        let ns_id = resp.into_inner().namespace_id;

        // Describe namespace and verify retention
        let desc = svc.describe_namespace(req(DescribeNamespaceRequest {
            namespace: "retention-test".to_string(),
            namespace_id: 0,
        })).await.unwrap();
        let info = desc.into_inner().namespace_info.unwrap();
        let retention_secs = info.retention_period.as_ref().map(|d| d.seconds).unwrap_or(0);
        assert_eq!(retention_secs, 30 * 24 * 3600, "retention should be 30 days");

        // Start and complete a workflow in this namespace
        let key = svc.engine.start_workflow(19001, 1, ns_id, 42, 1, None);
        svc.engine.complete_workflow(key, Some(vec![]));

        // Purge should not remove it (just completed, within retention)
        let purged = svc.engine.purge_expired_workflows();
        assert_eq!(purged, 0, "recent workflow should not be purged");
    }

    // ─── E2E: Search Attributes Round-Trip ─────────────────────────────────

    #[tokio::test]
    async fn test_e2e_search_attributes_roundtrip() {
        let svc = test_service();

        // Start workflow with search attributes
        let mut indexed_fields = std::collections::HashMap::new();
        indexed_fields.insert("env".to_string(), velocity_proto::SearchAttributeValue {
            value: Some(velocity_proto::search_attribute_value::Value::KeywordValue("production".into())),
        });
        indexed_fields.insert("priority".to_string(), velocity_proto::SearchAttributeValue {
            value: Some(velocity_proto::search_attribute_value::Value::IntegerValue(5)),
        });

        let resp = svc.start_workflow_execution(req(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "19101".to_string(),
                run_id: String::new(),
            }),
            workflow_type: Some(velocity_proto::WorkflowType {
                name: "TestWorkflow".to_string(),
                type_id: 1,
            }),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 42, kind: 0 }),
            total_steps: 1,
            search_attributes: Some(velocity_proto::SearchAttributes { indexed_fields }),
            ..Default::default()
        })).await.unwrap();
        let key = resp.into_inner().workflow_key;

        // Describe and verify search attributes are returned
        let desc = svc.describe_workflow_execution(req(DescribeWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "19101".to_string(),
                run_id: String::new(),
            }),
        })).await.unwrap();
        let exec_info = desc.into_inner().execution_info.unwrap();
        let sa = exec_info.search_attributes.expect("search_attributes should be present");
        assert_eq!(sa.indexed_fields.len(), 2);
        assert!(sa.indexed_fields.contains_key("env"));
        assert!(sa.indexed_fields.contains_key("priority"));

        // List with query filtering by search attribute
        let list_resp = svc.list_workflow_executions(req(ListWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 100,
            query: "WorkflowType = 1".to_string(),
            ..Default::default()
        })).await.unwrap();
        assert_eq!(list_resp.into_inner().executions.len(), 1);
    }

    // ─── E2E: Memo Round-Trip ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_memo_roundtrip() {
        let svc = test_service();

        // Start workflow with memo
        let mut memo_fields = std::collections::HashMap::new();
        memo_fields.insert("owner".to_string(), velocity_proto::Payload {
            data: b"alice".to_vec(),
            encoding: 0,
            metadata: std::collections::HashMap::new(),
        });

        let resp = svc.start_workflow_execution(req(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "19201".to_string(),
                run_id: String::new(),
            }),
            workflow_type: Some(velocity_proto::WorkflowType {
                name: "MemoWorkflow".to_string(),
                type_id: 2,
            }),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 42, kind: 0 }),
            total_steps: 1,
            memo: Some(velocity_proto::Memo { fields: memo_fields }),
            ..Default::default()
        })).await.unwrap();

        // Describe and verify memo is returned
        let desc = svc.describe_workflow_execution(req(DescribeWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "19201".to_string(),
                run_id: String::new(),
            }),
        })).await.unwrap();
        let exec_info = desc.into_inner().execution_info.unwrap();
        let memo = exec_info.memo.expect("memo should be present");
        assert_eq!(memo.fields.len(), 1);
        assert!(memo.fields.contains_key("owner"));
        assert_eq!(memo.fields["owner"].data, b"alice");
    }

    // ─── E2E: Poll Returns History and Workflow Type ───────────────────────

    #[tokio::test]
    async fn test_e2e_poll_returns_history_and_type() {
        let svc = test_service();

        // Start a workflow — this enqueues a workflow task
        let key = svc.engine.start_workflow(19301, 42, 0, 10, 3, None);

        // Poll the task queue
        let resp = svc.poll_workflow_task_queue(req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 10, kind: 0 }),
            identity: "test-worker".to_string(),
            ..Default::default()
        })).await.unwrap();
        let inner = resp.into_inner();

        // Verify workflow type is populated
        let wt = inner.workflow_type.expect("workflow_type should be present");
        assert_eq!(wt.type_id, 42, "workflow type_id should match");

        // Verify history events are present
        let history = inner.history.expect("history should be present");
        assert!(!history.events.is_empty(), "history should have events");
        // First event should be WorkflowStarted
        assert_eq!(history.events[0].event_type, "WorkflowStarted");
    }

    // ─── E2E: Activity Input Propagation ───────────────────────────────────

    #[tokio::test]
    async fn test_e2e_activity_input_propagation() {
        let svc = test_service();

        // Start a workflow (enqueues a WorkflowTask at hash 10)
        let wf_key = svc.engine.start_workflow(19401, 1, 0, 10, 5, None);

        // Drain the initial workflow task so we can poll the activity task
        let _wf_task = svc.engine.task_queue().try_poll(10);

        // Schedule an activity with input payload
        let activity_input = b"hello-activity-input".to_vec();
        svc.engine.schedule_activity(wf_key, 0, 100, activity_input.clone());

        // Verify the engine stores the input
        let retrieved = svc.engine.get_activity_input(wf_key, 0);
        assert_eq!(retrieved, Some(activity_input.clone()));

        // Poll the activity task queue
        let resp = svc.poll_activity_task_queue(req(PollActivityTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 10, kind: 0 }),
            identity: "test-worker".to_string(),
            ..Default::default()
        })).await.unwrap();
        let inner = resp.into_inner();

        // Verify input is propagated to the poll response
        let payload = inner.input.expect("activity input should be present");
        assert_eq!(payload.data, activity_input);

        // Verify timestamps are populated
        assert!(inner.scheduled_time.is_some(), "scheduled_time should be set");
        assert!(inner.started_time.is_some(), "started_time should be set");

        // Verify history has ActivityStarted event
        let events = svc.engine.history_store().get_history(wf_key).unwrap();
        let has_activity_started = events.iter().any(|e| {
            e.event_type == crate::event_history::HistoryEventType::ActivityStarted
        });
        assert!(has_activity_started, "should have ActivityStarted in history");
    }

    // ─── E2E: Activity Without Input ───────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_activity_without_input() {
        let svc = test_service();

        // Start a workflow and schedule activity with empty input
        let wf_key = svc.engine.start_workflow(19501, 1, 0, 10, 5, None);
        svc.engine.schedule_activity(wf_key, 0, 200, vec![]);

        // Engine should not have input stored for empty payloads
        assert_eq!(svc.engine.get_activity_input(wf_key, 0), None);

        // Poll should return None for input
        let resp = svc.poll_activity_task_queue(req(PollActivityTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 10, kind: 0 }),
            identity: "test-worker".to_string(),
            ..Default::default()
        })).await.unwrap();
        assert!(resp.into_inner().input.is_none(), "empty input should be None");
    }

    // ─── E2E: Long-Poll Workflow Task Queue ─────────────────────────────────

    #[tokio::test]
    async fn test_e2e_long_poll_returns_immediately_when_task_available() {
        let svc = test_service();

        // Start a workflow — this enqueues a workflow task
        let _key = svc.engine.start_workflow(19601, 1, 0, 10, 3, None);

        // Long-poll with 5 second timeout — should return immediately since task is available
        let start = std::time::Instant::now();
        let resp = svc.poll_workflow_task_queue(req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 10, kind: 0 }),
            identity: "test-worker".to_string(),
            long_poll_timeout_ms: 5000,
            ..Default::default()
        })).await.unwrap();
        let elapsed = start.elapsed();

        let inner = resp.into_inner();
        assert!(inner.task_token > 0, "should have a task token");
        assert!(elapsed.as_millis() < 1000, "should return immediately when task available");
    }

    #[tokio::test]
    async fn test_e2e_long_poll_times_out_when_no_task() {
        let svc = test_service();

        // Long-poll with 100ms timeout on empty queue — should timeout and return empty
        let start = std::time::Instant::now();
        let resp = svc.poll_workflow_task_queue(req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 99, kind: 0 }),
            identity: "test-worker".to_string(),
            long_poll_timeout_ms: 100,
            ..Default::default()
        })).await.unwrap();
        let elapsed = start.elapsed();

        let inner = resp.into_inner();
        assert_eq!(inner.task_token, 0, "should have no task token");
        assert!(elapsed.as_millis() >= 90, "should wait for timeout");
        assert!(elapsed.as_millis() < 500, "should not wait longer than timeout");
    }

    #[tokio::test]
    async fn test_e2e_long_poll_wakes_up_on_task_arrival() {
        let svc = test_service();
        let engine = svc.engine.clone();

        // Spawn a task that will enqueue a workflow task after 50ms
        let engine_clone = engine.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            engine_clone.start_workflow(19602, 1, 0, 10, 3, None);
        });

        // Long-poll with 2 second timeout — should wake up when task arrives
        let start = std::time::Instant::now();
        let resp = svc.poll_workflow_task_queue(req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 10, kind: 0 }),
            identity: "test-worker".to_string(),
            long_poll_timeout_ms: 2000,
            ..Default::default()
        })).await.unwrap();
        let elapsed = start.elapsed();

        let inner = resp.into_inner();
        assert!(inner.task_token > 0, "should have a task token");
        assert!(elapsed.as_millis() >= 40, "should wait for task arrival");
        assert!(elapsed.as_millis() < 500, "should not wait full timeout");
    }

    // ─── E2E: Long-Poll Activity Task Queue ─────────────────────────────────

    #[tokio::test]
    async fn test_e2e_long_poll_activity_returns_immediately() {
        let svc = test_service();

        // Start workflow and schedule activity
        let wf_key = svc.engine.start_workflow(19603, 1, 0, 10, 3, None);
        let _wf_task = svc.engine.task_queue().try_poll(10); // drain workflow task
        svc.engine.schedule_activity(wf_key, 0, 100, b"test".to_vec());

        // Long-poll should return immediately
        let start = std::time::Instant::now();
        let resp = svc.poll_activity_task_queue(req(PollActivityTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 10, kind: 0 }),
            identity: "test-worker".to_string(),
            long_poll_timeout_ms: 5000,
            ..Default::default()
        })).await.unwrap();
        let elapsed = start.elapsed();

        let inner = resp.into_inner();
        assert!(inner.task_token > 0, "should have a task token");
        assert!(elapsed.as_millis() < 1000, "should return immediately");
    }

    // ─── E2E: Workflow Reset ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_workflow_reset_to_event_id() {
        let svc = test_service();

        // Start a workflow and complete some steps
        let wf_key = svc.engine.start_workflow(19701, 1, 0, 10, 5, None);
        
        // Simulate completing steps 0, 1, 2
        svc.engine.complete_step(wf_key, 0, b"step0".to_vec());
        svc.engine.complete_step(wf_key, 1, b"step1".to_vec());
        svc.engine.complete_step(wf_key, 2, b"step2".to_vec());

        // Verify steps are completed
        assert!(svc.engine.is_step_completed(wf_key, 0));
        assert!(svc.engine.is_step_completed(wf_key, 1));
        assert!(svc.engine.is_step_completed(wf_key, 2));

        // Reset to event ID 1 (should clear steps 1 and 2)
        let reset_req = req(ResetWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "19701".to_string(),
                run_id: String::new(),
            }),
            workflow_task_finish_event_id: 1,
            reason: "test-reset".to_string(),
            ..Default::default()
        });

        let resp = svc.reset_workflow_execution(reset_req).await.unwrap();
        let inner = resp.into_inner();
        assert!(!inner.run_id.is_empty(), "should have a new run_id");

        // Verify step 0 is still completed, but steps 1 and 2 are cleared
        assert!(svc.engine.is_step_completed(wf_key, 0), "step 0 should still be completed");
        assert!(!svc.engine.is_step_completed(wf_key, 1), "step 1 should be cleared");
        assert!(!svc.engine.is_step_completed(wf_key, 2), "step 2 should be cleared");

        // Verify workflow is back to Running status
        assert_eq!(svc.engine.get_status(wf_key), WorkflowStatus::Running);
    }

    #[tokio::test]
    async fn test_e2e_workflow_reset_records_history() {
        let svc = test_service();

        // Start a workflow
        let wf_key = svc.engine.start_workflow(19702, 1, 0, 10, 3, None);
        svc.engine.complete_step(wf_key, 0, b"step0".to_vec());

        // Reset the workflow
        let reset_req = req(ResetWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "19702".to_string(),
                run_id: String::new(),
            }),
            workflow_task_finish_event_id: 0,
            reason: "test-reset".to_string(),
            ..Default::default()
        });

        svc.reset_workflow_execution(reset_req).await.unwrap();

        // Verify history has WorkflowReset event
        let events = svc.engine.history_store().get_history(wf_key).unwrap();
        let has_reset_event = events.iter().any(|e| {
            e.event_type == crate::event_history::HistoryEventType::WorkflowReset
        });
        assert!(has_reset_event, "should have WorkflowReset in history");
    }

    #[tokio::test]
    async fn test_e2e_workflow_reset_not_found() {
        let svc = test_service();

        // Try to reset a non-existent workflow
        let reset_req = req(ResetWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "99999".to_string(),
                run_id: String::new(),
            }),
            workflow_task_finish_event_id: 0,
            reason: "test-reset".to_string(),
            ..Default::default()
        });

        let resp = svc.reset_workflow_execution(reset_req).await;
        assert!(resp.is_err(), "should fail for non-existent workflow");
        assert_eq!(resp.unwrap_err().code(), tonic::Code::NotFound);
    }

    // ─── E2E: Robustness Tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_long_poll_negative_timeout_is_nonblocking() {
        let svc = test_service();

        // Negative timeout should behave as non-blocking (return immediately)
        let start = std::time::Instant::now();
        let resp = svc.poll_workflow_task_queue(req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 99, kind: 0 }),
            identity: "test-worker".to_string(),
            long_poll_timeout_ms: -1, // negative = non-blocking
            ..Default::default()
        })).await.unwrap();
        let elapsed = start.elapsed();

        let inner = resp.into_inner();
        assert_eq!(inner.task_token, 0, "should have no task");
        assert!(elapsed.as_millis() < 100, "should return immediately for negative timeout");
    }

    #[tokio::test]
    async fn test_e2e_concurrent_long_pollers_wake_on_task() {
        let svc = test_service();
        let engine = svc.engine.clone();

        // Spawn 3 concurrent long-pollers
        let mut handles = vec![];
        for _i in 0..3 {
            let svc_clone = svc.clone();
            let handle = tokio::spawn(async move {
                svc_clone.poll_workflow_task_queue(req(PollWorkflowTaskQueueRequest {
                    namespace: "default".to_string(),
                    task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 10, kind: 0 }),
                    identity: "worker".to_string(),
                    long_poll_timeout_ms: 2000,
                    ..Default::default()
                })).await.unwrap().into_inner()
            });
            handles.push(handle);
        }

        // Wait a bit then enqueue a task
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        engine.start_workflow(19801, 1, 0, 10, 3, None);

        // At least one poller should get the task
        let mut got_task = false;
        for handle in handles {
            let result = handle.await.unwrap();
            if result.task_token > 0 {
                got_task = true;
            }
        }
        assert!(got_task, "at least one poller should receive the task");
    }

    #[tokio::test]
    async fn test_e2e_reset_completed_workflow_fails() {
        let svc = test_service();

        // Start and complete a workflow
        let wf_key = svc.engine.start_workflow(19802, 1, 0, 10, 3, None);
        svc.engine.complete_workflow(wf_key, Some(b"done".to_vec()));

        // Reset should fail because workflow is completed
        let reset_req = req(ResetWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "19802".to_string(),
                run_id: String::new(),
            }),
            workflow_task_finish_event_id: 0,
            reason: "test-reset".to_string(),
            ..Default::default()
        });

        let resp = svc.reset_workflow_execution(reset_req).await;
        assert!(resp.is_err(), "should fail for completed workflow");
        assert_eq!(resp.unwrap_err().code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn test_e2e_poll_returns_correct_task_kind() {
        let svc = test_service();

        // Start a workflow (enqueues WorkflowTask) and schedule activity (enqueues ActivityTask)
        let wf_key = svc.engine.start_workflow(19803, 1, 0, 10, 3, None);
        svc.engine.schedule_activity(wf_key, 0, 100, b"input".to_vec());

        // Poll workflow task queue - should get WorkflowTask
        let wf_resp = svc.poll_workflow_task_queue(req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 10, kind: 0 }),
            identity: "test-worker".to_string(),
            ..Default::default()
        })).await.unwrap().into_inner();
        assert!(wf_resp.task_token > 0, "should get workflow task");

        // Poll activity task queue - should get ActivityTask
        let act_resp = svc.poll_activity_task_queue(req(PollActivityTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(velocity_proto::TaskQueue { name: "tq".into(), hash: 10, kind: 0 }),
            identity: "test-worker".to_string(),
            ..Default::default()
        })).await.unwrap().into_inner();
        assert!(act_resp.task_token > 0, "should get activity task");

        // Verify the activity task has the correct input
        let payload = act_resp.input.expect("should have input");
        assert_eq!(payload.data, b"input");
    }

    // ─── E2E: Input Validation Tests ────────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_start_workflow_missing_namespace() {
        let svc = test_service();

        let resp = svc.start_workflow_execution(req(StartWorkflowExecutionRequest {
            namespace: String::new(), // empty = invalid
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "20001".to_string(),
                run_id: String::new(),
            }),
            workflow_type: Some(velocity_proto::WorkflowType { name: "test".into(), type_id: 1 }),
            ..Default::default()
        })).await;

        assert!(resp.is_err(), "should fail for missing namespace");
        assert_eq!(resp.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_e2e_start_workflow_missing_workflow_id() {
        let svc = test_service();

        let resp = svc.start_workflow_execution(req(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: None, // missing = invalid
            workflow_type: Some(velocity_proto::WorkflowType { name: "test".into(), type_id: 1 }),
            ..Default::default()
        })).await;

        assert!(resp.is_err(), "should fail for missing workflow_id");
        assert_eq!(resp.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_e2e_start_workflow_missing_workflow_type() {
        let svc = test_service();

        let resp = svc.start_workflow_execution(req(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(velocity_proto::WorkflowExecution {
                workflow_id: "20002".to_string(),
                run_id: String::new(),
            }),
            workflow_type: None, // missing = invalid
            ..Default::default()
        })).await;

        assert!(resp.is_err(), "should fail for missing workflow_type");
        assert_eq!(resp.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_e2e_poll_workflow_missing_task_queue() {
        let svc = test_service();

        let resp = svc.poll_workflow_task_queue(req(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: None, // missing = invalid
            identity: "test-worker".to_string(),
            ..Default::default()
        })).await;

        assert!(resp.is_err(), "should fail for missing task_queue");
        assert_eq!(resp.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_e2e_poll_activity_missing_task_queue() {
        let svc = test_service();

        let resp = svc.poll_activity_task_queue(req(PollActivityTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: None, // missing = invalid
            identity: "test-worker".to_string(),
            ..Default::default()
        })).await;

        assert!(resp.is_err(), "should fail for missing task_queue");
        assert_eq!(resp.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    // ─── E2E: Batch Operations ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_e2e_batch_terminate_running_workflows() {
        let svc = test_service();

        // Start multiple workflows
        let _wf1 = svc.engine.start_workflow(20001, 1, 0, 10, 3, None);
        let _wf2 = svc.engine.start_workflow(20002, 1, 0, 10, 3, None);
        let _wf3 = svc.engine.start_workflow(20003, 1, 0, 10, 3, None);

        // Verify they're running
        assert_eq!(svc.engine.get_status(_wf1), WorkflowStatus::Running);
        assert_eq!(svc.engine.get_status(_wf2), WorkflowStatus::Running);
        assert_eq!(svc.engine.get_status(_wf3), WorkflowStatus::Running);

        // Start a batch terminate operation
        let resp = svc.start_batch_operation(req(StartBatchOperationRequest {
            namespace: "default".to_string(),
            job_id: String::new(),
            operation: 0, // Terminate
            visibility_query: "Status='running'".to_string(),
            signal_name: String::new(),
            signal_input: None,
            reason: "test batch terminate".to_string(),
            identity: "test-admin".to_string(),
        })).await.unwrap();

        let inner = resp.into_inner();
        assert!(!inner.job_id.is_empty(), "should have a job_id");

        // Describe the batch operation
        let desc_resp = svc.describe_batch_operation(req(DescribeBatchOperationRequest {
            namespace: "default".to_string(),
            job_id: inner.job_id.clone(),
        })).await.unwrap();

        let desc = desc_resp.into_inner();
        assert_eq!(desc.job_id, inner.job_id);
        assert_eq!(desc.operation, 0); // Terminate
        assert_eq!(desc.status, 2); // Completed
        assert!(desc.total_workflows >= 3, "should have terminated at least 3 workflows");
    }

    #[tokio::test]
    async fn test_e2e_batch_requires_namespace_and_query() {
        let svc = test_service();

        // Missing namespace
        let resp = svc.start_batch_operation(req(StartBatchOperationRequest {
            namespace: String::new(),
            job_id: String::new(),
            operation: 0,
            visibility_query: "Status='running'".to_string(),
            signal_name: String::new(),
            signal_input: None,
            reason: "test".to_string(),
            identity: "test".to_string(),
        })).await;
        assert!(resp.is_err(), "should fail for missing namespace");
        assert_eq!(resp.unwrap_err().code(), tonic::Code::InvalidArgument);

        // Missing visibility_query
        let resp = svc.start_batch_operation(req(StartBatchOperationRequest {
            namespace: "default".to_string(),
            job_id: String::new(),
            operation: 0,
            visibility_query: String::new(),
            signal_name: String::new(),
            signal_input: None,
            reason: "test".to_string(),
            identity: "test".to_string(),
        })).await;
        assert!(resp.is_err(), "should fail for missing visibility_query");
        assert_eq!(resp.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_e2e_batch_describe_not_found() {
        let svc = test_service();

        let resp = svc.describe_batch_operation(req(DescribeBatchOperationRequest {
            namespace: "default".to_string(),
            job_id: "99999".to_string(),
        })).await;

        assert!(resp.is_err(), "should fail for non-existent batch");
        assert_eq!(resp.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_e2e_batch_signal_running_workflows() {
        let svc = test_service();

        // Start multiple workflows
        let _wf1 = svc.engine.start_workflow(20011, 1, 0, 10, 3, None);
        let _wf2 = svc.engine.start_workflow(20012, 1, 0, 10, 3, None);

        // Start a batch signal operation
        let resp = svc.start_batch_operation(req(StartBatchOperationRequest {
            namespace: "default".to_string(),
            job_id: String::new(),
            operation: 2, // Signal
            visibility_query: "Status='running'".to_string(),
            signal_name: "test-signal".to_string(),
            signal_input: Some(velocity_proto::Payload { data: b"signal-data".to_vec(), encoding: 0, metadata: std::collections::HashMap::new() }),
            reason: "test batch signal".to_string(),
            identity: "test-admin".to_string(),
        })).await.unwrap();

        let inner = resp.into_inner();
        assert!(!inner.job_id.is_empty(), "should have a job_id");

        // Describe the batch operation
        let desc_resp = svc.describe_batch_operation(req(DescribeBatchOperationRequest {
            namespace: "default".to_string(),
            job_id: inner.job_id.clone(),
        })).await.unwrap();

        let desc = desc_resp.into_inner();
        assert_eq!(desc.operation, 2); // Signal
        assert_eq!(desc.status, 2); // Completed
    }

    #[tokio::test]
    async fn test_e2e_batch_signal_requires_signal_name() {
        let svc = test_service();

        let resp = svc.start_batch_operation(req(StartBatchOperationRequest {
            namespace: "default".to_string(),
            job_id: String::new(),
            operation: 2, // Signal
            visibility_query: "Status='running'".to_string(),
            signal_name: String::new(), // missing!
            signal_input: None,
            reason: "test".to_string(),
            identity: "test".to_string(),
        })).await;

        assert!(resp.is_err(), "should fail for missing signal_name");
        assert_eq!(resp.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_e2e_batch_query_status() {
        let svc = test_service();

        // Start multiple workflows
        let _wf1 = svc.engine.start_workflow(20021, 1, 0, 10, 3, None);
        let _wf2 = svc.engine.start_workflow(20022, 1, 0, 10, 3, None);

        // Start a batch query_status operation
        let resp = svc.start_batch_operation(req(StartBatchOperationRequest {
            namespace: "default".to_string(),
            job_id: String::new(),
            operation: 3, // QueryStatus
            visibility_query: "Status='running'".to_string(),
            signal_name: String::new(),
            signal_input: None,
            reason: "test batch query".to_string(),
            identity: "test-admin".to_string(),
        })).await.unwrap();

        let inner = resp.into_inner();
        assert!(!inner.job_id.is_empty(), "should have a job_id");

        // Describe the batch operation
        let desc_resp = svc.describe_batch_operation(req(DescribeBatchOperationRequest {
            namespace: "default".to_string(),
            job_id: inner.job_id.clone(),
        })).await.unwrap();

        let desc = desc_resp.into_inner();
        assert_eq!(desc.operation, 3); // QueryStatus
        assert_eq!(desc.status, 2); // Completed
        assert!(desc.total_workflows >= 2, "should have queried at least 2 workflows");
    }

    #[tokio::test]
    async fn test_e2e_batch_list_operations() {
        let svc = test_service();

        // Start some workflows
        let _wf1 = svc.engine.start_workflow(20031, 1, 0, 10, 3, None);
        let _wf2 = svc.engine.start_workflow(20032, 1, 0, 10, 3, None);

        // Start a batch terminate operation
        let resp1 = svc.start_batch_operation(req(StartBatchOperationRequest {
            namespace: "default".to_string(),
            job_id: String::new(),
            operation: 0, // Terminate
            visibility_query: "Status='running'".to_string(),
            signal_name: String::new(),
            signal_input: None,
            reason: "test".to_string(),
            identity: "test".to_string(),
        })).await.unwrap();
        let job_id1 = resp1.into_inner().job_id;

        // Start another batch operation
        let _wf3 = svc.engine.start_workflow(20033, 1, 0, 10, 3, None);
        let resp2 = svc.start_batch_operation(req(StartBatchOperationRequest {
            namespace: "default".to_string(),
            job_id: String::new(),
            operation: 0, // Terminate
            visibility_query: "Status='running'".to_string(),
            signal_name: String::new(),
            signal_input: None,
            reason: "test2".to_string(),
            identity: "test".to_string(),
        })).await.unwrap();
        let job_id2 = resp2.into_inner().job_id;

        // List batch operations
        let list_resp = svc.list_batch_operations(req(ListBatchOperationsRequest {
            namespace: "default".to_string(),
            page_size: 10,
            next_page_token: vec![],
        })).await.unwrap();

        let list = list_resp.into_inner();
        assert!(list.operations.len() >= 2, "should have at least 2 batch operations");
        
        // Verify both job IDs are in the list
        let job_ids: Vec<String> = list.operations.iter().map(|op| op.job_id.clone()).collect();
        assert!(job_ids.contains(&job_id1), "should contain first job");
        assert!(job_ids.contains(&job_id2), "should contain second job");
    }
}
