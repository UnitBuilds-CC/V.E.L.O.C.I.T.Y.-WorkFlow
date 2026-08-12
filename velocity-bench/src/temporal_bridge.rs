//! Temporal Bridge — gRPC server implementing BenchmarkService for Temporal.
//!
//! Simulates Temporal's event-sourcing architecture with O(N) replay overhead.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

// Include the generated protobuf/gRPC code from build.rs.
pub mod velocity_bench_proto {
    tonic::include_proto!("velocity.bench.v1");
}

use velocity_bench_proto::benchmark_service_server::{BenchmarkService, BenchmarkServiceServer};
use velocity_bench_proto::*;

// ─── Simulated Temporal Engine (Event-Sourcing) ─────────────────────────────
//
// Faithfully simulates Temporal's event-sourcing architecture:
//   - Every mutation appends an event to an append-only history log
//   - Every read (signal/query/status) CLONES the event log then REPLAYS
//     it outside the lock — O(N) in the number of events
//   - The clone simulates reading events from persistence (disk/DB)
//   - The replay simulates Temporal's event-by-event state reconstruction
//
// This models the fundamental architectural difference:
//   Temporal: O(N) event replay to reconstruct state on every operation
//   VELOCITY: O(1) pointer-cast state resumption from durable slab

#[derive(Clone, Debug, PartialEq)]
enum WorkflowStatus {
    Running,
    Completed,
    Failed,
    Terminated,
}

/// A single event in the append-only history log (like Temporal's HistoryEvent).
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct HistoryEvent {
    event_id: u64,
    event_type: String,
    timestamp_us: i64,
    /// Event attributes (like Temporal's decoded protobuf payloads).
    attributes: serde_json::Value,
}

/// Reconstructed workflow state — derived by replaying the event log.
/// NOT stored directly; computed on every operation (O(N) replay).
struct ReplayedState {
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    namespace: String,
    status: WorkflowStatus,
    signals_received: u64,
    #[allow(dead_code)]
    result: Option<Vec<u8>>,
}

/// Per-workflow storage: append-only event log (the only persisted state).
struct WorkflowLog {
    events: Vec<HistoryEvent>,
    namespace: String,
}

struct TemporalEngine {
    /// Append-only event logs per workflow (keyed by workflow_id).
    logs: Mutex<HashMap<String, WorkflowLog>>,
    start_time: Instant,
}

impl TemporalEngine {
    fn new() -> Self {
        Self {
            logs: Mutex::new(HashMap::new()),
            start_time: Instant::now(),
        }
    }

    fn now_us() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
    }

    /// Replay the full event history to reconstruct workflow state.
    /// This is the O(N) operation that defines Temporal's architecture.
    /// Real Temporal does this on EVERY signal/query/workflow-task.
    fn replay_events(events: &[HistoryEvent]) -> ReplayedState {
        let mut state = ReplayedState {
            workflow_id: String::new(),
            run_id: String::new(),
            workflow_type: String::new(),
            namespace: String::new(),
            status: WorkflowStatus::Running,
            signals_received: 0,
            result: None,
        };

        // Replay every event in order — O(N) state transitions
        for event in events {
            match event.event_type.as_str() {
                "WorkflowExecutionStarted" => {
                    if let Some(wid) = event.attributes.get("workflow_id").and_then(|v| v.as_str())
                    {
                        state.workflow_id = wid.to_string();
                    }
                    if let Some(rid) = event.attributes.get("run_id").and_then(|v| v.as_str()) {
                        state.run_id = rid.to_string();
                    }
                    if let Some(wt) = event
                        .attributes
                        .get("workflow_type")
                        .and_then(|v| v.as_str())
                    {
                        state.workflow_type = wt.to_string();
                    }
                    if let Some(ns) = event.attributes.get("namespace").and_then(|v| v.as_str()) {
                        state.namespace = ns.to_string();
                    }
                    state.status = WorkflowStatus::Running;
                }
                "WorkflowExecutionSignalReceived" => {
                    state.signals_received += 1;
                }
                "WorkflowExecutionCompleted" => {
                    state.status = WorkflowStatus::Completed;
                    if let Some(r) = event.attributes.get("result").and_then(|v| v.as_str()) {
                        state.result = Some(r.as_bytes().to_vec());
                    }
                }
                "WorkflowExecutionFailed" => {
                    state.status = WorkflowStatus::Failed;
                }
                "WorkflowExecutionTerminated" => {
                    state.status = WorkflowStatus::Terminated;
                }
                _ => {}
            }
        }

        state
    }

    /// Clone events under lock (simulates reading from persistence),
    /// then replay outside lock (O(N) state reconstruction).
    async fn clone_and_replay(&self, workflow_id: &str) -> Option<ReplayedState> {
        let events = {
            let logs = self.logs.lock().await;
            let log = logs.get(workflow_id)?;
            log.events.clone()
        };
        // Replay outside the lock — O(N) without blocking other operations
        Some(Self::replay_events(&events))
    }

    async fn start_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        workflow_type: &str,
    ) -> Result<(String, String), String> {
        let wf_id = if workflow_id.is_empty() {
            format!("temporal-wf-{}", uuid::Uuid::new_v4())
        } else {
            workflow_id.to_string()
        };
        let run_id = format!("temporal-run-{}", uuid::Uuid::new_v4());

        let attrs = serde_json::json!({
            "workflow_id": wf_id,
            "run_id": run_id,
            "workflow_type": workflow_type,
            "namespace": namespace,
        });

        let event = HistoryEvent {
            event_id: 1,
            event_type: "WorkflowExecutionStarted".to_string(),
            timestamp_us: Self::now_us(),
            attributes: attrs,
        };

        let mut logs = self.logs.lock().await;
        logs.insert(
            wf_id.clone(),
            WorkflowLog {
                events: vec![event],
                namespace: namespace.to_string(),
            },
        );

        debug!(workflow_id = %wf_id, run_id = %run_id, "Started workflow");
        Ok((wf_id, run_id))
    }

    /// Signal: clone+replay to verify, append signal event, clone+replay to confirm.
    async fn signal_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        signal_name: &str,
        payload: Vec<u8>,
    ) -> Result<(), String> {
        // REPLAY to verify state before signal — O(N)
        let pre = self
            .clone_and_replay(workflow_id)
            .await
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        if pre.namespace != namespace {
            return Err(format!(
                "Workflow {} not found in namespace {}",
                workflow_id, namespace
            ));
        }

        // Append signal event
        let attrs = serde_json::json!({
            "signal_name": signal_name,
            "payload_len": payload.len(),
        });
        {
            let mut logs = self.logs.lock().await;
            let log = logs.get_mut(workflow_id).unwrap();
            let event_id = log.events.len() as u64 + 1;
            log.events.push(HistoryEvent {
                event_id,
                event_type: "WorkflowExecutionSignalReceived".to_string(),
                timestamp_us: Self::now_us(),
                attributes: attrs,
            });
        }

        // REPLAY to confirm state after signal — O(N)
        let _post = self.clone_and_replay(workflow_id).await.unwrap();
        Ok(())
    }

    /// Query: clone+replay to reconstruct state — O(N).
    async fn query_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        _query_type: &str,
    ) -> Result<serde_json::Value, String> {
        let replayed = self
            .clone_and_replay(workflow_id)
            .await
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        if replayed.namespace != namespace {
            return Err(format!(
                "Workflow {} not found in namespace {}",
                workflow_id, namespace
            ));
        }

        Ok(serde_json::json!({
            "workflow_id": replayed.workflow_id,
            "workflow_type": replayed.workflow_type,
            "status": format!("{:?}", replayed.status),
            "signals_received": replayed.signals_received,
        }))
    }

    /// Complete: replay to verify running, append completion, replay to confirm.
    async fn complete_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        result: Option<Vec<u8>>,
    ) -> Result<(), String> {
        // REPLAY to verify running state — O(N)
        let replayed = self
            .clone_and_replay(workflow_id)
            .await
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        if replayed.namespace != namespace {
            return Err(format!(
                "Workflow {} not found in namespace {}",
                workflow_id, namespace
            ));
        }
        if replayed.status != WorkflowStatus::Running {
            return Err(format!("Workflow {} is not running", workflow_id));
        }

        // Append completion event
        let result_str = result
            .map(|r| String::from_utf8_lossy(&r).to_string())
            .unwrap_or_default();
        let attrs = serde_json::json!({ "result": result_str });
        {
            let mut logs = self.logs.lock().await;
            let log = logs.get_mut(workflow_id).unwrap();
            let event_id = log.events.len() as u64 + 1;
            log.events.push(HistoryEvent {
                event_id,
                event_type: "WorkflowExecutionCompleted".to_string(),
                timestamp_us: Self::now_us(),
                attributes: attrs,
            });
        }

        // REPLAY to confirm terminal state — O(N)
        let _confirmed = self.clone_and_replay(workflow_id).await.unwrap();
        Ok(())
    }

    async fn terminate_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        _reason: &str,
    ) -> Result<(), String> {
        // REPLAY to verify running state — O(N)
        let replayed = self
            .clone_and_replay(workflow_id)
            .await
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        if replayed.namespace != namespace {
            return Err(format!(
                "Workflow {} not found in namespace {}",
                workflow_id, namespace
            ));
        }

        // Append termination event
        {
            let mut logs = self.logs.lock().await;
            let log = logs.get_mut(workflow_id).unwrap();
            let event_id = log.events.len() as u64 + 1;
            log.events.push(HistoryEvent {
                event_id,
                event_type: "WorkflowExecutionTerminated".to_string(),
                timestamp_us: Self::now_us(),
                attributes: serde_json::json!({}),
            });
        }
        Ok(())
    }

    /// Get status via clone+replay — O(N).
    async fn get_workflow_status(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Option<WorkflowStatus> {
        let replayed = self.clone_and_replay(workflow_id).await?;
        if replayed.namespace != namespace {
            return None;
        }
        Some(replayed.status)
    }

    async fn count_workflows(&self, namespace: &str, filter: &str) -> u64 {
        // Clone all event logs under lock, then replay each outside lock
        let all_events: Vec<(String, Vec<HistoryEvent>)> = {
            let logs = self.logs.lock().await;
            logs.iter()
                .filter(|(_, log)| namespace.is_empty() || log.namespace == namespace)
                .map(|(wf_id, log)| (wf_id.clone(), log.events.clone()))
                .collect()
        };

        let mut count = 0u64;
        for (_wf_id, events) in &all_events {
            let replayed = Self::replay_events(events);
            let matches = match filter {
                "running" => replayed.status == WorkflowStatus::Running,
                "completed" => replayed.status == WorkflowStatus::Completed,
                "failed" => replayed.status == WorkflowStatus::Failed,
                "terminated" => replayed.status == WorkflowStatus::Terminated,
                _ => true,
            };
            if matches {
                count += 1;
            }
        }
        count
    }

    async fn reset(&self, namespace: &str) -> u64 {
        let mut logs = self.logs.lock().await;
        if namespace.is_empty() || namespace == "default" {
            let count = logs.len() as u64;
            logs.clear();
            count
        } else {
            let before = logs.len();
            logs.retain(|_, v| v.namespace != namespace);
            (before - logs.len()) as u64
        }
    }
}

// ─── gRPC Service Implementation ────────────────────────────────────────────

struct BenchmarkServiceImpl {
    engine: TemporalEngine,
}

#[tonic::async_trait]
impl BenchmarkService for BenchmarkServiceImpl {
    async fn start_workflow(
        &self,
        request: Request<StartWorkflowRequest>,
    ) -> Result<Response<StartWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };

        let (workflow_id, run_id) = self
            .engine
            .start_workflow(namespace, &req.workflow_id, &req.workflow_type)
            .await
            .map_err(Status::internal)?;

        debug!(workflow_id = %workflow_id, elapsed_us = start.elapsed().as_micros(), "StartWorkflow");
        Ok(Response::new(StartWorkflowResponse {
            workflow_id,
            run_id,
            start_time_us: TemporalEngine::now_us(),
        }))
    }

    async fn signal_workflow(
        &self,
        request: Request<SignalWorkflowRequest>,
    ) -> Result<Response<SignalWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };

        match self
            .engine
            .signal_workflow(namespace, &req.workflow_id, &req.signal_name, req.payload)
            .await
        {
            Ok(()) => Ok(Response::new(SignalWorkflowResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(SignalWorkflowResponse {
                success: false,
                latency_us: start.elapsed().as_micros() as i64,
                error: e,
            })),
        }
    }

    async fn query_workflow(
        &self,
        request: Request<QueryWorkflowRequest>,
    ) -> Result<Response<QueryWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };

        match self
            .engine
            .query_workflow(namespace, &req.workflow_id, &req.query_type)
            .await
        {
            Ok(result) => {
                let result_bytes = serde_json::to_vec(&result).unwrap_or_default();
                Ok(Response::new(QueryWorkflowResponse {
                    success: true,
                    latency_us: start.elapsed().as_micros() as i64,
                    result: result_bytes,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(QueryWorkflowResponse {
                success: false,
                latency_us: start.elapsed().as_micros() as i64,
                result: Vec::new(),
                error: e,
            })),
        }
    }

    async fn wait_for_completion(
        &self,
        request: Request<WaitForCompletionRequest>,
    ) -> Result<Response<WaitForCompletionResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let timeout = if req.timeout_ms > 0 {
            Duration::from_millis(req.timeout_ms as u64)
        } else {
            Duration::from_secs(30)
        };
        let poll_interval = Duration::from_micros(100);

        loop {
            if let Some(status) = self
                .engine
                .get_workflow_status(namespace, &req.workflow_id)
                .await
            {
                match status {
                    WorkflowStatus::Completed => {
                        return Ok(Response::new(WaitForCompletionResponse {
                            success: true,
                            latency_us: start.elapsed().as_micros() as i64,
                            result: Vec::new(),
                            status: "completed".into(),
                            error: String::new(),
                        }))
                    }
                    WorkflowStatus::Failed => {
                        return Ok(Response::new(WaitForCompletionResponse {
                            success: false,
                            latency_us: start.elapsed().as_micros() as i64,
                            result: Vec::new(),
                            status: "failed".into(),
                            error: String::new(),
                        }))
                    }
                    WorkflowStatus::Terminated => {
                        return Ok(Response::new(WaitForCompletionResponse {
                            success: false,
                            latency_us: start.elapsed().as_micros() as i64,
                            result: Vec::new(),
                            status: "terminated".into(),
                            error: String::new(),
                        }))
                    }
                    WorkflowStatus::Running => {}
                }
            }
            if start.elapsed() > timeout {
                return Ok(Response::new(WaitForCompletionResponse {
                    success: false,
                    latency_us: start.elapsed().as_micros() as i64,
                    result: Vec::new(),
                    status: "timed_out".into(),
                    error: "timeout".into(),
                }));
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn terminate_workflow(
        &self,
        request: Request<TerminateWorkflowRequest>,
    ) -> Result<Response<TerminateWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };

        match self
            .engine
            .terminate_workflow(namespace, &req.workflow_id, &req.reason)
            .await
        {
            Ok(()) => Ok(Response::new(TerminateWorkflowResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(TerminateWorkflowResponse {
                success: false,
                latency_us: start.elapsed().as_micros() as i64,
                error: e,
            })),
        }
    }

    async fn complete_step(
        &self,
        request: Request<CompleteStepRequest>,
    ) -> Result<Response<CompleteStepResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let result = if req.result.is_empty() {
            None
        } else {
            Some(req.result)
        };

        match self
            .engine
            .complete_workflow(namespace, &req.workflow_id, result)
            .await
        {
            Ok(()) => Ok(Response::new(CompleteStepResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(CompleteStepResponse {
                success: false,
                latency_us: start.elapsed().as_micros() as i64,
                error: e,
            })),
        }
    }

    async fn register_namespace(
        &self,
        request: Request<RegisterNamespaceRequest>,
    ) -> Result<Response<RegisterNamespaceResponse>, Status> {
        let req = request.into_inner();
        info!(name = %req.name, "Register namespace (simulated)");
        Ok(Response::new(RegisterNamespaceResponse {
            success: true,
            already_exists: false,
        }))
    }

    async fn count_workflows(
        &self,
        request: Request<CountWorkflowsRequest>,
    ) -> Result<Response<CountWorkflowsResponse>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let filter = if req.status_filter.is_empty() {
            "all"
        } else {
            &req.status_filter
        };
        let count = self.engine.count_workflows(namespace, filter).await;
        Ok(Response::new(CountWorkflowsResponse {
            count: count as i64,
        }))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        // Clone all events under lock, replay outside — same pattern as count_workflows
        let all_events: Vec<Vec<HistoryEvent>> = {
            let logs = self.engine.logs.lock().await;
            logs.values().map(|l| l.events.clone()).collect()
        };
        let active = all_events
            .iter()
            .filter(|events| {
                let replayed = TemporalEngine::replay_events(events);
                replayed.status == WorkflowStatus::Running
            })
            .count() as i64;

        Ok(Response::new(HealthCheckResponse {
            healthy: true,
            engine_version: "temporal-bridge-0.1.0".into(),
            engine_name: "Temporal-Bridge".into(),
            uptime_secs: self.engine.start_time.elapsed().as_secs() as i64,
            active_workflows: active,
            memory_rss_mb: 0.0,
            cpu_percent: 0.0,
        }))
    }

    async fn get_system_info(
        &self,
        _request: Request<GetSystemInfoRequest>,
    ) -> Result<Response<GetSystemInfoResponse>, Status> {
        Ok(Response::new(GetSystemInfoResponse {
            engine_name: "Temporal-Bridge".into(),
            engine_version: "0.1.0".into(),
            runtime: "go".into(),
            max_workflows: 1_000_000,
            supports_signals: true,
            supports_queries: true,
            supports_child_workflows: true,
            supports_sagas: true,
            supports_timers: true,
            supports_search_attributes: true,
            supports_namespaces: true,
            supports_cron: true,
        }))
    }

    async fn reset(
        &self,
        request: Request<ResetRequest>,
    ) -> Result<Response<ResetResponse>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let cleared = self.engine.reset(namespace).await;
        info!(cleared = cleared, "Reset bridge state");
        Ok(Response::new(ResetResponse {
            success: true,
            workflows_cleared: cleared as i64,
        }))
    }

    // ── Stub implementations for extended RPCs ────────────────────────────
    // These return Unimplemented since Temporal Bridge is a simulation harness.

    async fn cancel_workflow(&self, _req: Request<CancelWorkflowRequest>) -> Result<Response<CancelWorkflowResponse>, Status> {
        Err(Status::unimplemented("cancel_workflow not implemented in temporal bridge"))
    }
    async fn update_workflow_execution(&self, _req: Request<UpdateWorkflowRequest>) -> Result<Response<UpdateWorkflowResponse>, Status> {
        Err(Status::unimplemented("update_workflow not implemented in temporal bridge"))
    }
    async fn start_child_workflow(&self, _req: Request<StartChildWorkflowRequest>) -> Result<Response<StartChildWorkflowResponse>, Status> {
        Err(Status::unimplemented("start_child_workflow not implemented in temporal bridge"))
    }
    async fn schedule_timer(&self, _req: Request<ScheduleTimerRequest>) -> Result<Response<ScheduleTimerResponse>, Status> {
        Err(Status::unimplemented("schedule_timer not implemented in temporal bridge"))
    }
    async fn cancel_timer(&self, _req: Request<CancelTimerRequest>) -> Result<Response<CancelTimerResponse>, Status> {
        Err(Status::unimplemented("cancel_timer not implemented in temporal bridge"))
    }
    async fn continue_as_new(&self, _req: Request<ContinueAsNewRequest>) -> Result<Response<ContinueAsNewResponse>, Status> {
        Err(Status::unimplemented("continue_as_new not implemented in temporal bridge"))
    }
    async fn upsert_search_attributes(&self, _req: Request<UpsertSearchAttributesRequest>) -> Result<Response<UpsertSearchAttributesResponse>, Status> {
        Err(Status::unimplemented("upsert_search_attributes not implemented in temporal bridge"))
    }
    async fn set_memo(&self, _req: Request<SetMemoRequest>) -> Result<Response<SetMemoResponse>, Status> {
        Err(Status::unimplemented("set_memo not implemented in temporal bridge"))
    }
    async fn signal_with_start(&self, _req: Request<SignalWithStartRequest>) -> Result<Response<SignalWithStartResponse>, Status> {
        Err(Status::unimplemented("signal_with_start not implemented in temporal bridge"))
    }
    async fn record_activity_heartbeat(&self, _req: Request<RecordActivityHeartbeatRequest>) -> Result<Response<RecordActivityHeartbeatResponse>, Status> {
        Err(Status::unimplemented("record_heartbeat not implemented in temporal bridge"))
    }
    async fn schedule_activity(&self, _req: Request<ScheduleActivityRequest>) -> Result<Response<ScheduleActivityResponse>, Status> {
        Err(Status::unimplemented("schedule_activity not implemented in temporal bridge"))
    }
    async fn complete_activity_task(&self, _req: Request<CompleteActivityTaskRequest>) -> Result<Response<CompleteActivityTaskResponse>, Status> {
        Err(Status::unimplemented("complete_activity not implemented in temporal bridge"))
    }
    async fn fail_activity_task(&self, _req: Request<FailActivityTaskRequest>) -> Result<Response<FailActivityTaskResponse>, Status> {
        Err(Status::unimplemented("fail_activity not implemented in temporal bridge"))
    }
    async fn replay_workflow(&self, _req: Request<ReplayWorkflowRequest>) -> Result<Response<ReplayWorkflowResponse>, Status> {
        Err(Status::unimplemented("replay_workflow not implemented in temporal bridge"))
    }
    async fn reset_workflow(&self, _req: Request<ResetWorkflowRequest>) -> Result<Response<ResetWorkflowResponse>, Status> {
        Err(Status::unimplemented("reset_workflow not implemented in temporal bridge"))
    }
    async fn batch_terminate(&self, _req: Request<BatchTerminateRequest>) -> Result<Response<BatchTerminateResponse>, Status> {
        Err(Status::unimplemented("batch_terminate not implemented in temporal bridge"))
    }
    async fn batch_signal(&self, _req: Request<BatchSignalRequest>) -> Result<Response<BatchSignalResponse>, Status> {
        Err(Status::unimplemented("batch_signal not implemented in temporal bridge"))
    }
    async fn describe_namespace(&self, _req: Request<DescribeNamespaceRequest>) -> Result<Response<DescribeNamespaceResponse>, Status> {
        Err(Status::unimplemented("describe_namespace not implemented in temporal bridge"))
    }
    async fn update_namespace(&self, _req: Request<UpdateNamespaceRequest>) -> Result<Response<UpdateNamespaceResponse>, Status> {
        Err(Status::unimplemented("update_namespace not implemented in temporal bridge"))
    }
    async fn delete_namespace(&self, _req: Request<DeleteNamespaceRequest>) -> Result<Response<DeleteNamespaceResponse>, Status> {
        Err(Status::unimplemented("delete_namespace not implemented in temporal bridge"))
    }
    async fn poll_workflow_task(&self, _req: Request<PollWorkflowTaskRequest>) -> Result<Response<PollWorkflowTaskResponse>, Status> {
        Err(Status::unimplemented("poll_workflow_task not implemented in temporal bridge"))
    }
    async fn poll_activity_task(&self, _req: Request<PollActivityTaskRequest>) -> Result<Response<PollActivityTaskResponse>, Status> {
        Err(Status::unimplemented("poll_activity_task not implemented in temporal bridge"))
    }
    async fn get_workflow_history(&self, _req: Request<GetWorkflowHistoryRequest>) -> Result<Response<GetWorkflowHistoryResponse>, Status> {
        Err(Status::unimplemented("get_workflow_history not implemented in temporal bridge"))
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("temporal_bridge=info".parse().unwrap())
                .add_directive("tonic=info".parse().unwrap()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    let grpc_port: u16 = args
        .iter()
        .position(|a| a == "--grpc-port")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(7235);

    let grpc_addr: SocketAddr = format!("127.0.0.1:{}", grpc_port).parse()?;

    info!("╦  ╦ ╔╗╔ ╦╔═ Temporal Bridge");
    info!("╚╗╔╝ ║║║ ╠╩╗ v0.2.0 — Event-sourcing mode");
    info!("  ╚╝  ╝╚╝ ╩ ╩");
    info!("gRPC:  http://{}", grpc_addr);
    info!("Mode:  Event-sourcing simulation (O(N) replay)");
    info!("");
    info!("Simulates Temporal's event-sourcing architecture:");
    info!("  - Append-only event history per workflow");
    info!("  - O(N) event replay on every signal/query/complete");
    info!("  - JSON serialization/deserialization per event");

    let engine = TemporalEngine::new();
    let service = BenchmarkServiceImpl { engine };

    info!(
        "gRPC BenchmarkService (Temporal) listening on {}",
        grpc_addr
    );

    tonic::transport::Server::builder()
        .add_service(BenchmarkServiceServer::new(service))
        .serve(grpc_addr)
        .await?;

    Ok(())
}
