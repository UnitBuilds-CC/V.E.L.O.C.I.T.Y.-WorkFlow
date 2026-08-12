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
    Cancelled,
    ContinuedAsNew,
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
    cancel_requested: bool,
    search_attributes: std::collections::HashMap<String, String>,
    memo: std::collections::HashMap<String, String>,
    updates_received: u64,
    activities_scheduled: u64,
    activities_completed: u64,
    activities_failed: u64,
    heartbeats_recorded: u64,
    timers_scheduled: u64,
    timers_cancelled: u64,
    child_workflows_started: u64,
}

/// Per-workflow storage: append-only event log (the only persisted state).
struct WorkflowLog {
    events: Vec<HistoryEvent>,
    namespace: String,
}

/// Namespace metadata (mirrors Temporal's namespace registration).
#[derive(Clone, Debug)]
struct NamespaceInfo {
    name: String,
    description: String,
    state: String,
    retention_days: u32,
    owner_email: String,
    is_global: bool,
    created_at: i64,
}

struct TemporalEngine {
    /// Append-only event logs per workflow (keyed by workflow_id).
    logs: Mutex<HashMap<String, WorkflowLog>>,
    /// Registered namespaces.
    namespaces: Mutex<HashMap<String, NamespaceInfo>>,
    start_time: Instant,
}

impl TemporalEngine {
    fn new() -> Self {
        let mut default_ns = HashMap::new();
        default_ns.insert(
            "default".to_string(),
            NamespaceInfo {
                name: "default".to_string(),
                description: "Default namespace".to_string(),
                state: "REGISTERED".to_string(),
                retention_days: 7,
                owner_email: String::new(),
                is_global: false,
                created_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
            },
        );
        Self {
            logs: Mutex::new(HashMap::new()),
            namespaces: Mutex::new(default_ns),
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
            cancel_requested: false,
            search_attributes: std::collections::HashMap::new(),
            memo: std::collections::HashMap::new(),
            updates_received: 0,
            activities_scheduled: 0,
            activities_completed: 0,
            activities_failed: 0,
            heartbeats_recorded: 0,
            timers_scheduled: 0,
            timers_cancelled: 0,
            child_workflows_started: 0,
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
                "WorkflowExecutionCancelled" => {
                    state.status = WorkflowStatus::Cancelled;
                    state.cancel_requested = true;
                }
                "WorkflowExecutionContinuedAsNew" => {
                    state.status = WorkflowStatus::ContinuedAsNew;
                }
                "WorkflowExecutionUpdated" => {
                    state.updates_received += 1;
                }
                "SearchAttributesUpserted" => {
                    if let Some(attrs) = event
                        .attributes
                        .get("attributes")
                        .and_then(|v| v.as_object())
                    {
                        for (k, v) in attrs {
                            if let Some(s) = v.as_str() {
                                state.search_attributes.insert(k.clone(), s.to_string());
                            }
                        }
                    }
                }
                "MemoSet" => {
                    if let Some(m) = event.attributes.get("memo").and_then(|v| v.as_object()) {
                        for (k, v) in m {
                            if let Some(s) = v.as_str() {
                                state.memo.insert(k.clone(), s.to_string());
                            }
                        }
                    }
                }
                "ActivityTaskScheduled" => {
                    state.activities_scheduled += 1;
                }
                "ActivityTaskCompleted" => {
                    state.activities_completed += 1;
                }
                "ActivityTaskFailed" => {
                    state.activities_failed += 1;
                }
                "ActivityHeartbeatRecorded" => {
                    state.heartbeats_recorded += 1;
                }
                "TimerStarted" => {
                    state.timers_scheduled += 1;
                }
                "TimerCancelled" => {
                    state.timers_cancelled += 1;
                }
                "StartChildWorkflowExecutionInitiated" => {
                    state.child_workflows_started += 1;
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
                "cancelled" => replayed.status == WorkflowStatus::Cancelled,
                "continued_as_new" => replayed.status == WorkflowStatus::ContinuedAsNew,
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

    // ── Helper: append event to workflow log ────────────────────────────────
    async fn append_event(
        &self,
        workflow_id: &str,
        event_type: &str,
        attrs: serde_json::Value,
    ) -> Result<u64, String> {
        let mut logs = self.logs.lock().await;
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        let event_id = log.events.len() as u64 + 1;
        log.events.push(HistoryEvent {
            event_id,
            event_type: event_type.to_string(),
            timestamp_us: Self::now_us(),
            attributes: attrs,
        });
        Ok(event_id)
    }

    // ── Helper: replay + verify running ─────────────────────────────────────
    async fn verify_running(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<ReplayedState, String> {
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
            return Err(format!(
                "Workflow {} is not running (status: {:?})",
                workflow_id, replayed.status
            ));
        }
        Ok(replayed)
    }

    // ── Tier 1: Core features ───────────────────────────────────────────────

    async fn cancel_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        reason: &str,
    ) -> Result<(), String> {
        self.verify_running(namespace, workflow_id).await?;
        self.append_event(
            workflow_id,
            "WorkflowExecutionCancelled",
            serde_json::json!({"reason": reason}),
        )
        .await?;
        Ok(())
    }

    async fn update_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        update_name: &str,
        update_id: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        self.verify_running(namespace, workflow_id).await?;
        self.append_event(
            workflow_id,
            "WorkflowExecutionUpdated",
            serde_json::json!({
                "update_name": update_name, "update_id": update_id, "payload_len": payload.len(),
            }),
        )
        .await?;
        Ok(format!(r#"{{"update_id":"{}","status":"COMPLETED"}}"#, update_id).into_bytes())
    }

    async fn start_child_workflow(
        &self,
        namespace: &str,
        parent_wf_id: &str,
        wf_type: &str,
        child_wf_id: &str,
    ) -> Result<(String, String), String> {
        self.verify_running(namespace, parent_wf_id).await?;
        let child_id = if child_wf_id.is_empty() {
            format!("child-{}", uuid::Uuid::new_v4())
        } else {
            child_wf_id.to_string()
        };
        let child_run_id = format!("run-{}", uuid::Uuid::new_v4());
        self.append_event(parent_wf_id, "StartChildWorkflowExecutionInitiated", serde_json::json!({
            "child_workflow_id": child_id, "workflow_type": wf_type, "child_run_id": child_run_id,
        })).await?;
        // Also start the child workflow as its own log
        self.start_workflow(namespace, &child_id, wf_type).await?;
        Ok((child_id, child_run_id))
    }

    async fn schedule_timer(
        &self,
        namespace: &str,
        workflow_id: &str,
        timer_id: &str,
        duration_ms: i64,
    ) -> Result<String, String> {
        self.verify_running(namespace, workflow_id).await?;
        let tid = if timer_id.is_empty() {
            format!("timer-{}", uuid::Uuid::new_v4())
        } else {
            timer_id.to_string()
        };
        self.append_event(
            workflow_id,
            "TimerStarted",
            serde_json::json!({
                "timer_id": tid, "duration_ms": duration_ms,
            }),
        )
        .await?;
        Ok(tid)
    }

    async fn cancel_timer(
        &self,
        namespace: &str,
        workflow_id: &str,
        timer_id: &str,
    ) -> Result<(), String> {
        self.verify_running(namespace, workflow_id).await?;
        self.append_event(
            workflow_id,
            "TimerCancelled",
            serde_json::json!({"timer_id": timer_id}),
        )
        .await?;
        Ok(())
    }

    async fn continue_as_new(
        &self,
        namespace: &str,
        workflow_id: &str,
        wf_type: &str,
    ) -> Result<String, String> {
        self.verify_running(namespace, workflow_id).await?;
        let new_run_id = format!("run-{}", uuid::Uuid::new_v4());
        self.append_event(
            workflow_id,
            "WorkflowExecutionContinuedAsNew",
            serde_json::json!({
                "new_run_id": new_run_id, "new_workflow_type": wf_type,
            }),
        )
        .await?;
        Ok(new_run_id)
    }

    async fn upsert_search_attributes(
        &self,
        namespace: &str,
        workflow_id: &str,
        attrs: std::collections::HashMap<String, String>,
    ) -> Result<(), String> {
        self.verify_running(namespace, workflow_id).await?;
        self.append_event(
            workflow_id,
            "SearchAttributesUpserted",
            serde_json::json!({"attributes": attrs}),
        )
        .await?;
        Ok(())
    }

    async fn set_memo(
        &self,
        namespace: &str,
        workflow_id: &str,
        memo: std::collections::HashMap<String, String>,
    ) -> Result<(), String> {
        self.verify_running(namespace, workflow_id).await?;
        self.append_event(workflow_id, "MemoSet", serde_json::json!({"memo": memo}))
            .await?;
        Ok(())
    }

    async fn signal_with_start(
        &self,
        namespace: &str,
        wf_type: &str,
        workflow_id: &str,
        signal_name: &str,
        payload: Vec<u8>,
    ) -> Result<(String, String, bool, bool), String> {
        // Check if workflow exists and is running
        if let Ok(replayed) = self.verify_running(namespace, workflow_id).await {
            // Signal existing workflow
            self.signal_workflow(namespace, workflow_id, signal_name, payload)
                .await?;
            Ok((replayed.workflow_id, replayed.run_id, false, true))
        } else {
            // Start new workflow and signal it
            let (wf_id, run_id) = self.start_workflow(namespace, workflow_id, wf_type).await?;
            self.signal_workflow(namespace, &wf_id, signal_name, payload)
                .await?;
            Ok((wf_id, run_id, true, true))
        }
    }

    // ── Tier 2: Activity & operational ──────────────────────────────────────

    async fn record_heartbeat(
        &self,
        namespace: &str,
        workflow_id: &str,
        activity_id: &str,
    ) -> Result<bool, String> {
        self.verify_running(namespace, workflow_id).await?;
        self.append_event(
            workflow_id,
            "ActivityHeartbeatRecorded",
            serde_json::json!({"activity_id": activity_id}),
        )
        .await?;
        let replayed = self.clone_and_replay(workflow_id).await.unwrap();
        Ok(replayed.cancel_requested)
    }

    async fn schedule_activity(
        &self,
        namespace: &str,
        workflow_id: &str,
        activity_id: &str,
        activity_type: &str,
    ) -> Result<String, String> {
        self.verify_running(namespace, workflow_id).await?;
        let aid = if activity_id.is_empty() {
            format!("act-{}", uuid::Uuid::new_v4())
        } else {
            activity_id.to_string()
        };
        self.append_event(
            workflow_id,
            "ActivityTaskScheduled",
            serde_json::json!({
                "activity_id": aid, "activity_type": activity_type,
            }),
        )
        .await?;
        Ok(aid)
    }

    async fn complete_activity(
        &self,
        namespace: &str,
        workflow_id: &str,
        activity_id: &str,
    ) -> Result<(), String> {
        self.verify_running(namespace, workflow_id).await?;
        self.append_event(
            workflow_id,
            "ActivityTaskCompleted",
            serde_json::json!({"activity_id": activity_id}),
        )
        .await?;
        Ok(())
    }

    async fn fail_activity(
        &self,
        namespace: &str,
        workflow_id: &str,
        activity_id: &str,
        reason: &str,
        _non_retryable: bool,
    ) -> Result<(bool, u32), String> {
        let replayed = self.verify_running(namespace, workflow_id).await?;
        self.append_event(workflow_id, "ActivityTaskFailed", serde_json::json!({
            "activity_id": activity_id, "reason": reason, "attempt": replayed.activities_failed + 1,
        })).await?;
        // Simple retry: always retry up to 3 attempts
        let will_retry = (replayed.activities_failed + 1) < 3;
        Ok((will_retry, (replayed.activities_failed + 1) as u32 + 1))
    }

    async fn replay_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<(u64, String), String> {
        let events = {
            let logs = self.logs.lock().await;
            let log = logs
                .get(workflow_id)
                .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
            if log.namespace != namespace {
                return Err(format!(
                    "Workflow {} not found in namespace {}",
                    workflow_id, namespace
                ));
            }
            log.events.clone()
        };
        let replayed = Self::replay_events(&events);
        let status_str = format!("{:?}", replayed.status);
        Ok((events.len() as u64, status_str))
    }

    async fn reset_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        reset_to_event_id: i64,
        reason: &str,
    ) -> Result<String, String> {
        let events = {
            let logs = self.logs.lock().await;
            let log = logs
                .get(workflow_id)
                .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
            if log.namespace != namespace {
                return Err("Namespace mismatch".into());
            }
            log.events.len() as i64
        };
        if reset_to_event_id <= 0 || reset_to_event_id > events {
            return Err(format!(
                "Event ID {} out of range (1..{})",
                reset_to_event_id, events
            ));
        }
        let new_run_id = format!("run-{}", uuid::Uuid::new_v4());
        self.append_event(
            workflow_id,
            "WorkflowExecutionReset",
            serde_json::json!({
                "reset_to_event_id": reset_to_event_id, "reason": reason, "new_run_id": new_run_id,
            }),
        )
        .await?;
        Ok(new_run_id)
    }

    async fn batch_terminate(&self, namespace: &str, reason: &str, max_count: i64) -> u64 {
        // Only terminate RUNNING workflows (replay to check status)
        let targets: Vec<String> = {
            let all_events: Vec<(String, Vec<HistoryEvent>)> = {
                let logs = self.logs.lock().await;
                logs.iter()
                    .filter(|(_, log)| log.namespace == namespace)
                    .map(|(id, log)| (id.clone(), log.events.clone()))
                    .collect()
            };
            all_events
                .iter()
                .filter(|(_, events)| {
                    let s = Self::replay_events(events);
                    s.status == WorkflowStatus::Running
                })
                .map(|(id, _)| id.clone())
                .take(if max_count > 0 {
                    max_count as usize
                } else {
                    usize::MAX
                })
                .collect()
        };
        let mut count = 0u64;
        for wf_id in &targets {
            if self
                .terminate_workflow(namespace, wf_id, reason)
                .await
                .is_ok()
            {
                count += 1;
            }
        }
        count
    }

    async fn batch_signal(
        &self,
        namespace: &str,
        signal_name: &str,
        payload: Vec<u8>,
        max_count: i64,
    ) -> u64 {
        let targets: Vec<String> = {
            let all_events: Vec<(String, Vec<HistoryEvent>)> = {
                let logs = self.logs.lock().await;
                logs.iter()
                    .filter(|(_, log)| log.namespace == namespace)
                    .map(|(id, log)| (id.clone(), log.events.clone()))
                    .collect()
            };
            all_events
                .iter()
                .filter(|(_, events)| {
                    let s = Self::replay_events(events);
                    s.status == WorkflowStatus::Running
                })
                .map(|(id, _)| id.clone())
                .take(if max_count > 0 {
                    max_count as usize
                } else {
                    usize::MAX
                })
                .collect()
        };
        let mut count = 0u64;
        for wf_id in &targets {
            if self
                .signal_workflow(namespace, wf_id, signal_name, payload.clone())
                .await
                .is_ok()
            {
                count += 1;
            }
        }
        count
    }

    // ── Tier 3: Namespace & production ──────────────────────────────────────

    async fn get_workflow_history(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<Vec<HistoryEvent>, String> {
        let logs = self.logs.lock().await;
        let log = logs
            .get(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        if log.namespace != namespace {
            return Err("Namespace mismatch".into());
        }
        Ok(log.events.clone())
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
                    WorkflowStatus::Cancelled => {
                        return Ok(Response::new(WaitForCompletionResponse {
                            success: false,
                            latency_us: start.elapsed().as_micros() as i64,
                            result: Vec::new(),
                            status: "cancelled".into(),
                            error: String::new(),
                        }))
                    }
                    WorkflowStatus::ContinuedAsNew => {
                        return Ok(Response::new(WaitForCompletionResponse {
                            success: true,
                            latency_us: start.elapsed().as_micros() as i64,
                            result: Vec::new(),
                            status: "continued_as_new".into(),
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
        let mut namespaces = self.engine.namespaces.lock().await;
        let already_exists = namespaces.contains_key(&req.name);
        if !already_exists {
            namespaces.insert(
                req.name.clone(),
                NamespaceInfo {
                    name: req.name.clone(),
                    description: req.description.clone(),
                    state: "REGISTERED".to_string(),
                    retention_days: 7,
                    owner_email: String::new(),
                    is_global: false,
                    created_at: TemporalEngine::now_us() / 1_000_000,
                },
            );
        }
        info!(name = %req.name, already_exists = already_exists, "Register namespace");
        Ok(Response::new(RegisterNamespaceResponse {
            success: true,
            already_exists,
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

    // ── Full implementations for all extended RPCs ────────────────────────

    async fn cancel_workflow(
        &self,
        req: Request<CancelWorkflowRequest>,
    ) -> Result<Response<CancelWorkflowResponse>, Status> {
        let start = Instant::now();
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .cancel_workflow(ns, &r.workflow_id, &r.reason)
            .await
        {
            Ok(()) => Ok(Response::new(CancelWorkflowResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(CancelWorkflowResponse {
                success: false,
                latency_us: start.elapsed().as_micros() as i64,
                error: e,
            })),
        }
    }
    async fn update_workflow_execution(
        &self,
        req: Request<UpdateWorkflowRequest>,
    ) -> Result<Response<UpdateWorkflowResponse>, Status> {
        let start = Instant::now();
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .update_workflow(ns, &r.workflow_id, &r.update_name, &r.update_id, r.payload)
            .await
        {
            Ok(result) => Ok(Response::new(UpdateWorkflowResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                result,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(UpdateWorkflowResponse {
                success: false,
                latency_us: 0,
                result: Vec::new(),
                error: e,
            })),
        }
    }
    async fn start_child_workflow(
        &self,
        req: Request<StartChildWorkflowRequest>,
    ) -> Result<Response<StartChildWorkflowResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .start_child_workflow(ns, &r.parent_workflow_id, &r.workflow_type, &r.workflow_id)
            .await
        {
            Ok((child_id, child_run_id)) => Ok(Response::new(StartChildWorkflowResponse {
                workflow_id: child_id,
                run_id: child_run_id,
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(StartChildWorkflowResponse {
                workflow_id: String::new(),
                run_id: String::new(),
                success: false,
                error: e,
            })),
        }
    }
    async fn schedule_timer(
        &self,
        req: Request<ScheduleTimerRequest>,
    ) -> Result<Response<ScheduleTimerResponse>, Status> {
        let start = Instant::now();
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .schedule_timer(ns, &r.workflow_id, &r.timer_id, r.duration_ms)
            .await
        {
            Ok(tid) => Ok(Response::new(ScheduleTimerResponse {
                success: true,
                timer_id: tid,
                latency_us: start.elapsed().as_micros() as i64,
            })),
            Err(_e) => Ok(Response::new(ScheduleTimerResponse {
                success: false,
                timer_id: String::new(),
                latency_us: 0,
            })),
        }
    }
    async fn cancel_timer(
        &self,
        req: Request<CancelTimerRequest>,
    ) -> Result<Response<CancelTimerResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .cancel_timer(ns, &r.workflow_id, &r.timer_id)
            .await
        {
            Ok(()) => Ok(Response::new(CancelTimerResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(CancelTimerResponse {
                success: false,
                error: e,
            })),
        }
    }
    async fn continue_as_new(
        &self,
        req: Request<ContinueAsNewRequest>,
    ) -> Result<Response<ContinueAsNewResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        let wt = if r.workflow_type.is_empty() {
            "default"
        } else {
            &r.workflow_type
        };
        match self.engine.continue_as_new(ns, &r.workflow_id, wt).await {
            Ok(new_run_id) => Ok(Response::new(ContinueAsNewResponse {
                new_run_id,
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ContinueAsNewResponse {
                new_run_id: String::new(),
                success: false,
                error: e,
            })),
        }
    }
    async fn upsert_search_attributes(
        &self,
        req: Request<UpsertSearchAttributesRequest>,
    ) -> Result<Response<UpsertSearchAttributesResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .upsert_search_attributes(ns, &r.workflow_id, r.search_attributes)
            .await
        {
            Ok(()) => Ok(Response::new(UpsertSearchAttributesResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(UpsertSearchAttributesResponse {
                success: false,
                error: e,
            })),
        }
    }
    async fn set_memo(
        &self,
        req: Request<SetMemoRequest>,
    ) -> Result<Response<SetMemoResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self.engine.set_memo(ns, &r.workflow_id, r.memo).await {
            Ok(()) => Ok(Response::new(SetMemoResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(SetMemoResponse {
                success: false,
                error: e,
            })),
        }
    }
    async fn signal_with_start(
        &self,
        req: Request<SignalWithStartRequest>,
    ) -> Result<Response<SignalWithStartResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .signal_with_start(
                ns,
                &r.workflow_type,
                &r.workflow_id,
                &r.signal_name,
                r.signal_payload,
            )
            .await
        {
            Ok((wf_id, run_id, started, signaled)) => Ok(Response::new(SignalWithStartResponse {
                workflow_id: wf_id,
                run_id,
                started,
                signaled,
            })),
            Err(e) => Err(Status::internal(e)),
        }
    }
    async fn record_activity_heartbeat(
        &self,
        req: Request<RecordActivityHeartbeatRequest>,
    ) -> Result<Response<RecordActivityHeartbeatResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .record_heartbeat(ns, &r.workflow_id, &r.activity_id)
            .await
        {
            Ok(cancel_requested) => Ok(Response::new(RecordActivityHeartbeatResponse {
                success: true,
                cancel_requested,
            })),
            Err(_e) => Ok(Response::new(RecordActivityHeartbeatResponse {
                success: false,
                cancel_requested: false,
            })),
        }
    }
    async fn schedule_activity(
        &self,
        req: Request<ScheduleActivityRequest>,
    ) -> Result<Response<ScheduleActivityResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .schedule_activity(ns, &r.workflow_id, &r.activity_id, &r.activity_type)
            .await
        {
            Ok(aid) => Ok(Response::new(ScheduleActivityResponse {
                activity_id: aid,
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ScheduleActivityResponse {
                activity_id: String::new(),
                success: false,
                error: e,
            })),
        }
    }
    async fn complete_activity_task(
        &self,
        req: Request<CompleteActivityTaskRequest>,
    ) -> Result<Response<CompleteActivityTaskResponse>, Status> {
        let start = Instant::now();
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .complete_activity(ns, &r.workflow_id, &r.activity_id)
            .await
        {
            Ok(()) => Ok(Response::new(CompleteActivityTaskResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(CompleteActivityTaskResponse {
                success: false,
                latency_us: 0,
                error: e,
            })),
        }
    }
    async fn fail_activity_task(
        &self,
        req: Request<FailActivityTaskRequest>,
    ) -> Result<Response<FailActivityTaskResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .fail_activity(
                ns,
                &r.workflow_id,
                &r.activity_id,
                &r.reason,
                r.non_retryable,
            )
            .await
        {
            Ok((will_retry, next)) => Ok(Response::new(FailActivityTaskResponse {
                success: true,
                will_retry,
                next_attempt: next as i32,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(FailActivityTaskResponse {
                success: false,
                will_retry: false,
                next_attempt: 0,
                error: e,
            })),
        }
    }
    async fn replay_workflow(
        &self,
        req: Request<ReplayWorkflowRequest>,
    ) -> Result<Response<ReplayWorkflowResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self.engine.replay_workflow(ns, &r.workflow_id).await {
            Ok((events, status)) => Ok(Response::new(ReplayWorkflowResponse {
                success: true,
                events_replayed: events as i64,
                final_status: status,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ReplayWorkflowResponse {
                success: false,
                events_replayed: 0,
                final_status: String::new(),
                error: e,
            })),
        }
    }
    async fn reset_workflow(
        &self,
        req: Request<ResetWorkflowRequest>,
    ) -> Result<Response<ResetWorkflowResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self
            .engine
            .reset_workflow(ns, &r.workflow_id, r.reset_to_event_id, &r.reason)
            .await
        {
            Ok(new_run_id) => Ok(Response::new(ResetWorkflowResponse {
                new_run_id,
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ResetWorkflowResponse {
                new_run_id: String::new(),
                success: false,
                error: e,
            })),
        }
    }
    async fn batch_terminate(
        &self,
        req: Request<BatchTerminateRequest>,
    ) -> Result<Response<BatchTerminateResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        let count = self
            .engine
            .batch_terminate(ns, &r.reason, r.max_count)
            .await;
        Ok(Response::new(BatchTerminateResponse {
            terminated_count: count as i64,
        }))
    }
    async fn batch_signal(
        &self,
        req: Request<BatchSignalRequest>,
    ) -> Result<Response<BatchSignalResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        let count = self
            .engine
            .batch_signal(ns, &r.signal_name, r.payload, r.max_count)
            .await;
        Ok(Response::new(BatchSignalResponse {
            signaled_count: count as i64,
        }))
    }
    async fn describe_namespace(
        &self,
        req: Request<DescribeNamespaceRequest>,
    ) -> Result<Response<DescribeNamespaceResponse>, Status> {
        let r = req.into_inner();
        let namespaces = self.engine.namespaces.lock().await;
        match namespaces.get(&r.name) {
            Some(ns) => Ok(Response::new(DescribeNamespaceResponse {
                name: ns.name.clone(),
                id: format!("ns-{}", ns.name),
                description: ns.description.clone(),
                state: ns.state.clone(),
                retention_days: ns.retention_days,
                owner_email: ns.owner_email.clone(),
                is_global: ns.is_global,
                created_at: ns.created_at,
            })),
            None => Err(Status::not_found(format!("Namespace {} not found", r.name))),
        }
    }
    async fn update_namespace(
        &self,
        req: Request<UpdateNamespaceRequest>,
    ) -> Result<Response<UpdateNamespaceResponse>, Status> {
        let r = req.into_inner();
        let mut namespaces = self.engine.namespaces.lock().await;
        match namespaces.get_mut(&r.name) {
            Some(ns) => {
                if !r.description.is_empty() {
                    ns.description = r.description;
                }
                if r.retention_days > 0 {
                    ns.retention_days = r.retention_days;
                }
                if !r.owner_email.is_empty() {
                    ns.owner_email = r.owner_email;
                }
                Ok(Response::new(UpdateNamespaceResponse {
                    success: true,
                    error: String::new(),
                }))
            }
            None => Ok(Response::new(UpdateNamespaceResponse {
                success: false,
                error: format!("Namespace {} not found", r.name),
            })),
        }
    }
    async fn delete_namespace(
        &self,
        req: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceResponse>, Status> {
        let r = req.into_inner();
        // Remove namespace registration
        {
            let mut namespaces = self.engine.namespaces.lock().await;
            namespaces.remove(&r.name);
        }
        // Clear all workflows in this namespace
        let cleared = self.engine.reset(&r.name).await;
        debug!(namespace = %r.name, cleared = cleared, "Deleted namespace");
        Ok(Response::new(DeleteNamespaceResponse {
            success: true,
            error: String::new(),
        }))
    }
    async fn poll_workflow_task(
        &self,
        req: Request<PollWorkflowTaskRequest>,
    ) -> Result<Response<PollWorkflowTaskResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        // Find a running workflow in this namespace/task queue
        let all_events: Vec<(String, Vec<HistoryEvent>)> = {
            let logs = self.engine.logs.lock().await;
            logs.iter()
                .filter(|(_, log)| log.namespace == ns)
                .map(|(id, log)| (id.clone(), log.events.clone()))
                .collect()
        };
        for (wf_id, events) in &all_events {
            let replayed = TemporalEngine::replay_events(events);
            if replayed.status == WorkflowStatus::Running {
                if let Some(last) = events.last() {
                    return Ok(Response::new(PollWorkflowTaskResponse {
                        task_token: format!("wt-{}-{}", wf_id, uuid::Uuid::new_v4()),
                        event_id: last.event_id as i64,
                        event_type: last.event_type.clone(),
                        workflow_execution: Vec::new(),
                        has_task: true,
                    }));
                }
            }
        }
        Ok(Response::new(PollWorkflowTaskResponse {
            task_token: String::new(),
            event_id: 0,
            event_type: String::new(),
            workflow_execution: Vec::new(),
            has_task: false,
        }))
    }
    async fn poll_activity_task(
        &self,
        req: Request<PollActivityTaskRequest>,
    ) -> Result<Response<PollActivityTaskResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        // Scan event logs for workflows with scheduled-but-not-completed activities
        let all_events: Vec<(String, Vec<HistoryEvent>)> = {
            let logs = self.engine.logs.lock().await;
            logs.iter()
                .filter(|(_, log)| log.namespace == ns)
                .map(|(id, log)| (id.clone(), log.events.clone()))
                .collect()
        };
        for (wf_id, events) in &all_events {
            let replayed = TemporalEngine::replay_events(events);
            if replayed.status != WorkflowStatus::Running {
                continue;
            }
            // Find scheduled activities that haven't been completed/failed yet
            let mut scheduled: Vec<(String, String)> = Vec::new(); // (activity_id, activity_type)
            let mut completed: std::collections::HashSet<String> = std::collections::HashSet::new();
            for event in events {
                match event.event_type.as_str() {
                    "ActivityTaskScheduled" => {
                        if let (Some(aid), Some(atype)) = (
                            event.attributes.get("activity_id").and_then(|v| v.as_str()),
                            event
                                .attributes
                                .get("activity_type")
                                .and_then(|v| v.as_str()),
                        ) {
                            scheduled.push((aid.to_string(), atype.to_string()));
                        }
                    }
                    "ActivityTaskCompleted" | "ActivityTaskFailed" => {
                        if let Some(aid) =
                            event.attributes.get("activity_id").and_then(|v| v.as_str())
                        {
                            completed.insert(aid.to_string());
                        }
                    }
                    _ => {}
                }
            }
            // Return the first uncompleted activity
            for (aid, atype) in &scheduled {
                if !completed.contains(aid.as_str()) {
                    return Ok(Response::new(PollActivityTaskResponse {
                        task_token: format!("at-{}-{}", wf_id, uuid::Uuid::new_v4()),
                        activity_id: aid.clone(),
                        activity_type: atype.clone(),
                        input: Vec::new(),
                        workflow_id: wf_id.clone(),
                        has_task: true,
                        scheduled_time: TemporalEngine::now_us(),
                    }));
                }
            }
        }
        Ok(Response::new(PollActivityTaskResponse {
            task_token: String::new(),
            activity_id: String::new(),
            activity_type: String::new(),
            input: Vec::new(),
            workflow_id: String::new(),
            has_task: false,
            scheduled_time: 0,
        }))
    }
    async fn get_workflow_history(
        &self,
        req: Request<GetWorkflowHistoryRequest>,
    ) -> Result<Response<GetWorkflowHistoryResponse>, Status> {
        let r = req.into_inner();
        let ns = if r.namespace.is_empty() {
            "default"
        } else {
            &r.namespace
        };
        match self.engine.get_workflow_history(ns, &r.workflow_id).await {
            Ok(events) => {
                let total = events.len() as i64;
                let page_size = if r.max_page_size > 0 {
                    r.max_page_size as usize
                } else {
                    1000
                };
                let serialized: Vec<Vec<u8>> = events
                    .iter()
                    .take(page_size)
                    .map(|e| {
                        serde_json::to_vec(&serde_json::json!({
                            "event_id": e.event_id, "event_type": e.event_type,
                            "timestamp_us": e.timestamp_us, "attributes": e.attributes,
                        }))
                        .unwrap_or_default()
                    })
                    .collect();
                Ok(Response::new(GetWorkflowHistoryResponse {
                    events: serialized,
                    next_page_token: Vec::new(),
                    total_event_count: total,
                }))
            }
            Err(e) => Err(Status::not_found(e)),
        }
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

    let bind_ip: String = args
        .iter()
        .position(|a| a == "--ip")
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| "127.0.0.1".to_string());

    let grpc_addr: SocketAddr = format!("{}:{}", bind_ip, grpc_port).parse()?;

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
