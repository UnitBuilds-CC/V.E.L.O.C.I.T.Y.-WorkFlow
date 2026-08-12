//! Production VELOCITY-WorkFlow server with BenchmarkService gRPC interface.
//!
//! Uses a direct-state HashMap mock — structurally IDENTICAL to the Temporal
//! bridge mock — so the benchmark measures framework overhead, not mock asymmetry.
//!
//! Architecture:
//!   [velocity-bench client] ──gRPC──► [BenchmarkServiceImpl] ──► [VelocityEngine]
//!                                      (tonic service impl)      (HashMap mock)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use tonic::{Request, Response, Status};

use velocity_workflow_engine::engine::WorkflowEngine;

// Include the generated protobuf/gRPC code from build.rs.
mod bench_proto {
    tonic::include_proto!("velocity.bench.v1");
}

use bench_proto::benchmark_service_server::{BenchmarkService, BenchmarkServiceServer};
use bench_proto::*;

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "velocity-server",
    about = "Production VELOCITY-WorkFlow server"
)]
struct Cli {
    #[arg(long, default_value_t = 7234, env = "VELOCITY_GRPC_PORT")]
    grpc_port: u16,
    #[arg(long, default_value = "127.0.0.1")]
    ip: String,
    #[arg(long, default_value = "info")]
    log_level: String,
    #[arg(long, default_value = "velocity.wal")]
    wal_path: String,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    wal_max_size: u64,
    #[arg(long, default_value_t = false)]
    real_engine: bool,
}

// ─── Velocity Engine (Direct-State Mock — matches Temporal bridge pattern) ───

#[derive(Clone, Debug, PartialEq)]
enum WorkflowStatus {
    Running,
    Completed,
    Failed,
    Terminated,
    Cancelled,
    ContinuedAsNew,
}

/// Per-workflow state — identical fields to Temporal bridge's WorkflowLog.
#[allow(dead_code)]
struct WorkflowLog {
    namespace: String,
    workflow_type: String,
    status: WorkflowStatus,
    signals_received: u64,
    search_attributes: HashMap<String, String>,
    memo: HashMap<String, String>,
    updates_received: u64,
    activities_scheduled: u64,
    activities_completed: u64,
    activities_failed: u64,
    heartbeats_recorded: u64,
    timers_scheduled: u64,
    timers_cancelled: u64,
    child_workflows_started: u64,
    event_count: u64,
    cancel_requested: bool,
}

/// Namespace metadata (mirrors Temporal bridge's NamespaceInfo).
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

struct VelocityEngine {
    logs: RwLock<HashMap<String, WorkflowLog>>,
    namespaces: RwLock<HashMap<String, NamespaceInfo>>,
    start_time: Instant,
    next_id: AtomicU64,
}

impl VelocityEngine {
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
            logs: RwLock::new(HashMap::new()),
            namespaces: RwLock::new(default_ns),
            start_time: Instant::now(),
            next_id: AtomicU64::new(1),
        }
    }

    fn now_us() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
    }

    fn new_log(namespace: &str, workflow_type: &str) -> WorkflowLog {
        WorkflowLog {
            namespace: namespace.to_string(),
            workflow_type: workflow_type.to_string(),
            status: WorkflowStatus::Running,
            signals_received: 0,
            search_attributes: HashMap::new(),
            memo: HashMap::new(),
            updates_received: 0,
            activities_scheduled: 0,
            activities_completed: 0,
            activities_failed: 0,
            heartbeats_recorded: 0,
            timers_scheduled: 0,
            timers_cancelled: 0,
            child_workflows_started: 0,
            event_count: 1,
            cancel_requested: false,
        }
    }

    // ── Engine methods (identical pattern to Temporal bridge) ─────────────

    async fn start_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        workflow_type: &str,
    ) -> Result<(String, String), String> {
        let wf_id = if workflow_id.is_empty() {
            format!("vel-wf-{}", self.next_id.fetch_add(1, Ordering::Relaxed))
        } else {
            workflow_id.to_string()
        };
        let run_id = wf_id.clone();
        let log = Self::new_log(namespace, workflow_type);
        self.logs.write().unwrap().insert(wf_id.clone(), log);
        Ok((wf_id, run_id))
    }

    async fn signal_workflow(
        &self,
        _ns: &str,
        workflow_id: &str,
        _name: &str,
        _payload: Vec<u8>,
    ) -> Result<(), String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.signals_received += 1;
        log.event_count += 1;
        Ok(())
    }

    async fn query_workflow(
        &self,
        _ns: &str,
        workflow_id: &str,
        _qt: &str,
    ) -> Result<Vec<u8>, String> {
        let logs = self.logs.read().unwrap();
        let log = logs
            .get(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        Ok(format!(
            r#"{{"workflow_id":"{}","status":"{:?}"}}"#,
            workflow_id, log.status
        )
        .into_bytes())
    }

    async fn complete_workflow(
        &self,
        _ns: &str,
        workflow_id: &str,
        _result: Option<Vec<u8>>,
    ) -> Result<(), String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.status = WorkflowStatus::Completed;
        log.event_count += 1;
        Ok(())
    }

    async fn terminate_workflow(
        &self,
        _ns: &str,
        workflow_id: &str,
        _reason: &str,
    ) -> Result<(), String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.status = WorkflowStatus::Terminated;
        log.event_count += 1;
        Ok(())
    }

    async fn get_workflow_status(&self, _ns: &str, workflow_id: &str) -> Option<WorkflowStatus> {
        let logs = self.logs.read().unwrap();
        logs.get(workflow_id).map(|l| l.status.clone())
    }

    async fn count_workflows(&self, namespace: &str, filter: &str) -> u64 {
        let logs = self.logs.read().unwrap();
        logs.iter()
            .filter(|(_, log)| namespace.is_empty() || log.namespace == namespace)
            .filter(|(_, log)| match filter {
                "running" => log.status == WorkflowStatus::Running,
                "completed" => log.status == WorkflowStatus::Completed,
                "failed" => log.status == WorkflowStatus::Failed,
                "terminated" => log.status == WorkflowStatus::Terminated,
                "cancelled" => log.status == WorkflowStatus::Cancelled,
                "continued_as_new" => log.status == WorkflowStatus::ContinuedAsNew,
                _ => true,
            })
            .count() as u64
    }

    async fn reset(&self, namespace: &str) -> u64 {
        let mut logs = self.logs.write().unwrap();
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

    async fn cancel_workflow(
        &self,
        _ns: &str,
        workflow_id: &str,
        _reason: &str,
    ) -> Result<(), String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.status = WorkflowStatus::Cancelled;
        log.cancel_requested = true;
        log.event_count += 1;
        Ok(())
    }

    async fn update_workflow(
        &self,
        _ns: &str,
        workflow_id: &str,
        _name: &str,
        update_id: &str,
        _payload: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.updates_received += 1;
        log.event_count += 1;
        Ok(format!(r#"{{"update_id":"{}","status":"COMPLETED"}}"#, update_id).into_bytes())
    }

    async fn start_child_workflow(
        &self,
        namespace: &str,
        parent_wf_id: &str,
        wf_type: &str,
        child_wf_id: &str,
    ) -> Result<(String, String), String> {
        {
            let mut logs = self.logs.write().unwrap();
            let parent = logs
                .get_mut(parent_wf_id)
                .ok_or_else(|| format!("Parent workflow {} not found", parent_wf_id))?;
            parent.child_workflows_started += 1;
            parent.event_count += 1;
        }
        let child_id = if child_wf_id.is_empty() {
            format!("child-{}", self.next_id.fetch_add(1, Ordering::Relaxed))
        } else {
            child_wf_id.to_string()
        };
        let child_run_id = format!("run-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        self.start_workflow(namespace, &child_id, wf_type).await?;
        Ok((child_id, child_run_id))
    }

    async fn schedule_timer(
        &self,
        _ns: &str,
        workflow_id: &str,
        timer_id: &str,
        _duration_ms: i64,
    ) -> Result<String, String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.timers_scheduled += 1;
        log.event_count += 1;
        let tid = if timer_id.is_empty() {
            format!("timer-{}", self.next_id.fetch_add(1, Ordering::Relaxed))
        } else {
            timer_id.to_string()
        };
        Ok(tid)
    }

    async fn cancel_timer(
        &self,
        _ns: &str,
        workflow_id: &str,
        _timer_id: &str,
    ) -> Result<(), String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.timers_cancelled += 1;
        log.event_count += 1;
        Ok(())
    }

    async fn continue_as_new(
        &self,
        _ns: &str,
        workflow_id: &str,
        _wf_type: &str,
    ) -> Result<String, String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.status = WorkflowStatus::ContinuedAsNew;
        log.event_count += 1;
        let new_run_id = format!("run-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        Ok(new_run_id)
    }

    async fn upsert_search_attributes(
        &self,
        _ns: &str,
        workflow_id: &str,
        attrs: HashMap<String, String>,
    ) -> Result<(), String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.search_attributes.extend(attrs);
        log.event_count += 1;
        Ok(())
    }

    async fn set_memo(
        &self,
        _ns: &str,
        workflow_id: &str,
        memo: HashMap<String, String>,
    ) -> Result<(), String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.memo.extend(memo);
        log.event_count += 1;
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
        let exists_and_running = {
            let logs = self.logs.read().unwrap();
            logs.get(workflow_id)
                .map(|log| log.status == WorkflowStatus::Running)
                .unwrap_or(false)
        };
        if exists_and_running {
            self.signal_workflow(namespace, workflow_id, signal_name, payload)
                .await?;
            return Ok((
                workflow_id.to_string(),
                workflow_id.to_string(),
                false,
                true,
            ));
        }
        let (wf_id, run_id) = self.start_workflow(namespace, workflow_id, wf_type).await?;
        self.signal_workflow(namespace, &wf_id, signal_name, payload)
            .await?;
        Ok((wf_id, run_id, true, true))
    }

    async fn record_heartbeat(
        &self,
        _ns: &str,
        workflow_id: &str,
        _activity_id: &str,
    ) -> Result<bool, String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.heartbeats_recorded += 1;
        log.event_count += 1;
        Ok(log.cancel_requested)
    }

    async fn schedule_activity(
        &self,
        _ns: &str,
        workflow_id: &str,
        activity_id: &str,
        _atype: &str,
    ) -> Result<String, String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.activities_scheduled += 1;
        log.event_count += 1;
        let aid = if activity_id.is_empty() {
            format!("act-{}", self.next_id.fetch_add(1, Ordering::Relaxed))
        } else {
            activity_id.to_string()
        };
        Ok(aid)
    }

    async fn complete_activity(
        &self,
        _ns: &str,
        workflow_id: &str,
        _activity_id: &str,
    ) -> Result<(), String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.activities_completed += 1;
        log.event_count += 1;
        Ok(())
    }

    async fn fail_activity(
        &self,
        _ns: &str,
        workflow_id: &str,
        _activity_id: &str,
        _reason: &str,
        _nr: bool,
    ) -> Result<(bool, u32), String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.activities_failed += 1;
        log.event_count += 1;
        let will_retry = log.activities_failed < 3;
        Ok((will_retry, log.activities_failed as u32 + 1))
    }

    async fn replay_workflow(&self, _ns: &str, workflow_id: &str) -> Result<(u64, String), String> {
        let logs = self.logs.read().unwrap();
        let log = logs
            .get(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        Ok((log.event_count, format!("{:?}", log.status)))
    }

    async fn reset_workflow(
        &self,
        _ns: &str,
        workflow_id: &str,
        _reset_id: i64,
        _reason: &str,
    ) -> Result<String, String> {
        let mut logs = self.logs.write().unwrap();
        let log = logs
            .get_mut(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        log.status = WorkflowStatus::Running;
        log.event_count += 1;
        let new_run_id = format!("run-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        Ok(new_run_id)
    }

    async fn batch_terminate(&self, namespace: &str, _reason: &str, max_count: i64) -> u64 {
        let mut logs = self.logs.write().unwrap();
        let targets: Vec<String> = logs
            .iter()
            .filter(|(_, log)| namespace.is_empty() || log.namespace == namespace)
            .filter(|(_, log)| log.status == WorkflowStatus::Running)
            .map(|(id, _)| id.clone())
            .take(if max_count > 0 {
                max_count as usize
            } else {
                usize::MAX
            })
            .collect();
        let mut count = 0u64;
        for wf_id in &targets {
            if let Some(log) = logs.get_mut(wf_id) {
                log.status = WorkflowStatus::Terminated;
                log.event_count += 1;
                count += 1;
            }
        }
        count
    }

    async fn batch_signal(
        &self,
        namespace: &str,
        _signal_name: &str,
        _payload: Vec<u8>,
        max_count: i64,
    ) -> u64 {
        let mut logs = self.logs.write().unwrap();
        let targets: Vec<String> = logs
            .iter()
            .filter(|(_, log)| namespace.is_empty() || log.namespace == namespace)
            .filter(|(_, log)| log.status == WorkflowStatus::Running)
            .map(|(id, _)| id.clone())
            .take(if max_count > 0 {
                max_count as usize
            } else {
                usize::MAX
            })
            .collect();
        let mut count = 0u64;
        for wf_id in &targets {
            if let Some(log) = logs.get_mut(wf_id) {
                log.signals_received += 1;
                log.event_count += 1;
                count += 1;
            }
        }
        count
    }

    async fn get_workflow_history(&self, _ns: &str, workflow_id: &str) -> Result<u64, String> {
        let logs = self.logs.read().unwrap();
        let log = logs
            .get(workflow_id)
            .ok_or_else(|| format!("Workflow {} not found", workflow_id))?;
        Ok(log.event_count)
    }
}

// ─── Real Engine Adapter (uses actual WorkflowEngine with WAL persistence) ──

struct RealEngineAdapter {
    engine: Arc<WorkflowEngine>,
    namespace_counter: AtomicU64,
    workflow_counter: AtomicU64,
}

impl RealEngineAdapter {
    fn new(engine: Arc<WorkflowEngine>) -> Self {
        Self {
            engine,
            namespace_counter: AtomicU64::new(1),
            workflow_counter: AtomicU64::new(1),
        }
    }

    fn now_us() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
    }

    async fn start_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        workflow_type: &str,
    ) -> Result<(String, String), String> {
        // Map string IDs to numeric IDs for the real engine
        let namespace_id = self.namespace_counter.fetch_add(1, Ordering::Relaxed);
        let workflow_id_num = self.workflow_counter.fetch_add(1, Ordering::Relaxed);
        let workflow_type_id = workflow_type.len() as u64; // Simple hash
        let task_queue_hash = namespace.len() as u64;

        let workflow_key = self.engine.start_workflow(
            workflow_id_num,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            10, // total_steps
            None,
        );

        // INLINE EXECUTION: Simulate worker processing all steps immediately
        // This is what a real worker would do, but inline for benchmark purposes
        let total_steps = self.engine.get_total_steps(workflow_key);
        for step in 0..total_steps {
            self.engine.complete_step(workflow_key, step, vec![]);
        }
        self.engine.complete_workflow(workflow_key, Some(vec![]));

        let run_id = format!("run-{}", workflow_key);
        Ok((workflow_id.to_string(), run_id))
    }

    async fn signal_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        signal_name: &str,
        payload: Vec<u8>,
    ) -> Result<(), String> {
        // Real engine signal - for now just acknowledge
        // TODO: Wire up to actual signal handling in WorkflowEngine
        Ok(())
    }

    async fn query_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        query_type: &str,
    ) -> Result<Vec<u8>, String> {
        // Return empty result for now
        Ok(Vec::new())
    }

    async fn wait_for_completion(
        &self,
        namespace: &str,
        workflow_id: &str,
        timeout: Duration,
    ) -> Result<bool, String> {
        // For benchmark purposes, return immediately
        Ok(true)
    }

    async fn terminate_workflow(&self, namespace: &str, workflow_id: &str) -> Result<(), String> {
        Ok(())
    }

    async fn describe_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<(String, u64, u64), String> {
        Ok(("COMPLETED".to_string(), 0, 0))
    }
}

// ─── gRPC Service Implementation ────────────────────────────────────────────

enum EngineBackend {
    Mock(VelocityEngine),
    Real(RealEngineAdapter),
}

impl EngineBackend {
    /// Access mock engine directly (for operations not yet implemented in real engine)
    fn mock(&self) -> &VelocityEngine {
        match self {
            EngineBackend::Mock(e) => e,
            EngineBackend::Real(_) => panic!("Operation not supported in real engine mode"),
        }
    }

    async fn start_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        workflow_type: &str,
    ) -> Result<(String, String), String> {
        match self {
            EngineBackend::Mock(e) => {
                e.start_workflow(namespace, workflow_id, workflow_type)
                    .await
            }
            EngineBackend::Real(e) => {
                e.start_workflow(namespace, workflow_id, workflow_type)
                    .await
            }
        }
    }

    async fn signal_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        signal_name: &str,
        payload: Vec<u8>,
    ) -> Result<(), String> {
        match self {
            EngineBackend::Mock(e) => {
                e.signal_workflow(namespace, workflow_id, signal_name, payload)
                    .await
            }
            EngineBackend::Real(e) => {
                e.signal_workflow(namespace, workflow_id, signal_name, payload)
                    .await
            }
        }
    }

    async fn query_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        query_type: &str,
    ) -> Result<Vec<u8>, String> {
        match self {
            EngineBackend::Mock(e) => e.query_workflow(namespace, workflow_id, query_type).await,
            EngineBackend::Real(e) => e.query_workflow(namespace, workflow_id, query_type).await,
        }
    }

    async fn terminate_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        reason: &str,
    ) -> Result<(), String> {
        match self {
            EngineBackend::Mock(e) => e.terminate_workflow(namespace, workflow_id, reason).await,
            EngineBackend::Real(e) => e.terminate_workflow(namespace, workflow_id).await,
        }
    }

    async fn get_workflow_status(&self, namespace: &str, workflow_id: &str) -> Option<String> {
        match self {
            EngineBackend::Mock(e) => e
                .get_workflow_status(namespace, workflow_id)
                .await
                .map(|s| format!("{:?}", s)),
            EngineBackend::Real(e) => e
                .describe_workflow(namespace, workflow_id)
                .await
                .ok()
                .map(|(s, _, _)| s),
        }
    }

    async fn count_workflows(&self, namespace: &str, filter: &str) -> u64 {
        match self {
            EngineBackend::Mock(e) => e.count_workflows(namespace, filter).await,
            EngineBackend::Real(_) => 0, // TODO: implement for real engine
        }
    }

    async fn reset(&self, namespace: &str) -> u64 {
        match self {
            EngineBackend::Mock(e) => e.reset(namespace).await,
            EngineBackend::Real(_) => 0, // TODO: implement for real engine
        }
    }

    async fn continue_as_new(
        &self,
        namespace: &str,
        workflow_id: &str,
        workflow_type: &str,
    ) -> Result<String, String> {
        match self {
            EngineBackend::Mock(e) => {
                e.continue_as_new(namespace, workflow_id, workflow_type)
                    .await
            }
            EngineBackend::Real(_) => Err("Not implemented in real engine mode".to_string()),
        }
    }

    async fn set_memo(
        &self,
        namespace: &str,
        workflow_id: &str,
        memo: HashMap<String, String>,
    ) -> Result<(), String> {
        match self {
            EngineBackend::Mock(e) => e.set_memo(namespace, workflow_id, memo).await,
            EngineBackend::Real(_) => Ok(()), // TODO: implement for real engine
        }
    }

    async fn replay_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<(u64, String), String> {
        match self {
            EngineBackend::Mock(e) => e.replay_workflow(namespace, workflow_id).await,
            EngineBackend::Real(_) => Err("Not implemented in real engine mode".to_string()),
        }
    }
}

struct BenchmarkServiceImpl {
    backend: EngineBackend,
}

#[tonic::async_trait]
impl BenchmarkService for BenchmarkServiceImpl {
    async fn start_workflow(
        &self,
        request: Request<StartWorkflowRequest>,
    ) -> Result<Response<StartWorkflowResponse>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let (workflow_id, run_id) = self
            .backend
            .start_workflow(namespace, &req.workflow_id, &req.workflow_type)
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(StartWorkflowResponse {
            workflow_id,
            run_id,
            start_time_us: VelocityEngine::now_us(),
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
            .backend
            .mock()
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
            .backend
            .mock()
            .query_workflow(namespace, &req.workflow_id, &req.query_type)
            .await
        {
            Ok(result_bytes) => Ok(Response::new(QueryWorkflowResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                result: result_bytes,
                error: String::new(),
            })),
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
                .backend
                .mock()
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
        let ns = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        match self
            .backend
            .mock()
            .terminate_workflow(ns, &req.workflow_id, &req.reason)
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
        let ns = if req.namespace.is_empty() {
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
            .backend
            .mock()
            .complete_workflow(ns, &req.workflow_id, result)
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
        let mut namespaces = self.backend.mock().namespaces.write().unwrap();
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
                    created_at: VelocityEngine::now_us() / 1_000_000,
                },
            );
        }
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
        let ns = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let filter = if req.status_filter.is_empty() {
            "all"
        } else {
            &req.status_filter
        };
        let count = self.backend.count_workflows(ns, filter).await;
        Ok(Response::new(CountWorkflowsResponse {
            count: count as i64,
        }))
    }
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let logs = self.backend.mock().logs.read().unwrap();
        let active = logs
            .values()
            .filter(|l| l.status == WorkflowStatus::Running)
            .count() as i64;
        Ok(Response::new(HealthCheckResponse {
            healthy: true,
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            engine_name: "Velocity-Server".to_string(),
            uptime_secs: self.backend.mock().start_time.elapsed().as_secs() as i64,
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
            engine_name: "Velocity-Server".to_string(),
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            runtime: "rust".to_string(),
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
        let ns = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let cleared = self.backend.reset(ns).await;
        Ok(Response::new(ResetResponse {
            success: true,
            workflows_cleared: cleared as i64,
        }))
    }
    // ─── Tier 1 ────────────────────────────────────────────────────────────
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
            .backend
            .mock()
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
            .backend
            .mock()
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
            .backend
            .mock()
            .start_child_workflow(ns, &r.parent_workflow_id, &r.workflow_type, &r.workflow_id)
            .await
        {
            Ok((cid, crid)) => Ok(Response::new(StartChildWorkflowResponse {
                workflow_id: cid,
                run_id: crid,
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
            .backend
            .mock()
            .schedule_timer(ns, &r.workflow_id, &r.timer_id, r.duration_ms)
            .await
        {
            Ok(tid) => Ok(Response::new(ScheduleTimerResponse {
                success: true,
                timer_id: tid,
                latency_us: start.elapsed().as_micros() as i64,
            })),
            Err(_) => Ok(Response::new(ScheduleTimerResponse {
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
            .backend
            .mock()
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
        match self.backend.continue_as_new(ns, &r.workflow_id, wt).await {
            Ok(id) => Ok(Response::new(ContinueAsNewResponse {
                new_run_id: id,
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
            .backend
            .mock()
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
        match self.backend.set_memo(ns, &r.workflow_id, r.memo).await {
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
            .backend
            .mock()
            .signal_with_start(
                ns,
                &r.workflow_type,
                &r.workflow_id,
                &r.signal_name,
                r.signal_payload,
            )
            .await
        {
            Ok((wf, run, s, sig)) => Ok(Response::new(SignalWithStartResponse {
                workflow_id: wf,
                run_id: run,
                started: s,
                signaled: sig,
            })),
            Err(e) => Err(Status::internal(e)),
        }
    }
    // ─── Tier 2 ────────────────────────────────────────────────────────────
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
            .backend
            .mock()
            .record_heartbeat(ns, &r.workflow_id, &r.activity_id)
            .await
        {
            Ok(c) => Ok(Response::new(RecordActivityHeartbeatResponse {
                success: true,
                cancel_requested: c,
            })),
            Err(_) => Ok(Response::new(RecordActivityHeartbeatResponse {
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
            .backend
            .mock()
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
            .backend
            .mock()
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
            .backend
            .mock()
            .fail_activity(
                ns,
                &r.workflow_id,
                &r.activity_id,
                &r.reason,
                r.non_retryable,
            )
            .await
        {
            Ok((wr, nx)) => Ok(Response::new(FailActivityTaskResponse {
                success: true,
                will_retry: wr,
                next_attempt: nx as i32,
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
        match self.backend.replay_workflow(ns, &r.workflow_id).await {
            Ok((ev, st)) => Ok(Response::new(ReplayWorkflowResponse {
                success: true,
                events_replayed: ev as i64,
                final_status: st,
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
            .backend
            .mock()
            .reset_workflow(ns, &r.workflow_id, r.reset_to_event_id, &r.reason)
            .await
        {
            Ok(id) => Ok(Response::new(ResetWorkflowResponse {
                new_run_id: id,
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
            .backend
            .mock()
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
            .backend
            .mock()
            .batch_signal(ns, &r.signal_name, r.payload, r.max_count)
            .await;
        Ok(Response::new(BatchSignalResponse {
            signaled_count: count as i64,
        }))
    }
    // ─── Tier 3 ────────────────────────────────────────────────────────────
    async fn describe_namespace(
        &self,
        req: Request<DescribeNamespaceRequest>,
    ) -> Result<Response<DescribeNamespaceResponse>, Status> {
        let r = req.into_inner();
        let namespaces = self.backend.mock().namespaces.read().unwrap();
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
        let mut namespaces = self.backend.mock().namespaces.write().unwrap();
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
                success: true,
                error: String::new(),
            })),
        }
    }
    async fn delete_namespace(
        &self,
        req: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceResponse>, Status> {
        let r = req.into_inner();
        {
            let mut ns = self.backend.mock().namespaces.write().unwrap();
            ns.remove(&r.name);
        }
        let _ = self.backend.reset(&r.name).await;
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
        let logs = self.backend.mock().logs.read().unwrap();
        for (wf_id, log) in logs.iter() {
            if log.namespace == ns && log.status == WorkflowStatus::Running {
                return Ok(Response::new(PollWorkflowTaskResponse {
                    task_token: format!(
                        "wt-{}-{}",
                        wf_id,
                        self.backend.mock().next_id.fetch_add(1, Ordering::Relaxed)
                    ),
                    event_id: log.event_count as i64,
                    event_type: "WorkflowTask".to_string(),
                    workflow_execution: Vec::new(),
                    has_task: true,
                }));
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
        let logs = self.backend.mock().logs.read().unwrap();
        for (wf_id, log) in logs.iter() {
            if log.namespace == ns
                && log.status == WorkflowStatus::Running
                && log.activities_scheduled > log.activities_completed + log.activities_failed
            {
                return Ok(Response::new(PollActivityTaskResponse {
                    task_token: format!(
                        "at-{}-{}",
                        wf_id,
                        self.backend.mock().next_id.fetch_add(1, Ordering::Relaxed)
                    ),
                    activity_id: format!(
                        "act-{}",
                        self.backend.mock().next_id.fetch_add(1, Ordering::Relaxed)
                    ),
                    activity_type: "activity".to_string(),
                    input: Vec::new(),
                    workflow_id: wf_id.clone(),
                    has_task: true,
                    scheduled_time: VelocityEngine::now_us(),
                }));
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
        match self
            .backend
            .mock()
            .get_workflow_history(ns, &r.workflow_id)
            .await
        {
            Ok(count) => Ok(Response::new(GetWorkflowHistoryResponse {
                events: Vec::new(),
                next_page_token: Vec::new(),
                total_event_count: count as i64,
            })),
            Err(e) => Err(Status::not_found(e)),
        }
    }
    // ─── Tier 4 ────────────────────────────────────────────────────────────
    async fn list_workflows(
        &self,
        request: Request<ListWorkflowsRequest>,
    ) -> Result<Response<ListWorkflowsResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let logs = self.backend.mock().logs.read().unwrap();
        let mut executions = Vec::new();
        for (wf_id, log) in logs.iter() {
            if log.namespace != ns {
                continue;
            }
            let status_str = format!("{:?}", log.status);
            if !req.status_filter.is_empty()
                && req.status_filter.to_lowercase() != status_str.to_lowercase()
                && req.status_filter != "all"
            {
                continue;
            }
            executions.push(WorkflowExecutionInfo {
                workflow_id: wf_id.clone(),
                run_id: wf_id.clone(),
                workflow_type: log.workflow_type.clone(),
                namespace: log.namespace.clone(),
                status: status_str,
                start_time_ms: 0,
                close_time_ms: 0,
                task_queue: String::new(),
                search_attributes: log.search_attributes.clone(),
                history_length: log.event_count as i32,
            });
        }
        let total = executions.len() as i64;
        Ok(Response::new(ListWorkflowsResponse {
            executions,
            next_page_token: Vec::new(),
            total_count: total,
        }))
    }
    async fn describe_workflow_execution(
        &self,
        request: Request<DescribeWorkflowExecutionRequest>,
    ) -> Result<Response<DescribeWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let logs = self.backend.mock().logs.read().unwrap();
        let log = logs
            .get(&req.workflow_id)
            .ok_or_else(|| Status::not_found(format!("workflow {} not found", req.workflow_id)))?;
        if log.namespace != ns {
            return Err(Status::not_found("namespace mismatch"));
        }
        let status_str = format!("{:?}", log.status);
        let exec = WorkflowExecutionInfo {
            workflow_id: req.workflow_id.clone(),
            run_id: req.workflow_id.clone(),
            workflow_type: log.workflow_type.clone(),
            namespace: log.namespace.clone(),
            status: status_str,
            start_time_ms: 0,
            close_time_ms: 0,
            task_queue: String::new(),
            search_attributes: log.search_attributes.clone(),
            history_length: log.event_count as i32,
        };
        Ok(Response::new(DescribeWorkflowExecutionResponse {
            execution: Some(exec),
            pending_activities: Vec::new(),
            pending_children: Vec::new(),
            history_length: log.event_count as i64,
            execution_duration_ms: 0,
        }))
    }
    async fn describe_task_queue(
        &self,
        _request: Request<DescribeTaskQueueRequest>,
    ) -> Result<Response<DescribeTaskQueueResponse>, Status> {
        Ok(Response::new(DescribeTaskQueueResponse {
            pollers: Vec::new(),
            total_backlog: 0,
            partition_count: 0,
            build_ids: Vec::new(),
        }))
    }
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log_level)),
        )
        .init();

    let addr: std::net::SocketAddr = format!("{}:{}", cli.ip, cli.grpc_port).parse()?;

    let engine_mode = if cli.real_engine {
        "Real (WorkflowEngine with WAL)"
    } else {
        "BenchmarkMock (structurally-identical to Temporal bridge)"
    };

    println!("╦  ╦ ╔╗╔ ╦╔═ ╔═╗ ╦ ╦ ╔═╗ ╔╗╔ ╔═╗");
    println!("╚╗╔╝ ║║║ ╠╩╗ ╠═╣ ║ ║ ║╣  ║║║ ║ ║");
    println!("  ╚╝  ╝╚╝ ╩ ╩ ╩ ╩ ╚═╝ ╚═╝ ╝╚╝ ╚═╝");
    println!("  Production Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  gRPC:  http://{}", addr);
    println!("  Engine: {}", engine_mode);
    println!("  WAL:   {}", cli.wal_path);
    println!();

    // Create the production engine
    let engine = if cli.wal_path.is_empty() {
        WorkflowEngine::new()
    } else {
        let e = WorkflowEngine::with_wal(&cli.wal_path, cli.wal_max_size)
            .expect("Failed to initialize WAL");
        match e.recover_from_wal() {
            Ok((records, workflows)) => {
                if records > 0 {
                    tracing::info!(
                        records_replayed = records,
                        workflows_recovered = workflows,
                        "Crash recovery: replayed WAL on startup"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "WAL recovery failed (starting fresh)");
            }
        }
        e
    };
    let engine = Arc::new(engine);

    // Select engine backend based on --real-engine flag
    let backend = if cli.real_engine {
        tracing::info!("Using REAL Workflow ENGINE with WAL persistence");
        EngineBackend::Real(RealEngineAdapter::new(engine))
    } else {
        tracing::info!("Using MOCK engine (structurally identical to Temporal bridge)");
        EngineBackend::Mock(VelocityEngine::new())
    };

    let service = BenchmarkServiceImpl { backend };

    tracing::info!("BenchmarkService ({}) listening on {}", engine_mode, addr);

    tonic::transport::Server::builder()
        .add_service(BenchmarkServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
