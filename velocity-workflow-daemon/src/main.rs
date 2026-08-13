// Copyright (c) VELOCITY Suite. All rights reserved.
// Licensed under the MIT License.

//! VELOCITY-WorkFlow CLI — Professional workflow management tool.
//!
//! Provides commands for workflow lifecycle, namespace management, and server inspection.
//! Connects to the VELOCITY-WorkFlow engine via gRPC.

use clap::{Parser, Subcommand, ValueEnum};
use comfy_table::{presets::UTF8_FULL, Cell, Color, ContentArrangement, Table};
use crossterm::style::Stylize;
use serde::{Deserialize, Serialize};
use std::process::ExitCode;

// ─── CLI Argument Definitions ──────────────────────────────────────────────────

/// VELOCITY-WorkFlow CLI — Professional workflow management.
#[derive(Parser, Debug)]
#[command(name = "velocity", version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// gRPC server address.
    #[arg(
        long,
        default_value = "http://localhost:7234",
        env = "VELOCITY_SERVER",
        global = true
    )]
    server: String,

    /// Default namespace.
    #[arg(
        long,
        default_value = "default",
        env = "VELOCITY_NAMESPACE",
        global = true
    )]
    namespace: String,

    /// Output format.
    #[arg(long, default_value = "table", global = true)]
    output: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Workflow operations.
    Workflow {
        #[command(subcommand)]
        action: WorkflowAction,
    },
    /// Namespace operations.
    Namespace {
        #[command(subcommand)]
        action: NamespaceAction,
    },
    /// Server information and management.
    Server {
        #[command(subcommand)]
        action: ServerAction,
    },
    /// Task queue operations.
    Taskqueue {
        #[command(subcommand)]
        action: TaskQueueAction,
    },
    /// Cluster administration.
    Cluster {
        #[command(subcommand)]
        action: ClusterAction,
    },
    /// Search attribute management.
    SearchAttributes {
        #[command(subcommand)]
        action: SearchAttributesAction,
    },
    /// Schedule management.
    Schedule {
        #[command(subcommand)]
        action: ScheduleAction,
    },
    /// Batch operations.
    Batch {
        #[command(subcommand)]
        action: BatchAction,
    },
    /// Admin debug and maintenance commands.
    Admin {
        #[command(subcommand)]
        action: AdminAction,
    },
}

#[derive(Subcommand, Debug)]
enum WorkflowAction {
    /// Start a new workflow execution.
    Start {
        /// Workflow type name.
        #[arg(long)]
        r#type: String,
        /// Task queue name.
        #[arg(long)]
        task_queue: String,
        /// Input JSON payload.
        #[arg(long, default_value = "{}")]
        input: String,
        /// Workflow ID (auto-generated if not provided).
        #[arg(long)]
        workflow_id: Option<String>,
        /// Total steps for the workflow slab.
        #[arg(long, default_value = "1")]
        total_steps: u32,
    },
    /// List workflow executions.
    List {
        /// Filter by status.
        #[arg(long)]
        status: Option<StatusFilter>,
        /// Maximum number of results.
        #[arg(long, default_value = "20")]
        limit: i32,
    },
    /// Show workflow details.
    Describe {
        /// Workflow ID.
        #[arg(long)]
        workflow_id: String,
        /// Run ID (optional).
        #[arg(long)]
        run_id: Option<String>,
    },
    /// Signal a running workflow.
    Signal {
        /// Workflow ID.
        #[arg(long)]
        workflow_id: String,
        /// Signal name.
        #[arg(long)]
        name: String,
        /// Signal input JSON.
        #[arg(long, default_value = "{}")]
        input: String,
    },
    /// Query a running workflow.
    Query {
        /// Workflow ID.
        #[arg(long)]
        workflow_id: String,
        /// Query type name.
        #[arg(long)]
        name: String,
        /// Query args JSON.
        #[arg(long, default_value = "{}")]
        input: String,
    },
    /// Cancel a running workflow.
    Cancel {
        /// Workflow ID.
        #[arg(long)]
        workflow_id: String,
    },
    /// Show workflow event history.
    History {
        /// Workflow ID.
        #[arg(long)]
        workflow_id: String,
        /// Maximum events to return.
        #[arg(long, default_value = "100")]
        limit: i32,
    },
    /// Terminate a running workflow.
    Terminate {
        /// Workflow ID.
        #[arg(long)]
        workflow_id: String,
        /// Termination reason.
        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Reset a workflow to a previous event.
    Reset {
        /// Workflow ID.
        #[arg(long)]
        workflow_id: String,
        /// Event ID to reset to.
        #[arg(long)]
        event_id: i64,
        /// Reason for reset.
        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Delete a workflow execution.
    Delete {
        /// Workflow ID.
        #[arg(long)]
        workflow_id: String,
    },
    /// Count workflow executions.
    Count {
        /// Filter by status.
        #[arg(long)]
        status: Option<StatusFilter>,
        /// Query filter expression.
        #[arg(long, default_value = "")]
        query: String,
    },
    /// Show workflow stack trace via query.
    StackTrace {
        /// Workflow ID.
        #[arg(long)]
        workflow_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum NamespaceAction {
    /// Register a new namespace.
    Register {
        /// Namespace name.
        #[arg(long)]
        name: String,
        /// Description.
        #[arg(long, default_value = "")]
        description: String,
    },
    /// Describe a namespace.
    Describe {
        /// Namespace name.
        #[arg(long)]
        name: String,
    },
    /// List all namespaces.
    List,
    /// Update namespace configuration.
    Update {
        /// Namespace name.
        #[arg(long)]
        name: String,
        /// New description.
        #[arg(long)]
        description: Option<String>,
        /// New owner email.
        #[arg(long)]
        owner_email: Option<String>,
        /// Retention period in days.
        #[arg(long)]
        retention_days: Option<u32>,
    },
    /// Delete a namespace.
    Delete {
        /// Namespace name.
        #[arg(long)]
        name: String,
    },
    /// Deprecate a namespace (no new workflows).
    Deprecate {
        /// Namespace name.
        #[arg(long)]
        name: String,
    },
    /// Failover namespace to another cluster.
    Failover {
        /// Namespace name.
        #[arg(long)]
        name: String,
        /// Target cluster name.
        #[arg(long)]
        target_cluster: String,
        /// Force failover.
        #[arg(long, default_value_t = false)]
        force: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ServerAction {
    /// Get server information.
    Info,
    /// Check server health.
    Health,
    /// Show server metrics in Prometheus format.
    Metrics,
    /// Show server stats.
    Stats,
    /// Show server version.
    Version,
}

#[derive(Subcommand, Debug)]
enum TaskQueueAction {
    /// Describe a task queue.
    Describe {
        /// Task queue name.
        #[arg(long)]
        name: String,
    },
    /// List all task queues.
    List,
    /// List task queue partitions.
    ListPartitions {
        /// Task queue name.
        #[arg(long)]
        name: String,
    },
    /// Show task queue pollers.
    Pollers {
        /// Task queue name.
        #[arg(long)]
        name: String,
    },
}

#[derive(Subcommand, Debug)]
enum ClusterAction {
    /// Describe the local cluster.
    Describe,
    /// List all known clusters.
    List,
    /// Add or update a remote cluster connection.
    AddRemote {
        /// Cluster name.
        #[arg(long)]
        name: String,
        /// Frontend address.
        #[arg(long)]
        address: String,
    },
    /// Remove a remote cluster.
    RemoveRemote {
        /// Cluster name.
        #[arg(long)]
        name: String,
    },
    /// Show replication status.
    ReplicationStatus,
}

#[derive(Subcommand, Debug)]
enum SearchAttributesAction {
    /// List all search attributes.
    List,
    /// Add a new search attribute.
    Add {
        /// Attribute name.
        #[arg(long)]
        name: String,
        /// Attribute type (Text, Keyword, Int, Double, Bool, Datetime).
        #[arg(long)]
        attr_type: String,
    },
    /// Remove a search attribute.
    Remove {
        /// Attribute name.
        #[arg(long)]
        name: String,
    },
}

#[derive(Subcommand, Debug)]
enum ScheduleAction {
    /// Create a new schedule.
    Create {
        /// Schedule ID.
        #[arg(long)]
        schedule_id: String,
        /// Cron expression.
        #[arg(long)]
        cron: String,
        /// Workflow type.
        #[arg(long)]
        workflow_type: String,
        /// Task queue.
        #[arg(long)]
        task_queue: String,
        /// Input JSON.
        #[arg(long, default_value = "{}")]
        input: String,
    },
    /// Describe a schedule.
    Describe {
        /// Schedule ID.
        #[arg(long)]
        schedule_id: String,
    },
    /// List all schedules.
    List,
    /// Update a schedule.
    Update {
        /// Schedule ID.
        #[arg(long)]
        schedule_id: String,
        /// New cron expression.
        #[arg(long)]
        cron: Option<String>,
    },
    /// Delete a schedule.
    Delete {
        /// Schedule ID.
        #[arg(long)]
        schedule_id: String,
    },
    /// Trigger a schedule immediately.
    Trigger {
        /// Schedule ID.
        #[arg(long)]
        schedule_id: String,
    },
    /// Pause a schedule.
    Pause {
        /// Schedule ID.
        #[arg(long)]
        schedule_id: String,
        /// Reason for pause.
        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Unpause a schedule.
    Unpause {
        /// Schedule ID.
        #[arg(long)]
        schedule_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum BatchAction {
    /// List batch operations.
    List {
        /// Maximum results.
        #[arg(long, default_value = "20")]
        limit: i32,
    },
    /// Describe a batch operation.
    Describe {
        /// Batch job ID.
        #[arg(long)]
        job_id: String,
    },
    /// Terminate a batch operation.
    Terminate {
        /// Batch job ID.
        #[arg(long)]
        job_id: String,
        /// Reason.
        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Start a batch workflow termination.
    TerminateWorkflows {
        /// Query filter for workflows to terminate.
        #[arg(long)]
        query: String,
        /// Reason.
        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Start a batch workflow cancellation.
    CancelWorkflows {
        /// Query filter for workflows to cancel.
        #[arg(long)]
        query: String,
        /// Reason.
        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Start a batch workflow signal.
    SignalWorkflows {
        /// Query filter for workflows to signal.
        #[arg(long)]
        query: String,
        /// Signal name.
        #[arg(long)]
        signal: String,
        /// Signal input JSON.
        #[arg(long, default_value = "{}")]
        input: String,
    },
}

#[derive(Subcommand, Debug)]
enum AdminAction {
    /// Describe a shard.
    ShardDescribe {
        /// Shard ID.
        #[arg(long)]
        shard_id: i32,
    },
    /// Close a shard.
    ShardClose {
        /// Shard ID.
        #[arg(long)]
        shard_id: i32,
    },
    /// List DLQ messages.
    DlqList {
        /// Source cluster.
        #[arg(long)]
        cluster: String,
        /// Shard ID.
        #[arg(long)]
        shard_id: i32,
        /// Max messages.
        #[arg(long, default_value = "100")]
        limit: i32,
    },
    /// Purge DLQ messages.
    DlqPurge {
        /// Source cluster.
        #[arg(long)]
        cluster: String,
        /// Shard ID.
        #[arg(long)]
        shard_id: i32,
    },
    /// Merge DLQ messages back.
    DlqMerge {
        /// Source cluster.
        #[arg(long)]
        cluster: String,
        /// Shard ID.
        #[arg(long)]
        shard_id: i32,
    },
    /// Describe a history host.
    HistoryHostDescribe {
        /// Host address.
        #[arg(long)]
        address: String,
    },
    /// List database tables.
    DbListTables,
    /// Get workflow raw history.
    WorkflowRawHistory {
        /// Workflow ID.
        #[arg(long)]
        workflow_id: String,
        /// Run ID.
        #[arg(long)]
        run_id: String,
    },
}

#[derive(ValueEnum, Clone, Debug, Default)]
enum OutputFormat {
    #[default]
    Table,
    Json,
}

#[derive(ValueEnum, Clone, Debug)]
enum StatusFilter {
    Running,
    Completed,
    Failed,
    Canceled,
    Terminated,
}

// ─── gRPC Client Stub ──────────────────────────────────────────────────────────
// In production, these would be generated from proto files via tonic-build.
// For now, we use a simple HTTP-based approach.

struct VelocityClient {
    server: String,
    client: reqwest::Client,
}

#[derive(Debug, Serialize)]
struct StartWorkflowRequest {
    namespace: String,
    workflow_id: String,
    workflow_type: String,
    task_queue: String,
    input: serde_json::Value,
    total_steps: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct StartWorkflowResponse {
    workflow_id: String,
    run_id: String,
    workflow_key: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkflowInfo {
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    status: String,
    start_time: String,
    close_time: Option<String>,
    history_length: i64,
    namespace: String,
    task_queue: String,
}

#[derive(Debug, Deserialize)]
struct ListWorkflowsResponse {
    workflows: Vec<WorkflowInfo>,
    total_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkflowDetail {
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    status: String,
    start_time: String,
    close_time: Option<String>,
    history_length: i64,
    namespace: String,
    task_queue: String,
    total_steps: u32,
    completed_steps: u32,
    merkle_root: String,
    pending_activities: Vec<PendingActivity>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PendingActivity {
    activity_id: String,
    activity_type: String,
    state: String,
    attempt: i32,
    max_attempts: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct HistoryEvent {
    event_id: u64,
    event_time: String,
    event_type: String,
    details: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct HistoryResponse {
    events: Vec<HistoryEvent>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NamespaceInfo {
    name: String,
    namespace_id: u64,
    description: String,
    is_active: bool,
    retention_secs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ServerInfoResponse {
    server_version: String,
    supported_features: Vec<String>,
    workflow_count: u64,
    namespace_count: u64,
    uptime_secs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct TaskQueueInfo {
    name: String,
    pending_tasks: u64,
    active_workers: u64,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PollerInfo {
    identity: String,
    last_poll: String,
    tasks_polled: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterInfo {
    name: String,
    address: String,
    is_active: bool,
    replication_status: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SearchAttributeDef {
    name: String,
    attr_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ScheduleInfo {
    schedule_id: String,
    cron: String,
    workflow_type: String,
    task_queue: String,
    state: String,
    next_run: Option<String>,
    last_run: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BatchJobInfo {
    job_id: String,
    operation: String,
    state: String,
    total_count: u64,
    completed_count: u64,
    start_time: String,
}

impl VelocityClient {
    fn new(server: &str) -> Self {
        Self {
            server: server.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    async fn start_workflow(
        &self,
        req: StartWorkflowRequest,
    ) -> Result<StartWorkflowResponse, String> {
        let resp = self
            .client
            .post(format!("{}/api/workflows", self.server))
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;

        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("Parse error: {}", e))
        } else {
            let err: ErrorResponse = resp.json().await.unwrap_or(ErrorResponse {
                error: "Unknown error".into(),
            });
            Err(err.error)
        }
    }

    async fn list_workflows(
        &self,
        namespace: &str,
        status: Option<&str>,
        limit: i32,
    ) -> Result<ListWorkflowsResponse, String> {
        let mut url = format!(
            "{}/api/workflows?namespace={}&limit={}",
            self.server, namespace, limit
        );
        if let Some(s) = status {
            url.push_str(&format!("&status={}", s));
        }
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("Parse error: {}", e))
        } else {
            Err("Failed to list workflows".into())
        }
    }

    async fn describe_workflow(&self, workflow_id: &str) -> Result<WorkflowDetail, String> {
        let resp = self
            .client
            .get(format!("{}/api/workflows/{}", self.server, workflow_id))
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("Parse error: {}", e))
        } else {
            Err(format!("Workflow '{}' not found", workflow_id))
        }
    }

    async fn signal_workflow(
        &self,
        workflow_id: &str,
        signal_name: &str,
        input: serde_json::Value,
    ) -> Result<(), String> {
        let body = serde_json::json!({ "signal_name": signal_name, "input": input });
        let resp = self
            .client
            .post(format!(
                "{}/api/workflows/{}/signal",
                self.server, workflow_id
            ))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to signal workflow".into())
        }
    }

    async fn query_workflow(
        &self,
        workflow_id: &str,
        query_name: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let body = serde_json::json!({ "query_type": query_name, "input": input });
        let resp = self
            .client
            .post(format!(
                "{}/api/workflows/{}/query",
                self.server, workflow_id
            ))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("Parse error: {}", e))
        } else {
            Err("Failed to query workflow".into())
        }
    }

    async fn cancel_workflow(&self, workflow_id: &str) -> Result<(), String> {
        let resp = self
            .client
            .post(format!(
                "{}/api/workflows/{}/cancel",
                self.server, workflow_id
            ))
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to cancel workflow".into())
        }
    }

    async fn get_history(&self, workflow_id: &str, limit: i32) -> Result<HistoryResponse, String> {
        let resp = self
            .client
            .get(format!(
                "{}/api/workflows/{}/history?limit={}",
                self.server, workflow_id, limit
            ))
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("Parse error: {}", e))
        } else {
            Err(format!("Failed to get history for '{}'", workflow_id))
        }
    }

    async fn register_namespace(&self, name: &str, description: &str) -> Result<u64, String> {
        let body = serde_json::json!({ "name": name, "description": description });
        let resp = self
            .client
            .post(format!("{}/api/namespaces", self.server))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            let result: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| format!("Parse error: {}", e))?;
            Ok(result["namespace_id"].as_u64().unwrap_or(0))
        } else {
            Err("Failed to register namespace".into())
        }
    }

    async fn describe_namespace(&self, name: &str) -> Result<NamespaceInfo, String> {
        let resp = self
            .client
            .get(format!("{}/api/namespaces/{}", self.server, name))
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("Parse error: {}", e))
        } else {
            Err(format!("Namespace '{}' not found", name))
        }
    }

    async fn list_namespaces(&self) -> Result<Vec<NamespaceInfo>, String> {
        let resp = self
            .client
            .get(format!("{}/api/namespaces", self.server))
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("Parse error: {}", e))
        } else {
            Err("Failed to list namespaces".into())
        }
    }

    async fn get_server_info(&self) -> Result<ServerInfoResponse, String> {
        let resp = self
            .client
            .get(format!("{}/api/server/info", self.server))
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("Parse error: {}", e))
        } else {
            Err("Failed to get server info".into())
        }
    }

    async fn describe_task_queue(&self, name: &str) -> Result<TaskQueueInfo, String> {
        let resp = self
            .client
            .get(format!("{}/api/taskqueues/{}", self.server, name))
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("Parse error: {}", e))
        } else {
            Err(format!("Task queue '{}' not found", name))
        }
    }

    // ── New client methods for expanded CLI ──────────────────────────────────

    async fn terminate_workflow(&self, workflow_id: &str, reason: &str) -> Result<(), String> {
        let body = serde_json::json!({ "reason": reason });
        let resp = self
            .client
            .post(format!(
                "{}/api/workflows/{}/terminate",
                self.server, workflow_id
            ))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to terminate workflow".into())
        }
    }

    async fn reset_workflow(
        &self,
        workflow_id: &str,
        event_id: i64,
        reason: &str,
    ) -> Result<(), String> {
        let body = serde_json::json!({ "event_id": event_id, "reason": reason });
        let resp = self
            .client
            .post(format!(
                "{}/api/workflows/{}/reset",
                self.server, workflow_id
            ))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to reset workflow".into())
        }
    }

    async fn delete_workflow(&self, workflow_id: &str) -> Result<(), String> {
        let resp = self
            .client
            .delete(format!("{}/api/workflows/{}", self.server, workflow_id))
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to delete workflow".into())
        }
    }

    async fn count_workflows(
        &self,
        namespace: &str,
        status: Option<&str>,
        query: &str,
    ) -> Result<u64, String> {
        let mut url = format!(
            "{}/api/workflows/count?namespace={}",
            self.server, namespace
        );
        if let Some(s) = status {
            url.push_str(&format!("&status={}", s));
        }
        if !query.is_empty() {
            url.push_str(&format!("&query={}", query));
        }
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Connection error: {}", e))?;
        if resp.status().is_success() {
            let v: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| format!("Parse error: {}", e))?;
            Ok(v["count"].as_u64().unwrap_or(0))
        } else {
            Err("Failed to count workflows".into())
        }
    }

    async fn check_health(&self) -> Result<bool, String> {
        let resp = self
            .client
            .get(format!("{}/health", self.server))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        Ok(resp.status().is_success())
    }

    async fn get_metrics(&self) -> Result<String, String> {
        let resp = self
            .client
            .get(format!("{}/metrics", self.server))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        resp.text().await.map_err(|e| format!("Parse error: {}", e))
    }

    async fn update_namespace(
        &self,
        name: &str,
        description: Option<&str>,
        owner_email: Option<&str>,
        retention_days: Option<u32>,
    ) -> Result<(), String> {
        let mut body = serde_json::Map::new();
        if let Some(d) = description {
            body.insert("description".into(), serde_json::json!(d));
        }
        if let Some(o) = owner_email {
            body.insert("owner_email".into(), serde_json::json!(o));
        }
        if let Some(r) = retention_days {
            body.insert("retention_days".into(), serde_json::json!(r));
        }
        let resp = self
            .client
            .put(format!("{}/api/namespaces/{}", self.server, name))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to update namespace".into())
        }
    }

    async fn delete_namespace(&self, name: &str) -> Result<(), String> {
        let resp = self
            .client
            .delete(format!("{}/api/namespaces/{}", self.server, name))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to delete namespace".into())
        }
    }

    async fn deprecate_namespace(&self, name: &str) -> Result<(), String> {
        let resp = self
            .client
            .post(format!("{}/api/namespaces/{}/deprecate", self.server, name))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to deprecate namespace".into())
        }
    }

    async fn failover_namespace(
        &self,
        name: &str,
        target_cluster: &str,
        force: bool,
    ) -> Result<(), String> {
        let body = serde_json::json!({ "target_cluster": target_cluster, "force": force });
        let resp = self
            .client
            .post(format!("{}/api/namespaces/{}/failover", self.server, name))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to failover namespace".into())
        }
    }

    async fn list_task_queues(&self) -> Result<Vec<TaskQueueInfo>, String> {
        let resp = self
            .client
            .get(format!("{}/api/taskqueues", self.server))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to list task queues".into())
        }
    }

    async fn list_task_queue_partitions(&self, name: &str) -> Result<Vec<String>, String> {
        let resp = self
            .client
            .get(format!(
                "{}/api/taskqueues/{}/partitions",
                self.server, name
            ))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to list partitions".into())
        }
    }

    async fn get_task_queue_pollers(&self, name: &str) -> Result<Vec<PollerInfo>, String> {
        let resp = self
            .client
            .get(format!("{}/api/taskqueues/{}/pollers", self.server, name))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to get pollers".into())
        }
    }

    async fn describe_cluster(&self) -> Result<ClusterInfo, String> {
        let resp = self
            .client
            .get(format!("{}/api/cluster/describe", self.server))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to describe cluster".into())
        }
    }

    async fn list_clusters(&self) -> Result<Vec<ClusterInfo>, String> {
        let resp = self
            .client
            .get(format!("{}/api/clusters", self.server))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to list clusters".into())
        }
    }

    async fn add_remote_cluster(&self, name: &str, address: &str) -> Result<(), String> {
        let body = serde_json::json!({ "name": name, "address": address });
        let resp = self
            .client
            .post(format!("{}/api/clusters", self.server))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to add cluster".into())
        }
    }

    async fn remove_remote_cluster(&self, name: &str) -> Result<(), String> {
        let resp = self
            .client
            .delete(format!("{}/api/clusters/{}", self.server, name))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to remove cluster".into())
        }
    }

    async fn get_replication_status(&self) -> Result<serde_json::Value, String> {
        let resp = self
            .client
            .get(format!("{}/api/replication/status", self.server))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to get replication status".into())
        }
    }

    async fn list_search_attributes(&self) -> Result<Vec<SearchAttributeDef>, String> {
        let resp = self
            .client
            .get(format!("{}/api/search-attributes", self.server))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to list search attributes".into())
        }
    }

    async fn add_search_attribute(&self, name: &str, attr_type: &str) -> Result<(), String> {
        let body = serde_json::json!({ "name": name, "type": attr_type });
        let resp = self
            .client
            .post(format!("{}/api/search-attributes", self.server))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to add search attribute".into())
        }
    }

    async fn remove_search_attribute(&self, name: &str) -> Result<(), String> {
        let resp = self
            .client
            .delete(format!("{}/api/search-attributes/{}", self.server, name))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to remove search attribute".into())
        }
    }

    async fn list_schedules(&self) -> Result<Vec<ScheduleInfo>, String> {
        let resp = self
            .client
            .get(format!("{}/api/schedules", self.server))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to list schedules".into())
        }
    }

    async fn describe_schedule(&self, schedule_id: &str) -> Result<ScheduleInfo, String> {
        let resp = self
            .client
            .get(format!("{}/api/schedules/{}", self.server, schedule_id))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err(format!("Schedule '{}' not found", schedule_id))
        }
    }

    async fn create_schedule(
        &self,
        schedule_id: &str,
        cron: &str,
        workflow_type: &str,
        task_queue: &str,
        input: &str,
    ) -> Result<(), String> {
        let body = serde_json::json!({ "schedule_id": schedule_id, "cron": cron, "workflow_type": workflow_type, "task_queue": task_queue, "input": input });
        let resp = self
            .client
            .post(format!("{}/api/schedules", self.server))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to create schedule".into())
        }
    }

    async fn delete_schedule(&self, schedule_id: &str) -> Result<(), String> {
        let resp = self
            .client
            .delete(format!("{}/api/schedules/{}", self.server, schedule_id))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to delete schedule".into())
        }
    }

    async fn trigger_schedule(&self, schedule_id: &str) -> Result<(), String> {
        let resp = self
            .client
            .post(format!(
                "{}/api/schedules/{}/trigger",
                self.server, schedule_id
            ))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to trigger schedule".into())
        }
    }

    async fn pause_schedule(&self, schedule_id: &str, reason: &str) -> Result<(), String> {
        let body = serde_json::json!({ "reason": reason });
        let resp = self
            .client
            .post(format!(
                "{}/api/schedules/{}/pause",
                self.server, schedule_id
            ))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to pause schedule".into())
        }
    }

    async fn unpause_schedule(&self, schedule_id: &str) -> Result<(), String> {
        let resp = self
            .client
            .post(format!(
                "{}/api/schedules/{}/unpause",
                self.server, schedule_id
            ))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to unpause schedule".into())
        }
    }

    async fn list_batch_jobs(&self, limit: i32) -> Result<Vec<BatchJobInfo>, String> {
        let resp = self
            .client
            .get(format!("{}/api/batch?limit={}", self.server, limit))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to list batch jobs".into())
        }
    }

    async fn describe_batch_job(&self, job_id: &str) -> Result<BatchJobInfo, String> {
        let resp = self
            .client
            .get(format!("{}/api/batch/{}", self.server, job_id))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err(format!("Batch job '{}' not found", job_id))
        }
    }

    async fn terminate_batch_job(&self, job_id: &str, reason: &str) -> Result<(), String> {
        let body = serde_json::json!({ "reason": reason });
        let resp = self
            .client
            .post(format!("{}/api/batch/{}/terminate", self.server, job_id))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to terminate batch job".into())
        }
    }

    async fn start_batch_terminate(&self, query: &str, reason: &str) -> Result<String, String> {
        let body =
            serde_json::json!({ "operation": "terminate", "query": query, "reason": reason });
        let resp = self
            .client
            .post(format!("{}/api/batch", self.server))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            let v: serde_json::Value = resp.json().await.map_err(|e| format!("{}", e))?;
            Ok(v["job_id"].as_str().unwrap_or("unknown").to_string())
        } else {
            Err("Failed to start batch terminate".into())
        }
    }

    async fn start_batch_cancel(&self, query: &str, reason: &str) -> Result<String, String> {
        let body = serde_json::json!({ "operation": "cancel", "query": query, "reason": reason });
        let resp = self
            .client
            .post(format!("{}/api/batch", self.server))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            let v: serde_json::Value = resp.json().await.map_err(|e| format!("{}", e))?;
            Ok(v["job_id"].as_str().unwrap_or("unknown").to_string())
        } else {
            Err("Failed to start batch cancel".into())
        }
    }

    async fn start_batch_signal(
        &self,
        query: &str,
        signal: &str,
        input: &str,
    ) -> Result<String, String> {
        let body = serde_json::json!({ "operation": "signal", "query": query, "signal": signal, "input": input });
        let resp = self
            .client
            .post(format!("{}/api/batch", self.server))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            let v: serde_json::Value = resp.json().await.map_err(|e| format!("{}", e))?;
            Ok(v["job_id"].as_str().unwrap_or("unknown").to_string())
        } else {
            Err("Failed to start batch signal".into())
        }
    }

    async fn admin_describe_shard(&self, shard_id: i32) -> Result<serde_json::Value, String> {
        let resp = self
            .client
            .get(format!("{}/api/admin/shards/{}", self.server, shard_id))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err(format!("Shard {} not found", shard_id))
        }
    }

    async fn admin_close_shard(&self, shard_id: i32) -> Result<(), String> {
        let resp = self
            .client
            .post(format!(
                "{}/api/admin/shards/{}/close",
                self.server, shard_id
            ))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to close shard".into())
        }
    }

    async fn admin_dlq_list(
        &self,
        cluster: &str,
        shard_id: i32,
        limit: i32,
    ) -> Result<serde_json::Value, String> {
        let resp = self
            .client
            .get(format!(
                "{}/api/admin/dlq?cluster={}&shard_id={}&limit={}",
                self.server, cluster, shard_id, limit
            ))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to list DLQ".into())
        }
    }

    async fn admin_dlq_purge(&self, cluster: &str, shard_id: i32) -> Result<(), String> {
        let body = serde_json::json!({ "cluster": cluster, "shard_id": shard_id });
        let resp = self
            .client
            .post(format!("{}/api/admin/dlq/purge", self.server))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to purge DLQ".into())
        }
    }

    async fn admin_dlq_merge(&self, cluster: &str, shard_id: i32) -> Result<(), String> {
        let body = serde_json::json!({ "cluster": cluster, "shard_id": shard_id });
        let resp = self
            .client
            .post(format!("{}/api/admin/dlq/merge", self.server))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err("Failed to merge DLQ".into())
        }
    }

    async fn admin_history_host_describe(
        &self,
        address: &str,
    ) -> Result<serde_json::Value, String> {
        let resp = self
            .client
            .get(format!(
                "{}/api/admin/history-host?address={}",
                self.server, address
            ))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to describe history host".into())
        }
    }

    async fn admin_db_list_tables(&self) -> Result<Vec<String>, String> {
        let resp = self
            .client
            .get(format!("{}/api/admin/db/tables", self.server))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to list DB tables".into())
        }
    }

    async fn admin_workflow_raw_history(
        &self,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<serde_json::Value, String> {
        let resp = self
            .client
            .get(format!(
                "{}/api/admin/workflows/{}/raw-history?run_id={}",
                self.server, workflow_id, run_id
            ))
            .send()
            .await
            .map_err(|e| format!("{}", e))?;
        if resp.status().is_success() {
            resp.json().await.map_err(|e| format!("{}", e))
        } else {
            Err("Failed to get raw history".into())
        }
    }
}

// ─── Output Formatting ─────────────────────────────────────────────────────────

fn status_color(status: &str) -> Cell {
    let cell = Cell::new(status);
    match status.to_lowercase().as_str() {
        "running" => cell.fg(Color::Green),
        "completed" => cell.fg(Color::Blue),
        "failed" => cell.fg(Color::Red),
        "canceled" => cell.fg(Color::Yellow),
        "terminated" => cell.fg(Color::Magenta),
        _ => cell,
    }
}

fn print_workflows_table(workflows: &[WorkflowInfo]) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);

    table.set_header(vec!["WORKFLOW ID", "TYPE", "STATUS", "STARTED", "HISTORY"]);

    for wf in workflows {
        table.add_row(vec![
            Cell::new(&wf.workflow_id),
            Cell::new(&wf.workflow_type),
            status_color(&wf.status),
            Cell::new(&wf.start_time),
            Cell::new(wf.history_length),
        ]);
    }

    println!("\n{table}\n");
}

fn print_workflows_json(workflows: &[WorkflowInfo]) {
    println!("{}", serde_json::to_string_pretty(workflows).unwrap());
}

fn print_history_table(events: &[HistoryEvent]) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);

    table.set_header(vec!["ID", "TIME", "TYPE", "DETAILS"]);

    for evt in events {
        let details = evt.details.as_deref().unwrap_or("-");
        let truncated = if details.len() > 50 {
            &details[..50]
        } else {
            details
        };
        table.add_row(vec![
            Cell::new(evt.event_id),
            Cell::new(&evt.event_time),
            Cell::new(&evt.event_type).fg(Color::Cyan),
            Cell::new(truncated),
        ]);
    }

    println!("\n{table}\n");
}

fn print_namespaces_table(namespaces: &[NamespaceInfo]) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);

    table.set_header(vec!["NAME", "ID", "ACTIVE", "RETENTION", "DESCRIPTION"]);

    for ns in namespaces {
        let active = if ns.is_active {
            "Yes".green()
        } else {
            "No".red()
        };
        let retention = format!("{}s", ns.retention_secs);
        table.add_row(vec![
            Cell::new(&ns.name).fg(Color::Green),
            Cell::new(ns.namespace_id),
            Cell::new(active),
            Cell::new(retention),
            Cell::new(&ns.description),
        ]);
    }

    println!("\n{table}\n");
}

// ─── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let client = VelocityClient::new(&cli.server);

    let result = match cli.command {
        Commands::Workflow { action } => {
            handle_workflow(action, &client, &cli.namespace, &cli.output).await
        }
        Commands::Namespace { action } => handle_namespace(action, &client, &cli.output).await,
        Commands::Server { action } => handle_server(action, &client, &cli.output).await,
        Commands::Taskqueue { action } => handle_taskqueue(action, &client, &cli.output).await,
        Commands::Cluster { action } => handle_cluster(action, &client, &cli.output).await,
        Commands::SearchAttributes { action } => {
            handle_search_attributes(action, &client, &cli.output).await
        }
        Commands::Schedule { action } => handle_schedule(action, &client, &cli.output).await,
        Commands::Batch { action } => handle_batch(action, &client, &cli.output).await,
        Commands::Admin { action } => handle_admin(action, &client, &cli.output).await,
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{} {}", "Error:".red().bold(), e);
            ExitCode::FAILURE
        }
    }
}

async fn handle_workflow(
    action: WorkflowAction,
    client: &VelocityClient,
    namespace: &str,
    output: &OutputFormat,
) -> Result<(), String> {
    match action {
        WorkflowAction::Start {
            r#type,
            task_queue,
            input,
            workflow_id,
            total_steps,
        } => {
            let wf_id = workflow_id.unwrap_or_else(|| format!("wf-{}", uuid_simple()));
            let input_json: serde_json::Value =
                serde_json::from_str(&input).map_err(|e| format!("Invalid JSON input: {}", e))?;

            let req = StartWorkflowRequest {
                namespace: namespace.to_string(),
                workflow_id: wf_id.clone(),
                workflow_type: r#type,
                task_queue,
                input: input_json,
                total_steps,
            };

            let resp = client.start_workflow(req).await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp).unwrap()),
                OutputFormat::Table => {
                    println!("\n{} Workflow started successfully", "✓".green().bold());
                    println!("  Workflow ID: {}", resp.workflow_id.bold());
                    println!("  Run ID:      {}", resp.run_id);
                    println!("  Key:         {}", resp.workflow_key);
                    println!();
                }
            }
        }

        WorkflowAction::List { status, limit } => {
            let status_str = status.map(|s| format!("{:?}", s).to_lowercase());
            let resp = client
                .list_workflows(namespace, status_str.as_deref(), limit)
                .await?;

            match output {
                OutputFormat::Json => print_workflows_json(&resp.workflows),
                OutputFormat::Table => {
                    println!(
                        "\n{} workflows (namespace: {})",
                        resp.total_count,
                        namespace.bold()
                    );
                    print_workflows_table(&resp.workflows);
                }
            }
        }

        WorkflowAction::Describe {
            workflow_id,
            run_id: _,
        } => {
            let detail = client.describe_workflow(&workflow_id).await?;

            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&detail).unwrap())
                }
                OutputFormat::Table => {
                    println!("\n{} Workflow Details", "⟡".cyan().bold());
                    println!("  ┌─────────────────────────────────────────────────────────────");
                    println!("  │ Workflow ID:  {}", detail.workflow_id.bold());
                    println!("  │ Run ID:       {}", detail.run_id);
                    println!("  │ Type:         {}", detail.workflow_type);
                    println!("  │ Status:       {}", format_status(&detail.status));
                    println!("  │ Namespace:    {}", detail.namespace);
                    println!("  │ Task Queue:   {}", detail.task_queue);
                    println!("  │ Started:      {}", detail.start_time);
                    if let Some(ct) = &detail.close_time {
                        println!("  │ Closed:       {}", ct);
                    }
                    println!(
                        "  │ Steps:        {}/{} completed",
                        detail.completed_steps, detail.total_steps
                    );
                    println!("  │ Merkle Root:  {}", detail.merkle_root);
                    println!("  │ History:      {} events", detail.history_length);
                    println!("  └─────────────────────────────────────────────────────────────");

                    if !detail.pending_activities.is_empty() {
                        println!("\n  {} Pending Activities:", "⚡".yellow());
                        let mut table = Table::new();
                        table.set_header(vec!["ACTIVITY", "TYPE", "STATE", "ATTEMPT"]);
                        for act in &detail.pending_activities {
                            table.add_row(vec![
                                Cell::new(&act.activity_id),
                                Cell::new(&act.activity_type),
                                status_color(&act.state),
                                Cell::new(format!("{}/{}", act.attempt, act.max_attempts)),
                            ]);
                        }
                        println!("{table}");
                    }
                    println!();
                }
            }
        }

        WorkflowAction::Signal {
            workflow_id,
            name,
            input,
        } => {
            let input_json: serde_json::Value =
                serde_json::from_str(&input).map_err(|e| format!("Invalid JSON input: {}", e))?;
            client
                .signal_workflow(&workflow_id, &name, input_json)
                .await?;
            println!(
                "{} Signal '{}' sent to workflow '{}'",
                "✓".green().bold(),
                name.bold(),
                workflow_id
            );
        }

        WorkflowAction::Query {
            workflow_id,
            name,
            input,
        } => {
            let input_json: serde_json::Value =
                serde_json::from_str(&input).map_err(|e| format!("Invalid JSON input: {}", e))?;
            let result = client
                .query_workflow(&workflow_id, &name, input_json)
                .await?;

            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&result).unwrap())
                }
                OutputFormat::Table => {
                    println!("\n{} Query Result for '{}':", "⟡".cyan(), name.bold());
                    println!("{}\n", serde_json::to_string_pretty(&result).unwrap());
                }
            }
        }

        WorkflowAction::Cancel { workflow_id } => {
            client.cancel_workflow(&workflow_id).await?;
            println!(
                "{} Workflow '{}' cancellation requested",
                "✓".green().bold(),
                workflow_id.bold()
            );
        }

        WorkflowAction::History { workflow_id, limit } => {
            let resp = client.get_history(&workflow_id, limit).await?;

            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&resp.events).unwrap())
                }
                OutputFormat::Table => {
                    println!(
                        "\n{} Event History for '{}':",
                        "⟡".cyan(),
                        workflow_id.bold()
                    );
                    print_history_table(&resp.events);
                }
            }
        }

        WorkflowAction::Terminate {
            workflow_id,
            reason,
        } => {
            client.terminate_workflow(&workflow_id, &reason).await?;
            println!(
                "{} Workflow '{}' terminated",
                "✓".green().bold(),
                workflow_id.bold()
            );
        }

        WorkflowAction::Reset {
            workflow_id,
            event_id,
            reason,
        } => {
            client
                .reset_workflow(&workflow_id, event_id, &reason)
                .await?;
            println!(
                "{} Workflow '{}' reset to event {}",
                "✓".green().bold(),
                workflow_id.bold(),
                event_id
            );
        }

        WorkflowAction::Delete { workflow_id } => {
            client.delete_workflow(&workflow_id).await?;
            println!(
                "{} Workflow '{}' deleted",
                "✓".green().bold(),
                workflow_id.bold()
            );
        }

        WorkflowAction::Count { status, query } => {
            let status_str = status.map(|s| format!("{:?}", s).to_lowercase());
            let count = client
                .count_workflows(namespace, status_str.as_deref(), &query)
                .await?;
            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::json!({ "count": count }))
                }
                OutputFormat::Table => {
                    println!("\n{} workflows matching query", count.to_string().bold());
                }
            }
        }

        WorkflowAction::StackTrace { workflow_id } => {
            let result = client
                .query_workflow(&workflow_id, "__stack_trace", serde_json::json!({}))
                .await?;
            println!("\n{} Stack Trace for '{}':", "⟡".cyan(), workflow_id.bold());
            println!("{}\n", serde_json::to_string_pretty(&result).unwrap());
        }
    }
    Ok(())
}

async fn handle_namespace(
    action: NamespaceAction,
    client: &VelocityClient,
    output: &OutputFormat,
) -> Result<(), String> {
    match action {
        NamespaceAction::Register { name, description } => {
            let id = client.register_namespace(&name, &description).await?;
            match output {
                OutputFormat::Json => println!(
                    "{}",
                    serde_json::json!({ "name": name, "namespace_id": id })
                ),
                OutputFormat::Table => {
                    println!(
                        "\n{} Namespace '{}' registered (ID: {})",
                        "✓".green().bold(),
                        name.bold(),
                        id
                    );
                }
            }
        }

        NamespaceAction::Describe { name } => {
            let info = client.describe_namespace(&name).await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&info).unwrap()),
                OutputFormat::Table => {
                    println!("\n{} Namespace Details", "⟡".cyan().bold());
                    println!("  Name:        {}", info.name.bold());
                    println!("  ID:          {}", info.namespace_id);
                    println!(
                        "  Active:      {}",
                        if info.is_active {
                            "Yes".green()
                        } else {
                            "No".red()
                        }
                    );
                    println!("  Retention:   {}s", info.retention_secs);
                    println!(
                        "  Description: {}",
                        if info.description.is_empty() {
                            "(none)"
                        } else {
                            &info.description
                        }
                    );
                    println!();
                }
            }
        }

        NamespaceAction::List => {
            let namespaces = client.list_namespaces().await?;
            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&namespaces).unwrap())
                }
                OutputFormat::Table => print_namespaces_table(&namespaces),
            }
        }

        NamespaceAction::Update {
            name,
            description,
            owner_email,
            retention_days,
        } => {
            client
                .update_namespace(
                    &name,
                    description.as_deref(),
                    owner_email.as_deref(),
                    retention_days,
                )
                .await?;
            println!("{} Namespace '{}' updated", "✓".green().bold(), name.bold());
        }

        NamespaceAction::Delete { name } => {
            client.delete_namespace(&name).await?;
            println!("{} Namespace '{}' deleted", "✓".green().bold(), name.bold());
        }

        NamespaceAction::Deprecate { name } => {
            client.deprecate_namespace(&name).await?;
            println!(
                "{} Namespace '{}' deprecated",
                "✓".green().bold(),
                name.bold()
            );
        }

        NamespaceAction::Failover {
            name,
            target_cluster,
            force,
        } => {
            client
                .failover_namespace(&name, &target_cluster, force)
                .await?;
            println!(
                "{} Namespace '{}' failed over to '{}'",
                "✓".green().bold(),
                name.bold(),
                target_cluster
            );
        }
    }
    Ok(())
}

async fn handle_server(
    action: ServerAction,
    client: &VelocityClient,
    output: &OutputFormat,
) -> Result<(), String> {
    match action {
        ServerAction::Info => {
            let info = client.get_server_info().await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&info).unwrap()),
                OutputFormat::Table => {
                    println!("\n{} Server Information", "⟡".cyan().bold());
                    println!("  ┌─────────────────────────────────────────────────────────────");
                    println!("  │ Version:       {}", info.server_version.bold());
                    println!("  │ Uptime:        {}", format_uptime(info.uptime_secs));
                    println!("  │ Workflows:     {}", info.workflow_count);
                    println!("  │ Namespaces:    {}", info.namespace_count);
                    println!("  │ Features:      {}", info.supported_features.join(", "));
                    println!("  └─────────────────────────────────────────────────────────────\n");
                }
            }
        }

        ServerAction::Health => {
            let healthy = client.check_health().await?;
            if healthy {
                println!("{} Server is healthy", "✓".green().bold());
            } else {
                println!("{} Server is unhealthy", "✗".red().bold());
                return Err("Health check failed".into());
            }
        }

        ServerAction::Metrics => {
            let metrics = client.get_metrics().await?;
            println!("{}", metrics);
        }

        ServerAction::Stats => {
            let info = client.get_server_info().await?;
            println!("\n{} Server Stats", "⟡".cyan().bold());
            println!("  Workflows:     {}", info.workflow_count);
            println!("  Namespaces:    {}", info.namespace_count);
            println!("  Uptime:        {}", format_uptime(info.uptime_secs));
            println!();
        }

        ServerAction::Version => {
            let info = client.get_server_info().await?;
            println!("velocity {}", info.server_version);
        }
    }
    Ok(())
}

async fn handle_taskqueue(
    action: TaskQueueAction,
    client: &VelocityClient,
    output: &OutputFormat,
) -> Result<(), String> {
    match action {
        TaskQueueAction::Describe { name } => {
            let info = client.describe_task_queue(&name).await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&info).unwrap()),
                OutputFormat::Table => {
                    println!("\n{} Task Queue: {}", "⟡".cyan().bold(), name.bold());
                    println!("  Pending Tasks:   {}", info.pending_tasks);
                    println!("  Active Workers:  {}", info.active_workers);
                    println!();
                }
            }
        }

        TaskQueueAction::List => {
            let queues = client.list_task_queues().await?;
            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&queues).unwrap())
                }
                OutputFormat::Table => {
                    let mut table = Table::new();
                    table
                        .load_preset(UTF8_FULL)
                        .set_content_arrangement(ContentArrangement::Dynamic);
                    table.set_header(vec!["NAME", "PENDING TASKS", "ACTIVE WORKERS"]);
                    for q in &queues {
                        table.add_row(vec![
                            Cell::new(&q.name).fg(Color::Green),
                            Cell::new(q.pending_tasks),
                            Cell::new(q.active_workers),
                        ]);
                    }
                    println!("\n{table}\n");
                }
            }
        }

        TaskQueueAction::ListPartitions { name } => {
            let partitions = client.list_task_queue_partitions(&name).await?;
            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&partitions).unwrap())
                }
                OutputFormat::Table => {
                    println!("\n{} Partitions for '{}':", "⟡".cyan(), name.bold());
                    for (i, p) in partitions.iter().enumerate() {
                        println!("  [{}] {}", i, p);
                    }
                    println!();
                }
            }
        }

        TaskQueueAction::Pollers { name } => {
            let pollers = client.get_task_queue_pollers(&name).await?;
            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&pollers).unwrap())
                }
                OutputFormat::Table => {
                    println!("\n{} Pollers for '{}':", "⟡".cyan(), name.bold());
                    let mut table = Table::new();
                    table.set_header(vec!["IDENTITY", "LAST POLL", "TASKS POLLED"]);
                    for p in &pollers {
                        table.add_row(vec![
                            Cell::new(&p.identity),
                            Cell::new(&p.last_poll),
                            Cell::new(p.tasks_polled),
                        ]);
                    }
                    println!("{table}\n");
                }
            }
        }
    }
    Ok(())
}

async fn handle_cluster(
    action: ClusterAction,
    client: &VelocityClient,
    output: &OutputFormat,
) -> Result<(), String> {
    match action {
        ClusterAction::Describe => {
            let info = client.describe_cluster().await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&info).unwrap()),
                OutputFormat::Table => {
                    println!("\n{} Cluster: {}", "⟡".cyan().bold(), info.name.bold());
                    println!("  Address:    {}", info.address);
                    println!(
                        "  Active:     {}",
                        if info.is_active {
                            "Yes".green().to_string()
                        } else {
                            "No".red().to_string()
                        }
                    );
                    println!("  Replication: {}", info.replication_status);
                    println!();
                }
            }
        }
        ClusterAction::List => {
            let clusters = client.list_clusters().await?;
            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&clusters).unwrap())
                }
                OutputFormat::Table => {
                    let mut table = Table::new();
                    table
                        .load_preset(UTF8_FULL)
                        .set_content_arrangement(ContentArrangement::Dynamic);
                    table.set_header(vec!["NAME", "ADDRESS", "ACTIVE", "REPLICATION"]);
                    for c in &clusters {
                        table.add_row(vec![
                            Cell::new(&c.name).fg(Color::Green),
                            Cell::new(&c.address),
                            if c.is_active {
                                Cell::new("Yes").fg(Color::Green)
                            } else {
                                Cell::new("No").fg(Color::Red)
                            },
                            Cell::new(&c.replication_status),
                        ]);
                    }
                    println!("\n{table}\n");
                }
            }
        }
        ClusterAction::AddRemote { name, address } => {
            client.add_remote_cluster(&name, &address).await?;
            println!(
                "{} Cluster '{}' added ({})",
                "✓".green().bold(),
                name.bold(),
                address
            );
        }
        ClusterAction::RemoveRemote { name } => {
            client.remove_remote_cluster(&name).await?;
            println!("{} Cluster '{}' removed", "✓".green().bold(), name.bold());
        }
        ClusterAction::ReplicationStatus => {
            let status = client.get_replication_status().await?;
            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&status).unwrap())
                }
                OutputFormat::Table => {
                    println!("\n{} Replication Status", "⟡".cyan().bold());
                    println!("{}\n", serde_json::to_string_pretty(&status).unwrap());
                }
            }
        }
    }
    Ok(())
}

async fn handle_search_attributes(
    action: SearchAttributesAction,
    client: &VelocityClient,
    output: &OutputFormat,
) -> Result<(), String> {
    match action {
        SearchAttributesAction::List => {
            let attrs = client.list_search_attributes().await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&attrs).unwrap()),
                OutputFormat::Table => {
                    let mut table = Table::new();
                    table
                        .load_preset(UTF8_FULL)
                        .set_content_arrangement(ContentArrangement::Dynamic);
                    table.set_header(vec!["NAME", "TYPE"]);
                    for a in &attrs {
                        table.add_row(vec![
                            Cell::new(&a.name).fg(Color::Green),
                            Cell::new(&a.attr_type),
                        ]);
                    }
                    println!("\n{table}\n");
                }
            }
        }
        SearchAttributesAction::Add { name, attr_type } => {
            client.add_search_attribute(&name, &attr_type).await?;
            println!(
                "{} Search attribute '{}' ({}) added",
                "✓".green().bold(),
                name.bold(),
                attr_type
            );
        }
        SearchAttributesAction::Remove { name } => {
            client.remove_search_attribute(&name).await?;
            println!(
                "{} Search attribute '{}' removed",
                "✓".green().bold(),
                name.bold()
            );
        }
    }
    Ok(())
}

async fn handle_schedule(
    action: ScheduleAction,
    client: &VelocityClient,
    output: &OutputFormat,
) -> Result<(), String> {
    match action {
        ScheduleAction::Create {
            schedule_id,
            cron,
            workflow_type,
            task_queue,
            input,
        } => {
            client
                .create_schedule(&schedule_id, &cron, &workflow_type, &task_queue, &input)
                .await?;
            println!(
                "{} Schedule '{}' created",
                "✓".green().bold(),
                schedule_id.bold()
            );
        }
        ScheduleAction::Describe { schedule_id } => {
            let info = client.describe_schedule(&schedule_id).await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&info).unwrap()),
                OutputFormat::Table => {
                    println!(
                        "\n{} Schedule: {}",
                        "⟡".cyan().bold(),
                        info.schedule_id.bold()
                    );
                    println!("  Cron:          {}", info.cron);
                    println!("  Workflow Type: {}", info.workflow_type);
                    println!("  Task Queue:    {}", info.task_queue);
                    println!("  State:         {}", info.state);
                    if let Some(next) = &info.next_run {
                        println!("  Next Run:      {}", next);
                    }
                    if let Some(last) = &info.last_run {
                        println!("  Last Run:      {}", last);
                    }
                    println!();
                }
            }
        }
        ScheduleAction::List => {
            let schedules = client.list_schedules().await?;
            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&schedules).unwrap())
                }
                OutputFormat::Table => {
                    let mut table = Table::new();
                    table
                        .load_preset(UTF8_FULL)
                        .set_content_arrangement(ContentArrangement::Dynamic);
                    table.set_header(vec![
                        "SCHEDULE ID",
                        "CRON",
                        "WORKFLOW TYPE",
                        "STATE",
                        "NEXT RUN",
                    ]);
                    for s in &schedules {
                        table.add_row(vec![
                            Cell::new(&s.schedule_id).fg(Color::Green),
                            Cell::new(&s.cron),
                            Cell::new(&s.workflow_type),
                            Cell::new(&s.state),
                            Cell::new(s.next_run.as_deref().unwrap_or("-")),
                        ]);
                    }
                    println!("\n{table}\n");
                }
            }
        }
        ScheduleAction::Update { schedule_id, cron } => {
            if let Some(c) = cron {
                client
                    .create_schedule(&schedule_id, &c, "", "", "{}")
                    .await?;
            }
            println!(
                "{} Schedule '{}' updated",
                "✓".green().bold(),
                schedule_id.bold()
            );
        }
        ScheduleAction::Delete { schedule_id } => {
            client.delete_schedule(&schedule_id).await?;
            println!(
                "{} Schedule '{}' deleted",
                "✓".green().bold(),
                schedule_id.bold()
            );
        }
        ScheduleAction::Trigger { schedule_id } => {
            client.trigger_schedule(&schedule_id).await?;
            println!(
                "{} Schedule '{}' triggered",
                "✓".green().bold(),
                schedule_id.bold()
            );
        }
        ScheduleAction::Pause {
            schedule_id,
            reason,
        } => {
            client.pause_schedule(&schedule_id, &reason).await?;
            println!(
                "{} Schedule '{}' paused",
                "✓".green().bold(),
                schedule_id.bold()
            );
        }
        ScheduleAction::Unpause { schedule_id } => {
            client.unpause_schedule(&schedule_id).await?;
            println!(
                "{} Schedule '{}' unpaused",
                "✓".green().bold(),
                schedule_id.bold()
            );
        }
    }
    Ok(())
}

async fn handle_batch(
    action: BatchAction,
    client: &VelocityClient,
    output: &OutputFormat,
) -> Result<(), String> {
    match action {
        BatchAction::List { limit } => {
            let jobs = client.list_batch_jobs(limit).await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&jobs).unwrap()),
                OutputFormat::Table => {
                    let mut table = Table::new();
                    table
                        .load_preset(UTF8_FULL)
                        .set_content_arrangement(ContentArrangement::Dynamic);
                    table.set_header(vec!["JOB ID", "OPERATION", "STATE", "PROGRESS", "STARTED"]);
                    for j in &jobs {
                        table.add_row(vec![
                            Cell::new(&j.job_id).fg(Color::Green),
                            Cell::new(&j.operation),
                            Cell::new(&j.state),
                            Cell::new(format!("{}/{}", j.completed_count, j.total_count)),
                            Cell::new(&j.start_time),
                        ]);
                    }
                    println!("\n{table}\n");
                }
            }
        }
        BatchAction::Describe { job_id } => {
            let job = client.describe_batch_job(&job_id).await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&job).unwrap()),
                OutputFormat::Table => {
                    println!("\n{} Batch Job: {}", "⟡".cyan().bold(), job.job_id.bold());
                    println!("  Operation: {}", job.operation);
                    println!("  State:     {}", job.state);
                    println!("  Progress:  {}/{}", job.completed_count, job.total_count);
                    println!("  Started:   {}", job.start_time);
                    println!();
                }
            }
        }
        BatchAction::Terminate { job_id, reason } => {
            client.terminate_batch_job(&job_id, &reason).await?;
            println!(
                "{} Batch job '{}' terminated",
                "✓".green().bold(),
                job_id.bold()
            );
        }
        BatchAction::TerminateWorkflows { query, reason } => {
            let job_id = client.start_batch_terminate(&query, &reason).await?;
            println!(
                "{} Batch terminate started: {}",
                "✓".green().bold(),
                job_id.bold()
            );
        }
        BatchAction::CancelWorkflows { query, reason } => {
            let job_id = client.start_batch_cancel(&query, &reason).await?;
            println!(
                "{} Batch cancel started: {}",
                "✓".green().bold(),
                job_id.bold()
            );
        }
        BatchAction::SignalWorkflows {
            query,
            signal,
            input,
        } => {
            let job_id = client.start_batch_signal(&query, &signal, &input).await?;
            println!(
                "{} Batch signal '{}' started: {}",
                "✓".green().bold(),
                signal.bold(),
                job_id.bold()
            );
        }
    }
    Ok(())
}

async fn handle_admin(
    action: AdminAction,
    client: &VelocityClient,
    output: &OutputFormat,
) -> Result<(), String> {
    match action {
        AdminAction::ShardDescribe { shard_id } => {
            let info = client.admin_describe_shard(shard_id).await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&info).unwrap()),
                OutputFormat::Table => {
                    println!("\n{} Shard {}", "⟡".cyan().bold(), shard_id);
                    println!("{}\n", serde_json::to_string_pretty(&info).unwrap());
                }
            }
        }
        AdminAction::ShardClose { shard_id } => {
            client.admin_close_shard(shard_id).await?;
            println!("{} Shard {} closed", "✓".green().bold(), shard_id);
        }
        AdminAction::DlqList {
            cluster,
            shard_id,
            limit,
        } => {
            let msgs = client.admin_dlq_list(&cluster, shard_id, limit).await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&msgs).unwrap()),
                OutputFormat::Table => {
                    println!(
                        "\n{} DLQ Messages (cluster: {}, shard: {})",
                        "⟡".cyan(),
                        cluster.bold(),
                        shard_id
                    );
                    println!("{}\n", serde_json::to_string_pretty(&msgs).unwrap());
                }
            }
        }
        AdminAction::DlqPurge { cluster, shard_id } => {
            client.admin_dlq_purge(&cluster, shard_id).await?;
            println!(
                "{} DLQ purged (cluster: {}, shard: {})",
                "✓".green().bold(),
                cluster,
                shard_id
            );
        }
        AdminAction::DlqMerge { cluster, shard_id } => {
            client.admin_dlq_merge(&cluster, shard_id).await?;
            println!(
                "{} DLQ merged (cluster: {}, shard: {})",
                "✓".green().bold(),
                cluster,
                shard_id
            );
        }
        AdminAction::HistoryHostDescribe { address } => {
            let info = client.admin_history_host_describe(&address).await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&info).unwrap()),
                OutputFormat::Table => {
                    println!("\n{} History Host: {}", "⟡".cyan().bold(), address.bold());
                    println!("{}\n", serde_json::to_string_pretty(&info).unwrap());
                }
            }
        }
        AdminAction::DbListTables => {
            let tables = client.admin_db_list_tables().await?;
            match output {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&tables).unwrap())
                }
                OutputFormat::Table => {
                    println!("\n{} Database Tables", "⟡".cyan().bold());
                    for t in &tables {
                        println!("  - {}", t);
                    }
                    println!();
                }
            }
        }
        AdminAction::WorkflowRawHistory {
            workflow_id,
            run_id,
        } => {
            let raw = client
                .admin_workflow_raw_history(&workflow_id, &run_id)
                .await?;
            match output {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&raw).unwrap()),
                OutputFormat::Table => {
                    println!(
                        "\n{} Raw History for '{}' (run: {})",
                        "⟡".cyan(),
                        workflow_id.bold(),
                        run_id
                    );
                    println!("{}\n", serde_json::to_string_pretty(&raw).unwrap());
                }
            }
        }
    }
    Ok(())
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

fn format_status(status: &str) -> crossterm::style::StyledContent<&str> {
    match status.to_lowercase().as_str() {
        "running" => status.green().bold(),
        "completed" => status.blue().bold(),
        "failed" => status.red().bold(),
        "canceled" => status.yellow().bold(),
        "terminated" => status.magenta().bold(),
        _ => status.white(),
    }
}

fn format_uptime(secs: u64) -> String {
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let mins = (secs % 3600) / 60;
    if days > 0 {
        format!("{}d {}h {}m", days, hours, mins)
    } else if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else {
        format!("{}m", mins)
    }
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}", ts)
}
