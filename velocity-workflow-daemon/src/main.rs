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
        default_value = "http://localhost:50051",
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
    /// Server information.
    Server {
        #[command(subcommand)]
        action: ServerAction,
    },
    /// Task queue operations.
    Taskqueue {
        #[command(subcommand)]
        action: TaskQueueAction,
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
}

#[derive(Subcommand, Debug)]
enum ServerAction {
    /// Get server information.
    Info,
}

#[derive(Subcommand, Debug)]
enum TaskQueueAction {
    /// Describe a task queue.
    Describe {
        /// Task queue name.
        #[arg(long)]
        name: String,
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
