using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Server;

/// <summary>
/// Extension methods for mapping VELOCITY WorkFlow API endpoints.
/// Provides dashboard stats, health checks, event history, and workflow management endpoints.
/// </summary>
public static class ApiEndpoints
{
    /// <summary>
    /// Map all additional API endpoints for the VELOCITY WorkFlow Web UI.
    /// </summary>
    public static WebApplication MapVelocityApiEndpoints(this WebApplication app,
        ConcurrentDictionary<string, HttpResponse> sseClients)
    {
        // ─── Dashboard Statistics ──────────────────────────────────────────────
        app.MapGet("/api/stats", (WorkflowRuntime runtime) =>
        {
            var totalWorkflows = runtime.WorkflowCount;
            var running = runtime.CountByStatus(WorkflowExecutionStatus.Running);
            var completed = runtime.CountByStatus(WorkflowExecutionStatus.Completed);
            var failed = runtime.CountByStatus(WorkflowExecutionStatus.Failed);
            var canceled = runtime.CountByStatus(WorkflowExecutionStatus.Canceled);
            var terminated = runtime.CountByStatus(WorkflowExecutionStatus.Terminated);
            var timedOut = runtime.CountByStatus(WorkflowExecutionStatus.TimedOut);

            // Calculate average latency from completed workflows
            var allWorkflows = runtime.ListWorkflows(ulong.MaxValue, 1000);
            double avgLatencyMs = 0;
            var completedWorkflows = allWorkflows
                .Where(w => w.Status == WorkflowExecutionStatus.Completed && w.CloseTimeMs.HasValue)
                .ToList();

            if (completedWorkflows.Count > 0)
            {
                avgLatencyMs = completedWorkflows
                    .Average(w => (double)(w.CloseTimeMs!.Value - w.StartTimeMs));
            }

            // Throughput: workflows completed in last hour
            var oneHourAgo = (ulong)DateTimeOffset.UtcNow.AddHours(-1).ToUnixTimeMilliseconds();
            var throughputLastHour = completedWorkflows
                .Count(w => w.CloseTimeMs.HasValue && w.CloseTimeMs.Value >= oneHourAgo);

            // Throughput: workflows completed in last minute (for mini chart)
            var oneMinuteAgo = (ulong)DateTimeOffset.UtcNow.AddMinutes(-1).ToUnixTimeMilliseconds();
            var throughputLastMinute = completedWorkflows
                .Count(w => w.CloseTimeMs.HasValue && w.CloseTimeMs.Value >= oneMinuteAgo);

            return Results.Json(new
            {
                total_workflows = totalWorkflows,
                running = running,
                completed = completed,
                failed = failed,
                canceled = canceled,
                terminated = terminated,
                timed_out = timedOut,
                avg_latency_ms = Math.Round(avgLatencyMs, 2),
                throughput_last_hour = throughputLastHour,
                throughput_last_minute = throughputLastMinute,
                namespace_count = runtime.NamespaceCount,
                pending_tasks = runtime.PendingTimers,
                cron_schedules = runtime.CronScheduleCount
            });
        });

        // ─── Health Check ──────────────────────────────────────────────────────
        app.MapGet("/api/health", (WorkflowRuntime runtime) =>
        {
            var process = Process.GetCurrentProcess();
            var uptimeSecs = (long)(DateTime.UtcNow - process.StartTime.ToUniversalTime()).TotalSeconds;
            var memoryMb = Math.Round((double)process.WorkingSet64 / 1024 / 1024, 2);

            // Engine health: can we query workflow count?
            string engineStatus = "healthy";
            try
            {
                _ = runtime.WorkflowCount;
            }
            catch (Exception ex)
            {
                engineStatus = $"degraded: {ex.Message}";
            }

            return Results.Json(new
            {
                status = engineStatus == "healthy" ? "healthy" : "degraded",
                engine = engineStatus,
                db_connection = "in-memory",
                memory_mb = memoryMb,
                uptime_secs = uptimeSecs,
                workflow_count = runtime.WorkflowCount,
                namespace_count = runtime.NamespaceCount,
                server_version = "0.1.0"
            });
        });

        // ─── Workflow Event History ────────────────────────────────────────────
        app.MapGet("/api/workflows/{workflowId}/events", (WorkflowRuntime runtime, string workflowId, ulong? startEventId, uint? limit) =>
        {
            ulong key = ulong.TryParse(workflowId, out var k) ? k : (ulong)workflowId.GetHashCode();

            var status = runtime.GetStatus(key);
            if (status == WorkflowExecutionStatus.Void)
            {
                return Results.NotFound(new { error = "Workflow not found" });
            }

            var events = runtime.GetHistoryEvents(key, startEventId ?? 1, limit ?? 100);

            var eventNames = new Dictionary<int, string>
            {
                { 0, "workflow_started" },
                { 1, "step_completed" },
                { 2, "signal_received" },
                { 3, "query_executed" },
                { 4, "workflow_completed" },
                { 5, "workflow_failed" },
                { 6, "workflow_canceled" },
                { 7, "workflow_terminated" },
                { 8, "timer_scheduled" },
                { 9, "timer_fired" },
                { 10, "activity_scheduled" },
                { 11, "activity_completed" },
                { 12, "activity_failed" },
                { 13, "child_workflow_started" },
                { 14, "child_workflow_completed" },
                { 15, "child_workflow_failed" },
                { 16, "update_received" },
                { 17, "search_attribute_set" }
            };

            var result = events.Select(e =>
            {
                var eventName = eventNames.TryGetValue(e.EventType, out var name) ? name : $"unknown_{e.EventType}";
                return new
                {
                    event_id = e.EventId,
                    event_type = eventName,
                    event_type_id = e.EventType,
                    event_time = DateTimeOffset.FromUnixTimeMilliseconds((long)e.TimestampMs).ToString("o"),
                    details = e.Payload != null ? System.Text.Encoding.UTF8.GetString(e.Payload) : null
                };
            });

            return Results.Json(new { events = result, total_count = result.Count() });
        });

        // ─── Signal History ────────────────────────────────────────────────────
        app.MapGet("/api/workflows/{workflowId}/signals", (WorkflowRuntime runtime, string workflowId) =>
        {
            ulong key = ulong.TryParse(workflowId, out var k) ? k : (ulong)workflowId.GetHashCode();

            var status = runtime.GetStatus(key);
            if (status == WorkflowExecutionStatus.Void)
            {
                return Results.NotFound(new { error = "Workflow not found" });
            }

            // Extract signal events from history
            var events = runtime.GetHistoryEvents(key, 1, 1000);
            var signalEvents = events
                .Where(e => e.EventType == 2) // signal_received
                .Select(e => new
                {
                    event_id = e.EventId,
                    signal_time = DateTimeOffset.FromUnixTimeMilliseconds((long)e.TimestampMs).ToString("o"),
                    details = e.Payload != null ? System.Text.Encoding.UTF8.GetString(e.Payload) : null
                });

            return Results.Json(new { signals = signalEvents, total_count = signalEvents.Count() });
        });

        // ─── Query History ─────────────────────────────────────────────────────
        app.MapGet("/api/workflows/{workflowId}/queries", (WorkflowRuntime runtime, string workflowId) =>
        {
            ulong key = ulong.TryParse(workflowId, out var k) ? k : (ulong)workflowId.GetHashCode();

            var status = runtime.GetStatus(key);
            if (status == WorkflowExecutionStatus.Void)
            {
                return Results.NotFound(new { error = "Workflow not found" });
            }

            // Extract query events from history
            var events = runtime.GetHistoryEvents(key, 1, 1000);
            var queryEvents = events
                .Where(e => e.EventType == 3) // query_executed
                .Select(e => new
                {
                    event_id = e.EventId,
                    query_time = DateTimeOffset.FromUnixTimeMilliseconds((long)e.TimestampMs).ToString("o"),
                    details = e.Payload != null ? System.Text.Encoding.UTF8.GetString(e.Payload) : null
                });

            return Results.Json(new { queries = queryEvents, total_count = queryEvents.Count() });
        });

        // ─── Terminate Workflow ────────────────────────────────────────────────
        app.MapPost("/api/workflows/{workflowId}/terminate", async (WorkflowRuntime runtime, string workflowId, HttpContext context) =>
        {
            ulong key = ulong.TryParse(workflowId, out var k) ? k : (ulong)workflowId.GetHashCode();

            var status = runtime.GetStatus(key);
            if (status == WorkflowExecutionStatus.Void)
            {
                return Results.NotFound(new { error = "Workflow not found" });
            }

            if (status != WorkflowExecutionStatus.Running)
            {
                return Results.BadRequest(new { error = $"Workflow is not running (status: {status})" });
            }

            runtime.TerminateWorkflow(key);

            // Broadcast SSE event
            await BroadcastSse(sseClients, new { type = "workflow_updated", workflow_id = workflowId });

            return Results.Json(new { success = true, message = "Workflow terminated" });
        });

        // ─── List Task Queues ──────────────────────────────────────────────────
        app.MapGet("/api/taskqueues", (WorkflowRuntime runtime) =>
        {
            // Get all workflows and extract unique task queue hashes
            var workflows = runtime.ListWorkflows(ulong.MaxValue, 10000);
            var taskQueues = workflows
                .GroupBy(w => w.TaskQueueHash)
                .Select(g =>
                {
                    var pending = runtime.PendingTasks(g.Key);
                    return new
                    {
                        task_queue_hash = g.Key.ToString(),
                        workflow_count = g.Count(),
                        pending_tasks = pending,
                        running_workflows = g.Count(w => w.Status == WorkflowExecutionStatus.Running),
                        completed_workflows = g.Count(w => w.Status == WorkflowExecutionStatus.Completed)
                    };
                })
                .ToList();

            return Results.Json(new { task_queues = taskQueues, total_count = taskQueues.Count });
        });

        return app;
    }

    /// <summary>
    /// Broadcast an SSE event to all connected clients.
    /// </summary>
    private static async Task BroadcastSse(ConcurrentDictionary<string, HttpResponse> clients, object data)
    {
        var json = JsonSerializer.Serialize(data);
        var message = $"data: {json}\n\n";

        foreach (var client in clients.Values)
        {
            try
            {
                await client.WriteAsync(message);
                await client.Body.FlushAsync();
            }
            catch { /* Client disconnected */ }
        }
    }
}
