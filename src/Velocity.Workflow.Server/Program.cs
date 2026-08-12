using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Velocity.Workflow.Core;
using Velocity.Workflow.Server;

var builder = WebApplication.CreateBuilder(args);

// ── JWT Token Validator ────────────────────────────────────────────────────
// Configure via appsettings.json "Jwt" section. When SigningKey is empty, JWT
// validation is disabled and the interceptor falls back to legacy subject:role parsing.
var jwtSigningKey = builder.Configuration["Jwt:SigningKey"];
var jwtIssuer = builder.Configuration["Jwt:Issuer"];
var jwtAudience = builder.Configuration["Jwt:Audience"];

// Register JWT validator as singleton
builder.Services.AddSingleton<JwtTokenValidator>(sp =>
{
    var logger = sp.GetRequiredService<ILogger<JwtTokenValidator>>();
    return new JwtTokenValidator(jwtSigningKey, jwtIssuer, jwtAudience, logger);
});

// ── gRPC with auth/rate-limit interceptor ──────────────────────────────────
builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<AuthRateLimitInterceptor>();
});

// Register the interceptor as scoped (it gets JwtTokenValidator injected)
builder.Services.AddScoped<AuthRateLimitInterceptor>();

// ── Rust-backed WorkflowRuntime ────────────────────────────────────────────
builder.Services.AddSingleton<WorkflowRuntime>(sp =>
{
    var runtime = new WorkflowRuntime();
    return runtime;
});

var app = builder.Build();

// ── Static files & Web UI ──────────────────────────────────────────────────
app.UseDefaultFiles(); // Serve index.html from wwwroot
app.UseStaticFiles();

// ── SSE Client Registry ────────────────────────────────────────────────────
var sseClients = new ConcurrentDictionary<string, HttpResponse>();

// Log JWT status at startup
if (!string.IsNullOrEmpty(jwtSigningKey))
    app.Logger.LogInformation("JWT authentication ENABLED (issuer={Issuer}, audience={Audience})", jwtIssuer, jwtAudience);
else
    app.Logger.LogWarning("JWT authentication DISABLED — using legacy subject:role auth (set Jwt:SigningKey in appsettings.json to enable)");

app.MapGrpcService<WorkflowGrpcService>();
app.MapGrpcService<AdminGrpcService>();
app.MapGet("/", () => Results.Redirect("/index.html"));

// Health check endpoint (used by Docker/K8s probes)
app.MapGet("/health", (WorkflowRuntime runtime) =>
{
    return Results.Json(new
    {
        status = "healthy",
        timestamp = DateTimeOffset.UtcNow.ToString("o"),
        version = "0.1.0",
        workflow_count = runtime.WorkflowCount,
        namespace_count = runtime.NamespaceCount
    });
});

// Prometheus metrics scraping endpoint
app.MapGet("/metrics", (WorkflowRuntime runtime) =>
{
    var metrics = runtime.ExportPrometheusMetrics();
    return Results.Text(metrics, "text/plain; charset=utf-8");
});

// ─── REST API for Web UI ─────────────────────────────────────────────────────

// Server-Sent Events endpoint for real-time updates
app.MapGet("/api/events", async (HttpContext context) =>
{
    var clientId = Guid.NewGuid().ToString();
    context.Response.ContentType = "text/event-stream";
    context.Response.Headers.CacheControl = "no-cache";
    context.Response.Headers.Connection = "keep-alive";
    
    sseClients.TryAdd(clientId, context.Response);
    
    try
    {
        // Send initial connection event
        await context.Response.WriteAsync($"data: {{\"type\":\"connected\",\"clientId\":\"{clientId}\"}}\n\n");
        await context.Response.Body.FlushAsync();
        
        // Keep connection alive
        while (!context.RequestAborted.IsCancellationRequested)
        {
            await Task.Delay(15000, context.RequestAborted);
            await context.Response.WriteAsync($": ping\n\n");
            await context.Response.Body.FlushAsync();
        }
    }
    catch (OperationCanceledException) { }
    finally
    {
        sseClients.TryRemove(clientId, out _);
    }
});

// Server info endpoint
app.MapGet("/api/server/info", (WorkflowRuntime runtime) =>
{
    return Results.Json(new
    {
        server_version = "0.1.0",
        supported_features = new[] { "signals", "queries", "child_workflows", "timers", "namespaces" },
        workflow_count = runtime.WorkflowCount,
        namespace_count = runtime.NamespaceCount,
        uptime_secs = (long)(DateTime.UtcNow - Process.GetCurrentProcess().StartTime.ToUniversalTime()).TotalSeconds
    });
});

// List workflows
app.MapGet("/api/workflows", (WorkflowRuntime runtime, string? namespaceName, string? status, int? limit) =>
{
    ulong nsFilter = ulong.MaxValue;
    if (!string.IsNullOrEmpty(namespaceName) && namespaceName != "default")
    {
        nsFilter = (ulong)namespaceName.GetHashCode();
    }
    
    var workflows = runtime.ListWorkflows(nsFilter, limit ?? 50);
    
    // Filter by status if provided
    if (!string.IsNullOrEmpty(status))
    {
        var statusEnum = status.ToLower() switch
        {
            "running" => WorkflowExecutionStatus.Running,
            "completed" => WorkflowExecutionStatus.Completed,
            "failed" => WorkflowExecutionStatus.Failed,
            "canceled" => WorkflowExecutionStatus.Canceled,
            _ => (WorkflowExecutionStatus?)null
        };
        
        if (statusEnum.HasValue)
        {
            workflows = workflows.Where(w => w.Status == statusEnum.Value).ToList();
        }
    }
    
    var result = workflows.Select(w => new
    {
        workflow_id = w.WorkflowId.ToString(),
        run_id = w.RunId.ToString(),
        workflow_type = w.WorkflowTypeId.ToString(),
        status = w.Status.ToString(),
        start_time = DateTimeOffset.FromUnixTimeMilliseconds((long)w.StartTimeMs).ToString("o"),
        close_time = w.CloseTimeMs.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds((long)w.CloseTimeMs.Value).ToString("o") : null,
        history_length = 0,
        namespace_name = w.NamespaceId.ToString(),
        task_queue = w.TaskQueueHash.ToString()
    });
    
    return Results.Json(new { workflows = result, total_count = result.Count() });
});

// Get workflow detail
app.MapGet("/api/workflows/{workflowId}", (WorkflowRuntime runtime, string workflowId) =>
{
    ulong key;
    if (!ulong.TryParse(workflowId, out key))
    {
        key = (ulong)workflowId.GetHashCode();
    }
    
    var status = runtime.GetStatus(key);
    if (status == WorkflowExecutionStatus.Void)
    {
        return Results.NotFound(new { error = "Workflow not found" });
    }
    
    var desc = runtime.DescribeWorkflow(key);
    if (desc == null)
    {
        return Results.NotFound(new { error = "Workflow not found" });
    }
    
    // Get Merkle root via slab verification
    var slabValid = runtime.VerifySlab(key);
    var merkleRoot = slabValid ? "verified" : "unverified";
    
    return Results.Json(new
    {
        workflow_id = workflowId,
        run_id = key.ToString(),
        workflow_type = desc.WorkflowTypeId.ToString(),
        status = desc.Status.ToString(),
        start_time = DateTimeOffset.FromUnixTimeMilliseconds((long)desc.StartTimeMs).ToString("o"),
        close_time = desc.CloseTimeMs.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds((long)desc.CloseTimeMs.Value).ToString("o") : null,
        history_length = desc.EventSequence,
        namespace_name = desc.NamespaceId.ToString(),
        task_queue = desc.TaskQueueHash.ToString(),
        total_steps = desc.TotalSteps,
        completed_steps = desc.CompletedSteps,
        merkle_root = merkleRoot,
        pending_activities = Array.Empty<object>()
    });
});

// Get workflow history
app.MapGet("/api/workflows/{workflowId}/history", (WorkflowRuntime runtime, string workflowId, int? limit) =>
{
    // Return empty history for now - would need engine support
    return Results.Json(new { events = Array.Empty<object>() });
});

// Start workflow
app.MapPost("/api/workflows", async (WorkflowRuntime runtime, HttpContext context) =>
{
    var body = await JsonSerializer.DeserializeAsync<JsonElement>(context.Request.Body);
    
    var workflowType = body.TryGetProperty("workflow_type", out var wt) ? wt.GetString() ?? "default" : "default";
    var taskQueue = body.TryGetProperty("task_queue", out var tq) ? tq.GetString() ?? "default" : "default";
    var totalSteps = body.TryGetProperty("total_steps", out var ts) ? (uint)ts.GetInt32() : 1;
    
    byte[]? input = null;
    if (body.TryGetProperty("input", out var inp))
    {
        input = JsonSerializer.SerializeToUtf8Bytes(inp);
    }
    
    var workflowId = (ulong)Guid.NewGuid().GetHashCode();
    var workflowTypeId = (ulong)workflowType.GetHashCode();
    var namespaceId = (ulong)"default".GetHashCode();
    var taskQueueHash = (ulong)taskQueue.GetHashCode();
    
    var key = runtime.StartWorkflow(workflowId, workflowTypeId, namespaceId, taskQueueHash, totalSteps, input);
    
    // Broadcast SSE event
    await BroadcastSse(sseClients, new { type = "workflow_updated", workflow_id = workflowId.ToString() });
    
    return Results.Json(new
    {
        workflow_id = workflowId.ToString(),
        run_id = key.ToString(),
        workflow_key = key
    });
});

// Signal workflow
app.MapPost("/api/workflows/{workflowId}/signal", async (WorkflowRuntime runtime, string workflowId, HttpContext context) =>
{
    var body = await JsonSerializer.DeserializeAsync<JsonElement>(context.Request.Body);
    
    ulong key = ulong.TryParse(workflowId, out var k) ? k : (ulong)workflowId.GetHashCode();
    var signalName = body.TryGetProperty("signal_name", out var sn) ? sn.GetString() ?? "" : "";
    var signalNameId = (ulong)signalName.GetHashCode();
    
    byte[]? payload = null;
    if (body.TryGetProperty("input", out var inp))
    {
        payload = JsonSerializer.SerializeToUtf8Bytes(inp);
    }
    
    runtime.Signal(key, signalNameId, payload);
    
    await BroadcastSse(sseClients, new { type = "workflow_updated", workflow_id = workflowId });
    
    return Results.Json(new { success = true });
});

// Query workflow
app.MapPost("/api/workflows/{workflowId}/query", async (WorkflowRuntime runtime, string workflowId, HttpContext context) =>
{
    var body = await JsonSerializer.DeserializeAsync<JsonElement>(context.Request.Body);
    
    ulong key = ulong.TryParse(workflowId, out var k) ? k : (ulong)workflowId.GetHashCode();
    var queryType = body.TryGetProperty("query_type", out var qt) ? qt.GetString() ?? "" : "";
    var queryNameId = (ulong)queryType.GetHashCode();
    
    byte[]? input = null;
    if (body.TryGetProperty("input", out var inp))
    {
        input = JsonSerializer.SerializeToUtf8Bytes(inp);
    }
    
    var result = runtime.ExecuteQuery(key, queryNameId, input);
    
    if (result == null || result.Length == 0)
    {
        var status = runtime.GetStatus(key);
        return Results.Json(new { result = status.ToString() });
    }
    
    return Results.Json(new { result = System.Text.Encoding.UTF8.GetString(result) });
});

// Cancel workflow
app.MapPost("/api/workflows/{workflowId}/cancel", async (WorkflowRuntime runtime, string workflowId) =>
{
    ulong key = ulong.TryParse(workflowId, out var k) ? k : (ulong)workflowId.GetHashCode();
    runtime.CancelWorkflow(key);
    
    await BroadcastSse(sseClients, new { type = "workflow_updated", workflow_id = workflowId });
    
    return Results.Json(new { success = true });
});

// List namespaces
app.MapGet("/api/namespaces", (WorkflowRuntime runtime) =>
{
    var namespaces = runtime.ListNamespaces();
    return Results.Json(namespaces.Select(n => new
    {
        name = n.Name,
        namespace_id = n.Id,
        description = "",
        is_active = n.IsActive,
        retention_secs = n.RetentionDays * 86400
    }));
});

// Register namespace
app.MapPost("/api/namespaces", async (HttpContext context, WorkflowRuntime runtime) =>
{
    var body = await JsonSerializer.DeserializeAsync<JsonElement>(context.Request.Body);
    var name = body.TryGetProperty("name", out var n) ? n.GetString() ?? "" : "";
    var description = body.TryGetProperty("description", out var d) ? d.GetString() ?? "" : "";
    
    if (string.IsNullOrEmpty(name))
    {
        return Results.BadRequest(new { error = "Namespace name is required" });
    }
    
    var id = runtime.RegisterNamespace(name);
    return Results.Json(new { namespace_id = id, name });
});

// Describe namespace
app.MapGet("/api/namespaces/{name}", (WorkflowRuntime runtime, string name) =>
{
    var namespaces = runtime.ListNamespaces();
    var ns = namespaces.FirstOrDefault(n => n.Name == name);
    
    if (ns == null)
    {
        return Results.NotFound(new { error = "Namespace not found" });
    }
    
    return Results.Json(new
    {
        name = ns.Name,
        namespace_id = ns.Id,
        description = "",
        is_active = ns.IsActive,
        retention_secs = ns.RetentionDays * 86400
    });
});

// Describe task queue
app.MapGet("/api/taskqueues/{name}", (WorkflowRuntime runtime, string name) =>
{
    var hash = (ulong)name.GetHashCode();
    var pending = runtime.PendingTasks(hash);
    
    return Results.Json(new
    {
        name,
        pending_tasks = pending,
        active_workers = 0 // Would need worker registry access
    });
});

// ─── Additional API endpoints from ApiEndpoints.cs ──────────────────────────────
app.MapVelocityApiEndpoints(sseClients);

// Helper to broadcast SSE events
async Task BroadcastSse(ConcurrentDictionary<string, HttpResponse> clients, object data)
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

app.Run();
