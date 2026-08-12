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

// Log JWT status at startup
if (!string.IsNullOrEmpty(jwtSigningKey))
    app.Logger.LogInformation("JWT authentication ENABLED (issuer={Issuer}, audience={Audience})", jwtIssuer, jwtAudience);
else
    app.Logger.LogWarning("JWT authentication DISABLED — using legacy subject:role auth (set Jwt:SigningKey in appsettings.json to enable)");

app.MapGrpcService<WorkflowGrpcService>();
app.MapGrpcService<AdminGrpcService>();
app.MapGet("/", () => "Velocity Workflow gRPC Server — communication via gRPC only");

// Prometheus metrics scraping endpoint
app.MapGet("/metrics", (WorkflowRuntime runtime) =>
{
    var metrics = runtime.ExportPrometheusMetrics();
    return Results.Text(metrics, "text/plain; charset=utf-8");
});

app.Run();
