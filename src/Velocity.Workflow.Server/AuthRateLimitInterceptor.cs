using Grpc.Core;
using Grpc.Core.Interceptors;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Server;

/// <summary>
/// gRPC server interceptor that enforces authentication/authorization and rate limiting.
/// Supports two auth modes:
/// 1. JWT Bearer tokens (when JwtTokenValidator is enabled) — validates signature, expiry, extracts claims
/// 2. Legacy subject:role format (fallback when JWT is not configured)
/// </summary>
public class AuthRateLimitInterceptor : Interceptor
{
    private readonly WorkflowRuntime _runtime;
    private readonly ILogger<AuthRateLimitInterceptor> _logger;
    private readonly JwtTokenValidator? _jwtValidator;

    /// <summary>Permission bits matching Temporal's RBAC model.</summary>
    private const uint PERM_READ = 1;
    private const uint PERM_WRITE = 2;
    private const uint PERM_ADMIN = 4;

    /// <summary>RPCs that are read-only and require only PERM_READ.</summary>
    private static readonly HashSet<string> ReadOnlyMethods = new(StringComparer.OrdinalIgnoreCase)
    {
        "/workflow.WorkflowService/DescribeWorkflow",
        "/workflow.WorkflowService/ListWorkflows",
        "/workflow.WorkflowService/CountWorkflows",
        "/workflow.WorkflowService/GetWorkflowExecutionHistory",
        "/workflow.WorkflowService/DescribeTaskQueue",
        "/workflow.WorkflowService/DescribeNamespace",
        "/workflow.WorkflowService/ListNamespaces",
        "/workflow.WorkflowService/PollActivityTaskQueue",
        "/workflow.WorkflowService/QueryWorkflow",
    };

    /// <summary>RPCs that require PERM_ADMIN (namespace management, batch ops).</summary>
    private static readonly HashSet<string> AdminMethods = new(StringComparer.OrdinalIgnoreCase)
    {
        "/workflow.WorkflowService/RegisterNamespace",
    };

    public AuthRateLimitInterceptor(
        WorkflowRuntime runtime,
        ILogger<AuthRateLimitInterceptor> logger,
        JwtTokenValidator? jwtValidator = null)
    {
        _runtime = runtime;
        _logger = logger;
        _jwtValidator = jwtValidator;
    }

    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        // ── Extract caller identity from metadata ──────────────────────────
        var authHeader = context.RequestHeaders.Get("authorization")?.Value;
        string? subject = null;
        string? role = null;

        if (!string.IsNullOrEmpty(authHeader))
        {
            if (authHeader.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
            {
                var token = authHeader["Bearer ".Length..].Trim();

                // Try JWT validation first if enabled
                if (_jwtValidator is { IsEnabled: true })
                {
                    var claims = _jwtValidator.Validate(token);
                    if (claims is null)
                    {
                        _logger.LogWarning("JWT validation failed for request to {Method}", context.Method);
                        throw new RpcException(new Status(StatusCode.Unauthenticated,
                            "Invalid or expired JWT token"));
                    }

                    subject = claims.Subject;
                    // Use the highest-privilege role for authorization
                    role = SelectHighestRole(claims.Roles);

                    _logger.LogDebug("JWT authenticated: subject={Subject} roles={Roles} expires={Expires}",
                        subject, string.Join(",", claims.Roles), claims.ExpiresAt);
                }
                else
                {
                    // Legacy: treat the bearer value as the subject directly
                    subject = token;
                    role = "operator";
                }
            }
            else if (authHeader.Contains(':'))
            {
                // Legacy subject:role format
                var parts = authHeader.Split(':', 2);
                subject = parts[0];
                role = parts[1];
            }
            else
            {
                subject = authHeader;
                role = "reader";
            }
        }

        // ── Determine required permission for this RPC ─────────────────────
        var methodPath = context.Method;
        uint requiredPerm;
        if (AdminMethods.Contains(methodPath))
            requiredPerm = PERM_ADMIN;
        else if (ReadOnlyMethods.Contains(methodPath))
            requiredPerm = PERM_READ;
        else
            requiredPerm = PERM_WRITE; // Default: write for workflow mutations

        // ── Authorize (skip if no auth configured / anonymous access) ──────
        if (subject is not null && role is not null)
        {
            if (!_runtime.Authorize(subject, role, requiredPerm))
            {
                _logger.LogWarning("Authorization denied: subject={Subject} role={Role} method={Method}",
                    subject, role, methodPath);
                throw new RpcException(new Status(StatusCode.PermissionDenied,
                    $"Authorization denied for role '{role}' on method '{methodPath}'"));
            }
        }

        // ── Rate limit check ───────────────────────────────────────────────
        // Extract namespace from request metadata or use a default bucket
        var nsHeader = context.RequestHeaders.Get("x-namespace")?.Value ?? "default";
        ulong namespaceId = (ulong)nsHeader.GetHashCode();

        if (!_runtime.TryRateLimit(namespaceId))
        {
            _logger.LogWarning("Rate limit exceeded: namespace={Namespace} method={Method}",
                nsHeader, methodPath);
            throw new RpcException(new Status(StatusCode.ResourceExhausted,
                $"Rate limit exceeded for namespace '{nsHeader}'"));
        }

        // ── Proceed to the actual handler ──────────────────────────────────
        return continuation(request, context);
    }

    /// <summary>
    /// Select the highest-privilege role from a list of JWT roles.
    /// Priority: admin > operator > writer > reader
    /// </summary>
    private static string SelectHighestRole(List<string> roles)
    {
        if (roles.Contains("admin", StringComparer.OrdinalIgnoreCase)) return "admin";
        if (roles.Contains("operator", StringComparer.OrdinalIgnoreCase)) return "operator";
        if (roles.Contains("writer", StringComparer.OrdinalIgnoreCase)) return "writer";
        return roles.FirstOrDefault() ?? "reader";
    }
}
