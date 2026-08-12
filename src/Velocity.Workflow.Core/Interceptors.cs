using System;
using System.Threading.Tasks;

namespace Velocity.Workflow.Core;

// ─── Interceptor Interfaces ──────────────────────────────────────────────────

/// <summary>
/// Intercepts workflow lifecycle events. Implement this interface to add
/// cross-cutting concerns (logging, metrics, tracing) to workflow execution.
/// </summary>
public interface IWorkflowInterceptor
{
    /// <summary>Called before a workflow starts.</summary>
    ValueTask OnWorkflowStarting(WorkflowInterceptContext context) => default;

    /// <summary>Called after a workflow completes successfully.</summary>
    ValueTask OnWorkflowCompleted(WorkflowInterceptContext context) => default;

    /// <summary>Called when a workflow fails with an exception.</summary>
    ValueTask OnWorkflowFailed(WorkflowInterceptContext context, Exception exception) => default;

    /// <summary>Called when a signal is delivered to a workflow.</summary>
    ValueTask OnSignalReceived(WorkflowInterceptContext context, string signalName) => default;

    /// <summary>Called when a query is received.</summary>
    ValueTask OnQueryReceived(WorkflowInterceptContext context, string queryName) => default;
}

/// <summary>
/// Intercepts activity execution. Use for retry policies, logging, metrics, etc.
/// </summary>
public interface IActivityInterceptor
{
    /// <summary>Called before an activity executes.</summary>
    ValueTask OnActivityStarting(ActivityInterceptContext context) => default;

    /// <summary>Called after an activity completes successfully.</summary>
    ValueTask OnActivityCompleted(ActivityInterceptContext context) => default;

    /// <summary>Called when an activity fails.</summary>
    ValueTask OnActivityFailed(ActivityInterceptContext context, Exception exception) => default;
}

// ─── Context Objects ──────────────────────────────────────────────────────────

/// <summary>Context passed to workflow interceptors.</summary>
public sealed class WorkflowInterceptContext
{
    public ulong WorkflowKey { get; init; }
    public ulong WorkflowId { get; init; }
    public ulong WorkflowTypeId { get; init; }
    public ulong NamespaceId { get; init; }
    public string? WorkflowTypeName { get; init; }
    public DateTimeOffset StartTime { get; init; }
}

/// <summary>Context passed to activity interceptors.</summary>
public sealed class ActivityInterceptContext
{
    public ulong WorkflowKey { get; init; }
    public uint StepIndex { get; init; }
    public ulong ActivityNameId { get; init; }
    public string? ActivityName { get; init; }
    public uint Attempt { get; init; }
}

// ─── Interceptor Pipeline ─────────────────────────────────────────────────────

/// <summary>
/// Chains multiple workflow interceptors into a single pipeline.
/// Each interceptor is called in order for each lifecycle event.
/// </summary>
public sealed class WorkflowInterceptorPipeline
{
    private readonly IWorkflowInterceptor[] _interceptors;

    public WorkflowInterceptorPipeline(params IWorkflowInterceptor[] interceptors)
    {
        _interceptors = interceptors;
    }

    public int Count => _interceptors.Length;

    public async ValueTask RaiseStarting(WorkflowInterceptContext ctx)
    {
        foreach (var interceptor in _interceptors)
            await interceptor.OnWorkflowStarting(ctx);
    }

    public async ValueTask RaiseCompleted(WorkflowInterceptContext ctx)
    {
        foreach (var interceptor in _interceptors)
            await interceptor.OnWorkflowCompleted(ctx);
    }

    public async ValueTask RaiseFailed(WorkflowInterceptContext ctx, Exception ex)
    {
        foreach (var interceptor in _interceptors)
            await interceptor.OnWorkflowFailed(ctx, ex);
    }

    public async ValueTask RaiseSignalReceived(WorkflowInterceptContext ctx, string signalName)
    {
        foreach (var interceptor in _interceptors)
            await interceptor.OnSignalReceived(ctx, signalName);
    }
}

/// <summary>
/// Chains multiple activity interceptors into a single pipeline.
/// </summary>
public sealed class ActivityInterceptorPipeline
{
    private readonly IActivityInterceptor[] _interceptors;

    public ActivityInterceptorPipeline(params IActivityInterceptor[] interceptors)
    {
        _interceptors = interceptors;
    }

    public int Count => _interceptors.Length;

    public async ValueTask RaiseStarting(ActivityInterceptContext ctx)
    {
        foreach (var interceptor in _interceptors)
            await interceptor.OnActivityStarting(ctx);
    }

    public async ValueTask RaiseCompleted(ActivityInterceptContext ctx)
    {
        foreach (var interceptor in _interceptors)
            await interceptor.OnActivityCompleted(ctx);
    }

    public async ValueTask RaiseFailed(ActivityInterceptContext ctx, Exception ex)
    {
        foreach (var interceptor in _interceptors)
            await interceptor.OnActivityFailed(ctx, ex);
    }
}

// ─── Built-in Interceptors ────────────────────────────────────────────────────

/// <summary>
/// Logs workflow and activity lifecycle events to an Action&lt;string&gt; callback.
/// </summary>
public sealed class LoggingInterceptor : IWorkflowInterceptor, IActivityInterceptor
{
    private readonly Action<string> _log;

    public LoggingInterceptor(Action<string>? log = null)
    {
        _log = log ?? Console.WriteLine;
    }

    public ValueTask OnWorkflowStarting(WorkflowInterceptContext context)
    {
        _log($"[WORKFLOW] Starting workflow {context.WorkflowId} (type={context.WorkflowTypeId}, ns={context.NamespaceId})");
        return default;
    }

    public ValueTask OnWorkflowCompleted(WorkflowInterceptContext context)
    {
        _log($"[WORKFLOW] Completed workflow {context.WorkflowId}");
        return default;
    }

    public ValueTask OnWorkflowFailed(WorkflowInterceptContext context, Exception exception)
    {
        _log($"[WORKFLOW] Failed workflow {context.WorkflowId}: {exception.Message}");
        return default;
    }

    public ValueTask OnSignalReceived(WorkflowInterceptContext context, string signalName)
    {
        _log($"[WORKFLOW] Signal '{signalName}' received by workflow {context.WorkflowId}");
        return default;
    }

    public ValueTask OnActivityStarting(ActivityInterceptContext context)
    {
        _log($"[ACTIVITY] Starting activity '{context.ActivityName}' step={context.StepIndex} attempt={context.Attempt}");
        return default;
    }

    public ValueTask OnActivityCompleted(ActivityInterceptContext context)
    {
        _log($"[ACTIVITY] Completed activity '{context.ActivityName}' step={context.StepIndex}");
        return default;
    }

    public ValueTask OnActivityFailed(ActivityInterceptContext context, Exception exception)
    {
        _log($"[ACTIVITY] Failed activity '{context.ActivityName}': {exception.Message}");
        return default;
    }
}

/// <summary>
/// Tracks workflow and activity execution metrics (counts, durations).
/// </summary>
public sealed class MetricsInterceptor : IWorkflowInterceptor, IActivityInterceptor
{
    private long _workflowsStarted;
    private long _workflowsCompleted;
    private long _workflowsFailed;
    private long _activitiesStarted;
    private long _activitiesCompleted;
    private long _activitiesFailed;
    private long _signalsReceived;

    public long WorkflowsStarted => System.Threading.Interlocked.Read(ref _workflowsStarted);
    public long WorkflowsCompleted => System.Threading.Interlocked.Read(ref _workflowsCompleted);
    public long WorkflowsFailed => System.Threading.Interlocked.Read(ref _workflowsFailed);
    public long ActivitiesStarted => System.Threading.Interlocked.Read(ref _activitiesStarted);
    public long ActivitiesCompleted => System.Threading.Interlocked.Read(ref _activitiesCompleted);
    public long ActivitiesFailed => System.Threading.Interlocked.Read(ref _activitiesFailed);
    public long SignalsReceived => System.Threading.Interlocked.Read(ref _signalsReceived);

    public ValueTask OnWorkflowStarting(WorkflowInterceptContext context)
    {
        System.Threading.Interlocked.Increment(ref _workflowsStarted);
        return default;
    }

    public ValueTask OnWorkflowCompleted(WorkflowInterceptContext context)
    {
        System.Threading.Interlocked.Increment(ref _workflowsCompleted);
        return default;
    }

    public ValueTask OnWorkflowFailed(WorkflowInterceptContext context, Exception exception)
    {
        System.Threading.Interlocked.Increment(ref _workflowsFailed);
        return default;
    }

    public ValueTask OnSignalReceived(WorkflowInterceptContext context, string signalName)
    {
        System.Threading.Interlocked.Increment(ref _signalsReceived);
        return default;
    }

    public ValueTask OnActivityStarting(ActivityInterceptContext context)
    {
        System.Threading.Interlocked.Increment(ref _activitiesStarted);
        return default;
    }

    public ValueTask OnActivityCompleted(ActivityInterceptContext context)
    {
        System.Threading.Interlocked.Increment(ref _activitiesCompleted);
        return default;
    }

    public ValueTask OnActivityFailed(ActivityInterceptContext context, Exception exception)
    {
        System.Threading.Interlocked.Increment(ref _activitiesFailed);
        return default;
    }

    /// <summary>Reset all counters to zero.</summary>
    public void Reset()
    {
        System.Threading.Interlocked.Exchange(ref _workflowsStarted, 0);
        System.Threading.Interlocked.Exchange(ref _workflowsCompleted, 0);
        System.Threading.Interlocked.Exchange(ref _workflowsFailed, 0);
        System.Threading.Interlocked.Exchange(ref _activitiesStarted, 0);
        System.Threading.Interlocked.Exchange(ref _activitiesCompleted, 0);
        System.Threading.Interlocked.Exchange(ref _activitiesFailed, 0);
        System.Threading.Interlocked.Exchange(ref _signalsReceived, 0);
    }
}
