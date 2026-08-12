using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Velocity.Workflow.Core.Testing;

/// <summary>
/// Test environment for workflow unit testing with time-skipping support.
/// Provides a deterministic clock, mock activity execution, and workflow replay.
/// Mirrors Temporal's TestWorkflowEnvironment / TestServer functionality.
/// </summary>
public sealed class TestWorkflowEnvironment : IDisposable
{
    private readonly WorkflowRuntime _runtime;
    private readonly TestClock _clock;
    private readonly ConcurrentDictionary<ulong, Func<byte[]?, byte[]?>> _activityHandlers;
    private readonly ConcurrentDictionary<ulong, Func<byte[]?, byte[]?>> _workflowResultHandlers;
    private bool _disposed;

    public TestWorkflowEnvironment()
    {
        _runtime = new WorkflowRuntime();
        _clock = new TestClock();
        _activityHandlers = new ConcurrentDictionary<ulong, Func<byte[]?, byte[]?>>();
        _workflowResultHandlers = new ConcurrentDictionary<ulong, Func<byte[]?, byte[]?>>();
    }

    /// <summary>The underlying runtime (for direct FFI access if needed).</summary>
    public WorkflowRuntime Runtime => _runtime;

    /// <summary>The deterministic test clock.</summary>
    public TestClock Clock => _clock;

    /// <summary>
    /// Register a mock activity handler. When the activity is scheduled,
    /// the handler is called synchronously and the result is stored.
    /// </summary>
    public void RegisterActivity(ulong activityNameId, Func<byte[]?, byte[]?> handler)
    {
        _activityHandlers[activityNameId] = handler;
    }

    /// <summary>
    /// Register a mock activity handler by name (uses the same hash as WorkflowContext).
    /// </summary>
    public void RegisterActivity(string name, Func<byte[]?, byte[]?> handler)
    {
        ulong id = HashName(name);
        _activityHandlers[id] = handler;
    }

    /// <summary>
    /// Start a test workflow and auto-execute all activities through registered handlers.
    /// Returns the workflow key.
    /// </summary>
    public ulong StartWorkflow(ulong workflowId, ulong workflowTypeId, ulong namespaceId,
        ulong taskQueueHash, uint totalSteps, byte[]? input = null)
    {
        return _runtime.StartWorkflow(workflowId, workflowTypeId, namespaceId, taskQueueHash, totalSteps, input);
    }

    /// <summary>
    /// Execute all pending tasks in the task queue. Activities are resolved through
    /// registered handlers. Workflow tasks advance the step counter.
    /// </summary>
    public int ExecutePendingTasks(ulong taskQueueHash, int maxIterations = 100)
    {
        int executed = 0;
        while (executed < maxIterations)
        {
            var task = _runtime.PollTask(taskQueueHash);
            if (task is null) break;

            switch (task.TaskKind)
            {
                case TaskKind.WorkflowTask:
                    // Workflow task: just acknowledge (the runner handles step advancement)
                    executed++;
                    break;

                case TaskKind.ActivityTask:
                    // Execute the activity through the registered handler
                    if (_activityHandlers.TryGetValue(task.ActivityNameId, out var handler))
                    {
                        var result = handler(null);
                        _runtime.CompleteStep(task.WorkflowKey, task.StepIndex, result);
                    }
                    else
                    {
                        // No handler registered — complete with empty result
                        _runtime.CompleteStep(task.WorkflowKey, task.StepIndex);
                    }
                    executed++;
                    break;

                case TaskKind.SignalTask:
                    executed++;
                    break;

                case TaskKind.TimerTask:
                    executed++;
                    break;
            }
        }
        return executed;
    }

    /// <summary>
    /// Advance the test clock by the specified duration. Fires any timers that
    /// would have expired during this period.
    /// </summary>
    public void AdvanceTime(TimeSpan duration)
    {
        _clock.Advance(duration);
    }

    /// <summary>
    /// Get the current status of a workflow.
    /// </summary>
    public WorkflowExecutionStatus GetStatus(ulong workflowKey)
    {
        return _runtime.GetStatus(workflowKey);
    }

    /// <summary>
    /// Signal a workflow in the test environment.
    /// </summary>
    public void SignalWorkflow(ulong workflowKey, ulong signalNameId, byte[]? payload = null)
    {
        _runtime.Signal(workflowKey, signalNameId, payload);
    }

    /// <summary>
    /// Hash a name to a u64 (same algorithm as WorkflowContext).
    /// </summary>
    public static ulong HashName(string name)
    {
        ulong hash = 0;
        foreach (char c in name)
            hash = hash * 31 + c;
        return hash;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _runtime.Dispose();
            _disposed = true;
        }
    }
}

/// <summary>
/// Deterministic clock for testing. Time only advances when explicitly told to.
/// Supports scheduling timers that fire when the clock advances past their deadline.
/// </summary>
public sealed class TestClock
{
    private DateTimeOffset _currentTime;
    private readonly List<(DateTimeOffset fireAt, Action callback)> _timers;
    private readonly object _lock = new();

    public TestClock()
    {
        _currentTime = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        _timers = new List<(DateTimeOffset, Action)>();
    }

    /// <summary>Current deterministic time.</summary>
    public DateTimeOffset Now
    {
        get { lock (_lock) return _currentTime; }
    }

    /// <summary>Advance the clock by a duration. Fires any expired timers.</summary>
    public void Advance(TimeSpan duration)
    {
        List<Action> toFire;
        lock (_lock)
        {
            _currentTime += duration;
            toFire = new List<Action>();
            _timers.RemoveAll(t =>
            {
                if (t.fireAt <= _currentTime)
                {
                    toFire.Add(t.callback);
                    return true;
                }
                return false;
            });
        }
        foreach (var callback in toFire)
            callback();
    }

    /// <summary>Set the clock to a specific time.</summary>
    public void SetTime(DateTimeOffset time)
    {
        List<Action> toFire;
        lock (_lock)
        {
            _currentTime = time;
            toFire = new List<Action>();
            _timers.RemoveAll(t =>
            {
                if (t.fireAt <= _currentTime)
                {
                    toFire.Add(t.callback);
                    return true;
                }
                return false;
            });
        }
        foreach (var callback in toFire)
            callback();
    }

    /// <summary>Schedule a callback to fire at a specific time.</summary>
    public void ScheduleTimer(DateTimeOffset fireAt, Action callback)
    {
        lock (_lock)
        {
            _timers.Add((fireAt, callback));
        }
    }

    /// <summary>Schedule a callback to fire after a delay from now.</summary>
    public void ScheduleTimer(TimeSpan delay, Action callback)
    {
        lock (_lock)
        {
            _timers.Add((_currentTime + delay, callback));
        }
    }

    /// <summary>Get the number of pending timers.</summary>
    public int PendingTimerCount
    {
        get { lock (_lock) return _timers.Count; }
    }
}
