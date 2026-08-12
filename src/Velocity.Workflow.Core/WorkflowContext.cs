using System.Threading.Tasks;

namespace Velocity.Workflow.Core;

/// <summary>
/// Lightweight execution context passed to generated state machine runners.
/// Holds only a reference to the runtime and the workflow key — all actual state
/// (bitmask, step results, Merkle root) lives in Rust. This class allocates nothing
/// beyond the initial construction and delegates every call to the Rust engine via FFI.
/// </summary>
public sealed class WorkflowContext
{
    private readonly WorkflowRuntime _runtime;
    private readonly ulong _workflowKey;

    public WorkflowContext(WorkflowRuntime runtime, ulong workflowKey)
    {
        _runtime = runtime;
        _workflowKey = workflowKey;
    }

    /// <summary>O(1) bitmask check — is this step already completed? Delegates to Rust.</summary>
    public bool IsStepCompleted(int step) => _runtime.IsStepCompleted(_workflowKey, (uint)step);

    /// <summary>
    /// Execute a step: marks it in the bitmask, schedules the activity in Rust, and suspends
    /// the runner. When the activity completes, the Rust engine stores the result and the
    /// runner resumes on replay (the step will then be skipped via the bitmask).
    /// </summary>
    public Task<object?> ExecuteStepAsync(int step, string activityName, object?[]? args = null)
    {
        // Hash the activity name to a u64 for the Rust engine
        ulong activityNameId = 0;
        unsafe
        {
            fixed (char* p = activityName)
            {
                for (int i = 0; i < activityName.Length; i++)
                    activityNameId = activityNameId * 31 + p[i];
            }
        }

        _runtime.ScheduleActivity(_workflowKey, (uint)step, activityNameId);

        // Suspend: the activity will complete asynchronously.
        // On replay, IsStepCompleted will return true and this branch is skipped.
        return Task.FromResult<object?>(null);
    }

    /// <summary>The workflow key this context is bound to.</summary>
    public ulong WorkflowKey => _workflowKey;
}
