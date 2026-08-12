using System;

namespace Velocity.Workflow.Core;

public enum ParentClosePolicy
{
    Terminate = 0,
    Abandon = 1,
    Cancel = 2
}

public struct ChildWorkflowOptions
{
    public string WorkflowId { get; set; }
    public ParentClosePolicy ParentClosePolicy { get; set; }
    public TimeSpan ExecutionTimeout { get; set; }

    public static ChildWorkflowOptions Default => new ChildWorkflowOptions
    {
        WorkflowId = string.Empty,
        ParentClosePolicy = ParentClosePolicy.Terminate,
        ExecutionTimeout = TimeSpan.FromHours(1)
    };
}
