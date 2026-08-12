using System;

namespace Velocity.Workflow.Core;

[AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = false)]
public sealed class WorkflowSignalAttribute : Attribute
{
    public string SignalName { get; }
    public WorkflowSignalAttribute(string name = "") => SignalName = name;
}

[AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = false)]
public sealed class WorkflowQueryAttribute : Attribute
{
    public string QueryName { get; }
    public WorkflowQueryAttribute(string name = "") => QueryName = name;
}

[AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = false)]
public sealed class WorkflowUpdateAttribute : Attribute
{
    public string UpdateName { get; }
    public WorkflowUpdateAttribute(string name = "") => UpdateName = name;
}
