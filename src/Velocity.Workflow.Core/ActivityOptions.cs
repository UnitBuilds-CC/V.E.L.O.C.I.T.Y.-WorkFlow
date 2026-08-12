using System;

namespace Velocity.Workflow.Core;

public struct ActivityOptions
{
    public TimeSpan ScheduleToStart { get; set; }
    public TimeSpan StartToClose { get; set; }
    public TimeSpan ScheduleToClose { get; set; }
    public TimeSpan HeartbeatTimeout { get; set; }

    public static ActivityOptions Default => new ActivityOptions
    {
        ScheduleToStart = TimeSpan.FromMinutes(1),
        StartToClose = TimeSpan.FromMinutes(5),
        ScheduleToClose = TimeSpan.FromMinutes(10),
        HeartbeatTimeout = TimeSpan.FromSeconds(30)
    };
}
