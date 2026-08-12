using System;

namespace Velocity.Workflow.Core;

public struct CronSchedule
{
    public string CronExpression { get; set; }
    public TimeSpan Jitter { get; set; }
    public bool Paused { get; set; }

    public CronSchedule(string cronExpression)
    {
        CronExpression = cronExpression;
        Jitter = TimeSpan.Zero;
        Paused = false;
    }
}
