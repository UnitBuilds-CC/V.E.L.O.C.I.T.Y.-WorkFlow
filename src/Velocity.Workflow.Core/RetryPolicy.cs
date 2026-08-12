using System;
using System.Collections.Generic;

namespace Velocity.Workflow.Core;

public struct RetryPolicy
{
    public TimeSpan InitialInterval { get; set; }
    public double BackoffCoefficient { get; set; }
    public TimeSpan MaximumInterval { get; set; }
    public int MaximumAttempts { get; set; }
    public HashSet<Type>? NonRetryableErrorTypes { get; set; }

    public static RetryPolicy Default => new RetryPolicy
    {
        InitialInterval = TimeSpan.FromSeconds(1),
        BackoffCoefficient = 2.0,
        MaximumInterval = TimeSpan.FromMinutes(1),
        MaximumAttempts = 5,
        NonRetryableErrorTypes = new HashSet<Type>()
    };

    public readonly TimeSpan GetDelayForAttempt(int attempt)
    {
        if (attempt <= 1) return InitialInterval;
        double factor = Math.Pow(BackoffCoefficient, attempt - 1);
        double ticks = InitialInterval.Ticks * factor;
        TimeSpan delay = TimeSpan.FromTicks((long)ticks);
        return delay > MaximumInterval ? MaximumInterval : delay;
    }
}
