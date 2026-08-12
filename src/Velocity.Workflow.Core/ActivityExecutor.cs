using System;
using System.Threading.Tasks;

namespace Velocity.Workflow.Core;

public static class ActivityExecutor
{
    public static async Task<TResult> ExecuteWithRetryAsync<TResult>(
        Func<Task<TResult>> activityFunc,
        ActivityOptions options,
        RetryPolicy policy,
        Action<object>? heartbeatCallback = null)
    {
        int attempt = 0;
        while (true)
        {
            attempt++;
            try
            {
                // Record heartbeat if configured
                heartbeatCallback?.Invoke($"Attempt_{attempt}");
                return await activityFunc();
            }
            catch (Exception ex)
            {
                if (policy.NonRetryableErrorTypes != null && policy.NonRetryableErrorTypes.Contains(ex.GetType()))
                {
                    throw; // Non-retryable error
                }

                if (policy.MaximumAttempts > 0 && attempt >= policy.MaximumAttempts)
                {
                    throw; // Max attempts exceeded
                }

                TimeSpan delay = policy.GetDelayForAttempt(attempt);
                await Task.Delay(delay);
            }
        }
    }
}
