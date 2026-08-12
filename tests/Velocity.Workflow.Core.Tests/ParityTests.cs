using System;
using System.Threading.Tasks;
using Velocity.Workflow.Core;
using Xunit;

namespace Velocity.Workflow.Core.Tests;

public class ParityTests
{
    [Fact]
    public async Task ActivityExecutor_Executes_With_Exponential_Backoff_Retries()
    {
        int attempts = 0;
        var options = ActivityOptions.Default;
        var policy = new RetryPolicy
        {
            InitialInterval = TimeSpan.FromMilliseconds(10),
            BackoffCoefficient = 2.0,
            MaximumInterval = TimeSpan.FromMilliseconds(100),
            MaximumAttempts = 3
        };

        var result = await ActivityExecutor.ExecuteWithRetryAsync(async () =>
        {
            attempts++;
            if (attempts < 3) throw new InvalidOperationException("Transient Failure");
            return await Task.FromResult("SuccessResult");
        }, options, policy);

        Assert.Equal("SuccessResult", result);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public void WorkflowChannel_Sends_And_Receives_Signals()
    {
        var channel = new WorkflowChannel<string>();
        channel.SendSignal("OrderPaid");

        Assert.Equal(1, channel.Count);
        Assert.True(channel.TryReceiveSignal(out string signal));
        Assert.Equal("OrderPaid", signal);
        Assert.Equal(0, channel.Count);
    }

    [Fact]
    public void SearchAttributes_Stores_And_Retrieves_Metadata()
    {
        var search = new SearchAttributes();
        search.Set("CustomInt", 42);
        search.Set("CustomString", "EnterpriseOrder");

        Assert.True(search.TryGet("CustomInt", out int valInt));
        Assert.Equal(42, valInt);

        Assert.True(search.TryGet("CustomString", out string valStr));
        Assert.Equal("EnterpriseOrder", valStr);
    }
}
