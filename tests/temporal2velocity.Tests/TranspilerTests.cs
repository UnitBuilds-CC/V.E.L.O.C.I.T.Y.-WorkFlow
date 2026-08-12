using temporal2velocity;
using Xunit;

namespace temporal2velocity.Tests;

public class TranspilerTests
{
    [Fact]
    public void TranspileSourceCode_Strips_Temporal_ProxyActivities_And_Injects_Durable_Attribute()
    {
        string input = """
            import { proxyActivities, sleep } from '@temporalio/workflow';
            const { chargeCreditCard } = proxyActivities<typeof activities>({ startToCloseTimeout: '1m' });

            export async function processPaymentWorkflow(id: string) {
                await chargeCreditCard(id);
                await sleep('10s');
            }
            """;

        string output = TranspilerEngine.TranspileSourceCode(input);

        Assert.Contains("@velocity/core", output);
        Assert.DoesNotContain("proxyActivities", output);
        Assert.Contains("@Durable()", output);
        Assert.Contains("Task.Delay", output);
    }

    [Fact]
    public void HydrateFromTemporalJson_Creates_Valid_SlabHeader_With_Bitmasks()
    {
        var slab = TranspilerEngine.HydrateFromTemporalJson(555, 777, 5);

        Assert.True(slab.IsValid);
        Assert.Equal(555UL, slab.WorkflowId);
        Assert.Equal(5U, slab.CurrentStep);
        Assert.True(slab.IsStepSet(0));
        Assert.True(slab.IsStepSet(4));
        Assert.False(slab.IsStepSet(5));
    }
}
