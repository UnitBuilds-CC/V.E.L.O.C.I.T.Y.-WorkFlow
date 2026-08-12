using Velocity.Workflow.Core;
using Xunit;

namespace Velocity.Workflow.Generators.Tests;

public partial class SampleWorkflowTarget
{
    [DurableWorkflow]
    public void ExecuteOrderFlow()
    {
        // Sample workflow target
    }
}

public unsafe class GeneratorTests
{
    [Fact]
    public void GeneratedRunner_Is_Emitted_And_Executes_Slab_Step()
    {
        var header = new DurableSlabHeader
        {
            Magic = 0x564C4354, // "VLCT"
            WorkflowId = 1234,
            RunId = 5678,
            TotalSteps = 5
        };

        int step = SampleWorkflowTarget.ExecuteOrderFlow_GeneratedRunner(ref header);

        Assert.Equal(1, step);
        Assert.True(header.IsStepSet(0));
    }
}
