using System.Runtime.InteropServices;
using Velocity.Workflow.Core;
using Xunit;

namespace Velocity.Workflow.Core.Tests;

public unsafe class SlabHeaderTests
{
    [Fact]
    public void SlabHeader_Size_Must_Be_Exactly_128_Bytes()
    {
        int size = sizeof(DurableSlabHeader);
        Assert.Equal(128, size);
    }

    [Fact]
    public void SlabHeader_Magic_And_Validation_Check()
    {
        var header = new DurableSlabHeader
        {
            Magic = 0x564C4354, // "VLCT"
            WorkflowId = 100,
            RunId = 200,
            TotalSteps = 10
        };

        Assert.True(header.IsValid);
        Assert.False(header.IsStepSet(0));
    }

    [Fact]
    public void SlabHeader_Bitmask_Operations_Work_Correctly()
    {
        var header = new DurableSlabHeader();
        header.BitmaskWord0 |= (1UL << 5); // Step 5 set

        Assert.True(header.IsStepSet(5));
        Assert.False(header.IsStepSet(4));

        header.BitmaskWord1 |= (1UL << 10); // Step 64 + 10 = 74 set
        Assert.True(header.IsStepSet(74));
    }
}
