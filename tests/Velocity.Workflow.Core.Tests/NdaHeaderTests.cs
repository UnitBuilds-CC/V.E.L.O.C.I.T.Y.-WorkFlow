using Velocity.Workflow.Core;
using Xunit;

namespace Velocity.Workflow.Core.Tests;

public unsafe class NdaHeaderTests
{
    [Fact]
    public void NdaHeader_Size_Must_Be_Exactly_48_Bytes()
    {
        int size = sizeof(NdaHeader);
        Assert.Equal(48, size);
    }

    [Fact]
    public void NdaHeader_Magic_And_Validation_Check()
    {
        var header = new NdaHeader
        {
            Magic = 0x3141444E, // "NDA1"
            TripleCount = 5,
            CommandCount = 2,
            StringPoolOffset = 64
        };

        Assert.True(header.IsValid);
    }
}
