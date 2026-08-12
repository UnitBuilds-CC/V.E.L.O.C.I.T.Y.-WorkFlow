using Velocity.Workflow.Core;
using Xunit;

namespace Velocity.Workflow.Core.Tests;

public unsafe class VctpHeaderTests
{
    [Fact]
    public void VctpPacketHeader_Size_Must_Be_Exactly_32_Bytes()
    {
        int size = sizeof(VctpPacketHeader);
        Assert.Equal(32, size);
    }

    [Fact]
    public void VctpPacketHeader_Native_Create_Works()
    {
        var header = new VctpPacketHeader();
        int status = NativeBridge.VelocityVctpPacketCreate(42, 999, 128, 64, &header);

        Assert.Equal(0, status);
        Assert.True(header.IsValid);
        Assert.Equal(42UL, header.SequenceNumber);
        Assert.Equal(999UL, header.WorkflowId);
        Assert.Equal(128U, header.SlabOffset);
        Assert.Equal(64U, header.PayloadLength);
    }
}
