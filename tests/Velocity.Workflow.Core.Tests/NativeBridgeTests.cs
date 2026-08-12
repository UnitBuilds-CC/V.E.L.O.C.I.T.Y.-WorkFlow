using System;
using System.Runtime.InteropServices;
using Velocity.Workflow.Core;
using Xunit;

namespace Velocity.Workflow.Core.Tests;

public unsafe class NativeBridgeTests
{
    [Fact]
    public void NativeBridge_VelocitySlabCreate_And_Verify_Works()
    {
        var header = new DurableSlabHeader();
        int status = NativeBridge.VelocitySlabCreate(999, 888, 10, &header);

        Assert.Equal(0, status);
        Assert.Equal(999UL, header.WorkflowId);
        Assert.Equal(888UL, header.RunId);
        Assert.Equal(10U, header.TotalSteps);
        Assert.True(header.IsValid);

        // Verify Merkle Root calculation via Rust engine
        int isVerified = NativeBridge.VelocitySlabVerify(&header);
        Assert.Equal(1, isVerified);

        // Mark Step 0 completed via Rust engine
        int markResult = NativeBridge.VelocitySlabMarkStep(&header, 0);
        Assert.Equal(0, markResult);
        Assert.Equal(1U, header.CurrentStep);
        Assert.True(header.IsStepSet(0));

        // Re-verify Merkle Root after step modification
        isVerified = NativeBridge.VelocitySlabVerify(&header);
        Assert.Equal(1, isVerified);
    }
}
