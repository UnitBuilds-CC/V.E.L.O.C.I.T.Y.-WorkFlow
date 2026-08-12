using System;
using Xunit;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Core.Tests;

/// <summary>
/// Tests for Batch 37 features: Hardware Abstraction Layer (HAL) integration,
/// Merkle ECC self-healing loop, SmartNIC offload, TEE protection,
/// and AST-based transpiler.
/// </summary>
public class Batch37FeatureTests
{
    // --- HAL Init & Status ---

    [Fact]
    public void Hal_Init()
    {
        Assert.True(NativeBridge.HalInit());
    }

    [Fact]
    public void Hal_IsEccEnabled()
    {
        NativeBridge.HalInit();
        Assert.True(NativeBridge.HalIsEccEnabled());
    }

    [Fact]
    public void Hal_IsNicEnabled()
    {
        NativeBridge.HalInit();
        Assert.True(NativeBridge.HalIsNicEnabled());
    }

    [Fact]
    public void Hal_IsTeeEnabled()
    {
        NativeBridge.HalInit();
        Assert.True(NativeBridge.HalIsTeeEnabled());
    }

    // --- Slab Write/Read ---

    [Fact]
    public unsafe void Hal_SlabWriteReturnsParityLength()
    {
        NativeBridge.HalInit();
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        var merkleRoot = new byte[32];

        fixed (byte* d = data)
        fixed (byte* m = merkleRoot)
        {
            var parityLen = NativeBridge.HalOnSlabWrite(1001, d, (uint)data.Length, m);
            Assert.True(parityLen > 0); // ECC parity should be computed
        }
    }

    [Fact]
    public unsafe void Hal_SlabReadReturnsValid()
    {
        NativeBridge.HalInit();
        var data = new byte[] { 10, 20, 30, 40, 50, 60, 70, 80 };
        var merkleRoot = new byte[32];

        fixed (byte* d = data)
        fixed (byte* m = merkleRoot)
        {
            // Write first to establish parity
            NativeBridge.HalOnSlabWrite(1002, d, (uint)data.Length, m);

            // Read should return 0 (Valid)
            var result = NativeBridge.HalOnSlabRead(1002, d, (uint)data.Length, m);
            Assert.Equal(0u, result); // 0 = Valid
        }
    }

    // --- Merkle ECC Self-Healing ---

    [Fact]
    public unsafe void Hal_MerkLEccSelfHeal()
    {
        NativeBridge.HalInit();
        var data = new byte[] { 42, 43, 44, 45, 46, 47, 48, 49 };

        fixed (byte* d = data)
        {
            // First call stores Merkle root + parity
            var result = NativeBridge.HalMerkleEccSelfHeal(1003, d, (uint)data.Length);
            Assert.Equal(0u, result); // 0 = Valid

            // Second call should also be valid
            result = NativeBridge.HalMerkleEccSelfHeal(1003, d, (uint)data.Length);
            Assert.Equal(0u, result); // 0 = Valid
        }
    }

    // --- Statistics ---

    [Fact]
    public void Hal_Statistics()
    {
        NativeBridge.HalInit();
        // These should not throw
        var writes = NativeBridge.HalSlabWriteCount();
        var reads = NativeBridge.HalSlabReadCount();
        var eccVerifications = NativeBridge.HalEccVerifications();
        var eccRepairs = NativeBridge.HalEccRepairs();
        var nicOffloads = NativeBridge.HalNicOffloadCount();
        var teeEnclaves = NativeBridge.HalTeeEnclaveCount();

        // Sanity: counts should be reasonable
        Assert.True(writes < 1_000_000);
        Assert.True(reads < 1_000_000);
    }

    // --- Cleanup ---

    [Fact]
    public void Hal_CleanupWorkflow()
    {
        NativeBridge.HalInit();
        // Should not throw
        NativeBridge.HalCleanupWorkflow(9999);
    }

    // --- Merkle Root Computation ---

    [Fact]
    public unsafe void Hal_ComputeMerkleRoot_Deterministic()
    {
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        var root1 = new byte[32];
        var root2 = new byte[32];

        fixed (byte* d = data)
        fixed (byte* r1 = root1)
        fixed (byte* r2 = root2)
        {
            NativeBridge.HalComputeMerkleRoot(d, (uint)data.Length, r1);
            NativeBridge.HalComputeMerkleRoot(d, (uint)data.Length, r2);
        }

        // Same data → same Merkle root
        Assert.Equal(root1, root2);

        // Different data → different root
        var data2 = new byte[] { 99, 98, 97, 96, 95, 94, 93, 92 };
        var root3 = new byte[32];
        fixed (byte* d = data2)
        fixed (byte* r3 = root3)
        {
            NativeBridge.HalComputeMerkleRoot(d, (uint)data2.Length, r3);
        }
        Assert.NotEqual(root1, root3);
    }

    // --- AST Transpiler ---

    [Fact]
    public void AstTranspiler_UsingRewrite()
    {
        var source = @"using Temporalio.Client;
using Temporalio.Workflows;

namespace Test;

public class MyWorkflow { }";

        var result = temporal2velocity.AstTranspilerEngine.Transpile(source, out var stats);
        Assert.Contains("Velocity.Workflow.Core", result);
        Assert.True(stats.UsingDirectivesRewritten > 0);
    }

    [Fact]
    public void AstTranspiler_MemberAccessRewrite()
    {
        var source = @"namespace Test;
public class MyWorkflow
{
    public void Run()
    {
        var now = DateTime.UtcNow;
        var id = Guid.NewGuid();
    }
}";

        var result = temporal2velocity.AstTranspilerEngine.Transpile(source, out var stats);
        Assert.Contains("WorkflowClock.UtcNow", result);
        Assert.Contains("WorkflowGuid.NewGuid", result);
        Assert.True(stats.MemberAccesses_Rewritten >= 2);
    }

    [Fact]
    public void AstTranspiler_ObjectCreationRewrite()
    {
        var source = @"namespace Test;
public class MyWorkflow
{
    public void Run()
    {
        var rng = new Random();
    }
}";

        var result = temporal2velocity.AstTranspilerEngine.Transpile(source, out var stats);
        Assert.Contains("WorkflowRandom", result);
        Assert.True(stats.ObjectCreations_Rewritten >= 1);
    }

    [Fact]
    public void AstTranspiler_DurableAttributeInjection()
    {
        var source = @"namespace Test;
public class MyWorkflow
{
    public async Task RunAsync()
    {
        await Task.Delay(100);
    }
}";

        var result = temporal2velocity.AstTranspilerEngine.Transpile(source, out var stats);
        Assert.Contains("[DurableWorkflow]", result);
        Assert.True(stats.Attributes_Injected >= 1);
    }

    [Fact]
    public void AstTranspiler_EmptyInput()
    {
        var result = temporal2velocity.AstTranspilerEngine.Transpile("", out var stats);
        Assert.Equal("", result);
        Assert.Equal(0, stats.TotalReplacements);
    }

    [Fact]
    public void AstTranspiler_NoFalseMatchesInStrings()
    {
        // AST-based transpiler should NOT rewrite inside string literals
        var source = @"namespace Test;
public class MyWorkflow
{
    public void Run()
    {
        var s = ""DateTime.UtcNow is non-deterministic"";
    }
}";

        var result = temporal2velocity.AstTranspilerEngine.Transpile(source, out var stats);
        // The string literal should be preserved as-is
        Assert.Contains("DateTime.UtcNow is non-deterministic", result);
    }
}
