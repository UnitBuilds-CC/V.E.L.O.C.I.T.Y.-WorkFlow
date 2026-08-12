using System;
using Xunit;
using static Velocity.Workflow.Core.NativeBridge;

namespace Velocity.Workflow.Core.Tests;

/// <summary>
/// Batch 39 feature tests — Hot-Swap, Slab Visualization, and SlabVisualizer utility.
/// </summary>
public unsafe class Batch39FeatureTests
{
    // ─── Hot-Swap Tests ──────────────────────────────────────────────────

    [Fact]
    public void HotSwap_Register_ReturnsNonZero()
    {
        var desc = "test patch"u8;
        fixed (byte* p = desc)
        {
            var patchId = HotSwapRegister(100, p, (uint)desc.Length, 2, 42);
            Assert.NotEqual(0UL, patchId);
        }
    }

    [Fact]
    public void HotSwap_Apply_ReturnsSuccess()
    {
        var desc = "apply test"u8;
        fixed (byte* p = desc)
        {
            var patchId = HotSwapRegister(200, p, (uint)desc.Length, 1, 10);
            var result = HotSwapApply(patchId, 1001);
            Assert.Equal(1U, result);
        }
    }

    [Fact]
    public void HotSwap_Rollback_ReturnsSuccess()
    {
        var desc = "rollback test"u8;
        fixed (byte* p = desc)
        {
            var patchId = HotSwapRegister(300, p, (uint)desc.Length, 1, 10);
            HotSwapApply(patchId, 2001);
            var result = HotSwapRollback(2001);
            Assert.Equal(1U, result);
        }
    }

    [Fact]
    public void HotSwap_PatchCount_Increases()
    {
        var before = HotSwapPatchCount();
        var desc = "count test"u8;
        fixed (byte* p = desc)
        {
            HotSwapRegister(400, p, (uint)desc.Length, 0, 1);
        }
        var after = HotSwapPatchCount();
        Assert.True(after > before);
    }

    [Fact]
    public void HotSwap_LatestVersion_TracksVersions()
    {
        var desc1 = "v1"u8;
        var desc2 = "v2"u8;
        fixed (byte* p1 = desc1)
        fixed (byte* p2 = desc2)
        {
            HotSwapRegister(500, p1, (uint)desc1.Length, 0, 1);
            var v1 = HotSwapLatestVersion(500);
            HotSwapRegister(500, p2, (uint)desc2.Length, 1, 2);
            var v2 = HotSwapLatestVersion(500);
            Assert.True(v2 > v1);
        }
    }

    [Fact]
    public void HotSwap_PatchedWorkflowCount_NonNegative()
    {
        var count = HotSwapPatchedWorkflowCount();
        Assert.True(count >= 0);
    }

    // ─── Slab Visualization Tests ────────────────────────────────────────

    [Fact]
    public void SlabHeaderSize_Returns128()
    {
        var size = SlabHeaderSize();
        Assert.Equal(128U, size);
    }

    [Fact]
    public void SlabCount_NullEngine_ReturnsZero()
    {
        var count = SlabCount(null);
        Assert.Equal(0U, count);
    }

    [Fact]
    public void SlabCount_WithEngine_ReturnsNonNegative()
    {
        var engine = VelocityEngineCreate();
        Assert.False(engine == null);
        var count = SlabCount(engine);
        Assert.True(count >= 0);
        VelocityEngineDestroy(engine);
    }

    [Fact]
    public void SlabVerifyMerkle_NullEngine_ReturnsZero()
    {
        var result = SlabVerifyMerkle(null, 1234);
        Assert.Equal(0U, result);
    }

    [Fact]
    public void SlabVerifyMerkle_ValidWorkflow_ReturnsValid()
    {
        var engine = VelocityEngineCreate();
        var key = VelocityEngineStartWorkflow(engine, 600, 1, 0, 99, 3, null, 0);
        Assert.True(key > 0);

        // Complete all steps to make the slab valid
        for (uint step = 0; step < 3; step++)
        {
            byte[] data = { (byte)step };
            fixed (byte* p = data)
            {
                VelocityEngineCompleteStep(engine, key, step, p, 1);
            }
        }

        var result = SlabVerifyMerkle(engine, key);
        Assert.Equal(1U, result);
        VelocityEngineDestroy(engine);
    }

    // ─── SlabVisualizer Utility Tests ────────────────────────────────────

    [Fact]
    public void FormatBitmask_EmptyBits_ReturnsPlaceholder()
    {
        var result = SlabVisualizer.FormatBitmask(null!);
        Assert.Equal("0000...0000", result);
    }

    [Fact]
    public void FormatBitmask_SingleBit_ReturnsBinary()
    {
        var bits = new ulong[] { 1, 0, 0, 0 };
        var result = SlabVisualizer.FormatBitmask(bits);
        Assert.Contains("1", result);
        Assert.Equal(64 * 4 + 3, result.Length); // 256 bits + 3 spaces
    }

    [Fact]
    public void FormatHex_EmptyBytes_ReturnsEmpty()
    {
        var result = SlabVisualizer.FormatHex(Array.Empty<byte>());
        Assert.Equal("(empty)", result);
    }

    [Fact]
    public void FormatHex_SomeBytes_ReturnsHex()
    {
        var bytes = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var result = SlabVisualizer.FormatHex(bytes);
        Assert.Equal("de ad be ef", result);
    }

    [Fact]
    public void FormatHex_Truncates()
    {
        var bytes = new byte[32];
        for (int i = 0; i < 32; i++) bytes[i] = (byte)i;
        var result = SlabVisualizer.FormatHex(bytes, 4);
        Assert.Contains("...", result);
    }

    [Fact]
    public void CountSetBits_AllZero_ReturnsZero()
    {
        var bits = new ulong[] { 0, 0, 0, 0 };
        Assert.Equal(0U, SlabVisualizer.CountSetBits(bits));
    }

    [Fact]
    public void CountSetBits_SomeBits_ReturnsCorrect()
    {
        var bits = new ulong[] { 0xFF, 0, 0, 0 };
        Assert.Equal(8U, SlabVisualizer.CountSetBits(bits));
    }

    [Fact]
    public void CountSetBits_NullBits_ReturnsZero()
    {
        Assert.Equal(0U, SlabVisualizer.CountSetBits(null!));
    }

    [Fact]
    public void IsValidMerkleHex_ValidHex_ReturnsTrue()
    {
        var hex = new string('a', 64);
        Assert.True(SlabVisualizer.IsValidMerkleHex(hex));
    }

    [Fact]
    public void IsValidMerkleHex_WrongLength_ReturnsFalse()
    {
        Assert.False(SlabVisualizer.IsValidMerkleHex("abc"));
    }

    [Fact]
    public void IsValidMerkleHex_EmptyString_ReturnsFalse()
    {
        Assert.False(SlabVisualizer.IsValidMerkleHex(""));
    }

    [Fact]
    public void IsValidMerkleHex_InvalidChars_ReturnsFalse()
    {
        var hex = new string('z', 64);
        Assert.False(SlabVisualizer.IsValidMerkleHex(hex));
    }

    [Fact]
    public void FormatSlab_ProducesOutput()
    {
        var slab = new SlabVisualizer.SlabSnapshot(
            WorkflowKey: 12345,
            Magic: SlabVisualizer.MagicVLCT,
            SchemaVersion: 1,
            WorkflowId: 100,
            RunId: 200,
            CurrentStep: 3,
            TotalSteps: 5,
            MerkleRoot: new byte[32],
            BitmaskBits: new ulong[] { 7, 0, 0, 0 },
            CompletedSteps: 3,
            IsValid: true,
            MerkleValid: true
        );
        var output = SlabVisualizer.FormatSlab(slab);
        Assert.Contains("SLAB HEADER", output);
        Assert.Contains("VALID", output);
        Assert.Contains("12345", output);
    }

    [Fact]
    public void FormatSummary_ProducesOutput()
    {
        var summary = new SlabVisualizer.SlabSummary(
            TotalSlabs: 10,
            ValidSlabs: 9,
            InvalidSlabs: 1,
            MerkleValidCount: 8,
            MerkleInvalidCount: 2,
            TotalWorkflows: 10,
            TotalStepsCompleted: 42,
            CompletionRate: 0.9
        );
        var output = SlabVisualizer.FormatSummary(summary);
        Assert.Contains("VELOCITY Slab Summary", output);
        Assert.Contains("10", output);
    }
}
