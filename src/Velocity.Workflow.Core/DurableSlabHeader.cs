using System;
using System.Runtime.InteropServices;

namespace Velocity.Workflow.Core;

/// <summary>
/// Memory-aligned 128-byte blittable structure matching the Rust SlabHeader layout.
/// Enables O(1) zero-copy state restoration and sub-nanosecond direct pointer operations.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 128)]
public unsafe struct DurableSlabHeader
{
    [FieldOffset(0)]  public uint Magic;               // 4 Bytes: "VLCT" (0x564C4354)
    [FieldOffset(4)]  public uint SchemaVersion;      // 4 Bytes: Version ID
    [FieldOffset(8)]  public ulong WorkflowId;        // 8 Bytes: Unique workflow ID
    [FieldOffset(16)] public ulong RunId;             // 8 Bytes: Unique run ID
    [FieldOffset(24)] public uint CurrentStep;        // 4 Bytes: Current step index
    [FieldOffset(28)] public uint TotalSteps;         // 4 Bytes: Total planned steps
    
    // 32 Bytes: Cryptographic SHA-256 Merkle root
    [FieldOffset(32)] public fixed byte MerkleRoot[32];

    // 32 Bytes: Bitmask256 completion vector (4x ulong)
    [FieldOffset(64)] public ulong BitmaskWord0;
    [FieldOffset(72)] public ulong BitmaskWord1;
    [FieldOffset(80)] public ulong BitmaskWord2;
    [FieldOffset(88)] public ulong BitmaskWord3;

    // 32 Bytes: Reserved slot padding for backward-compatible binary schema updates
    [FieldOffset(96)] public fixed byte ReservedPadding[32];

    public readonly bool IsValid => Magic == 0x564C4354;

    public readonly bool IsStepSet(int stepIndex)
    {
        if (stepIndex < 0 || stepIndex >= 256) return false;
        int word = stepIndex / 64;
        int bit = stepIndex % 64;
        ulong mask = 1UL << bit;

        return word switch
        {
            0 => (BitmaskWord0 & mask) != 0,
            1 => (BitmaskWord1 & mask) != 0,
            2 => (BitmaskWord2 & mask) != 0,
            3 => (BitmaskWord3 & mask) != 0,
            _ => false
        };
    }
}
