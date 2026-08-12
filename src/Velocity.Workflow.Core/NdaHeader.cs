using System.Runtime.InteropServices;

namespace Velocity.Workflow.Core;

/// <summary>
/// Memory-aligned 48-byte blittable structure matching the Neural Document Architecture (NDA) header.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 48)]
public unsafe struct NdaHeader
{
    [FieldOffset(0)]  public uint Magic;              // 4 Bytes: "NDA1" (0x3141444E)
    [FieldOffset(4)]  public uint Flags;              // 4 Bytes: Config / bitmask flags
    
    // 32 Bytes: Cryptographic SHA-256 Merkle root
    [FieldOffset(8)]  public fixed byte MerkleRoot[32];

    [FieldOffset(40)] public uint TripleCount;        // 4 Bytes: Semantic triples count
    [FieldOffset(44)] public ushort CommandCount;     // 2 Bytes: Canvas command count
    [FieldOffset(46)] public ushort StringPoolOffset; // 2 Bytes: Offset to string pool

    public readonly bool IsValid => Magic == 0x3141444E;
}
