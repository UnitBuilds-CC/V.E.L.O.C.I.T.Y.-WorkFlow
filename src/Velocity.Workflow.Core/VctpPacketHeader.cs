using System.Runtime.InteropServices;

namespace Velocity.Workflow.Core;

/// <summary>
/// Memory-aligned 32-byte blittable structure matching the Rust VctpPacketHeader #[repr(C)] layout.
/// Enables zero-copy memory transport for inter-node slab delta synchronization.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 32)]
public unsafe struct VctpPacketHeader
{
    [FieldOffset(0)]  public uint Magic;              // 4 Bytes: "VCTP" (0x50544356)
    // 4 Bytes padding for 8-byte ulong alignment
    [FieldOffset(8)]  public ulong SequenceNumber;    // 8 Bytes: Monotonic packet sequence ID
    [FieldOffset(16)] public ulong WorkflowId;        // 8 Bytes: Associated workflow ID
    [FieldOffset(24)] public uint SlabOffset;         // 4 Bytes: Byte offset in target slab
    [FieldOffset(28)] public uint PayloadLength;      // 4 Bytes: Length of bitmask or slab delta payload

    public readonly bool IsValid => Magic == 0x50544356;
}
