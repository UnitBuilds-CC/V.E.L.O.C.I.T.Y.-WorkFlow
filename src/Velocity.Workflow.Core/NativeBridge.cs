using System;
using System.Runtime.InteropServices;

namespace Velocity.Workflow.Core;

/// <summary>
/// Direct unmanaged P/Invoke C-ABI bridge to the Rust velocity_workflow_core FFI library.
/// Executes slab operations, Merkle root verification, and bitmask tracking with zero GC allocation.
/// </summary>
public static unsafe partial class NativeBridge
{
    private const string DllName = "velocity_workflow_core";

    [LibraryImport(DllName, EntryPoint = "velocity_slab_create")]
    public static partial int VelocitySlabCreate(ulong workflowId, ulong runId, uint totalSteps, DurableSlabHeader* outHeader);

    [LibraryImport(DllName, EntryPoint = "velocity_slab_mark_step")]
    public static partial int VelocitySlabMarkStep(DurableSlabHeader* header, uint stepIndex);

    [LibraryImport(DllName, EntryPoint = "velocity_slab_verify")]
    public static partial int VelocitySlabVerify(DurableSlabHeader* header);

    [LibraryImport(DllName, EntryPoint = "velocity_slab_merge_crdt")]
    public static partial int VelocitySlabMergeCrdt(void* targetCounter, void* sourceCounter);

    [LibraryImport(DllName, EntryPoint = "velocity_nda_verify")]
    public static partial int VelocityNdaVerify(NdaHeader* header);

    [LibraryImport(DllName, EntryPoint = "velocity_arena_alloc")]
    public static partial int VelocityArenaAlloc(void* arenaPage, byte* payloadPtr, nuint payloadLen, nuint* outOffset);

    [LibraryImport(DllName, EntryPoint = "velocity_vctp_packet_create")]
    public static partial int VelocityVctpPacketCreate(ulong sequenceNumber, ulong workflowId, uint slabOffset, uint payloadLength, VctpPacketHeader* outHeader);
}
