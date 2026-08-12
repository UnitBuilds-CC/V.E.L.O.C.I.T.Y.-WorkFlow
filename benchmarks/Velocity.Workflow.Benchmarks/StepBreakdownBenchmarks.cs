using System;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Benchmarks;

[MemoryDiagnoser]
[InProcess]
public unsafe class StepBreakdownBenchmarks
{
    private DurableSlabHeader _slabHeader;
    private NdaHeader _ndaHeader;
    private VctpPacketHeader _vctpHeader;
    private void* _arenaPtr;
    private byte[] _payload;

    [GlobalSetup]
    public void Setup()
    {
        _slabHeader = new DurableSlabHeader
        {
            Magic = 0x564C4354,
            WorkflowId = 1001,
            RunId = 2002,
            TotalSteps = 100,
            CurrentStep = 50
        };

        _ndaHeader = new NdaHeader
        {
            Magic = 0x3141444E,
            TripleCount = 10,
            CommandCount = 5,
            StringPoolOffset = 64
        };

        _vctpHeader = new VctpPacketHeader
        {
            Magic = 0x50544356,
            SequenceNumber = 100,
            WorkflowId = 1001,
            SlabOffset = 128,
            PayloadLength = 64
        };

        _payload = System.Text.Encoding.UTF8.GetBytes("Dynamic_Overflow_Payload_Data_Buffer_Tier2");
        _arenaPtr = NativeMemory.AllocZeroed(65536);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (_arenaPtr != null)
        {
            NativeMemory.Free(_arenaPtr);
        }
    }

    [Benchmark(Description = "Step 1: Slab Creation & Merkle Hash (Rust FFI)")]
    public int Step1_Slab_Create_And_Merkle_Hash()
    {
        fixed (DurableSlabHeader* ptr = &_slabHeader)
        {
            return NativeBridge.VelocitySlabCreate(999, 888, 100, ptr);
        }
    }

    [Benchmark(Description = "Step 2: Bitmask Step Mark & Transition (Rust FFI)")]
    public int Step2_Bitmask_Step_Mark()
    {
        fixed (DurableSlabHeader* ptr = &_slabHeader)
        {
            return NativeBridge.VelocitySlabMarkStep(ptr, 51);
        }
    }

    [Benchmark(Description = "Step 3: Merkle Root SHA-256 Verification (Rust FFI)")]
    public int Step3_Merkle_Root_Verification()
    {
        fixed (DurableSlabHeader* ptr = &_slabHeader)
        {
            return NativeBridge.VelocitySlabVerify(ptr);
        }
    }

    [Benchmark(Description = "Step 4: NDA Binary Document Proof Verification")]
    public int Step4_Nda_Binary_Document_Verification()
    {
        fixed (NdaHeader* ptr = &_ndaHeader)
        {
            return NativeBridge.VelocityNdaVerify(ptr);
        }
    }

    [Benchmark(Description = "Step 5: VCTP Packet Header Construction")]
    public int Step5_Vctp_Packet_Construction()
    {
        fixed (VctpPacketHeader* ptr = &_vctpHeader)
        {
            return NativeBridge.VelocityVctpPacketCreate(101, 1001, 128, 64, ptr);
        }
    }

    [Benchmark(Description = "Step 6: Tier-2 Bump Arena Payload Allocation")]
    public int Step6_Tier2_Bump_Arena_Alloc()
    {
        nuint outOffset = 0;
        fixed (byte* pPtr = _payload)
        {
            return NativeBridge.VelocityArenaAlloc(_arenaPtr, pPtr, (nuint)_payload.Length, &outOffset);
        }
    }

    [Benchmark(Description = "Step 7: O(1) Direct Memory Pointer Resumption")]
    public ulong Step7_O1_Direct_Memory_Pointer_Resumption()
    {
        fixed (DurableSlabHeader* ptr = &_slabHeader)
        {
            return ptr->WorkflowId + ptr->CurrentStep;
        }
    }
}
