using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Benchmarks;

[MemoryDiagnoser]
public unsafe class SlabVsReplayBenchmark
{
    private DurableSlabHeader _slabHeader;
    private string _jsonEventHistory = string.Empty;

    [GlobalSetup]
    public void Setup()
    {
        _slabHeader = new DurableSlabHeader
        {
            Magic = 0x564C4354,
            WorkflowId = 1001,
            RunId = 2002,
            TotalSteps = 1000,
            CurrentStep = 500
        };
        _slabHeader.BitmaskWord0 = 0xFFFFFFFFFFFFFFFF;
        _slabHeader.BitmaskWord1 = 0xFFFFFFFFFFFFFFFF;

        // Simulate 500 JSON events history payload
        var events = new List<object>();
        for (int i = 0; i < 500; i++)
        {
            events.Add(new { StepIndex = i, Timestamp = DateTime.UtcNow, ResultPayload = $"StepResult_{i}" });
        }
        _jsonEventHistory = JsonSerializer.Serialize(events);
    }

    [Benchmark(Baseline = true)]
    public int Temporal_Simulated_Event_Replay_Loop()
    {
        // Simulate reading and deserializing 500 JSON events to rebuild local variables
        var events = JsonSerializer.Deserialize<List<JsonElement>>(_jsonEventHistory);
        int currentStep = 0;
        foreach (var ev in events!)
        {
            currentStep = ev.GetProperty("StepIndex").GetInt32() + 1;
        }
        return currentStep;
    }

    [Benchmark]
    public int Velocity_O1_Slab_Pointer_Cast_Resumption()
    {
        fixed (DurableSlabHeader* ptr = &_slabHeader)
        {
            // O(1) instantaneous memory pointer cast - 0 bytes allocation
            return (int)ptr->CurrentStep;
        }
    }
}
