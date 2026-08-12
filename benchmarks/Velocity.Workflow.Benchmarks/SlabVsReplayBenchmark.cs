using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(RunStrategy.Throughput, launchCount: 1, warmupCount: 3, iterationCount: 10)]
public unsafe class SlabVsReplayBenchmark
{
    private DurableSlabHeader _slabHeader;
    private string _jsonEventHistory10 = string.Empty;
    private string _jsonEventHistory100 = string.Empty;
    private string _jsonEventHistory1000 = string.Empty;

    [Params(10, 100, 1000)]
    public int StepCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _slabHeader = new DurableSlabHeader
        {
            Magic = 0x564C4354, // "VLCT"
            WorkflowId = 1001,
            RunId = 2002,
            TotalSteps = (uint)StepCount,
            CurrentStep = (uint)(StepCount / 2)
        };
        _slabHeader.BitmaskWord0 = 0xFFFFFFFFFFFFFFFF;
        _slabHeader.BitmaskWord1 = 0xFFFFFFFFFFFFFFFF;

        _jsonEventHistory10 = GenerateJsonHistory(10);
        _jsonEventHistory100 = GenerateJsonHistory(100);
        _jsonEventHistory1000 = GenerateJsonHistory(1000);
    }

    private static string GenerateJsonHistory(int count)
    {
        var events = new List<object>(count);
        for (int i = 0; i < count; i++)
        {
            events.Add(new
            {
                EventId = i + 1,
                EventType = "ActivityTaskCompleted",
                Timestamp = 1754480000000 + i,
                ResultPayload = $"StepResult_Data_Payload_Offset_{i}"
            });
        }
        return JsonSerializer.Serialize(events);
    }

    [Benchmark(Baseline = true)]
    public int Temporal_Simulated_Event_Replay_Loop()
    {
        string json = StepCount switch
        {
            10 => _jsonEventHistory10,
            100 => _jsonEventHistory100,
            1000 => _jsonEventHistory1000,
            _ => _jsonEventHistory1000
        };

        // Simulate reading and deserializing N JSON events to rebuild local variables
        var events = JsonSerializer.Deserialize<List<JsonElement>>(json);
        int currentStep = 0;
        foreach (var ev in events!)
        {
            currentStep = ev.GetProperty("EventId").GetInt32();
        }
        return currentStep;
    }

    [Benchmark]
    public int Velocity_O1_Slab_Pointer_Cast_Resumption()
    {
        fixed (DurableSlabHeader* ptr = &_slabHeader)
        {
            // O(1) instantaneous memory pointer cast - 0 bytes allocation regardless of N steps
            return (int)ptr->CurrentStep;
        }
    }
}
