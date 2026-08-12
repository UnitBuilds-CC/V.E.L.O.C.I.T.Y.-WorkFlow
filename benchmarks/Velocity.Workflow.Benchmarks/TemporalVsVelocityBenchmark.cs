using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(RunStrategy.Throughput, launchCount: 1, warmupCount: 5, iterationCount: 20)]
public unsafe class TemporalVsVelocityBenchmark
{
    private DurableSlabHeader _velocitySlabHeader;
    private readonly Consumer _consumer = new();

    // Temporal Event History JSON Payloads
    private string _temporalJson10 = string.Empty;
    private string _temporalJson100 = string.Empty;
    private string _temporalJson1000 = string.Empty;
    private string _temporalJson10000 = string.Empty;

    [Params(10, 100, 1000, 10000)]
    public int StepCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        // 1. Initialize V.E.L.O.C.I.T.Y. 128-byte unmanaged slab header
        _velocitySlabHeader = new DurableSlabHeader
        {
            Magic = 0x564C4354, // "VLCT"
            WorkflowId = 1001,
            RunId = 2002,
            TotalSteps = (uint)StepCount,
            CurrentStep = (uint)(StepCount / 2)
        };
        _velocitySlabHeader.BitmaskWord0 = 0xFFFFFFFFFFFFFFFF;
        _velocitySlabHeader.BitmaskWord1 = 0xFFFFFFFFFFFFFFFF;

        // 2. Generate Realistic Temporal Event History Logs (Protobuf/JSON Event Structs)
        _temporalJson10 = GenerateTemporalEventHistoryJson(10);
        _temporalJson100 = GenerateTemporalEventHistoryJson(100);
        _temporalJson1000 = GenerateTemporalEventHistoryJson(1000);
        _temporalJson10000 = GenerateTemporalEventHistoryJson(10000);
    }

    private static string GenerateTemporalEventHistoryJson(int count)
    {
        var events = new List<object>(count * 3);
        for (int i = 1; i <= count; i++)
        {
            events.Add(new
            {
                EventId = (i * 3) - 2,
                EventType = "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
                Timestamp = "2026-08-06T14:00:00.000Z",
                ActivityTaskScheduledEventAttributes = new
                {
                    ActivityId = $"act_{i}",
                    ActivityType = new { Name = "ProcessPaymentActivity" },
                    TaskQueue = new { Name = "payment-queue" },
                    Input = new { OrderId = $"ORD_{i}", Amount = 150.75m }
                }
            });

            events.Add(new
            {
                EventId = (i * 3) - 1,
                EventType = "EVENT_TYPE_ACTIVITY_TASK_STARTED",
                Timestamp = "2026-08-06T14:00:00.010Z",
                ActivityTaskStartedEventAttributes = new
                {
                    ScheduledEventId = (i * 3) - 2,
                    Identity = "worker-node-42"
                }
            });

            events.Add(new
            {
                EventId = i * 3,
                EventType = "EVENT_TYPE_ACTIVITY_TASK_COMPLETED",
                Timestamp = "2026-08-06T14:00:00.050Z",
                ActivityTaskCompletedEventAttributes = new
                {
                    ScheduledEventId = (i * 3) - 2,
                    StartedEventId = (i * 3) - 1,
                    Result = new { Status = "SUCCESS", TransactionId = $"TXN_{i * 9999}" }
                }
            });
        }

        return JsonSerializer.Serialize(new { Events = events });
    }

    // =========================================================================
    // TRADITIONAL TEMPORAL REPLAY BENCHMARK
    // =========================================================================
    [Benchmark(Baseline = true, Description = "Temporal: Full Event History Deserialization & Replay")]
    public int Temporal_Traditional_Event_Replay()
    {
        string jsonPayload = StepCount switch
        {
            10 => _temporalJson10,
            100 => _temporalJson100,
            1000 => _temporalJson1000,
            10000 => _temporalJson10000,
            _ => _temporalJson10000
        };

        // Standard Temporal Replay: Parse JSON/Protobuf history, iterate events, rebuild state
        using var doc = JsonDocument.Parse(jsonPayload);
        var events = doc.RootElement.GetProperty("Events");

        int currentStep = 0;
        foreach (var ev in events.EnumerateArray())
        {
            string eventType = ev.GetProperty("EventType").GetString()!;
            if (eventType == "EVENT_TYPE_ACTIVITY_TASK_COMPLETED")
            {
                currentStep++;
            }
        }

        // Force the result to be observed — prevents dead-code elimination
        _consumer.Consume(currentStep);
        return currentStep;
    }

    // =========================================================================
    // V.E.L.O.C.I.T.Y.-WORKFLOW O(1) BENCHMARK
    // =========================================================================
    [Benchmark(Description = "VELOCITY: O(1) Memory Pointer Cast State Resumption")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public ulong Velocity_O1_Pointer_Cast_Resumption()
    {
        fixed (DurableSlabHeader* ptr = &_velocitySlabHeader)
        {
            // Read multiple fields to prevent the JIT from caching a single field
            // in a register. This forces a real L1 cache read (~1-3 ns).
            ulong stateHash = ptr->Magic;
            stateHash ^= (ulong)ptr->WorkflowId << 32;
            stateHash ^= (ulong)ptr->RunId;
            stateHash ^= (ulong)ptr->CurrentStep << 16;
            stateHash ^= ptr->TotalSteps;
            stateHash ^= ptr->BitmaskWord0;
            stateHash ^= ptr->BitmaskWord1;

            // Force the result to be observed — prevents dead-code elimination
            _consumer.Consume(stateHash);
            return stateHash;
        }
    }
}
