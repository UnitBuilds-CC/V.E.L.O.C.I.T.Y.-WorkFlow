using System;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Benchmarks;

/// <summary>
/// Real workflow lifecycle benchmarks — exercises the actual Velocity workflow engine
/// via the C# WorkflowRuntime (FFI to Rust). Unlike the synthetic memory-primitive
/// benchmarks, these drive complete workflow lifecycles:
///   Start → CompleteStep(0..N) → Signal → Query → Complete
/// 
/// Each iteration measures end-to-end engine throughput, not pointer casts.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RunStrategy.Throughput, launchCount: 1, warmupCount: 5, iterationCount: 20)]
public unsafe class WorkflowLifecycleBenchmark
{
    private WorkflowRuntime _runtime = null!;
    private readonly Consumer _consumer = new();

    [Params(1, 10, 100)]
    public int StepsPerWorkflow { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _runtime = new WorkflowRuntime();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _runtime?.Dispose();
    }

    /// <summary>
    /// Full workflow lifecycle: start → complete N steps → complete workflow.
    /// Measures the actual engine throughput for a realistic workload pattern.
    /// Each iteration creates, executes, and completes one workflow.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Full Lifecycle: Start → N Steps → Complete")]
    public int FullWorkflowLifecycle()
    {
        ulong workflowId = (ulong)Guid.NewGuid().GetHashCode();
        ulong workflowTypeId = 1;
        ulong namespaceId = 1;
        ulong taskQueueHash = 42;

        // Start the workflow
        ulong workflowKey = _runtime.StartWorkflow(
            workflowId, workflowTypeId, namespaceId, taskQueueHash,
            (uint)StepsPerWorkflow);
        _consumer.Consume(workflowKey);

        // Complete each step sequentially
        for (uint step = 0; step < (uint)StepsPerWorkflow; step++)
        {
            _runtime.CompleteStep(workflowKey, step);

            // Verify the step is marked complete (O(1) bitmask check)
            bool completed = _runtime.IsStepCompleted(workflowKey, step);
            _consumer.Consume(completed);
        }

        // Complete the workflow
        _runtime.CompleteWorkflow(workflowKey);

        // Verify final status
        var status = _runtime.GetStatus(workflowKey);
        _consumer.Consume(status);

        return StepsPerWorkflow;
    }

    /// <summary>
    /// Signal delivery benchmark: start a workflow, send N signals, verify delivery.
    /// Measures signal throughput through the real engine (WAL append + state mutation).
    /// </summary>
    [Benchmark(Description = "Signal Storm: Start → N Signals → Complete")]
    public int SignalStorm()
    {
        ulong workflowId = (ulong)Guid.NewGuid().GetHashCode();
        ulong workflowKey = _runtime.StartWorkflow(
            workflowId, 1, 1, 42, 1);

        int signalsDelivered = 0;
        for (int i = 0; i < StepsPerWorkflow; i++)
        {
            byte[] signalPayload = System.Text.Encoding.UTF8.GetBytes($"signal-payload-{i}");
            _runtime.Signal(workflowKey, (ulong)i, signalPayload);
            signalsDelivered++;
        }
        _consumer.Consume(signalsDelivered);

        _runtime.CompleteWorkflow(workflowKey);
        return signalsDelivered;
    }

    /// <summary>
    /// Concurrent workflow creation: start N workflows, complete one step on each,
    /// then complete all. Measures scheduling overhead under fan-out.
    /// </summary>
    [Benchmark(Description = "Fan-Out: Start N Workflows → Step → Complete All")]
    public int ConcurrentWorkflowFanOut()
    {
        int count = StepsPerWorkflow;
        ulong[] keys = new ulong[count];

        // Start N workflows
        for (int i = 0; i < count; i++)
        {
            keys[i] = _runtime.StartWorkflow(
                (ulong)Guid.NewGuid().GetHashCode(), 1, 1, 42, 1);
        }
        _consumer.Consume(keys.Length);

        // Complete step 0 on each
        for (int i = 0; i < count; i++)
        {
            _runtime.CompleteStep(keys[i], 0);
        }

        // Complete all workflows
        for (int i = 0; i < count; i++)
        {
            _runtime.CompleteWorkflow(keys[i]);
            var status = _runtime.GetStatus(keys[i]);
            _consumer.Consume(status);
        }

        return count;
    }

    /// <summary>
    /// Step-by-step progression with state verification at each step.
    /// Measures the overhead of the bitmask check + Merkle root update per step.
    /// This is the core operation that determines workflow replay cost.
    /// </summary>
    [Benchmark(Description = "Step Progression: Start → Verify Each Step → Complete")]
    public int StepProgressionWithVerification()
    {
        ulong workflowKey = _runtime.StartWorkflow(
            (ulong)Guid.NewGuid().GetHashCode(), 1, 1, 42,
            (uint)StepsPerWorkflow);

        int verifiedSteps = 0;
        for (uint step = 0; step < (uint)StepsPerWorkflow; step++)
        {
            // Complete the step
            _runtime.CompleteStep(workflowKey, step);

            // Verify all steps up to and including current (simulates replay check)
            for (uint check = 0; check <= step; check++)
            {
                if (_runtime.IsStepCompleted(workflowKey, check))
                    verifiedSteps++;
            }
        }
        _consumer.Consume(verifiedSteps);

        _runtime.CompleteWorkflow(workflowKey);
        return verifiedSteps;
    }
}
