using System;
using BenchmarkDotNet.Running;

namespace Velocity.Workflow.Benchmarks;

internal class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("=========================================================");
        Console.WriteLine(" V.E.L.O.C.I.T.Y.-WorkFlow Benchmark & Crash Fuzz Suite ");
        Console.WriteLine("=========================================================");

        if (args.Length > 0 && args[0] == "--fuzz")
        {
            CrashFuzzHarness.RunFuzzingPass(1000);
            return;
        }

        if (args.Length > 0 && args[0] == "--step-bench")
        {
            Console.WriteLine("Running Step-by-Step Micro-Benchmark Breakdown...");
            BenchmarkRunner.Run<StepBreakdownBenchmarks>();
            return;
        }

        if (args.Length > 0 && args[0] == "--temporal-vs-velocity")
        {
            Console.WriteLine("Running Traditional Temporal vs V.E.L.O.C.I.T.Y.-WorkFlow Head-to-Head Benchmark...");
            BenchmarkRunner.Run<TemporalVsVelocityBenchmark>();
            return;
        }

        if (args.Length > 0 && args[0] == "--lifecycle")
        {
            Console.WriteLine("Running Real Workflow Lifecycle Benchmarks (engine via FFI)...");
            BenchmarkRunner.Run<WorkflowLifecycleBenchmark>();
            return;
        }

        // Default run BenchmarkDotNet suite for Temporal vs Velocity comparison
        Console.WriteLine("Running Head-to-Head BenchmarkDotNet Suite...");
        BenchmarkRunner.Run<TemporalVsVelocityBenchmark>();
    }
}
