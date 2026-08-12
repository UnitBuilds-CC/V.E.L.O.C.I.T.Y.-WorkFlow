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

        // Default run BenchmarkDotNet suite
        Console.WriteLine("Running BenchmarkDotNet suite...");
        BenchmarkRunner.Run<SlabVsReplayBenchmark>();
    }
}
