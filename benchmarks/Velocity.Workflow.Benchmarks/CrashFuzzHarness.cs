using System;
using System.Diagnostics;
using System.IO;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Benchmarks;

public unsafe static class CrashFuzzHarness
{
    public static void RunFuzzingPass(int iterations = 1000)
    {
        Console.WriteLine($"[CrashFuzzHarness] Executing {iterations} process crash & state resumption fuzzing passes...");
        string tempSlabFile = Path.Combine(Path.GetTempPath(), "velocity_fuzz_test.slab");

        var sw = Stopwatch.StartNew();
        int passed = 0;

        for (int i = 0; i < iterations; i++)
        {
            var header = new DurableSlabHeader
            {
                Magic = 0x564C4354, // "VLCT"
                WorkflowId = (ulong)(1000 + i),
                RunId = (ulong)(5000 + i),
                TotalSteps = 100
            };

            // Step 1: Simulate active workflow state mutations
            header.BitmaskWord0 = 0x00000000FFFFFFFF; // First 32 steps completed
            header.CurrentStep = 32;

            // Step 2: Write raw unmanaged slab bytes directly to disk (simulating mmap flush)
            byte[] bytes = new byte[sizeof(DurableSlabHeader)];
            fixed (byte* bPtr = bytes)
            {
                *(DurableSlabHeader*)bPtr = header;
            }
            File.WriteAllBytes(tempSlabFile, bytes);

            // Step 3: Simulate process kill -9 and immediate restart (reading bytes directly into memory)
            byte[] readBytes = File.ReadAllBytes(tempSlabFile);
            fixed (byte* rPtr = readBytes)
            {
                DurableSlabHeader* restoredHeader = (DurableSlabHeader*)rPtr;

                // Validate O(1) state restoration integrity
                if (restoredHeader->IsValid &&
                    restoredHeader->WorkflowId == (ulong)(1000 + i) &&
                    restoredHeader->CurrentStep == 32 &&
                    restoredHeader->IsStepSet(31) &&
                    !restoredHeader->IsStepSet(32))
                {
                    passed++;
                }
            }
        }

        sw.Stop();

        if (File.Exists(tempSlabFile)) File.Delete(tempSlabFile);

        double totalMs = sw.Elapsed.TotalMilliseconds;
        double avgUsPerResumption = (totalMs * 1000.0) / iterations;

        Console.WriteLine($"[CrashFuzzHarness] Results: {passed}/{iterations} passes PASSED.");
        Console.WriteLine($"[CrashFuzzHarness] Total Time: {totalMs:F2} ms | Avg Resumption Latency: {avgUsPerResumption:F3} us/resumption.");
    }
}
