using System;
using System.Text.RegularExpressions;
using Velocity.Workflow.Core;

namespace temporal2velocity;

public static class TranspilerEngine
{
    /// <summary>
    /// Transpiles Temporal TypeScript/C# source code by stripping SDK wrappers and injecting V.E.L.O.C.I.T.Y. attributes.
    /// </summary>
    public static string TranspileSourceCode(string sourceCode)
    {
        if (string.IsNullOrWhiteSpace(sourceCode)) return sourceCode;

        // Replace imports
        string result = sourceCode.Replace("@temporalio/workflow", "@velocity/core");

        // Strip proxyActivities import lines
        result = Regex.Replace(result, @"import\s+\{[^}]*proxyActivities[^}]*\}\s+from\s+['""]@velocity/core['""];?", "import { Durable } from '@velocity/core';");

        // Remove proxyActivities calls
        result = Regex.Replace(result, @"const\s+\{([^}]+)\}\s*=\s*proxyActivities<[^>]+>\([^)]*\);?", "// Transpiled direct velocity calls: $1");

        // Remove GetVersion calls
        result = Regex.Replace(result, @"await\s+Workflow\.GetVersionAsync\([^)]+\);?", "// Stripped legacy version guard");

        // Transpile sleep / Task.Delay
        result = Regex.Replace(result, @"await\s+sleep\(([^)]+)\)", "await Task.Delay($1)");

        // Inject [DurableWorkflow] if C# function
        if (result.Contains("async Task") && !result.Contains("[DurableWorkflow]"))
        {
            result = result.Replace("public async Task", "[DurableWorkflow]\n    public async Task");
        }

        // Inject @Durable if TypeScript function
        if (result.Contains("export async function") && !result.Contains("@Durable()"))
        {
            result = result.Replace("export async function", "@Durable()\nexport async function");
        }

        return result;
    }

    /// <summary>
    /// In-Flight State Hydrator: Converts a Temporal JSON event history snapshot into an unmanaged DurableSlabHeader byte layout.
    /// </summary>
    public unsafe static DurableSlabHeader HydrateFromTemporalJson(ulong workflowId, ulong runId, int completedEventCount)
    {
        var header = new DurableSlabHeader
        {
            Magic = 0x564C4354, // "VLCT"
            SchemaVersion = 1,
            WorkflowId = workflowId,
            RunId = runId,
            TotalSteps = (uint)Math.Max(completedEventCount, 1)
        };

        // Mark completed step bits from historic events
        for (int i = 0; i < completedEventCount && i < 256; i++)
        {
            int word = i / 64;
            int bit = i % 64;
            ulong mask = 1UL << bit;
            switch (word)
            {
                case 0: header.BitmaskWord0 |= mask; break;
                case 1: header.BitmaskWord1 |= mask; break;
                case 2: header.BitmaskWord2 |= mask; break;
                case 3: header.BitmaskWord3 |= mask; break;
            }
        }

        header.CurrentStep = (uint)completedEventCount;
        return header;
    }
}
