using System;
using System.IO;

namespace temporal2velocity;

internal class Program
{
    static int Main(string[] args)
    {
        Console.WriteLine("=================================================");
        Console.WriteLine(" temporal2velocity Enterprise Migration Suite ");
        Console.WriteLine("=================================================");

        if (args.Length == 0)
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  temporal2velocity --src <filepath>     Transpile Temporal SDK workflow to V.E.L.O.C.I.T.Y.");
            Console.WriteLine("  temporal2velocity --hydrate <events>   Hydrate Temporal event history to .slab header");
            return 0;
        }

        if (args[0] == "--src" && args.Length > 1)
        {
            string path = args[1];
            if (!File.Exists(path))
            {
                Console.WriteLine($"Error: File not found '{path}'");
                return 1;
            }

            string source = File.ReadAllText(path);
            string transpiled = TranspilerEngine.TranspileSourceCode(source);
            Console.WriteLine($"Transpiled Code Output:\n{transpiled}");
            return 0;
        }

        if (args[0] == "--hydrate" && args.Length > 2)
        {
            if (ulong.TryParse(args[1], out ulong wfId) && int.TryParse(args[2], out int count))
            {
                var slab = TranspilerEngine.HydrateFromTemporalJson(wfId, 9999, count);
                Console.WriteLine($"Hydrated Slab Header: WorkflowId={slab.WorkflowId}, CompletedSteps={slab.CurrentStep}, IsValid={slab.IsValid}");
                return 0;
            }
        }

        Console.WriteLine("Invalid arguments.");
        return 1;
    }
}
