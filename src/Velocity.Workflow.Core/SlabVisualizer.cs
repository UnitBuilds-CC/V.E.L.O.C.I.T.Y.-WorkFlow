using System;
using System.Runtime.InteropServices;
using System.Text;

namespace Velocity.Workflow.Core;

/// <summary>
/// Slab Visualizer — terminal-based tool for inspecting raw slab memory,
/// bitmask state, and Merkle roots per workflow.
/// Provides a real-time view into the engine's memory-mapped slab headers.
/// </summary>
public static unsafe partial class SlabVisualizer
{
    // ─── Slab Header Layout (128 bytes, matching Rust SlabHeader) ────────

    /// <summary>Size of the slab header in bytes (matches Rust SLAB_HEADER_SIZE).</summary>
    public const int SlabHeaderSize = 128;

    /// <summary>Magic value "VLCT" = 0x564C4354.</summary>
    public const uint MagicVLCT = 0x564C4354;

    // ─── Slab Snapshot ──────────────────────────────────────────────────

    /// <summary>Snapshot of a single slab header's state.</summary>
    public record SlabSnapshot(
        ulong WorkflowKey,
        uint Magic,
        uint SchemaVersion,
        ulong WorkflowId,
        ulong RunId,
        uint CurrentStep,
        uint TotalSteps,
        byte[] MerkleRoot,
        ulong[] BitmaskBits,
        uint CompletedSteps,
        bool IsValid,
        bool MerkleValid);

    /// <summary>Summary of all slab headers in the engine.</summary>
    public record SlabSummary(
        int TotalSlabs,
        int ValidSlabs,
        int InvalidSlabs,
        int MerkleValidCount,
        int MerkleInvalidCount,
        ulong TotalWorkflows,
        ulong TotalStepsCompleted,
        double CompletionRate);

    // ─── FFI Bindings ───────────────────────────────────────────────────

    [LibraryImport("velocity_workflow_engine", EntryPoint = "velocity_slab_dump")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial int SlabDump(void* engineHandle, IntPtr buffer, int bufferSize);

    [LibraryImport("velocity_workflow_engine", EntryPoint = "velocity_slab_count")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial uint SlabCount(void* engineHandle);

    [LibraryImport("velocity_workflow_engine", EntryPoint = "velocity_slab_header_size")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial uint SlabHeaderSizeNative();

    [LibraryImport("velocity_workflow_engine", EntryPoint = "velocity_slab_get_merkle_root")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial int SlabGetMerkleRoot(void* engineHandle, ulong workflowKey, IntPtr buffer, int bufferSize);

    [LibraryImport("velocity_workflow_engine", EntryPoint = "velocity_slab_verify_merkle")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial uint SlabVerifyMerkle(void* engineHandle, ulong workflowKey);

    [LibraryImport("velocity_workflow_engine", EntryPoint = "velocity_slab_get_bitmask")]
    [UnmanagedCallConv(CallConvs = new[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial int SlabGetBitmask(void* engineHandle, ulong workflowKey, IntPtr buffer, int bufferSize);

    // ─── Public API ─────────────────────────────────────────────────────

    /// <summary>
    /// Format a slab snapshot as a human-readable string for terminal display.
    /// </summary>
    public static string FormatSlab(SlabSnapshot slab)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"╔══════════════════════════════════════════════════════════════╗");
        sb.AppendLine($"║  SLAB HEADER — Workflow Key: {slab.WorkflowKey,16}              ║");
        sb.AppendLine($"╠══════════════════════════════════════════════════════════════╣");
        sb.AppendLine($"║  Magic:          0x{slab.Magic:X8}  {(slab.IsValid ? "✓ VALID" : "✗ INVALID")}              ║");
        sb.AppendLine($"║  Schema:         v{slab.SchemaVersion}                                    ║");
        sb.AppendLine($"║  Workflow ID:    {slab.WorkflowId,16}                          ║");
        sb.AppendLine($"║  Run ID:         {slab.RunId,16}                          ║");
        sb.AppendLine($"║  Step:           {slab.CurrentStep} / {slab.TotalSteps}  ({slab.CompletedSteps} completed)           ║");
        sb.AppendLine($"║  Merkle Root:    {FormatHex(slab.MerkleRoot, 16)}  {(slab.MerkleValid ? "✓" : "✗")}  ║");
        sb.AppendLine($"║  Bitmask:        {FormatBitmask(slab.BitmaskBits)}  ║");
        sb.AppendLine($"╚══════════════════════════════════════════════════════════════╝");
        return sb.ToString();
    }

    /// <summary>
    /// Format a slab summary showing aggregate statistics.
    /// </summary>
    public static string FormatSummary(SlabSummary summary)
    {
        var sb = new StringBuilder();
        sb.AppendLine("═══════════════════════════════════════════════════");
        sb.AppendLine("  VELOCITY Slab Summary");
        sb.AppendLine("═══════════════════════════════════════════════════");
        sb.AppendLine($"  Total Slabs:        {summary.TotalSlabs}");
        sb.AppendLine($"  Valid Slabs:        {summary.ValidSlabs}");
        sb.AppendLine($"  Invalid Slabs:      {summary.InvalidSlabs}");
        sb.AppendLine($"  Merkle Valid:       {summary.MerkleValidCount}");
        sb.AppendLine($"  Merkle Invalid:     {summary.MerkleInvalidCount}");
        sb.AppendLine($"  Total Workflows:    {summary.TotalWorkflows}");
        sb.AppendLine($"  Steps Completed:    {summary.TotalStepsCompleted}");
        sb.AppendLine($"  Completion Rate:    {summary.CompletionRate:P1}");
        sb.AppendLine("═══════════════════════════════════════════════════");
        return sb.ToString();
    }

    /// <summary>
    /// Format a bitmask as a binary string showing completed steps.
    /// </summary>
    public static string FormatBitmask(ulong[] bits)
    {
        if (bits == null || bits.Length == 0) return "0000...0000";

        var sb = new StringBuilder();
        for (int i = bits.Length - 1; i >= 0; i--)
        {
            sb.Append(Convert.ToString((long)bits[i], 2).PadLeft(64, '0'));
            if (i > 0) sb.Append(' ');
        }
        return sb.ToString();
    }

    /// <summary>
    /// Format a byte array as hex with truncation.
    /// </summary>
    public static string FormatHex(byte[] bytes, int maxBytes = 16)
    {
        if (bytes == null || bytes.Length == 0) return "(empty)";

        var sb = new StringBuilder();
        int count = Math.Min(bytes.Length, maxBytes);
        for (int i = 0; i < count; i++)
        {
            sb.Append(bytes[i].ToString("x2"));
            if (i < count - 1) sb.Append(' ');
        }
        if (bytes.Length > maxBytes) sb.Append("...");
        return sb.ToString();
    }

    /// <summary>
    /// Count the number of set bits in a bitmask.
    /// </summary>
    public static uint CountSetBits(ulong[] bits)
    {
        uint count = 0;
        if (bits == null) return 0;
        foreach (var b in bits)
            count += (uint)System.Numerics.BitOperations.PopCount(b);
        return count;
    }

    /// <summary>
    /// Verify a Merkle root hex string matches expected format (64 hex chars).
    /// </summary>
    public static bool IsValidMerkleHex(string hex)
    {
        if (string.IsNullOrEmpty(hex)) return false;
        if (hex.Length != 64) return false;
        foreach (char c in hex)
        {
            if (!Uri.IsHexDigit(c)) return false;
        }
        return true;
    }
}
