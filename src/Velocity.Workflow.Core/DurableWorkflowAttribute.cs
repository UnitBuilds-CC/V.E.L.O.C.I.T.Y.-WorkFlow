using System;

namespace Velocity.Workflow.Core;

/// <summary>
/// Decorates C# methods to trigger build-time Roslyn AST transpilation into a zero-allocation
/// hardware-native state machine backed by V.E.L.O.C.I.T.Y. memory slabs.
/// </summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class DurableWorkflowAttribute : Attribute
{
    /// <summary>
    /// Pre-allocated memory slab size in bytes (Default: 4096 bytes).
    /// </summary>
    public int SlabSize { get; set; } = 4096;

    /// <summary>
    /// Workflow schema version for slot padding migrations.
    /// </summary>
    public int Version { get; set; } = 1;

    /// <summary>
    /// Enable cryptographic Merkle root hash verification on every state mutation.
    /// </summary>
    public bool CryptographicProof { get; set; } = true;

    public DurableWorkflowAttribute() { }

    public DurableWorkflowAttribute(int slabSize)
    {
        SlabSize = slabSize;
    }
}
