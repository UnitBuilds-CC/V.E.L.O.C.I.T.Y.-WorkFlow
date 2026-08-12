using System;

namespace Velocity.Workflow.Core;

/// <summary>
/// Deterministic clock replacement for DateTime.UtcNow / DateTime.Now inside durable workflows.
/// Returns engine-controlled tick offsets rather than wall-clock time, ensuring replay determinism.
///
/// The Roslyn CodeFix auto-rewrites DateTime.UtcNow → WorkflowClock.UtcNow at compile time.
/// In production mode, this reads from the slab's deterministic tick counter.
/// In dev mode, this falls back to DateTime.UtcNow for debugging convenience.
/// </summary>
public static class WorkflowClock
{
    private static long _devModeTickOffset;
    private static bool _devMode;

    /// <summary>
    /// Deterministic UTC timestamp. In production mode, returns the engine's replay-safe tick.
    /// In dev mode, returns real UTC time (forgiving for development).
    /// </summary>
    public static DateTime UtcNow => _devMode
        ? DateTime.UtcNow
        : new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(_devModeTickOffset);

    /// <summary>
    /// Deterministic Unix timestamp in milliseconds.
    /// </summary>
    public static long UtcNowMs => _devMode
        ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        : new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() + _devModeTickOffset;

    /// <summary>
    /// Called by the engine to set the current deterministic tick during replay.
    /// </summary>
    public static void SetTick(long tickMs) => _devModeTickOffset = tickMs;

    /// <summary>
    /// Advance the tick by a delta (used for step-based time progression).
    /// </summary>
    public static void AdvanceTick(long deltaMs) => _devModeTickOffset += deltaMs;

    /// <summary>
    /// Enable dev mode — falls back to real wall-clock time for debugging.
    /// </summary>
    public static void EnableDevMode() => _devMode = true;

    /// <summary>
    /// Disable dev mode — uses deterministic engine ticks (production behavior).
    /// </summary>
    public static void DisableDevMode() => _devMode = false;

    /// <summary>
    /// Whether dev mode is currently active.
    /// </summary>
    public static bool IsDevMode => _devMode;
}

/// <summary>
/// Deterministic GUID generator for durable workflows.
/// Produces seeded, reproducible GUIDs derived from workflow ID and step index.
///
/// The Roslyn CodeFix auto-rewrites Guid.NewGuid() → WorkflowGuid.NewGuid() at compile time.
/// </summary>
public static class WorkflowGuid
{
    private static ulong _workflowId;
    private static int _stepIndex;

    /// <summary>
    /// Generate a deterministic GUID based on the current workflow context.
    /// Uses a seeded hash of (workflowId, stepIndex) for reproducibility.
    /// </summary>
    public static Guid NewGuid()
    {
        // Deterministic: hash of workflow context
        ulong seed = _workflowId ^ ((ulong)(uint)_stepIndex << 32);
        return FromSeed(seed);
    }

    /// <summary>
    /// Generate a deterministic GUID with an explicit salt value.
    /// </summary>
    public static Guid NewGuid(long salt)
    {
        ulong seed = _workflowId ^ ((ulong)(uint)_stepIndex << 32) ^ (ulong)salt;
        return FromSeed(seed);
    }

    /// <summary>
    /// Advance the step index (called by the engine between steps).
    /// </summary>
    public static void AdvanceStep() => _stepIndex++;

    /// <summary>
    /// Set the workflow context for deterministic generation.
    /// </summary>
    public static void SetContext(ulong workflowId, int stepIndex)
    {
        _workflowId = workflowId;
        _stepIndex = stepIndex;
    }

    /// <summary>
    /// Reset the step counter (called at workflow start).
    /// </summary>
    public static void Reset(ulong workflowId)
    {
        _workflowId = workflowId;
        _stepIndex = 0;
    }

    /// <summary>
    /// Create a deterministic GUID from a 64-bit seed using FNV-1a-style mixing.
    /// </summary>
    private static Guid FromSeed(ulong seed)
    {
        // FNV-1a inspired mixing for good distribution
        const ulong FnvPrime = 1099511628211UL;
        const ulong FnvOffset = 14695981039346656037UL;

        ulong h1 = FnvOffset ^ seed;
        h1 *= FnvPrime;
        h1 ^= h1 >> 33;

        ulong h2 = FnvOffset ^ (seed * 2654435761UL);
        h2 *= FnvPrime;
        h2 ^= h2 >> 33;

        Span<byte> bytes = stackalloc byte[16];
        BitConverter.TryWriteBytes(bytes.Slice(0, 8), h1);
        BitConverter.TryWriteBytes(bytes.Slice(8, 8), h2);

        // Set version 4 (random-like) and variant bits for UUID compatibility
        bytes[7] = (byte)((bytes[7] & 0x0F) | 0x40); // Version 4
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80); // Variant 10

        return new Guid(bytes);
    }
}

/// <summary>
/// Deterministic random number generator for durable workflows.
/// Produces seeded, reproducible random values derived from workflow context.
///
/// The Roslyn CodeFix auto-rewrites new System.Random() → new WorkflowRandom() at compile time.
/// </summary>
public class WorkflowRandom
{
    private ulong _state;

    public WorkflowRandom()
    {
        // Default: seed from workflow context
        _state = 0x9E3779B97F4A7C15UL; // golden ratio constant
    }

    public WorkflowRandom(int seed)
    {
        _state = (ulong)seed ^ 0x9E3779B97F4A7C15UL;
    }

    /// <summary>
    /// Set the seed from the workflow engine context for deterministic replay.
    /// </summary>
    public void SeedFromContext(ulong workflowId, int stepIndex)
    {
        _state = workflowId ^ ((ulong)(uint)stepIndex << 32) ^ 0x9E3779B97F4A7C15UL;
    }

    /// <summary>
    /// Returns a non-negative random integer (xorshift64*).
    /// </summary>
    public int Next()
    {
        _state ^= _state >> 12;
        _state ^= _state << 25;
        _state ^= _state >> 27;
        return (int)((_state * 0x2545F4914F6CDD1DUL) & 0x7FFFFFFF);
    }

    /// <summary>
    /// Returns a non-negative random integer less than maxValue.
    /// </summary>
    public int Next(int maxValue)
    {
        if (maxValue <= 0) throw new ArgumentOutOfRangeException(nameof(maxValue));
        return (int)((uint)Next() % (uint)maxValue);
    }

    /// <summary>
    /// Returns a random integer in [minValue, maxValue).
    /// </summary>
    public int Next(int minValue, int maxValue)
    {
        if (minValue >= maxValue) throw new ArgumentOutOfRangeException(nameof(minValue));
        long range = (long)maxValue - minValue;
        return minValue + (int)((uint)Next() % (uint)range);
    }

    /// <summary>
    /// Returns a random double in [0.0, 1.0).
    /// </summary>
    public double NextDouble()
    {
        return (Next() & 0x7FFFFFFF) / (double)int.MaxValue;
    }

    /// <summary>
    /// Fill a buffer with deterministic random bytes.
    /// </summary>
    public void NextBytes(byte[] buffer)
    {
        for (int i = 0; i < buffer.Length; i++)
        {
            buffer[i] = (byte)Next(256);
        }
    }
}
