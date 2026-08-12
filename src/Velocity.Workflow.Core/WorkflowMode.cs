namespace Velocity.Workflow.Core;

/// <summary>
/// Runtime execution mode for the workflow engine.
/// Implements the base.md vision of "Progressive Engine Modes":
///   - Dev Mode: forgiving, reflection-based, hot-reload friendly, full trace logs
///   - Release Mode: zero-copy, bitmask-driven, no debug hooks, bare-metal performance
/// </summary>
public enum WorkflowExecutionMode
{
    /// <summary>
    /// Development mode: permits dynamic heap allocations, captures full trace logs,
    /// uses reflection-based driver so developers can hot-reload and debug with VS/VSCode.
    /// Falls back to DateTime.UtcNow for WorkflowClock, real Guid.NewGuid for WorkflowGuid.
    /// </summary>
    Development = 0,

    /// <summary>
    /// Production/release mode: strips debug hooks, computes static byte layouts,
    /// uses unmanaged slabs, zero GC allocations, deterministic replay via bitmask.
    /// All WorkflowClock/Guid/Random calls are engine-controlled.
    /// </summary>
    Release = 1,
}

/// <summary>
/// Configuration for the workflow engine execution mode.
/// Set at engine startup and immutable during runtime.
/// </summary>
public class WorkflowModeConfig
{
    /// <summary>
    /// The execution mode (Development or Release).
    /// </summary>
    public WorkflowExecutionMode Mode { get; }

    /// <summary>
    /// Whether to enable hot-reload support (Dev mode only).
    /// </summary>
    public bool EnableHotReload { get; }

    /// <summary>
    /// Whether to capture full execution traces (Dev mode only).
    /// </summary>
    public bool EnableFullTracing { get; }

    /// <summary>
    /// Whether to allow dynamic heap allocations in workflow code (Dev mode only).
    /// </summary>
    public bool AllowDynamicAlloc { get; }

    /// <summary>
    /// Whether to enforce determinism at compile time via Roslyn analyzers.
    /// In Dev mode, violations are warnings. In Release mode, violations are errors.
    /// </summary>
    public bool EnforceDeterminism { get; }

    public WorkflowModeConfig(WorkflowExecutionMode mode,
        bool enableHotReload = false, bool enableFullTracing = false,
        bool allowDynamicAlloc = false, bool enforceDeterminism = true)
    {
        Mode = mode;
        EnableHotReload = enableHotReload;
        EnableFullTracing = enableFullTracing;
        AllowDynamicAlloc = allowDynamicAlloc;
        EnforceDeterminism = enforceDeterminism;
    }

    /// <summary>
    /// Create a development mode configuration (forgiving, debug-friendly).
    /// </summary>
    public static WorkflowModeConfig Development() => new(
        WorkflowExecutionMode.Development,
        enableHotReload: true,
        enableFullTracing: true,
        allowDynamicAlloc: true,
        enforceDeterminism: false);

    /// <summary>
    /// Create a release/production mode configuration (strict, zero-copy).
    /// </summary>
    public static WorkflowModeConfig Release() => new(
        WorkflowExecutionMode.Release,
        enableHotReload: false,
        enableFullTracing: false,
        allowDynamicAlloc: false,
        enforceDeterminism: true);

    /// <summary>
    /// Whether the engine is in development mode.
    /// </summary>
    public bool IsDevelopment => Mode == WorkflowExecutionMode.Development;

    /// <summary>
    /// Whether the engine is in release/production mode.
    /// </summary>
    public bool IsRelease => Mode == WorkflowExecutionMode.Release;
}

/// <summary>
/// Global mode switch for the workflow engine.
/// The Roslyn generators check this at code generation time to emit different code paths.
/// </summary>
public static class WorkflowMode
{
    private static WorkflowModeConfig _config = WorkflowModeConfig.Development();

    /// <summary>
    /// Get the current execution mode configuration.
    /// </summary>
    public static WorkflowModeConfig Current => _config;

    /// <summary>
    /// Set the execution mode. Must be called before engine startup.
    /// </summary>
    public static void Configure(WorkflowModeConfig config)
    {
        _config = config;

        // Sync deterministic primitives with mode
        if (config.IsDevelopment)
        {
            WorkflowClock.EnableDevMode();
        }
        else
        {
            WorkflowClock.DisableDevMode();
        }
    }

    /// <summary>
    /// Whether the engine is currently in development mode.
    /// </summary>
    public static bool IsDev => _config.IsDevelopment;

    /// <summary>
    /// Whether the engine is currently in release/production mode.
    /// </summary>
    public static bool IsRelease => _config.IsRelease;
}
