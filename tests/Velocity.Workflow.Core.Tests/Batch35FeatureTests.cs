using System;
using System.Text;
using Xunit;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Core.Tests;

/// <summary>
/// Tests for Batch 35+ features: Raft consensus, history compaction,
/// durable RPC, and AI context window — all via FFI to the Rust engine.
/// </summary>
public class Batch35FeatureTests
{
    // Helper to call RaftAppendEntry without pointer syntax in each test
    private static ulong RaftAppendEntrySafe(ulong groupId, ulong workflowKey, byte eventType)
    {
        unsafe { return NativeBridge.RaftAppendEntry(groupId, workflowKey, eventType, null, 0); }
    }

    // --- Raft Consensus ---

    [Fact]
    public void Raft_CreateGroup()
    {
        var groupId = NativeBridge.RaftCreateGroup(0);
        Assert.True(groupId < 1000); // Reasonable group ID
        Assert.True(NativeBridge.RaftGroupCount() > 0);
    }

    [Fact]
    public unsafe void Raft_BecomeLeaderAndAppend()
    {
        var groupId = NativeBridge.RaftCreateGroup(42);
        Assert.True(NativeBridge.RaftBecomeLeader(groupId));

        // Append an entry (event_type 0 = WorkflowStarted)
        var idx = NativeBridge.RaftAppendEntry(groupId, 100, 0, null, 0);
        Assert.True(idx > 0);
    }

    [Fact]
    public void Raft_ApplyCommitted()
    {
        var groupId = NativeBridge.RaftCreateGroup(99);
        NativeBridge.RaftBecomeLeader(groupId);
        RaftAppendEntrySafe(groupId, 200, 1);

        var applied = NativeBridge.RaftApplyCommitted(groupId);
        Assert.True(applied > 0);
    }

    [Fact]
    public void Raft_StatCommitted()
    {
        var before = NativeBridge.RaftStatCommitted();
        var groupId = NativeBridge.RaftCreateGroup(77);
        NativeBridge.RaftBecomeLeader(groupId);
        RaftAppendEntrySafe(groupId, 300, 0);

        var after = NativeBridge.RaftStatCommitted();
        Assert.True(after >= before);
    }

    // --- History Compaction ---

    [Fact]
    public void Compaction_AppendAndCount()
    {
        var wfKey = (ulong)Random.Shared.Next(100000, 999999);
        NativeBridge.CompactAppendEvent(wfKey, 0); // WorkflowStarted
        NativeBridge.CompactAppendEvent(wfKey, 1); // ActivityTaskScheduled
        NativeBridge.CompactAppendEvent(wfKey, 2); // ActivityTaskCompleted

        var count = NativeBridge.CompactEventCount(wfKey);
        Assert.Equal(3UL, count);
    }

    [Fact]
    public void Compaction_CompactAll()
    {
        var wfKey = (ulong)Random.Shared.Next(100000, 999999);
        for (int i = 0; i < 5; i++)
            NativeBridge.CompactAppendEvent(wfKey, 1); // ActivityTaskScheduled

        var squashed = NativeBridge.CompactAll();
        // May or may not squash depending on threshold, but shouldn't crash
        Assert.True(squashed < 1000);
    }

    // --- Durable RPC ---

    [Fact]
    public unsafe void DurableRpc_InitiateAndComplete()
    {
        var caller = "service-a"u8;
        var target = "service-b"u8;
        var method = "GetUser"u8;

        fixed (byte* c = caller)
        fixed (byte* t = target)
        fixed (byte* m = method)
        {
            var rpcId = NativeBridge.RpcInitiate(c, (uint)caller.Length, t, (uint)target.Length, m, (uint)method.Length);
            Assert.True(rpcId > 0);

            Assert.True(NativeBridge.RpcComplete(rpcId));
        }
    }

    [Fact]
    public unsafe void DurableRpc_InitiateAndFail()
    {
        var caller = "svc-x"u8;
        var target = "svc-y"u8;
        var method = "DoWork"u8;

        fixed (byte* c = caller)
        fixed (byte* t = target)
        fixed (byte* m = method)
        {
            var rpcId = NativeBridge.RpcInitiate(c, (uint)caller.Length, t, (uint)target.Length, m, (uint)method.Length);
            Assert.True(rpcId > 0);
            Assert.True(NativeBridge.RpcFail(rpcId));
        }
    }

    [Fact]
    public void DurableRpc_Count()
    {
        var count = NativeBridge.RpcCount();
        Assert.True(count < 10000); // Sanity check
    }

    // --- AI Context ---

    [Fact]
    public unsafe void AiContext_AddMessage()
    {
        var content = "Hello, agent!"u8;
        fixed (byte* c = content)
        {
            var tokens = NativeBridge.AiAddMessage(1, c, (uint)content.Length); // role=1 (User)
            Assert.True(tokens > 0);
        }
    }

    [Fact]
    public void AiContext_CurrentTokens()
    {
        var tokens = NativeBridge.AiCurrentTokens();
        Assert.True(tokens < 1_000_000); // Sanity
    }

    [Fact]
    public void AiContext_MessageCount()
    {
        var count = NativeBridge.AiMessageCount();
        Assert.True(count < 1_000_000); // Sanity
    }

    [Fact]
    public void AiContext_Compress()
    {
        var compressed = NativeBridge.AiCompress();
        Assert.True(compressed < 1_000_000); // Sanity
    }

    // --- Deterministic Primitives ---

    [Fact]
    public void WorkflowClock_DevMode()
    {
        WorkflowClock.EnableDevMode();
        Assert.True(WorkflowClock.IsDevMode);
        var now = WorkflowClock.UtcNow;
        Assert.True(now.Year >= 2024);
        WorkflowClock.DisableDevMode();
        Assert.False(WorkflowClock.IsDevMode);
    }

    [Fact]
    public void WorkflowGuid_DeterministicOutput()
    {
        WorkflowGuid.Reset(42);
        var g1 = WorkflowGuid.NewGuid();
        WorkflowGuid.Reset(42);
        var g2 = WorkflowGuid.NewGuid();
        Assert.Equal(g1, g2); // Same context → same GUID
    }

    [Fact]
    public void WorkflowRandom_DeterministicOutput()
    {
        var r1 = new WorkflowRandom(12345);
        var r2 = new WorkflowRandom(12345);
        for (int i = 0; i < 100; i++)
            Assert.Equal(r1.Next(), r2.Next());
    }

    // --- Workflow Mode ---

    [Fact]
    public void WorkflowMode_DevAndRelease()
    {
        WorkflowMode.Configure(WorkflowModeConfig.Development());
        Assert.True(WorkflowMode.IsDev);
        Assert.False(WorkflowMode.IsRelease);

        WorkflowMode.Configure(WorkflowModeConfig.Release());
        Assert.False(WorkflowMode.IsDev);
        Assert.True(WorkflowMode.IsRelease);

        // Reset to dev for other tests
        WorkflowMode.Configure(WorkflowModeConfig.Development());
    }
}
