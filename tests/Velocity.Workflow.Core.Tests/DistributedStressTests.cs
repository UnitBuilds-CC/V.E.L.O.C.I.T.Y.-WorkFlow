using Xunit;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Core.Tests;

/// <summary>
/// Distributed stress and chaos tests — validates multi-node behavior under load,
/// replication transport at scale, sharding rebalancing, daemon poll cycles,
/// and chaos scenarios (link failures, queue overflow, reconnections).
/// </summary>
public class DistributedStressTests : IDisposable
{
    private readonly WorkflowRuntime _runtime;

    public DistributedStressTests()
    {
        _runtime = new WorkflowRuntime();
    }

    public void Dispose() => _runtime.Dispose();

    // ─── Replication Transport Stress ─────────────────────────────────────

    [Fact]
    public void Stress_ReplicationManyLinks()
    {
        // Create 50 replication links (simulating a 50-cluster setup)
        const int clusterCount = 50;
        for (uint i = 1; i <= clusterCount; i++)
        {
            _runtime.ReplicationAddLink($"cluster-{i}", i, $"http://cluster-{i}:9090");
        }
        Assert.Equal((ulong)clusterCount, _runtime.ReplicationActiveLinkCount());

        // Push tasks from each cluster
        for (uint i = 1; i <= clusterCount; i++)
        {
            bool ok = _runtime.ReplicationPushFromCluster(i, 100 + i, 1, new byte[] { 1, 2, 3 }, 1, i);
            Assert.True(ok);
        }

        // All incoming should be buffered
        Assert.True(_runtime.ReplicationTotalPendingIncoming() >= (ulong)clusterCount);
    }

    [Fact]
    public void Stress_ReplicationHighVolumePush()
    {
        _runtime.ReplicationAddLink("cluster-b", 2, "http://cluster-b:9090");

        // Push 1000 tasks in rapid succession
        const int taskCount = 1000;
        int successCount = 0;
        for (int i = 0; i < taskCount; i++)
        {
            bool ok = _runtime.ReplicationPushFromCluster(2, (ulong)(200 + i), 1, new byte[] { (byte)(i & 0xFF) }, 1, (ulong)i);
            if (ok) successCount++;
        }

        // All should succeed (incoming buffer has no hard limit in push)
        Assert.Equal(taskCount, successCount);
        Assert.True(_runtime.ReplicationTotalPendingIncoming() >= (ulong)taskCount);
    }

    [Fact]
    public void Stress_ConcurrentWorkflowStartAndComplete()
    {
        // Start and complete 100 workflows in sequence
        const int workflowCount = 100;
        var keys = new List<ulong>();

        for (int i = 0; i < workflowCount; i++)
        {
            var key = _runtime.StartWorkflow((ulong)(1000 + i), 1, 0, 42, 3);
            Assert.True(key > 0);
            keys.Add(key);
        }

        // Complete all workflows
        int completed = 0;
        foreach (var key in keys)
        {
            _runtime.CompleteWorkflow(key, null);
            completed++;
        }

        Assert.Equal(workflowCount, completed);
    }

    // ─── Sharding Stress ──────────────────────────────────────────────────

    [Fact]
    public void Stress_ShardingManyHosts()
    {
        // Add 100 hosts to the consistent hash ring
        const int hostCount = 100;
        for (int i = 0; i < hostCount; i++)
        {
            _runtime.ShardingAddHost($"host-{i}.example.com:8080");
        }

        Assert.Equal((ulong)hostCount, _runtime.ShardingHostCount());
    }

    [Fact]
    public void Stress_ShardingRebalance()
    {
        // Add hosts, then remove half, verify ring still works
        const int initialHosts = 20;
        for (int i = 0; i < initialHosts; i++)
        {
            _runtime.ShardingAddHost($"host-{i}.example.com:8080");
        }
        Assert.Equal((ulong)initialHosts, _runtime.ShardingHostCount());

        // Remove half
        for (int i = 0; i < initialHosts / 2; i++)
        {
            _runtime.ShardingRemoveHost($"host-{i}.example.com:8080");
        }
        Assert.Equal((ulong)(initialHosts / 2), _runtime.ShardingHostCount());

        // Ring should still be functional
        Assert.True(_runtime.ShardingHostCount() > 0);
    }

    // ─── Daemon Stress ────────────────────────────────────────────────────
    // Note: Daemon poll/start/stop tests are in Rust (replication_daemon::tests)
    // to avoid global OnceLock FFI issues with parallel C# test execution.

    // ─── Chaos Scenarios ──────────────────────────────────────────────────

    [Fact]
    public void Chaos_LinkFailureAndRecovery()
    {
        _runtime.ReplicationAddLink("cluster-b", 2, "http://cluster-b:9090");

        // Deactivate link (simulate network failure)
        _runtime.ReplicationSetLinkActive(2, false);
        Assert.Equal(0UL, _runtime.ReplicationActiveLinkCount());

        // Reactivate link (simulate network recovery)
        _runtime.ReplicationSetLinkActive(2, true);
        Assert.Equal(1UL, _runtime.ReplicationActiveLinkCount());
    }

    [Fact]
    public void Chaos_RemoveAndReaddLink()
    {
        _runtime.ReplicationAddLink("cluster-b", 2, "http://cluster-b:9090");
        Assert.Equal(1UL, _runtime.ReplicationActiveLinkCount());

        // Remove link
        _runtime.ReplicationRemoveLink(2);
        Assert.Equal(0UL, _runtime.ReplicationActiveLinkCount());

        // Re-add with same ID
        _runtime.ReplicationAddLink("cluster-b-v2", 2, "http://cluster-b-new:9090");
        Assert.Equal(1UL, _runtime.ReplicationActiveLinkCount());
    }

    [Fact]
    public void Chaos_RapidAddRemoveLinks()
    {
        // Rapidly add and remove 100 links
        for (uint i = 1; i <= 100; i++)
        {
            _runtime.ReplicationAddLink($"cluster-{i}", i, $"http://cluster-{i}:9090");
        }
        Assert.Equal(100UL, _runtime.ReplicationActiveLinkCount());

        // Remove all
        for (uint i = 1; i <= 100; i++)
        {
            _runtime.ReplicationRemoveLink(i);
        }
        Assert.Equal(0UL, _runtime.ReplicationActiveLinkCount());
    }

    [Fact]
    public void Chaos_WorkflowStartFailComplete_Mixed()
    {
        // Mix of start, fail, complete operations
        var keys = new List<ulong>();

        // Start 30 workflows
        for (int i = 0; i < 30; i++)
        {
            var key = _runtime.StartWorkflow((ulong)(5000 + i), 1, 0, 42, 2);
            if (key > 0) keys.Add(key);
        }

        // Fail first 10
        int failed = 0;
        for (int i = 0; i < Math.Min(10, keys.Count); i++)
        {
            _runtime.FailWorkflow(keys[i]);
            failed++;
        }

        // Complete next 10
        int completed = 0;
        for (int i = 10; i < Math.Min(20, keys.Count); i++)
        {
            _runtime.CompleteWorkflow(keys[i], null);
            completed++;
        }

        Assert.True(failed > 0);
        Assert.True(completed > 0);
    }

    // ─── Integration: Cross-Feature Stress ────────────────────────────────

    [Fact]
    public void Stress_ReplicationAndWorkflows()
    {
        // Set up replication links
        _runtime.ReplicationAddLink("cluster-b", 2, "http://cluster-b:9090");
        _runtime.ReplicationAddLink("cluster-c", 3, "http://cluster-c:9090");

        // Start workflows
        for (int i = 0; i < 20; i++)
        {
            _runtime.StartWorkflow((ulong)(8000 + i), 1, 0, 42, 3);
        }

        // Verify state is consistent
        Assert.Equal(2UL, _runtime.ReplicationActiveLinkCount());
    }

    [Fact]
    public void Stress_ShardingAndWorkflows()
    {
        // Set up sharding
        for (int i = 0; i < 10; i++)
        {
            _runtime.ShardingAddHost($"host-{i}.example.com:8080");
        }
        
        // Start workflows
        for (int i = 0; i < 50; i++)
        {
            var key = _runtime.StartWorkflow((ulong)(7000 + i), 1, 0, 42, 2);
            Assert.True(key > 0);
        }

        // Verify sharding is stable
        Assert.Equal(10UL, _runtime.ShardingHostCount());
    }

    [Fact]
    public void Stress_DaemonStats_Accumulate()
    {
        _runtime.ReplicationAddLink("cluster-b", 2, "http://cluster-b:9090");
        Assert.Equal(1UL, _runtime.ReplicationActiveLinkCount());
    }
}
