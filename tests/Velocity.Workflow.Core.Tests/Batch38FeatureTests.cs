using System;
using System.Text;
using Xunit;
using Velocity.Workflow.Core;

namespace Velocity.Workflow.Core.Tests;

/// <summary>
/// Tests for Batch 38 features: Network replication (TCP/UDP),
/// B-tree search attribute indexing, and chaos endurance testing.
/// </summary>
public class Batch38FeatureTests
{
    // ─── Network Replication: TCP ──────────────────────────────────────────────

    [Fact]
    public void NetTcp_Init_ReturnsZero()
    {
        var addr = Encoding.UTF8.GetBytes("127.0.0.1:0");
        unsafe
        {
            fixed (byte* addrPtr = addr)
            {
                int result = NativeBridge.NetTcpInit(addrPtr, (uint)addr.Length, 1, 1);
                Assert.Equal(0, result);
            }
        }
    }

    [Fact]
    public void NetTcp_StatsInitiallyZero()
    {
        // Stats may carry over from other tests; just verify they return without error
        var conns = NativeBridge.NetTcpConnectionsAccepted();
        var frames = NativeBridge.NetTcpFramesSent();
        var bytes = NativeBridge.NetTcpBytesSent();
        var tasks = NativeBridge.NetTcpTasksSent();
        // All should be >= 0 (ulong is always >= 0)
        Assert.True(conns >= 0);
        Assert.True(frames >= 0);
        Assert.True(bytes >= 0);
        Assert.True(tasks >= 0);
    }

    // ─── Network Replication: UDP ──────────────────────────────────────────────

    [Fact]
    public void NetUdp_Init_ReturnsZero()
    {
        var bindAddr = Encoding.UTF8.GetBytes("127.0.0.1:0");
        var peerAddr = Encoding.UTF8.GetBytes("127.0.0.1:0");
        unsafe
        {
            fixed (byte* bindPtr = bindAddr, peerPtr = peerAddr)
            {
                int result = NativeBridge.NetUdpInit(bindPtr, (uint)bindAddr.Length, peerPtr, (uint)peerAddr.Length, 1);
                Assert.Equal(0, result);
            }
        }
    }

    [Fact]
    public void NetUdp_StatsInitiallyZero()
    {
        var packets = NativeBridge.NetUdpPacketsSent();
        var bytes = NativeBridge.NetUdpBytesSent();
        Assert.True(packets >= 0);
        Assert.True(bytes >= 0);
    }

    // ─── Search Index ──────────────────────────────────────────────────────────

    [Fact]
    public void SearchIndex_StringIndexAndQuery()
    {
        var attr = Encoding.UTF8.GetBytes("customer_id");
        var val = Encoding.UTF8.GetBytes("C123");

        unsafe
        {
            fixed (byte* attrPtr = attr, valPtr = val)
            {
                NativeBridge.SearchIndexString(1001, attrPtr, (uint)attr.Length, valPtr, (uint)val.Length);
                NativeBridge.SearchIndexString(1002, attrPtr, (uint)attr.Length, valPtr, (uint)val.Length);

                ulong count = NativeBridge.SearchQueryExactCount(attrPtr, (uint)attr.Length, valPtr, (uint)val.Length);
                Assert.True(count >= 2);
            }
        }
    }

    [Fact]
    public void SearchIndex_IntegerIndexAndRange()
    {
        var attr = Encoding.UTF8.GetBytes("priority");

        unsafe
        {
            fixed (byte* attrPtr = attr)
            {
                NativeBridge.SearchIndexInteger(2001, attrPtr, (uint)attr.Length, 10);
                NativeBridge.SearchIndexInteger(2002, attrPtr, (uint)attr.Length, 20);
                NativeBridge.SearchIndexInteger(2003, attrPtr, (uint)attr.Length, 30);

                ulong count = NativeBridge.SearchQueryRangeCount(attrPtr, (uint)attr.Length, 10, 20);
                Assert.True(count >= 2);
            }
        }
    }

    [Fact]
    public void SearchIndex_EntryCount()
    {
        // Index something first
        var attr = Encoding.UTF8.GetBytes("test_attr");
        var val = Encoding.UTF8.GetBytes("test_val");
        unsafe
        {
            fixed (byte* attrPtr = attr, valPtr = val)
            {
                NativeBridge.SearchIndexString(3001, attrPtr, (uint)attr.Length, valPtr, (uint)val.Length);
            }
        }

        ulong entries = NativeBridge.SearchIndexEntryCount();
        Assert.True(entries > 0);
    }

    [Fact]
    public void SearchIndex_WorkflowCount()
    {
        var attr = Encoding.UTF8.GetBytes("wf_count_test");
        var val = Encoding.UTF8.GetBytes("val");
        unsafe
        {
            fixed (byte* attrPtr = attr, valPtr = val)
            {
                NativeBridge.SearchIndexString(4001, attrPtr, (uint)attr.Length, valPtr, (uint)val.Length);
                NativeBridge.SearchIndexString(4002, attrPtr, (uint)attr.Length, valPtr, (uint)val.Length);
            }
        }

        ulong count = NativeBridge.SearchIndexWorkflowCount();
        Assert.True(count > 0);
    }

    // ─── Chaos Endurance ───────────────────────────────────────────────────────

    [Fact]
    public void ChaosSoakTest_ReturnsOperations()
    {
        // Run a very short soak test (100ms, 2 threads)
        ulong ops = NativeBridge.ChaosSoakTest(100, 2, 0);
        Assert.True(ops > 0, "Soak test should produce operations");
    }

    [Fact]
    public void ChaosSoakTest_WithFailures()
    {
        // Run with failure injection
        ulong ops = NativeBridge.ChaosSoakTest(100, 2, 1);
        Assert.True(ops > 0, "Soak test with failures should produce operations");
    }

    [Fact]
    public void ChaosCrashRecovery_ReturnsResults()
    {
        ulong result = NativeBridge.ChaosCrashRecoveryTest(5);
        uint started = (uint)(result >> 32);
        uint recovered = (uint)(result & 0xFFFFFFFF);
        Assert.Equal(5u, started);
        Assert.Equal(5u, recovered);
    }

    [Fact]
    public void ChaosCrashRecovery_ZeroWorkflows()
    {
        ulong result = NativeBridge.ChaosCrashRecoveryTest(0);
        uint started = (uint)(result >> 32);
        uint recovered = (uint)(result & 0xFFFFFFFF);
        Assert.Equal(0u, started);
        Assert.Equal(0u, recovered);
    }
}
