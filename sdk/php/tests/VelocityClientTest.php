<?php

declare(strict_types=1);

namespace Velocity\SDK\Tests;

use PHPUnit\Framework\TestCase;
use Velocity\SDK\Testing\MockVelocityClient;
use Velocity\SDK\Testing\TestWorkflowEnvironment;
use Velocity\SDK\Exceptions\WorkflowNotFoundException;
use Velocity\SDK\Exceptions\WorkflowAlreadyCompletedException;
use Velocity\SDK\Interceptors\InterceptorChain;
use Velocity\SDK\Interceptors\LoggingInterceptor;
use Velocity\SDK\Interceptors\MetricsInterceptor;

class VelocityClientTest extends TestCase
{
    // ─── MockClient tests ────────────────────────────────────────────────

    public function testStartWorkflowReturnsKey(): void
    {
        $client = new MockVelocityClient();
        $key = $client->startWorkflow('test-workflow');
        $this->assertGreaterThan(0, $key);
    }

    public function testDescribeWorkflowReturnsRunning(): void
    {
        $client = new MockVelocityClient();
        $key = $client->startWorkflow('test-workflow', totalSteps: 5);
        $desc = $client->describeWorkflow($key);
        $this->assertSame('running', $desc['status']);
        $this->assertSame(5, $desc['total_steps']);
    }

    public function testCompleteWorkflowSetsCompleted(): void
    {
        $client = new MockVelocityClient();
        $key = $client->startWorkflow('test-workflow');
        $this->assertTrue($client->completeWorkflow($key, 'done'));
        $desc = $client->describeWorkflow($key);
        $this->assertSame('completed', $desc['status']);
    }

    public function testDescribeNonexistentThrows(): void
    {
        $client = new MockVelocityClient();
        $this->expectException(WorkflowNotFoundException::class);
        $client->describeWorkflow(99999);
    }

    public function testCompleteAlreadyCompletedThrows(): void
    {
        $client = new MockVelocityClient();
        $key = $client->startWorkflow('test-workflow');
        $client->completeWorkflow($key);
        $this->expectException(WorkflowAlreadyCompletedException::class);
        $client->completeWorkflow($key);
    }

    public function testSignalWorkflow(): void
    {
        $client = new MockVelocityClient();
        $key = $client->startWorkflow('test-workflow');
        $this->assertTrue($client->signalWorkflow($key, 'my-signal', 'payload'));
        $signals = $client->getSignals($key);
        $this->assertCount(1, $signals);
        $this->assertSame('my-signal', $signals[0]['signal_name']);
    }

    public function testCancelWorkflow(): void
    {
        $client = new MockVelocityClient();
        $key = $client->startWorkflow('test-workflow');
        $this->assertTrue($client->cancelWorkflow($key));
        $desc = $client->describeWorkflow($key);
        $this->assertSame('canceled', $desc['status']);
    }

    // ─── TestWorkflowEnvironment tests ───────────────────────────────────

    public function testEnvAssertWorkflowCompleted(): void
    {
        $env = new TestWorkflowEnvironment();
        $key = $env->startWorkflow('test-workflow');
        $env->completeWorkflow($key, 'ok');
        $env->assertWorkflowCompleted($key); // Should not throw.
        $this->assertTrue(true);
    }

    public function testEnvAssertSignalReceived(): void
    {
        $env = new TestWorkflowEnvironment();
        $key = $env->startWorkflow('test-workflow');
        $env->signalWorkflow($key, 'approval', 'yes');
        $env->assertSignalReceived($key, 'approval'); // Should not throw.
        $this->assertTrue(true);
    }

    public function testEnvReset(): void
    {
        $env = new TestWorkflowEnvironment();
        $env->startWorkflow('test-workflow');
        $env->reset();
        $this->expectException(WorkflowNotFoundException::class);
        $env->getClient()->describeWorkflow(1);
    }

    // ─── Interceptor tests ───────────────────────────────────────────────

    public function testInterceptorChainInvokesInOrder(): void
    {
        $chain = new InterceptorChain();
        $metrics = new MetricsInterceptor();
        $chain->add($metrics);
        $chain->add(new LoggingInterceptor());

        $chain->invokeStart('test-type', 1);
        $chain->invokeComplete(1, 'result');

        $snapshot = $metrics->getMetrics();
        $this->assertSame(1, $snapshot['workflow_starts']);
        $this->assertSame(1, $snapshot['workflow_completions']);
    }
}
