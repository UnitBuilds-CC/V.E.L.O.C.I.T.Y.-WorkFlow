<?php

declare(strict_types=1);

namespace Velocity\SDK\Tests;

use PHPUnit\Framework\TestCase;
use Velocity\SDK\Client\VelocityClientInterface;
use Velocity\SDK\Workflow\WorkflowExecution;
use Velocity\SDK\Workflow\WorkflowOptions;
use Velocity\SDK\Activity\ActivityOptions;
use Velocity\SDK\Activity\ActivityStub;
use Velocity\SDK\Testing\MockVelocityClient;

/**
 * Integration tests exercising the full SDK surface:
 * client → workflow options → execution → activity stubs.
 *
 * These tests use the MockVelocityClient to simulate engine responses
 * without requiring a live server or native library.
 */
class IntegrationTest extends TestCase
{
    private MockVelocityClient $client;

    protected function setUp(): void
    {
        $this->client = new MockVelocityClient();
    }

    public function testWorkflowLifecycleWithOptions(): void
    {
        $options = WorkflowOptions::new()
            ->withNamespace('integration-test')
            ->withTaskQueue('high-priority')
            ->withTotalSteps(3)
            ->withExecutionTimeoutMs(5_000);

        $this->assertSame('integration-test', $options->getNamespace());
        $this->assertSame('high-priority', $options->getTaskQueue());
        $this->assertSame(3, $options->getTotalSteps());

        $key = $this->client->startWorkflow('lifecycle-wf', totalSteps: 3);
        $this->assertGreaterThan(0, $key);

        $desc = $this->client->describeWorkflow($key);
        $this->assertSame('running', $desc['status']);
        $this->assertSame(3, $desc['total_steps']);

        $this->assertTrue($this->client->completeWorkflow($key, 'done'));
        $desc = $this->client->describeWorkflow($key);
        $this->assertSame('completed', $desc['status']);
    }

    public function testWorkflowExecutionHandle(): void
    {
        $exec = new WorkflowExecution(
            key: 42,
            workflowType: 'test-type',
            namespace: 'prod',
            status: 'running',
        );

        $this->assertSame(42, $exec->getKey());
        $this->assertSame('test-type', $exec->getWorkflowType());
        $this->assertSame('prod', $exec->getNamespace());
        $this->assertTrue($exec->isRunning());
        $this->assertFalse($exec->isTerminal());

        $exec->setStatus('completed');
        $this->assertFalse($exec->isRunning());
        $this->assertTrue($exec->isTerminal());
    }

    public function testWorkflowOptionsBuilder(): void
    {
        $opts = WorkflowOptions::defaults();
        $this->assertSame('default', $opts->getNamespace());
        $this->assertSame('default', $opts->getTaskQueue());

        $opts->withNamespace('ns1')
             ->withTaskQueue('tq1')
             ->withWorkflowId('wf-123')
             ->withMemo('test memo')
             ->withSearchAttributes(['env' => 'prod']);

        $arr = $opts->toArray();
        $this->assertSame('ns1', $arr['namespace']);
        $this->assertSame('tq1', $arr['task_queue']);
        $this->assertSame('wf-123', $arr['workflow_id']);
        $this->assertSame('test memo', $arr['memo']);
        $this->assertSame(['env' => 'prod'], $arr['search_attributes']);
    }

    public function testActivityOptionsDefaults(): void
    {
        $opts = ActivityOptions::defaults();
        $this->assertSame(10_000, $opts->getStartToCloseTimeoutMs());
        $this->assertSame(60_000, $opts->getScheduleToCloseTimeoutMs());
        $this->assertSame(0, $opts->getHeartbeatTimeoutMs());
        $this->assertSame(1, $opts->getRetryMaxAttempts());
    }

    public function testActivityStubExecution(): void
    {
        $opts = ActivityOptions::new()
            ->withStartToCloseTimeoutMs(5_000)
            ->withRetryMaxAttempts(3)
            ->withTaskQueue('activity-workers');

        $stub = new ActivityStub($this->client, 'ProcessPayment', $opts);

        $this->assertSame('ProcessPayment', $stub->getActivityType());
        $this->assertSame(0, $stub->getExecutionCount());

        $result = $stub->execute('{"amount": 100}');
        $this->assertNotEmpty($result);
        $this->assertSame(1, $stub->getExecutionCount());

        $decoded = json_decode($result, true);
        $this->assertSame('ProcessPayment', $decoded['activity_type']);
        $this->assertSame(1, $decoded['attempt']);
    }

    public function testActivityStubAsyncExecution(): void
    {
        $stub = new ActivityStub($this->client, 'SendEmail');

        $key1 = $stub->executeAsync('to=user@example.com');
        $key2 = $stub->executeAsync('to=admin@example.com');

        $this->assertNotSame($key1, $key2);
        $this->assertSame(2, $stub->getExecutionCount());
    }

    public function testSignalAndCancelWorkflow(): void
    {
        $key = $this->client->startWorkflow('signal-test-wf');

        $this->assertTrue($this->client->signalWorkflow($key, 'approve', 'yes'));
        $signals = $this->client->getSignals($key);
        $this->assertCount(1, $signals);
        $this->assertSame('approve', $signals[0]['signal_name']);

        $this->assertTrue($this->client->cancelWorkflow($key));
        $desc = $this->client->describeWorkflow($key);
        $this->assertSame('canceled', $desc['status']);
    }

    public function testMultipleWorkflowsIsolation(): void
    {
        $key1 = $this->client->startWorkflow('wf-a');
        $key2 = $this->client->startWorkflow('wf-b');

        $this->assertNotSame($key1, $key2);

        $this->client->completeWorkflow($key1, 'result-a');
        $this->assertSame('completed', $this->client->describeWorkflow($key1)['status']);
        $this->assertSame('running', $this->client->describeWorkflow($key2)['status']);
    }
}
