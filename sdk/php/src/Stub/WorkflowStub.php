<?php

declare(strict_types=1);

namespace Velocity\SDK\Stub;

use Velocity\SDK\VelocityClient;
use Velocity\SDK\Codec\PayloadCodecInterface;
use Velocity\SDK\Codec\JsonPayloadCodec;

/**
 * Typed workflow execution stub.
 *
 * Provides a convenient interface for starting, signaling, querying,
 * and waiting for workflow results with automatic payload encoding/decoding.
 *
 * Usage:
 *     $stub = (new WorkflowStub($client, 'order-processing'))
 *         ->withNamespace('default')
 *         ->withTaskQueue('orders')
 *         ->withCodec(new JsonPayloadCodec());
 *
 *     $stub->start(['orderId' => '12345']);
 *     $stub->signal('approve', ['approved' => true]);
 *     $result = $stub->result();
 */
class WorkflowStub
{
    private VelocityClient $client;
    private string $workflowType;
    private string $namespace = 'default';
    private string $taskQueue = 'default';
    private PayloadCodecInterface $codec;
    private ?int $workflowKey = null;

    public function __construct(VelocityClient $client, string $workflowType)
    {
        $this->client = $client;
        $this->workflowType = $workflowType;
        $this->codec = new JsonPayloadCodec();
    }

    /** Set the namespace. Returns this for chaining. */
    public function withNamespace(string $namespace): self
    {
        $this->namespace = $namespace;
        return $this;
    }

    /** Set the task queue. Returns this for chaining. */
    public function withTaskQueue(string $taskQueue): self
    {
        $this->taskQueue = $taskQueue;
        return $this;
    }

    /** Set the payload codec. Returns this for chaining. */
    public function withCodec(PayloadCodecInterface $codec): self
    {
        $this->codec = $codec;
        return $this;
    }

    /**
     * Start workflow execution.
     *
     * @param mixed $input Input data (will be encoded via codec)
     * @return self This stub for chaining
     */
    public function start(mixed $input = null): self
    {
        $payload = $input !== null ? $this->codec->encode($input) : '';
        $this->workflowKey = $this->client->startWorkflow(
            $this->workflowType,
            $this->namespace,
            $this->taskQueue,
            $payload
        );
        return $this;
    }

    /**
     * Send a signal to the workflow.
     *
     * @param string $signalName Name of the signal
     * @param mixed $data Signal payload (will be encoded)
     */
    public function signal(string $signalName, mixed $data = null): void
    {
        $this->ensureStarted();
        $payload = $data !== null ? $this->codec->encode($data) : '';
        $this->client->signalWorkflow($this->workflowKey, $signalName, $payload);
    }

    /**
     * Query the workflow state.
     *
     * @param string $queryType Type of query
     * @param mixed $args Query arguments (will be encoded)
     * @return mixed Decoded query result
     */
    public function query(string $queryType, mixed $args = null): mixed
    {
        $this->ensureStarted();
        $payload = $args !== null ? $this->codec->encode($args) : '';
        $result = $this->client->queryWorkflow($this->workflowKey, $queryType, $payload);
        return $result !== null && $result !== '' ? $this->codec->decode($result) : null;
    }

    /**
     * Wait for workflow completion and return the result.
     *
     * @return mixed Decoded workflow result
     */
    public function result(): mixed
    {
        $this->ensureStarted();
        $result = $this->client->waitForCompletion($this->workflowKey);
        return $result !== null && $result !== '' ? $this->codec->decode($result) : null;
    }

    /** Cancel the workflow. */
    public function cancel(): void
    {
        $this->ensureStarted();
        $this->client->cancelWorkflow($this->workflowKey);
    }

    /**
     * Terminate the workflow.
     *
     * @param string $reason Termination reason
     */
    public function terminate(string $reason = ''): void
    {
        $this->ensureStarted();
        $this->client->terminateWorkflow($this->workflowKey, $reason);
    }

    /** Get the workflow key (null if not started). */
    public function getWorkflowKey(): ?int
    {
        return $this->workflowKey;
    }

    private function ensureStarted(): void
    {
        if ($this->workflowKey === null) {
            throw new \RuntimeException('Workflow not started. Call start() first.');
        }
    }
}
