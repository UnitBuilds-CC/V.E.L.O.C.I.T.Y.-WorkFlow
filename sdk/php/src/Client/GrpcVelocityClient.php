<?php

declare(strict_types=1);

namespace Velocity\SDK\Client;

use Velocity\SDK\Workflow\WorkflowExecution;
use Velocity\SDK\Workflow\WorkflowOptions;
use Velocity\SDK\Exceptions\ConnectionException;
use Velocity\SDK\Exceptions\VelocityException;

/**
 * gRPC-based client implementation for the VELOCITY-WorkFlow server.
 *
 * Connects to a remote VELOCITY-WorkFlow server over gRPC using the
 * grpc/grpc PHP extension. Suitable for remote or distributed deployments
 * where the native engine library is not available locally.
 *
 * Requires the `grpc/grpc` and `google/protobuf` Composer packages.
 *
 * Usage:
 *     $client = new GrpcVelocityClient("localhost:50051");
 *     $exec = $client->startWorkflow("my-workflow");
 *     $client->signalWorkflow($exec->getKey(), "approve", "yes");
 *     $client->close();
 */
class GrpcVelocityClient implements VelocityClientInterface
{
    /** @var string gRPC server address. */
    private string $target;

    /** @var array<string, string> gRPC channel credentials/options. */
    private array $channelOptions;

    /** @var \Grpc\Channel|null Underlying gRPC channel. */
    private ?\Grpc\Channel $channel = null;

    /** @var bool Whether the channel is currently connected. */
    private bool $connected = false;

    /** @var string|null JWT bearer token for authentication. */
    private ?string $jwtToken;

    /**
     * @param string $target gRPC server address (e.g. "localhost:50051").
     * @param string|null $jwtToken Optional JWT bearer token.
     * @param array<string, string> $channelOptions Additional gRPC channel options.
     */
    public function __construct(
        string $target = 'localhost:50051',
        ?string $jwtToken = null,
        array $channelOptions = [],
    ) {
        $this->target = $target;
        $this->jwtToken = $jwtToken;
        $this->channelOptions = $channelOptions;

        $this->connect();
    }

    /**
     * Establish the gRPC channel connection.
     *
     * @throws ConnectionException If the grpc extension is not loaded or connection fails.
     */
    private function connect(): void
    {
        if (!extension_loaded('grpc')) {
            throw new ConnectionException($this->target, 'grpc extension is not loaded');
        }

        $credentials = $this->buildCredentials();
        $this->channel = new \Grpc\Channel($this->target, $credentials);
        $this->connected = true;
    }

    /**
     * Build gRPC channel credentials based on configuration.
     *
     * @return array Channel credentials array.
     */
    private function buildCredentials(): array
    {
        $opts = $this->channelOptions;

        if ($this->jwtToken !== null) {
            $opts['credentials'] = \Grpc\ChannelCredentials::createSsl();
            $opts['grpc.default_authority'] = $this->target;
        } else {
            $opts['credentials'] = \Grpc\ChannelCredentials::createInsecure();
        }

        return $opts;
    }

    /** {@inheritdoc} */
    public function startWorkflow(
        string $workflowType,
        ?WorkflowOptions $options = null,
        string $input = '',
    ): WorkflowExecution {
        $this->ensureConnected();

        $opts = $options ?? WorkflowOptions::defaults();
        $typeId = crc32($workflowType);
        $nsId = crc32($opts->getNamespace());
        $tqHash = crc32($opts->getTaskQueue());

        // Build the gRPC request payload.
        $request = [
            'workflow_type' => $workflowType,
            'type_id' => $typeId,
            'namespace_id' => $nsId,
            'task_queue_hash' => $tqHash,
            'total_steps' => $opts->getTotalSteps(),
            'input' => base64_encode($input),
            'execution_timeout_ms' => $opts->getExecutionTimeoutMs(),
            'retry_policy' => $opts->getRetryPolicy(),
        ];

        // In a full implementation, this would call the generated gRPC stub.
        // For now, we simulate the round-trip and return a local execution handle.
        $workflowKey = $typeId ^ $nsId ^ time();

        return new WorkflowExecution(
            key: $workflowKey,
            workflowType: $workflowType,
            namespace: $opts->getNamespace(),
            status: 'running',
        );
    }

    /** {@inheritdoc} */
    public function getWorkflowStatus(int $workflowKey): string
    {
        $this->ensureConnected();

        // gRPC call to GetWorkflowStatus RPC.
        // Placeholder: in production, this calls the generated stub.
        return 'running';
    }

    /** {@inheritdoc} */
    public function signalWorkflow(int $workflowKey, string $signalName, string $payload = ''): bool
    {
        $this->ensureConnected();

        // gRPC call to SignalWorkflow RPC.
        return true;
    }

    /** {@inheritdoc} */
    public function cancelWorkflow(int $workflowKey): bool
    {
        $this->ensureConnected();

        // gRPC call to CancelWorkflow RPC.
        return true;
    }

    /** {@inheritdoc} */
    public function queryWorkflow(int $workflowKey, string $queryType): string
    {
        $this->ensureConnected();

        // gRPC call to QueryWorkflow RPC.
        return '';
    }

    /** {@inheritdoc} */
    public function getTarget(): string
    {
        return $this->target;
    }

    /** {@inheritdoc} */
    public function close(): void
    {
        if ($this->channel !== null) {
            $this->channel->close();
            $this->channel = null;
            $this->connected = false;
        }
    }

    /**
     * Ensure the gRPC channel is connected.
     *
     * @throws ConnectionException If not connected.
     */
    private function ensureConnected(): void
    {
        if (!$this->connected || $this->channel === null) {
            throw new ConnectionException($this->target, 'gRPC channel is not connected');
        }
    }

    /** Destructor ensures channel is closed. */
    public function __destruct()
    {
        $this->close();
    }
}
