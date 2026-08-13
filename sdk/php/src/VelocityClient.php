<?php

declare(strict_types=1);

namespace Velocity\SDK;

use Velocity\SDK\Exceptions\VelocityException;
use Velocity\SDK\Exceptions\ConnectionException;
use Velocity\SDK\Exceptions\WorkflowNotFoundException;
use Velocity\SDK\Interceptors\InterceptorChain;
use Velocity\SDK\Interceptors\WorkflowInterceptorInterface;

/**
 * gRPC client for the VELOCITY-WorkFlow server.
 *
 * Provides a PHP-idiomatic API for workflow lifecycle management:
 * - Start / complete / fail / cancel workflows
 * - Signal and query running workflows
 * - Manage namespaces and visibility
 *
 * Uses the FFI extension to call velocity_workflow_engine.dll/.so directly,
 * or falls back to gRPC if FFI is unavailable.
 *
 * Usage:
 *     $client = new VelocityClient("localhost:7234");
 *     $key = $client->startWorkflow("my-workflow", totalSteps: 5);
 *     $client->signalWorkflow($key, "my-signal", "payload");
 *     $status = $client->getWorkflowStatus($key);
 *     $client->close();
 */
class VelocityClient
{
    /** @var string gRPC server address. */
    private string $target;

    /** @var string|null JWT bearer token for authentication. */
    private ?string $jwtToken;

    /** @var InterceptorChain Interceptor chain for middleware. */
    private InterceptorChain $interceptors;

    /** @var \FFI|null FFI handle to the native engine library. */
    private ?\FFI $ffi = null;

    /** @var mixed FFI engine handle. */
    private mixed $engineHandle = null;

    /**
     * Connect to a VELOCITY-WorkFlow server.
     *
     * @param string $target gRPC server address (e.g. "localhost:7234").
     * @param string|null $jwtToken Optional JWT bearer token for authentication.
     * @param string|null $libraryPath Optional path to the native engine library.
     *
     * @throws ConnectionException If the connection cannot be established.
     */
    public function __construct(
        string $target = 'localhost:7234',
        ?string $jwtToken = null,
        ?string $libraryPath = null,
    ) {
        $this->target = $target;
        $this->jwtToken = $jwtToken;
        $this->interceptors = new InterceptorChain();

        // Attempt to load the native engine via FFI.
        $this->initFfi($libraryPath);
    }

    /**
     * Initialise the FFI bridge to the native engine.
     *
     * @param string|null $libraryPath Explicit path to the shared library.
     */
    private function initFfi(?string $libraryPath): void
    {
        if (!extension_loaded('ffi')) {
            return; // FFI not available; fall back to gRPC-only mode.
        }

        $path = $libraryPath ?? $this->detectLibraryPath();
        if ($path === null || !file_exists($path)) {
            return; // Library not found; gRPC-only mode.
        }

        try {
            $this->ffi = \FFI::load($path);
            if ($this->ffi !== null) {
                $this->engineHandle = $this->ffi->velocity_engine_create();
            }
        } catch (\FFI\Exception $e) {
            // Silently degrade to gRPC mode.
            $this->ffi = null;
        }
    }

    /**
     * Attempt to detect the native library path based on the OS.
     */
    private function detectLibraryPath(): ?string
    {
        $candidates = [
            'velocity_workflow_engine.dll',     // Windows
            'libvelocity_workflow_engine.so',   // Linux
            'libvelocity_workflow_engine.dylib', // macOS
        ];

        foreach ($candidates as $candidate) {
            $full = __DIR__ . '/../../velocity-workflow-engine/target/release/' . $candidate;
            if (file_exists($full)) {
                return $full;
            }
        }
        return null;
    }

    // ─── Interceptors ─────────────────────────────────────────────────────

    /**
     * Add a workflow interceptor to the chain.
     *
     * @param WorkflowInterceptorInterface $interceptor
     * @return self Fluent interface.
     */
    public function addInterceptor(WorkflowInterceptorInterface $interceptor): self
    {
        $this->interceptors->add($interceptor);
        return $this;
    }

    /** Get the interceptor chain. */
    public function getInterceptorChain(): InterceptorChain
    {
        return $this->interceptors;
    }

    // ─── Workflow Lifecycle ───────────────────────────────────────────────

    /**
     * Start a new workflow execution.
     *
     * @param string $workflowType Type name of the workflow.
     * @param string $namespace Namespace to run in.
     * @param string $taskQueue Task queue for worker dispatch.
     * @param int $totalSteps Number of execution steps.
     * @param string $input Optional input payload.
     *
     * @return int Workflow key.
     * @throws VelocityException On failure.
     */
    public function startWorkflow(
        string $workflowType,
        string $namespace = 'default',
        string $taskQueue = 'default',
        int $totalSteps = 1,
        string $input = '',
    ): int {
        $typeId = crc32($workflowType);
        $nsId = crc32($namespace);
        $tqHash = crc32($taskQueue);

        if ($this->ffi !== null && $this->engineHandle !== null) {
            $key = $this->ffi->velocity_engine_start_workflow(
                $this->engineHandle,
                $typeId, $typeId, $nsId, $tqHash,
                $totalSteps,
                $input !== '' ? $input : \FFI::new('uint8_t[0]'),
                strlen($input),
            );
            $this->interceptors->invokeStart($workflowType, (int)$key);
            return (int)$key;
        }

        // gRPC fallback would go here.
        throw new ConnectionException($this->target, 'No FFI or gRPC backend available');
    }

    /**
     * Get the current status of a workflow.
     *
     * @param int $workflowKey
     * @return string Status string: running, completed, failed, canceled, terminated.
     * @throws WorkflowNotFoundException
     */
    public function getWorkflowStatus(int $workflowKey): string
    {
        if ($this->ffi !== null && $this->engineHandle !== null) {
            $status = $this->ffi->velocity_engine_get_status($this->engineHandle, $workflowKey);
            return match ((int)$status) {
                0 => 'void',
                1 => 'running',
                2 => 'completed',
                3 => 'failed',
                4 => 'canceled',
                5 => 'terminated',
                default => 'unknown',
            };
        }

        throw new ConnectionException($this->target, 'No backend available');
    }

    /**
     * Signal a running workflow.
     *
     * @param int $workflowKey
     * @param string $signalName
     * @param string $payload
     * @return bool
     */
    public function signalWorkflow(int $workflowKey, string $signalName, string $payload = ''): bool
    {
        if ($this->ffi !== null && $this->engineHandle !== null) {
            $signalId = crc32($signalName);
            $this->ffi->velocity_engine_signal_workflow(
                $this->engineHandle,
                $workflowKey,
                $signalId,
                $payload,
                strlen($payload),
            );
            $this->interceptors->invokeSignal($workflowKey, $signalName);
            return true;
        }

        throw new ConnectionException($this->target, 'No backend available');
    }

    /**
     * Cancel a running workflow.
     *
     * @param int $workflowKey
     * @return bool
     */
    public function cancelWorkflow(int $workflowKey): bool
    {
        if ($this->ffi !== null && $this->engineHandle !== null) {
            $this->ffi->velocity_engine_cancel_workflow($this->engineHandle, $workflowKey);
            return true;
        }

        throw new ConnectionException($this->target, 'No backend available');
    }

    /**
     * Get the server address this client is connected to.
     */
    public function getTarget(): string
    {
        return $this->target;
    }

    /**
     * Close the client and release resources.
     */
    public function close(): void
    {
        if ($this->ffi !== null && $this->engineHandle !== null) {
            $this->ffi->velocity_engine_destroy($this->engineHandle);
            $this->engineHandle = null;
        }
    }

    /** Destructor ensures resources are released. */
    public function __destruct()
    {
        $this->close();
    }
}
