<?php

declare(strict_types=1);

namespace Velocity\SDK\Client;

use Velocity\SDK\Workflow\WorkflowExecution;
use Velocity\SDK\Workflow\WorkflowOptions;
use Velocity\SDK\Exceptions\ConnectionException;
use Velocity\SDK\Exceptions\VelocityException;

/**
 * FFI-based client implementation for the VELOCITY-WorkFlow engine.
 *
 * Calls the native velocity_workflow_engine shared library directly via
 * PHP's FFI extension. Provides the lowest-latency path for co-located
 * deployments where the engine library is available on the same host.
 *
 * Falls back to an error if the FFI extension is not loaded or the
 * native library cannot be found.
 *
 * Usage:
 *     $client = new FfiVelocityClient(libraryPath: "/usr/lib/libvelocity_workflow_engine.so");
 *     $exec = $client->startWorkflow("my-workflow");
 *     $client->close();
 */
class FfiVelocityClient implements VelocityClientInterface
{
    /** @var \FFI Native FFI handle. */
    private \FFI $ffi;

    /** @var mixed Native engine handle. */
    private mixed $engineHandle;

    /** @var string Display target (for interface compliance). */
    private string $target;

    /** @var bool Whether the engine has been initialised. */
    private bool $initialised = false;

    /**
     * Load the native engine library and create an engine instance.
     *
     * @param string|null $libraryPath Explicit path to the shared library.
     * @param string $target Logical target name for display.
     *
     * @throws ConnectionException If FFI is unavailable or library cannot be loaded.
     */
    public function __construct(?string $libraryPath = null, string $target = 'ffi://local')
    {
        $this->target = $target;

        if (!extension_loaded('ffi')) {
            throw new ConnectionException($target, 'FFI extension is not loaded');
        }

        $path = $libraryPath ?? $this->detectLibraryPath();
        if ($path === null || !file_exists($path)) {
            throw new ConnectionException($target, 'Native engine library not found');
        }

        try {
            $this->ffi = \FFI::load($path);
            $this->engineHandle = $this->ffi->velocity_engine_create();
            $this->initialised = true;
        } catch (\FFI\Exception $e) {
            throw new ConnectionException($target, 'Failed to load native engine: ' . $e->getMessage());
        }
    }

    /**
     * Detect the native library path based on the current OS.
     */
    private function detectLibraryPath(): ?string
    {
        $candidates = [
            'velocity_workflow_engine.dll',
            'libvelocity_workflow_engine.so',
            'libvelocity_workflow_engine.dylib',
        ];

        foreach ($candidates as $candidate) {
            $full = __DIR__ . '/../../../velocity-workflow-engine/target/release/' . $candidate;
            if (file_exists($full)) {
                return $full;
            }
        }

        return null;
    }

    /** {@inheritdoc} */
    public function startWorkflow(
        string $workflowType,
        ?WorkflowOptions $options = null,
        string $input = '',
    ): WorkflowExecution {
        $this->ensureInitialised();

        $opts = $options ?? WorkflowOptions::defaults();
        $typeId = crc32($workflowType);
        $nsId = crc32($opts->getNamespace());
        $tqHash = crc32($opts->getTaskQueue());

        $key = $this->ffi->velocity_engine_start_workflow(
            $this->engineHandle,
            $typeId, $typeId, $nsId, $tqHash,
            $opts->getTotalSteps(),
            $input !== '' ? $input : \FFI::new('uint8_t[0]'),
            strlen($input),
        );

        return new WorkflowExecution(
            key: (int)$key,
            workflowType: $workflowType,
            namespace: $opts->getNamespace(),
            status: 'running',
        );
    }

    /** {@inheritdoc} */
    public function getWorkflowStatus(int $workflowKey): string
    {
        $this->ensureInitialised();

        $code = $this->ffi->velocity_engine_get_status($this->engineHandle, $workflowKey);

        return match ((int)$code) {
            0 => 'void',
            1 => 'running',
            2 => 'completed',
            3 => 'failed',
            4 => 'canceled',
            5 => 'terminated',
            default => 'unknown',
        };
    }

    /** {@inheritdoc} */
    public function signalWorkflow(int $workflowKey, string $signalName, string $payload = ''): bool
    {
        $this->ensureInitialised();

        $signalId = crc32($signalName);
        $this->ffi->velocity_engine_signal_workflow(
            $this->engineHandle,
            $workflowKey,
            $signalId,
            $payload,
            strlen($payload),
        );

        return true;
    }

    /** {@inheritdoc} */
    public function cancelWorkflow(int $workflowKey): bool
    {
        $this->ensureInitialised();

        $this->ffi->velocity_engine_cancel_workflow($this->engineHandle, $workflowKey);

        return true;
    }

    /** {@inheritdoc} */
    public function queryWorkflow(int $workflowKey, string $queryType): string
    {
        $this->ensureInitialised();

        // Query support depends on the engine's query handler.
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
        if ($this->initialised && $this->engineHandle !== null) {
            $this->ffi->velocity_engine_destroy($this->engineHandle);
            $this->engineHandle = null;
            $this->initialised = false;
        }
    }

    /**
     * Ensure the engine has been initialised.
     *
     * @throws ConnectionException If not initialised.
     */
    private function ensureInitialised(): void
    {
        if (!$this->initialised) {
            throw new ConnectionException($this->target, 'FFI engine is not initialised');
        }
    }

    /** Destructor ensures resources are released. */
    public function __destruct()
    {
        $this->close();
    }
}
