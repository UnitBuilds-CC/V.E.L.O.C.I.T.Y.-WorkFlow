<?php
/**
 * Velocity PHP Migration Tool
 *
 * Scans a PHP codebase for Temporal, Restate, or DBOS workflow patterns
 * and converts them to Velocity PHP SDK workflows.
 *
 * Usage:
 *   php sdk/php/bin/velocity-migrate --src ./my_project --from temporal
 *   php sdk/php/bin/velocity-migrate --src workflow.php --from auto
 *   php sdk/php/bin/velocity-migrate --detect ./my_project
 */

namespace VelocitySDK\Migrate;

class MigrationTool
{
    /** Pattern definition */
    private array $name;
    private string $sourcePattern;
    private string $targetTemplate;
    private string $sourceFramework;

    /** ─── Temporal → Velocity Patterns ──────────────────────────────────── */

    public static function getTemporalPatterns(): array
    {
        return [
            // Import/use replacements
            ['name' => 'temporal-use-workflow',
             'pattern' => '/use\s+Temporal\\\\Workflow\\\\/',
             'target'  => 'use VelocitySDK\Workflow;',
             'framework' => 'temporal'],
            ['name' => 'temporal-use-activity',
             'pattern' => '/use\s+Temporal\\\\Activity\\\\/',
             'target'  => 'use VelocitySDK\Activity;',
             'framework' => 'temporal'],
            ['name' => 'temporal-use-client',
             'pattern' => '/use\s+Temporal\\\\Client\\\\/',
             'target'  => 'use VelocitySDK\Client\GrpcVelocityClient;',
             'framework' => 'temporal'],
            ['name' => 'temporal-use-worker',
             'pattern' => '/use\s+Temporal\\\\Worker\\\\/',
             'target'  => 'use VelocitySDK\Worker\Worker;',
             'framework' => 'temporal'],

            // Attribute replacements
            ['name' => 'temporal-workflow-method',
             'pattern' => '/#\[WorkflowMethod/',
             'target'  => '#[Workflow',
             'framework' => 'temporal'],
            ['name' => 'temporal-activity-method',
             'pattern' => '/#\[ActivityMethod/',
             'target'  => '#[Activity',
             'framework' => 'temporal'],
            ['name' => 'temporal-signal-method',
             'pattern' => '/#\[SignalMethod/',
             'target'  => '#[Signal',
             'framework' => 'temporal'],
            ['name' => 'temporal-query-method',
             'pattern' => '/#\[QueryMethod/',
             'target'  => '#[Query',
             'framework' => 'temporal'],

            // Method call replacements
            ['name' => 'temporal-activity-stub',
             'pattern' => '/\$workflow->newActivityStub\(\s*(\w+)::class\s*\)/',
             'target'  => '$ctx->newActivityStub($1::class)',
             'framework' => 'temporal'],
            ['name' => 'temporal-sleep',
             'pattern' => '/Workflow::sleep\(/',
             'target'  => '$ctx->sleep(',
             'framework' => 'temporal'],
            ['name' => 'temporal-side-effect',
             'pattern' => '/Workflow::sideEffect\(/',
             'target'  => '$ctx->sideEffect(',
             'framework' => 'temporal'],
            ['name' => 'temporal-signal-channel',
             'pattern' => '/Workflow::getSignalChannel\(\s*[\'"](\w+)[\'"]\s*\)/',
             'target'  => '$ctx->getSignalChannel(\'$1\')',
             'framework' => 'temporal'],

            // Client/Worker
            ['name' => 'temporal-client-create',
             'pattern' => '/TemporalClient::create\(/',
             'target'  => 'new GrpcVelocityClient(',
             'framework' => 'temporal'],
            ['name' => 'temporal-worker-new',
             'pattern' => '/new\s+WorkerFactory\(/',
             'target'  => 'new Worker(',
             'framework' => 'temporal'],
            ['name' => 'temporal-start-workflow',
             'pattern' => '/\$client->start\s*\(\s*(\w+)::class/',
             'target'  => '$client->executeWorkflow($1::class',
             'framework' => 'temporal'],
            // Search attributes
            ['name' => 'temporal-search-attributes',
             'pattern' => '/Workflow::getSearchAttributes\(/',
             'target'  => '$ctx->getSearchAttributes(',
             'framework' => 'temporal'],
            // Memo
            ['name' => 'temporal-memo',
             'pattern' => '/Workflow::getMemo\(/',
             'target'  => '$ctx->getMemo(',
             'framework' => 'temporal'],
            // Update handler
            ['name' => 'temporal-update-handler',
             'pattern' => '/#\[UpdateMethod\]/',
             'target'  => '#[UpdateMethod]',
             'framework' => 'temporal'],
            // Continue-as-new
            ['name' => 'temporal-continue-as-new',
             'pattern' => '/Workflow::continueAsNew\(/',
             'target'  => '$ctx->continueAsNew(',
             'framework' => 'temporal'],
            // ─── Child Workflow Patterns ─────────────────────────────────────────────
            ['name' => 'temporal-execute-child-workflow',
             'pattern' => '/Workflow::executeChildWorkflow\(/',
             'target'  => '$ctx->executeChildWorkflow(',
             'framework' => 'temporal'],
            ['name' => 'temporal-child-workflow-options',
             'pattern' => '/new\s+ChildWorkflowOptions\(/',
             'target'  => 'new ChildWorkflowOptions(',
             'framework' => 'temporal'],
            ['name' => 'temporal-child-workflow-future',
             'pattern' => '/ChildWorkflowFuture/',
             'target'  => 'ChildWorkflowFuture',
             'framework' => 'temporal'],
            // ─── Activity Options Patterns ───────────────────────────────────────────
            ['name' => 'temporal-activity-options',
             'pattern' => '/new\s+ActivityOptions\(/',
             'target'  => 'new ActivityOptions(',
             'framework' => 'temporal'],
            ['name' => 'temporal-execute-local-activity',
             'pattern' => '/Workflow::executeLocalActivity\(/',
             'target'  => '$ctx->executeLocalActivity(',
             'framework' => 'temporal'],
            ['name' => 'temporal-local-activity-options',
             'pattern' => '/new\s+LocalActivityOptions\(/',
             'target'  => 'new LocalActivityOptions(',
             'framework' => 'temporal'],
            // ─── Coroutine & Concurrency Patterns ────────────────────────────────────
            ['name' => 'temporal-async-run',
             'pattern' => '/Async::run\(/',
             'target'  => '$ctx->asyncRun(',
             'framework' => 'temporal'],
            ['name' => 'temporal-workflow-await',
             'pattern' => '/Workflow::await\(/',
             'target'  => '$ctx->await(',
             'framework' => 'temporal'],
            ['name' => 'temporal-workflow-await-with-timeout',
             'pattern' => '/Workflow::awaitWithTimeout\(/',
             'target'  => '$ctx->awaitWithTimeout(',
             'framework' => 'temporal'],
            ['name' => 'temporal-promise',
             'pattern' => '/PromiseInterface/',
             'target'  => 'PromiseInterface',
             'framework' => 'temporal'],
            // ─── Relay/Nexus Operation Patterns ──────────────────────────────────────
            ['name' => 'temporal-new-nexus-client',
             'pattern' => '/Workflow::newNexusClient\(/',
             'target'  => '$ctx->newRelayClient(',
             'framework' => 'temporal'],
            ['name' => 'temporal-nexus-execute-operation',
             'pattern' => '/\$nexusClient->executeOperation\(/',
             'target'  => '$relayClient->execute(',
             'framework' => 'temporal'],
            ['name' => 'temporal-nexus-operation-options',
             'pattern' => '/new\s+NexusOperationOptions\(/',
             'target'  => 'new RelayOperationOptions(',
             'framework' => 'temporal'],
            // ─── Activity Context Patterns ───────────────────────────────────────────
            ['name' => 'temporal-activity-get-info',
             'pattern' => '/Activity::getInfo\(/',
             'target'  => '$ctx->getInfo(',
             'framework' => 'temporal'],
            ['name' => 'temporal-activity-record-heartbeat',
             'pattern' => '/Activity::heartbeat\(/',
             'target'  => '$ctx->heartbeat(',
             'framework' => 'temporal'],
            // ─── Workflow Context Patterns ───────────────────────────────────────────
            ['name' => 'temporal-workflow-get-info',
             'pattern' => '/Workflow::getInfo\(/',
             'target'  => '$ctx->getWorkflowInfo(',
             'framework' => 'temporal'],
            ['name' => 'temporal-workflow-get-logger',
             'pattern' => '/Workflow::getLogger\(/',
             'target'  => '$ctx->logger(',
             'framework' => 'temporal'],
            ['name' => 'temporal-workflow-with-cancel',
             'pattern' => '/Workflow::withCancel\(/',
             'target'  => '$ctx->withCancel(',
             'framework' => 'temporal'],
            ['name' => 'temporal-signal-external-workflow',
             'pattern' => '/Workflow::signalExternalWorkflow\(/',
             'target'  => '$ctx->signalExternalWorkflow(',
             'framework' => 'temporal'],
            ['name' => 'temporal-workflow-get-version',
             'pattern' => '/Workflow::getVersion\(/',
             'target'  => '$ctx->getVersion(',
             'framework' => 'temporal'],
            ['name' => 'temporal-upsert-search-attributes',
             'pattern' => '/Workflow::upsertSearchAttributes\(/',
             'target'  => '$ctx->upsertSearchAttributes(',
             'framework' => 'temporal'],
            ['name' => 'temporal-upsert-memo',
             'pattern' => '/Workflow::upsertMemo\(/',
             'target'  => '$ctx->upsertMemo(',
             'framework' => 'temporal'],
            // ─── Error Handling Patterns ─────────────────────────────────────────────
            ['name' => 'temporal-new-application-error',
             'pattern' => '/new\s+ApplicationError\(/',
             'target'  => 'new VelocityApplicationError(',
             'framework' => 'temporal'],
            ['name' => 'temporal-canceled-error',
             'pattern' => '/CanceledException/',
             'target'  => 'VelocityCanceledException',
             'framework' => 'temporal'],
            ['name' => 'temporal-import-nexus-package',
             'pattern' => '/use\s+Temporal\\\\Nexus\\\\/',
             'target'  => 'use VelocitySDK\\Relay;',
             'framework' => 'temporal'],
        ];
    }

    /** ─── Restate → Velocity Patterns ─────────────────────────────────── */

    public static function getRestatePatterns(): array
    {
        return [
            ['name' => 'restate-use',
             'pattern' => '/use\s+Restate\\\\/',
             'target'  => 'use VelocitySDK;',
             'framework' => 'restate'],
            ['name' => 'restate-context-run',
             'pattern' => '/\$context->run\(/',
             'target'  => '$ctx->executeActivity(',
             'framework' => 'restate'],
            ['name' => 'restate-context-call',
             'pattern' => '/\$context->call\(/',
             'target'  => '$ctx->executeActivity(',
             'framework' => 'restate'],
            ['name' => 'restate-context-sleep',
             'pattern' => '/\$context->sleep\(/',
             'target'  => '$ctx->sleep(',
             'framework' => 'restate'],
            ['name' => 'restate-context-get',
             'pattern' => '/\$context->get\(\s*[\'"](\w+)[\'"]\s*\)/',
             'target'  => '$ctx->getState(\'$1\')',
             'framework' => 'restate'],
            ['name' => 'restate-context-set',
             'pattern' => '/\$context->set\(\s*[\'"]+(\w+)[\'"]+\s*,/',
             'target'  => '$ctx->setState(\'$1\',',
             'framework' => 'restate'],
            // Idempotency key
            ['name' => 'restate-idempotency-key',
             'pattern' => '/\$context->idempotencyKey\(/',
             'target'  => '$ctx->idempotencyKey(',
             'framework' => 'restate'],
            // Service client
            ['name' => 'restate-service-client',
             'pattern' => '/RestateClient::create\(/',
             'target'  => 'VelocityClient::create(',
             'framework' => 'restate'],
        ];
    }

    /** ─── DBOS → Velocity Patterns ────────────────────────────────────── */

    public static function getDbosPatterns(): array
    {
        return [
            ['name' => 'dbos-use',
             'pattern' => '/use\s+DBOS\\\\/',
             'target'  => 'use VelocitySDK;',
             'framework' => 'dbos'],
            ['name' => 'dbos-workflow-attr',
             'pattern' => '/#\[DBOS\\\\Workflow/',
             'target'  => '#[Workflow',
             'framework' => 'dbos'],
            ['name' => 'dbos-transaction-attr',
             'pattern' => '/#\[DBOS\\\\Transaction/',
             'target'  => '#[Activity',
             'framework' => 'dbos'],
            ['name' => 'dbos-sleep',
             'pattern' => '/DBOS::sleep\(/',
             'target'  => '$ctx->sleep(',
             'framework' => 'dbos'],
            ['name' => 'dbos-recv',
             'pattern' => '/DBOS::recv\(/',
             'target'  => '$ctx->recv(',
             'framework' => 'dbos'],
            ['name' => 'dbos-set-event',
             'pattern' => '/DBOS::setEvent\(/',
             'target'  => '$ctx->setEvent(',
             'framework' => 'dbos'],
            // Queue operations
            ['name' => 'dbos-queue-enqueue',
             'pattern' => '/DBOS::enqueue\(/',
             'target'  => '$ctx->enqueue(',
             'framework' => 'dbos'],
            ['name' => 'dbos-queue-dequeue',
             'pattern' => '/DBOS::dequeue\(/',
             'target'  => '$ctx->dequeue(',
             'framework' => 'dbos'],
            // HTTP handler
            ['name' => 'dbos-http-handler',
             'pattern' => '/#\[DBOS\\\\HttpHandler/',
             'target'  => '#[HttpHandler',
             'framework' => 'dbos'],
        ];
    }

    /** ─── Inter-Flavor Migration Patterns (Server ↔ Binary ↔ Embedded) ─── */

    public static function getInterFlavorPatterns(string $source, string $target): array
    {
        $all = [
            'server→binary' => [
                ['name' => 's2b-import', 'pattern' => '/Velocity\\\\Workflow/', 'target' => 'Velocity\\Binary\\Workflow', 'framework' => 'server'],
                ['name' => 's2b-execute-activity', 'pattern' => '/\$ctx->executeActivity\(/', 'target' => '$ctx->invoke(', 'framework' => 'server'],
                ['name' => 's2b-child-workflow', 'pattern' => '/\$ctx->executeChildWorkflow\(/', 'target' => '$ctx->invoke(', 'framework' => 'server'],
                ['name' => 's2b-get-signal', 'pattern' => '/\$ctx->getSignalChannel\(/', 'target' => '$ctx->promise(', 'framework' => 'server'],
                ['name' => 's2b-wait-signal', 'pattern' => '/\$ctx->waitForSignal\(/', 'target' => '$ctx->awaitCondition(', 'framework' => 'server'],
                ['name' => 's2b-set-state', 'pattern' => '/\$ctx->setState\(/', 'target' => '$ctx->set(', 'framework' => 'server'],
                ['name' => 's2b-get-state', 'pattern' => '/\$ctx->getState\(/', 'target' => '$ctx->get(', 'framework' => 'server'],
                ['name' => 's2b-relay-client', 'pattern' => '/\$ctx->newRelayClient\(/', 'target' => '$ctx->newServiceClient(', 'framework' => 'server'],
            ],
            'server→embedded' => [
                ['name' => 's2e-import', 'pattern' => '/Velocity\\\\Workflow/', 'target' => 'Velocity\\Embedded\\Workflow', 'framework' => 'server'],
                ['name' => 's2e-execute-activity', 'pattern' => '/\$ctx->executeActivity\(/', 'target' => '$ctx->invoke(', 'framework' => 'server'],
                ['name' => 's2e-child-workflow', 'pattern' => '/\$ctx->executeChildWorkflow\(/', 'target' => '$ctx->startChildWorkflow(', 'framework' => 'server'],
                ['name' => 's2e-get-signal', 'pattern' => '/\$ctx->getSignalChannel\(/', 'target' => '$ctx->awaitSignal(', 'framework' => 'server'],
                ['name' => 's2e-relay-client', 'pattern' => '/\$ctx->newRelayClient\(/', 'target' => '$ctx->newClient(', 'framework' => 'server'],
            ],
            'binary→server' => [
                ['name' => 'b2s-import', 'pattern' => '/Velocity\\\\Binary\\\\Workflow/', 'target' => 'Velocity\\Workflow', 'framework' => 'binary'],
                ['name' => 'b2s-invoke', 'pattern' => '/\$ctx->invoke\(/', 'target' => '$ctx->executeActivity(', 'framework' => 'binary'],
                ['name' => 'b2s-promise', 'pattern' => '/\$ctx->promise\(/', 'target' => '$ctx->getSignalChannel(', 'framework' => 'binary'],
                ['name' => 'b2s-set', 'pattern' => '/\$ctx->set\(/', 'target' => '$ctx->setState(', 'framework' => 'binary'],
                ['name' => 'b2s-get', 'pattern' => '/\$ctx->get\(/', 'target' => '$ctx->getState(', 'framework' => 'binary'],
                ['name' => 'b2s-service-client', 'pattern' => '/\$ctx->newServiceClient\(/', 'target' => '$ctx->newRelayClient(', 'framework' => 'binary'],
            ],
            'binary→embedded' => [
                ['name' => 'b2e-import', 'pattern' => '/Velocity\\\\Binary\\\\Workflow/', 'target' => 'Velocity\\Embedded\\Workflow', 'framework' => 'binary'],
                ['name' => 'b2e-promise', 'pattern' => '/\$ctx->promise\(/', 'target' => '$ctx->awaitSignal(', 'framework' => 'binary'],
                ['name' => 'b2e-set', 'pattern' => '/\$ctx->set\(/', 'target' => '$ctx->setState(', 'framework' => 'binary'],
                ['name' => 'b2e-get', 'pattern' => '/\$ctx->get\(/', 'target' => '$ctx->getState(', 'framework' => 'binary'],
                ['name' => 'b2e-service-client', 'pattern' => '/\$ctx->newServiceClient\(/', 'target' => '$ctx->newClient(', 'framework' => 'binary'],
            ],
            'embedded→server' => [
                ['name' => 'e2s-import', 'pattern' => '/Velocity\\\\Embedded\\\\Workflow/', 'target' => 'Velocity\\Workflow', 'framework' => 'embedded'],
                ['name' => 'e2s-await-signal', 'pattern' => '/\$ctx->awaitSignal\(/', 'target' => '$ctx->getSignalChannel(', 'framework' => 'embedded'],
                ['name' => 'e2s-child-wf', 'pattern' => '/\$ctx->startChildWorkflow\(/', 'target' => '$ctx->executeChildWorkflow(', 'framework' => 'embedded'],
                ['name' => 'e2s-client', 'pattern' => '/\$ctx->newClient\(/', 'target' => '$ctx->newRelayClient(', 'framework' => 'embedded'],
            ],
            'embedded→binary' => [
                ['name' => 'e2b-import', 'pattern' => '/Velocity\\\\Embedded\\\\Workflow/', 'target' => 'Velocity\\Binary\\Workflow', 'framework' => 'embedded'],
                ['name' => 'e2b-await-signal', 'pattern' => '/\$ctx->awaitSignal\(/', 'target' => '$ctx->promise(', 'framework' => 'embedded'],
                ['name' => 'e2b-set-state', 'pattern' => '/\$ctx->setState\(/', 'target' => '$ctx->set(', 'framework' => 'embedded'],
                ['name' => 'e2b-get-state', 'pattern' => '/\$ctx->getState\(/', 'target' => '$ctx->get(', 'framework' => 'embedded'],
                ['name' => 'e2b-child-wf', 'pattern' => '/\$ctx->startChildWorkflow\(/', 'target' => '$ctx->invoke(', 'framework' => 'embedded'],
                ['name' => 'e2b-client', 'pattern' => '/\$ctx->newClient\(/', 'target' => '$ctx->newServiceClient(', 'framework' => 'embedded'],
            ],
        ];
        return $all["$source→$target"] ?? [];
    }

    /** ─── Framework Detection ─────────────────────────────────────────── */

    public static function detectFramework(string $content): array
    {
        $scores = ['temporal' => 0, 'restate' => 0, 'dbos' => 0, 'server' => 0, 'binary' => 0, 'embedded' => 0];
        $evidence = ['temporal' => [], 'restate' => [], 'dbos' => [], 'server' => [], 'binary' => [], 'embedded' => []];

        // Temporal
        if (str_contains($content, 'Temporal\\Workflow')) { $scores['temporal'] += 3; $evidence['temporal'][] = 'Temporal workflow import'; }
        if (str_contains($content, 'Temporal\\Activity')) { $scores['temporal'] += 3; $evidence['temporal'][] = 'Temporal activity import'; }
        if (str_contains($content, '#[WorkflowMethod')) { $scores['temporal'] += 2; $evidence['temporal'][] = '#[WorkflowMethod]'; }
        if (str_contains($content, 'Workflow::sleep')) { $scores['temporal'] += 1; $evidence['temporal'][] = 'Workflow::sleep'; }
        if (str_contains($content, 'Workflow::getSearchAttributes') || str_contains($content, 'Workflow::continueAsNew')) { $scores['temporal'] += 1; $evidence['temporal'][] = 'Temporal advanced workflow API'; }
        if (str_contains($content, '#[UpdateMethod]')) { $scores['temporal'] += 1; $evidence['temporal'][] = 'UpdateMethod handler'; }

        // Restate
        if (str_contains($content, 'Restate\\')) { $scores['restate'] += 3; $evidence['restate'][] = 'Restate import'; }
        if (str_contains($content, '$context->run(')) { $scores['restate'] += 1; $evidence['restate'][] = '$context->run()'; }
        if (str_contains($content, '$context->idempotencyKey') || str_contains($content, 'RestateClient')) { $scores['restate'] += 1; $evidence['restate'][] = 'Restate idempotency/client'; }

        // DBOS
        if (str_contains($content, 'DBOS\\')) { $scores['dbos'] += 3; $evidence['dbos'][] = 'DBOS import'; }
        if (str_contains($content, '#[DBOS\Workflow')) { $scores['dbos'] += 2; $evidence['dbos'][] = '#[DBOS\Workflow]'; }
        if (str_contains($content, 'DBOS::enqueue') || str_contains($content, '#[DBOS\HttpHandler')) { $scores['dbos'] += 1; $evidence['dbos'][] = 'DBOS queue/HTTP handler'; }

        // Velocity Server
        if (str_contains($content, 'Velocity\\Workflow') && !str_contains($content, 'Velocity\\Binary') && !str_contains($content, 'Velocity\\Embedded')) { $scores['server'] += 3; $evidence['server'][] = 'Velocity Server import'; }
        if (str_contains($content, '$ctx->executeActivity(')) { $scores['server'] += 1; $evidence['server'][] = '$ctx->executeActivity()'; }
        if (str_contains($content, '$ctx->getSignalChannel(')) { $scores['server'] += 1; $evidence['server'][] = '$ctx->getSignalChannel()'; }

        // Velocity Binary
        if (str_contains($content, 'Velocity\\Binary')) { $scores['binary'] += 3; $evidence['binary'][] = 'Velocity Binary import'; }
        if (str_contains($content, '$ctx->newServiceClient(')) { $scores['binary'] += 1; $evidence['binary'][] = '$ctx->newServiceClient()'; }

        // Velocity Embedded
        if (str_contains($content, 'Velocity\\Embedded')) { $scores['embedded'] += 3; $evidence['embedded'][] = 'Velocity Embedded import'; }
        if (str_contains($content, '$ctx->awaitSignal(')) { $scores['embedded'] += 1; $evidence['embedded'][] = '$ctx->awaitSignal()'; }

        $best = 'temporal';
        $bestScore = 0;
        foreach ($scores as $fw => $score) {
            if ($score > $bestScore) { $best = $fw; $bestScore = $score; }
        }
        $total = array_sum($scores);
        $confidence = $total > 0 ? $bestScore / $total : 0.0;

        return ['framework' => $best, 'confidence' => $confidence, 'evidence' => $evidence[$best]];
    }

    /** ─── File Migration ──────────────────────────────────────────────── */

    public static function migrateFile(string $content, string $sourceFramework, string $targetFlavor = 'server'): array
    {
        $result = ['success' => true, 'detected' => '', 'transformations' => 0, 'error' => null];

        if ($sourceFramework === 'auto') {
            $detection = self::detectFramework($content);
            $result['detected'] = $detection['framework'];
            if ($detection['confidence'] < 0.3) {
                return [$content, ['success' => false, 'error' => 'Low confidence detection']];
            }
            $sourceFramework = $detection['framework'];
        } else {
            $result['detected'] = $sourceFramework;
        }

        // Check if this is an inter-flavor migration
        $velocityFlavors = ['server', 'binary', 'embedded'];
        if (in_array($sourceFramework, $velocityFlavors) && $sourceFramework !== $targetFlavor) {
            $patterns = self::getInterFlavorPatterns($sourceFramework, $targetFlavor);
            if (empty($patterns)) {
                return [$content, ['success' => false, 'error' => "No inter-flavor patterns: $sourceFramework → $targetFlavor"]];
            }
            $migrated = $content;
            $count = 0;
            foreach ($patterns as $p) {
                $newText = preg_replace($p['pattern'], $p['target'], $migrated, -1, $n);
                if ($n > 0) {
                    $migrated = $newText;
                    $count += $n;
                }
            }
            $result['transformations'] = $count;
            return [$migrated, $result];
        }

        $patterns = match($sourceFramework) {
            'temporal' => self::getTemporalPatterns(),
            'restate'  => self::getRestatePatterns(),
            'dbos'     => self::getDbosPatterns(),
            default    => null,
        };

        if ($patterns === null) {
            return [$content, ['success' => false, 'error' => "Unknown framework: $sourceFramework"]];
        }

        $migrated = $content;
        $count = 0;
        foreach ($patterns as $p) {
            $newText = preg_replace($p['pattern'], $p['target'], $migrated, -1, $n);
            if ($n > 0) {
                $migrated = $newText;
                $count += $n;
            }
        }
        $result['transformations'] = $count;

        return [$migrated, $result];
    }

    /** ─── Project Scanner ─────────────────────────────────────────────── */

    private static array $SKIP_DIRS = ['vendor', '.git', 'node_modules', 'var', 'cache'];

    public static function scanPhpFiles(string $rootDir): array
    {
        $files = [];
        $iterator = new \RecursiveDirectoryIterator($rootDir, \RecursiveDirectoryIterator::SKIP_DOTS);
        $filter = new \RecursiveCallbackFilterIterator($iterator, function ($current) {
            if ($current->isDir()) {
                return !in_array($current->getFilename(), self::$SKIP_DIRS);
            }
            return true;
        });

        foreach (new \RecursiveIteratorIterator($filter) as $file) {
            if ($file->isFile() && str_ends_with($file->getFilename(), '.php')) {
                $files[] = $file->getPathname();
            }
        }
        return $files;
    }

    public static function hasWorkflowContent(string $content): bool
    {
        $indicators = ['Temporal\\', 'Restate\\', 'DBOS\\', '#[WorkflowMethod', '#[ActivityMethod', 'Workflow::sleep', 'Velocity\\Workflow', 'Velocity\\Binary', 'Velocity\\Embedded'];
        foreach ($indicators as $ind) {
            if (str_contains($content, $ind)) return true;
        }
        return false;
    }

    /** ─── Bulk Migration ──────────────────────────────────────────────── */

    public static function bulkMigrate(string $sourceDir, string $outputDir, string $from, bool $dryRun = false, string $targetFlavor = 'server'): array
    {
        $files = self::scanPhpFiles($sourceDir);
        $results = ['total' => count($files), 'migrated' => 0, 'failed' => 0, 'skipped' => 0, 'details' => []];

        foreach ($files as $filePath) {
            $content = file_get_contents($filePath);
            if (!self::hasWorkflowContent($content)) { $results['skipped']++; continue; }

            [$migrated, $result] = self::migrateFile($content, $from, $targetFlavor);

            if ($result['success'] && !$dryRun) {
                $relPath = str_replace($sourceDir . DIRECTORY_SEPARATOR, '', $filePath);
                $outPath = $outputDir . DIRECTORY_SEPARATOR . $relPath;
                @mkdir(dirname($outPath), 0755, true);
                file_put_contents($outPath, $migrated);
                $results['migrated']++;
            } elseif ($result['success']) {
                $results['migrated']++;
            } else {
                $results['failed']++;
            }

            $results['details'][] = array_merge($result, ['path' => $filePath]);
        }

        return $results;
    }
}

// ─── CLI Entry Point ─────────────────────────────────────────────────────────

if (php_sapi_name() === 'cli' && basename(__FILE__) === basename($GLOBALS['argv'][0] ?? '')) {
    $args = array_slice($GLOBALS['argv'], 1);
    $src = null; $from = 'auto'; $to = 'server'; $output = null; $dryRun = false; $detect = false;

    for ($i = 0; $i < count($args); $i++) {
        switch ($args[$i]) {
            case '--src': $src = $args[++$i]; break;
            case '--from': $from = $args[++$i]; break;
            case '--to': $to = $args[++$i]; break;
            case '--output': case '-o': $output = $args[++$i]; break;
            case '--dry-run': $dryRun = true; break;
            case '--detect': $detect = true; break;
            case '--help': case '-h':
                echo "Velocity PHP Migration Tool\n\n";
                echo "Usage:\n";
                echo "  php migrate.php --src <file|dir> --from <temporal|restate|dbos|auto>\n";
                echo "  php migrate.php --detect <dir>\n";
                exit(0);
        }
    }

    if (!$src) { fwrite(STDERR, "Error: --src is required\n"); exit(1); }

    if ($detect) {
        $files = MigrationTool::scanPhpFiles($src);
        echo "Scanning " . count($files) . " PHP files in $src...\n";
        foreach ($files as $f) {
            $content = file_get_contents($f);
            $d = MigrationTool::detectFramework($content);
            if ($d['confidence'] > 0.3) {
                $rel = str_replace($src . DIRECTORY_SEPARATOR, '', $f);
                echo "  $rel: {$d['framework']} (" . round($d['confidence'] * 100) . "%)\n";
            }
        }
        exit(0);
    }

    if (is_file($src)) {
        $content = file_get_contents($src);
        [$migrated, $result] = MigrationTool::migrateFile($content, $from, $to);
        if (!$result['success']) { fwrite(STDERR, "Failed: {$result['error']}\n"); exit(1); }
        if ($output) { file_put_contents($output, $migrated); echo "Written to: $output\n"; }
        else { echo $migrated; }
        echo "\nDetected: {$result['detected']}\nTransformations: {$result['transformations']}\n";
        exit(0);
    }

    if (is_dir($src)) {
        $outputDir = $output ?? dirname($src) . '/velocity-migrated';
        echo "Scanning: $src\n";
        echo "Output: " . ($dryRun ? '(dry run)' : $outputDir) . "\n";
        echo "Source framework: $from\n\n";

        $results = MigrationTool::bulkMigrate($src, $outputDir, $from, $dryRun, $to);
        echo "Results:\n";
        echo "  Total: {$results['total']}\n";
        echo "  Migrated: {$results['migrated']}\n";
        echo "  Failed: {$results['failed']}\n";
        echo "  Skipped: {$results['skipped']}\n";

        foreach ($results['details'] as $r) {
            $status = $r['success'] ? 'OK' : 'FAIL';
            echo "  [$status] {$r['path']} ({$r['detected']}, {$r['transformations']} changes)\n";
        }
    }
}
