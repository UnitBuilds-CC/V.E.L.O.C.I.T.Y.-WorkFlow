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

    /** ─── Framework Detection ─────────────────────────────────────────── */

    public static function detectFramework(string $content): array
    {
        $scores = ['temporal' => 0, 'restate' => 0, 'dbos' => 0];
        $evidence = ['temporal' => [], 'restate' => [], 'dbos' => []];

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

    public static function migrateFile(string $content, string $sourceFramework): array
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
        $indicators = ['Temporal\\', 'Restate\\', 'DBOS\\', '#[WorkflowMethod', '#[ActivityMethod', 'Workflow::sleep'];
        foreach ($indicators as $ind) {
            if (str_contains($content, $ind)) return true;
        }
        return false;
    }

    /** ─── Bulk Migration ──────────────────────────────────────────────── */

    public static function bulkMigrate(string $sourceDir, string $outputDir, string $from, bool $dryRun = false): array
    {
        $files = self::scanPhpFiles($sourceDir);
        $results = ['total' => count($files), 'migrated' => 0, 'failed' => 0, 'skipped' => 0, 'details' => []];

        foreach ($files as $filePath) {
            $content = file_get_contents($filePath);
            if (!self::hasWorkflowContent($content)) { $results['skipped']++; continue; }

            [$migrated, $result] = self::migrateFile($content, $from);

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
    $src = null; $from = 'auto'; $output = null; $dryRun = false; $detect = false;

    for ($i = 0; $i < count($args); $i++) {
        switch ($args[$i]) {
            case '--src': $src = $args[++$i]; break;
            case '--from': $from = $args[++$i]; break;
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
        [$migrated, $result] = MigrationTool::migrateFile($content, $from);
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

        $results = MigrationTool::bulkMigrate($src, $outputDir, $from, $dryRun);
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
