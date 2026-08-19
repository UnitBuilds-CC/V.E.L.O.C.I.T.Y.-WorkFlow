<?php
/**
 * Global registry for auto-applied workflows and activities.
 *
 * This class maintains the registry of workflow and activity handlers
 * that are discovered via attribute scanning.
 */

declare(strict_types=1);

namespace Velocity\SDK\AutoApply;

use ReflectionClass;
use ReflectionFunction;
use Velocity\SDK\Attributes\Workflow;
use Velocity\SDK\Attributes\Activity;
use Velocity\SDK\Attributes\Signal;
use Velocity\SDK\Attributes\Query;
use Velocity\SDK\Attributes\Update;

class Registry
{
    private static array $workflows = [];
    private static array $activities = [];

    /**
     * Register a workflow class.
     */
    public static function registerWorkflow(string $workflowType, string $className): void
    {
        self::$workflows[$workflowType] = $className;
    }

    /**
     * Register an activity function.
     */
    public static function registerActivity(string $activityName, callable $handler): void
    {
        self::$activities[$activityName] = $handler;
    }

    /**
     * Get all registered workflow types and their class names.
     */
    public static function getRegisteredWorkflows(): array
    {
        return self::$workflows;
    }

    /**
     * Get all registered activity names and their handlers.
     */
    public static function getRegisteredActivities(): array
    {
        return self::$activities;
    }

    /**
     * Clear both registries (useful for testing).
     */
    public static function clear(): void
    {
        self::$workflows = [];
        self::$activities = [];
    }

    /**
     * Count of registered workflows.
     */
    public static function workflowCount(): int
    {
        return count(self::$workflows);
    }

    /**
     * Count of registered activities.
     */
    public static function activityCount(): int
    {
        return count(self::$activities);
    }

    /**
     * Scan a class for workflow and activity attributes and register them.
     */
    public static function scanClass(string $className): void
    {
        $reflection = new ReflectionClass($className);

        // Check for #[Workflow] attribute on the class
        $workflowAttrs = $reflection->getAttributes(Workflow::class);
        if (!empty($workflowAttrs)) {
            $workflowAttr = $workflowAttrs[0]->newInstance();
            $workflowType = $workflowAttr->name ?? $className;
            self::registerWorkflow($workflowType, $className);

            // Scan methods for signal/query/update handlers
            foreach ($reflection->getMethods() as $method) {
                // Signal handlers
                foreach ($method->getAttributes(Signal::class) as $attr) {
                    $signalAttr = $attr->newInstance();
                    $signalName = $signalAttr->name ?? $method->getName();
                    // Store signal handler mapping (workflow_type -> signal_name -> method_name)
                    // This will be used by the Worker when dispatching signals
                }

                // Query handlers
                foreach ($method->getAttributes(Query::class) as $attr) {
                    $queryAttr = $attr->newInstance();
                    $queryName = $queryAttr->name ?? $method->getName();
                    // Store query handler mapping
                }

                // Update handlers
                foreach ($method->getAttributes(Update::class) as $attr) {
                    $updateAttr = $attr->newInstance();
                    $updateName = $updateAttr->name ?? $method->getName();
                    // Store update handler mapping
                }
            }
        }

        // Check for #[Activity] attributes on methods
        foreach ($reflection->getMethods() as $method) {
            $activityAttrs = $method->getAttributes(Activity::class);
            if (!empty($activityAttrs)) {
                $activityAttr = $activityAttrs[0]->newInstance();
                $activityName = $activityAttr->name ?? $method->getName();
                self::registerActivity($activityName, [$className, $method->getName()]);
            }
        }
    }

    /**
     * Scan a file for functions with #[Activity] attribute and register them.
     */
    public static function scanFile(string $filePath): void
    {
        if (!file_exists($filePath)) {
            return;
        }

        require_once $filePath;

        $content = file_get_contents($filePath);
        // Simple regex to find function declarations with #[Activity] attribute
        // In production, use a proper PHP parser like nikic/php-parser
        if (preg_match_all('/#\[Activity.*?\]\s*function\s+(\w+)/s', $content, $matches)) {
            foreach ($matches[1] as $functionName) {
                if (function_exists($functionName)) {
                    $reflection = new ReflectionFunction($functionName);
                    $activityAttrs = $reflection->getAttributes(Activity::class);
                    if (!empty($activityAttrs)) {
                        $activityAttr = $activityAttrs[0]->newInstance();
                        $activityName = $activityAttr->name ?? $functionName;
                        self::registerActivity($activityName, $functionName);
                    }
                }
            }
        }
    }

    /**
     * Scan a directory for PHP files and register all workflows/activities.
     */
    public static function scanDirectory(string $directory): void
    {
        if (!is_dir($directory)) {
            return;
        }

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($directory, \RecursiveDirectoryIterator::SKIP_DOTS)
        );

        foreach ($iterator as $file) {
            if ($file->getExtension() === 'php') {
                self::scanFile($file->getPathname());
            }
        }
    }
}
