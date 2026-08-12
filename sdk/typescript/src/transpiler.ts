/**
 * VELOCITY-WorkFlow TypeScript SDK — Regex-based AST transpiler.
 *
 * Transforms Temporal TypeScript workflow files into VELOCITY-compatible code
 * by rewriting imports, decorators, and API calls using pattern-based transforms.
 *
 * This operates on source text directly, performing well-defined text
 * transformations that handle imports, decorators, method calls, and
 * version guard injection.
 *
 * ## Design Rationale
 * TypeScript 7.x restructured its compiler API into unstable subpath exports.
 * Rather than depend on unstable internal APIs, this transpiler uses precise
 * regex patterns that are sufficient for the well-defined transformation set
 * required for Temporal→VELOCITY migration.
 */

// ─── Transpiler Configuration ────────────────────────────────────────────────

export interface TranspilerConfig {
  /** Target namespace for velocity imports. */
  velocityNamespace?: string;
  /** Whether to inject version guards. */
  injectVersionGuards?: boolean;
  /** Whether to rewrite timer calls. */
  rewriteTimers?: boolean;
  /** Whether to rewrite signal/query handlers. */
  rewriteHandlers?: boolean;
}

const DEFAULT_CONFIG: Required<TranspilerConfig> = {
  velocityNamespace: '@velocity-workflow/sdk',
  injectVersionGuards: true,
  rewriteTimers: true,
  rewriteHandlers: true,
};

// ─── Transpiler Statistics ───────────────────────────────────────────────────

export interface TranspileStats {
  importsRewritten: number;
  decoratorsRewritten: number;
  methodCallsRewritten: number;
  signalHandlersRewritten: number;
  queryHandlersRewritten: number;
  timerCallsRewritten: number;
  versionGuardsInjected: number;
  totalNodesVisited: number;
  phases: string[];
}

export interface TranspileResult {
  code: string;
  stats: TranspileStats;
  diagnostics: string[];
}

function emptyStats(): TranspileStats {
  return {
    importsRewritten: 0,
    decoratorsRewritten: 0,
    methodCallsRewritten: 0,
    signalHandlersRewritten: 0,
    queryHandlersRewritten: 0,
    timerCallsRewritten: 0,
    versionGuardsInjected: 0,
    totalNodesVisited: 0,
    phases: [],
  };
}

// ─── Temporal import patterns ────────────────────────────────────────────────

const TEMPORAL_IMPORTS = [
  '@temporalio/client',
  '@temporalio/worker',
  '@temporalio/workflow',
  'temporal-client',
];

// ─── AST Transpiler ──────────────────────────────────────────────────────────

/**
 * Transpile Temporal TypeScript workflow source code into VELOCITY-compatible code.
 *
 * Transformation phases:
 * 1. Rewrite Temporal imports to VELOCITY imports
 * 2. Rewrite @Workflow/@Activity decorators to @DurableWorkflow/@ActivityMethod
 * 3. Rewrite ctx.sleep() to velocity.sleep()
 * 4. Rewrite ctx.signal() to velocity.signal()
 * 5. Rewrite ctx.query() to velocity.query()
 * 6. Rewrite temporal.proxyActivities() to velocity.activities()
 * 7. Rewrite executeWorkflow() to velocityExecute()
 * 8. Inject version guards if configured
 */
export function transpileTypeScript(
  source: string,
  config?: TranspilerConfig,
): TranspileResult {
  const cfg = { ...DEFAULT_CONFIG, ...config };
  const stats = emptyStats();
  const diagnostics: string[] = [];
  let code = source;

  // Count approximate "nodes visited" as lines of source
  stats.totalNodesVisited = Math.max(1, code.split('\n').length);

  // Phase 1: Rewrite Temporal imports → VELOCITY imports
  for (const temporalImport of TEMPORAL_IMPORTS) {
    // Match: import { ... } from '@temporalio/client';
    // Also handles: import ... from '@temporalio/client';
    const escaped = temporalImport.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const importRegex = new RegExp(
      `(from\\s+['"])${escaped}(['"])`,
      'g',
    );
    const matches = code.match(importRegex);
    if (matches) {
      stats.importsRewritten += matches.length;
      stats.phases.push('ImportRewrite');
      code = code.replace(importRegex, `$1${cfg.velocityNamespace}$2`);
    }
  }

  // Phase 2: Rewrite decorators
  // @Workflow() → @DurableWorkflow()
  const workflowDecoratorRegex = /@Workflow\s*\(\s*\)/g;
  const wfMatches = code.match(workflowDecoratorRegex);
  if (wfMatches) {
    stats.decoratorsRewritten += wfMatches.length;
    stats.phases.push('DecoratorRewrite');
    code = code.replace(workflowDecoratorRegex, '@DurableWorkflow()');
  }

  // @Activity() → @ActivityMethod()
  const activityDecoratorRegex = /@Activity\s*\(\s*\)/g;
  const actMatches = code.match(activityDecoratorRegex);
  if (actMatches) {
    stats.decoratorsRewritten += actMatches.length;
    stats.phases.push('DecoratorRewrite');
    code = code.replace(activityDecoratorRegex, '@ActivityMethod()');
  }

  // Phase 3: Rewrite ctx method calls
  if (cfg.rewriteTimers) {
    // ctx.sleep(...) → velocity.sleep(...)
    const sleepRegex = /\bctx\.sleep\s*\(/g;
    const sleepMatches = code.match(sleepRegex);
    if (sleepMatches) {
      stats.timerCallsRewritten += sleepMatches.length;
      stats.phases.push('TimerRewrite');
      code = code.replace(sleepRegex, 'velocity.sleep(');
    }
  }

  if (cfg.rewriteHandlers) {
    // ctx.signal(...) → velocity.signal(...)
    const signalRegex = /\bctx\.signal\s*\(/g;
    const signalMatches = code.match(signalRegex);
    if (signalMatches) {
      stats.signalHandlersRewritten += signalMatches.length;
      stats.phases.push('SignalRewrite');
      code = code.replace(signalRegex, 'velocity.signal(');
    }

    // ctx.query(...) → velocity.query(...)
    const queryRegex = /\bctx\.query\s*\(/g;
    const queryMatches = code.match(queryRegex);
    if (queryMatches) {
      stats.queryHandlersRewritten += queryMatches.length;
      stats.phases.push('QueryRewrite');
      code = code.replace(queryRegex, 'velocity.query(');
    }
  }

  // Phase 4: Rewrite temporal.proxyActivities → velocity.activities
  const proxyRegex = /\btemporal\.proxyActivities\s*\(/g;
  const proxyMatches = code.match(proxyRegex);
  if (proxyMatches) {
    stats.methodCallsRewritten += proxyMatches.length;
    stats.phases.push('ActivityProxyRewrite');
    code = code.replace(proxyRegex, 'velocity.activities(');
  }

  // Phase 5: Rewrite executeWorkflow → velocityExecute
  const execRegex = /\bexecuteWorkflow\s*\(/g;
  const execMatches = code.match(execRegex);
  if (execMatches) {
    stats.methodCallsRewritten += execMatches.length;
    stats.phases.push('ExecuteWorkflowRewrite');
    code = code.replace(execRegex, 'velocityExecute(');
  }

  // Phase 6: Inject version guard
  if (cfg.injectVersionGuards) {
    const versionGuard = `// VELOCITY Version Guard — auto-injected by transpiler\nconst __VELOCITY_VERSION__ = 1;\n`;
    code = versionGuard + code;
    stats.versionGuardsInjected++;
    stats.phases.push('VersionGuard');
  }

  // Deduplicate phases
  stats.phases = Array.from(new Set(stats.phases));

  return {
    code,
    stats,
    diagnostics,
  };
}

// ─── Convenience ─────────────────────────────────────────────────────────────

/**
 * Check if a source file contains Temporal-specific patterns.
 */
export function isTemporalWorkflow(source: string): boolean {
  return (
    source.includes('@temporalio/') ||
    source.includes('temporal-client') ||
    source.includes('proxyActivities') ||
    source.includes('executeWorkflow')
  );
}
