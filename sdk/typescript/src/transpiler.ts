/**
 * VELOCITY-WorkFlow TypeScript SDK — SWC-based AST transpiler.
 *
 * Transforms Temporal TypeScript workflow files into VELOCITY-compatible code
 * by rewriting imports, decorators, and API calls at the AST level.
 *
 * This replaces the regex-based approach with proper TypeScript AST manipulation
 * using the TypeScript compiler API (typescript package), avoiding false matches
 * in string literals and comments.
 */

import * as ts from 'typescript';

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

// ─── AST Transpiler ──────────────────────────────────────────────────────────

/**
 * Transpile Temporal TypeScript workflow source code into VELOCITY-compatible code.
 *
 * Uses the TypeScript compiler API for proper AST-level transformations:
 * 1. Rewrite Temporal imports to VELOCITY imports
 * 2. Rewrite @Workflow/@Activity decorators to @DurableWorkflow/@ActivityMethod
 * 3. Rewrite ctx.sleep() to velocity timer calls
 * 4. Rewrite ctx.signal() to velocity signal calls
 * 5. Rewrite workflow.execute() patterns
 * 6. Inject version guards if configured
 */
export function transpileTypeScript(
  source: string,
  config?: TranspilerConfig,
): TranspileResult {
  const cfg = { ...DEFAULT_CONFIG, ...config };
  const stats = emptyStats();
  const diagnostics: string[] = [];

  // Phase 1: Parse the source into an AST
  const sourceFile = ts.createSourceFile(
    'workflow.ts',
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );

  // Phase 2: Create a transformer
  const transformer: ts.TransformerFactory<ts.SourceFile> = (context) => {
    const visit: ts.Visitor = (node) => {
      stats.totalNodesVisited++;

      // Phase 2a: Rewrite import declarations
      if (ts.isImportDeclaration(node) && ts.isStringLiteral(node.moduleSpecifier)) {
        const moduleText = node.moduleSpecifier.text;
        if (moduleText === '@temporalio/client' || moduleText === '@temporalio/worker' ||
            moduleText === '@temporalio/workflow' || moduleText === 'temporal-client') {
          stats.importsRewritten++;
          stats.phases.push('ImportRewrite');
          return ts.factory.updateImportDeclaration(
            node,
            node.modifiers,
            node.importClause,
            ts.factory.createStringLiteral(cfg.velocityNamespace),
            node.assertClause,
          );
        }
      }

      // Phase 2b: Rewrite decorators
      if (ts.isClassDeclaration(node) && node.decorators) {
        const newDecorators = node.decorators.map((dec) => {
          if (ts.isCallExpression(dec.expression)) {
            const expr = dec.expression.expression;
            if (ts.isIdentifier(expr)) {
              const name = expr.text;
              if (name === 'Workflow') {
                stats.decoratorsRewritten++;
                stats.phases.push('DecoratorRewrite');
                return ts.factory.createDecorator(
                  ts.factory.updateCallExpression(
                    dec.expression,
                    ts.factory.createIdentifier('DurableWorkflow'),
                    dec.expression.typeArguments,
                    dec.expression.arguments,
                  ),
                );
              }
              if (name === 'Activity') {
                stats.decoratorsRewritten++;
                return ts.factory.createDecorator(
                  ts.factory.updateCallExpression(
                    dec.expression,
                    ts.factory.createIdentifier('ActivityMethod'),
                    dec.expression.typeArguments,
                    dec.expression.arguments,
                  ),
                );
              }
            }
          }
          return dec;
        });

        return ts.factory.updateClassDeclaration(
          node,
          newDecorators,
          node.modifiers,
          node.name,
          node.typeParameters,
          node.heritageClauses,
          node.members,
        );
      }

      // Phase 2c: Rewrite method calls (ctx.sleep, ctx.signal, etc.)
      if (ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression)) {
        const prop = node.expression.name.text;
        const obj = node.expression.expression;

        if (ts.isIdentifier(obj) && obj.text === 'ctx') {
          // ctx.sleep() → velocity.sleep()
          if (prop === 'sleep' && cfg.rewriteTimers) {
            stats.timerCallsRewritten++;
            stats.phases.push('TimerRewrite');
            return ts.factory.updateCallExpression(
              node,
              ts.factory.createPropertyAccessExpression(
                ts.factory.createIdentifier('velocity'),
                'sleep',
              ),
              node.typeArguments,
              node.arguments,
            );
          }

          // ctx.signal() → velocity.signal()
          if (prop === 'signal' && cfg.rewriteHandlers) {
            stats.signalHandlersRewritten++;
            stats.phases.push('SignalRewrite');
            return ts.factory.updateCallExpression(
              node,
              ts.factory.createPropertyAccessExpression(
                ts.factory.createIdentifier('velocity'),
                'signal',
              ),
              node.typeArguments,
              node.arguments,
            );
          }

          // ctx.query() → velocity.query()
          if (prop === 'query' && cfg.rewriteHandlers) {
            stats.queryHandlersRewritten++;
            stats.phases.push('QueryRewrite');
            return ts.factory.updateCallExpression(
              node,
              ts.factory.createPropertyAccessExpression(
                ts.factory.createIdentifier('velocity'),
                'query',
              ),
              node.typeArguments,
              node.arguments,
            );
          }
        }

        // proxyActivities → velocity.activities
        if (prop === 'proxyActivities' && ts.isIdentifier(obj) && obj.text === 'temporal') {
          stats.methodCallsRewritten++;
          stats.phases.push('ActivityProxyRewrite');
          return ts.factory.updateCallExpression(
            node,
            ts.factory.createPropertyAccessExpression(
              ts.factory.createIdentifier('velocity'),
              'activities',
            ),
            node.typeArguments,
            node.arguments,
          );
        }
      }

      // Phase 2d: Rewrite executeWorkflow calls
      if (ts.isCallExpression(node) && ts.isIdentifier(node.expression)) {
        if (node.expression.text === 'executeWorkflow') {
          stats.methodCallsRewritten++;
          stats.phases.push('ExecuteWorkflowRewrite');
          return ts.factory.updateCallExpression(
            node,
            ts.factory.createIdentifier('velocityExecute'),
            node.typeArguments,
            node.arguments,
          );
        }
      }

      return ts.visitEachChild(node, visit, context);
    };

    return (sf) => ts.visitNode(sf, visit) as ts.SourceFile;
  };

  // Phase 3: Apply the transformer
  const result = ts.transform(sourceFile, [transformer]);
  const printer = ts.createPrinter({ newLine: ts.NewLineKind.LineFeed });
  let outputCode = printer.printFile(result.transformed[0]);
  result.dispose();

  // Phase 4: Inject version guard if configured
  if (cfg.injectVersionGuards) {
    const versionGuard = `// VELOCITY Version Guard — auto-injected by transpiler\nconst __VELOCITY_VERSION__ = 1;\n`;
    outputCode = versionGuard + outputCode;
    stats.versionGuardsInjected++;
    stats.phases.push('VersionGuard');
  }

  // Deduplicate phases
  stats.phases = [...new Set(stats.phases)];

  return {
    code: outputCode,
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
