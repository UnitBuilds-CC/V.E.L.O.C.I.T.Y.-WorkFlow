/**
 * C# AST Transpiler Bridge
 *
 * Integrates the C# Roslyn-based AST transpiler (tools/temporal2velocity)
 * into the main migration toolkit. Provides a TypeScript API for invoking
 * the C# transpiler as a subprocess, and a fallback regex-based transpiler
 * for environments where .NET is not available.
 *
 * The C# AST transpiler provides superior results for C# inputs:
 *   - Proper syntax tree walking (no false matches in strings/comments)
 *   - Semantic-aware rewrites (type-qualified member access)
 *   - Attribute injection via syntax tree manipulation
 *   - Signal/query handler detection via attribute inspection
 */

import * as fs from 'fs';
import * as path from 'path';
import { execSync } from 'child_process';

// ─── Types ───────────────────────────────────────────────────────────────────

/** Result of a C# transpilation. */
export interface CSharpTranspileResult {
  /** Transpiled output code. */
  output: string;
  /** Whether the AST-based transpiler was used (vs regex fallback). */
  astMode: boolean;
  /** Statistics from the transpilation. */
  stats: CSharpTranspileStats;
  /** Any error that occurred. */
  error?: string;
}

/** Statistics from transpilation. */
export interface CSharpTranspileStats {
  usingDirectivesRewritten: number;
  memberAccessesRewritten: number;
  objectCreationsRewritten: number;
  attributesInjected: number;
  signalHandlersConverted: number;
  queryHandlersConverted: number;
  childWorkflowsConverted: number;
  versionGuardsRemoved: number;
  timerCallsConverted: number;
  totalReplacements: number;
}

// ─── C# AST Transpiler (subprocess) ─────────────────────────────────────────

/** Path to the C# transpiler tool. */
function getTranspilerPath(): string {
  // Look for the transpiler relative to the migration toolkit
  const toolkitDir = path.resolve(__dirname, '..', '..');
  const projectRoot = path.resolve(toolkitDir, '..');
  return path.join(projectRoot, 'tools', 'temporal2velocity');
}

/**
 * Check if the .NET runtime is available for the C# AST transpiler.
 */
export function isDotNetAvailable(): boolean {
  try {
    execSync('dotnet --version', { stdio: 'pipe' });
    return true;
  } catch {
    return false;
  }
}

/**
 * Transpile C# code using the Roslyn AST transpiler (subprocess).
 * Falls back to regex-based transpilation if .NET is not available.
 */
export function transpileCSharp(sourceCode: string): CSharpTranspileResult {
  const transpilerDir = getTranspilerPath();

  // Try the AST-based transpiler first
  if (isDotNetAvailable() && fs.existsSync(path.join(transpilerDir, 'temporal2velocity.csproj'))) {
    try {
      // Write source to a temp file
      const tmpFile = path.join(transpilerDir, '_tmp_input.cs');
      fs.writeFileSync(tmpFile, sourceCode, 'utf-8');

      try {
        const output = execSync(
          `dotnet run --project "${transpilerDir}" -- --src "${tmpFile}"`,
          { encoding: 'utf-8', timeout: 30000, stdio: ['pipe', 'pipe', 'pipe'] }
        );

        // Parse the output (the transpiler prints "Transpiled Code Output:\n<code>")
        const codeStart = output.indexOf('Transpiled Code Output:\n');
        const transpiledCode = codeStart >= 0
          ? output.slice(codeStart + 'Transpiled Code Output:\n'.length)
          : output;

        return {
          output: transpiledCode.trim(),
          astMode: true,
          stats: {
            usingDirectivesRewritten: 0,
            memberAccessesRewritten: 0,
            objectCreationsRewritten: 0,
            attributesInjected: 0,
            signalHandlersConverted: 0,
            queryHandlersConverted: 0,
            childWorkflowsConverted: 0,
            versionGuardsRemoved: 0,
            timerCallsConverted: 0,
            totalReplacements: 0,
          },
        };
      } finally {
        // Clean up temp file
        try { fs.unlinkSync(tmpFile); } catch {}
      }
    } catch (err: any) {
      // Fall through to regex-based transpiler
      console.warn(`C# AST transpiler failed, falling back to regex: ${err.message}`);
    }
  }

  // Fallback: regex-based C# transpilation
  return transpileCSharpRegex(sourceCode);
}

// ─── Regex-Based C# Fallback Transpiler ──────────────────────────────────────

/**
 * Regex-based C# transpiler (fallback when .NET is not available).
 * Provides basic import/using rewrites and pattern replacements.
 */
export function transpileCSharpRegex(sourceCode: string): CSharpTranspileResult {
  const stats: CSharpTranspileStats = {
    usingDirectivesRewritten: 0,
    memberAccessesRewritten: 0,
    objectCreationsRewritten: 0,
    attributesInjected: 0,
    signalHandlersConverted: 0,
    queryHandlersConverted: 0,
    childWorkflowsConverted: 0,
    versionGuardsRemoved: 0,
    timerCallsConverted: 0,
    totalReplacements: 0,
  };

  let result = sourceCode;

  // Phase 1: Using directive rewrites
  const usingReplacements: [RegExp, string][] = [
    [/using\s+Temporalio\.Client\s*;/g, 'using Velocity.Workflow.Core;'],
    [/using\s+Temporalio\.Workflows\s*;/g, 'using Velocity.Workflow.Core;'],
    [/using\s+Temporalio\.Activities\s*;/g, 'using Velocity.Workflow.Core;'],
    [/using\s+Temporalio\.Exceptions\s*;/g, 'using Velocity.Workflow.Core;'],
    [/using\s+Temporalio\.Converters\s*;/g, '// Velocity uses built-in slab serialization'],
  ];

  for (const [pattern, replacement] of usingReplacements) {
    const before = result;
    result = result.replace(pattern, replacement);
    if (result !== before) stats.usingDirectivesRewritten++;
  }

  // Phase 2: Member access rewrites
  const memberReplacements: [RegExp, string][] = [
    [/DateTime\.UtcNow/g, 'WorkflowClock.UtcNow'],
    [/DateTime\.Now/g, 'WorkflowClock.UtcNow'],
    [/Guid\.NewGuid\(\)/g, 'WorkflowGuid.NewGuid()'],
    [/new\s+Random\(\)/g, 'new WorkflowRandom()'],
    [/Workflow\.Timer\.Sleep\(/g, 'await Task.Delay('],
  ];

  for (const [pattern, replacement] of memberReplacements) {
    const before = result;
    result = result.replace(pattern, replacement);
    if (result !== before) stats.memberAccessesRewritten++;
  }

  // Phase 3: Attribute injection
  if (result.includes('async Task') && !result.includes('[DurableWorkflow]')) {
    result = result.replace(
      /(\s*)(public\s+async\s+Task)/g,
      '$1[DurableWorkflow]\n$1$2'
    );
    stats.attributesInjected++;
  }

  // Phase 4: Signal/Query handler conversion
  const beforeSignal = result;
  result = result.replace(
    /\[WorkflowSignal\]\s*(?:public\s+)?(?:async\s+)?(?:Task|void)\s+(\w+)/g,
    '[VelocitySignal("$1")]\n    public async Task $1'
  );
  if (result !== beforeSignal) stats.signalHandlersConverted++;

  const beforeQuery = result;
  result = result.replace(
    /\[WorkflowQuery\]\s*(?:public\s+)?(?:async\s+)?(?:Task<[^>]+>|[^ (]+)\s+(\w+)/g,
    '[VelocityQuery("$1")]\n    public $1'
  );
  if (result !== beforeQuery) stats.queryHandlersConverted++;

  // Phase 5: Child workflow conversion
  const beforeChild = result;
  result = result.replace(
    /Workflow\.ExecuteChildAsync<[^>]+>\(\s*(\w+)\s*,/g,
    'await ctx.ExecuteChildWorkflowAsync($1,'
  );
  if (result !== beforeChild) stats.childWorkflowsConverted++;

  // Phase 6: Version guard removal
  const beforeVersion = result;
  result = result.replace(
    /await\s+Workflow\.GetVersionAsync\([^)]+\);?/g,
    '// Stripped legacy version guard — Velocity uses slab schema evolution'
  );
  result = result.replace(
    /Workflow\.GetVersion\([^)]+\);?/g,
    '// Stripped legacy version guard — Velocity uses slab schema evolution'
  );
  if (result !== beforeVersion) stats.versionGuardsRemoved++;

  // Phase 7: Timer conversion
  const beforeTimer = result;
  result = result.replace(
    /Workflow\.Delay\(/g,
    'Task.Delay('
  );
  if (result !== beforeTimer) stats.timerCallsConverted++;

  // Phase 8: ActivityOptions rewrite
  result = result.replace(
    /ActivityOptions\.Builder\(\)/g,
    'VelocityActivityOptions()'
  );

  stats.totalReplacements = stats.usingDirectivesRewritten +
    stats.memberAccessesRewritten + stats.objectCreationsRewritten +
    stats.attributesInjected + stats.signalHandlersConverted +
    stats.queryHandlersConverted + stats.childWorkflowsConverted +
    stats.versionGuardsRemoved + stats.timerCallsConverted;

  return {
    output: result,
    astMode: false,
    stats,
  };
}

// ─── Integration with Main Migration Pipeline ────────────────────────────────

/**
 * Transpile a C# file as part of the migration pipeline.
 * This is the main entry point for C# migration from the toolkit.
 */
export function transpileCSharpFile(filePath: string): CSharpTranspileResult {
  const sourceCode = fs.readFileSync(filePath, 'utf-8');
  return transpileCSharp(sourceCode);
}

/**
 * Check if a file is a C# file.
 */
export function isCSharpFile(filePath: string): boolean {
  return path.extname(filePath).toLowerCase() === '.cs';
}
