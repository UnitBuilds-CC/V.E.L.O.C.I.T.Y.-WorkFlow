/**
 * VELOCITY-WorkFlow TypeScript SDK — Tests for the SWC-based AST transpiler.
 */

import { transpileTypeScript, isTemporalWorkflow, TranspileResult } from './transpiler';

describe('TypeScript AST Transpiler', () => {
  test('rewrites Temporal imports to VELOCITY imports', () => {
    const source = `import { Connection } from '@temporalio/client';`;
    const result = transpileTypeScript(source);
    expect(result.code).toContain('@velocity-workflow/sdk');
    expect(result.code).not.toContain('@temporalio/client');
    expect(result.stats.importsRewritten).toBe(1);
  });

  test('rewrites @Workflow decorator to @DurableWorkflow', () => {
    const source = `
@Workflow()
class MyWorkflow {
  async execute() {}
}`;
    const result = transpileTypeScript(source);
    expect(result.code).toContain('DurableWorkflow');
    expect(result.stats.decoratorsRewritten).toBe(1);
  });

  test('rewrites @Activity decorator to @ActivityMethod', () => {
    const source = `
@Activity()
class MyActivity {
  async run() {}
}`;
    const result = transpileTypeScript(source);
    expect(result.code).toContain('ActivityMethod');
    expect(result.stats.decoratorsRewritten).toBe(1);
  });

  test('rewrites ctx.sleep() to velocity.sleep()', () => {
    const source = `async function wf(ctx: any) { await ctx.sleep(1000); }`;
    const result = transpileTypeScript(source);
    expect(result.code).toContain('velocity.sleep');
    expect(result.stats.timerCallsRewritten).toBe(1);
  });

  test('rewrites ctx.signal() to velocity.signal()', () => {
    const source = `async function wf(ctx: any) { ctx.signal('approval', data); }`;
    const result = transpileTypeScript(source);
    expect(result.code).toContain('velocity.signal');
    expect(result.stats.signalHandlersRewritten).toBe(1);
  });

  test('rewrites proxyActivities to velocity.activities', () => {
    const source = `const activities = temporal.proxyActivities({ activities });`;
    const result = transpileTypeScript(source);
    expect(result.code).toContain('velocity.activities');
    expect(result.stats.methodCallsRewritten).toBe(1);
  });

  test('rewrites executeWorkflow to velocityExecute', () => {
    const source = `const handle = await executeWorkflow('my-wf', options);`;
    const result = transpileTypeScript(source);
    expect(result.code).toContain('velocityExecute');
    expect(result.stats.methodCallsRewritten).toBe(1);
  });

  test('injects version guard when configured', () => {
    const source = `const x = 1;`;
    const result = transpileTypeScript(source, { injectVersionGuards: true });
    expect(result.code).toContain('__VELOCITY_VERSION__');
    expect(result.stats.versionGuardsInjected).toBe(1);
  });

  test('does not inject version guard when disabled', () => {
    const source = `const x = 1;`;
    const result = transpileTypeScript(source, { injectVersionGuards: false });
    expect(result.code).not.toContain('__VELOCITY_VERSION__');
  });

  test('does not rewrite unrelated imports', () => {
    const source = `import { something } from 'lodash';`;
    const result = transpileTypeScript(source);
    expect(result.code).toContain('lodash');
    expect(result.stats.importsRewritten).toBe(0);
  });

  test('handles empty source', () => {
    const result = transpileTypeScript('');
    expect(result.stats.totalNodesVisited).toBeGreaterThan(0);
    expect(result.diagnostics).toHaveLength(0);
  });

  test('tracks all phases', () => {
    const source = `
import { Connection } from '@temporalio/client';
async function wf(ctx: any) { await ctx.sleep(100); }
`;
    const result = transpileTypeScript(source);
    expect(result.stats.phases).toContain('ImportRewrite');
    expect(result.stats.phases).toContain('TimerRewrite');
  });
});

describe('isTemporalWorkflow', () => {
  test('detects Temporal imports', () => {
    expect(isTemporalWorkflow(`import { x } from '@temporalio/client'`)).toBe(true);
  });

  test('detects proxyActivities', () => {
    expect(isTemporalWorkflow(`temporal.proxyActivities({})`)).toBe(true);
  });

  test('detects executeWorkflow', () => {
    expect(isTemporalWorkflow(`executeWorkflow('test')`)).toBe(true);
  });

  test('returns false for non-Temporal code', () => {
    expect(isTemporalWorkflow(`const x = 1;`)).toBe(false);
  });
});
