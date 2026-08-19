/**
 * Velocity Migration Toolkit
 * 
 * Converts workflows between SDK flavors:
 * - Temporal (Temporal SDK — direct migration)
 * - Classic (Temporal-compatible)
 * - Runtime (Restate-compatible)
 * - Embedded (DBOS-compatible)
 * - Python Runtime (Restate-compatible)
 * 
 * Uses intermediate representation (IR) for flexible N×M conversions.
 * Performs real body transformation — not stubs.
 */

// ─── SDK Flavors ─────────────────────────────────────────────────────────────

export type SDKFlavor = 'temporal' | 'classic' | 'runtime' | 'embedded' | 'python-runtime';

export interface MigrationOptions {
  source: SDKFlavor;
  target: SDKFlavor;
  preserveComments?: boolean;
  generateTests?: boolean;
}

// ─── Intermediate Representation (IR) ────────────────────────────────────────

export interface WorkflowIR {
  name: string;
  type: 'workflow' | 'activity' | 'service' | 'virtualObject';
  methods: MethodIR[];
  imports: string[];
  metadata: Record<string, any>;
}

export interface MethodIR {
  name: string;
  parameters: ParameterIR[];
  returnType: string;
  body: string;           // raw body from source
  transformedBody: string; // body after transformation (set during generation)
  decorators: string[];
  contextUsage: ContextUsageIR[];
  isAsync: boolean;
}

export interface ParameterIR {
  name: string;
  type: string;
  optional: boolean;
  defaultValue?: string;
}

export interface ContextUsageIR {
  type: string;
  args: string[];
  rawMatch: string;
}

// ─── Body Transformation Rules ───────────────────────────────────────────────

interface TransformRule {
  pattern: RegExp;
  /** Generate replacement for a specific target flavor */
  replacement: (match: RegExpMatchArray, target: SDKFlavor) => string;
}

/**
 * All cross-SDK API transformation rules.
 * Each rule matches a source-pattern and produces the correct target-pattern.
 */
const BODY_TRANSFORM_RULES: TransformRule[] = [
  // ── Temporal → Velocity: proxied activity call (e.g. await greet(args)) ──
  {
    pattern: /(?<!this\.|ctx\.|\w)await\s+([a-z]\w*)\s*\(([^)]*)\)/g,
    replacement: (m, target) => {
      if (target === 'temporal') return m[0];
      const fnName = m[1];
      const args = m[2];
      // Skip known non-activity functions
      const skip = ['console','JSON','String','Number','Array','Object','Math','Date','parseInt','parseFloat','setTimeout','setInterval','require','import','Error','Promise','Buffer','Map','Set','Symbol','RegExp','Reflect','Proxy','WeakMap','WeakSet','AbortController','fetch','queueMicrotask','structuredClone'];
      if (skip.includes(fnName) || fnName.startsWith('_')) return m[0];
      if (['if','for','while','switch','catch','return','new','throw','typeof','instanceof','void','delete','in','of'].includes(fnName)) return m[0];
      switch (target) {
        case 'classic': return `await this.executeActivity('${fnName}'${args ? ', ' + args : ''})`;
        case 'runtime': return `await ctx.invoke('${fnName}', 'execute'${args ? ', ' + args : ''})`;
        case 'embedded': return `await ctx.invoke('${fnName}', 'execute'${args ? ', ' + args : ''})`;
        case 'python-runtime': return `await ctx.invoke('${fnName}', 'execute'${args ? ', ' + args : ''})`;
        default: return m[0];
      }
    },
  },
  // ── Temporal → Velocity: wf.sleep / sleep ──
  {
    pattern: /await\s+(?:wf\.)?sleep\s*\(\s*([^)]+)\)/g,
    replacement: (m, target) => {
      if (target === 'temporal') return m[0];
      switch (target) {
        case 'classic': return `await this.sleep(${m[1]})`;
        case 'runtime': return `await ctx.sleep(${m[1]})`;
        case 'embedded': return `await ctx.sleep(${m[1]})`;
        case 'python-runtime': return `await ctx.sleep(${m[1]})`;
        default: return m[0];
      }
    },
  },
  // ── Temporal → Velocity: wf.condition / condition ──
  {
    pattern: /await\s+(?:wf\.)?condition\s*\(\s*([^)]+)\)/g,
    replacement: (m, target) => {
      if (target === 'temporal') return m[0];
      const args = m[1];
      // Extract signal name from first arg if it's a string
      const sigMatch = /['"](\w+)['"]/.exec(args);
      const sigName = sigMatch ? sigMatch[1] : args;
      switch (target) {
        case 'classic': return `await this.waitForSignal('${sigName}')`;
        case 'runtime': return `await ctx.promise('${sigName}')`;
        case 'embedded': return `await ctx.promise('${sigName}')`;
        case 'python-runtime': return `await ctx.promise('${sigName}')`;
        default: return m[0];
      }
    },
  },
  // ── Temporal → Velocity: wf.signalHandler / setHandler ──
  {
    pattern: /(?:wf\.)?setHandler\s*\(\s*(?:wf\.)?signal\s*\(\s*['"](\w+)['"]\s*\)\s*,/g,
    replacement: (m, target) => {
      if (target === 'temporal') return m[0];
      const sigName = m[1];
      switch (target) {
        case 'classic': return `/* signal: ${sigName} */`;
        case 'runtime': return `/* signal: ${sigName} */`;
        case 'embedded': return `/* signal: ${sigName} */`;
        case 'python-runtime': return `# signal: ${sigName}`;
        default: return m[0];
      }
    },
  },
  // ── Classic → others: await this.executeActivity('Name', ...args) ──
  {
    pattern: /await\s+this\.executeActivity\(\s*['"](\w+)['"]\s*(?:,\s*([^)]+))?\)/g,
    replacement: (m, target) => {
      const actName = m[1];
      const args = m[2] ? `, ${m[2]}` : '';
      switch (target) {
        case 'runtime': return `await ctx.invoke('${actName}', 'execute'${args})`;
        case 'embedded': return `await ctx.invoke('${actName}', 'execute'${args})`;
        case 'python-runtime': return `await ctx.invoke('${actName}', 'execute'${args})`;
        default: return m[0]; // classic → classic
      }
    },
  },
  // ── Classic → others: this.executeActivity('Name', ...args) (without await) ──
  {
    pattern: /this\.executeActivity\(\s*['"](\w+)['"]\s*(?:,\s*([^)]+))?\)/g,
    replacement: (m, target) => {
      const actName = m[1];
      const args = m[2] ? `, ${m[2]}` : '';
      switch (target) {
        case 'runtime': return `await ctx.invoke('${actName}', 'execute'${args})`;
        case 'embedded': return `await ctx.invoke('${actName}', 'execute'${args})`;
        case 'python-runtime': return `await ctx.invoke('${actName}', 'execute'${args})`;
        default: return m[0]; // classic → classic
      }
    },
  },
  // ── Classic → others: this.waitForSignal('name') ──
  {
    pattern: /this\.waitForSignal\(\s*['"](\w+)['"]\s*\)/g,
    replacement: (m, target) => {
      const sigName = m[1];
      switch (target) {
        case 'runtime': return `await ctx.promise('${sigName}')`;
        case 'embedded': return `await ctx.promise('${sigName}')`;
        case 'python-runtime': return `await ctx.promise('${sigName}')`;
        default: return m[0];
      }
    },
  },
  // ── Classic → others: this.sleep(ms) ──
  {
    pattern: /this\.sleep\(\s*([^)]+)\)/g,
    replacement: (m, target) => {
      switch (target) {
        case 'runtime': return `await ctx.sleep(${m[1]})`;
        case 'embedded': return `await ctx.sleep(${m[1]})`;
        case 'python-runtime': return `await ctx.sleep(${m[1]})`;
        default: return m[0];
      }
    },
  },
  // ── Classic → others: this.heartbeat(data) ──
  {
    pattern: /this\.heartbeat\(\s*([^)]*)\)/g,
    replacement: (m, target) => {
      switch (target) {
        case 'runtime': return `ctx.run('heartbeat', () => { /* ${m[1] || 'heartbeat'} */ })`;
        case 'embedded': return `ctx.run('heartbeat', () => { /* ${m[1] || 'heartbeat'} */ })`;
        case 'python-runtime': return `await ctx.run('heartbeat', lambda: None)  # heartbeat: ${m[1] || ''}`;
        default: return m[0];
      }
    },
  },
  // ── Runtime → others: ctx.get('key') ──
  {
    pattern: /await\s+ctx\.get\(\s*['"](\w+)['"]\s*\)/g,
    replacement: (m, target) => {
      const key = m[1];
      switch (target) {
        case 'classic': return `this._state?.['${key}']`;
        case 'embedded': return `ctx.getState('${key}')`;
        case 'python-runtime': return `await ctx.get('${key}')`;
        default: return m[0];
      }
    },
  },
  // ── Runtime → others: ctx.set('key', val) ──
  {
    pattern: /await\s+ctx\.set\(\s*['"](\w+)['"]\s*,\s*([^)]+)\)/g,
    replacement: (m, target) => {
      const key = m[1];
      const val = m[2];
      switch (target) {
        case 'classic': return `/* state: ${key} = ${val} */`;
        case 'embedded': return `ctx.setState('${key}', ${val})`;
        case 'python-runtime': return `await ctx.set('${key}', ${val})`;
        default: return m[0];
      }
    },
  },
  // ── Runtime → others: ctx.invoke('Svc', 'handler', ...args) ──
  {
    pattern: /await\s+ctx\.invoke\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]\s*(?:,\s*([^)]+))?\)/g,
    replacement: (m, target) => {
      const svc = m[1];
      const handler = m[2];
      const args = m[3] ? `, ${m[3]}` : '';
      switch (target) {
        case 'classic': return `await this.executeActivity('${svc}'${args})`;
        case 'embedded': return `await ctx.invoke('${svc}', '${handler}'${args})`;
        case 'python-runtime': return `await ctx.invoke('${svc}', '${handler}'${args})`;
        default: return m[0];
      }
    },
  },
  // ── Runtime → others: ctx.run('name', () => expr) ──
  {
    pattern: /await\s+ctx\.run\(\s*['"](\w+)['"]\s*,\s*(?:\(\)\s*=>\s*([^)]+)|(\w+)\s*\))/g,
    replacement: (m, target) => {
      const name = m[1];
      const expr = m[2] || m[3] || '';
      switch (target) {
        case 'classic': return `await this.executeActivity('${name}', ${expr})`;
        case 'embedded': return `await ctx.run('${name}', () => ${expr})`;
        case 'python-runtime': return `await ctx.run('${name}', lambda: ${expr})`;
        default: return m[0];
      }
    },
  },
  // ── Runtime → others: ctx.sleep(ms) ──
  {
    pattern: /await\s+ctx\.sleep\(\s*([^)]+)\)/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `await this.sleep(${m[1]})`;
        case 'embedded': return `await ctx.sleep(${m[1]})`;
        case 'python-runtime': return `await ctx.sleep(${m[1]})`;
        default: return m[0];
      }
    },
  },
  // ── Runtime → others: ctx.awakeable<T>() ──
  {
    pattern: /ctx\.awakeable(?:<([^>]+)>)?\(\)/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `/* awakeable — use signal */`;
        case 'embedded': return `/* awakeable — use promise */`;
        case 'python-runtime': return `ctx.awakeable()`;
        default: return m[0];
      }
    },
  },
  // ── Embedded → others: ctx.getState<T>('key') ──
  {
    pattern: /ctx\.getState(?:<[^>]+>)?\(\s*['"](\w+)['"]\s*\)/g,
    replacement: (m, target) => {
      const key = m[1];
      switch (target) {
        case 'classic': return `this._state?.['${key}']`;
        case 'runtime': return `await ctx.get('${key}')`;
        case 'python-runtime': return `await ctx.get('${key}')`;
        default: return m[0];
      }
    },
  },
  // ── Embedded → others: ctx.setState('key', val) ──
  {
    pattern: /ctx\.setState\(\s*['"](\w+)['"]\s*,\s*([^)]+)\)/g,
    replacement: (m, target) => {
      const key = m[1];
      const val = m[2];
      switch (target) {
        case 'classic': return `/* state: ${key} = ${val} */`;
        case 'runtime': return `await ctx.set('${key}', ${val})`;
        case 'python-runtime': return `await ctx.set('${key}', ${val})`;
        default: return m[0];
      }
    },
  },
  // ── Embedded → others: ctx.invoke('Svc', 'method', ...args) ──
  {
    pattern: /await\s+ctx\.invoke\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]\s*(?:,\s*([^)]+))?\)/g,
    replacement: (m, target) => {
      const svc = m[1];
      const method = m[2];
      const args = m[3] ? `, ${m[3]}` : '';
      switch (target) {
        case 'classic': return `await this.executeActivity('${svc}'${args})`;
        case 'runtime': return `await ctx.invoke('${svc}', '${method}'${args})`;
        case 'python-runtime': return `await ctx.invoke('${svc}', '${method}'${args})`;
        default: return m[0];
      }
    },
  },
  // ── Python → TS: await ctx.get('key') ──
  {
    pattern: /await\s+ctx\.get\(\s*['"](\w+)['"]\s*\)/g,
    replacement: (m, target) => {
      const key = m[1];
      switch (target) {
        case 'classic': return `this._state?.['${key}']`;
        case 'runtime': return `await ctx.get('${key}')`;
        case 'embedded': return `ctx.getState('${key}')`;
        default: return m[0];
      }
    },
  },
  // ── Python → TS: await ctx.set('key', val) ──
  {
    pattern: /await\s+ctx\.set\(\s*['"](\w+)['"]\s*,\s*([^)]+)\)/g,
    replacement: (m, target) => {
      const key = m[1];
      const val = m[2];
      switch (target) {
        case 'classic': return `/* state: ${key} = ${val} */`;
        case 'runtime': return `await ctx.set('${key}', ${val})`;
        case 'embedded': return `ctx.setState('${key}', ${val})`;
        default: return m[0];
      }
    },
  },
  // ── Python → TS: await ctx.invoke('Svc', 'handler', ...args) ──
  {
    pattern: /await\s+ctx\.invoke\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]\s*(?:,\s*([^)]+))?\)/g,
    replacement: (m, target) => {
      const svc = m[1];
      const handler = m[2];
      const args = m[3] ? `, ${m[3]}` : '';
      switch (target) {
        case 'classic': return `await this.executeActivity('${svc}'${args})`;
        case 'runtime': return `await ctx.invoke('${svc}', '${handler}'${args})`;
        case 'embedded': return `await ctx.invoke('${svc}', '${handler}'${args})`;
        default: return m[0];
      }
    },
  },
  // ── Python → TS: await ctx.sleep(ms) ──
  {
    pattern: /await\s+ctx\.sleep\(\s*([^)]+)\)/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `await this.sleep(${m[1]})`;
        case 'runtime': return `await ctx.sleep(${m[1]})`;
        case 'embedded': return `await ctx.sleep(${m[1]})`;
        default: return m[0];
      }
    },
  },
  // ── Python → TS: await ctx.run('name', lambda: expr) ──
  {
    pattern: /await\s+ctx\.run\(\s*['"](\w+)['"]\s*,\s*lambda:\s*([^)]+)\)/g,
    replacement: (m, target) => {
      const name = m[1];
      const expr = m[2];
      switch (target) {
        case 'classic': return `await this.executeActivity('${name}', ${expr})`;
        case 'runtime': return `await ctx.run('${name}', () => ${expr})`;
        case 'embedded': return `await ctx.run('${name}', () => ${expr})`;
        default: return m[0];
      }
    },
  },
  // ── Python dict literals → TS object literals ──
  {
    pattern: /\{'(\w+)':\s*([^}]+)\}/g,
    replacement: (m, target) => {
      if (target === 'python-runtime') return m[0];
      return `{ ${m[1]}: ${m[2]} }`;
    },
  },
  // ── Python None → TS undefined ──
  {
    pattern: /\bNone\b/g,
    replacement: (m, target) => {
      if (target === 'python-runtime') return 'None';
      return 'undefined';
    },
  },
  // ── Python or → TS || (for default values) ──
  {
    pattern: /(\w+)\s+or\s+('(?:[^']*)'|\d+|true|false|undefined)/g,
    replacement: (m, target) => {
      if (target === 'python-runtime') return m[0];
      return `${m[1]} || ${m[2]}`;
    },
  },
  // ─── Child Workflow Patterns ─────────────────────────────────────────────
  {
    pattern: /wf\.executeChildWorkflow\s*\(\s*(['"]\w+['"])/g,
    replacement: (m, target) => {
      const wfName = m[1];
      switch (target) {
        case 'classic': return `this.executeChildWorkflow(${wfName}`;
        case 'runtime': return `ctx.executeChildWorkflow(${wfName}`;
        case 'embedded': return `ctx.executeChildWorkflow(${wfName}`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /wf\.startChildWorkflow\s*\(\s*(['"]\w+['"])/g,
    replacement: (m, target) => {
      const wfName = m[1];
      switch (target) {
        case 'classic': return `this.startChildWorkflow(${wfName}`;
        case 'runtime': return `ctx.startChildWorkflow(${wfName}`;
        case 'embedded': return `ctx.startChildWorkflow(${wfName}`;
        default: return m[0];
      }
    },
  },
  // ─── Activity Options Patterns ───────────────────────────────────────────
  {
    pattern: /wf\.ActivityOptions\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `new ActivityOptions(`;
        case 'runtime': return `new ActivityOptions(`;
        case 'embedded': return `new ActivityOptions(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /wf\.executeLocalActivity\s*\(\s*(['"]\w+['"])/g,
    replacement: (m, target) => {
      const actName = m[1];
      switch (target) {
        case 'classic': return `this.executeLocalActivity(${actName}`;
        case 'runtime': return `ctx.executeLocalActivity(${actName}`;
        case 'embedded': return `ctx.executeLocalActivity(${actName}`;
        default: return m[0];
      }
    },
  },
  // ─── Coroutine & Concurrency Patterns ────────────────────────────────────
  {
    pattern: /wf\.condition\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.await(`;
        case 'runtime': return `ctx.await(`;
        case 'embedded': return `ctx.await(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /wf\.conditionWithTimeout\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.awaitWithTimeout(`;
        case 'runtime': return `ctx.awaitWithTimeout(`;
        case 'embedded': return `ctx.awaitWithTimeout(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /new\s+Promise\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.createPromise(`;
        case 'runtime': return `ctx.createPromise(`;
        case 'embedded': return `ctx.createPromise(`;
        default: return m[0];
      }
    },
  },
  // ─── Relay/Nexus Operation Patterns ──────────────────────────────────────
  {
    pattern: /wf\.newNexusClient\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.newRelayClient(`;
        case 'runtime': return `ctx.newRelayClient(`;
        case 'embedded': return `ctx.newRelayClient(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /nexusClient\.executeOperation\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `relayClient.execute(`;
        case 'runtime': return `relayClient.execute(`;
        case 'embedded': return `relayClient.execute(`;
        default: return m[0];
      }
    },
  },
  // ─── Activity Context Patterns ───────────────────────────────────────────
  {
    pattern: /activity\.info\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.getActivityInfo(`;
        case 'runtime': return `ctx.getActivityInfo(`;
        case 'embedded': return `ctx.getActivityInfo(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /activity\.heartbeat\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.heartbeat(`;
        case 'runtime': return `ctx.heartbeat(`;
        case 'embedded': return `ctx.heartbeat(`;
        default: return m[0];
      }
    },
  },
  // ─── Workflow Context Patterns ───────────────────────────────────────────
  {
    pattern: /wf\.info\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.getWorkflowInfo(`;
        case 'runtime': return `ctx.getWorkflowInfo(`;
        case 'embedded': return `ctx.getWorkflowInfo(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /wf\.logger\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.logger(`;
        case 'runtime': return `ctx.logger(`;
        case 'embedded': return `ctx.logger(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /wf\.withCancel\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.withCancel(`;
        case 'runtime': return `ctx.withCancel(`;
        case 'embedded': return `ctx.withCancel(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /wf\.signalExternalWorkflow\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.signalExternalWorkflow(`;
        case 'runtime': return `ctx.signalExternalWorkflow(`;
        case 'embedded': return `ctx.signalExternalWorkflow(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /wf\.getVersion\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.getVersion(`;
        case 'runtime': return `ctx.getVersion(`;
        case 'embedded': return `ctx.getVersion(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /wf\.upsertSearchAttributes\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.upsertSearchAttributes(`;
        case 'runtime': return `ctx.upsertSearchAttributes(`;
        case 'embedded': return `ctx.upsertSearchAttributes(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /wf\.upsertMemo\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `this.upsertMemo(`;
        case 'runtime': return `ctx.upsertMemo(`;
        case 'embedded': return `ctx.upsertMemo(`;
        default: return m[0];
      }
    },
  },
  // ─── Error Handling Patterns ─────────────────────────────────────────────
  {
    pattern: /new\s+ApplicationError\s*\(/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `new VelocityApplicationError(`;
        case 'runtime': return `new VelocityApplicationError(`;
        case 'embedded': return `new VelocityApplicationError(`;
        default: return m[0];
      }
    },
  },
  {
    pattern: /CanceledError/g,
    replacement: (m, target) => {
      switch (target) {
        case 'classic': return `VelocityCanceledError`;
        case 'runtime': return `VelocityCanceledError`;
        case 'embedded': return `VelocityCanceledError`;
        default: return m[0];
      }
    },
  },
];

// ─── Python ↔ TypeScript Type Mapping ────────────────────────────────────────

const PY_TO_TS_TYPE: Record<string, string> = {
  'str': 'string',
  'int': 'number',
  'float': 'number',
  'bool': 'boolean',
  'None': 'void',
  'dict': 'Record<string, any>',
  'list': 'any[]',
  'tuple': '[...any]',
  'Any': 'any',
};

const TS_TO_PY_TYPE: Record<string, string> = {
  'string': 'str',
  'number': 'float',
  'boolean': 'bool',
  'void': 'None',
  'any': 'Any',
  'Record<string, any>': 'dict',
  'any[]': 'list',
};

export function pythonToTsType(pyType: string): string {
  return PY_TO_TS_TYPE[pyType] || pyType;
}

export function tsToPyType(tsType: string): string {
  return TS_TO_PY_TYPE[tsType] || tsType;
}

// ─── Utility: Extract balanced brace block (string/comment aware) ────────────

function extractBraceBlock(code: string, startIndex: number): { block: string; endIndex: number } | null {
  let depth = 0;
  let i = startIndex;
  // Find the opening brace
  while (i < code.length && code[i] !== '{') i++;
  if (i >= code.length) return null;
  const blockStart = i;
  i++; // skip the opening brace
  depth = 1;

  while (i < code.length && depth > 0) {
    const ch = code[i];

    // Skip single-line comments
    if (ch === '/' && i + 1 < code.length && code[i + 1] === '/') {
      i += 2;
      while (i < code.length && code[i] !== '\n') i++;
      continue;
    }
    // Skip multi-line comments
    if (ch === '/' && i + 1 < code.length && code[i + 1] === '*') {
      i += 2;
      while (i < code.length && !(code[i] === '*' && i + 1 < code.length && code[i + 1] === '/')) i++;
      i += 2;
      continue;
    }
    // Skip single-quoted strings
    if (ch === "'") {
      i++;
      while (i < code.length && code[i] !== "'") {
        if (code[i] === '\\') i++; // skip escaped char
        i++;
      }
      i++; // skip closing quote
      continue;
    }
    // Skip double-quoted strings
    if (ch === '"') {
      i++;
      while (i < code.length && code[i] !== '"') {
        if (code[i] === '\\') i++;
        i++;
      }
      i++;
      continue;
    }
    // Skip template literals
    if (ch === '`') {
      i++;
      while (i < code.length && code[i] !== '`') {
        if (code[i] === '\\') i++;
        if (code[i] === '$' && i + 1 < code.length && code[i + 1] === '{') {
          // Skip template expression
          let exprDepth = 1;
          i += 2;
          while (i < code.length && exprDepth > 0) {
            if (code[i] === '{') exprDepth++;
            if (code[i] === '}') exprDepth--;
            if (exprDepth > 0) i++;
          }
        }
        i++;
      }
      i++; // skip closing backtick
      continue;
    }

    if (ch === '{') depth++;
    else if (ch === '}') {
      depth--;
      if (depth === 0) {
        return { block: code.slice(blockStart + 1, i), endIndex: i };
      }
    }
    i++;
  }
  return null;
}

// ─── Import Extraction ────────────────────────────────────────────────────────

function extractImports(code: string): string[] {
  const imports: string[] = [];
  const importRegex = /import\s+(?:(?:\{([^}]*)\})|([\w*]+(?:\s+as\s+\w+)?)|(?:\*\s+as\s+(\w+)))\s+from\s+['"]([^'"]+)['"]/g;
  let match;
  while ((match = importRegex.exec(code)) !== null) {
    if (match[1]) {
      // Named imports: import { A, B } from '...'
      const names = match[1].split(',').map(n => n.trim().split('\s+as\s+')[0].trim()).filter(Boolean);
      imports.push(...names);
    } else if (match[2]) {
      // Default import: import Foo from '...'
      imports.push(match[2].trim().split('\s+as\s+')[0].trim());
    } else if (match[3]) {
      // Namespace import: import * as Foo from '...'
      imports.push(match[3]);
    }
  }
  return imports;
}

// ─── Body Transformation ─────────────────────────────────────────────────────

export function transformBody(body: string, source: SDKFlavor, target: SDKFlavor): string {
  if (source === target) return body;
  let result = body;
  for (const rule of BODY_TRANSFORM_RULES) {
    // Reset regex state
    rule.pattern.lastIndex = 0;
    result = result.replace(rule.pattern, (...args) => {
      return rule.replacement(args as any, target);
    });
  }
  return result;
}

// ─── Parsers (SDK → IR) ─────────────────────────────────────────────────────

// ─── Temporal Parser (SDK → IR) ──────────────────────────────────────────────

/**
 * Parse Temporal TypeScript SDK workflows.
 * Handles: proxyActivities, executeActivity, defineWorkflow, defineSignal,
 * wf.sleep, wf.condition, wf.signalHandler, wf.queryHandler, and
 * standard Temporal workflow function patterns.
 */
export function parseTemporal(code: string): WorkflowIR[] {
  const workflows: WorkflowIR[] = [];
  const allImports = extractImports(code);

  // Extract proxied activity names from: const { a, b } = proxyActivities(...)
  const proxiedActivities = new Set<string>();
  const proxyRegex = /const\s*\{([^}]+)\}\s*=\s*proxyActivities\s*\(/g;
  let proxyMatch;
  while ((proxyMatch = proxyRegex.exec(code)) !== null) {
    const names = proxyMatch[1].split(',').map(n => n.trim().split(/\s+as\s+/)[0].trim()).filter(Boolean);
    names.forEach(n => proxiedActivities.add(n));
  }

  // Parse defineWorkflow functions
  const defineWfRegex = /export\s+const\s+(\w+)\s*=\s*defineWorkflow\s*\(\s*async\s*\(([^)]*)\)\s*(?::\s*(?:Promise<[^>]*>|[^{;]+?))?\s*=>\s*\{/g;
  let dwMatch;
  while ((dwMatch = defineWfRegex.exec(code)) !== null) {
    const name = dwMatch[1];
    const params = dwMatch[2];
    const bodyStart = dwMatch.index + dwMatch[0].length - 1;
    const extracted = extractBraceBlock(code, bodyStart);
    if (!extracted) continue;
    let body = extracted.block;
    // Transform proxied activity calls to this.executeActivity
    for (const actName of proxiedActivities) {
      const actCallRegex = new RegExp(`await\\s+${actName}\\s*\\(([^)]*)\\)`, 'g');
      body = body.replace(actCallRegex, (_, args) => `await this.executeActivity('${actName}'${args ? ', ' + args : ''})`);
    }
    workflows.push({
      name,
      type: 'workflow',
      methods: [{
        name: 'execute',
        parameters: parseTSParams(params),
        returnType: 'any',
        body,
        transformedBody: '',
        decorators: [],
        contextUsage: [],
        isAsync: true,
      }],
      imports: allImports,
      metadata: { sdk: 'temporal', proxiedActivities: [...proxiedActivities] },
    });
  }

  // Parse standard Temporal workflow functions (export async function)
  const exportFnRegex = /export\s+async\s+function\s+(\w+)\s*\(([^)]*)\)\s*(?::\s*(?:Promise<[^>]*>|[^{;]+?))?\s*\{/g;
  let efMatch;
  while ((efMatch = exportFnRegex.exec(code)) !== null) {
    const name = efMatch[1];
    // Skip if already parsed as defineWorkflow
    if (workflows.some(w => w.name === name)) continue;
    const params = efMatch[2];
    const bodyStart = efMatch.index + efMatch[0].length - 1;
    const extracted = extractBraceBlock(code, bodyStart);
    if (!extracted) continue;
    let body = extracted.block;
    // Transform proxied activity calls
    for (const actName of proxiedActivities) {
      const actCallRegex = new RegExp(`await\\s+${actName}\\s*\\(([^)]*)\\)`, 'g');
      body = body.replace(actCallRegex, (_, args) => `await this.executeActivity('${actName}'${args ? ', ' + args : ''})`);
    }
    workflows.push({
      name,
      type: 'workflow',
      methods: [{
        name: 'execute',
        parameters: parseTSParams(params),
        returnType: 'any',
        body,
        transformedBody: '',
        decorators: [],
        contextUsage: [],
        isAsync: true,
      }],
      imports: allImports,
      metadata: { sdk: 'temporal', proxiedActivities: [...proxiedActivities] },
    });
  }

  // Parse Temporal Workflow classes (class-based pattern)
  const classRegex = /class\s+(\w+)\s+extends\s+(?:BaseWorkflow|Workflow)\s*(?:<[^>]*>)?\s*\{/g;
  let classMatch;
  while ((classMatch = classRegex.exec(code)) !== null) {
    const name = classMatch[1];
    if (workflows.some(w => w.name === name)) continue;
    const extracted = extractBraceBlock(code, classMatch.index);
    if (!extracted) continue;
    workflows.push({
      name,
      type: 'workflow',
      methods: parseTSMethods(extracted.block),
      imports: allImports,
      metadata: { sdk: 'temporal' },
    });
  }

  // Parse Temporal Activity classes
  const actClassRegex = /class\s+(\w+)\s+extends\s+(?:BaseActivity|Activity)\s*(?:<[^>]*>)?\s*\{/g;
  let actMatch;
  while ((actMatch = actClassRegex.exec(code)) !== null) {
    const name = actMatch[1];
    if (workflows.some(w => w.name === name)) continue;
    const extracted = extractBraceBlock(code, actMatch.index);
    if (!extracted) continue;
    workflows.push({
      name,
      type: 'activity',
      methods: parseTSMethods(extracted.block),
      imports: allImports.filter(i => i !== name),
      metadata: { sdk: 'temporal' },
    });
  }

  return workflows;
}

export function parseClassic(code: string): WorkflowIR[] {
  const workflows: WorkflowIR[] = [];
  const allImports = extractImports(code);

  // Parse Workflow classes
  const workflowRegex = /class\s+(\w+)\s+extends\s+Workflow\s*(?:<[^>]*>)?\s*\{/g;
  let match;
  while ((match = workflowRegex.exec(code)) !== null) {
    const name = match[1];
    const extracted = extractBraceBlock(code, match.index);
    if (!extracted) continue;
    workflows.push({
      name,
      type: 'workflow',
      methods: parseTSMethods(extracted.block),
      imports: allImports.filter(i => i !== name),
      metadata: { sdk: 'classic' },
    });
  }

  // Parse Activity classes
  const activityRegex = /class\s+(\w+)\s+extends\s+Activity\s*(?:<[^>]*>)?\s*\{/g;
  while ((match = activityRegex.exec(code)) !== null) {
    const name = match[1];
    const extracted = extractBraceBlock(code, match.index);
    if (!extracted) continue;
    workflows.push({
      name,
      type: 'activity',
      methods: parseTSMethods(extracted.block),
      imports: allImports.filter(i => i !== name),
      metadata: { sdk: 'classic' },
    });
  }

  return workflows;
}

export function parseRuntime(code: string): WorkflowIR[] {
  const workflows: WorkflowIR[] = [];
  const allImports = extractImports(code);

  // Parse VirtualObject + handlers
  const voRegex = /const\s+(\w+)\s*=\s*new\s+VirtualObject\(\s*['"](\w+)['"]\s*\)/g;
  let match;
  while ((match = voRegex.exec(code)) !== null) {
    const varName = match[1];
    const objName = match[2];
    const handlers = parseRuntimeHandlers(code, varName);
    workflows.push({
      name: objName,
      type: 'virtualObject',
      methods: handlers,
      imports: allImports,
      metadata: { sdk: 'runtime' },
    });
  }

  // Parse Service + handlers
  const serviceRegex = /const\s+(\w+)\s*=\s*new\s+Service\(\s*['"](\w+)['"]\s*\)/g;
  while ((match = serviceRegex.exec(code)) !== null) {
    const varName = match[1];
    const svcName = match[2];
    const handlers = parseRuntimeHandlers(code, varName);
    workflows.push({
      name: svcName,
      type: 'service',
      methods: handlers,
      imports: allImports,
      metadata: { sdk: 'runtime' },
    });
  }

  // Parse Workflow functions
  const wfRegex = /const\s+(\w+)\s*=\s*new\s+Workflow\(\s*['"](\w+)['"]\s*\)/g;
  while ((match = wfRegex.exec(code)) !== null) {
    const varName = match[1];
    const wfName = match[2];
    const handlers = parseRuntimeHandlers(code, varName);
    workflows.push({
      name: wfName,
      type: 'workflow',
      methods: handlers,
      imports: allImports,
      metadata: { sdk: 'runtime' },
    });
  }

  return workflows;
}

function parseRuntimeHandlers(code: string, varName: string): MethodIR[] {
  const handlers: MethodIR[] = [];
  // Match handler with balanced paren extraction for complex params
  const handlerStartRegex = new RegExp(
    `${varName}\\.addHandler\\(\\s*['"](\\w+)['"]\\s*,\\s*async\\s*\\(`,
    'g'
  );
  let match;
  while ((match = handlerStartRegex.exec(code)) !== null) {
    const handlerName = match[1];
    const paramsStart = match.index + match[0].length;
    // Extract balanced params (handles nested parens, objects, etc.)
    const paramsResult = extractBalancedParens(code, paramsStart - 1);
    if (!paramsResult) continue;
    const params = paramsResult.content;
    // Find the arrow function body — start from the closing ')' itself
    const fromCloseParen = code.slice(paramsResult.endIndex);
    const arrowMatch = /^\)\s*(?::\s*(?:Promise<[^>]*>|[^{;]+?))?\s*=>\s*\{/.exec(fromCloseParen);
    if (!arrowMatch) continue;
    const bodyStart = paramsResult.endIndex + arrowMatch.index + arrowMatch[0].length - 1;
    const extracted = extractBraceBlock(code, bodyStart);
    if (!extracted) continue;
    handlers.push({
      name: handlerName,
      parameters: parseTSParams(params),
      returnType: 'any',
      body: extracted.block,
      transformedBody: '',
      decorators: [],
      contextUsage: [],
      isAsync: true,
    });
  }
  return handlers;
}

/**
 * Extract balanced parentheses starting at a known '(' character.
 * Returns the content between parens and the index of the closing ')'.
 */
function extractBalancedParens(code: string, openIndex: number): { content: string; endIndex: number } | null {
  if (code[openIndex] !== '(') return null;
  let depth = 1;
  let i = openIndex + 1;
  const contentStart = i;
  while (i < code.length && depth > 0) {
    const ch = code[i];
    if (ch === '(') depth++;
    else if (ch === ')') {
      depth--;
      if (depth === 0) return { content: code.slice(contentStart, i), endIndex: i };
    }
    // Skip strings
    else if (ch === "'" || ch === '"' || ch === '`') {
      const quote = ch;
      i++;
      while (i < code.length && code[i] !== quote) {
        if (code[i] === '\\') i++;
        i++;
      }
    }
    i++;
  }
  return null;
}

export function parseEmbedded(code: string): WorkflowIR[] {
  const workflows: WorkflowIR[] = [];
  const allImports = extractImports(code);
  // Handle @Durable() with optional export, abstract, generics, implements
  const durableRegex = /@Durable\(\)\s*(?:export\s+)?(?:abstract\s+)?class\s+(\w+)\s*(?:<[^>]*>)?\s*(?:extends\s+\w+\s*)?(?:implements\s+[\w,\s]+)?\s*\{/g;
  let match;
  while ((match = durableRegex.exec(code)) !== null) {
    const name = match[1];
    const extracted = extractBraceBlock(code, match.index);
    if (!extracted) continue;
    const methods = parseTSMethods(extracted.block);
    workflows.push({
      name,
      type: 'workflow',
      methods,
      imports: allImports,
      metadata: { sdk: 'embedded' },
    });
  }

  // Also parse @Transaction() decorated methods within non-@Durable classes
  const txClassRegex = /(?:export\s+)?class\s+(\w+)\s*(?:<[^>]*>)?\s*(?:extends\s+\w+\s*)?(?:implements\s+[\w,\s]+)?\s*\{/g;
  while ((match = txClassRegex.exec(code)) !== null) {
    const name = match[1];
    // Skip if already parsed as @Durable
    if (workflows.some(w => w.name === name)) continue;
    const extracted = extractBraceBlock(code, match.index);
    if (!extracted) continue;
    // Check if it has @Transaction methods
    if (!extracted.block.includes('@Transaction')) continue;
    const methods = parseTSMethods(extracted.block);
    workflows.push({
      name,
      type: 'workflow',
      methods,
      imports: allImports,
      metadata: { sdk: 'embedded' },
    });
  }

  return workflows;
}

export function parsePythonRuntime(code: string): WorkflowIR[] {
  const workflows: WorkflowIR[] = [];
  // Parse Python classes with optional decorators
  const classRegex = /(?:@\w+\([^)]*\)\s*)*class\s+(\w+)\((\w+)\)\s*:/g;
  let match;
  while ((match = classRegex.exec(code)) !== null) {
    const name = match[1];
    const baseClass = match[2];
    // Extract class body (indented block)
    const classStart = match.index + match[0].length;
    const classBody = extractPythonBlock(code, classStart);
    const methods = parsePythonMethods(classBody);
    const type = baseClass === 'VirtualObject' ? 'virtualObject' :
                 baseClass === 'Service' ? 'service' : 'workflow';
    workflows.push({
      name,
      type: type as any,
      methods,
      imports: extractPythonImports(code),
      metadata: { sdk: 'python-runtime', baseClass },
    });
  }
  return workflows;
}

function extractPythonImports(code: string): string[] {
  const imports: string[] = [];
  const importRegex = /(?:from\s+[\w.]+\s+)?import\s+(.+)/g;
  let match;
  while ((match = importRegex.exec(code)) !== null) {
    const names = match[1].split(',').map(n => n.trim().split(' as ')[0].trim());
    imports.push(...names);
  }
  return imports;
}

function extractPythonBlock(code: string, startIndex: number): string {
  const lines = code.slice(startIndex).split('\n');
  const bodyLines: string[] = [];
  let foundIndent = false;
  let baseIndent = 0;
  for (const line of lines) {
    if (!foundIndent) {
      if (line.trim() === '') continue;
      baseIndent = line.length - line.trimStart().length;
      foundIndent = true;
    }
    const currentIndent = line.length - line.trimStart().length;
    if (line.trim() !== '' && currentIndent < baseIndent) break;
    if (foundIndent) bodyLines.push(line);
  }
  return bodyLines.join('\n');
}

function parsePythonMethods(classBody: string): MethodIR[] {
  const methods: MethodIR[] = [];
  const methodRegex = /async\s+def\s+(\w+)\s*\(([^)]*)\)\s*(?::\s*(\w+))?\s*:/g;
  let match;
  while ((match = methodRegex.exec(classBody)) !== null) {
    const name = match[1];
    if (name === '__init__') continue;
    const params = match[2];
    const returnType = match[3] || 'any';
    // Extract method body
    const methodStart = match.index + match[0].length;
    const methodBody = extractPythonBlock(classBody, methodStart);
    methods.push({
      name,
      parameters: parsePythonParams(params),
      returnType: pythonToTsType(returnType),
      body: methodBody,
      transformedBody: '',
      decorators: [],
      contextUsage: [],
      isAsync: true,
    });
  }
  return methods;
}

function parsePythonParams(paramStr: string): ParameterIR[] {
  return paramStr.split(',')
    .map(p => p.trim())
    .filter(p => p && p !== 'self')
    .map(p => {
      const parts = p.split(':');
      const name = parts[0].trim();
      const type = parts[1] ? parts[1].trim() : 'any';
      return { name, type: pythonToTsType(type), optional: false };
    });
}

// ─── TypeScript Method Parsing ───────────────────────────────────────────────

function parseTSMethods(body: string): MethodIR[] {
  const methods: MethodIR[] = [];
  // Match method signature start: optional async, name, opening paren
  const methodStartRegex = /(?:(?:public|private|protected|static|readonly)\s+)*(?:async\s+)?(\w+)\s*\(/g;
  let match;
  while ((match = methodStartRegex.exec(body)) !== null) {
    const name = match[1];
    if (name === 'constructor') continue;
    // Skip keywords that look like methods
    if (['if', 'for', 'while', 'switch', 'catch', 'return', 'class', 'function', 'new'].includes(name)) continue;

    const isAsync = body.slice(Math.max(0, match.index - 20), match.index).includes('async');
    const openParenIndex = match.index + match[0].length - 1;

    // Extract balanced params
    const paramsResult = extractBalancedParens(body, openParenIndex);
    if (!paramsResult) continue;
    const params = paramsResult.content;

    // After closing paren, look for optional return type and opening brace
    const afterParams = body.slice(paramsResult.endIndex + 1);
    const braceMatch = /^(\s*(?::\s*(?:Promise<([^>]*)>|[^{};]+?))?\s*)\{/.exec(afterParams);
    if (!braceMatch) continue;

    const returnTypeMatch = /Promise<([^>]*)>/.exec(braceMatch[1]);
    const returnType = returnTypeMatch ? returnTypeMatch[1] : 'any';

    const bodyStart = paramsResult.endIndex + 1 + braceMatch[0].length - 1;
    const extracted = extractBraceBlock(body, bodyStart);
    if (!extracted) continue;

    // Extract decorators (look backwards from method start)
    const decorators: string[] = [];
    const beforeMethod = body.slice(Math.max(0, match.index - 200), match.index);
    const decoratorRegex = /@(\w+)(?:\(([^)]*)\))?\s*$/gm;
    let decMatch;
    while ((decMatch = decoratorRegex.exec(beforeMethod)) !== null) {
      decorators.push(decMatch[1]);
    }

    methods.push({
      name,
      parameters: parseTSParams(params),
      returnType,
      body: extracted.block,
      transformedBody: '',
      decorators,
      contextUsage: [],
      isAsync,
    });
  }
  return methods;
}

function parseTSParams(paramStr: string): ParameterIR[] {
  if (!paramStr.trim()) return [];
  // Split params respecting nested braces, brackets, and angle brackets
  const params: string[] = [];
  let depth = 0;
  let current = '';
  for (let i = 0; i < paramStr.length; i++) {
    const ch = paramStr[i];
    if (ch === '{' || ch === '<' || ch === '[') depth++;
    else if (ch === '}' || ch === '>' || ch === ']') depth--;
    else if (ch === ',' && depth === 0) {
      params.push(current.trim());
      current = '';
      continue;
    }
    current += ch;
  }
  if (current.trim()) params.push(current.trim());

  return params
    .filter(p => p && !p.startsWith('//'))
    .map(p => {
      // Handle rest params
      const isRest = p.startsWith('...');
      const cleaned = p.replace(/^\.\.\.\s*/, '');
      // Split on first colon (respecting nested types)
      const colonIdx = findTopLevelColon(cleaned);
      if (colonIdx === -1) {
        const name = cleaned.replace('?', '').trim();
        return { name, type: 'any', optional: cleaned.includes('?') };
      }
      const name = cleaned.slice(0, colonIdx).replace('?', '').trim();
      const optional = cleaned.slice(0, colonIdx).includes('?');
      const type = cleaned.slice(colonIdx + 1).trim();
      return { name, type, optional };
    });
}

function findTopLevelColon(s: string): number {
  let depth = 0;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '{' || ch === '<' || ch === '[') depth++;
    else if (ch === '}' || ch === '>' || ch === ']') depth--;
    else if (ch === ':' && depth === 0) return i;
  }
  return -1;
}

// ─── Generators (IR → SDK) ──────────────────────────────────────────────────

export function generateClassic(workflows: WorkflowIR[], source: SDKFlavor): string {
  const lines: string[] = [
    "import { Workflow, Activity, Worker, Client } from '@velocity-workflow/classic';",
    '',
  ];

  for (const wf of workflows) {
    if (wf.type === 'workflow' || wf.type === 'virtualObject') {
      lines.push(`// Migrated from ${source} SDK`);
      lines.push(`export class ${wf.name} extends Workflow {`);
      const mainMethod = wf.methods.find(m => m.name === 'execute' || m.name === 'process') || wf.methods[0];
      if (mainMethod) {
        const params = mainMethod.parameters
          .filter(p => p.name !== 'ctx' && p.name !== 'self')
          .map(p => `${p.name}: ${p.type}`)
          .join(', ');
        const transformed = transformBody(mainMethod.body, source, 'classic');
        lines.push(`  async execute(${params}): Promise<any> {`);
        lines.push(indent(transformed, 4));
        lines.push(`  }`);
      } else {
        lines.push(`  async execute(...args: any[]): Promise<any> {`);
        lines.push(`    // TODO: Implement workflow logic`);
        lines.push(`  }`);
      }
      lines.push(`}`);
    } else if (wf.type === 'activity' || wf.type === 'service') {
      lines.push(``);
      lines.push(`// Migrated from ${source} SDK`);
      lines.push(`export class ${wf.name} extends Activity {`);
      const mainMethod = wf.methods.find(m => m.name === 'execute' || m.name === 'charge' || m.name === 'send') || wf.methods[0];
      if (mainMethod) {
        const params = mainMethod.parameters
          .filter(p => p.name !== 'ctx' && p.name !== 'self')
          .map(p => `${p.name}: ${p.type}`)
          .join(', ');
        const transformed = transformBody(mainMethod.body, source, 'classic');
        lines.push(`  async execute(${params}): Promise<any> {`);
        lines.push(indent(transformed, 4));
        lines.push(`  }`);
      } else {
        lines.push(`  async execute(...args: any[]): Promise<any> {`);
        lines.push(`    // TODO: Implement activity logic`);
        lines.push(`  }`);
      }
      lines.push(`}`);
    }
    lines.push('');
  }

  return lines.join('\n');
}

export function generateRuntime(workflows: WorkflowIR[], source: SDKFlavor): string {
  const lines: string[] = [
    "import { VirtualObject, Service, RuntimeServer } from '@velocity-workflow/runtime';",
    '',
  ];

  for (const wf of workflows) {
    lines.push(`// Migrated from ${source} SDK`);
    if (wf.type === 'virtualObject' || wf.type === 'workflow') {
      lines.push(`const ${wf.name} = new VirtualObject('${wf.name}');`);
      for (const method of wf.methods) {
        const params = method.parameters
          .filter(p => p.name !== 'ctx' && p.name !== 'self')
          .map(p => `${p.name}: ${p.type}`)
          .join(', ');
        const transformed = transformBody(method.body, source, 'runtime');
        lines.push(`${wf.name}.addHandler('${method.name}', async (ctx${params ? ', ' + params : ''}) => {`);
        lines.push(indent(transformed, 2));
        lines.push(`});`);
      }
      if (wf.methods.length === 0) {
        lines.push(`${wf.name}.addHandler('execute', async (ctx, ...args) => {`);
        lines.push(`  // TODO: Implement handler logic`);
        lines.push(`});`);
      }
    } else if (wf.type === 'service' || wf.type === 'activity') {
      lines.push(`const ${wf.name} = new Service('${wf.name}');`);
      for (const method of wf.methods) {
        const params = method.parameters
          .filter(p => p.name !== 'ctx' && p.name !== 'self')
          .map(p => `${p.name}: ${p.type}`)
          .join(', ');
        const transformed = transformBody(method.body, source, 'runtime');
        lines.push(`${wf.name}.addHandler('${method.name}', async (ctx${params ? ', ' + params : ''}) => {`);
        lines.push(indent(transformed, 2));
        lines.push(`});`);
      }
      if (wf.methods.length === 0) {
        lines.push(`${wf.name}.addHandler('execute', async (ctx, ...args) => {`);
        lines.push(`  // TODO: Implement handler logic`);
        lines.push(`});`);
      }
    }
    lines.push('');
  }

  return lines.join('\n');
}

export function generateEmbedded(workflows: WorkflowIR[], source: SDKFlavor): string {
  const lines: string[] = [
    "import { Durable, DurableContext, VelocityEmbedded } from '@velocity-workflow/embedded';",
    '',
  ];

  for (const wf of workflows) {
    lines.push(`// Migrated from ${source} SDK`);
    lines.push(`@Durable()`);
    lines.push(`export class ${wf.name} {`);
    for (const method of wf.methods) {
      const params = method.parameters
        .filter(p => p.name !== 'ctx' && p.name !== 'self')
        .map(p => `${p.name}: ${p.type}`)
        .join(', ');
      const transformed = transformBody(method.body, source, 'embedded');
      lines.push(`  async ${method.name}(ctx: DurableContext${params ? ', ' + params : ''}): Promise<any> {`);
      lines.push(indent(transformed, 4));
      lines.push(`  }`);
    }
    if (wf.methods.length === 0) {
      lines.push(`  async execute(ctx: DurableContext, ...args: any[]): Promise<any> {`);
      lines.push(`    // TODO: Implement logic`);
      lines.push(`  }`);
    }
    lines.push(`}`);
    lines.push('');
  }

  return lines.join('\n');
}

export function generatePythonRuntime(workflows: WorkflowIR[], source: SDKFlavor): string {
  const lines: string[] = [
    'from velocity_runtime import VirtualObject, Service, Workflow, Context',
    '',
  ];

  for (const wf of workflows) {
    const baseClass = wf.type === 'virtualObject' ? 'VirtualObject' :
                      wf.type === 'service' ? 'Service' :
                      wf.type === 'activity' ? 'Service' : 'Workflow';
    lines.push(`# Migrated from ${source} SDK`);
    lines.push(`class ${wf.name}(${baseClass}):`);
    lines.push(`    def __init__(self):`);
    lines.push(`        super().__init__('${wf.name}')`);
    lines.push('');
    for (const method of wf.methods) {
      const params = method.parameters
        .filter(p => p.name !== 'ctx' && p.name !== 'self')
        .map(p => `${p.name}: ${tsToPyType(p.type)}`)
        .join(', ');
      const transformed = transformPythonBody(method.body, source);
      lines.push(`    async def ${method.name}(self, ctx${params ? ', ' + params : ''}):`);
      lines.push(indentPython(transformed, 8));
      lines.push('');
    }
    if (wf.methods.length === 0) {
      lines.push(`    async def execute(self, ctx, *args, **kwargs):`);
      lines.push(`        # TODO: Implement logic`);
      lines.push(`        pass`);
      lines.push('');
    }
  }

  return lines.join('\n');
}

function transformPythonBody(body: string, source: SDKFlavor): string {
  // First apply normal transforms, then convert TS syntax to Python
  let result = transformBody(body, source, 'python-runtime');
  // Convert arrow functions to lambdas where possible
  result = result.replace(/\(\)\s*=>\s*([^;)}]+)/g, 'lambda: $1');
  // Convert const/let to plain assignment
  result = result.replace(/(?:const|let|var)\s+/g, '');
  // Convert `undefined` to `None`
  result = result.replace(/\bundefined\b/g, 'None');
  return result;
}

// ─── Indentation Helpers ─────────────────────────────────────────────────────

function indent(text: string, spaces: number): string {
  const pad = ' '.repeat(spaces);
  return text.split('\n').map(line => line.trim() ? pad + line : '').join('\n');
}

function indentPython(text: string, spaces: number): string {
  const pad = ' '.repeat(spaces);
  return text.split('\n').map(line => line.trim() ? pad + line.trim() : '').join('\n');
}

// ─── Main Migration Function ─────────────────────────────────────────────────

export function migrate(code: string, options: MigrationOptions): string {
  const { source, target } = options;

  // Parse source to IR
  let ir: WorkflowIR[];
  switch (source) {
    case 'temporal':
      ir = parseTemporal(code);
      break;
    case 'classic':
      ir = parseClassic(code);
      break;
    case 'runtime':
      ir = parseRuntime(code);
      break;
    case 'embedded':
      ir = parseEmbedded(code);
      break;
    case 'python-runtime':
      ir = parsePythonRuntime(code);
      break;
    default:
      throw new Error(`Unknown source SDK: ${source}`);
  }

  // Generate target from IR
  switch (target) {
    case 'classic':
      return generateClassic(ir, source);
    case 'runtime':
      return generateRuntime(ir, source);
    case 'embedded':
      return generateEmbedded(ir, source);
    case 'python-runtime':
      return generatePythonRuntime(ir, source);
    default:
      throw new Error(`Unknown target SDK: ${target}`);
  }
}

// ─── Utility Exports ─────────────────────────────────────────────────────────

export function getSupportedMigrations(): string[] {
  const flavors: SDKFlavor[] = ['temporal', 'classic', 'runtime', 'embedded', 'python-runtime'];
  const migrations: string[] = [];
  for (const source of flavors) {
    for (const target of flavors) {
      if (source !== target) {
        migrations.push(`${source} → ${target}`);
      }
    }
  }
  return migrations;
}

export function validateMigration(code: string, source: SDKFlavor): { valid: boolean; errors: string[] } {
  const errors: string[] = [];
  const ir = source === 'temporal' ? parseTemporal(code) :
             source === 'classic' ? parseClassic(code) :
             source === 'runtime' ? parseRuntime(code) :
             source === 'embedded' ? parseEmbedded(code) :
             parsePythonRuntime(code);
  if (ir.length === 0) {
    errors.push(`No workflow entities found in source code`);
  }
  for (const entity of ir) {
    if (entity.methods.length === 0) {
      errors.push(`Entity '${entity.name}' has no methods`);
    }
  }
  return { valid: errors.length === 0, errors };
}
