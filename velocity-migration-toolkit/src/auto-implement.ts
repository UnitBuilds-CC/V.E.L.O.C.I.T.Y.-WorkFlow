/**
 * Auto-Implement Engine
 *
 * Scans a codebase for non-durable workflow patterns and automatically converts
 * them to durable Velocity workflows. Detects common patterns from frameworks
 * like Express, Fastify, NestJS, plain async services, and retry-heavy code,
 * then rewrites them as durable workflows.
 *
 * Detection patterns:
 *   - Express/Fastify route handlers with async logic → durable workflow + activities
 *   - Classes with retry/circuit-breaker patterns → durable workflow with retry
 *   - Async service classes with sequential calls → durable workflow
 *   - Functions with manual compensation/rollback → durable workflow with undo
 *   - Scheduled job handlers → durable workflow with timer
 *   - State machine implementations → durable workflow with signals
 */

import * as fs from 'fs';
import * as path from 'path';
import { SDKFlavor, migrate } from './index';
import { scanProject, detectFramework, DiscoveredFile } from './scanner';

// ─── Pattern Detection ───────────────────────────────────────────────────────

/** A detected pattern in source code that can be converted to a durable workflow. */
export interface DetectedPattern {
  /** Pattern type identifier. */
  type: PatternType;
  /** Confidence 0.0–1.0. */
  confidence: number;
  /** The matched code region. */
  codeRegion: string;
  /** Start line in the file. */
  startLine: number;
  /** End line in the file. */
  endLine: number;
  /** Extracted entities (function names, class names, etc.). */
  entities: string[];
  /** Human-readable description of what was detected. */
  description: string;
}

export type PatternType =
  | 'express-route'
  | 'fastify-route'
  | 'nestjs-controller'
  | 'retry-pattern'
  | 'compensation-pattern'
  | 'async-service'
  | 'scheduled-job'
  | 'state-machine'
  | 'saga-pattern'
  | 'event-handler'
  | 'temporal-workflow'
  | 'restate-service'
  | 'dbos-transaction';

/** A pattern scanner that looks for a specific non-durable pattern. */
interface PatternScanner {
  type: PatternType;
  /** File extensions this scanner applies to. */
  extensions: string[];
  /** Scan file content and return detected patterns. */
  scan(content: string, filePath: string): DetectedPattern[];
}

// ─── Pattern Scanners ────────────────────────────────────────────────────────

const expressScanner: PatternScanner = {
  type: 'express-route',
  extensions: ['.ts', '.js', '.tsx', '.jsx'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    // Match: app.get/post/put/delete('/path', async (req, res) => { ... })
    const routeRegex = /app\.(get|post|put|delete|patch)\s*\(\s*['"]([^'"]+)['"]\s*,\s*async\s*\(([^)]*)\)\s*(?::\s*\w+)?\s*=>\s*\{/g;
    let match;
    while ((match = routeRegex.exec(content)) !== null) {
      const [fullMatch, method, routePath, params] = match;
      const startLine = content.slice(0, match.index).split('\n').length;
      const endLine = startLine + fullMatch.split('\n').length;
      // Check if the route body has async operations (DB calls, HTTP calls, etc.)
      const bodyStart = match.index + fullMatch.length - 1;
      const body = extractBraceBlock(content, bodyStart);
      if (body && hasAsyncOperations(body)) {
        const fnName = `${method}_${routePath.replace(/[^a-zA-Z0-9]/g, '_')}`;
        patterns.push({
          type: 'express-route',
          confidence: 0.85,
          codeRegion: fullMatch,
          startLine,
          endLine,
          entities: [fnName, routePath],
          description: `Express ${method.toUpperCase()} ${routePath} route with async operations`,
        });
      }
    }
    return patterns;
  },
};

const fastifyScanner: PatternScanner = {
  type: 'fastify-route',
  extensions: ['.ts', '.js', '.tsx', '.jsx'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const routeRegex = /fastify\.(get|post|put|delete|patch)\s*\(\s*['"]([^'"]+)['"]\s*,\s*async\s*\(([^)]*)\)\s*(?::\s*\w+)?\s*(?:=>\s*\{|\{)/g;
    let match;
    while ((match = routeRegex.exec(content)) !== null) {
      const [fullMatch, method, routePath] = match;
      const startLine = content.slice(0, match.index).split('\n').length;
      const bodyStart = match.index + fullMatch.length - 1;
      const body = extractBraceBlock(content, bodyStart);
      if (body && hasAsyncOperations(body)) {
        const fnName = `${method}_${routePath.replace(/[^a-zA-Z0-9]/g, '_')}`;
        patterns.push({
          type: 'fastify-route',
          confidence: 0.85,
          codeRegion: fullMatch,
          startLine,
          endLine: startLine + fullMatch.split('\n').length,
          entities: [fnName, routePath],
          description: `Fastify ${method.toUpperCase()} ${routePath} route with async operations`,
        });
      }
    }
    return patterns;
  },
};

const retryScanner: PatternScanner = {
  type: 'retry-pattern',
  extensions: ['.ts', '.js', '.tsx', '.jsx', '.py', '.go', '.java', '.rs'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    // Match: for (retry) loops, try/catch with retry logic, backoff patterns
    const retryPatterns = [
      /(?:for|while)\s*\(\s*(?:let\s+)?(?:retry|attempt|i)\s*[<=<]/g,
      /(?:retry|backoff|exponential.*backoff|withRetry|retryAsync)/gi,
      /(?:catch|except).*\{[\s\S]*?(?:retry|attempt|sleep|delay)/gi,
      /(?:maxRetries|max_retries|MAX_RETRIES|retryCount|retry_count)/g,
    ];

    for (const pattern of retryPatterns) {
      let match;
      while ((match = pattern.exec(content)) !== null) {
        const startLine = content.slice(0, match.index).split('\n').length;
        // Find the enclosing function/class
        const enclosing = findEnclosingFunction(content, match.index);
        if (enclosing) {
          patterns.push({
            type: 'retry-pattern',
            confidence: 0.7,
            codeRegion: enclosing.code,
            startLine: enclosing.startLine,
            endLine: enclosing.endLine,
            entities: [enclosing.name],
            description: `Retry pattern in ${enclosing.name}`,
          });
        }
      }
    }
    // Deduplicate by function name
    const seen = new Set<string>();
    return patterns.filter(p => {
      const key = p.entities[0] || `${p.startLine}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  },
};

const compensationScanner: PatternScanner = {
  type: 'compensation-pattern',
  extensions: ['.ts', '.js', '.tsx', '.jsx', '.py', '.go', '.java'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    // Look for rollback/compensate/undo/reverse patterns
    const compPatterns = [
      /(?:rollback|compensate|undo|reverse|revert|cleanup).*function/gi,
      /(?:try\s*\{[\s\S]*?catch[\s\S]*?(?:rollback|compensate|undo))/gi,
      /(?:saga|orchestrat|coordinat)/gi,
    ];

    for (const pattern of compPatterns) {
      let match;
      while ((match = pattern.exec(content)) !== null) {
        const startLine = content.slice(0, match.index).split('\n').length;
        const enclosing = findEnclosingFunction(content, match.index);
        if (enclosing) {
          patterns.push({
            type: 'compensation-pattern',
            confidence: 0.8,
            codeRegion: enclosing.code,
            startLine: enclosing.startLine,
            endLine: enclosing.endLine,
            entities: [enclosing.name],
            description: `Compensation/saga pattern in ${enclosing.name}`,
          });
        }
      }
    }
    const seen = new Set<string>();
    return patterns.filter(p => {
      const key = p.entities[0] || `${p.startLine}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  },
};

const asyncServiceScanner: PatternScanner = {
  type: 'async-service',
  extensions: ['.ts', '.js', '.tsx', '.jsx'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    // Classes with multiple async methods that call external services
    const classRegex = /class\s+(\w+(?:Service|Handler|Controller|Manager|Processor))\s*(?:extends|implements|\{)/g;
    let match;
    while ((match = classRegex.exec(content)) !== null) {
      const className = match[1];
      const bodyStart = match.index + match[0].length - 1;
      const body = extractBraceBlock(content, bodyStart);
      if (!body) continue;

      // Count async methods and external calls
      const asyncMethods = (body.match(/async\s+\w+/g) || []).length;
      const externalCalls = (body.match(/(?:fetch|axios|http\.|grpc|db\.|query|execute|send)/g) || []).length;

      if (asyncMethods >= 2 && externalCalls >= 2) {
        const startLine = content.slice(0, match.index).split('\n').length;
        patterns.push({
          type: 'async-service',
          confidence: 0.75,
          codeRegion: match[0] + body,
          startLine,
          endLine: startLine + (match[0] + body).split('\n').length,
          entities: [className],
          description: `Async service class ${className} with ${asyncMethods} async methods and ${externalCalls} external calls`,
        });
      }
    }
    return patterns;
  },
};

const scheduledJobScanner: PatternScanner = {
  type: 'scheduled-job',
  extensions: ['.ts', '.js', '.tsx', '.jsx', '.py', '.go', '.java'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const cronPatterns = [
      /(?:cron|schedule|setInterval|cronJob|scheduledTask|@Schedule|@Cron)/gi,
      /(?:every\s+\d+\s*(?:ms|seconds?|minutes?|hours?))/gi,
    ];

    for (const pattern of cronPatterns) {
      let match;
      while ((match = pattern.exec(content)) !== null) {
        const startLine = content.slice(0, match.index).split('\n').length;
        const enclosing = findEnclosingFunction(content, match.index);
        if (enclosing) {
          patterns.push({
            type: 'scheduled-job',
            confidence: 0.7,
            codeRegion: enclosing.code,
            startLine: enclosing.startLine,
            endLine: enclosing.endLine,
            entities: [enclosing.name],
            description: `Scheduled job pattern in ${enclosing.name}`,
          });
        }
      }
    }
    const seen = new Set<string>();
    return patterns.filter(p => {
      const key = p.entities[0] || `${p.startLine}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  },
};

const stateMachineScanner: PatternScanner = {
  type: 'state-machine',
  extensions: ['.ts', '.js', '.tsx', '.jsx', '.py', '.go', '.java'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const smPatterns = [
      /(?:state\s*machine|StateMachine|stateMachine)/g,
      /(?:transition|nextState|currentState|setState)/g,
      /(?:enum\s+\w*(?:State|Status|Phase))/g,
    ];

    for (const pattern of smPatterns) {
      let match;
      while ((match = pattern.exec(content)) !== null) {
        const startLine = content.slice(0, match.index).split('\n').length;
        const enclosing = findEnclosingFunction(content, match.index);
        if (enclosing) {
          patterns.push({
            type: 'state-machine',
            confidence: 0.65,
            codeRegion: enclosing.code,
            startLine: enclosing.startLine,
            endLine: enclosing.endLine,
            entities: [enclosing.name],
            description: `State machine pattern in ${enclosing.name}`,
          });
        }
      }
    }
    const seen = new Set<string>();
    return patterns.filter(p => {
      const key = p.entities[0] || `${p.startLine}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  },
};

const temporalScanner: PatternScanner = {
  type: 'temporal-workflow',
  extensions: ['.ts', '.js', '.py', '.go', '.java'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const temporalSigs = [
      /from\s+['"]@temporalio\//,
      /import\s+.*temporalio/,
      /using\s+Temporal\.Sdk/,
      /from\s+temporalio\s+import/,
    ];
    for (const sig of temporalSigs) {
      if (sig.test(content)) {
        patterns.push({
          type: 'temporal-workflow',
          confidence: 0.95,
          codeRegion: content.slice(0, Math.min(content.length, 500)),
          startLine: 1,
          endLine: content.split('\n').length,
          entities: ['(entire file)'],
          description: `Temporal SDK imports detected — use migration toolkit for full conversion`,
        });
        break;
      }
    }
    return patterns;
  },
};

const restateScanner: PatternScanner = {
  type: 'restate-service',
  extensions: ['.ts', '.js', '.py', '.rs'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const restateSigs = [
      /from\s+['"]@restatedev\//,
      /import\s+restate/,
      /from\s+restate\s+import/,
      /restate_sdk/,
    ];
    for (const sig of restateSigs) {
      if (sig.test(content)) {
        patterns.push({
          type: 'restate-service',
          confidence: 0.95,
          codeRegion: content.slice(0, Math.min(content.length, 500)),
          startLine: 1,
          endLine: content.split('\n').length,
          entities: ['(entire file)'],
          description: `Restate SDK imports detected — use migration toolkit for full conversion`,
        });
        break;
      }
    }
    return patterns;
  },
};

const dbosScanner: PatternScanner = {
  type: 'dbos-transaction',
  extensions: ['.ts', '.js', '.py'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const dbosSigs = [
      /from\s+['"]@dbos-inc\//,
      /from\s+dbos\s+import/,
      /@DBOS\./,
    ];
    for (const sig of dbosSigs) {
      if (sig.test(content)) {
        patterns.push({
          type: 'dbos-transaction',
          confidence: 0.95,
          codeRegion: content.slice(0, Math.min(content.length, 500)),
          startLine: 1,
          endLine: content.split('\n').length,
          entities: ['(entire file)'],
          description: `DBOS SDK imports detected — use migration toolkit for full conversion`,
        });
        break;
      }
    }
    return patterns;
  },
};

// ─── Gap Pattern Scanners ────────────────────────────────────────────────────

/** Detects search attribute usage that could benefit from durable workflow context. */
const searchAttributesScanner: PatternScanner = {
  type: 'search-attributes',
  extensions: ['.ts', '.js', '.py', '.go', '.java', '.rs'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const sigs = [
      /workflow\.searchAttributes?\s*\(/,
      /workflow\.getSearchAttributes\s*\(/,
      /Workflow::search_attributes\s*\(/,
      /Workflow::getSearchAttributes\s*\(/,
    ];
    for (const sig of sigs) {
      const match = content.match(sig);
      if (match) {
        patterns.push({
          type: 'search-attributes',
          confidence: 0.85,
          codeRegion: content.slice(Math.max(0, match.index! - 100), Math.min(content.length, match.index! + 200)),
          startLine: content.slice(0, match.index).split('\n').length,
          endLine: content.slice(0, match.index).split('\n').length,
          entities: ['search-attributes-usage'],
          description: `Search attributes usage detected — migrate to Velocity for durable context-aware search attributes`,
        });
      }
    }
    return patterns;
  },
};

/** Detects queue operations that could benefit from durable queue processing. */
const queueOperationScanner: PatternScanner = {
  type: 'queue-operation',
  extensions: ['.ts', '.js', '.py', '.go', '.java', '.rs', '.php', '.rb'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const sigs = [
      /DBOS\.enqueue\s*\(/,
      /DBOS\.dequeue\s*\(/,
      /dbos::enqueue\s*\(/,
      /dbos\.Enqueue\s*\(/,
    ];
    for (const sig of sigs) {
      const match = content.match(sig);
      if (match) {
        patterns.push({
          type: 'queue-operation',
          confidence: 0.9,
          codeRegion: content.slice(Math.max(0, match.index! - 100), Math.min(content.length, match.index! + 200)),
          startLine: content.slice(0, match.index).split('\n').length,
          endLine: content.slice(0, match.index).split('\n').length,
          entities: ['queue-operation'],
          description: `Queue operation detected — migrate to Velocity for durable queue processing with automatic retries`,
        });
      }
    }
    return patterns;
  },
};

/** Detects HTTP handlers that could benefit from durable HTTP workflow integration. */
const httpHandlerScanner: PatternScanner = {
  type: 'http-handler',
  extensions: ['.ts', '.js', '.py', '.go', '.java', '.rs', '.php', '.rb'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const sigs = [
      /@DBOS\.httpHandler\s*\(/,
      /#\[dbos::http_handler\]/,
      /#\[DBOS\\HttpHandler/,
      /dbos\.HTTPHandler\s*\(/,
    ];
    for (const sig of sigs) {
      const match = content.match(sig);
      if (match) {
        patterns.push({
          type: 'http-handler',
          confidence: 0.85,
          codeRegion: content.slice(Math.max(0, match.index! - 100), Math.min(content.length, match.index! + 200)),
          startLine: content.slice(0, match.index).split('\n').length,
          endLine: content.slice(0, match.index).split('\n').length,
          entities: ['http-handler'],
          description: `HTTP handler detected — migrate to Velocity for durable HTTP-triggered workflows`,
        });
      }
    }
    return patterns;
  },
};

/** Detects update handler patterns that could benefit from durable update processing. */
const updateHandlerScanner: PatternScanner = {
  type: 'update-handler',
  extensions: ['.ts', '.js', '.py', '.go', '.java', '.rs'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const sigs = [
      /@workflow\.update/,
      /#\[temporal::update\]/,
      /@UpdateMethod/,
      /workflow\.SetUpdateHandler\s*\(/,
    ];
    for (const sig of sigs) {
      const match = content.match(sig);
      if (match) {
        patterns.push({
          type: 'update-handler',
          confidence: 0.85,
          codeRegion: content.slice(Math.max(0, match.index! - 100), Math.min(content.length, match.index! + 200)),
          startLine: content.slice(0, match.index).split('\n').length,
          endLine: content.slice(0, match.index).split('\n').length,
          entities: ['update-handler'],
          description: `Update handler detected — migrate to Velocity for durable update processing`,
        });
      }
    }
    return patterns;
  },
};

/** Detects continue-as-new patterns that could benefit from Velocity's workflow continuation. */
const continueAsNewScanner: PatternScanner = {
  type: 'continue-as-new',
  extensions: ['.ts', '.js', '.py', '.go', '.java', '.rs'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const sigs = [
      /workflow\.continueAsNew\s*\(/,
      /workflow\.continue_as_new\s*\(/,
      /Workflow::continue_as_new\s*\(/,
      /Workflow::continueAsNew\s*\(/,
    ];
    for (const sig of sigs) {
      const match = content.match(sig);
      if (match) {
        patterns.push({
          type: 'continue-as-new',
          confidence: 0.9,
          codeRegion: content.slice(Math.max(0, match.index! - 100), Math.min(content.length, match.index! + 200)),
          startLine: content.slice(0, match.index).split('\n').length,
          endLine: content.slice(0, match.index).split('\n').length,
          entities: ['continue-as-new'],
          description: `Continue-as-new detected — migrate to Velocity for durable workflow continuation with state preservation`,
        });
      }
    }
    return patterns;
  },
};

/** Detects idempotency key patterns that could benefit from Velocity's built-in idempotency. */
const idempotencyScanner: PatternScanner = {
  type: 'idempotency',
  extensions: ['.ts', '.js', '.py', '.go', '.java', '.rs', '.php', '.rb'],
  scan(content, filePath) {
    const patterns: DetectedPattern[] = [];
    const sigs = [
      /ctx\.idempotencyKey/,
      /context\.idempotency_key/,
      /ctx\.IdempotencyKey\s*\(/,
      /context\.idempotencyKey\s*\(/,
    ];
    for (const sig of sigs) {
      const match = content.match(sig);
      if (match) {
        patterns.push({
          type: 'idempotency',
          confidence: 0.85,
          codeRegion: content.slice(Math.max(0, match.index! - 100), Math.min(content.length, match.index! + 200)),
          startLine: content.slice(0, match.index).split('\n').length,
          endLine: content.slice(0, match.index).split('\n').length,
          entities: ['idempotency-key'],
          description: `Idempotency key detected — migrate to Velocity for built-in idempotency support`,
        });
      }
    }
    return patterns;
  },
};

/** All registered pattern scanners. */
const ALL_SCANNERS: PatternScanner[] = [
  expressScanner,
  fastifyScanner,
  retryScanner,
  compensationScanner,
  asyncServiceScanner,
  scheduledJobScanner,
  stateMachineScanner,
  temporalScanner,
  restateScanner,
  dbosScanner,
  searchAttributesScanner,
  queueOperationScanner,
  httpHandlerScanner,
  updateHandlerScanner,
  continueAsNewScanner,
  idempotencyScanner,
];

// ─── Code Generation ─────────────────────────────────────────────────────────

/**
 * Generate a durable Velocity workflow from a detected pattern.
 */
function generateDurableWorkflow(pattern: DetectedPattern, targetFlavor: SDKFlavor): string {
  switch (targetFlavor) {
    case 'server': return generateServerWorkflow(pattern);
    case 'binary': return generateBinaryWorkflow(pattern);
    case 'embedded': return generateEmbeddedWorkflow(pattern);
    default: return generateServerWorkflow(pattern);
  }
}

function generateServerWorkflow(pattern: DetectedPattern): string {
  const name = toPascalCase(pattern.entities[0] || 'AutoGenerated');

  switch (pattern.type) {
    case 'express-route':
    case 'fastify-route':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type}

export class ${name}Workflow extends Workflow {
  async execute(...args: any[]): Promise<any> {
    // Step 1: Validate input
    const result = await this.executeActivity('validateInput', args);

    // Step 2: Execute main business logic
    const output = await this.executeActivity('processRequest', result);

    // Step 3: Persist result
    await this.executeActivity('persistResult', output);

    return output;
  }
}

export class ValidateInputActivity extends Activity {
  async execute(args: any[]): Promise<any> {
    // TODO: Move validation logic from original route handler
    return args;
  }
}

export class ProcessRequestActivity extends Activity {
  async execute(input: any): Promise<any> {
    // TODO: Move core processing logic from original route handler
    return input;
  }
}

export class PersistResultActivity extends Activity {
  async execute(result: any): Promise<any> {
    // TODO: Move persistence logic from original route handler
    return result;
  }
}
`;

    case 'retry-pattern':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type} — now has built-in durable retry

export class ${name}Workflow extends Workflow {
  async execute(...args: any[]): Promise<any> {
    // Retry is now handled durably by the Velocity engine
    const result = await this.executeActivity('${name}Activity', args);
    return result;
  }
}

export class ${name}Activity extends Activity {
  async execute(args: any[]): Promise<any> {
    // TODO: Move logic from original function
    // Retry/backoff is now handled by the engine — no manual retry needed
    return args;
  }
}
`;

    case 'compensation-pattern':
    case 'saga-pattern':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type} — now has durable compensation

export class ${name}Workflow extends Workflow {
  async execute(...args: any[]): Promise<any> {
    const step1Result = await this.executeActivity('step1', args);
    try {
      const step2Result = await this.executeActivity('step2', step1Result);
      const step3Result = await this.executeActivity('step3', step2Result);
      return step3Result;
    } catch (error) {
      // Compensation is now durable — survives crashes
      await this.executeActivity('compensateStep2', step1Result);
      await this.executeActivity('compensateStep1', step1Result);
      throw error;
    }
  }
}

// TODO: Implement activity classes for each step and compensation
`;

    case 'async-service':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type} — service methods are now durable

export class ${name}Workflow extends Workflow {
  async execute(...args: any[]): Promise<any> {
    // Each service method becomes an activity
    const result1 = await this.executeActivity('method1', args);
    const result2 = await this.executeActivity('method2', result1);
    return result2;
  }
}

// TODO: Create Activity classes for each service method
`;

    case 'scheduled-job':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type} — now has durable timers

export class ${name}Workflow extends Workflow {
  async execute(...args: any[]): Promise<any> {
    // Timer is now durable — survives restarts
    await this.sleep(60000); // TODO: Set correct interval
    const result = await this.executeActivity('${name}Activity', args);
    return result;
  }
}

export class ${name}Activity extends Activity {
  async execute(args: any[]): Promise<any> {
    // TODO: Move job logic here
    return args;
  }
}
`;

    case 'state-machine':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type} — now uses durable signals for state transitions

export class ${name}Workflow extends Workflow {
  async execute(...args: any[]): Promise<any> {
    let state = 'init';

    while (state !== 'done') {
      // Signals are now durable — state transitions survive crashes
      const signal = await this.waitForSignal('transition');
      state = await this.executeActivity('handleTransition', { state, signal });
    }

    return state;
  }
}
`;

    case 'temporal-workflow':
      return `// Auto-detected: ${pattern.description}
// Use the migration toolkit for full conversion:
//   velocity-migrate --project <dir> --from temporal --to classic
// This file contains Temporal SDK imports that should be migrated, not auto-implemented.
`;

    case 'restate-service':
      return `// Auto-detected: ${pattern.description}
// Use the migration toolkit for full conversion:
//   velocity-migrate --project <dir> --from runtime --to classic
`;

    case 'dbos-transaction':
      return `// Auto-detected: ${pattern.description}
// Use the migration toolkit for full conversion:
//   velocity-migrate --project <dir> --from embedded --to classic
`;

    case 'search-attributes':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type} — now has durable context-aware search attributes

export class ${name}Workflow extends Workflow {
  async execute(...args: any[]): Promise<any> {
    // Search attributes are now managed through the durable workflow context
    // They persist across workflow execution and are queryable
    const result = await this.executeActivity('${name}Activity', args);
    return result;
  }
}

export class ${name}Activity extends Activity {
  async execute(args: any[]): Promise<any> {
    // TODO: Move logic from original code
    // Search attributes can be set via: this.workflowContext.setSearchAttributes({...})
    return args;
  }
}
`;

    case 'queue-operation':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type} — now has durable queue processing

export class ${name}Workflow extends Workflow {
  async execute(...args: any[]): Promise<any> {
    // Queue operations are now durable — messages survive crashes
    const result = await this.executeActivity('${name}Activity', args);
    return result;
  }
}

export class ${name}Activity extends Activity {
  async execute(args: any[]): Promise<any> {
    // TODO: Move queue processing logic here
    // Enqueue/dequeue are now handled durably by the Velocity engine
    return args;
  }
}
`;

    case 'http-handler':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type} — now has durable HTTP-triggered workflows

export class ${name}Workflow extends Workflow {
  async execute(httpRequest: any): Promise<any> {
    // HTTP handler is now integrated with durable workflow execution
    const result = await this.executeActivity('processHttpRequest', httpRequest);
    return result;
  }
}

export class ProcessHttpRequestActivity extends Activity {
  async execute(request: any): Promise<any> {
    // TODO: Move HTTP handling logic here
    // Request/response is now durably tracked by the workflow engine
    return { status: 200, body: request };
  }
}
`;

    case 'update-handler':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type} — now has durable update processing

export class ${name}Workflow extends Workflow {
  async execute(...args: any[]): Promise<any> {
    // Update handler is now durable — updates survive crashes
    const result = await this.executeActivity('${name}Activity', args);
    return result;
  }
}

export class ${name}Activity extends Activity {
  async execute(args: any[]): Promise<any> {
    // TODO: Move update handling logic here
    // Updates are now processed durably with automatic retry
    return args;
  }
}
`;

    case 'continue-as-new':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type} — now has durable workflow continuation

export class ${name}Workflow extends Workflow {
  async execute(...args: any[]): Promise<any> {
    // Continue-as-new is now handled durably by the Velocity engine
    // State is preserved across workflow continuations
    const result = await this.executeActivity('${name}Activity', args);
    return result;
  }
}

export class ${name}Activity extends Activity {
  async execute(args: any[]): Promise<any> {
    // TODO: Move logic from original code
    // Workflow continuation state is automatically persisted
    return args;
  }
}
`;

    case 'idempotency':
      return `import { Workflow, Activity, Worker, Client } from '@velocity-workflow/server';

// Auto-implemented from: ${pattern.description}
// Original pattern: ${pattern.type} — now has built-in idempotency

export class ${name}Workflow extends Workflow {
  async execute(...args: any[]): Promise<any> {
    // Idempotency is now built into the Velocity engine
    // Duplicate requests are automatically deduplicated
    const result = await this.executeActivity('${name}Activity', args);
    return result;
  }
}

export class ${name}Activity extends Activity {
  async execute(args: any[]): Promise<any> {
    // TODO: Move logic from original code
    // No manual idempotency key management needed
    return args;
  }
}
`;

    default:
      return `// Auto-implemented workflow for pattern: ${pattern.type}\n// TODO: Implement\n`;
  }
}

function generateBinaryWorkflow(pattern: DetectedPattern): string {
  const name = toPascalCase(pattern.entities[0] || 'AutoGenerated');
  return `import { VirtualObject, Service, RuntimeServer } from '@velocity-workflow/binary';

// Auto-implemented from: ${pattern.description}

const ${name} = new VirtualObject('${name}');

${name}.addHandler('execute', async (ctx, ...args) => {
  // TODO: Move logic from original ${pattern.type}
  const result = await ctx.invoke('${name}Handler', 'process', ...args);
  return result;
});

${name}.addHandler('process', async (ctx, input) => {
  // TODO: Implement processing logic
  return input;
});
`;
}

function generateEmbeddedWorkflow(pattern: DetectedPattern): string {
  const name = toPascalCase(pattern.entities[0] || 'AutoGenerated');
  return `import { Durable, DurableContext, VelocityEmbedded } from '@velocity-workflow/embedded';

// Auto-implemented from: ${pattern.description}

@Durable()
export class ${name} {
  async execute(ctx: DurableContext, ...args: any[]): Promise<any> {
    // TODO: Move logic from original ${pattern.type}
    const result = await ctx.invoke('${name}Activity', 'process', ...args);
    return result;
  }
}
`;
}

// ─── Helper Functions ────────────────────────────────────────────────────────

function extractBraceBlock(code: string, startIndex: number): string | null {
  let depth = 0;
  let i = startIndex;
  while (i < code.length && code[i] !== '{') i++;
  if (i >= code.length) return null;
  const blockStart = i;
  i++;
  depth = 1;
  while (i < code.length && depth > 0) {
    const ch = code[i];
    if (ch === "'" || ch === '"' || ch === '`') {
      i++;
      while (i < code.length && code[i] !== ch) { if (code[i] === '\\') i++; i++; }
      i++;
      continue;
    }
    if (ch === '{') depth++;
    else if (ch === '}') { depth--; if (depth === 0) return code.slice(blockStart + 1, i); }
    i++;
  }
  return null;
}

function findEnclosingFunction(content: string, index: number): { name: string; code: string; startLine: number; endLine: number } | null {
  const before = content.slice(0, index);
  // Look backwards for function/method definition
  const fnPatterns = [
    /(?:async\s+)?function\s+(\w+)\s*\(/g,
    /(?:async\s+)?(\w+)\s*\([^)]*\)\s*(?::\s*\w+)?\s*\{/g,
    /(?:async\s+)?def\s+(\w+)\s*\(/g,
    /(?:async\s+)?func\s+(\w+)\s*\(/g,
  ];

  let bestMatch: { name: string; index: number } | null = null;
  for (const pattern of fnPatterns) {
    let match;
    while ((match = pattern.exec(before)) !== null) {
      if (!bestMatch || match.index > bestMatch.index) {
        bestMatch = { name: match[1], index: match.index };
      }
    }
  }

  if (!bestMatch) return null;

  const startLine = content.slice(0, bestMatch.index).split('\n').length;
  // Find the end of the function (approximate: next 50 lines or next function)
  const afterStart = content.slice(bestMatch.index);
  const lines = afterStart.split('\n');
  const endLine = startLine + Math.min(lines.length, 50);

  return {
    name: bestMatch.name,
    code: lines.slice(0, endLine - startLine).join('\n'),
    startLine,
    endLine,
  };
}

function hasAsyncOperations(body: string): boolean {
  const asyncIndicators = [
    /await\s/,
    /\.then\(/,
    /Promise\./,
    /fetch\(/,
    /axios\./,
    /db\./,
    /database\./,
    /query\(/,
    /execute\(/,
    /http\./,
    /grpc\./,
    /send\(/,
    /publish\(/,
  ];
  return asyncIndicators.some(p => p.test(body));
}

function toPascalCase(str: string): string {
  return str
    .replace(/[^a-zA-Z0-9]/g, ' ')
    .split(/\s+/)
    .filter(Boolean)
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join('');
}

// ─── Main Auto-Implement Function ────────────────────────────────────────────

/** Result of auto-implementing a single file. */
export interface AutoImplementFileResult {
  sourcePath: string;
  outputPath?: string;
  success: boolean;
  error?: string;
  patternsDetected?: string[];
  generatedWorkflows?: number;
}

/** Result of a full auto-implement run. */
export interface AutoImplementResult {
  filesScanned: number;
  candidatesFound: number;
  converted: number;
  failed: number;
  outputDir: string;
  results: AutoImplementFileResult[];
  durationMs: number;
}

/** Options for auto-implement. */
export interface AutoImplementOptions {
  sourceDir: string;
  outputDir: string;
  targetFlavor: SDKFlavor;
  dryRun?: boolean;
  generateTests?: boolean;
}

/**
 * Auto-implement: scan a codebase for non-durable workflow patterns
 * and convert them to durable Velocity workflows.
 */
export function autoImplement(options: AutoImplementOptions): AutoImplementResult {
  const startTime = Date.now();
  const results: AutoImplementFileResult[] = [];
  let candidatesFound = 0;
  let converted = 0;
  let failed = 0;

  // Scan the project for source files
  const scan = scanProject(options.sourceDir);

  for (const file of scan.files) {
    let content: string;
    try {
      content = fs.readFileSync(file.filePath, 'utf-8');
    } catch {
      continue;
    }

    // Run all applicable pattern scanners
    const applicableScanners = ALL_SCANNERS.filter(s =>
      s.extensions.includes(file.extension)
    );

    const allPatterns: DetectedPattern[] = [];
    for (const scanner of applicableScanners) {
      const patterns = scanner.scan(content, file.filePath);
      allPatterns.push(...patterns);
    }

    if (allPatterns.length === 0) continue;

    candidatesFound += allPatterns.length;
    const patternTypes = [...new Set(allPatterns.map(p => p.type))];

    // Check if this is a known framework that should use migration instead
    const frameworkPatterns = allPatterns.filter(p =>
      ['temporal-workflow', 'restate-service', 'dbos-transaction'].includes(p.type)
    );

    if (frameworkPatterns.length > 0) {
      // For known frameworks, recommend migration toolkit instead
      results.push({
        sourcePath: file.relativePath,
        success: true,
        patternsDetected: patternTypes,
        generatedWorkflows: 0,
      });
      continue;
    }

    // Generate durable workflows for each detected pattern
    try {
      const generatedFiles: string[] = [];

      for (const pattern of allPatterns) {
        // Skip low-confidence patterns
        if (pattern.confidence < 0.5) continue;

        const workflowCode = generateDurableWorkflow(pattern, options.targetFlavor);
        const workflowName = toPascalCase(pattern.entities[0] || 'AutoGenerated');
        const outputFileName = `${workflowName.toLowerCase()}.workflow${file.extension}`;
        const outputPath = path.join(options.outputDir, outputFileName);

        if (!options.dryRun) {
          fs.mkdirSync(path.dirname(outputPath), { recursive: true });
          fs.writeFileSync(outputPath, workflowCode, 'utf-8');
        }

        generatedFiles.push(outputFileName);
      }

      converted += generatedFiles.length;
      results.push({
        sourcePath: file.relativePath,
        outputPath: options.dryRun ? undefined : options.outputDir,
        success: true,
        patternsDetected: patternTypes,
        generatedWorkflows: generatedFiles.length,
      });
    } catch (err: any) {
      failed++;
      results.push({
        sourcePath: file.relativePath,
        success: false,
        error: err.message || String(err),
        patternsDetected: patternTypes,
      });
    }
  }

  return {
    filesScanned: scan.files.length,
    candidatesFound,
    converted,
    failed,
    outputDir: options.outputDir,
    results,
    durationMs: Date.now() - startTime,
  };
}
