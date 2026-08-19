/**
 * Directory scanner, framework auto-detection, and bulk conversion engine.
 *
 * Scans entire projects for workflow files, detects the source framework
 * (Temporal, Restate, DBOS, or Velocity flavor), and performs bulk migration
 * with dependency resolution across files.
 */

import * as fs from 'fs';
import * as path from 'path';
import { SDKFlavor, migrate, validateMigration } from './index';

// ─── File Discovery ──────────────────────────────────────────────────────────

/** Supported source file extensions for migration. */
const WORKFLOW_EXTENSIONS = new Set([
  '.ts', '.tsx', '.js', '.jsx',
  '.py',
  '.go',
  '.java',
  '.rs',
  '.php',
  '.rb',
  '.cs',
]);

/** Directories to skip during scanning. */
const SKIP_DIRS = new Set([
  'node_modules', '.git', '.svn', '.hg', 'dist', 'build', 'target',
  'bin', 'obj', '.next', '.nuxt', '__pycache__', '.venv', 'venv',
  'vendor', '.cargo', '.gradle', '.maven',
]);

/** A file discovered during scanning. */
export interface DiscoveredFile {
  /** Absolute path to the file. */
  filePath: string;
  /** Relative path from the project root. */
  relativePath: string;
  /** File extension (e.g. '.ts'). */
  extension: string;
  /** File size in bytes. */
  sizeBytes: number;
}

/** Result of scanning a project directory. */
export interface ScanResult {
  /** Project root directory. */
  rootDir: string;
  /** All discovered workflow-related files. */
  files: DiscoveredFile[];
  /** Files grouped by extension. */
  byExtension: Record<string, DiscoveredFile[]>;
  /** Total number of files scanned (including non-workflow). */
  totalFilesScanned: number;
}

/**
 * Recursively scan a directory for workflow-related source files.
 * Skips node_modules, .git, build artifacts, etc.
 */
export function scanProject(rootDir: string): ScanResult {
  const files: DiscoveredFile[] = [];
  let totalFilesScanned = 0;

  function walk(dir: string): void {
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return; // Permission denied or inaccessible directory
    }

    for (const entry of entries) {
      const name = entry.name;
      const fullPath = path.join(dir, name);

      if (entry.isDirectory()) {
        if (SKIP_DIRS.has(name) || name.startsWith('.')) continue;
        walk(fullPath);
        continue;
      }

      if (!entry.isFile()) continue;
      totalFilesScanned++;

      const ext = path.extname(name).toLowerCase();
      if (!WORKFLOW_EXTENSIONS.has(ext)) continue;

      // Quick content check: does the file contain workflow-like patterns?
      let content: string;
      try {
        content = fs.readFileSync(fullPath, 'utf-8');
      } catch {
        continue;
      }

      if (!looksLikeWorkflow(content, ext)) continue;

      const stat = fs.statSync(fullPath);
      files.push({
        filePath: fullPath,
        relativePath: path.relative(rootDir, fullPath),
        extension: ext,
        sizeBytes: stat.size,
      });
    }
  }

  walk(rootDir);

  const byExtension: Record<string, DiscoveredFile[]> = {};
  for (const f of files) {
    if (!byExtension[f.extension]) byExtension[f.extension] = [];
    byExtension[f.extension].push(f);
  }

  return { rootDir, files, byExtension, totalFilesScanned };
}

// ─── Framework Auto-Detection ────────────────────────────────────────────────

/** Confidence score for framework detection. */
export interface DetectionResult {
  /** Detected framework. */
  framework: SDKFlavor;
  /** Confidence 0.0–1.0. */
  confidence: number;
  /** Evidence strings that led to this detection. */
  evidence: string[];
}

/** Import patterns that identify each framework. */
const FRAMEWORK_SIGNATURES: Record<string, { patterns: RegExp[]; flavor: SDKFlavor }> = {
  'temporal': {
    flavor: 'temporal',
    patterns: [
      /from\s+['"]@temporalio\//,
      /from\s+['"]temporalio/,
      /import\s+.*['"]go\.temporal\.io/,
      /using\s+Temporal\.Sdk/,
      /from\s+temporalio\s+import/,
      /Temporal::new/,
      /workflow\.Context/,
      /workflow\.ExecuteActivity/,
      /workflow\.searchAttributes\(/,
      /workflow\.getMemo\(/,
      /@workflow\.update/,
      /workflow\.continueAsNew\(/,
    ],
  },
  'restate': {
    flavor: 'binary',
    patterns: [
      /from\s+['"]@restatedev\//,
      /import\s+restate/,
      /from\s+restate\s+import/,
      /restate\.service/,
      /restate\.handler/,
      /ctx\.run\(/,
      /ctx\.invoke\(/,
      /\[restate::service\]/,
      /restate_sdk/,
      /ctx\.idempotencyKey/,
      /restate\.ServiceClient/,
    ],
  },
  'dbos': {
    flavor: 'embedded',
    patterns: [
      /from\s+['"]@dbos-inc\//,
      /from\s+dbos\s+import/,
      /import\s+dbos/,
      /@DBOS\.(workflow|transaction)/,
      /@dbos\.(workflow|transaction)/,
      /DBOS\.sleep/,
      /DBOS\.recv/,
      /dbos\.transaction/,
      /DBOS\.enqueue\(/,
      /DBOS\.dequeue\(/,
      /@DBOS\.httpHandler/,
    ],
  },
  'velocity-server': {
    flavor: 'server',
    patterns: [
      /from\s+['"]@velocity-workflow\/server/,
      /from\s+['"]velocity_sdk.*server/,
      /extends\s+Workflow/,
      /this\.executeActivity\(/,
      /this\.waitForSignal\(/,
    ],
  },
  'velocity-binary': {
    flavor: 'binary',
    patterns: [
      /from\s+['"]@velocity-workflow\/binary/,
      /from\s+['"]velocity_sdk.*binary/,
      /VirtualObject/,
      /ctx\.sleep\(/,
    ],
  },
  'velocity-embedded': {
    flavor: 'embedded',
    patterns: [
      /from\s+['"]@velocity-workflow\/embedded/,
      /from\s+['"]velocity_sdk.*embedded/,
      /@Durable/,
      /WorkflowHelpers\./,
    ],
  },
};

/**
 * Auto-detect the source framework by scanning imports and patterns in code.
 * Can analyze a single file or an entire project directory.
 */
export function detectFramework(source: string, extension?: string): DetectionResult {
  const scores: Record<string, { score: number; evidence: string[] }> = {};

  for (const [name, config] of Object.entries(FRAMEWORK_SIGNATURES)) {
    scores[name] = { score: 0, evidence: [] };
    for (const pattern of config.patterns) {
      const matches = source.match(new RegExp(pattern, 'gm'));
      if (matches) {
        scores[name].score += matches.length;
        scores[name].evidence.push(...matches.slice(0, 3).map(m => m.trim()));
      }
    }
  }

  // Find the best match
  let best = '';
  let bestScore = 0;
  for (const [name, data] of Object.entries(scores)) {
    if (data.score > bestScore) {
      best = name;
      bestScore = data.score;
    }
  }

  if (bestScore === 0) {
    return { framework: 'server', confidence: 0, evidence: ['No framework signatures detected'] };
  }

  const totalScore = Object.values(scores).reduce((s, d) => s + d.score, 0);
  const confidence = Math.min(1.0, bestScore / Math.max(totalScore, 1));

  return {
    framework: FRAMEWORK_SIGNATURES[best].flavor,
    confidence,
    evidence: scores[best].evidence.slice(0, 5),
  };
}

/**
 * Detect framework for an entire project by scanning all source files.
 * Returns the aggregate detection result.
 */
export function detectProjectFramework(rootDir: string): DetectionResult {
  const scan = scanProject(rootDir);
  const allEvidence: string[] = [];
  const scores: Record<string, number> = {};

  for (const file of scan.files) {
    try {
      const content = fs.readFileSync(file.filePath, 'utf-8');
      const result = detectFramework(content, file.extension);
      if (result.confidence > 0) {
        scores[result.framework] = (scores[result.framework] || 0) + result.confidence;
        allEvidence.push(...result.evidence.map(e => `${file.relativePath}: ${e}`));
      }
    } catch {
      continue;
    }
  }

  let best: SDKFlavor = 'server';
  let bestScore = 0;
  for (const [flavor, score] of Object.entries(scores)) {
    if (score > bestScore) {
      best = flavor as SDKFlavor;
      bestScore = score;
    }
  }

  const totalScore = Object.values(scores).reduce((s, n) => s + n, 0);
  return {
    framework: best,
    confidence: totalScore > 0 ? bestScore / totalScore : 0,
    evidence: allEvidence.slice(0, 10),
  };
}

// ─── Bulk Migration ──────────────────────────────────────────────────────────

/** Result of migrating a single file. */
export interface FileMigrationResult {
  /** Source file path. */
  sourcePath: string;
  /** Output file path (if written). */
  outputPath?: string;
  /** Whether migration succeeded. */
  success: boolean;
  /** Error message if failed. */
  error?: string;
  /** Detected framework for this file. */
  detectedFramework?: SDKFlavor;
  /** Number of transformations applied. */
  transformationsApplied?: number;
}

/** Result of a bulk project migration. */
export interface BulkMigrationResult {
  /** Total files discovered. */
  totalFiles: number;
  /** Files successfully migrated. */
  migrated: number;
  /** Files that failed migration. */
  failed: number;
  /** Files skipped (no workflow content). */
  skipped: number;
  /** Per-file results. */
  results: FileMigrationResult[];
  /** Source framework detected. */
  sourceFramework: SDKFlavor;
  /** Target framework. */
  targetFramework: SDKFlavor;
  /** Duration in milliseconds. */
  durationMs: number;
}

/** Options for bulk migration. */
export interface BulkMigrationOptions {
  /** Source directory to scan. */
  sourceDir: string;
  /** Output directory for migrated files. */
  outputDir: string;
  /** Source framework (or 'auto' for auto-detection). */
  source: SDKFlavor | 'auto';
  /** Target framework. */
  target: SDKFlavor;
  /** File extensions to include (default: all workflow extensions). */
  extensions?: string[];
  /** Whether to preserve comments. */
  preserveComments?: boolean;
  /** Whether to generate test files. */
  generateTests?: boolean;
  /** Dry run — detect and validate but don't write output. */
  dryRun?: boolean;
}

/**
 * Perform bulk migration of an entire project directory.
 * Auto-detects framework if source is 'auto', scans for workflow files,
 * and migrates each one to the target flavor.
 */
export function bulkMigrate(options: BulkMigrationOptions): BulkMigrationResult {
  const startTime = Date.now();
  const results: FileMigrationResult[] = [];

  // Scan the project
  const scan = scanProject(options.sourceDir);

  // Detect framework if auto
  let sourceFlavor: SDKFlavor;
  if (options.source === 'auto') {
    const detection = detectProjectFramework(options.sourceDir);
    sourceFlavor = detection.framework;
    if (detection.confidence < 0.3) {
      console.warn(`Warning: Low confidence (${detection.confidence.toFixed(2)}) in framework detection`);
    }
  } else {
    sourceFlavor = options.source;
  }

  // Migrate each discovered file
  let migrated = 0;
  let failed = 0;
  let skipped = 0;

  for (const file of scan.files) {
    try {
      const content = fs.readFileSync(file.filePath, 'utf-8');

      // Per-file auto-detection override
      let fileFlavor = sourceFlavor;
      if (options.source === 'auto') {
        const fileDetection = detectFramework(content, file.extension);
        if (fileDetection.confidence > 0.5) {
          fileFlavor = fileDetection.framework;
        }
      }

      // Skip if source == target
      if (fileFlavor === options.target) {
        skipped++;
        results.push({ sourcePath: file.relativePath, success: true, detectedFramework: fileFlavor });
        continue;
      }

      // Perform migration
      const migratedCode = migrate(content, {
        source: fileFlavor,
        target: options.target,
        preserveComments: options.preserveComments,
        generateTests: options.generateTests,
      });

      if (!options.dryRun) {
        // Compute output path
        const relPath = path.relative(options.sourceDir, file.filePath);
        const outputPath = path.join(options.outputDir, relPath);

        // Ensure output directory exists
        fs.mkdirSync(path.dirname(outputPath), { recursive: true });
        fs.writeFileSync(outputPath, migratedCode, 'utf-8');
      }

      migrated++;
      results.push({
        sourcePath: file.relativePath,
        outputPath: options.dryRun ? undefined : path.join(options.outputDir, file.relativePath),
        success: true,
        detectedFramework: fileFlavor,
        transformationsApplied: countTransformations(content, migratedCode),
      });
    } catch (err: any) {
      failed++;
      results.push({
        sourcePath: file.relativePath,
        success: false,
        error: err.message || String(err),
        detectedFramework: sourceFlavor,
      });
    }
  }

  return {
    totalFiles: scan.files.length,
    migrated,
    failed,
    skipped,
    results,
    sourceFramework: sourceFlavor,
    targetFramework: options.target,
    durationMs: Date.now() - startTime,
  };
}

// ─── Internal Helpers ────────────────────────────────────────────────────────

/**
 * Quick heuristic check: does this file contain workflow-like patterns?
 */
function looksLikeWorkflow(content: string, ext: string): boolean {
  const indicators = [
    // Temporal
    /@temporalio/, /temporalio/, /workflow\.Context/, /WorkflowMethod/,
    /ExecuteActivity/, /SignalMethod/, /QueryMethod/,
    // Restate
    /@restatedev/, /restate/, /ctx\.run\(/, /ctx\.invoke\(/,
    // DBOS
    /@DBOS/, /dbos\.(workflow|transaction)/, /DBOS\.(sleep|recv)/,
    // Velocity
    /@velocity-workflow/, /velocity_sdk/, /WorkflowHelpers/,
    /executeActivity/, /waitForSignal/, /VirtualObject/,
    // Generic patterns
    /class\s+\w+Workflow/, /class\s+\w+Activity/, /class\s+\w+Service/,
    /def\s+\w+_workflow/, /def\s+\w+_activity/,
    /fn\s+\w+_workflow/, /fn\s+\w+_activity/,
    /func\s+\w+Workflow/, /func\s+\w+Activity/,
    /@durable/i, /@workflow/i, /@activity/i,
  ];

  let matchCount = 0;
  for (const pattern of indicators) {
    if (pattern.test(content)) matchCount++;
  }

  return matchCount >= 2;
}

/**
 * Count the number of line-level transformations between source and output.
 */
function countTransformations(source: string, output: string): number {
  const srcLines = source.split('\n');
  const outLines = output.split('\n');
  let changes = 0;
  const maxLen = Math.max(srcLines.length, outLines.length);
  for (let i = 0; i < maxLen; i++) {
    if (srcLines[i] !== outLines[i]) changes++;
  }
  return changes;
}
