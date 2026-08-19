#!/usr/bin/env node

/**
 * Velocity Migration Toolkit CLI
 *
 * Supports:
 *   - Single-file migration:  velocity-migrate file.ts --from temporal --to classic
 *   - Project migration:      velocity-migrate --project ./src --to classic
 *   - Auto-detect framework:  velocity-migrate --project ./src --from auto --to classic
 *   - Auto-implement:         velocity-migrate --auto-implement ./src --target-flavor classic
 *   - Dry run:                velocity-migrate --project ./src --to classic --dry-run
 */

import * as fs from 'fs';
import * as path from 'path';
import { migrate, getSupportedMigrations, validateMigration, SDKFlavor } from './index';
import {
  scanProject, detectFramework, detectProjectFramework, bulkMigrate,
  BulkMigrationOptions, ScanResult, DetectionResult,
} from './scanner';
import { autoImplement, AutoImplementOptions } from './auto-implement';

const args = process.argv.slice(2);

if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
  console.log(`
Velocity Migration Toolkit
==========================

Convert workflows between SDK flavors:
- temporal: Temporal SDK (direct migration source)
- classic: Temporal-compatible SDK
- runtime: Restate-compatible SDK
- embedded: DBOS-compatible SDK
- python-runtime: Python Restate-compatible SDK

Usage:
  # Single-file migration
  velocity-migrate <source-file> --from <sdk> --to <sdk> [--output <file>]

  # Project-level migration (scans entire directory)
  velocity-migrate --project <dir> --to <sdk> [--from <sdk>|auto] [--output-dir <dir>]

  # Auto-implement (convert non-durable code to durable workflows)
  velocity-migrate --auto-implement <dir> --target-flavor <sdk>

  # Detect framework in a project
  velocity-migrate --detect <dir>

Examples:
  velocity-migrate workflow.ts --from temporal --to classic
  velocity-migrate --project ./workflows --from auto --to classic --output-dir ./migrated
  velocity-migrate --project ./workflows --to runtime --dry-run
  velocity-migrate --auto-implement ./src --target-flavor classic
  velocity-migrate --detect ./my-project

Supported Migrations:
${getSupportedMigrations().map(m => `  - ${m}`).join('\n')}

Options:
  --help, -h          Show this help message
  --output, -o        Output file path (default: stdout)
  --project           Project directory to scan for migration
  --output-dir        Output directory for project migration
  --from              Source SDK (or 'auto' for auto-detection)
  --to                Target SDK
  --dry-run           Detect and validate without writing output
  --preserve-comments Preserve original comments
  --generate-tests    Generate test files for migrated code
  --validate          Validate source before migrating
  --detect            Detect framework in a directory
  --auto-implement    Convert non-durable code to durable workflows
  --target-flavor     Target Velocity flavor for auto-implement
`);
  process.exit(0);
}

// ─── Mode: Framework Detection ───────────────────────────────────────────────

if (args.includes('--detect')) {
  const detectIdx = args.indexOf('--detect');
  const targetDir = args[detectIdx + 1];
  if (!targetDir || targetDir.startsWith('--')) {
    console.error('Error: --detect requires a directory path');
    process.exit(1);
  }
  if (!fs.existsSync(targetDir)) {
    console.error(`Error: Directory not found: ${targetDir}`);
    process.exit(1);
  }

  console.log(`Scanning project: ${targetDir}`);
  const scan = scanProject(targetDir);
  console.log(`Found ${scan.files.length} workflow files (${scan.totalFilesScanned} total files scanned)`);

  const detection = detectProjectFramework(targetDir);
  console.log(`\nDetected framework: ${detection.framework}`);
  console.log(`Confidence: ${(detection.confidence * 100).toFixed(1)}%`);
  if (detection.evidence.length > 0) {
    console.log(`Evidence:`);
    detection.evidence.forEach(e => console.log(`  - ${e}`));
  }

  // Show per-extension breakdown
  const exts = Object.entries(scan.byExtension);
  if (exts.length > 0) {
    console.log(`\nFiles by extension:`);
    exts.forEach(([ext, files]) => console.log(`  ${ext}: ${files.length} files`));
  }
  process.exit(0);
}

// ─── Mode: Auto-Implement ────────────────────────────────────────────────────

if (args.includes('--auto-implement')) {
  const aiIdx = args.indexOf('--auto-implement');
  const targetDir = args[aiIdx + 1];
  const targetFlavorIdx = args.indexOf('--target-flavor');

  if (!targetDir || targetDir.startsWith('--')) {
    console.error('Error: --auto-implement requires a directory path');
    process.exit(1);
  }
  if (targetFlavorIdx === -1) {
    console.error('Error: --auto-implement requires --target-flavor <sdk>');
    process.exit(1);
  }

  const targetFlavor = args[targetFlavorIdx + 1] as SDKFlavor;
  const outputDir = args.includes('--output-dir')
    ? args[args.indexOf('--output-dir') + 1]
    : path.join(targetDir, '..', 'velocity-migrated');

  const options: AutoImplementOptions = {
    sourceDir: targetDir,
    outputDir,
    targetFlavor,
    dryRun: args.includes('--dry-run'),
    generateTests: args.includes('--generate-tests'),
  };

  console.log(`Auto-implement: scanning ${targetDir} for non-durable workflow patterns...`);
  const result = autoImplement(options);

  console.log(`\nAuto-Implement Results:`);
  console.log(`  Files scanned: ${result.filesScanned}`);
  console.log(`  Candidates found: ${result.candidatesFound}`);
  console.log(`  Converted: ${result.converted}`);
  console.log(`  Failed: ${result.failed}`);
  console.log(`  Output: ${options.dryRun ? '(dry run)' : result.outputDir}`);

  if (result.results.length > 0) {
    console.log(`\nPer-file results:`);
    for (const r of result.results) {
      const status = r.success ? '✓' : '✗';
      const patterns = r.patternsDetected?.join(', ') || '';
      console.log(`  ${status} ${r.sourcePath} [${patterns}]${r.error ? ` — ${r.error}` : ''}`);
    }
  }
  process.exit(0);
}

// ─── Mode: Project Migration ─────────────────────────────────────────────────

if (args.includes('--project')) {
  const projIdx = args.indexOf('--project');
  const projectDir = args[projIdx + 1];
  const toIndex = args.indexOf('--to');
  const fromIndex = args.indexOf('--from');

  if (!projectDir || projectDir.startsWith('--')) {
    console.error('Error: --project requires a directory path');
    process.exit(1);
  }
  if (toIndex === -1) {
    console.error('Error: --project requires --to <sdk>');
    process.exit(1);
  }

  const from = fromIndex !== -1 ? args[fromIndex + 1] : 'auto';
  const to = args[toIndex + 1] as SDKFlavor;
  const outputDir = args.includes('--output-dir')
    ? args[args.indexOf('--output-dir') + 1]
    : path.join(projectDir, '..', 'velocity-migrated');

  const options: BulkMigrationOptions = {
    sourceDir: projectDir,
    outputDir,
    source: from as SDKFlavor | 'auto',
    target: to,
    preserveComments: args.includes('--preserve-comments'),
    generateTests: args.includes('--generate-tests'),
    dryRun: args.includes('--dry-run'),
  };

  console.log(`Scanning project: ${projectDir}`);
  console.log(`Migration: ${from} → ${to}`);
  console.log(`Output: ${options.dryRun ? '(dry run)' : outputDir}`);
  console.log('');

  const result = bulkMigrate(options);

  console.log(`Migration Results:`);
  console.log(`  Source framework: ${result.sourceFramework}`);
  console.log(`  Target framework: ${result.targetFramework}`);
  console.log(`  Total files: ${result.totalFiles}`);
  console.log(`  Migrated: ${result.migrated}`);
  console.log(`  Failed: ${result.failed}`);
  console.log(`  Skipped: ${result.skipped}`);
  console.log(`  Duration: ${result.durationMs}ms`);

  if (result.results.some(r => !r.success)) {
    console.log(`\nFailed files:`);
    result.results.filter(r => !r.success).forEach(r => {
      console.log(`  ✗ ${r.sourcePath}: ${r.error}`);
    });
  }

  if (result.results.some(r => r.success && r.transformationsApplied)) {
    console.log(`\nSuccessful migrations:`);
    result.results.filter(r => r.success && r.transformationsApplied).forEach(r => {
      console.log(`  ✓ ${r.sourcePath} (${r.transformationsApplied} changes) [${r.detectedFramework}]`);
    });
  }

  process.exit(result.failed > 0 ? 1 : 0);
}

// ─── Mode: Single-File Migration (original) ──────────────────────────────────

const sourceFile = args[0];
const fromIndex = args.indexOf('--from');
const toIndex = args.indexOf('--to');
const outputIndex = args.indexOf('--output') !== -1 ? args.indexOf('--output') : args.indexOf('-o');

if (fromIndex === -1 || toIndex === -1) {
  console.error('Error: --from and --to are required');
  process.exit(1);
}

const from = args[fromIndex + 1] as SDKFlavor;
const to = args[toIndex + 1] as SDKFlavor;
const outputFile = outputIndex !== -1 ? args[outputIndex + 1] : null;
const preserveComments = args.includes('--preserve-comments');
const generateTests = args.includes('--generate-tests');
const validate = args.includes('--validate');

// Read source file
if (!fs.existsSync(sourceFile)) {
  console.error(`Error: Source file not found: ${sourceFile}`);
  process.exit(1);
}

const sourceCode = fs.readFileSync(sourceFile, 'utf-8');

// Validate source if requested
if (validate) {
  const validation = validateMigration(sourceCode, from);
  if (!validation.valid) {
    console.error('Source validation failed:');
    validation.errors.forEach(e => console.error(`  - ${e}`));
    process.exit(1);
  }
  console.log('✓ Source validation passed');
}

// Perform migration
try {
  let migratedCode = migrate(sourceCode, {
    source: from,
    target: to,
    preserveComments,
    generateTests,
  });

  if (outputFile) {
    fs.writeFileSync(outputFile, migratedCode, 'utf-8');
    console.log(`✓ Migrated code written to: ${outputFile}`);

    // Generate test file if requested
    if (generateTests) {
      const testFile = outputFile.replace(/\.\w+$/, '.test.ts');
      const testContent = generateTestFile(outputFile, to);
      fs.writeFileSync(testFile, testContent, 'utf-8');
      console.log(`✓ Test file written to: ${testFile}`);
    }
  } else {
    console.log(migratedCode);
  }

  console.log(`\n✓ Successfully migrated from ${from} to ${to}`);
} catch (error: any) {
  console.error('Migration failed:', error.message || error);
  process.exit(1);
}

function generateTestFile(filePath: string, flavor: SDKFlavor): string {
  const moduleName = path.basename(filePath, path.extname(filePath));
  return `import { ${flavor === 'server' ? 'Workflow, Activity' : flavor === 'binary' ? 'VirtualObject, Service' : 'Durable, DurableContext'} } from '@velocity-workflow/${flavor === 'python-runtime' ? 'binary' : flavor}';

describe('${moduleName} (migrated)', () => {
  test('should be defined', () => {
    // TODO: Add tests for migrated ${flavor} code
    expect(true).toBe(true);
  });
});
`;
}
