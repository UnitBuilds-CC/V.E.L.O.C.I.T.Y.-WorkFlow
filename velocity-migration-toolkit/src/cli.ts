#!/usr/bin/env node

import * as fs from 'fs';
import * as path from 'path';
import { migrate, getSupportedMigrations, validateMigration, SDKFlavor } from './index';

const args = process.argv.slice(2);

if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
  console.log(`
Velocity Migration Toolkit
==========================

Convert workflows between SDK flavors:
- classic: Temporal-compatible SDK
- runtime: Restate-compatible SDK
- embedded: DBOS-compatible SDK
- python-runtime: Python Restate-compatible SDK

Usage:
  velocity-migrate <source-file> --from <source-sdk> --to <target-sdk> [--output <output-file>]

Examples:
  velocity-migrate workflow.ts --from classic --to runtime
  velocity-migrate workflow.ts --from runtime --to embedded --output migrated.ts
  velocity-migrate workflow.py --from python-runtime --to classic

Supported Migrations:
${getSupportedMigrations().map(m => `  - ${m}`).join('\n')}

Options:
  --help, -h          Show this help message
  --output, -o        Output file path (default: stdout)
  --preserve-comments Preserve original comments
  --generate-tests    Generate test files for migrated code
  --validate          Validate source before migrating
`);
  process.exit(0);
}

// Parse arguments
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
  return `import { ${flavor === 'classic' ? 'Workflow, Activity' : flavor === 'runtime' ? 'VirtualObject, Service' : 'Durable, DurableContext'} } from '@velocity-workflow/${flavor === 'python-runtime' ? 'runtime' : flavor}';

describe('${moduleName} (migrated)', () => {
  test('should be defined', () => {
    // TODO: Add tests for migrated ${flavor} code
    expect(true).toBe(true);
  });
});
`;
}
