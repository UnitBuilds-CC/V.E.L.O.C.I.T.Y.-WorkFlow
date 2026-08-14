/**
 * Velocity Classic — Main Entry Point
 * 
 * Starts the worker and HTTP server for benchmarking.
 */

import { Worker, Workflow, Activity } from './index';
import { VelocityServer } from './server';

// ─── Benchmark Workflow ──────────────────────────────────────────────────────

class BenchmarkWorkflow extends Workflow {
  static typeName = 'benchmarkWorkflow';

  async execute(input: string): Promise<{ result: string; steps: string[] }> {
    const steps: string[] = [];

    // Step 1: Process
    const processed = await this.executeActivity<string>('processActivity', input);
    steps.push('processed');

    // Step 2: Validate
    const validated = await this.executeActivity<string>('validateActivity', processed);
    steps.push('validated');

    // Step 3: Finalize
    const finalized = await this.executeActivity<string>('finalizeActivity', validated);
    steps.push('finalized');

    return { result: finalized, steps };
  }
}

// ─── Activities ──────────────────────────────────────────────────────────────

class ProcessActivity extends Activity {
  static typeName = 'processActivity';

  async execute(input: string): Promise<string> {
    // Simulate some processing
    return `processed-${input}-${Date.now()}`;
  }
}

class ValidateActivity extends Activity {
  static typeName = 'validateActivity';

  async execute(input: string): Promise<string> {
    // Simulate validation
    return `validated-${input}`;
  }
}

class FinalizeActivity extends Activity {
  static typeName = 'finalizeActivity';

  async execute(input: string): Promise<string> {
    // Simulate finalization
    return `finalized-${input}`;
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log('[Velocity Classic] Starting...');

  // Create worker
  const worker = await Worker.create({
    taskQueue: 'benchmark',
    logLevel: 'info',
    maxConcurrentWorkflows: 100,
    maxConcurrentActivities: 200,
  });

  // Register workflows and activities
  worker.registerWorkflow(BenchmarkWorkflow);
  worker.registerActivity(ProcessActivity);
  worker.registerActivity(ValidateActivity);
  worker.registerActivity(FinalizeActivity);

  console.log('[Velocity Classic] Worker created');
  console.log(`[Velocity Classic] Workflows: ${worker.workflowTypes.join(', ')}`);
  console.log(`[Velocity Classic] Activities: ${worker.activityTypes.join(', ')}`);

  // Start worker
  await worker.run();
  console.log('[Velocity Classic] Worker running');

  // Create and start HTTP server
  const server = new VelocityServer({
    port: 8083,
    host: '0.0.0.0',
    worker,
  });

  await server.start();
  console.log(`[Velocity Classic] HTTP server listening on ${server.host}:${server.port}`);
}

main().catch((err) => {
  console.error('[Velocity Classic] Fatal error:', err);
  process.exit(1);
});
