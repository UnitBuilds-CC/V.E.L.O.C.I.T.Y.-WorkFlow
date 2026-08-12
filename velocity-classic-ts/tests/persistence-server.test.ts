import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  Workflow, Activity, Worker, Client, WorkflowStatus,
  FileJournalBackend, VelocityServer,
} from '../src/index';

// ─── Test Helpers ────────────────────────────────────────────────────────────

class SimpleWorkflow extends Workflow {
  async execute(msg: string): Promise<any> {
    return { result: msg };
  }
}

function tmpDir(): string {
  return path.join(os.tmpdir(), `velocity-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
}

// ─── File Journal Persistence ────────────────────────────────────────────────

describe('File Journal Persistence', () => {
  let journalDir: string;

  beforeEach(() => {
    journalDir = tmpDir();
  });

  afterEach(() => {
    if (fs.existsSync(journalDir)) {
      fs.rmSync(journalDir, { recursive: true, force: true });
    }
  });

  test('create and retrieve journal', () => {
    const backend = new FileJournalBackend({ journalDir });
    const journal = backend.createJournal('wf-1', 'TestWorkflow');
    expect(journal.workflowId).toBe('wf-1');
    expect(journal.workflowType).toBe('TestWorkflow');
    expect(journal.status).toBe(WorkflowStatus.RUNNING);

    const retrieved = backend.getJournal('wf-1');
    expect(retrieved).toBeDefined();
    expect(retrieved!.workflowId).toBe('wf-1');
    backend.close();
  });

  test('append events to journal', () => {
    const backend = new FileJournalBackend({ journalDir });
    backend.createJournal('wf-2', 'TestWorkflow');
    backend.appendEvent('wf-2', 'activity_started', { activity: 'charge' });
    backend.appendEvent('wf-2', 'activity_completed', { activity: 'charge', result: 'ok' });

    const journal = backend.getJournal('wf-2');
    expect(journal!.events.length).toBe(2);
    expect(journal!.events[0].eventType).toBe('activity_started');
    expect(journal!.events[1].eventType).toBe('activity_completed');
    backend.close();
  });

  test('update status', () => {
    const backend = new FileJournalBackend({ journalDir });
    backend.createJournal('wf-3', 'TestWorkflow');
    backend.updateStatus('wf-3', WorkflowStatus.COMPLETED);

    const journal = backend.getJournal('wf-3');
    expect(journal!.status).toBe(WorkflowStatus.COMPLETED);
    backend.close();
  });

  test('flush writes to disk', () => {
    const backend = new FileJournalBackend({ journalDir });
    backend.createJournal('wf-4', 'TestWorkflow');
    backend.appendEvent('wf-4', 'started', {});
    backend.flush();

    const files = fs.readdirSync(journalDir);
    expect(files.length).toBe(1);
    expect(files[0]).toContain('wf-4');

    const content = JSON.parse(fs.readFileSync(path.join(journalDir, files[0]), 'utf-8'));
    expect(content.workflowId).toBe('wf-4');
    expect(content.events.length).toBe(1);
    backend.close();
  });

  test('load from disk recovers journals', () => {
    // Write a journal
    const backend1 = new FileJournalBackend({ journalDir });
    backend1.createJournal('wf-5', 'TestWorkflow');
    backend1.appendEvent('wf-5', 'started', {});
    backend1.flush();
    backend1.close();

    // Load from disk in a new backend
    const backend2 = new FileJournalBackend({ journalDir });
    const loaded = backend2.loadFromDisk();
    expect(loaded.length).toBe(1);
    expect(loaded[0].workflowId).toBe('wf-5');
    expect(loaded[0].events.length).toBe(1);
    backend2.close();
  });

  test('get incomplete workflows', () => {
    const backend = new FileJournalBackend({ journalDir });
    backend.createJournal('wf-running', 'TestWorkflow');
    backend.createJournal('wf-completed', 'TestWorkflow');
    backend.updateStatus('wf-completed', WorkflowStatus.COMPLETED);

    const incomplete = backend.getIncompleteWorkflows();
    expect(incomplete.length).toBe(1);
    expect(incomplete[0].workflowId).toBe('wf-running');
    backend.close();
  });

  test('list journals with filter', () => {
    const backend = new FileJournalBackend({ journalDir });
    backend.createJournal('wf-a', 'TestWorkflow');
    backend.createJournal('wf-b', 'TestWorkflow');
    backend.updateStatus('wf-b', WorkflowStatus.COMPLETED);

    const running = backend.listJournals({ status: WorkflowStatus.RUNNING });
    expect(running.length).toBe(1);
    expect(running[0].workflowId).toBe('wf-a');

    const completed = backend.listJournals({ status: WorkflowStatus.COMPLETED });
    expect(completed.length).toBe(1);
    expect(completed[0].workflowId).toBe('wf-b');
    backend.close();
  });

  test('delete journal removes from disk', () => {
    const backend = new FileJournalBackend({ journalDir });
    backend.createJournal('wf-del', 'TestWorkflow');
    backend.flush();
    expect(fs.readdirSync(journalDir).length).toBe(1);

    backend.deleteJournal('wf-del');
    expect(fs.readdirSync(journalDir).length).toBe(0);
    backend.close();
  });
});

// ─── HTTP Server ─────────────────────────────────────────────────────────────

describe('Velocity HTTP Server', () => {
  let worker: Worker;
  let server: VelocityServer;
  let port: number;

  beforeAll(async () => {
    worker = await Worker.create({ taskQueue: 'test' });
    worker.registerWorkflow(SimpleWorkflow);
    await worker.run();

    port = 19876 + Math.floor(Math.random() * 100);
    server = new VelocityServer({ port, worker });
    await server.start();
  });

  afterAll(async () => {
    await server.stop();
    await worker.shutdown();
  });

  test('health check returns healthy', async () => {
    const res = await fetch(`http://localhost:${port}/api/health`);
    const data: any = await res.json();
    expect(data.success).toBe(true);
    expect(data.data.status).toBe('healthy');
  });

  test('start workflow via HTTP', async () => {
    const res = await fetch(`http://localhost:${port}/api/workflows`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        workflow_id: 'http-wf-1',
        workflow_type: 'SimpleWorkflow',
        input: ['hello'],
      }),
    });
    const data: any = await res.json();
    expect(data.success).toBe(true);
    expect(data.data.workflowId).toBe('http-wf-1');
  });

  test('list workflows via HTTP', async () => {
    const res = await fetch(`http://localhost:${port}/api/workflows`);
    const data: any = await res.json();
    expect(data.success).toBe(true);
    expect(Array.isArray(data.data)).toBe(true);
  });

  test('describe workflow via HTTP', async () => {
    // Start a workflow first
    await fetch(`http://localhost:${port}/api/workflows`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        workflow_id: 'http-wf-2',
        workflow_type: 'SimpleWorkflow',
        input: ['test'],
      }),
    });
    await new Promise(r => setTimeout(r, 50));

    const res = await fetch(`http://localhost:${port}/api/workflows/http-wf-2`);
    const data: any = await res.json();
    expect(data.success).toBe(true);
    expect(data.data.workflowId).toBe('http-wf-2');
  });

  test('terminate workflow via HTTP', async () => {
    await fetch(`http://localhost:${port}/api/workflows`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        workflow_id: 'http-wf-3',
        workflow_type: 'SimpleWorkflow',
        input: ['test'],
      }),
    });
    await new Promise(r => setTimeout(r, 50));

    const res = await fetch(`http://localhost:${port}/api/workflows/http-wf-3/terminate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ reason: 'testing' }),
    });
    const data: any = await res.json();
    expect(data.success).toBe(true);
  });

  test('404 for unknown routes', async () => {
    const res = await fetch(`http://localhost:${port}/api/unknown`);
    const data: any = await res.json();
    expect(data.success).toBe(false);
    expect(data.error).toContain('Not found');
  });
});
