/**
 * File-based persistence layer for Velocity Classic SDK.
 * 
 * Writes workflow journals to disk for crash recovery.
 * Each workflow execution gets a journal file that records all events.
 */

import * as fs from 'fs';
import * as path from 'path';
import { WorkflowStatus } from './index';

// ─── Types ───────────────────────────────────────────────────────────────────

export interface JournalEvent {
  sequenceNumber: number;
  timestamp: number;
  eventType: string;
  data: any;
}

export interface WorkflowJournal {
  workflowId: string;
  workflowType: string;
  status: WorkflowStatus;
  events: JournalEvent[];
  createdAt: number;
  updatedAt: number;
}

export interface PersistenceConfig {
  journalDir: string;
  flushIntervalMs?: number;
  maxJournalSizeMb?: number;
}

// ─── File Journal Backend ────────────────────────────────────────────────────

export class FileJournalBackend {
  private _journalDir: string;
  private _journals = new Map<string, WorkflowJournal>();
  private _dirty = new Set<string>();
  private _flushTimer: ReturnType<typeof setInterval> | null = null;
  private _maxJournalSizeMb: number;

  constructor(config: PersistenceConfig) {
    this._journalDir = config.journalDir;
    this._maxJournalSizeMb = config.maxJournalSizeMb ?? 10;
    
    // Ensure journal directory exists
    if (!fs.existsSync(this._journalDir)) {
      fs.mkdirSync(this._journalDir, { recursive: true });
    }

    // Start periodic flush
    const flushInterval = config.flushIntervalMs ?? 1000;
    this._flushTimer = setInterval(() => this.flush(), flushInterval);
  }

  // ─── Journal Operations ──────────────────────────────────────────────────

  createJournal(workflowId: string, workflowType: string): WorkflowJournal {
    const journal: WorkflowJournal = {
      workflowId,
      workflowType,
      status: WorkflowStatus.RUNNING,
      events: [],
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
    this._journals.set(workflowId, journal);
    this._dirty.add(workflowId);
    return journal;
  }

  getJournal(workflowId: string): WorkflowJournal | undefined {
    return this._journals.get(workflowId);
  }

  appendEvent(workflowId: string, eventType: string, data: any): void {
    const journal = this._journals.get(workflowId);
    if (!journal) throw new Error(`Journal not found: ${workflowId}`);
    
    journal.events.push({
      sequenceNumber: journal.events.length + 1,
      timestamp: Date.now(),
      eventType,
      data,
    });
    journal.updatedAt = Date.now();
    this._dirty.add(workflowId);
  }

  updateStatus(workflowId: string, status: WorkflowStatus): void {
    const journal = this._journals.get(workflowId);
    if (!journal) throw new Error(`Journal not found: ${workflowId}`);
    
    journal.status = status;
    journal.updatedAt = Date.now();
    this.appendEvent(workflowId, 'status_changed', { status });
  }

  listJournals(filter?: { status?: WorkflowStatus }): WorkflowJournal[] {
    let journals = Array.from(this._journals.values());
    if (filter?.status) {
      journals = journals.filter(j => j.status === filter.status);
    }
    return journals.sort((a, b) => b.updatedAt - a.updatedAt);
  }

  deleteJournal(workflowId: string): void {
    this._journals.delete(workflowId);
    this._dirty.delete(workflowId);
    const filePath = this._journalPath(workflowId);
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
  }

  // ─── Persistence ─────────────────────────────────────────────────────────

  flush(): void {
    for (const workflowId of this._dirty) {
      const journal = this._journals.get(workflowId);
      if (!journal) continue;
      
      const filePath = this._journalPath(workflowId);
      const content = JSON.stringify(journal, null, 2);
      
      // Check size limit
      const sizeMb = Buffer.byteLength(content) / (1024 * 1024);
      if (sizeMb > this._maxJournalSizeMb) {
        // Truncate old events, keep last 1000
        journal.events = journal.events.slice(-1000);
      }
      
      fs.writeFileSync(filePath, content, 'utf-8');
    }
    this._dirty.clear();
  }

  loadFromDisk(): WorkflowJournal[] {
    const loaded: WorkflowJournal[] = [];
    
    if (!fs.existsSync(this._journalDir)) return loaded;
    
    const files = fs.readdirSync(this._journalDir).filter(f => f.endsWith('.journal.json'));
    for (const file of files) {
      try {
        const filePath = path.join(this._journalDir, file);
        const content = fs.readFileSync(filePath, 'utf-8');
        const journal: WorkflowJournal = JSON.parse(content);
        this._journals.set(journal.workflowId, journal);
        loaded.push(journal);
      } catch (err) {
        // Skip corrupt journal files
        console.warn(`Failed to load journal: ${file}`, err);
      }
    }
    
    return loaded;
  }

  // ─── Recovery ─────────────────────────────────────────────────────────────

  getIncompleteWorkflows(): WorkflowJournal[] {
    return this.listJournals().filter(j => 
      j.status === WorkflowStatus.RUNNING || 
      j.status === WorkflowStatus.CONTINUING_AS_NEW
    );
  }

  // ─── Cleanup ──────────────────────────────────────────────────────────────

  close(): void {
    if (this._flushTimer) {
      clearInterval(this._flushTimer);
      this._flushTimer = null;
    }
    // Final flush
    this.flush();
  }

  private _journalPath(workflowId: string): string {
    // Sanitize workflowId for filesystem
    const safe = workflowId.replace(/[^a-zA-Z0-9_-]/g, '_');
    return path.join(this._journalDir, `${safe}.journal.json`);
  }
}

// ─── In-Memory Backend (for testing) ─────────────────────────────────────────

export class InMemoryJournalBackend extends FileJournalBackend {
  constructor() {
    super({ journalDir: '__in_memory__' });
  }

  flush(): void {
    // No-op for in-memory
  }

  loadFromDisk(): WorkflowJournal[] {
    return [];
  }
}
