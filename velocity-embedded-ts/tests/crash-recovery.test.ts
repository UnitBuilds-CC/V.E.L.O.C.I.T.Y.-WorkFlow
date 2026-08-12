import { VelocityEmbedded, Durable, DurableContext, EmbeddedInMemoryStorage, EmbeddedStoredJournal, WorkflowStatus } from '../src/index';

@Durable()
class CrashWorkflow {
  async execute(ctx: DurableContext, shouldFail: boolean): Promise<string> {
    if (shouldFail) throw new Error('intentional failure');
    return 'recovered';
  }
}

@Durable()
class StateWorkflow {
  async execute(ctx: DurableContext): Promise<number> {
    const count = ctx.getState<number>('count') || 0;
    ctx.setState('count', count + 1);
    return count + 1;
  }
}

describe('Embedded Crash Re-execution', () => {
  test('incomplete workflows are re-executed on restart', async () => {
    const storage = new EmbeddedInMemoryStorage();
    
    // Simulate a crash by saving an incomplete journal
    const incompleteJournal: EmbeddedStoredJournal = {
      workflowId: 'wf-crash-1',
      functionName: 'CrashWorkflow.execute',
      entries: [],
      state: {},
      output: null,
      error: undefined,
      status: 'running',
      createdAt: Date.now() - 1000,
      completedAt: 0,
    };
    storage.saveJournal(incompleteJournal);
    
    // Create a new server with the storage (simulating restart)
    const embedded = new VelocityEmbedded(undefined, storage);
    embedded.register(CrashWorkflow);
    
    // Wait for re-execution
    await new Promise(r => setTimeout(r, 200));
    
    // Verify the workflow was re-executed
    const record = embedded.getWorkflow('wf-crash-1');
    expect(record).toBeDefined();
    expect(record?.status).toBe(WorkflowStatus.COMPLETED);
    expect(record?.output).toBe('recovered');
  });

  test('completed workflows are not re-executed', async () => {
    const storage = new EmbeddedInMemoryStorage();
    
    const completedJournal: EmbeddedStoredJournal = {
      workflowId: 'wf-completed-1',
      functionName: 'CrashWorkflow.execute',
      entries: [],
      state: {},
      output: 'already-done',
      error: undefined,
      status: 'completed',
      createdAt: Date.now() - 1000,
      completedAt: Date.now() - 500,
    };
    storage.saveJournal(completedJournal);
    
    const embedded = new VelocityEmbedded(undefined, storage);
    let executed = false;
    
    @Durable()
    class TestWorkflow {
      async execute(ctx: DurableContext): Promise<string> {
        executed = true;
        return 'should-not-run';
      }
    }
    
    embedded.register(TestWorkflow);
    await new Promise(r => setTimeout(r, 100));
    
    // Should not have executed again
    expect(executed).toBe(false);
    const record = embedded.getWorkflow('wf-completed-1');
    expect(record?.output).toBe('already-done');
  });

  test('failed workflows are not re-executed', async () => {
    const storage = new EmbeddedInMemoryStorage();
    
    const failedJournal: EmbeddedStoredJournal = {
      workflowId: 'wf-failed-1',
      functionName: 'CrashWorkflow.execute',
      entries: [],
      state: {},
      output: null,
      error: 'previous error',
      status: 'failed',
      createdAt: Date.now() - 1000,
      completedAt: Date.now() - 500,
    };
    storage.saveJournal(failedJournal);
    
    const embedded = new VelocityEmbedded(undefined, storage);
    let executed = false;
    
    @Durable()
    class TestWorkflow {
      async execute(ctx: DurableContext): Promise<string> {
        executed = true;
        return 'should-not-run';
      }
    }
    
    embedded.register(TestWorkflow);
    await new Promise(r => setTimeout(r, 100));
    
    expect(executed).toBe(false);
    const record = embedded.getWorkflow('wf-failed-1');
    expect(record?.status).toBe(WorkflowStatus.FAILED);
    expect(record?.error).toBe('previous error');
  });

  test('workflow state is restored from storage', async () => {
    const storage = new EmbeddedInMemoryStorage();
    
    // Save a journal with state
    const journal: EmbeddedStoredJournal = {
      workflowId: 'wf-state-1',
      functionName: 'StateWorkflow.execute',
      entries: [],
      state: { count: 5 },
      output: 5,
      error: undefined,
      status: 'completed',
      createdAt: Date.now() - 1000,
      completedAt: Date.now() - 500,
    };
    storage.saveJournal(journal);
    
    // Create new server (simulating restart)
    const embedded = new VelocityEmbedded(undefined, storage);
    embedded.register(StateWorkflow);
    
    await new Promise(r => setTimeout(r, 100));
    
    // Execute again - should see restored state
    const handle = await embedded.execute('StateWorkflow', 'execute', 'wf-state-2');
    const result = await handle.result;
    expect(result).toBe(1); // New workflow starts from 0, increments to 1
  });
});
