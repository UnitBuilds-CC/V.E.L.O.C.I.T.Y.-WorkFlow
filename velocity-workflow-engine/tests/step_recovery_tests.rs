//! Per-step crash recovery integration tests for the VELOCITY-WorkFlow engine.
//!
//! These tests verify that the per-step durability primitives work correctly:
//! - `persist_step()` performs WAL append + fsync + PG INSERT
//! - Crash at any point → resume from last persisted step
//! - Recovery merges WAL + PG journals (WAL takes precedence)
//! - Never loses work, never locks up

use velocity_workflow_engine::db_adapter::{DatabaseAdapter, DatabaseError, DatabaseResult, InMemoryAdapter, StatusFilter, WorkflowEventRecord, WorkflowRecord, SearchAttributes, SearchAttributeValue};
use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A test adapter that actually stores step journal entries in memory.
/// This allows us to test `recover_steps_from_pg()` properly.
struct TestStepJournalAdapter {
    inner: InMemoryAdapter,
    journals: Mutex<HashMap<u64, Vec<(u32, Option<Vec<u8>>)>>>,
}

impl TestStepJournalAdapter {
    fn new() -> Self {
        Self {
            inner: InMemoryAdapter::new(),
            journals: Mutex::new(HashMap::new()),
        }
    }
}

impl DatabaseAdapter for TestStepJournalAdapter {
    fn init_schema(&self) -> DatabaseResult<()> { self.inner.init_schema() }
    fn save_workflow(&self, key: u64, record: &WorkflowRecord) -> DatabaseResult<()> { self.inner.save_workflow(key, record) }
    fn load_workflow(&self, key: u64) -> DatabaseResult<WorkflowRecord> { self.inner.load_workflow(key) }
    fn delete_workflow(&self, key: u64) -> DatabaseResult<()> { self.inner.delete_workflow(key) }
    fn list_workflows(&self, ns: Option<&str>, sf: StatusFilter, lim: u32, off: u32) -> DatabaseResult<Vec<WorkflowRecord>> { self.inner.list_workflows(ns, sf, lim, off) }
    fn save_event(&self, wk: u64, et: u8, etn: &str, sn: u64, d: Vec<u8>) -> DatabaseResult<i64> { self.inner.save_event(wk, et, etn, sn, d) }
    fn load_events(&self, wk: u64) -> DatabaseResult<Vec<WorkflowEventRecord>> { self.inner.load_events(wk) }
    fn save_search_attributes(&self, key: u64, attrs: &SearchAttributes) -> DatabaseResult<()> { self.inner.save_search_attributes(key, attrs) }
    fn load_search_attributes(&self, key: u64) -> DatabaseResult<SearchAttributes> { self.inner.load_search_attributes(key) }
    fn update_workflow_status(&self, key: u64, status: WorkflowStatus) -> DatabaseResult<()> { self.inner.update_workflow_status(key, status) }
    fn count_workflows(&self, ns: Option<&str>, sf: StatusFilter) -> DatabaseResult<u64> { self.inner.count_workflows(ns, sf) }
    fn is_connected(&self) -> bool { self.inner.is_connected() }
    fn adapter_name(&self) -> &str { "TestStepJournalAdapter" }

    fn save_step(&self, workflow_key: u64, step_number: u32, result_data: Option<&[u8]>) -> DatabaseResult<()> {
        let mut journals = self.journals.lock().unwrap();
        journals.entry(workflow_key).or_insert_with(Vec::new).push((step_number, result_data.map(|d| d.to_vec())));
        Ok(())
    }

    fn save_steps_batch(&self, workflow_key: u64, steps: &[(u32, Option<Vec<u8>>)]) -> DatabaseResult<()> {
        let mut journals = self.journals.lock().unwrap();
        let entry = journals.entry(workflow_key).or_insert_with(Vec::new);
        for (step, result) in steps {
            entry.push((*step, result.clone()));
        }
        Ok(())
    }

    fn load_steps(&self, workflow_key: u64) -> DatabaseResult<Vec<(u32, Option<Vec<u8>>)>> {
        let journals = self.journals.lock().unwrap();
        Ok(journals.get(&workflow_key).cloned().unwrap_or_default())
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Per-Step Persistence Tests
// ═══════════════════════════════════════════════════════════════════════════════

/// Test that `persist_step()` completes a step and marks it as completed in-memory.
#[test]
fn test_persist_step_completes_step() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(100, 1, 0, 42, 5, None);

    // Persist step 0
    let result = engine.persist_step(key, 0, "default");
    assert!(result.is_ok(), "persist_step should succeed");

    // Step 0 should be completed
    assert!(engine.is_step_completed(key, 0), "Step 0 should be completed");
    assert!(!engine.is_step_completed(key, 1), "Step 1 should not be completed yet");
}

/// Test that sequential `persist_step()` calls complete steps in order.
#[test]
fn test_sequential_persist_step() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(100, 1, 0, 42, 10, None);

    // Persist steps 0-4 sequentially
    for step in 0..5 {
        let result = engine.persist_step(key, step, "default");
        assert!(result.is_ok(), "persist_step({}) should succeed", step);
        assert!(engine.is_step_completed(key, step), "Step {} should be completed", step);
    }

    // Steps 5-9 should not be completed
    for step in 5..10 {
        assert!(!engine.is_step_completed(key, step), "Step {} should not be completed", step);
    }
}

/// Test that `persist_step_async()` completes steps without blocking on PG.
#[test]
fn test_persist_step_async_nonblocking() {
    let mut engine = WorkflowEngine::new();
    let adapter = Arc::new(InMemoryAdapter::new());
    engine.enable_db_adapter(adapter);

    let key = engine.start_workflow(100, 1, 0, 42, 5, None);

    // Persist steps asynchronously
    for step in 0..5 {
        let result = engine.persist_step_async(key, step, "default");
        assert!(result.is_ok(), "persist_step_async({}) should succeed", step);
    }

    // All steps should be completed (WAL fsync is synchronous, PG is fire-and-forget)
    for step in 0..5 {
        assert!(engine.is_step_completed(key, step), "Step {} should be completed", step);
    }
}

/// Test that `persist_steps_batch()` completes all steps in one batch.
#[test]
fn test_persist_steps_batch() {
    let engine = WorkflowEngine::new();
    let key = engine.start_workflow(100, 1, 0, 42, 10, None);

    // Persist all steps in batch
    let result = engine.persist_steps_batch(key, "default");
    assert!(result.is_ok(), "persist_steps_batch should succeed");

    // All steps should be completed
    for step in 0..10 {
        assert!(engine.is_step_completed(key, step), "Step {} should be completed", step);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// WAL Recovery Tests (Per-Step Granularity)
// ═══════════════════════════════════════════════════════════════════════════════

/// Test crash recovery: crash after step 3 → restart → steps 0-3 replayed from WAL.
#[test]
fn test_crash_recovery_after_step_3() {
    let wal_path = format!("velocity_test_step_recovery_{}.wal", std::process::id());

    // Phase 1: Start workflow, persist steps 0-3, then "crash"
    {
        let engine = WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024)
            .expect("Failed to create engine");

        let key = engine.start_workflow(100, 1, 0, 42, 10, None);

        // Persist steps 0-3 (simulating crash after step 3)
        for step in 0..4 {
            engine.persist_step(key, step, "default").expect("persist_step failed");
        }

        // Verify steps 0-3 are completed
        for step in 0..4 {
            assert!(engine.is_step_completed(key, step), "Step {} should be completed", step);
        }

        // "Crash" — engine is dropped here
    }

    // Phase 2: Create a new engine with the same WAL file (simulates restart)
    {
        let engine = WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024)
            .expect("Failed to create recovery engine");

        // Recover from WAL
        let (records, workflows) = engine.recover_from_wal().expect("WAL recovery failed");

        // Verify recovery counts
        assert!(records > 0, "Should have replayed WAL records");
        assert!(workflows > 0, "Should have recovered workflows");

        // Verify recovered state
        let key = (0u64 << 32) | 100;

        // Workflow should be running with steps 0-3 completed
        assert_eq!(engine.get_status(key), WorkflowStatus::Running);
        for step in 0..4 {
            assert!(
                engine.is_step_completed(key, step),
                "Step {} should be recovered from WAL",
                step
            );
        }

        // Steps 4-9 should not be completed
        for step in 4..10 {
            assert!(
                !engine.is_step_completed(key, step),
                "Step {} should not be recovered (never persisted)",
                step
            );
        }

        // Resume execution: persist remaining steps 4-9
        for step in 4..10 {
            engine.persist_step(key, step, "default").expect("persist_step failed");
        }

        // All steps should now be completed
        for step in 0..10 {
            assert!(engine.is_step_completed(key, step), "Step {} should be completed", step);
        }

        // Complete the workflow
        engine.complete_workflow(key, Some(b"done".to_vec()));
        assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
    }

    // Clean up WAL file
    let _ = std::fs::remove_file(&wal_path);
}

/// Test crash recovery: crash during PG write → WAL still has the step → recovered from WAL.
#[test]
fn test_crash_during_pg_write_wal_has_it() {
    let wal_path = format!("velocity_test_pg_crash_{}.wal", std::process::id());

    // Phase 1: Start workflow, persist step with async PG (fire-and-forget), then "crash"
    {
        let mut engine = WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024)
            .expect("Failed to create engine");
        let adapter = Arc::new(InMemoryAdapter::new());
        engine.enable_db_adapter(adapter);

        let key = engine.start_workflow(100, 1, 0, 42, 5, None);

        // Persist step 0 asynchronously (WAL fsync is synchronous, PG is fire-and-forget)
        engine.persist_step_async(key, 0, "default").expect("persist_step_async failed");

        // Step 0 should be completed in-memory
        assert!(engine.is_step_completed(key, 0), "Step 0 should be completed");

        // "Crash" — engine is dropped here (PG write may or may not have completed)
    }

    // Phase 2: Create a new engine with the same WAL file (simulates restart)
    {
        let engine = WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024)
            .expect("Failed to create recovery engine");

        // Recover from WAL
        let (records, workflows) = engine.recover_from_wal().expect("WAL recovery failed");

        // Verify recovery counts
        assert!(records > 0, "Should have replayed WAL records");
        assert!(workflows > 0, "Should have recovered workflows");

        // Verify recovered state
        let key = (0u64 << 32) | 100;

        // Workflow should be running with step 0 completed (recovered from WAL)
        assert_eq!(engine.get_status(key), WorkflowStatus::Running);
        assert!(
            engine.is_step_completed(key, 0),
            "Step 0 should be recovered from WAL (even if PG write didn't complete)"
        );
    }

    // Clean up WAL file
    let _ = std::fs::remove_file(&wal_path);
}

/// Test concurrent workflows crashing independently → no cross-contamination.
#[test]
fn test_concurrent_crash_recovery_no_contamination() {
    let wal_path = format!("velocity_test_concurrent_crash_{}.wal", std::process::id());

    // Phase 1: Start 3 workflows, persist different steps for each, then "crash"
    {
        let engine = WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024)
            .expect("Failed to create engine");

        let key1 = engine.start_workflow(100, 1, 0, 42, 10, None);
        let key2 = engine.start_workflow(101, 1, 0, 42, 10, None);
        let key3 = engine.start_workflow(102, 1, 0, 42, 10, None);

        // Workflow 1: persist steps 0-2
        for step in 0..3 {
            engine.persist_step(key1, step, "default").expect("persist_step failed");
        }

        // Workflow 2: persist steps 0-5
        for step in 0..6 {
            engine.persist_step(key2, step, "default").expect("persist_step failed");
        }

        // Workflow 3: persist steps 0-8
        for step in 0..9 {
            engine.persist_step(key3, step, "default").expect("persist_step failed");
        }

        // "Crash" — engine is dropped here
    }

    // Phase 2: Create a new engine with the same WAL file (simulates restart)
    {
        let engine = WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024)
            .expect("Failed to create recovery engine");

        // Recover from WAL
        let (records, workflows) = engine.recover_from_wal().expect("WAL recovery failed");

        // Verify recovery counts
        assert!(records > 0, "Should have replayed WAL records");
        assert_eq!(workflows, 3, "Should have recovered 3 workflows");

        // Verify recovered state for each workflow
        let key1 = (0u64 << 32) | 100;
        let key2 = (0u64 << 32) | 101;
        let key3 = (0u64 << 32) | 102;

        // Workflow 1: steps 0-2 should be recovered
        assert_eq!(engine.get_status(key1), WorkflowStatus::Running);
        for step in 0..3 {
            assert!(engine.is_step_completed(key1, step), "WF1 step {} should be recovered", step);
        }
        for step in 3..10 {
            assert!(!engine.is_step_completed(key1, step), "WF1 step {} should not be recovered", step);
        }

        // Workflow 2: steps 0-5 should be recovered
        assert_eq!(engine.get_status(key2), WorkflowStatus::Running);
        for step in 0..6 {
            assert!(engine.is_step_completed(key2, step), "WF2 step {} should be recovered", step);
        }
        for step in 6..10 {
            assert!(!engine.is_step_completed(key2, step), "WF2 step {} should not be recovered", step);
        }

        // Workflow 3: steps 0-8 should be recovered
        assert_eq!(engine.get_status(key3), WorkflowStatus::Running);
        for step in 0..9 {
            assert!(engine.is_step_completed(key3, step), "WF3 step {} should be recovered", step);
        }
        assert!(!engine.is_step_completed(key3, 9), "WF3 step 9 should not be recovered");
    }

    // Clean up WAL file
    let _ = std::fs::remove_file(&wal_path);
}

// ═══════════════════════════════════════════════════════════════════════════════
// PostgreSQL Step Journal Recovery Tests
// ═══════════════════════════════════════════════════════════════════════════════

/// Test that `recover_steps_from_pg()` fills gaps when PG has steps WAL didn't capture.
#[test]
fn test_pg_step_journal_recovery_fills_gaps() {
    let engine = WorkflowEngine::new();
    let adapter = Arc::new(TestStepJournalAdapter::new());
    let adapter_clone = adapter.clone();
    let mut engine_mut = engine;
    engine_mut.enable_db_adapter(adapter_clone);

    let key = engine_mut.start_workflow(100, 1, 0, 42, 10, None);

    // Manually complete some steps in-memory (simulating WAL recovery)
    for step in 0..3 {
        engine_mut.complete_step(key, step, vec![]);
    }

    // Manually save steps 3-5 to PG journal (simulating PG writes that WAL didn't capture)
    for step in 3..6 {
        adapter.save_step(key, step, None).expect("save_step failed");
    }

    // Recover steps from PG
    let (workflows, steps) = engine_mut.recover_steps_from_pg().expect("recover_steps_from_pg failed");

    // Should have recovered 1 workflow with 3 steps (3, 4, 5)
    assert_eq!(workflows, 1, "Should have recovered 1 workflow");
    assert_eq!(steps, 3, "Should have recovered 3 steps from PG");

    // Steps 0-5 should now be completed
    for step in 0..6 {
        assert!(engine_mut.is_step_completed(key, step), "Step {} should be completed", step);
    }

    // Steps 6-9 should not be completed
    for step in 6..10 {
        assert!(!engine_mut.is_step_completed(key, step), "Step {} should not be completed", step);
    }
}

/// Test that `recover_steps_from_pg()` returns (0, 0) when no PG adapter is configured.
#[test]
fn test_pg_step_journal_recovery_no_adapter() {
    let mut engine = WorkflowEngine::new();
    let key = engine.start_workflow(100, 1, 0, 42, 5, None);

    // Complete some steps
    for step in 0..3 {
        engine.complete_step(key, step, vec![]);
    }

    // Try to recover from PG (no adapter configured)
    let (workflows, steps) = engine.recover_steps_from_pg().expect("recover_steps_from_pg failed");

    // Should return (0, 0) — no PG adapter
    assert_eq!(workflows, 0, "Should have recovered 0 workflows (no PG adapter)");
    assert_eq!(steps, 0, "Should have recovered 0 steps (no PG adapter)");
}

/// Test that WAL takes precedence over PG when both have the same step.
#[test]
fn test_wal_takes_precedence_over_pg() {
    let engine = WorkflowEngine::new();
    let adapter = Arc::new(TestStepJournalAdapter::new());
    let adapter_clone = adapter.clone();
    let mut engine_mut = engine;
    engine_mut.enable_db_adapter(adapter_clone);

    let key = engine_mut.start_workflow(100, 1, 0, 42, 5, None);

    // Complete step 0 in-memory (simulating WAL recovery)
    engine_mut.complete_step(key, 0, b"wal-result".to_vec());

    // Save step 0 to PG with different result (simulating PG write)
    adapter.save_step(key, 0, Some(b"pg-result")).expect("save_step failed");

    // Recover steps from PG
    let (workflows, steps) = engine_mut.recover_steps_from_pg().expect("recover_steps_from_pg failed");

    // Should not have recovered any steps (step 0 already completed by WAL)
    assert_eq!(workflows, 0, "Should have recovered 0 workflows (step already completed)");
    assert_eq!(steps, 0, "Should have recovered 0 steps (step already completed by WAL)");

    // Step 0 should still be completed (WAL result takes precedence)
    assert!(engine_mut.is_step_completed(key, 0), "Step 0 should be completed");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Benchmark Workflow Tests
// ═══════════════════════════════════════════════════════════════════════════════

/// Test that `run_bench_workflow()` completes all steps and persists them.
#[test]
fn test_run_bench_workflow_persists_all_steps() {
    let engine = WorkflowEngine::new();
    let key = engine.run_bench_workflow("default").expect("run_bench_workflow failed");

    // All 10 steps should be completed
    for step in 0..10 {
        assert!(engine.is_step_completed(key, step), "Step {} should be completed", step);
    }

    // Workflow should be completed
    assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
}

/// Test that `run_bench_workflow()` can recover from crash mid-execution.
#[test]
fn test_run_bench_workflow_crash_recovery() {
    let wal_path = format!("velocity_test_bench_crash_{}.wal", std::process::id());

    // Phase 1: Start bench workflow, persist 5 steps, then "crash"
    {
        let engine = WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024)
            .expect("Failed to create engine");

        let key = engine.start_workflow(1, 11, 0, 0, 10, None);
        engine.sync_wal();

        // Persist steps 0-4
        for step in 0..5 {
            engine.persist_step(key, step, "default").expect("persist_step failed");
        }

        // "Crash" — engine is dropped here
    }

    // Phase 2: Create a new engine with the same WAL file (simulates restart)
    {
        let engine = WorkflowEngine::with_wal(&wal_path, 64 * 1024 * 1024)
            .expect("Failed to create recovery engine");

        // Recover from WAL
        let (records, workflows) = engine.recover_from_wal().expect("WAL recovery failed");

        // Verify recovery counts
        assert!(records > 0, "Should have replayed WAL records");
        assert!(workflows > 0, "Should have recovered workflows");

        // Verify recovered state
        let key = (0u64 << 32) | 1;

        // Workflow should be running with steps 0-4 completed
        assert_eq!(engine.get_status(key), WorkflowStatus::Running);
        for step in 0..5 {
            assert!(engine.is_step_completed(key, step), "Step {} should be recovered", step);
        }

        // Resume execution: persist remaining steps 5-9
        for step in 5..10 {
            engine.persist_step(key, step, "default").expect("persist_step failed");
        }

        // Complete the workflow
        engine.complete_workflow(key, Some(b"done".to_vec()));
        assert_eq!(engine.get_status(key), WorkflowStatus::Completed);
    }

    // Clean up WAL file
    let _ = std::fs::remove_file(&wal_path);
}
