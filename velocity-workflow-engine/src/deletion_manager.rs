//! Deletion manager matching Temporal's service/history/deletemanager.
//!
//! Handles the multi-step workflow execution deletion pipeline:
//! 1. Create delete execution tasks (transfer, visibility, replication)
//! 2. Execute deletion steps in order
//! 3. Handle partial failures with retry
//! 4. Track deletion progress and completion

use std::collections::HashMap;
use std::sync::{Arc, RwLock, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::time::{SystemTime, Duration};

// ═══════════════════════════════════════════════════════════════════════════════
// Deletion Pipeline
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionStage {
    Pending,
    DeletingHistory,
    DeletingVisibility,
    DeletingReplication,
    DeletingArchival,
    DeletingSearchAttributes,
    DeletingMemo,
    CleanupComplete,
    Failed,
}

impl DeletionStage {
    pub fn is_terminal(&self) -> bool { matches!(self, DeletionStage::CleanupComplete | DeletionStage::Failed) }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "Pending", Self::DeletingHistory => "DeletingHistory",
            Self::DeletingVisibility => "DeletingVisibility", Self::DeletingReplication => "DeletingReplication",
            Self::DeletingArchival => "DeletingArchival", Self::DeletingSearchAttributes => "DeletingSearchAttributes",
            Self::DeletingMemo => "DeletingMemo", Self::CleanupComplete => "CleanupComplete",
            Self::Failed => "Failed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeletionRecord {
    pub record_id: String,
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub stage: DeletionStage,
    pub attempt: u32,
    pub max_attempts: u32,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    pub last_error: Option<String>,
    pub step_results: HashMap<String, StepResult>,
}

#[derive(Debug, Clone)]
pub struct StepResult {
    pub stage: DeletionStage,
    pub success: bool,
    pub items_deleted: u64,
    pub duration_ms: u64,
    pub error: Option<String>,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Deletion Manager
// ═══════════════════════════════════════════════════════════════════════════════

pub struct DeletionManager {
    pub records: RwLock<HashMap<String, DeletionRecord>>,
    pub next_id: AtomicU64,
    pub config: DeletionManagerConfig,
    pub stats: DeletionManagerStats,
    pub running: AtomicBool,
}

#[derive(Debug, Clone)]
pub struct DeletionManagerConfig {
    pub max_concurrent_deletions: u32,
    pub max_attempts_per_stage: u32,
    pub stage_timeout: Duration,
    pub batch_size: u32,
    pub retry_backoff: Duration,
}

impl Default for DeletionManagerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_deletions: 10, max_attempts_per_stage: 5,
            stage_timeout: Duration::from_secs(30), batch_size: 100,
            retry_backoff: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Default)]
pub struct DeletionManagerStats {
    pub deletions_started: AtomicU64,
    pub deletions_completed: AtomicU64,
    pub deletions_failed: AtomicU64,
    pub total_stages_executed: AtomicU64,
    pub total_items_deleted: AtomicU64,
    pub total_retries: AtomicU64,
}

impl DeletionManager {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()), next_id: AtomicU64::new(1),
            config: DeletionManagerConfig::default(),
            stats: DeletionManagerStats::default(), running: AtomicBool::new(true),
        }
    }

    pub fn start_deletion(&self, namespace_id: &str, workflow_id: &str, run_id: &str) -> Result<String, String> {
        let id = format!("del-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let record = DeletionRecord {
            record_id: id.clone(), namespace_id: namespace_id.to_string(),
            workflow_id: workflow_id.to_string(), run_id: run_id.to_string(),
            stage: DeletionStage::Pending, attempt: 0,
            max_attempts: self.config.max_attempts_per_stage,
            started_at: now_millis(), completed_at: None, last_error: None,
            step_results: HashMap::new(),
        };
        self.records.write().unwrap().insert(id.clone(), record);
        self.stats.deletions_started.fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }

    pub fn execute_next_stage(&self, record_id: &str) -> Result<DeletionStage, String> {
        let mut records = self.records.write().unwrap();
        let record = records.get_mut(record_id).ok_or("Record not found")?;
        if record.stage.is_terminal() {
            return Ok(record.stage);
        }
        let next_stage = match record.stage {
            DeletionStage::Pending => DeletionStage::DeletingHistory,
            DeletionStage::DeletingHistory => DeletionStage::DeletingVisibility,
            DeletionStage::DeletingVisibility => DeletionStage::DeletingReplication,
            DeletionStage::DeletingReplication => DeletionStage::DeletingArchival,
            DeletionStage::DeletingArchival => DeletionStage::DeletingSearchAttributes,
            DeletionStage::DeletingSearchAttributes => DeletionStage::DeletingMemo,
            DeletionStage::DeletingMemo => DeletionStage::CleanupComplete,
            _ => return Err("Invalid stage transition".into()),
        };
        // Simulate stage execution
        let result = StepResult {
            stage: next_stage, success: true, items_deleted: 1,
            duration_ms: 10, error: None,
        };
        record.step_results.insert(next_stage.as_str().to_string(), result);
        record.stage = next_stage;
        record.attempt = 0;
        self.stats.total_stages_executed.fetch_add(1, Ordering::Relaxed);
        self.stats.total_items_deleted.fetch_add(1, Ordering::Relaxed);
        if next_stage == DeletionStage::CleanupComplete {
            record.completed_at = Some(now_millis());
            self.stats.deletions_completed.fetch_add(1, Ordering::Relaxed);
        }
        Ok(next_stage)
    }

    pub fn fail_stage(&self, record_id: &str, error: &str) -> Result<bool, String> {
        let mut records = self.records.write().unwrap();
        let record = records.get_mut(record_id).ok_or("Record not found")?;
        record.attempt += 1;
        record.last_error = Some(error.to_string());
        if record.attempt >= record.max_attempts {
            record.stage = DeletionStage::Failed;
            record.completed_at = Some(now_millis());
            self.stats.deletions_failed.fetch_add(1, Ordering::Relaxed);
            Ok(false)
        } else {
            self.stats.total_retries.fetch_add(1, Ordering::Relaxed);
            Ok(true) // can retry
        }
    }

    pub fn get_record(&self, record_id: &str) -> Option<DeletionRecord> {
        self.records.read().unwrap().get(record_id).cloned()
    }

    pub fn pending_deletions(&self) -> Vec<DeletionRecord> {
        self.records.read().unwrap().values()
            .filter(|r| !r.stage.is_terminal())
            .cloned().collect()
    }

    pub fn completed_deletions(&self) -> Vec<DeletionRecord> {
        self.records.read().unwrap().values()
            .filter(|r| r.stage == DeletionStage::CleanupComplete)
            .cloned().collect()
    }

    pub fn failed_deletions(&self) -> Vec<DeletionRecord> {
        self.records.read().unwrap().values()
            .filter(|r| r.stage == DeletionStage::Failed)
            .cloned().collect()
    }

    pub fn execute_full_pipeline(&self, namespace_id: &str, workflow_id: &str, run_id: &str) -> Result<DeletionRecord, String> {
        let id = self.start_deletion(namespace_id, workflow_id, run_id)?;
        loop {
            let stage = self.execute_next_stage(&id)?;
            if stage.is_terminal() { break; }
        }
        self.get_record(&id).ok_or("Record lost".into())
    }

    pub fn active_count(&self) -> usize {
        self.records.read().unwrap().values().filter(|r| !r.stage.is_terminal()).count()
    }

    pub fn total_count(&self) -> usize { self.records.read().unwrap().len() }

    pub fn shutdown(&self) { self.running.store(false, Ordering::Relaxed); }
    pub fn is_running(&self) -> bool { self.running.load(Ordering::Relaxed) }
}

fn now_millis() -> i64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deletion_stage_terminal() {
        assert!(!DeletionStage::Pending.is_terminal());
        assert!(!DeletionStage::DeletingHistory.is_terminal());
        assert!(DeletionStage::CleanupComplete.is_terminal());
        assert!(DeletionStage::Failed.is_terminal());
    }

    #[test]
    fn test_start_deletion() {
        let mgr = DeletionManager::new();
        let id = mgr.start_deletion("ns", "wf", "run").unwrap();
        assert!(!id.is_empty());
        assert_eq!(mgr.active_count(), 1);
        let record = mgr.get_record(&id).unwrap();
        assert_eq!(record.stage, DeletionStage::Pending);
    }

    #[test]
    fn test_execute_stages() {
        let mgr = DeletionManager::new();
        let id = mgr.start_deletion("ns", "wf", "run").unwrap();
        let s1 = mgr.execute_next_stage(&id).unwrap();
        assert_eq!(s1, DeletionStage::DeletingHistory);
        let s2 = mgr.execute_next_stage(&id).unwrap();
        assert_eq!(s2, DeletionStage::DeletingVisibility);
        let s3 = mgr.execute_next_stage(&id).unwrap();
        assert_eq!(s3, DeletionStage::DeletingReplication);
        let s4 = mgr.execute_next_stage(&id).unwrap();
        assert_eq!(s4, DeletionStage::DeletingArchival);
        let s5 = mgr.execute_next_stage(&id).unwrap();
        assert_eq!(s5, DeletionStage::DeletingSearchAttributes);
        let s6 = mgr.execute_next_stage(&id).unwrap();
        assert_eq!(s6, DeletionStage::DeletingMemo);
        let s7 = mgr.execute_next_stage(&id).unwrap();
        assert_eq!(s7, DeletionStage::CleanupComplete);
        assert_eq!(mgr.active_count(), 0);
        assert_eq!(mgr.completed_deletions().len(), 1);
    }

    #[test]
    fn test_full_pipeline() {
        let mgr = DeletionManager::new();
        let record = mgr.execute_full_pipeline("ns", "wf", "run").unwrap();
        assert_eq!(record.stage, DeletionStage::CleanupComplete);
        assert!(record.completed_at.is_some());
        assert_eq!(record.step_results.len(), 7);
    }

    #[test]
    fn test_fail_and_retry() {
        let mgr = DeletionManager::new();
        let id = mgr.start_deletion("ns", "wf", "run").unwrap();
        mgr.execute_next_stage(&id).unwrap(); // DeletingHistory
        let can_retry = mgr.fail_stage(&id, "transient error").unwrap();
        assert!(can_retry);
        let record = mgr.get_record(&id).unwrap();
        assert_eq!(record.attempt, 1);
    }

    #[test]
    fn test_fail_exhausted() {
        let mgr = DeletionManager::new();
        let id = mgr.start_deletion("ns", "wf", "run").unwrap();
        mgr.execute_next_stage(&id).unwrap();
        for _ in 0..5 { mgr.fail_stage(&id, "error").unwrap(); }
        let record = mgr.get_record(&id).unwrap();
        assert_eq!(record.stage, DeletionStage::Failed);
        assert_eq!(mgr.failed_deletions().len(), 1);
    }

    #[test]
    fn test_multiple_deletions() {
        let mgr = DeletionManager::new();
        mgr.start_deletion("ns", "wf1", "r1").unwrap();
        mgr.start_deletion("ns", "wf2", "r2").unwrap();
        mgr.start_deletion("ns", "wf3", "r3").unwrap();
        assert_eq!(mgr.active_count(), 3);
        assert_eq!(mgr.total_count(), 3);
    }

    #[test]
    fn test_deletion_stats() {
        let mgr = DeletionManager::new();
        mgr.execute_full_pipeline("ns", "wf1", "r1").unwrap();
        mgr.execute_full_pipeline("ns", "wf2", "r2").unwrap();
        assert_eq!(mgr.stats.deletions_started.load(Ordering::Relaxed), 2);
        assert_eq!(mgr.stats.deletions_completed.load(Ordering::Relaxed), 2);
        assert_eq!(mgr.stats.total_stages_executed.load(Ordering::Relaxed), 14); // 7 stages * 2
    }

    #[test]
    fn test_deletion_shutdown() {
        let mgr = DeletionManager::new();
        assert!(mgr.is_running());
        mgr.shutdown();
        assert!(!mgr.is_running());
    }

    #[test]
    fn test_pending_deletions() {
        let mgr = DeletionManager::new();
        let id1 = mgr.start_deletion("ns", "wf1", "r1").unwrap();
        mgr.start_deletion("ns", "wf2", "r2").unwrap();
        // Run id1 through all stages to completion
        loop {
            let stage = mgr.execute_next_stage(&id1).unwrap();
            if stage.is_terminal() { break; }
        }
        assert_eq!(mgr.pending_deletions().len(), 1);
        assert_eq!(mgr.completed_deletions().len(), 1);
    }
}
