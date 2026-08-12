//! LSM-style automatic history compaction.
//!
//! Implements the base.md vision: "Engine-level Automated History Compaction" —
//! instead of forcing developers to call ContinueAsNew(), the engine automatically
//! compacts historic execution logs in the background, much like LSM-trees in
//! database engines (e.g., RocksDB).
//!
//! Intermediate transient steps (retried activity attempts, expired timers) are
//! automatically squashed into consolidated "State Delta" events, keeping history
//! log sizes flat without touching application code.
//!
//! Compaction levels:
//!   L0: Raw events (append-only write buffer)
//!   L1: Activity-level compaction (squash individual activity retries)
//!   L2: Workflow-step compaction (squash completed steps into state deltas)
//!   L3: Terminal compaction (keep only final state + last N events for audit)

use std::collections::{HashMap, VecDeque};

/// Compaction level in the LSM hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompactionLevel {
    L0 = 0,
    L1 = 1,
    L2 = 2,
    L3 = 3,
}

/// A history event that may be compacted.
#[derive(Debug, Clone)]
pub struct CompactableEvent {
    pub event_id: u64,
    pub workflow_key: u64,
    pub event_type: CompactableEventType,
    pub level: CompactionLevel,
    pub timestamp_ms: u64,
    pub payload: Vec<u8>,
    pub merged_range: Option<(u64, u64)>,
}

/// Types of events subject to compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactableEventType {
    WorkflowStarted,
    ActivityTaskScheduled,
    ActivityTaskStarted,
    ActivityTaskCompleted,
    ActivityTaskFailed,
    TimerStarted,
    TimerFired,
    TimerCanceled,
    SignalReceived,
    WorkflowTaskScheduled,
    WorkflowTaskStarted,
    WorkflowTaskCompleted,
    ActivityCompacted,
    StepCompacted,
    TerminalState,
}

/// Configuration for the compaction engine.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub l0_threshold: usize,
    pub l1_threshold: usize,
    pub l2_threshold: usize,
    pub l3_audit_trail_count: usize,
    pub auto_compact: bool,
    pub compaction_interval_ms: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_threshold: 100,
            l1_threshold: 50,
            l2_threshold: 20,
            l3_audit_trail_count: 10,
            auto_compact: true,
            compaction_interval_ms: 5000,
        }
    }
}

/// Statistics for the compaction engine.
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    pub total_events_l0: u64,
    pub total_events_l1: u64,
    pub total_events_l2: u64,
    pub total_events_l3: u64,
    pub l0_to_l1_compactions: u64,
    pub l1_to_l2_compactions: u64,
    pub l2_to_l3_compactions: u64,
    pub events_squashed: u64,
    pub bytes_freed: u64,
    pub total_compaction_runs: u64,
}

/// Per-workflow compaction state.
struct WorkflowHistory {
    l0: VecDeque<CompactableEvent>,
    l1: VecDeque<CompactableEvent>,
    l2: VecDeque<CompactableEvent>,
    l3: VecDeque<CompactableEvent>,
}

impl WorkflowHistory {
    fn new() -> Self {
        Self {
            l0: VecDeque::new(),
            l1: VecDeque::new(),
            l2: VecDeque::new(),
            l3: VecDeque::new(),
        }
    }

    fn total_events(&self) -> usize {
        self.l0.len() + self.l1.len() + self.l2.len() + self.l3.len()
    }
}

/// LSM-style history compaction engine.
pub struct HistoryCompactor {
    config: CompactionConfig,
    histories: HashMap<u64, WorkflowHistory>,
    next_event_id: u64,
    stats: CompactionStats,
}

impl HistoryCompactor {
    pub fn new(config: CompactionConfig) -> Self {
        Self {
            config,
            histories: HashMap::new(),
            next_event_id: 1,
            stats: CompactionStats::default(),
        }
    }

    /// Append a raw event to L0 for a workflow.
    pub fn append_event(
        &mut self,
        workflow_key: u64,
        event_type: CompactableEventType,
        payload: Vec<u8>,
    ) -> u64 {
        let event_id = self.next_event_id;
        self.next_event_id += 1;

        let event = CompactableEvent {
            event_id,
            workflow_key,
            event_type,
            level: CompactionLevel::L0,
            timestamp_ms: 0,
            payload,
            merged_range: None,
        };

        let history = self
            .histories
            .entry(workflow_key)
            .or_insert_with(WorkflowHistory::new);
        history.l0.push_back(event);
        self.stats.total_events_l0 += 1;

        event_id
    }

    /// Run compaction for a specific workflow. Returns the number of events squashed.
    pub fn compact_workflow(&mut self, workflow_key: u64) -> u64 {
        let mut squashed = 0;

        // Take the history out to avoid double-borrowing self
        let mut history = match self.histories.remove(&workflow_key) {
            Some(h) => h,
            None => return 0,
        };

        let l0_thresh = self.config.l0_threshold;
        let l1_thresh = self.config.l1_threshold;
        let l2_thresh = self.config.l2_threshold;
        let l3_audit = self.config.l3_audit_trail_count;

        if history.l0.len() >= l0_thresh {
            squashed +=
                Self::do_compact_l0_to_l1(&mut history, &mut self.next_event_id, &mut self.stats);
            self.stats.l0_to_l1_compactions += 1;
        }

        if history.l1.len() >= l1_thresh {
            squashed +=
                Self::do_compact_l1_to_l2(&mut history, &mut self.next_event_id, &mut self.stats);
            self.stats.l1_to_l2_compactions += 1;
        }

        if history.l2.len() >= l2_thresh {
            squashed += Self::do_compact_l2_to_l3(
                &mut history,
                &mut self.next_event_id,
                &mut self.stats,
                l3_audit,
            );
            self.stats.l2_to_l3_compactions += 1;
        }

        self.stats.total_compaction_runs += 1;
        self.stats.events_squashed += squashed;

        // Put the history back
        self.histories.insert(workflow_key, history);

        squashed
    }

    /// Run compaction across all workflows.
    pub fn compact_all(&mut self) -> u64 {
        let workflow_keys: Vec<u64> = self.histories.keys().copied().collect();
        let mut total_squashed = 0;
        for key in workflow_keys {
            total_squashed += self.compact_workflow(key);
        }
        total_squashed
    }

    /// L0 → L1: Group activity events and squash retries.
    fn do_compact_l0_to_l1(
        history: &mut WorkflowHistory,
        next_id: &mut u64,
        stats: &mut CompactionStats,
    ) -> u64 {
        let to_compact: Vec<CompactableEvent> = history.l0.drain(..).collect();
        let original_count = to_compact.len() as u64;

        let mut compacted: Vec<CompactableEvent> = Vec::new();
        let mut i = 0;

        while i < to_compact.len() {
            match to_compact[i].event_type {
                CompactableEventType::ActivityTaskScheduled
                | CompactableEventType::ActivityTaskStarted
                | CompactableEventType::ActivityTaskCompleted
                | CompactableEventType::ActivityTaskFailed => {
                    let group_start = i;
                    let mut group_end = i + 1;
                    while group_end < to_compact.len() {
                        match to_compact[group_end].event_type {
                            CompactableEventType::ActivityTaskScheduled
                            | CompactableEventType::ActivityTaskStarted
                            | CompactableEventType::ActivityTaskCompleted
                            | CompactableEventType::ActivityTaskFailed => group_end += 1,
                            _ => break,
                        }
                    }

                    let first = &to_compact[group_start];
                    let last = &to_compact[group_end - 1];
                    let eid = *next_id;
                    *next_id += 1;
                    compacted.push(CompactableEvent {
                        event_id: eid,
                        workflow_key: first.workflow_key,
                        event_type: CompactableEventType::ActivityCompacted,
                        level: CompactionLevel::L1,
                        timestamp_ms: last.timestamp_ms,
                        payload: last.payload.clone(),
                        merged_range: Some((first.event_id, last.event_id)),
                    });
                    i = group_end;
                }
                _ => {
                    let mut e = to_compact[i].clone();
                    e.level = CompactionLevel::L1;
                    compacted.push(e);
                    i += 1;
                }
            }
        }

        let squashed = original_count - compacted.len() as u64;
        for event in compacted {
            history.l1.push_back(event);
        }
        stats.total_events_l1 += history.l1.len() as u64;
        squashed
    }

    /// L1 → L2: Merge completed step sequences into state delta events.
    fn do_compact_l1_to_l2(
        history: &mut WorkflowHistory,
        next_id: &mut u64,
        stats: &mut CompactionStats,
    ) -> u64 {
        let to_compact: Vec<CompactableEvent> = history.l1.drain(..).collect();
        let original_count = to_compact.len() as u64;

        let mut compacted: Vec<CompactableEvent> = Vec::new();
        let mut i = 0;

        while i < to_compact.len() {
            if to_compact[i].event_type == CompactableEventType::WorkflowTaskScheduled {
                let group_start = i;
                let mut group_end = i + 1;
                while group_end < to_compact.len()
                    && to_compact[group_end].event_type
                        != CompactableEventType::WorkflowTaskCompleted
                {
                    group_end += 1;
                }
                if group_end < to_compact.len() {
                    group_end += 1;
                }

                let first = &to_compact[group_start];
                let last = &to_compact[(group_end - 1).min(to_compact.len() - 1)];
                let eid = *next_id;
                *next_id += 1;
                compacted.push(CompactableEvent {
                    event_id: eid,
                    workflow_key: first.workflow_key,
                    event_type: CompactableEventType::StepCompacted,
                    level: CompactionLevel::L2,
                    timestamp_ms: last.timestamp_ms,
                    payload: Vec::new(),
                    merged_range: Some((first.event_id, last.event_id)),
                });
                i = group_end;
            } else {
                let mut e = to_compact[i].clone();
                e.level = CompactionLevel::L2;
                compacted.push(e);
                i += 1;
            }
        }

        let squashed = original_count - compacted.len() as u64;
        for event in compacted {
            history.l2.push_back(event);
        }
        stats.total_events_l2 += history.l2.len() as u64;
        squashed
    }

    /// L2 → L3: Keep only terminal state + last N events for audit.
    fn do_compact_l2_to_l3(
        history: &mut WorkflowHistory,
        next_id: &mut u64,
        stats: &mut CompactionStats,
        audit_count: usize,
    ) -> u64 {
        let to_compact: Vec<CompactableEvent> = history.l2.drain(..).collect();
        let original_count = to_compact.len() as u64;

        if to_compact.len() > audit_count {
            let first = to_compact[0].clone();
            history.l3.push_back(first);

            let last = to_compact.last().unwrap();
            let eid = *next_id;
            *next_id += 1;
            history.l3.push_back(CompactableEvent {
                event_id: eid,
                workflow_key: last.workflow_key,
                event_type: CompactableEventType::TerminalState,
                level: CompactionLevel::L3,
                timestamp_ms: last.timestamp_ms,
                payload: Vec::new(),
                merged_range: Some((to_compact[0].event_id, last.event_id)),
            });

            let start = to_compact.len().saturating_sub(audit_count);
            for event in &to_compact[start..] {
                let mut e = event.clone();
                e.level = CompactionLevel::L3;
                history.l3.push_back(e);
            }
        } else {
            for mut event in to_compact {
                event.level = CompactionLevel::L3;
                history.l3.push_back(event);
            }
        }

        stats.total_events_l3 += history.l3.len() as u64;
        original_count
    }

    pub fn workflow_event_count(&self, workflow_key: u64) -> usize {
        self.histories
            .get(&workflow_key)
            .map(|h| h.total_events())
            .unwrap_or(0)
    }

    pub fn get_events(&self, workflow_key: u64, level: CompactionLevel) -> Vec<CompactableEvent> {
        self.histories
            .get(&workflow_key)
            .map(|h| match level {
                CompactionLevel::L0 => h.l0.iter().cloned().collect(),
                CompactionLevel::L1 => h.l1.iter().cloned().collect(),
                CompactionLevel::L2 => h.l2.iter().cloned().collect(),
                CompactionLevel::L3 => h.l3.iter().cloned().collect(),
            })
            .unwrap_or_default()
    }

    pub fn stats(&self) -> CompactionStats {
        self.stats.clone()
    }
    pub fn workflow_count(&self) -> usize {
        self.histories.len()
    }

    pub fn remove_workflow(&mut self, workflow_key: u64) -> bool {
        self.histories.remove(&workflow_key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CompactionConfig {
        CompactionConfig {
            l0_threshold: 5,
            l1_threshold: 10, // High to prevent L1→L2 cascade in unit tests
            l2_threshold: 10,
            l3_audit_trail_count: 3,
            ..Default::default()
        }
    }

    #[test]
    fn test_append_events() {
        let mut compactor = HistoryCompactor::new(test_config());
        compactor.append_event(1, CompactableEventType::WorkflowStarted, vec![]);
        compactor.append_event(1, CompactableEventType::ActivityTaskScheduled, vec![]);
        compactor.append_event(1, CompactableEventType::ActivityTaskCompleted, vec![]);
        assert_eq!(compactor.workflow_event_count(1), 3);
    }

    #[test]
    fn test_l0_to_l1_compaction() {
        let mut compactor = HistoryCompactor::new(test_config());
        compactor.append_event(1, CompactableEventType::WorkflowStarted, vec![]);
        compactor.append_event(1, CompactableEventType::ActivityTaskScheduled, vec![]);
        compactor.append_event(1, CompactableEventType::ActivityTaskStarted, vec![]);
        compactor.append_event(1, CompactableEventType::ActivityTaskCompleted, vec![]);
        compactor.append_event(1, CompactableEventType::SignalReceived, vec![]);

        let squashed = compactor.compact_workflow(1);
        assert!(squashed > 0);
        let l1_events = compactor.get_events(1, CompactionLevel::L1);
        assert!(!l1_events.is_empty());
    }

    #[test]
    fn test_multiple_workflows() {
        let mut compactor = HistoryCompactor::new(test_config());
        compactor.append_event(1, CompactableEventType::WorkflowStarted, vec![]);
        compactor.append_event(2, CompactableEventType::WorkflowStarted, vec![]);
        assert_eq!(compactor.workflow_count(), 2);
    }

    #[test]
    fn test_compact_all() {
        let mut compactor = HistoryCompactor::new(test_config());
        for wf in 0..3u64 {
            for _ in 0..6 {
                compactor.append_event(wf, CompactableEventType::ActivityTaskScheduled, vec![]);
            }
        }
        let total = compactor.compact_all();
        assert!(total > 0);
    }

    #[test]
    fn test_remove_workflow() {
        let mut compactor = HistoryCompactor::new(test_config());
        compactor.append_event(1, CompactableEventType::WorkflowStarted, vec![]);
        assert!(compactor.remove_workflow(1));
        assert_eq!(compactor.workflow_event_count(1), 0);
    }

    #[test]
    fn test_stats() {
        let mut compactor = HistoryCompactor::new(test_config());
        compactor.append_event(1, CompactableEventType::WorkflowStarted, vec![]);
        compactor.append_event(1, CompactableEventType::ActivityTaskScheduled, vec![]);
        let stats = compactor.stats();
        assert_eq!(stats.total_events_l0, 2);
    }
}
